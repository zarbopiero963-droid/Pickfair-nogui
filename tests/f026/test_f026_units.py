"""PF-ALL-F026 v2 / F2 — test unitari sul codice reale (nessun mock del codice sotto test).

- grandezze del conto dai soli campi exchange, separate dalla base dello stake;
- auto-reset deciso sul regolato (equita' di cassa), mai su dati non valutabili;
- il reset trattiene posizioni non regolate e riserve incerte;
- F2: al settlement senza table_id si libera il tavolo del mercato solo se univoco e certo.
"""
import math
from types import SimpleNamespace

import pytest

from core.duplication_guard import DuplicationGuard
from core.runtime_controller import RuntimeController
from core.table_manager import TableManager

account = RuntimeController._account_from_funds


@pytest.mark.parametrize("funds,expected", [
    ({"available": 71.0, "exposure": 29.0}, (71.0, 29.0, 100.0)),       # SIM: esposizione positiva
    ({"available": 71.0, "exposure": -29.0}, (71.0, 29.0, 100.0)),      # LIVE Betfair: exposure negativa
    ({"available": 100.0, "exposure": 0.0}, (100.0, 0.0, 100.0)),
    ({"available": 0.0, "exposure": -50.0}, (0.0, 50.0, 50.0)),         # conto interamente impegnato: non e' perdita
])
def test_account_quantities_are_separate_and_from_exchange_fields(funds, expected):
    a = account(funds)
    assert a["ok"] is True
    assert (a["available"], a["committed"], a["cash_equity"]) == pytest.approx(expected)


@pytest.mark.parametrize("funds", [
    None, "x", {}, {"available": 10.0},                                  # esposizione mancante: non vale zero
    {"exposure": 0.0}, {"available": "abc", "exposure": 0.0}, {"available": float("nan"), "exposure": 0.0},
    {"available": -1.0, "exposure": 0.0}, {"available": 10.0, "exposure": float("inf")},
    {"available": 10.0, "exposure": None},
])
def test_account_invalid_or_incomplete_funds_fail_closed(funds):
    assert account(funds)["ok"] is False


@pytest.mark.parametrize("funds", [{"available": 10.0}, {"exposure": 0.0}, {"available": 10.0, "exposure": None}])
def test_account_missing_field_is_reported_as_missing_not_as_zero(funds):
    assert account(funds)["reason"] == "BALANCE_FIELD_MISSING"


def _rc():
    rc = RuntimeController.__new__(RuntimeController)
    rc.table_manager = TableManager(table_count=3)
    rc.duplication_guard = DuplicationGuard()
    return rc


def test_cash_drawdown_uses_settled_cash_not_available():
    rc = _rc()
    rc._record_account_snapshot({"available": 100.0, "exposure": 0.0}, source="t")
    rc._record_account_snapshot({"available": 70.0, "exposure": 30.0}, source="t")   # 30 impegnati: nessuna perdita
    assert rc._cash_drawdown_pct() == 0.0
    rc._record_account_snapshot({"available": 72.0, "exposure": 8.0}, source="t")    # perdita regolata 20
    assert math.isclose(rc._cash_drawdown_pct(), 20.0)


def test_cash_drawdown_not_evaluable_without_valid_snapshot():
    rc = _rc()
    assert rc._cash_drawdown_pct() is None
    rc._record_account_snapshot({"available": 100.0, "exposure": 0.0}, source="t")
    rc._record_account_snapshot({"available": 50.0}, source="t")                     # esposizione mancante
    assert rc._cash_drawdown_pct() is None, "dato non valutabile: nessun auto-reset"


class _Bus:
    def __init__(self):
        self.events = []

    def publish(self, name, payload=None):
        self.events.append((name, dict(payload or {})))


def _rc_threshold(pct):
    rc = _rc()
    rc.config = SimpleNamespace(auto_reset_drawdown_pct=pct)
    rc.bus = _Bus()
    rc.cycle_resets = 0

    def _cycle():                                # ciclo di recovery: fuori dall'oggetto del test
        rc.cycle_resets += 1
    rc._reset_recovery_cycle_keeping_losses = _cycle
    return rc


def test_auto_reset_fires_only_at_or_beyond_threshold_on_settled_cash():
    rc = _rc_threshold(15.0)
    rc._record_account_snapshot({"available": 100.0, "exposure": 0.0}, source="t")
    rc._record_account_snapshot({"available": 60.0, "exposure": 30.0}, source="t")   # cassa -10%: sotto soglia
    assert rc._maybe_auto_reset(40.0) is False and rc.bus.events == [] and rc.cycle_resets == 0
    rc._record_account_snapshot({"available": 60.0, "exposure": 25.0}, source="t")   # cassa -15%: soglia
    assert rc._maybe_auto_reset(40.0) is True and rc.cycle_resets == 1
    name, payload = rc.bus.events[-1]
    assert name == "ROSERPINA_AUTO_RESET" and payload["drawdown_basis"] == "cash_equity"
    assert math.isclose(payload["drawdown_pct"], 15.0) and payload["available_drawdown_pct"] == 40.0
    assert rc._cash_equity_peak == 85.0, "il reset ribasa il picco di cassa (OC-04)"


def test_auto_reset_never_fires_on_unevaluable_account():
    rc = _rc_threshold(0.0)                     # soglia minima: qualunque drawdown valutato scatterebbe
    _activate(rc, 1, "EV-SETTLED", "1.10")
    rc._mark_market_settled("1.10")             # un reset lo libererebbe
    assert rc._maybe_auto_reset(99.0) is False, "nessuno snapshot: nessun auto-reset"
    rc._record_account_snapshot({"available": 100.0, "exposure": 0.0}, source="t")
    rc._record_account_snapshot({"available": 10.0}, source="t")                      # esposizione mancante
    assert rc._maybe_auto_reset(99.0) is False, "dato non valutabile: nessun auto-reset (fail-closed)"
    assert rc.bus.events == [] and rc.cycle_resets == 0
    assert rc.table_manager.snapshot()[0]["current_event_key"] == "EV-SETTLED"


def test_invalid_snapshot_never_moves_cash_peak():
    rc = _rc()
    rc._record_account_snapshot({"available": 100.0, "exposure": 0.0}, source="t")
    rc._record_account_snapshot({"available": 1e9, "exposure": float("nan")}, source="t")
    assert rc._cash_equity_peak == 100.0


def _activate(rc, tid, key, mid, exp=10.0):
    rc.duplication_guard.acquire(key)
    rc.table_manager.activate(table_id=tid, event_key=key, exposure=exp, market_id=mid, selection_id=7)


def test_reset_keeps_live_unsettled_positions_and_their_keys():
    rc = _rc()
    _activate(rc, 1, "EV-LIVE", "1.1")
    _activate(rc, 2, "EV-SETTLED", "1.2")
    rc._mark_market_settled("1.2")
    rc._reset_reservations_keeping_uncertain()
    tabs = {t["table_id"]: t for t in rc.table_manager.snapshot()}
    assert tabs[1]["current_event_key"] == "EV-LIVE" and tabs[1]["current_exposure"] == 10.0
    assert tabs[2]["status"] == "FREE"
    assert not rc.duplication_guard.acquire("EV-LIVE"), "chiave della posizione viva liberata dal reset"
    assert rc.duplication_guard.acquire("EV-SETTLED")


def test_ambiguous_reservation_survives_reset_even_if_its_market_is_settled():
    rc = _rc()
    _activate(rc, 1, "EV-AMB", "1.9", exp=5.0)
    rc._release_if_terminal({"event_key": "EV-AMB", "table_id": 1}, event_name="QUICK_BET_AMBIGUOUS")
    rc._mark_market_settled("1.9")
    rc._reset_reservations_keeping_uncertain()
    tab = {t["table_id"]: t for t in rc.table_manager.snapshot()}[1]
    assert tab["current_event_key"] == "EV-AMB" and tab["current_exposure"] == 5.0, tab
    assert not rc.duplication_guard.acquire("EV-AMB")


def test_mark_market_settled_ignores_empty_ids():
    rc = _rc()
    rc._mark_market_settled(None)
    rc._mark_market_settled("  ")
    assert not getattr(rc, "_settled_market_ids", set())


# ------------------------------------------------------------------ F2: rilascio al settlement senza table_id
def _tables(rc):
    return {t["table_id"]: t for t in rc.table_manager.snapshot()}


def test_settlement_without_table_id_releases_the_unique_table_of_the_market():
    rc = _rc()
    _activate(rc, 1, "EV-A", "1.10")
    _activate(rc, 2, "EV-B", "1.20")
    rc._release_settled_tables({"market_id": "1.10"}, table_id=None, pnl=-10.0)
    t = _tables(rc)
    assert t[1]["current_event_key"] == "" and t[1]["current_exposure"] == 0.0
    assert t[1]["status"] == "RECOVERY" and t[1]["loss_amount"] == 10.0, "P&L del tavolo come col table_id"
    assert t[2]["current_event_key"] == "EV-B"


def test_settlement_without_table_id_keeps_uncertain_reservation():
    rc = _rc()
    _activate(rc, 1, "EV-AMB", "1.10")
    rc._release_if_terminal({"event_key": "EV-AMB", "table_id": 1}, event_name="QUICK_BET_AMBIGUOUS")
    rc._release_settled_tables({"market_id": "1.10"}, table_id=None, pnl=5.0)
    assert _tables(rc)[1]["current_event_key"] == "EV-AMB"


def test_settlement_without_table_id_with_two_tables_on_market_releases_nothing():
    rc = _rc()
    _activate(rc, 1, "EV-A", "1.10")
    _activate(rc, 2, "EV-A2", "1.10")
    rc._release_settled_tables({"market_id": "1.10"}, table_id=None, pnl=5.0)
    t = _tables(rc)
    assert t[1]["current_event_key"] == "EV-A" and t[2]["current_event_key"] == "EV-A2"


def test_settlement_marks_market_even_when_its_tables_are_held_so_reset_can_free_them():
    rc = _rc()
    _activate(rc, 1, "EV-A", "1.10")
    _activate(rc, 2, "EV-A2", "1.10")
    rc._release_settled_tables({"market_id": "1.10"}, table_id=None, pnl=5.0)   # due tavoli: nessun rilascio
    assert "1.10" in rc._settled_market_ids
    rc._reset_reservations_keeping_uncertain()
    t = _tables(rc)
    assert t[1]["current_event_key"] == "" and t[2]["current_event_key"] == "", "mercato regolato: il reset libera"
    assert rc.duplication_guard.acquire("EV-A") and rc.duplication_guard.acquire("EV-A2")


def test_settlement_with_table_id_also_marks_the_market_settled():
    rc = _rc()
    _activate(rc, 1, "EV-A", "1.10")
    rc._release_settled_tables({"market_id": "1.10"}, table_id=1, pnl=2.0)
    assert "1.10" in rc._settled_market_ids


def test_settlement_without_market_and_table_releases_nothing():
    rc = _rc()
    _activate(rc, 1, "EV-A", "1.10")
    rc._release_settled_tables({}, table_id=None, pnl=5.0)
    assert _tables(rc)[1]["current_event_key"] == "EV-A"


def test_settlement_with_table_id_keeps_previous_behaviour():
    rc = _rc()
    _activate(rc, 1, "EV-A", "1.10")
    rc._release_settled_tables({"market_id": "1.99"}, table_id=1, pnl=2.0)
    assert _tables(rc)[1]["current_event_key"] == ""
