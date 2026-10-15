"""F032 (espansione) e RSR-12 (tavoli di recovery) — decisioni owner del mandato PF-ROSERPINA-CONTINUOUS-DELIVERY-04.

F032: la crescita per l'espansione si misura sull'equita' di cassa REGOLATA rispetto a una baseline persistente fissata
all'inizio del ciclo (transizioni esplicite: primo avvio, auto-reset, reset del ciclo), distinta dal saldo, dal
disponibile e dal picco del drawdown. Dato incerto => nessuna espansione.
RSR-12: il limite conta i tavoli con recovery attiva anche senza ordini; lo stato di recovery sopravvive al riavvio ed
esce solo con la chiusura esplicita (perdita recuperata) o con un reset autorizzato del ciclo.
Codice reale; Database SQLite reale su tmp_path; servizio exchange stub.
"""
import json
from types import SimpleNamespace

import pytest

from core.duplication_guard import DuplicationGuard
from core.money_management import RoserpinaMoneyManagement
from core.risk_desk import RiskDesk
from core.runtime_controller import RuntimeController
from core.system_state import DeskMode, RoserpinaConfig, RuntimeMode
from core.table_manager import TableManager
from database import Database


class _Bus:
    def __init__(self):
        self.events = []

    def publish(self, name, payload=None):
        self.events.append((name, dict(payload or {})))


class _Funds:
    def __init__(self, funds):
        self.funds = dict(funds)

    def get_account_funds(self):
        return dict(self.funds)


def _rc(db_path, funds=None, tables=3):
    rc = RuntimeController.__new__(RuntimeController)
    rc.db = Database(str(db_path))
    rc.bus = _Bus()
    rc.mode = RuntimeMode.ACTIVE
    rc.table_manager = TableManager(table_count=tables)
    rc.duplication_guard = DuplicationGuard()
    rc.risk_desk = RiskDesk()
    rc.betfair_service = _Funds(funds or {"available": 100.0, "exposure": 0.0})
    rc.config = SimpleNamespace(expansion_profit_pct=5.0, max_recovery_tables=2, allow_recovery=True)
    return rc


# ------------------------------------------------------------------ F032 — money management
def _mm(**over):
    cfg = RoserpinaConfig()
    cfg.max_single_bet_pct = cfg.max_total_exposure_pct = cfg.max_event_exposure_pct = 100.0
    cfg.min_stake = 0.01
    cfg.target_profit_cycle_pct = 4.0
    cfg.expansion_profit_pct = 5.0
    cfg.expansion_multiplier = 1.25
    for k, v in over.items():
        setattr(cfg, k, v)
    return RoserpinaMoneyManagement(cfg)


def _calc(mm, **sig):
    return mm.calculate(signal=dict({"price": 3.0, "side": "BACK", "bet_type": "BACK"}, **sig), bankroll_current=100.0,
                        equity_peak=100.0, current_total_exposure=0.0, event_current_exposure=0.0, table=None)


@pytest.mark.parametrize("growth,mode", [(4.99, DeskMode.NORMAL), (5.0, DeskMode.EXPANSION), (12.0, DeskMode.EXPANSION)])
def test_f032_growth_snapshot_drives_expansion(growth, mode):
    d = _calc(_mm(), expansion_growth_snapshot=growth)
    assert d.desk_mode is mode
    assert d.recommended_stake == pytest.approx(2.0 * (1.25 if mode is DeskMode.EXPANSION else 1.0))


def test_f032_unknown_growth_never_expands():
    assert _calc(_mm(), expansion_growth_snapshot=None).desk_mode is DeskMode.NORMAL


def test_f032_expansion_never_overrides_defense_or_caps():
    mm = _mm(max_single_bet_pct=2.0)                                   # cap singolo 2 su bankroll 100
    d = _calc(mm, expansion_growth_snapshot=50.0)
    assert d.recommended_stake <= 2.0 + 1e-9
    lock = mm.calculate(signal={"price": 3.0, "side": "BACK", "expansion_growth_snapshot": 50.0},
                        bankroll_current=70.0, equity_peak=100.0, current_total_exposure=0.0,
                        event_current_exposure=0.0, table=None)
    assert lock.approved is False and lock.reason == "desk_lockdown", "espansione non aggira il LOCKDOWN"


# ------------------------------------------------------------------ F032 — baseline del ciclo nel runtime
def test_f032_baseline_set_at_cycle_start_persisted_and_restored(tmp_path):
    rc = _rc(tmp_path / "a.db")
    assert rc._start_expansion_cycle("first_start", funds={"available": 100.0, "exposure": 0.0}) is True
    rc._record_account_snapshot({"available": 103.0, "exposure": 2.0}, source="settlement")   # cassa 105
    assert rc._expansion_growth_pct() == pytest.approx(5.0)
    again = _rc(tmp_path / "a.db")                                     # riavvio: stesso DB
    again._load_expansion_baseline()
    again._record_account_snapshot({"available": 103.0, "exposure": 2.0}, source="start")
    assert again._expansion_growth_pct() == pytest.approx(5.0), "baseline persa al riavvio"


def test_f032_baseline_is_not_the_drawdown_peak_and_does_not_follow_the_balance(tmp_path):
    rc = _rc(tmp_path / "b.db")
    rc._start_expansion_cycle("first_start", funds={"available": 100.0, "exposure": 0.0})
    for cash in (101.0, 103.0, 106.0):                                 # profitti regolati successivi
        rc._record_account_snapshot({"available": cash, "exposure": 0.0}, source="settlement")
        rc.risk_desk.sync_bankroll(cash)
    assert rc._expansion_growth_pct() == pytest.approx(6.0)
    assert rc.risk_desk.equity_peak == pytest.approx(106.0) and rc._expansion_baseline == pytest.approx(100.0)


def test_f032_open_positions_are_capital_not_profit(tmp_path):
    rc = _rc(tmp_path / "c.db")
    rc._start_expansion_cycle("first_start", funds={"available": 100.0, "exposure": 0.0})
    rc._record_account_snapshot({"available": 80.0, "exposure": 20.0}, source="settlement")   # 20 impegnati
    assert rc._expansion_growth_pct() == pytest.approx(0.0)


@pytest.mark.parametrize("case", ["no_baseline", "invalid_snapshot", "uncertain_reservation"])
def test_f032_uncertain_state_means_no_growth(tmp_path, case):
    rc = _rc(tmp_path / "d.db")
    if case != "no_baseline":
        rc._start_expansion_cycle("first_start", funds={"available": 100.0, "exposure": 0.0})
    rc._record_account_snapshot({"available": 120.0, "exposure": 0.0}, source="settlement")
    if case == "invalid_snapshot":
        rc._record_account_snapshot({"available": 120.0}, source="settlement")
    if case == "uncertain_reservation":
        rc._uncertain_reservations_map()["K"] = 1
    assert rc._expansion_growth_pct() is None


def test_f032_unreadable_funds_at_cycle_start_leave_no_baseline(tmp_path):
    rc = _rc(tmp_path / "e.db", funds={"available": 100.0})            # esposizione mancante
    assert rc._start_expansion_cycle("reset_cycle") is False
    rc._record_account_snapshot({"available": 150.0, "exposure": 0.0}, source="settlement")
    assert rc._expansion_growth_pct() is None


def test_f032_cycle_transition_rebases_baseline(tmp_path):
    rc = _rc(tmp_path / "f.db", funds={"available": 120.0, "exposure": 0.0})
    rc._start_expansion_cycle("first_start", funds={"available": 100.0, "exposure": 0.0})
    rc._record_account_snapshot({"available": 120.0, "exposure": 0.0}, source="settlement")
    assert rc._expansion_growth_pct() == pytest.approx(20.0)
    rc._start_expansion_cycle("reset_cycle")                           # transizione esplicita: legge l'exchange
    assert rc._expansion_growth_pct() == pytest.approx(0.0)
    state = json.loads(rc.db.get_settings()["roserpina.expansion_baseline_state"])
    assert state["baseline"] == pytest.approx(120.0) and state["reason"] == "reset_cycle"


def test_f032_signal_carries_growth_snapshot(tmp_path):
    rc = _rc(tmp_path / "g.db")
    rc._start_expansion_cycle("first_start", funds={"available": 100.0, "exposure": 0.0})
    rc._record_account_snapshot({"available": 110.0, "exposure": 0.0}, source="settlement")
    sig = rc._with_expansion_snapshot({"price": 2.0})
    assert sig["expansion_growth_snapshot"] == pytest.approx(10.0)
    rc._uncertain_reservations_map()["K"] = 1
    assert rc._with_expansion_snapshot({"price": 2.0})["expansion_growth_snapshot"] is None


# ------------------------------------------------------------------ RSR-12 — stato di recovery persistente
def _lose(rc, table_id, key, loss):
    rc.table_manager.activate(table_id=table_id, event_key=key, exposure=loss, market_id=f"1.{table_id}", selection_id=1)
    rc.table_manager.release(table_id, pnl=-loss)
    rc._persist_recovery_tables()


def test_rsr12_recovery_tables_survive_restart_and_keep_counting(tmp_path):
    rc = _rc(tmp_path / "r.db")
    _lose(rc, 1, "A", 3.0)
    _lose(rc, 2, "B", 2.0)
    assert rc._active_recovery_count() == 2
    again = _rc(tmp_path / "r.db")
    again._restore_recovery_tables()
    t = {x["table_id"]: x for x in again.table_manager.snapshot()}
    assert t[1]["status"] == "RECOVERY" and t[1]["loss_amount"] == pytest.approx(3.0)
    assert t[2]["loss_amount"] == pytest.approx(2.0)
    assert again._active_recovery_count() == 2, "il riavvio ha liberato capacita' di recovery"


def test_rsr12_idle_recovery_table_counts_and_explicit_closure_removes_it(tmp_path):
    rc = _rc(tmp_path / "s.db")
    _lose(rc, 1, "A", 3.0)
    assert rc._active_recovery_count() == 1                            # nessun ordine aperto, conta lo stesso
    rc.table_manager.activate(table_id=1, event_key="A2", exposure=1.0, market_id="1.1", selection_id=1)
    rc.table_manager.release(1, pnl=+3.0)                              # perdita recuperata: chiusura esplicita
    rc._persist_recovery_tables()
    assert rc._active_recovery_count() == 0
    again = _rc(tmp_path / "s.db")
    again._restore_recovery_tables()
    assert again._active_recovery_count() == 0


def test_rsr12_partial_recovery_stays_counted(tmp_path):
    rc = _rc(tmp_path / "t.db")
    _lose(rc, 1, "A", 3.0)
    rc.table_manager.activate(table_id=1, event_key="A2", exposure=1.0, market_id="1.1", selection_id=1)
    rc.table_manager.release(1, pnl=+1.0)
    rc._persist_recovery_tables()
    again = _rc(tmp_path / "t.db")
    again._restore_recovery_tables()
    assert again._active_recovery_count() == 1
    assert again.table_manager.snapshot()[0]["loss_amount"] == pytest.approx(2.0)


def test_rsr12_restore_never_overwrites_an_occupied_table(tmp_path):
    rc = _rc(tmp_path / "u.db")
    _lose(rc, 1, "A", 3.0)
    again = _rc(tmp_path / "u.db")
    again.table_manager.activate(table_id=1, event_key="LIVE", exposure=5.0, market_id="1.9", selection_id=1)
    again._restore_recovery_tables()
    t = again.table_manager.snapshot()[0]
    assert t["current_event_key"] == "LIVE" and t["current_exposure"] == pytest.approx(5.0)
    assert t["loss_amount"] == pytest.approx(3.0) and again._active_recovery_count() == 1


def test_rsr12_unreadable_state_is_fail_closed(tmp_path):
    rc = _rc(tmp_path / "v.db")
    rc.db.save_settings({"roserpina.recovery_tables_state": "{rotto"})
    assert rc._restore_recovery_tables() is False
    assert rc._recovery_state_unreadable is True
    assert rc._active_recovery_count() >= rc.config.max_recovery_tables, "stato illeggibile: capacita' non liberata"
