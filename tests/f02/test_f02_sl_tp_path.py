"""F-02 — SL/TP/trailing arrivano dal segnale o dalla configurazione fino al monitor, anche in SIM e dopo un riavvio.

Prima di F-02 (audit su #586): nessuna chiave SL/TP esisteva; la meta del tavolo non portava mai stop_loss/take_profit
(F-CFG-01), quindi il monitor non agiva mai; il monitor girava solo con lo streaming (mai in SIM o in poll); lo scatto
pubblicava una richiesta di cashout senza lato ne' quota, sempre rifiutata (R3-01). Contratto della scheda: default OFF,
NaN/-5/150 rifiutati, SL attivo in perdita => ``SL_TRIGGERED``, SL assente => nessun evento, nessun ordine dal trigger
finche' F-04.
"""
from __future__ import annotations

import json
import math
from types import SimpleNamespace

import pytest

from core.duplication_guard import DuplicationGuard
from core.runtime_controller import RuntimeController
from core.system_state import RoserpinaConfig, RuntimeMode
from core.table_manager import TableManager
from database import Database
from services.setting_service import InvalidSettingsError, SettingsService

pytestmark = [pytest.mark.unit, pytest.mark.safety]

REQUIRED = {"roserpina.target_profit_cycle_pct": 3.0, "roserpina.max_single_bet_pct": 18.0,
            "roserpina.max_total_exposure_pct": 35.0, "roserpina.max_event_exposure_pct": 18.0}


# ── configurazione ───────────────────────────────────────────────────────────────────────────────────────────────────

@pytest.fixture()
def svc(tmp_path):
    return SettingsService(Database(str(tmp_path / "f02.db")))


def test_sl_tp_trailing_are_off_by_default(svc):
    svc.save_settings(REQUIRED)
    cfg = svc.load_roserpina_config()
    assert cfg.stop_loss_pct is None and cfg.take_profit_pct is None and cfg.trailing_stop_pct is None


def test_sl_tp_survive_save_and_restart(svc, tmp_path):
    svc.save_settings({**REQUIRED, "roserpina.stop_loss_pct": 20.0, "roserpina.take_profit_pct": "35,5"})
    cfg = SettingsService(Database(str(tmp_path / "f02.db"))).load_roserpina_config()
    assert cfg.stop_loss_pct == 20.0 and cfg.take_profit_pct == 35.5 and not cfg.invalid_settings


def test_gui_round_trip_keeps_and_can_switch_off_sl(svc):
    svc.save_settings({**REQUIRED, "roserpina.stop_loss_pct": 20.0})
    cfg = svc.load_roserpina_config()
    svc.save_roserpina_config(cfg)
    assert svc.load_roserpina_config().stop_loss_pct == 20.0
    cfg.stop_loss_pct = None
    svc.save_roserpina_config(cfg)
    assert svc.load_roserpina_config().stop_loss_pct is None, "SL non disattivabile dalla tab Roserpina"


@pytest.mark.parametrize("key", ["roserpina.stop_loss_pct", "roserpina.take_profit_pct", "roserpina.trailing_stop_pct"])
@pytest.mark.parametrize("bad", ["nan", float("inf"), -5, 150, 0, "abc", True])
def test_invalid_sl_tp_is_rejected_at_save(svc, key, bad):
    with pytest.raises(InvalidSettingsError, match=key):
        svc.save_settings({**REQUIRED, key: bad})


def test_corrupt_sl_in_db_is_never_silently_off(svc):
    svc.db.save_settings({**REQUIRED, "roserpina.stop_loss_pct": "abc"})
    cfg = svc.load_roserpina_config()
    assert math.isnan(cfg.stop_loss_pct) and "roserpina.stop_loss_pct" in cfg.invalid_settings


# ── impostazioni della posizione ─────────────────────────────────────────────────────────────────────────────────────

def _rc(config=None):
    rc = RuntimeController.__new__(RuntimeController)
    rc.config = config or RoserpinaConfig()
    return rc


def test_position_takes_sl_tp_from_config():
    pcts, reason = _rc(RoserpinaConfig(stop_loss_pct=20.0, take_profit_pct=50.0))._sl_tp_settings({})
    assert reason == "" and pcts == {"stop_loss_pct": 20.0, "take_profit_pct": 50.0}


def test_signal_value_overrides_config():
    pcts, reason = _rc(RoserpinaConfig(stop_loss_pct=20.0))._sl_tp_settings({"stop_loss_pct": "10"})
    assert reason == "" and pcts == {"stop_loss_pct": 10.0}


def test_no_sl_tp_anywhere_means_off():
    assert _rc()._sl_tp_settings({}) == ({}, "")


@pytest.mark.parametrize("bad", ["abc", -1, 150, float("nan")])
def test_invalid_signal_sl_rejects_the_signal(bad):
    assert _rc(RoserpinaConfig(stop_loss_pct=20.0))._sl_tp_settings({"stop_loss_pct": bad}) == (
        {}, "stop_loss_pct_non_valido")


def test_invalid_config_sl_rejects_the_signal():
    assert _rc(RoserpinaConfig(stop_loss_pct=float("nan")))._sl_tp_settings({})[1] == "stop_loss_pct_non_valido"


def test_back_amounts_are_a_share_of_the_stake():
    # BACK 3.00 a 2.0: rischio 3.00; SL 20 % => 0.60, TP 50 % => 1.50
    meta = RuntimeController._sl_tp_meta({"stop_loss_pct": 20.0, "take_profit_pct": 50.0}, 3.0, 3.0)
    assert meta == {"stake": 3.0, "stop_loss_pct": 20.0, "stop_loss": 0.6, "take_profit_pct": 50.0, "take_profit": 1.5}


def test_lay_amounts_are_a_share_of_the_liability():
    # LAY 2.00 a 3.0: liability 4.00; SL 20 % => 0.80 (non 0.40 sullo stake)
    meta = RuntimeController._sl_tp_meta({"stop_loss_pct": 20.0}, 4.0, 2.0)
    assert meta["stop_loss"] == 0.8 and meta["stake"] == 2.0


def test_microstake_amounts_keep_cent_precision():
    meta = RuntimeController._sl_tp_meta({"stop_loss_pct": 25.0}, 0.10, 0.10)
    # 0,10 € (microstake) mai alzato; 0,025 => 0,02 (ROUND_HALF_EVEN del Decimal: SL piu' stretto, mai piu' largo)
    assert meta["stop_loss"] == 0.02 and meta["stake"] == 0.10


def test_no_pct_means_no_amounts():
    assert RuntimeController._sl_tp_meta({}, 3.0, 3.0) == {"stake": 3.0}


# ── monitor: evento una sola volta, nessun ordine ────────────────────────────────────────────────────────────────────

class _Bus:
    def __init__(self):
        self.published = []

    def publish(self, topic, payload):
        self.published.append((topic, payload))


def _table(sl=None, tp=None, market="1.5"):
    meta = {"price": 2.0, "bet_type": "BACK", "stake": 10.0, "stop_loss": sl, "take_profit": tp, "customer_ref": "r1"}
    return SimpleNamespace(table_id=1, status="ACTIVE", market_id=market, selection_id=11, meta=meta,
                           current_exposure=10.0)


def _book(back, lay, market="1.5"):
    # book negoziabile (stato di mercato e selezione come in listMarketBook / EX_MARKET_DEF): D-C4-01, PF-EXEC-05
    return {"marketId": market, "status": "OPEN", "runners": [{"selectionId": 11, "status": "ACTIVE", "ex": {
        "availableToBack": [{"price": back, "size": 100}], "availableToLay": [{"price": lay, "size": 100}]}}]}


def _monitor(table):
    rc = RuntimeController.__new__(RuntimeController)
    rc.mode = RuntimeMode.ACTIVE
    rc.bus = _Bus()
    rc.table_manager = SimpleNamespace(_tables={table.table_id: table})
    return rc


def test_stop_loss_in_loss_publishes_sl_triggered_once_and_no_order():
    table = _table(sl=1.0)
    rc = _monitor(table)
    rc._monitor_sl_tp_trailing(_book(2.48, 2.5))           # BACK 10 a 2.0, chiusura LAY a 2.5 => netto -2.00
    rc._monitor_sl_tp_trailing(_book(2.98, 3.0))           # peggiora ancora: nessun secondo evento
    events = [(t, p) for t, p in rc.bus.published]
    assert [t for t, _ in events] == ["SL_TRIGGERED"], events
    payload = events[0][1]
    assert payload["side"] == "BACK" and payload["threshold"] == 1.0 and payload["pnl"] < -1.0
    assert payload["customer_ref"] == "r1"
    assert all(t != "REQ_EXECUTE_CASHOUT" for t, _ in events), "ordine dal trigger prima di F-04"


def test_no_stop_loss_means_no_event():
    rc = _monitor(_table())
    rc._monitor_sl_tp_trailing(_book(2.98, 3.0))
    assert rc.bus.published == []


def test_take_profit_publishes_tp_triggered():
    rc = _monitor(_table(tp=1.0))
    rc._monitor_sl_tp_trailing(_book(1.6, 1.62))          # chiusura LAY a 1.62: profitto netto > 1
    assert [t for t, _ in rc.bus.published] == ["TP_TRIGGERED"]


# ── poller SIM/poll ──────────────────────────────────────────────────────────────────────────────────────────────────

class _Service:
    def __init__(self, books):
        self.books, self.calls = books, []

    def get_market_book_snapshot(self, market_id, include_prices=False):
        self.calls.append(market_id)
        return self.books.get(market_id)


def _poller(tables, books, *, feed=None, mode=RuntimeMode.ACTIVE):
    rc = RuntimeController.__new__(RuntimeController)
    rc.mode, rc.bus = mode, _Bus()
    rc.table_manager = SimpleNamespace(_tables={t.table_id: t for t in tables})
    rc.betfair_service = _Service(books)
    rc.streaming_feed = feed
    return rc


def test_poller_runs_the_monitor_without_streaming():
    rc = _poller([_table(sl=1.0)], {"1.5": _book(2.48, 2.5)})
    assert rc._sl_tp_poll_once() == 1
    assert [t for t, _ in rc.bus.published] == ["SL_TRIGGERED"]


def test_poller_reads_only_markets_with_an_armed_protection():
    unarmed = _table(market="9.9")
    unarmed.table_id = 2
    rc = _poller([_table(sl=1.0), unarmed], {"1.5": _book(2.0, 2.02), "9.9": _book(2.0, 2.02, market="9.9")})
    rc._sl_tp_poll_once()
    assert rc.betfair_service.calls == ["1.5"]


def test_poller_stops_reading_once_triggered():
    rc = _poller([_table(sl=1.0)], {"1.5": _book(2.48, 2.5)})
    rc._sl_tp_poll_once()
    rc._sl_tp_poll_once()
    assert rc.betfair_service.calls == ["1.5"]


def test_poller_yields_to_a_healthy_stream_and_covers_a_broken_one():
    healthy = SimpleNamespace(healthy=lambda: True)
    rc = _poller([_table(sl=1.0)], {"1.5": _book(2.48, 2.5)}, feed=healthy)
    assert rc._sl_tp_poll_once() == 0 and rc.betfair_service.calls == []
    broken = SimpleNamespace(healthy=lambda: False)
    rc = _poller([_table(sl=1.0)], {"1.5": _book(2.48, 2.5)}, feed=broken)
    assert rc._sl_tp_poll_once() == 1


def test_poller_does_nothing_when_not_active():
    rc = _poller([_table(sl=1.0)], {"1.5": _book(2.48, 2.5)}, mode=RuntimeMode.STOPPED)
    assert rc._sl_tp_poll_once() == 0 and rc.bus.published == []


def test_unreadable_book_is_no_decision():
    rc = _poller([_table(sl=1.0)], {})
    assert rc._sl_tp_poll_once() == 0 and rc.bus.published == []


# ── riavvio (F018) ───────────────────────────────────────────────────────────────────────────────────────────────────

class _OrdersDB:
    def __init__(self, rows):
        self.rows = rows

    def _execute(self, sql, *a, **k):
        return [dict(r) for r in self.rows]


class _Exchange:
    def __init__(self, current):
        self.current = current

    def list_current_orders(self, market_ids=None):
        return list(self.current)


def test_sl_tp_survive_the_restart_rebuild():
    payload = {"market_id": "1.1", "selection_id": 7, "event_key": "K1", "table_id": 3, "bet_type": "BACK",
               "stake": 3.0, "price": 2.0, "event_name": "A v B", "stop_loss_pct": 20.0, "take_profit_pct": 50.0}
    row = {"customer_ref": "r1", "status": "SUBMITTED", "payload_json": json.dumps(payload),
           "response_json": json.dumps({"instructionReports": [{"betId": "B1", "status": "SUCCESS"}]})}
    rc = RuntimeController.__new__(RuntimeController)
    rc.db, rc.betfair_service = _OrdersDB([row]), _Exchange([{"betId": "B1", "marketId": "1.1"}])
    rc.table_manager, rc.duplication_guard = TableManager(table_count=5), DuplicationGuard()
    assert rc._rebuild_open_positions()["rebuilt"] == ["r1"]
    meta = rc.table_manager.get_table(3).meta
    assert meta["stop_loss"] == 0.6 and meta["take_profit"] == 1.5 and meta["stake"] == 3.0, meta


def test_gui_save_without_trailing_field_keeps_the_saved_trailing(svc):
    # il trailing non ha ancora un campo GUI (F-03): un salvataggio della tab non lo spegne in silenzio
    svc.save_settings({**REQUIRED, "roserpina.trailing_stop_pct": 15.0})
    cfg = svc.load_roserpina_config()
    cfg.trailing_stop_pct = None
    svc.save_roserpina_config(cfg)
    assert svc.load_roserpina_config().trailing_stop_pct == 15.0


def test_gui_roserpina_tab_round_trips_sl_tp():
    # la tab costruisce RoserpinaConfig da zero: senza campi SL/TP ogni salvataggio dalla GUI spegnerebbe lo SL
    import inspect
    import mini_gui
    src = inspect.getsource(mini_gui)
    for name in ("stop_loss_pct", "take_profit_pct"):
        assert f"{name}=self._parse_hard_stop(self.rs_{name}_var.get()" in src, name
        assert src.count(f'self.rs_{name}_var.set(self._hard_stop_to_str(getattr(rs, "{name}", None)))') == 2, name


def test_trigger_called_twice_publishes_once():
    # la guardia sta nel trigger stesso, non solo nel monitor: qualunque chiamante (stream, poller, test) non ripete
    table = _table(sl=1.0)
    rc = _monitor(table)
    rc._trigger_auto_close_table(table, "STOP_LOSS_REACHED", -2.0)
    rc._trigger_auto_close_table(table, "STOP_LOSS_REACHED", -3.0)
    assert [t for t, _ in rc.bus.published] == ["SL_TRIGGERED"]
