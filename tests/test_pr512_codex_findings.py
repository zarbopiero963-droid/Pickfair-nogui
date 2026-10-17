"""PR #512 — rilievi Codex (commit 8ffb4f3) confermati con prova e corretti. Ogni test era RED prima della patch.

- 4303297576: blocco pre-invio del Microstake lasciava l'intento dedupe consumato (retry = DUPLICATE_BLOCKED);
- 4303297571: riserva transitoria da 2 EUR mai rilasciata dopo ACTIVE;
- 4303297596: snapshot del broker SIM non persistito dopo lo stub;
- 4303297564: righe pendenti senza attribuzione lasciavano ok=True (ACTIVE con esposizione non riservata);
- 4303297582: chiavi dedupe dei tavoli gia' occupati non riacquisite al riavvio nello stesso processo;
- 4303297575: betId annidato (BetfairClient) non letto nella ricostruzione F018;
- 4303297609: tavolo surplus con perdita perso al riavvio (RSR-12);
- 4303297600: write failure dello stato di recovery ignorata (RSR-12).
"""
import json
import os
import tempfile
from types import SimpleNamespace

import pytest

from core.duplication_guard import DuplicationGuard
from core.runtime_controller import RuntimeController
from core.table_manager import TableManager
from core.trading_engine import TradingEngine
from database import Database
from simulation_broker import SimulationBroker
from test_microstake_engine_native import _Bus, _Inline, _engine, _orders, _payload, _runtime
from test_microstake_sim_port import _Db as _SagaDb


def _engine_real_db(**rt):
    db = Database(os.path.join(tempfile.mkdtemp(prefix="pr512_"), "p.db"))
    broker = SimulationBroker(starting_balance=100.0)
    engine = TradingEngine(bus=_Bus(), db=db, client_getter=lambda: broker, executor=_Inline())
    engine.simulation_broker = broker
    engine.runtime_controller = _runtime(broker, **rt)
    return engine, broker, db


# ---------------------------------------------------------------- 4303297576: intento rilasciato su blocco pre-invio
def test_pre_send_microstake_block_releases_the_durable_intent_so_a_later_retry_is_not_a_duplicate():
    engine, broker, _ = _engine_real_db(available=1.0, bankroll=0.0)
    first = engine.submit_quick_bet(_payload("pf", size=0.10, price=2.0))
    assert first["status"] == "FAILED" and "transient_exposure_cap" in str(first["error"]) and _orders(broker) == []
    engine.runtime_controller.betfair_service.get_account_funds = lambda: {"available": 100.0, "ok": True}
    retry = engine.submit_quick_bet(_payload("pf", size=0.10, price=2.0))
    assert retry["status"] != "DUPLICATE_BLOCKED", retry
    assert retry["ok"] is True and _orders(broker)[0]["sizeRemaining"] == pytest.approx(0.10)


def test_ambiguous_outcome_still_keeps_the_intent_blocked():
    engine, broker, _ = _engine_real_db()
    engine._default_native_microstake_executor("SIMULATION")
    engine.microstake_executor.port.inject_fault("LOST_CANCEL_RESPONSE")
    assert engine.submit_quick_bet(_payload("amb", size=0.10))["status"] == "AMBIGUOUS"
    assert engine.submit_quick_bet(_payload("amb", size=0.10))["status"] == "DUPLICATE_BLOCKED"
    assert [op[0] for op in engine.microstake_executor.port.operation_log].count("PLACE") == 1


def test_post_exchange_failure_is_never_marked_pre_send():
    engine, broker, _ = _engine()
    engine._default_native_microstake_executor("SIMULATION")
    port = engine.microstake_executor.port
    original = port.broker.replace_orders
    port.broker.replace_orders = lambda **kw: {"status": "SUCCESS", "instructionReports": [{"status": "FAILURE", "betId": kw["bet_id"]}]}
    from test_microstake_engine_native import _book
    _book(broker, "1.100", 7, back=(3.0, 50.0))                               # target incrocia: serve il replace
    res = engine.microstake_executor.execute(_payload("pe", size=0.10, price=3.0))
    port.broker.replace_orders = original
    assert res["status"] == "FAILURE" and res["pre_send"] is False, res         # rollback dopo PLACE: non pre-invio


# ---------------------------------------------------------------- 4303297571: riserva transitoria rilasciata ad ACTIVE
def test_transient_reservation_is_released_after_activation_so_the_next_microstake_fits_the_same_funds():
    engine, broker, _ = _engine(available=2.0, bankroll=0.0)
    assert engine.submit_quick_bet(_payload("a", size=0.10, price=3.0))["ok"] is True
    assert engine.microstake_executor.port._reservations == {}, "la riserva dello stub resta dopo ACTIVE"
    assert engine.submit_quick_bet(_payload("b", size=0.10, price=3.0))["ok"] is True
    assert len(_orders(broker)) == 2


def test_reservation_is_kept_only_while_the_outcome_is_ambiguous():
    engine, broker, _ = _engine(available=2.0, bankroll=0.0)
    engine._default_native_microstake_executor("SIMULATION")
    engine.microstake_executor.port.inject_fault("LOST_CANCEL_RESPONSE")
    assert engine.submit_quick_bet(_payload("amb", size=0.10, price=3.0))["status"] == "AMBIGUOUS"
    assert list(engine.microstake_executor.port._reservations) == ["amb"]
    assert engine.submit_quick_bet(_payload("next", size=0.10, price=3.0))["ok"] is False   # 2.00 riservati: cap pieno


# ---------------------------------------------------------------- 4303297596: snapshot del broker dopo ogni operazione
def test_sim_broker_snapshot_is_persisted_after_each_saga_operation():
    from decimal import Decimal as D
    from core.microstake_safe import Side
    from core.microstake_sim_port import SimulationExchangePort
    db = _SagaDb()
    broker = SimulationBroker(starting_balance=100.0, db=db)
    port = SimulationExchangePort(broker)
    o = port.place(market_id="1.1", selection_id=7, side=Side.BACK, size=D("2.00"), price=D("3.00"), customer_ref="s1")
    assert len(db.states) == 1 and o.bet_id in db.states[-1][1]["orders"], "stub non nello snapshot di reload"
    port.cancel_reduction(o.bet_id, D("1.90"))
    assert len(db.states) == 2 and db.states[-1][1]["orders"][o.bet_id]["size"] == pytest.approx(0.10)
    assert broker.persist_snapshot() is True
    assert SimulationBroker(starting_balance=1.0).persist_snapshot() is False  # senza db: dichiarato, non silenzioso


# ---------------------------------------------------------------- F018: righe non attribuite, tavoli occupati, betId annidato
class _Rows:
    def __init__(self, rows):
        self.rows = rows

    def _execute(self, sql, *a, **k):
        return [dict(r) for r in self.rows]


class _Exchange:
    def __init__(self, current):
        self.current = current

    def list_current_orders(self, market_ids=None):
        return list(self.current)


def _row(ref, status, market, key, bet_id=None, nested=False, table_id=1):
    if bet_id and nested:
        resp = {"ok": True, "result": {"status": "SUCCESS", "instructionReports": [{"betId": bet_id, "status": "SUCCESS"}]}}
    elif bet_id:
        resp = {"instructionReports": [{"betId": bet_id, "status": "SUCCESS"}]}
    else:
        resp = {}
    payload = {"selection_id": 11, "table_id": table_id, "bet_type": "BACK", "stake": 3.0, "price": 2.0, "event_name": "A v B"}
    if market:
        payload["market_id"] = market
    if key:
        payload["event_key"] = key
    return {"customer_ref": ref, "status": status, "response_json": json.dumps(resp), "payload_json": json.dumps(payload)}


def _rc(rows, current, tables=3):
    rc = RuntimeController.__new__(RuntimeController)
    rc.db = _Rows(rows)
    rc.betfair_service = _Exchange(current)
    rc.table_manager = TableManager(table_count=tables)
    rc.duplication_guard = DuplicationGuard()
    return rc


def test_unattributed_pending_rows_keep_the_startup_not_ok():
    rc = _rc([_row("legacy", "INFLIGHT", market="", key="")], current=[])
    out = rc._rebuild_open_positions()
    assert out["ok"] is False and out["reason"].startswith("UNATTRIBUTED_PENDING_ORDERS:1")
    assert out["unattributed"] == ["legacy"]


def test_already_held_tables_reacquire_their_dedupe_key_in_the_new_guard():
    rc = _rc([_row("r1", "SUBMITTED", "1.1", "K1", bet_id="B1")], current=[{"betId": "B1", "marketId": "1.1"}])
    rc.table_manager.activate(table_id=1, event_key="K1", exposure=3.0, market_id="1.1", selection_id=11)
    out = rc._rebuild_open_positions()
    assert out["ok"] is True and out["held"] == ["r1"] and out["rebuilt"] == []
    assert rc.duplication_guard.acquire("K1") is False, "K1 non riacquisita: un secondo tavolo accetterebbe lo stesso evento"
    assert sum(1 for t in rc.table_manager.snapshot() if t["current_event_key"] == "K1") == 1


def test_nested_betfair_client_bet_id_is_read_during_rebuild():
    rc = _rc([_row("live", "SUBMITTED", "1.1", "K1", bet_id="B-LIVE-1", nested=True)],
             current=[{"betId": "B-LIVE-1", "marketId": "1.1"}, {"betId": "ALTRO", "marketId": "1.1"}])
    out = rc._rebuild_open_positions()
    assert out["rebuilt"] == ["live"] and out["uncertain"] == [], out


# ---------------------------------------------------------------- RSR-12: tavoli surplus e write failure
def test_surplus_recovery_table_is_recreated_on_restore():
    tm = TableManager(table_count=2)
    tm.restore_recovery_state({"5": 3.0})
    t = {x["table_id"]: x for x in tm.snapshot()}
    assert 5 in t and t[5]["status"] == "RECOVERY" and t[5]["loss_amount"] == pytest.approx(3.0)
    assert tm.recovery_state() == {5: 3.0}
    tm.restore_recovery_state({"9": 0.0})
    assert 9 not in {x["table_id"] for x in tm.snapshot()}                     # nessun debito: nessun tavolo fantasma


def test_recovery_state_write_failure_is_fail_closed_until_a_write_succeeds(tmp_path):
    rc = RuntimeController.__new__(RuntimeController)
    rc.table_manager = TableManager(table_count=3)
    rc.config = SimpleNamespace(max_recovery_tables=2)
    rc.table_manager.activate(table_id=1, event_key="A", exposure=1.0, market_id="1.1", selection_id=1)
    rc.table_manager.release(1, pnl=-2.0)
    real = Database(str(tmp_path / "r.db"))
    rc.db = SimpleNamespace(save_settings=lambda data: (_ for _ in ()).throw(RuntimeError("disk")))
    assert rc._persist_recovery_tables() is False
    rc._recovery_persist_failed = True                                          # come fa il percorso del settlement
    assert rc._active_recovery_count() >= 2, "scrittura fallita: capacita' di recovery non puo' restare libera"
    rc.db = real
    assert rc._persist_recovery_tables() is True
    assert rc._active_recovery_count() == 1
