"""PR #512 — rilievi Codex sul head 034af59 (ciclo 8). Un test per rilievo, scritto PRIMA della patch
(RED sul codice precedente), codice reale del progetto, deterministico e offline.

1. `BetfairService._well_formed_current_orders`: riga senza `betId`/`marketId` => CURRENT_ORDERS_MALFORMED
   (la ricostruzione F018 la leggeva come "posizione chiusa").
2. `NativeSimMicrostakeExecutor`: snapshot finale del broker non persistito => saga AMBIGUOUS, mai ACTIVE/SUCCESS.
3. `DutchingController._leg_liquidity_shortfall`: liquidita' ignota in modalita' blocco => shortfall LIQUIDITY_UNKNOWN.
4. `MicrostakeCoordinator.recover`: ROLLBACK_PENDING => cancel_all_remaining -> ROLLED_BACK, mai replace.
5. `reset_cycle`/`_maybe_auto_reset`: persistenza fallita => `_recovery_persist_failed`, evento, `persisted: False`.
6. `start()`: stato di recovery illeggibile => PAUSED (RECOVERY_STATE_UNREADABLE) e nessuna sovrascrittura.
7. docstring di `core/microstake_safe.py` veritiero.
10. `RoserpinaMoneyManagement._desk_mode`: crescita ignota => NORMAL anche con soglia 0.
"""
import json
import os
import sys
import threading
from decimal import Decimal as D

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tests", "f026"))

from services.betfair_service import BetfairService  # noqa: E402


# ------------------------------------------------------------------ 1. currentOrders senza identita'
class _OrdersBroker:
    def __init__(self, orders):
        self._orders = orders

    def get_current_orders(self, market_ids=None):
        return self._orders


@pytest.mark.parametrize("row", [
    {"marketId": "1.1"},                       # senza betId
    {"betId": "B1"},                           # senza marketId
    {"betId": "", "marketId": "1.1"},          # betId vuoto
    {"betId": "B1", "marketId": "  "},         # marketId vuoto
    {"betId": None, "marketId": "1.1"},
])
def test_f1_current_order_row_without_identity_is_malformed(row):
    with pytest.raises(RuntimeError, match="CURRENT_ORDERS_MALFORMED"):
        BetfairService._well_formed_current_orders([{"betId": "B0", "marketId": "1.0"}, row])


def test_f1_rows_with_identity_pass_in_both_spellings():
    rows = [{"betId": "B1", "marketId": "1.1"}, {"bet_id": "B2", "market_id": "1.2"}]
    assert BetfairService._well_formed_current_orders(rows) == rows


def test_f1_sim_facade_raises_on_row_without_bet_id_instead_of_closing_positions():
    svc = BetfairService.__new__(BetfairService)
    svc.simulation_mode = True
    svc.get_client = lambda: _OrdersBroker([{"marketId": "1.1"}])
    with pytest.raises(RuntimeError, match="CURRENT_ORDERS_MALFORMED"):
        svc.list_current_orders()


def test_f1_rebuild_goes_paused_on_row_without_bet_id():
    """Il percorso reale: F018 con una riga senza betId deve dare EXCHANGE_ORDERS_UNAVAILABLE, non 'closed'."""
    from test_f018_rebuild_units import _Exchange, _rc, _row

    class _Facade(_Exchange):
        def list_current_orders(self, market_ids=None):
            self.calls += 1
            return [BetfairService._normalize_startup_order(o)
                    for o in BetfairService._well_formed_current_orders(list(self.current))]

    rc = _rc([_row("r1", "SUBMITTED", "1.1", 1, "K1", bet_id="B1")], _Facade([{"marketId": "1.1"}]))
    res = rc._rebuild_open_positions()
    assert res["ok"] is False and res["reason"].startswith("EXCHANGE_ORDERS_UNAVAILABLE"), res
    assert res["closed"] == []


# ------------------------------------------------------------------ 2. snapshot finale non persistito
def _native_engine():
    sys.path.insert(0, os.path.join(ROOT, "tests"))
    from test_microstake_engine_native import _engine, _payload
    engine, broker, _ = _engine()
    engine._default_native_microstake_executor("SIMULATION")
    return engine, broker, _payload


def test_f2_failed_final_snapshot_leaves_the_saga_ambiguous_with_reservation_held():
    engine, broker, payload = _native_engine()
    ex = engine.microstake_executor
    broker.persist_snapshot = lambda: False
    res = ex.execute(payload("snap", size=0.10, price=3.0))
    assert res["status"] == "AMBIGUOUS" and res["order_unknown"] is True, res
    assert res["phase"] == "AMBIGUOUS" and res["reservation_held"] is True and res["snapshot_persisted"] is False
    assert "instructionReports" not in res
    saga = ex.store.load("snap")
    assert saga["phase"] == "AMBIGUOUS" and saga["reservation_held"] is True, saga
    assert "snap" in ex.store.pending_refs(), "la saga deve restare da riconciliare al riavvio"
    assert ex.port.has_reservation("snap")


def test_f2_failed_final_snapshot_is_reconciled_by_recover_without_a_new_place():
    engine, broker, payload = _native_engine()
    ex = engine.microstake_executor
    broker.persist_snapshot = lambda: False
    ex.execute(payload("snap-r", size=0.10, price=3.0))
    broker.persist_snapshot = lambda: True
    out = ex.recover_pending()
    assert out == {"snap-r": "ACTIVE"}, out
    assert [op[0] for op in ex.port.operation_log] == ["PLACE", "CANCEL"]
    assert ex.store.load("snap-r")["reservation_held"] is False and not ex.port.has_reservation("snap-r")


def test_f2_intermediate_snapshot_failure_recovered_by_the_final_one_stays_active():
    engine, broker, payload = _native_engine()
    calls = []
    broker.persist_snapshot = lambda: (calls.append(1), len(calls) > 1)[1]   # fallisce solo dopo il PLACE
    res = engine.microstake_executor.execute(payload("snap-ok", size=0.10, price=3.0))
    assert res["status"] == "SUCCESS" and res["phase"] == "ACTIVE", res
    assert res["snapshot_persisted"] is False and engine.microstake_executor.port.snapshot_failures == 1


def test_f2_recover_pending_does_not_promote_while_the_snapshot_still_fails():
    engine, broker, payload = _native_engine()
    ex = engine.microstake_executor
    broker.persist_snapshot = lambda: False
    ex.execute(payload("snap-x", size=0.10, price=3.0))
    ops = list(ex.port.operation_log)
    assert ex.recover_pending() == {"snap-x": "AMBIGUOUS"}
    assert ex.store.load("snap-x")["phase"] == "AMBIGUOUS" and ex.port.operation_log == ops


# ------------------------------------------------------------------ 3. liquidita' ignota in modalita' blocco
def _dutching_leg_shortfall(ladder, *, warning_only):
    from controllers.dutching_controller import DutchingController
    book = {"runners": [{"selectionId": 11, "ex": {"availableToBack": ladder}}]}
    item = {"selectionId": 11, "stake": 10.0, "side": "BACK", "price": 2.0}
    return DutchingController._leg_liquidity_shortfall(book, item, 1.0, 1.0, warning_only=warning_only)


@pytest.mark.parametrize("ladder", [{"price": 2.0, "size": 50}, "abc", 7])
def test_f3_unknown_liquidity_in_block_mode_is_a_shortfall(ladder):
    sf = _dutching_leg_shortfall(ladder, warning_only=False)
    assert sf is not None and sf["reason"] == "LIQUIDITY_UNKNOWN" and sf["available"] is None, sf
    assert sf["selectionId"] == 11 and sf["required"] == 10.0


def test_f3_selection_missing_from_the_book_is_unknown_in_block_mode():
    from controllers.dutching_controller import DutchingController
    book = {"runners": [{"selectionId": 99, "ex": {"availableToBack": [{"price": 2.0, "size": 50}]}}]}
    item = {"selectionId": 11, "stake": 10.0, "side": "BACK", "price": 2.0}
    sf = DutchingController._leg_liquidity_shortfall(book, item, 1.0, 1.0, warning_only=False)
    assert sf is not None and sf["reason"] == "LIQUIDITY_UNKNOWN"


@pytest.mark.parametrize("ladder", [{"price": 2.0, "size": 50}, "abc", 7])
def test_f3_unknown_liquidity_in_warning_only_mode_stays_unobserved(ladder):
    assert _dutching_leg_shortfall(ladder, warning_only=True) is None


def test_f3_known_sufficient_liquidity_is_never_a_shortfall_in_block_mode():
    assert _dutching_leg_shortfall([{"price": 2.0, "size": 50.0}], warning_only=False) is None


def test_f3_evaluate_guard_blocks_on_unknown_liquidity_only_when_armed():
    from types import SimpleNamespace
    from controllers.dutching_controller import DutchingController
    book = {"runners": [{"selectionId": 11, "ex": {"availableToBack": "rotto"}}]}
    ctl = DutchingController.__new__(DutchingController)
    ctl._market_book = lambda market_id: book
    results = [{"selectionId": 11, "stake": 10.0, "side": "BACK", "price": 2.0}]
    armed = ctl._evaluate_liquidity_guard(SimpleNamespace(liquidity_warning_only=False), {"market_id": "1.1"}, results)
    assert armed["warning"] is True and armed["shortfall"][0]["reason"] == "LIQUIDITY_UNKNOWN"
    observe = ctl._evaluate_liquidity_guard(SimpleNamespace(liquidity_warning_only=True), {"market_id": "1.1"}, results)
    assert observe == {"warning": False, "shortfall": []}
