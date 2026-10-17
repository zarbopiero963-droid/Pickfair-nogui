"""PF-ALL-F001 / F023 / F024 — test unitari mirati (codice reale, nessun mock dei componenti sotto test).

Coprono i rami che la suite B-RED raggiunge solo indirettamente:
- TableManager.force_unlock_if_event: associazione all'ordine, idempotenza, concorrenza;
- RuntimeController._release_if_terminal: FAILED rilascia, AMBIGUOUS trattiene (tavolo + chiave);
- TradingEngine._resolve_executor_future: task concluso allo scadere dell'attesa, attesa da
  worker dello stesso pool (rifiuto pre-invio), executor che restituisce un risultato concreto.
"""
import concurrent.futures
import threading
import time
import types

import pytest

from core.duplication_guard import DuplicationGuard
from core.table_manager import TableManager
from core.runtime_controller import RuntimeController
from core.trading_engine import TradingEngine, _ErrorePrimaDellInvio, ExecutionError
from core.trading_constants import ERROR_AMBIGUOUS, AMBIGUITY_SUBMIT_TIMEOUT
from executor_manager import ExecutorManager


def _active(tm, table_id=1, key="EV-1"):
    tm.activate(table_id=table_id, event_key=key, exposure=30.0, market_id="1.1", selection_id=7)


# ------------------------------------------------------------------ F023: TableManager
def test_force_unlock_if_event_releases_only_matching_reservation():
    tm = TableManager(table_count=2)
    _active(tm, 1, "EV-1")
    assert tm.force_unlock_if_event(1, "EV-OTHER") is False
    assert tm.total_exposure() == 30.0
    assert tm.force_unlock_if_event(1, "EV-1") is True
    assert tm.total_exposure() == 0.0


def test_force_unlock_if_event_is_idempotent_and_never_frees_reassigned_table():
    tm = TableManager(table_count=1)
    _active(tm, 1, "EV-OLD")
    assert tm.force_unlock_if_event(1, "EV-OLD") is True
    _active(tm, 1, "EV-NEW")                       # tavolo riassegnato a un altro ordine
    assert tm.force_unlock_if_event(1, "EV-OLD") is False   # evento terminale vecchio, duplicato
    assert tm.total_exposure() == 30.0
    assert tm.force_unlock_if_event(1, "") is False


def test_force_unlock_if_event_concurrent_terminal_events_release_once():
    tm = TableManager(table_count=1)
    _active(tm, 1, "EV-1")
    results, bar = [], threading.Barrier(8)

    def go():
        bar.wait(); results.append(tm.force_unlock_if_event(1, "EV-1"))
    ths = [threading.Thread(target=go) for _ in range(8)]
    [t.start() for t in ths]; [t.join() for t in ths]
    assert results.count(True) == 1 and tm.total_exposure() == 0.0


# ------------------------------------------------------------------ F023/F024: RuntimeController
def _runtime_stub():
    """RuntimeController reale senza __init__ completo: solo le dipendenze del metodo testato."""
    rc = RuntimeController.__new__(RuntimeController)
    rc.table_manager = TableManager(table_count=2)
    rc.duplication_guard = DuplicationGuard()
    return rc


def test_failed_releases_table_and_dedupe_key_of_that_order():
    rc = _runtime_stub()
    assert rc.duplication_guard.acquire("EV-1")
    _active(rc.table_manager, 1, "EV-1")
    rc._release_if_terminal({"event_key": "EV-1", "table_id": 1, "customer_ref": "r"}, event_name="QUICK_BET_FAILED")
    assert rc.table_manager.total_exposure() == 0.0
    assert rc.duplication_guard.acquire("EV-1"), "chiave rilasciata dopo un rifiuto confermato"


def test_ambiguous_keeps_table_exposure_and_dedupe_key():
    rc = _runtime_stub()
    assert rc.duplication_guard.acquire("EV-1")
    _active(rc.table_manager, 1, "EV-1")
    rc._release_if_terminal({"event_key": "EV-1", "table_id": 1, "customer_ref": "r"}, event_name="QUICK_BET_AMBIGUOUS")
    assert rc.table_manager.total_exposure() == 30.0, "F024: riserva liberata su esito incerto"
    assert not rc.duplication_guard.acquire("EV-1"), "F024: chiave duplicati liberata su esito incerto"


def test_failed_for_other_order_does_not_free_reassigned_table():
    rc = _runtime_stub()
    _active(rc.table_manager, 1, "EV-NEW")
    rc._release_if_terminal({"event_key": "EV-OLD", "table_id": 1}, event_name="QUICK_BET_FAILED")
    assert rc.table_manager.total_exposure() == 30.0


# ------------------------------------------------------------------ F001: attesa del Future
def _engine_with(executor):
    eng = TradingEngine.__new__(TradingEngine)
    eng.executor = executor
    return eng


def test_task_finishing_at_wait_expiry_returns_its_real_result():
    f = concurrent.futures.Future()
    f.set_running_or_notify_cancel()
    eng = _engine_with(types.SimpleNamespace(default_timeout=0.2))

    def finish():
        time.sleep(0.15); f.set_result({"status": "SUCCESS", "betId": "B1"})
    threading.Thread(target=finish).start()
    assert eng._resolve_executor_future(f)["betId"] == "B1"


def test_started_task_past_wait_is_ambiguous_not_failed():
    f = concurrent.futures.Future()
    f.set_running_or_notify_cancel()               # partito: cancel() impossibile
    eng = _engine_with(types.SimpleNamespace(default_timeout=0.05))
    with pytest.raises(ExecutionError) as ei:
        eng._resolve_executor_future(f)
    assert ei.value.error_type == ERROR_AMBIGUOUS and ei.value.ambiguity_reason == AMBIGUITY_SUBMIT_TIMEOUT


def test_pending_task_past_wait_is_cancelled_and_pre_send():
    f = concurrent.futures.Future()                # mai avviato
    eng = _engine_with(types.SimpleNamespace(default_timeout=0.05))
    with pytest.raises(_ErrorePrimaDellInvio) as ei:
        eng._resolve_executor_future(f)
    assert f.cancelled() and "timeout" not in str(ei.value).lower()


def test_client_timeout_error_raised_by_task_propagates_unchanged():
    f = concurrent.futures.Future()
    f.set_running_or_notify_cancel(); f.set_exception(TimeoutError("response lost after send"))
    eng = _engine_with(types.SimpleNamespace(default_timeout=1.0))
    with pytest.raises(TimeoutError, match="response lost"):
        eng._resolve_executor_future(f)


@pytest.mark.parametrize("raw", [None, 0, -1, "x", float("nan"), float("inf")])
def test_invalid_executor_timeout_falls_back_to_30s(raw):
    assert _engine_with(types.SimpleNamespace(default_timeout=raw))._executor_wait_seconds() == 30.0


def test_engine_on_same_pool_worker_refuses_before_send():
    ex = ExecutorManager(max_workers=1, default_timeout=1)
    eng = _engine_with(ex)
    called = []
    try:
        fut = ex.submit("outer", lambda: eng._execute_via_executor("quick_bet", lambda: called.append(1)))
        with pytest.raises(_ErrorePrimaDellInvio, match="REENTRANT"):
            fut.result(timeout=5)
        assert called == []
    finally:
        ex.shutdown(wait=True)


def test_synchronous_executor_result_is_returned_unchanged():
    class Sync:
        def submit(self, _name, fn):
            return fn()
    assert _engine_with(Sync())._execute_via_executor("quick_bet", lambda: {"ok": True, "bet_id": "B"}) == {"ok": True, "bet_id": "B"}


def test_no_executor_runs_directly():
    assert _engine_with(None)._execute_via_executor("quick_bet", lambda: 42) == 42


# ------------------------------------------------------------------ F001: risposte malformate / report
def test_top_level_success_with_failed_report_is_a_confirmed_rejection():
    eng = _engine_with(None)
    resp = {"status": "SUCCESS", "instructionReports": [{"status": "FAILURE", "orderStatus": "LAPSED", "betId": "X"}]}
    with pytest.raises(RuntimeError, match="DOWNSTREAM_REPORT_FAILURE"):
        eng._raise_if_failed_semantic_response(resp)


def test_mixed_reports_are_ambiguous():
    eng = _engine_with(None)
    resp = {"status": "SUCCESS", "instructionReports": [{"status": "SUCCESS", "betId": "A"}, {"status": "FAILURE"}]}
    with pytest.raises(ExecutionError) as ei:
        eng._raise_if_failed_semantic_response(resp)
    assert ei.value.error_type == ERROR_AMBIGUOUS


@pytest.mark.parametrize("resp", ["SUCCESS", ["SUCCESS"], 1, True, object()])
def test_non_dict_response_after_client_call_is_ambiguous(resp):
    with pytest.raises(ExecutionError) as ei:
        _engine_with(None)._raise_if_failed_semantic_response(resp)
    assert ei.value.error_type == ERROR_AMBIGUOUS


def test_nested_success_report_is_accepted():
    _engine_with(None)._raise_if_failed_semantic_response(
        {"ok": True, "result": {"status": "SUCCESS", "instructionReports": [{"status": "SUCCESS", "betId": "B"}]}})


# ------------------------------------------------------------------ F024: reset (auto-reset drawdown / reset_cycle)
def test_reset_keeps_ambiguous_reservation_and_frees_the_rest():
    rc = _runtime_stub()
    for key, tid in (("EV-AMB", 1), ("EV-OK", 2)):
        assert rc.duplication_guard.acquire(key)
        _active(rc.table_manager, tid, key)
    rc._release_if_terminal({"event_key": "EV-AMB", "table_id": 1}, event_name="QUICK_BET_AMBIGUOUS")
    # PF-ALL-F026: il reset libera solo posizioni con settlement confermato; il tavolo 2 ("EV-OK") e' regolato.
    rc.table_manager.activate(table_id=2, event_key="EV-OK", exposure=30.0, market_id="1.2", selection_id=7)
    rc._mark_market_settled("1.2")
    rc._reset_reservations_keeping_uncertain()
    tabs = {t["table_id"]: t for t in rc.table_manager.snapshot()}
    assert tabs[1]["current_exposure"] == 30.0 and tabs[1]["current_event_key"] == "EV-AMB"
    assert tabs[2]["status"] == "FREE"
    assert not rc.duplication_guard.acquire("EV-AMB"), "F024: chiave dell'ordine incerto liberata dal reset"
    assert rc.duplication_guard.acquire("EV-OK")


def test_later_certain_outcome_clears_uncertain_mark():
    rc = _runtime_stub()
    _active(rc.table_manager, 1, "EV-1")
    rc._release_if_terminal({"event_key": "EV-1", "table_id": 1}, event_name="QUICK_BET_AMBIGUOUS")
    rc._release_if_terminal({"event_key": "EV-1", "table_id": 1}, event_name="QUICK_BET_FAILED")
    assert rc._uncertain_reservations_map() == {}
    assert rc.table_manager.total_exposure() == 0.0


def test_reset_frees_settled_positions_without_uncertain_reservations():
    rc = _runtime_stub()
    for key, tid in (("EV-A", 1), ("EV-B", 2)):
        rc.duplication_guard.acquire(key); _active(rc.table_manager, tid, key)
    rc._mark_market_settled("1.1")              # PF-ALL-F026: solo i regolati sono liberabili dal reset
    rc._reset_reservations_keeping_uncertain()
    assert rc.table_manager.total_exposure() == 0.0
    assert rc.duplication_guard.acquire("EV-A") and rc.duplication_guard.acquire("EV-B")


# ------------------------------------------------------------------ F001: executor sincrono (submit esegue fn)
def test_sync_executor_client_error_after_send_is_not_reclassified_pre_send():
    """Un executor sincrono esegue fn dentro submit(): un errore del client DOPO l'invio deve
    propagarsi invariato (TimeoutError => AMBIGUOUS a valle), mai come 'nulla inviato'."""
    class Sync:
        def submit(self, _name, fn):
            return fn()

    def client_lost_response():
        raise TimeoutError("response lost after send")
    with pytest.raises(TimeoutError, match="response lost"):
        _engine_with(Sync())._execute_via_executor("quick_bet", client_lost_response)


def test_executor_refusing_before_running_fn_is_pre_send():
    class Closed:
        def submit(self, _name, fn):
            raise RuntimeError("ExecutorManager già chiuso")
    called = []
    with pytest.raises(_ErrorePrimaDellInvio, match="EXECUTOR_SUBMIT_REFUSED"):
        _engine_with(Closed())._execute_via_executor("quick_bet", lambda: called.append(1))
    assert called == []
