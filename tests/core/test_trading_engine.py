from __future__ import annotations

import sys
import threading
import time
import types
from typing import Any, Dict, List, Optional

import pytest

# ---------------------------------------------------------------------
# Shim per import "from order_manager import OrderManager"
# ---------------------------------------------------------------------
if "order_manager" not in sys.modules:
    mod = types.ModuleType("order_manager")
    mod.OrderManager = object
    sys.modules["order_manager"] = mod

from core.trading_engine import (
    TradingEngine,
    ExecutionError,
    REQ_QUICK_BET,
    CMD_QUICK_BET,
    STATUS_INFLIGHT,
    STATUS_SUBMITTED,
    STATUS_COMPLETED,
    STATUS_FAILED,
    STATUS_AMBIGUOUS,
    STATUS_DENIED,
    STATUS_ACCEPTED_FOR_PROCESSING,
    STATUS_DUPLICATE_BLOCKED,
    OUTCOME_SUCCESS,
    OUTCOME_FAILURE,
    OUTCOME_AMBIGUOUS,
    READY,
    DEGRADED,
    NOT_READY,
    AMBIGUITY_SUBMIT_TIMEOUT,
    AMBIGUITY_SUBMIT_UNKNOWN,
    ORIGIN_NORMAL,
    ORIGIN_COPY,
    ORIGIN_PATTERN,
)


class FakeBus:
    def __init__(self) -> None:
        self.subscriptions: List[tuple[str, Any]] = []
        self.published: List[tuple[str, Dict[str, Any]]] = []

    def subscribe(self, topic: str, handler: Any) -> None:
        self.subscriptions.append((topic, handler))

    def publish(self, event_name: str, payload: Dict[str, Any]) -> None:
        self.published.append((event_name, payload))


class FakeDB:
    def __init__(self) -> None:
        self.orders: Dict[str, Dict[str, Any]] = {}
        self.audit_events: List[Dict[str, Any]] = []
        self.next_id = 1

        self.fail_insert_order = False
        self.fail_update_order = False
        self.fail_get_order = False
        self.fail_insert_audit_event = False
        self.fail_find_duplicate_order = False
        self.force_order_exists_inflight = False

    def is_ready(self) -> bool:
        return True

    def insert_order(self, payload: Dict[str, Any]) -> str:
        if self.fail_insert_order:
            raise RuntimeError("DB_INSERT_ORDER_FAILED")
        order_id = str(self.next_id)
        self.next_id += 1
        self.orders[order_id] = dict(payload)
        return order_id

    def update_order(self, order_id: str, update: Dict[str, Any]) -> None:
        if self.fail_update_order:
            raise RuntimeError("DB_UPDATE_ORDER_FAILED")
        if order_id not in self.orders:
            raise KeyError(order_id)
        self.orders[order_id].update(dict(update))

    def get_order(self, order_id: str) -> Dict[str, Any]:
        if self.fail_get_order:
            raise RuntimeError("DB_GET_ORDER_FAILED")
        return dict(self.orders[order_id])

    def insert_audit_event(self, event: Dict[str, Any]) -> None:
        if self.fail_insert_audit_event:
            raise RuntimeError("DB_INSERT_AUDIT_FAILED")
        self.audit_events.append(dict(event))

    def order_exists_inflight(
        self,
        customer_ref: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ) -> bool:
        if self.force_order_exists_inflight:
            return True
        for row in self.orders.values():
            if row.get("customer_ref") == customer_ref:
                return True
            if row.get("correlation_id") == correlation_id:
                return True
        return False

    def find_duplicate_order(
        self,
        customer_ref: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ) -> Optional[str]:
        if self.fail_find_duplicate_order:
            raise RuntimeError("DB_FIND_DUPLICATE_FAILED")
        return "DUP-REF-1"

    def load_pending_customer_refs(self) -> List[str]:
        refs = []
        for row in self.orders.values():
            if row.get("status") == STATUS_INFLIGHT:
                refs.append(str(row.get("customer_ref")))
        return refs

    def load_pending_correlation_ids(self) -> List[str]:
        cids = []
        for row in self.orders.values():
            if row.get("status") == STATUS_INFLIGHT:
                cids.append(str(row.get("correlation_id")))
        return cids


class FakeAsyncDbWriter:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.events: List[Dict[str, Any]] = []

    def is_ready(self) -> bool:
        return True

    def write(self, event: Dict[str, Any]) -> None:
        if self.fail:
            raise RuntimeError("ASYNC_DB_WRITE_FAILED")
        self.events.append(dict(event))


class FakeClient:
    def __init__(self, *, response: Any = None, error: Exception | None = None) -> None:
        self.response = {"bet_id": "BET-1"} if response is None else response
        self.error = error
        self.calls: List[Dict[str, Any]] = []

    def place_bet(self, **payload: Any) -> Any:
        self.calls.append(dict(payload))
        if self.error is not None:
            raise self.error
        return self.response


class FakeExecutor:
    def __init__(self, *, mode: str = "passthrough", response: Any = None) -> None:
        self.mode = mode
        self.response = response
        self.calls: List[tuple[str, Any]] = []

    def is_ready(self) -> bool:
        return True

    def submit(self, operation_name: str, fn: Any) -> Any:
        self.calls.append((operation_name, fn))
        if self.mode == "passthrough":
            return fn()
        if self.mode == "return_response":
            return self.response
        if self.mode == "raise":
            raise RuntimeError("EXECUTOR_FAILED")
        return fn()


class FakeSafeMode:
    def __init__(self, enabled: bool = False, ready: bool = True) -> None:
        self.enabled = enabled
        self.ready = ready

    def is_enabled(self) -> bool:
        return self.enabled

    def is_ready(self) -> bool:
        return self.ready


class FakeRiskMiddleware:
    def __init__(
        self,
        *,
        allowed: bool = True,
        reason: Optional[str] = None,
        payload_mutation: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.allowed = allowed
        self.reason = reason
        self.payload_mutation = payload_mutation or {}

    def is_ready(self) -> bool:
        return True

    def check(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        merged = dict(payload)
        merged.update(self.payload_mutation)
        return {
            "allowed": self.allowed,
            "reason": self.reason,
            "payload": merged,
        }


class FakeReconciliationEngine:
    def __init__(self) -> None:
        self.enqueued: List[Dict[str, Any]] = []
        self.restart_calls = 0

    def is_ready(self) -> bool:
        return True

    def enqueue(self, **kwargs: Any) -> None:
        self.enqueued.append(dict(kwargs))

    def enqueue_pending(self) -> Dict[str, Any]:
        self.restart_calls += 1
        return {"triggered": True}


class FakeStateRecovery:
    def __init__(self, result: Any = None, error: Exception | None = None) -> None:
        self.result = {"ok": True, "reason": None} if result is None else result
        self.error = error
        self.calls = 0

    def is_ready(self) -> bool:
        return True

    def recover(self) -> Any:
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.result


class FakeAwaitable:
    def __await__(self):
        async def _inner():
            return {"bet_id": "BET-ASYNC"}
        return _inner().__await__()


def make_payload(**overrides: Any) -> Dict[str, Any]:
    payload = {
        "customer_ref": "CUST-1",
        "correlation_id": "CID-1",
        "event_key": "EVT-1",
        "simulation_mode": False,
    }
    payload.update(overrides)
    return payload


def make_engine(
    *,
    db: Optional[FakeDB] = None,
    bus: Optional[FakeBus] = None,
    client: Optional[FakeClient] = None,
    executor: Optional[Any] = None,
    safe_mode: Optional[Any] = None,
    risk_middleware: Optional[Any] = None,
    reconciliation_engine: Optional[Any] = None,
    state_recovery: Optional[Any] = None,
    async_db_writer: Optional[Any] = None,
):
    db = db or FakeDB()
    bus = bus or FakeBus()
    client = client or FakeClient()
    executor = executor if executor is not None else FakeExecutor(mode="passthrough")
    safe_mode = safe_mode or FakeSafeMode(enabled=False)
    risk_middleware = risk_middleware or FakeRiskMiddleware(allowed=True)
    reconciliation_engine = reconciliation_engine or FakeReconciliationEngine()
    state_recovery = state_recovery or FakeStateRecovery()
    async_db_writer = async_db_writer or FakeAsyncDbWriter()

    engine = TradingEngine(
        bus=bus,
        db=db,
        client_getter=lambda: client,
        executor=executor,
        safe_mode=safe_mode,
        risk_middleware=risk_middleware,
        reconciliation_engine=reconciliation_engine,
        state_recovery=state_recovery,
        async_db_writer=async_db_writer,
    )
    return engine, db, bus, client, executor, reconciliation_engine, state_recovery, async_db_writer


def test_start_sets_ready_when_required_dependencies_are_ready():
    engine, *_ = make_engine()
    readiness = engine.readiness()

    assert readiness["state"] in {READY, DEGRADED}
    assert readiness["health"]["db"]["state"] == READY
    assert readiness["health"]["client_getter"]["state"] == READY


def test_submit_success_returns_ack_not_terminal():
    engine, db, _, client, _, _, _, _ = make_engine()

    result = engine.submit_quick_bet(make_payload())

    assert result["ok"] is True
    assert result["status"] == STATUS_ACCEPTED_FOR_PROCESSING
    assert result["outcome"] == OUTCOME_SUCCESS
    assert result["is_terminal"] is False
    assert result["lifecycle_stage"] == "accepted"
    assert result["order_id"] is not None
    assert result["correlation_id"] == "CID-1"
    assert result["customer_ref"] == "CUST-1"
    assert result["event_key"] == "EVT-1"
    assert result["simulation_mode"] is False

    order = db.get_order(result["order_id"])
    assert order["status"] == STATUS_SUBMITTED
    assert "finalized" not in order
    assert len(client.calls) == 1


def test_ack_keeps_inflight_lock_until_terminal():
    engine, _, _, _, _, _, _, _ = make_engine()

    result = engine.submit_quick_bet(make_payload())

    assert result["status"] == STATUS_ACCEPTED_FOR_PROCESSING
    assert "CUST-1" in engine._inflight_keys


def test_safe_mode_denies_request_and_finalizes():
    engine, db, _, client, _, _, _, _ = make_engine(safe_mode=FakeSafeMode(enabled=True))

    result = engine.submit_quick_bet(make_payload())

    assert result["status"] == STATUS_DENIED
    assert result["outcome"] == OUTCOME_FAILURE
    assert result["is_terminal"] is True
    assert result["reason"] == "SAFE_MODE_ACTIVE"
    assert result["finalization_persisted"] is True
    assert len(client.calls) == 0
    assert result["order_id"] is None
    assert db.orders == {}


def test_risk_denied_persists_inflight_then_denied():
    risk = FakeRiskMiddleware(allowed=False, reason="RISK_BLOCK")
    engine, db, _, client, _, _, _, _ = make_engine(risk_middleware=risk)

    result = engine.submit_quick_bet(make_payload())

    assert result["status"] == STATUS_DENIED
    assert result["outcome"] == OUTCOME_FAILURE
    assert result["is_terminal"] is True
    assert result["reason"] == "RISK_BLOCK"
    assert result["order_id"] is not None
    assert len(client.calls) == 0

    order = db.get_order(result["order_id"])
    assert order["status"] == STATUS_DENIED
    assert order["risk_reason"] == "RISK_BLOCK"
    assert order["finalized"] is True


def test_duplicate_request_from_db_is_blocked_and_finalized():
    db = FakeDB()
    db.force_order_exists_inflight = True
    engine, _, bus, client, _, _, _, _ = make_engine(db=db)

    result = engine.submit_quick_bet(make_payload())

    assert result["status"] == STATUS_DUPLICATE_BLOCKED
    assert result["outcome"] == OUTCOME_SUCCESS
    assert result["is_terminal"] is True
    assert result["lifecycle_stage"] == "finalized"
    assert result["finalization_persisted"] is True
    assert len(client.calls) == 0

    order = db.get_order(result["order_id"])
    assert order["status"] == STATUS_DUPLICATE_BLOCKED
    assert order["duplicate_of"] == "DUP-REF-1"
    assert order["finalized"] is True

    assert any(name == "QUICK_BET_DUPLICATE" for name, _ in bus.published)


def test_duplicate_path_preserves_copy_meta_fields():
    db = FakeDB()
    db.force_order_exists_inflight = True
    engine, _, _, _, _, _, _, _ = make_engine(db=db)

    result = engine.submit_quick_bet(
        make_payload(
            copy_meta={
                "master_id": "M1",
                "master_position_id": "MP1",
                "action_id": "A1",
                "action_seq": 10,
                "copy_group_id": "CG1",
                "copy_mode": "FOLLOW",
                "ignored_key": "NOPE",
            }
        )
    )

    order = db.get_order(result["order_id"])
    assert order["order_origin"] == ORIGIN_COPY
    assert order["copy_meta"] == {
        "master_id": "M1",
        "master_position_id": "MP1",
        "action_id": "A1",
        "action_seq": 10,
        "copy_group_id": "CG1",
        "copy_mode": "FOLLOW",
    }


def test_invalid_request_preserves_pattern_origin_best_effort():
    engine, _, _, _, _, _, _, _ = make_engine()

    result = engine.submit_quick_bet(
        {
            "correlation_id": "CID-INVALID",
            "event_key": "EVT-X",
            "simulation_mode": True,
            "pattern_meta": {
                "pattern_id": "P1",
                "pattern_label": "Label",
                "selection_template": "Sel",
                "market_type": "OVER_UNDER",
                "bet_side": "BACK",
                "live_only": True,
                "event_context": {"league": "A"},
                "ignored": "DROP",
            },
        }
    )

    assert result["status"] == STATUS_FAILED
    assert result["outcome"] == OUTCOME_FAILURE
    assert result["order_id"] is None
    assert result["pattern_meta"] == {
        "pattern_id": "P1",
        "pattern_label": "Label",
        "selection_template": "Sel",
        "market_type": "OVER_UNDER",
        "bet_side": "BACK",
        "live_only": True,
        "event_context": {"league": "A"},
    }
    assert result["order_origin"] == ORIGIN_PATTERN
    assert result["simulation_mode"] is True
    assert result["event_key"] == "EVT-X"


def test_async_executor_not_normalized_goes_to_failed_terminal_or_degraded():
    executor = FakeExecutor(mode="return_response", response=FakeAwaitable())
    engine, _, _, _, _, _, _, _ = make_engine(executor=executor)

    result = engine.submit_quick_bet(make_payload())

    assert result["status"] == STATUS_FAILED
    assert result["outcome"] == OUTCOME_FAILURE
    assert "ASYNC_EXECUTOR_NOT_NORMALIZED" in (result["error"] or "")


def test_executor_returning_none_becomes_ambiguous():
    executor = FakeExecutor(mode="return_response", response=None)
    engine, db, _, _, _, rec, _, _ = make_engine(executor=executor)

    result = engine.submit_quick_bet(make_payload())

    assert result["status"] == STATUS_AMBIGUOUS
    assert result["outcome"] == OUTCOME_AMBIGUOUS
    assert result["is_terminal"] is True
    assert result["ambiguity_reason"] == AMBIGUITY_SUBMIT_UNKNOWN
    assert result["finalization_persisted"] is True

    order = db.get_order(result["order_id"])
    assert order["status"] == STATUS_AMBIGUOUS
    assert len(rec.enqueued) == 1
    assert rec.enqueued[0]["ambiguity_reason"] == AMBIGUITY_SUBMIT_UNKNOWN


def test_submit_timeout_becomes_ambiguous_with_timeout_reason():
    client = FakeClient(error=TimeoutError("network timeout"))
    engine, db, _, _, _, rec, _, _ = make_engine(client=client)

    result = engine.submit_quick_bet(make_payload())

    assert result["status"] == STATUS_AMBIGUOUS
    assert result["outcome"] == OUTCOME_AMBIGUOUS
    assert result["ambiguity_reason"] == AMBIGUITY_SUBMIT_TIMEOUT
    assert len(rec.enqueued) == 1

    order = db.get_order(result["order_id"])
    assert order["status"] == STATUS_AMBIGUOUS


def test_single_enqueue_on_ambiguity_only_once():
    executor = FakeExecutor(mode="return_response", response=None)
    engine, _, _, _, _, rec, _, _ = make_engine(executor=executor)

    result = engine.submit_quick_bet(make_payload())

    assert result["status"] == STATUS_AMBIGUOUS
    assert len(rec.enqueued) == 1


def test_transition_order_raises_on_db_state_mismatch():
    engine, db, _, _, _, _, _, _ = make_engine()

    order_id = db.insert_order(
        {
            "customer_ref": "CUST-X",
            "correlation_id": "CID-X",
            "status": STATUS_SUBMITTED,
            "payload": {},
            "created_at": time.time(),
            "outcome": None,
        }
    )

    ctx = engine._new_execution_context(
        {
            "customer_ref": "CUST-X",
            "correlation_id": "CID-X",
            "event_key": None,
            "simulation_mode": False,
        }
    )
    audit = engine._new_audit(ctx)

    with pytest.raises(RuntimeError, match="STATE_MISMATCH"):
        engine._transition_order(
            ctx,
            audit,
            order_id,
            STATUS_INFLIGHT,
            STATUS_FAILED,
            extra={"last_error": "boom"},
        )


def test_precheck_finalize_raises_on_non_terminal_db_state():
    engine, db, _, _, _, _, _, _ = make_engine()

    order_id = db.insert_order(
        {
            "customer_ref": "CUST-Z",
            "correlation_id": "CID-Z",
            "status": STATUS_INFLIGHT,
            "payload": {},
            "created_at": time.time(),
            "outcome": None,
        }
    )

    with pytest.raises(RuntimeError, match="FINALIZE_ON_NON_TERMINAL_DB_STATE"):
        engine._precheck_finalize(order_id)


def test_terminal_metadata_write_failure_returns_degraded_result():
    engine, db, _, _, _, _, _, _ = make_engine()

    order_id = db.insert_order(
        {
            "customer_ref": "CUST-T",
            "correlation_id": "CID-T",
            "status": STATUS_COMPLETED,
            "payload": {},
            "created_at": time.time(),
            "outcome": None,
        }
    )

    ctx = engine._new_execution_context(
        {
            "customer_ref": "CUST-T",
            "correlation_id": "CID-T",
            "event_key": "EVT-T",
            "simulation_mode": False,
        }
    )
    audit = engine._new_audit(ctx)

    original = engine._write_order_metadata

    def broken_write(order_id: Any, meta: Dict[str, Any]) -> None:
        raise RuntimeError("BROKEN_METADATA_WRITE")

    engine._write_order_metadata = broken_write
    try:
        result = engine._complete_order_lifecycle(
            ctx,
            audit,
            order_id=order_id,
            status=STATUS_COMPLETED,
            reason="DONE",
            extra_fields={},
        )
    finally:
        engine._write_order_metadata = original

    assert result["status"] == STATUS_COMPLETED
    assert result["outcome"] == OUTCOME_SUCCESS
    assert result["finalization_persisted"] is False
    assert result["is_terminal"] is False
    assert result["lifecycle_stage"] == "degraded"


def test_audit_db_failure_does_not_break_success_business_flow():
    db = FakeDB()
    db.fail_insert_audit_event = True
    engine, _, _, client, _, _, _, _ = make_engine(db=db)

    result = engine.submit_quick_bet(make_payload())

    assert result["status"] == STATUS_ACCEPTED_FOR_PROCESSING
    assert result["outcome"] == OUTCOME_SUCCESS
    assert result["is_terminal"] is False
    assert len(client.calls) == 1

    order = db.get_order(result["order_id"])
    assert order["status"] == STATUS_SUBMITTED


def test_audit_db_and_async_failures_still_do_not_break_success_flow():
    db = FakeDB()
    db.fail_insert_audit_event = True
    async_writer = FakeAsyncDbWriter(fail=True)

    engine, _, _, client, _, _, _, _ = make_engine(db=db, async_db_writer=async_writer)

    result = engine.submit_quick_bet(make_payload())

    assert result["status"] == STATUS_ACCEPTED_FOR_PROCESSING
    assert result["outcome"] == OUTCOME_SUCCESS
    assert len(client.calls) == 1


def test_fatal_path_returns_degraded_when_safe_mark_failed_cannot_transition():
    client = FakeClient(error=RuntimeError("CLIENT_FATAL"))
    engine, _, _, _, _, _, _, _ = make_engine(client=client)

    original = engine._safe_mark_failed

    def fake_safe_mark_failed(*args: Any, **kwargs: Any) -> bool:
        return False

    engine._safe_mark_failed = fake_safe_mark_failed
    try:
        result = engine.submit_quick_bet(make_payload())
    finally:
        engine._safe_mark_failed = original

    assert result["status"] == STATUS_FAILED
    assert result["outcome"] == OUTCOME_FAILURE
    assert result["is_terminal"] is False
    assert result["lifecycle_stage"] == "degraded"
    assert result["finalization_persisted"] is False
    assert result["reason"] == "ENGINE_FATAL_DB_UNAVAILABLE"


def test_copy_meta_passes_through_on_success_ack():
    engine, _, _, _, _, _, _, _ = make_engine()

    result = engine.submit_quick_bet(
        make_payload(
            copy_meta={
                "master_id": "M1",
                "master_position_id": "MP1",
                "action_id": "A1",
                "action_seq": 7,
                "copy_group_id": "CG1",
                "copy_mode": "SYNC",
                "ignored": "DROP",
            }
        )
    )

    assert result["status"] == STATUS_ACCEPTED_FOR_PROCESSING
    assert result["order_origin"] == ORIGIN_COPY
    assert result["copy_meta"] == {
        "master_id": "M1",
        "master_position_id": "MP1",
        "action_id": "A1",
        "action_seq": 7,
        "copy_group_id": "CG1",
        "copy_mode": "SYNC",
    }


def test_pattern_meta_passes_through_on_success_ack():
    engine, _, _, _, _, _, _, _ = make_engine()

    result = engine.submit_quick_bet(
        make_payload(
            pattern_meta={
                "pattern_id": "P1",
                "pattern_label": "LBL",
                "selection_template": "SEL",
                "market_type": "OU",
                "bet_side": "BACK",
                "live_only": True,
                "event_context": {"k": "v"},
                "ignored": "DROP",
            }
        )
    )

    assert result["status"] == STATUS_ACCEPTED_FOR_PROCESSING
    assert result["order_origin"] == ORIGIN_PATTERN
    assert result["pattern_meta"] == {
        "pattern_id": "P1",
        "pattern_label": "LBL",
        "selection_template": "SEL",
        "market_type": "OU",
        "bet_side": "BACK",
        "live_only": True,
        "event_context": {"k": "v"},
    }


def test_copy_and_pattern_mutually_exclusive_fails():
    engine, _, _, _, _, _, _, _ = make_engine()

    result = engine.submit_quick_bet(
        make_payload(
            copy_meta={"master_id": "M1"},
            pattern_meta={"pattern_id": "P1"},
        )
    )

    assert result["status"] == STATUS_FAILED
    assert result["reason"] == "INVALID_REQUEST"
    assert "COPY_AND_PATTERN_MUTUALLY_EXCLUSIVE" in (result["error"] or "")


def test_recover_after_restart_returns_normalized_shape_and_repopulates_ram():
    db = FakeDB()
    db.insert_order(
        {
            "customer_ref": "CUST-P",
            "correlation_id": "CID-P",
            "status": STATUS_INFLIGHT,
            "payload": {},
            "created_at": time.time(),
            "outcome": None,
        }
    )
    rec_engine = FakeReconciliationEngine()
    state_recovery = FakeStateRecovery(result={"ok": True, "reason": "RECOVERED"})
    engine, _, _, _, _, _, _, _ = make_engine(
        db=db,
        reconciliation_engine=rec_engine,
        state_recovery=state_recovery,
    )

    result = engine.recover_after_restart()

    assert result == {
        "ok": True,
        "status": "RECOVERY_TRIGGERED",
        "recovery": {"ok": True, "reason": "RECOVERED"},
        "reconcile": {"triggered": True},
        "ram_synced": True,
        "reason": "RECOVERED",
    }
    assert "CUST-P" in engine._inflight_keys
    assert "CID-P" in engine._seen_correlation_ids


def test_recover_after_restart_handles_recovery_exception():
    state_recovery = FakeStateRecovery(error=RuntimeError("RECOVERY_DOWN"))
    engine, _, _, _, _, _, _, _ = make_engine(state_recovery=state_recovery)

    result = engine.recover_after_restart()

    assert result["ok"] is False
    assert result["status"] == "RECOVERY_FAILED"
    assert "RECOVERY_EXCEPTION:RECOVERY_DOWN" in result["reason"]


def test_readiness_is_not_ready_if_required_dep_missing():
    bus = FakeBus()
    db = FakeDB()
    engine = TradingEngine(
        bus=bus,
        db=db,
        client_getter=None,
        executor=FakeExecutor(),
    )

    readiness = engine.readiness()
    assert readiness["state"] == NOT_READY


def test_bus_subscription_registers_expected_topics():
    engine, _, bus, _, _, _, _, _ = make_engine()

    topics = [topic for topic, _ in bus.subscriptions]
    assert REQ_QUICK_BET in topics
    assert CMD_QUICK_BET in topics
    assert "RECONCILE_NOW" in topics
    assert "RECOVER_PENDING" in topics


def test_dedup_ram_blocks_second_request_with_same_customer_ref():
    engine, _, _, _, _, _, _, _ = make_engine()

    first = engine.submit_quick_bet(make_payload(customer_ref="CUST-SAME", correlation_id="CID-1"))
    second = engine.submit_quick_bet(make_payload(customer_ref="CUST-SAME", correlation_id="CID-2"))

    assert first["status"] == STATUS_ACCEPTED_FOR_PROCESSING
    assert second["status"] == STATUS_DUPLICATE_BLOCKED


def test_dedup_seen_correlation_ids_trims_to_max_size():
    engine, _, _, _, _, _, _, _ = make_engine()

    engine._max_seen_cid_size = 3
    ctx1 = engine._new_execution_context({"customer_ref": "A", "correlation_id": "1"})
    ctx2 = engine._new_execution_context({"customer_ref": "B", "correlation_id": "2"})
    ctx3 = engine._new_execution_context({"customer_ref": "C", "correlation_id": "3"})
    ctx4 = engine._new_execution_context({"customer_ref": "D", "correlation_id": "4"})

    engine._register_dedup_keys(ctx1)
    engine._register_dedup_keys(ctx2)
    engine._register_dedup_keys(ctx3)
    engine._register_dedup_keys(ctx4)

    assert len(engine._seen_correlation_ids) == 3
    assert "1" not in engine._seen_correlation_ids
    assert "2" in engine._seen_correlation_ids
    assert "3" in engine._seen_correlation_ids
    assert "4" in engine._seen_correlation_ids


def test_concurrent_same_customer_ref_one_ack_one_duplicate():
    engine, _, _, _, _, _, _, _ = make_engine()
    results: List[Dict[str, Any]] = []
    barrier = threading.Barrier(2)

    def worker(correlation_id: str) -> None:
        barrier.wait()
        res = engine.submit_quick_bet(
            make_payload(customer_ref="CUST-CONC", correlation_id=correlation_id)
        )
        results.append(res)

    t1 = threading.Thread(target=worker, args=("CID-A",))
    t2 = threading.Thread(target=worker, args=("CID-B",))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    statuses = sorted([r["status"] for r in results])
    assert statuses == [STATUS_ACCEPTED_FOR_PROCESSING, STATUS_DUPLICATE_BLOCKED]