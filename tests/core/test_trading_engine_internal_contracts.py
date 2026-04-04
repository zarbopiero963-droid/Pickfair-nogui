from __future__ import annotations

import sys
import time
import types
from typing import Any, Dict, List, Optional

import pytest

if "order_manager" not in sys.modules:
    mod = types.ModuleType("order_manager")
    mod.OrderManager = object
    sys.modules["order_manager"] = mod

from core.trading_engine import (
    TradingEngine,
    STATUS_INFLIGHT,
    STATUS_SUBMITTED,
    STATUS_COMPLETED,
    STATUS_FAILED,
    STATUS_AMBIGUOUS,
    STATUS_DENIED,
    STATUS_DUPLICATE_BLOCKED,
    STATUS_ACCEPTED_FOR_PROCESSING,
    OUTCOME_SUCCESS,
    OUTCOME_FAILURE,
    OUTCOME_AMBIGUOUS,
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

    def is_ready(self) -> bool:
        return True

    def insert_order(self, payload: Dict[str, Any]) -> str:
        order_id = str(self.next_id)
        self.next_id += 1
        self.orders[order_id] = dict(payload)
        return order_id

    def update_order(self, order_id: str, update: Dict[str, Any]) -> None:
        self.orders[order_id].update(dict(update))

    def get_order(self, order_id: str) -> Dict[str, Any]:
        return dict(self.orders[order_id])

    def insert_audit_event(self, event: Dict[str, Any]) -> None:
        self.audit_events.append(dict(event))

    def order_exists_inflight(self, customer_ref: Optional[str] = None,
                              correlation_id: Optional[str] = None) -> bool:
        return False

    def find_duplicate_order(self, customer_ref: Optional[str] = None,
                             correlation_id: Optional[str] = None) -> Optional[str]:
        return None

    def load_pending_customer_refs(self) -> List[str]:
        return []

    def load_pending_correlation_ids(self) -> List[str]:
        return []


class FakeClient:
    def place_bet(self, **payload: Any) -> Any:
        return {"bet_id": "BET-1"}


class FakeExecutor:
    def is_ready(self) -> bool:
        return True

    def submit(self, operation_name: str, fn: Any) -> Any:
        return fn()


class FakeAsyncDbWriter:
    def is_ready(self) -> bool:
        return True

    def write(self, event: Dict[str, Any]) -> None:
        return None


def make_engine():
    db = FakeDB()
    bus = FakeBus()
    engine = TradingEngine(
        bus=bus,
        db=db,
        client_getter=lambda: FakeClient(),
        executor=FakeExecutor(),
        async_db_writer=FakeAsyncDbWriter(),
    )
    return engine, db, bus


def make_ctx(engine: TradingEngine, customer_ref: str = "CUST", correlation_id: str = "CID"):
    return engine._new_execution_context(
        {
            "customer_ref": customer_ref,
            "correlation_id": correlation_id,
            "event_key": "EVT",
            "simulation_mode": False,
        }
    )


def test_build_result_has_stable_contract_for_terminal():
    engine, _, _ = make_engine()
    ctx = make_ctx(engine)
    audit = engine._new_audit(ctx)

    result = engine._build_result(
        ctx,
        audit,
        status=STATUS_COMPLETED,
        outcome=OUTCOME_SUCCESS,
        order_id="1",
        reason="OK",
        error=None,
        ambiguity_reason=None,
        response={"bet_id": "B1"},
        extra_fields={"order_origin": ORIGIN_NORMAL},
        is_terminal=True,
    )

    assert result == {
        "ok": True,
        "status": STATUS_COMPLETED,
        "outcome": OUTCOME_SUCCESS,
        "is_terminal": True,
        "lifecycle_stage": "finalized",
        "order_id": "1",
        "correlation_id": "CID",
        "customer_ref": "CUST",
        "event_key": "EVT",
        "simulation_mode": False,
        "audit": {"correlation_id": "CID", "customer_ref": "CUST", "events": [], "index": 0, "order_origin": ORIGIN_NORMAL},
        "reason": "OK",
        "error": None,
        "ambiguity_reason": None,
        "response": {"bet_id": "B1"},
        "order_origin": ORIGIN_NORMAL,
    }


def test_build_ack_result_is_non_terminal_and_public_status_is_accepted():
    engine, _, _ = make_engine()
    ctx = make_ctx(engine)
    audit = engine._new_audit(ctx)

    result = engine._build_ack_result(
        ctx,
        audit,
        order_id="1",
        status=STATUS_SUBMITTED,
        response={"bet_id": "B1"},
        extra_fields={"order_origin": ORIGIN_NORMAL},
    )

    assert result["status"] == STATUS_ACCEPTED_FOR_PROCESSING
    assert result["outcome"] == OUTCOME_SUCCESS
    assert result["is_terminal"] is False
    assert result["lifecycle_stage"] == "accepted"


def test_build_ack_result_rejects_terminal_status():
    engine, _, _ = make_engine()
    ctx = make_ctx(engine)
    audit = engine._new_audit(ctx)

    with pytest.raises(RuntimeError, match="TERMINAL_STATUS_NOT_ALLOWED_HERE"):
        engine._build_ack_result(
            ctx,
            audit,
            order_id="1",
            status=STATUS_COMPLETED,
        )


def test_terminal_invariants_require_ambiguity_reason_for_ambiguous():
    engine, _, _ = make_engine()
    ctx = make_ctx(engine)
    audit = engine._new_audit(ctx)

    order_id = "1"
    engine.db.orders[order_id] = {
        "customer_ref": "CUST",
        "correlation_id": "CID",
        "status": STATUS_AMBIGUOUS,
        "payload": {},
        "created_at": time.time(),
    }

    with pytest.raises(RuntimeError, match="AMBIGUOUS_FINALIZE_REQUIRES_REASON"):
        engine._complete_order_lifecycle(
            ctx,
            audit,
            order_id=order_id,
            status=STATUS_AMBIGUOUS,
        )


def test_terminal_invariants_reject_error_on_denied():
    engine, _, _ = make_engine()
    ctx = make_ctx(engine)
    audit = engine._new_audit(ctx)

    order_id = "1"
    engine.db.orders[order_id] = {
        "customer_ref": "CUST",
        "correlation_id": "CID",
        "status": STATUS_DENIED,
        "payload": {},
        "created_at": time.time(),
    }

    with pytest.raises(RuntimeError, match="DENIED_SHOULD_NOT_CARRY_TECHNICAL_ERROR"):
        engine._complete_order_lifecycle(
            ctx,
            audit,
            order_id=order_id,
            status=STATUS_DENIED,
            error="TECH_ERR",
        )


def test_terminal_invariants_reject_ambiguity_reason_on_completed():
    engine, _, _ = make_engine()
    ctx = make_ctx(engine)
    audit = engine._new_audit(ctx)

    order_id = "1"
    engine.db.orders[order_id] = {
        "customer_ref": "CUST",
        "correlation_id": "CID",
        "status": STATUS_COMPLETED,
        "payload": {},
        "created_at": time.time(),
    }

    with pytest.raises(RuntimeError, match="COMPLETED_CANNOT_KEEP_AMBIGUITY_REASON"):
        engine._complete_order_lifecycle(
            ctx,
            audit,
            order_id=order_id,
            status=STATUS_COMPLETED,
            ambiguity_reason="NOPE",
        )


def test_write_order_metadata_rejects_status_field():
    engine, db, _ = make_engine()
    order_id = db.insert_order(
        {
            "customer_ref": "CUST",
            "correlation_id": "CID",
            "status": STATUS_COMPLETED,
            "payload": {},
            "created_at": time.time(),
        }
    )

    with pytest.raises(RuntimeError, match="METADATA_WRITE_MUST_NOT_CONTAIN_STATUS"):
        engine._write_order_metadata(order_id, {"status": STATUS_FAILED})


def test_build_terminal_metadata_contains_expected_fields():
    engine, _, _ = make_engine()

    meta = engine._build_terminal_metadata(
        outcome=OUTCOME_SUCCESS,
        reason="DONE",
        error=None,
        ambiguity_reason=None,
        response={"bet_id": "B1"},
        extra_fields={
            "order_origin": ORIGIN_COPY,
            "copy_meta": {"master_id": "M1"},
            "pattern_meta": {"pattern_id": "P1"},
        },
    )

    assert meta["outcome"] == OUTCOME_SUCCESS
    assert meta["reason"] == "DONE"
    assert meta["last_error"] is None
    assert meta["ambiguity_reason"] is None
    assert meta["finalized"] is True
    assert meta["response"] == {"bet_id": "B1"}
    assert meta["order_origin"] == ORIGIN_COPY
    assert meta["copy_meta"] == {"master_id": "M1"}
    assert meta["pattern_meta"] == {"pattern_id": "P1"}
    assert "updated_at" in meta


def test_merge_passthrough_fields_only_allows_supported_keys():
    engine, _, _ = make_engine()

    merged = engine._merge_passthrough_fields(
        {"x": 1},
        {
            "simulation_mode": True,
            "event_key": "E1",
            "order_origin": ORIGIN_PATTERN,
            "copy_meta": {"master_id": "M1"},
            "pattern_meta": {"pattern_id": "P1"},
            "ignored": "DROP",
        },
    )

    assert merged == {
        "x": 1,
        "simulation_mode": True,
        "event_key": "E1",
        "order_origin": ORIGIN_PATTERN,
        "copy_meta": {"master_id": "M1"},
        "pattern_meta": {"pattern_id": "P1"},
    }


def test_extract_origin_fields_best_effort_prefers_copy_when_valid():
    engine, _, _ = make_engine()

    out = engine._extract_origin_fields_best_effort(
        {
            "copy_meta": {
                "master_id": "M1",
                "master_position_id": "MP1",
                "action_id": "A1",
                "action_seq": 1,
                "copy_group_id": "CG1",
                "copy_mode": "FOLLOW",
                "ignored": "DROP",
            },
            "event_key": "EVT",
            "simulation_mode": True,
        }
    )

    assert out == {
        "order_origin": ORIGIN_COPY,
        "copy_meta": {
            "master_id": "M1",
            "master_position_id": "MP1",
            "action_id": "A1",
            "action_seq": 1,
            "copy_group_id": "CG1",
            "copy_mode": "FOLLOW",
        },
        "event_key": "EVT",
        "simulation_mode": True,
    }


def test_extract_origin_fields_best_effort_prefers_pattern_when_valid():
    engine, _, _ = make_engine()

    out = engine._extract_origin_fields_best_effort(
        {
            "pattern_meta": {
                "pattern_id": "P1",
                "pattern_label": "LBL",
                "selection_template": "SEL",
                "market_type": "OU",
                "bet_side": "BACK",
                "live_only": True,
                "event_context": {"league": "A"},
                "ignored": "DROP",
            }
        }
    )

    assert out == {
        "order_origin": ORIGIN_PATTERN,
        "pattern_meta": {
            "pattern_id": "P1",
            "pattern_label": "LBL",
            "selection_template": "SEL",
            "market_type": "OU",
            "bet_side": "BACK",
            "live_only": True,
            "event_context": {"league": "A"},
        },
    }


def test_normalize_request_rejects_non_dict():
    engine, _, _ = make_engine()

    with pytest.raises(ValueError, match="REQUEST_MUST_BE_DICT"):
        engine._normalize_request("not-a-dict")  # type: ignore[arg-type]


def test_normalize_request_requires_customer_ref():
    engine, _, _ = make_engine()

    with pytest.raises(ValueError, match="CUSTOMER_REF_REQUIRED"):
        engine._normalize_request({"correlation_id": "CID"})


def test_normalize_request_requires_meta_dict_types():
    engine, _, _ = make_engine()

    with pytest.raises(ValueError, match="COPY_META_MUST_BE_DICT"):
        engine._normalize_request(
            {
                "customer_ref": "C1",
                "correlation_id": "CID",
                "copy_meta": "bad",
            }
        )

    with pytest.raises(ValueError, match="PATTERN_META_MUST_BE_DICT"):
        engine._normalize_request(
            {
                "customer_ref": "C1",
                "correlation_id": "CID",
                "pattern_meta": "bad",
            }
        )


def test_normalize_request_auto_generates_correlation_id_when_enabled():
    engine, _, _ = make_engine()

    out = engine._normalize_request({"customer_ref": "C1"})

    assert out["customer_ref"] == "C1"
    assert isinstance(out["correlation_id"], str)
    assert out["order_origin"] == ORIGIN_NORMAL
    assert len(out["correlation_id"]) > 10


def test_normalize_request_requires_correlation_id_when_auto_generation_disabled():
    engine, _, _ = make_engine()
    engine.auto_generate_correlation_id = False

    with pytest.raises(ValueError, match="CORRELATION_ID_REQUIRED"):
        engine._normalize_request({"customer_ref": "C1"})


def test_safe_write_order_metadata_returns_false_on_error():
    engine, _, _ = make_engine()

    original = engine._write_order_metadata

    def broken(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("BROKEN")

    engine._write_order_metadata = broken
    try:
        ok = engine._safe_write_order_metadata("1", {"finalized": True})
    finally:
        engine._write_order_metadata = original

    assert ok is False


def test_safe_mark_failed_returns_false_on_transition_error():
    engine, db, _ = make_engine()

    order_id = db.insert_order(
        {
            "customer_ref": "CUST",
            "correlation_id": "CID",
            "status": STATUS_SUBMITTED,
            "payload": {},
            "created_at": time.time(),
        }
    )
    ctx = make_ctx(engine)
    audit = engine._new_audit(ctx)

    ok = engine._safe_mark_failed(
        ctx,
        audit,
        order_id,
        reason="ENGINE_FATAL",
        error="boom",
        from_status=STATUS_INFLIGHT,
    )

    assert ok is False


def test_emit_builds_parent_chain_and_monotonic_index():
    engine, _, _ = make_engine()
    ctx = make_ctx(engine)
    audit = engine._new_audit(ctx)

    r1 = engine._emit(ctx, audit, "E1", {"a": 1}, category="guard")
    r2 = engine._emit(ctx, audit, "E2", {"a": 2}, category="execution")
    r3 = engine._emit(ctx, audit, "E3", {"a": 3}, category="final")

    assert r1["memory_only"] is False
    assert r2["memory_only"] is False
    assert r3["memory_only"] is False

    events = audit["events"]
    assert len(events) == 3
    assert events[0]["index"] == 0
    assert events[1]["index"] == 1
    assert events[2]["index"] == 2
    assert events[0]["parent_event_id"] is None
    assert events[1]["parent_event_id"] == events[0]["event_id"]
    assert events[2]["parent_event_id"] == events[1]["event_id"]


def test_emit_adds_copy_category_suffix():
    engine, _, _ = make_engine()
    ctx = make_ctx(engine)
    audit = engine._new_audit(ctx)
    audit["order_origin"] = ORIGIN_COPY

    engine._emit(ctx, audit, "COPY_EVENT", {"x": 1}, category="execution")
    event = audit["events"][0]

    assert event["category"] == "execution_copy"
    assert event["payload"]["order_origin"] == ORIGIN_COPY


def test_emit_adds_pattern_category_suffix():
    engine, _, _ = make_engine()
    ctx = make_ctx(engine)
    audit = engine._new_audit(ctx)
    audit["order_origin"] = ORIGIN_PATTERN

    engine._emit(ctx, audit, "PATTERN_EVENT", {"x": 1}, category="guard")
    event = audit["events"][0]

    assert event["category"] == "guard_pattern"
    assert event["payload"]["order_origin"] == ORIGIN_PATTERN


def test_complete_order_lifecycle_success_shape():
    engine, db, bus = make_engine()
    ctx = make_ctx(engine)
    audit = engine._new_audit(ctx)

    order_id = db.insert_order(
        {
            "customer_ref": "CUST",
            "correlation_id": "CID",
            "status": STATUS_COMPLETED,
            "payload": {},
            "created_at": time.time(),
        }
    )

    result = engine._complete_order_lifecycle(
        ctx,
        audit,
        order_id=order_id,
        status=STATUS_COMPLETED,
        reason="OK",
        response={"bet_id": "B1"},
        extra_fields={"order_origin": ORIGIN_NORMAL},
    )

    assert result["status"] == STATUS_COMPLETED
    assert result["outcome"] == OUTCOME_SUCCESS
    assert result["is_terminal"] is True
    assert result["lifecycle_stage"] == "finalized"
    assert result["finalization_persisted"] is True
    assert any(name == "QUICK_BET_SUCCESS" for name, _ in bus.published)


def test_complete_order_lifecycle_duplicate_shape():
    engine, db, bus = make_engine()
    ctx = make_ctx(engine)
    audit = engine._new_audit(ctx)

    order_id = db.insert_order(
        {
            "customer_ref": "CUST",
            "correlation_id": "CID",
            "status": STATUS_DUPLICATE_BLOCKED,
            "payload": {},
            "created_at": time.time(),
        }
    )

    result = engine._complete_order_lifecycle(
        ctx,
        audit,
        order_id=order_id,
        status=STATUS_DUPLICATE_BLOCKED,
        reason="DUPLICATE_BLOCKED",
        terminal_bus_event="QUICK_BET_DUPLICATE",
        extra_fields={"order_origin": ORIGIN_NORMAL},
    )

    assert result["status"] == STATUS_DUPLICATE_BLOCKED
    assert result["outcome"] == OUTCOME_SUCCESS
    assert result["finalization_persisted"] is True
    assert any(name == "QUICK_BET_DUPLICATE" for name, _ in bus.published)