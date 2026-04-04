import random

import pytest


class FakeBus:
    def __init__(self):
        self.subscriptions = {}
        self.events = []

    def subscribe(self, topic, handler):
        self.subscriptions[topic] = handler

    def publish(self, name, payload):
        self.events.append((name, payload))


class FakeDB:
    def __init__(self):
        self.audit_events = []
        self.orders = {}

    def is_ready(self):
        return True

    def insert_audit_event(self, event):
        self.audit_events.append(event)

    def update_order(self, order_id, update):
        self.orders.setdefault(order_id, {})
        self.orders[order_id].update(update)

    def get_order(self, order_id):
        return self.orders.get(order_id, {"status": None, "finalized": False})

    def order_exists_inflight(self, *, customer_ref, correlation_id):
        return False

    def load_pending_customer_refs(self):
        return []

    def load_pending_correlation_ids(self):
        return []


class InlineExecutor:
    def is_ready(self):
        return True

    def submit(self, _name, fn):
        return fn()


@pytest.mark.invariant
def test_finalize_property_success_failure_release_inflight_but_not_ambiguous():
    from core.trading_engine import TradingEngine, _ExecutionContext

    engine = TradingEngine(
        bus=FakeBus(),
        db=FakeDB(),
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )

    cases = [
        ("COMPLETED", "SUCCESS", None, True),
        ("FAILED", "FAILURE", None, True),
        ("AMBIGUOUS", "AMBIGUOUS", "SUBMIT_TIMEOUT", False),
    ]

    for idx, (status, outcome, ambiguity_reason, should_release) in enumerate(cases, start=1):
        cid = f"CID-{idx}"
        cref = f"REF-{idx}"
        oid = f"ORD-{idx}"

        with engine._lock:
            engine._inflight_keys.add(cref)
            engine._seen_correlation_ids.add(cid)
            engine._seen_cid_order.append(cid)

        # prepopulate DB in terminal state so _precheck_finalize passes
        engine.db.orders[oid] = {
            "status": status,
            "finalized": False,
            "customer_ref": cref,
            "correlation_id": cid,
        }

        ctx = _ExecutionContext(
            correlation_id=cid,
            customer_ref=cref,
            created_at=0.0,
        )
        audit = engine._new_audit(ctx)

        result = engine._finalize(
            ctx=ctx,
            audit=audit,
            order_id=oid,
            status=status,
            outcome=outcome,
            ambiguity_reason=ambiguity_reason,
        )

        assert result["outcome"] == outcome
        assert result["status"] == status
        assert result["finalization_persisted"] is True

        if should_release:
            assert cref not in engine._inflight_keys
        else:
            assert cref in engine._inflight_keys

        assert cid in engine._seen_correlation_ids


@pytest.mark.invariant
def test_finalize_property_public_status_mapping_is_stable():
    from core.trading_engine import TradingEngine, _ExecutionContext

    engine = TradingEngine(
        bus=FakeBus(),
        db=FakeDB(),
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )

    statuses = [
        ("COMPLETED", "SUCCESS", "COMPLETED", None),
        ("FAILED", "FAILURE", "FAILED", None),
        ("DENIED", "FAILURE", "DENIED", None),
        ("AMBIGUOUS", "AMBIGUOUS", "AMBIGUOUS", "SUBMIT_TIMEOUT"),
        ("DUPLICATE_BLOCKED", "SUCCESS", "DUPLICATE_BLOCKED", None),
    ]

    for idx, (internal_status, outcome, public_status, ambiguity_reason) in enumerate(statuses, start=1):
        oid = f"ORD-MAP-{idx}"

        engine.db.orders[oid] = {
            "status": internal_status,
            "finalized": False,
        }

        ctx = _ExecutionContext(
            correlation_id=f"CID-MAP-{idx}",
            customer_ref=f"REF-MAP-{idx}",
            created_at=random.random(),
        )
        audit = engine._new_audit(ctx)

        result = engine._finalize(
            ctx=ctx,
            audit=audit,
            order_id=oid,
            status=internal_status,
            outcome=outcome,
            ambiguity_reason=ambiguity_reason,
        )

        assert result["status"] == public_status
        assert result["outcome"] == outcome


@pytest.mark.invariant
def test_finalize_rejects_ambiguous_without_reason():
    from core.trading_engine import TradingEngine, _ExecutionContext

    engine = TradingEngine(
        bus=FakeBus(),
        db=FakeDB(),
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )

    engine.db.orders["ORD-X"] = {
        "status": "AMBIGUOUS",
        "finalized": False,
    }

    ctx = _ExecutionContext("CID-X", "REF-X", 0.0)
    audit = engine._new_audit(ctx)

    with pytest.raises(RuntimeError, match="AMBIGUOUS_FINALIZE_REQUIRES_REASON"):
        engine._finalize(
            ctx=ctx,
            audit=audit,
            order_id="ORD-X",
            status="AMBIGUOUS",
            outcome="AMBIGUOUS",
        )


@pytest.mark.invariant
def test_finalize_rejects_denied_with_technical_error():
    from core.trading_engine import TradingEngine, _ExecutionContext

    engine = TradingEngine(
        bus=FakeBus(),
        db=FakeDB(),
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )

    engine.db.orders["ORD-Y"] = {
        "status": "DENIED",
        "finalized": False,
    }

    ctx = _ExecutionContext("CID-Y", "REF-Y", 0.0)
    audit = engine._new_audit(ctx)

    with pytest.raises(RuntimeError, match="DENIED_SHOULD_NOT_CARRY_TECHNICAL_ERROR"):
        engine._finalize(
            ctx=ctx,
            audit=audit,
            order_id="ORD-Y",
            status="DENIED",
            outcome="FAILURE",
            error="should not exist",
        )