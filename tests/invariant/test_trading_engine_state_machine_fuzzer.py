import pytest


class FakeBus:
    def __init__(self):
        self.subscriptions = {}

    def subscribe(self, topic, handler):
        self.subscriptions[topic] = handler

    def publish(self, _name, _payload):
        pass


class FakeDB:
    def __init__(self):
        self.orders = {}
        self.audit_events = []

    def is_ready(self):
        return True

    def insert_audit_event(self, event):
        self.audit_events.append(event)

    def load_pending_customer_refs(self):
        return []

    def load_pending_correlation_ids(self):
        return []

    def order_exists_inflight(self, *, customer_ref, correlation_id):
        return False

    def update_order(self, order_id, update):
        self.orders.setdefault(order_id, {})
        self.orders[order_id].update(update)

    def get_order(self, order_id):
        return self.orders.get(order_id)


class InlineExecutor:
    def is_ready(self):
        return True

    def submit(self, _name, fn):
        return fn()


@pytest.mark.invariant
@pytest.mark.parametrize(
    ("from_status", "to_status", "allowed"),
    [
        ("INFLIGHT", "SUBMITTED", True),
        ("INFLIGHT", "FAILED", True),
        ("INFLIGHT", "AMBIGUOUS", True),
        ("INFLIGHT", "DENIED", True),
        ("INFLIGHT", "DUPLICATE_BLOCKED", True),
        ("INFLIGHT", "COMPLETED", False),
        ("SUBMITTED", "COMPLETED", True),
        ("SUBMITTED", "FAILED", True),
        ("SUBMITTED", "AMBIGUOUS", True),
        ("SUBMITTED", "DENIED", False),
        ("SUBMITTED", "DUPLICATE_BLOCKED", False),
        ("AMBIGUOUS", "COMPLETED", True),
        ("AMBIGUOUS", "FAILED", True),
        ("AMBIGUOUS", "SUBMITTED", False),
        ("DENIED", "FAILED", False),
        ("FAILED", "COMPLETED", False),
        ("COMPLETED", "FAILED", False),
        ("DUPLICATE_BLOCKED", "FAILED", False),
    ],
)
def test_state_machine_transition_matrix(from_status, to_status, allowed):
    from core.trading_engine import TradingEngine, _ExecutionContext

    bus = FakeBus()
    db = FakeDB()

    engine = TradingEngine(
        bus=bus,
        db=db,
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )

    ctx = _ExecutionContext(
        correlation_id="CID-FUZZ-1",
        customer_ref="REF-FUZZ-1",
        created_at=0.0,
    )
    audit = engine._new_audit(ctx)

    db.orders["ORD-FUZZ-1"] = {
        "status": from_status,
        "finalized": False,
    }

    if allowed:
        engine._transition_order(
            ctx=ctx,
            audit=audit,
            order_id="ORD-FUZZ-1",
            from_status=from_status,
            to_status=to_status,
            extra={"marker": "ok"},
        )
        assert db.orders["ORD-FUZZ-1"]["status"] == to_status
        assert db.orders["ORD-FUZZ-1"]["marker"] == "ok"
    else:
        with pytest.raises(RuntimeError, match="ILLEGAL_ORDER_TRANSITION"):
            engine._transition_order(
                ctx=ctx,
                audit=audit,
                order_id="ORD-FUZZ-1",
                from_status=from_status,
                to_status=to_status,
                extra={"marker": "bad"},
            )


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