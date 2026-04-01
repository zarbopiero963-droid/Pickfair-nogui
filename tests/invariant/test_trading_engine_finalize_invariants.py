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

    def is_ready(self):
        return True

    def insert_audit_event(self, event):
        self.audit_events.append(event)

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
@pytest.mark.parametrize(
    "status,outcome,error,ambiguity_reason,expected_error",
    [
        ("AMBIGUOUS", "AMBIGUOUS", None, None, "AMBIGUOUS_FINALIZE_REQUIRES_REASON"),
        ("DENIED", "FAILURE", "TECH_ERROR", None, "DENIED_SHOULD_NOT_CARRY_TECHNICAL_ERROR"),
        ("COMPLETED", "SUCCESS", None, "SHOULD_NOT_EXIST", "COMPLETED_CANNOT_KEEP_AMBIGUITY_REASON"),
    ],
)
def test_finalize_guard_invariants_raise(status, outcome, error, ambiguity_reason, expected_error):
    from core.trading_engine import TradingEngine, _ExecutionContext

    engine = TradingEngine(
        bus=FakeBus(),
        db=FakeDB(),
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )

    ctx = _ExecutionContext(
        correlation_id="CID-FINALIZE-1",
        customer_ref="REF-FINALIZE-1",
        created_at=0.0,
    )
    audit = engine._new_audit(ctx)

    with pytest.raises(RuntimeError, match=expected_error):
        engine._finalize(
            ctx=ctx,
            audit=audit,
            order_id=None,
            status=status,
            outcome=outcome,
            error=error,
            ambiguity_reason=ambiguity_reason,
        )