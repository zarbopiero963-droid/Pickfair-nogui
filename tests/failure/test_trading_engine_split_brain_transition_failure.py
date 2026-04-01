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
        self.orders = {}
        self.audit_events = []
        self.seq = 0

    def is_ready(self):
        return True

    def insert_order(self, payload):
        self.seq += 1
        order_id = f"ORD-{self.seq}"
        self.orders[order_id] = dict(payload)
        return order_id

    def update_order(self, order_id, update):
        self.orders.setdefault(order_id, {})
        self.orders[order_id].update(update)

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


class ReconcileHook:
    def __init__(self):
        self.calls = []

    def is_ready(self):
        return True

    def enqueue(self, **kwargs):
        self.calls.append(dict(kwargs))


class SuccessfulOrderManager:
    def __init__(self):
        self.payloads = []

    def submit(self, payload):
        self.payloads.append(dict(payload))
        return {"bet_id": "BET-SPLIT-1", "accepted": True}


@pytest.mark.failure
def test_split_brain_submit_success_but_transition_to_submitted_fails_goes_ambiguous_and_enqueues_reconcile():
    from core.trading_engine import TradingEngine

    bus = FakeBus()
    db = FakeDB()
    reconcile = ReconcileHook()

    engine = TradingEngine(
        bus=bus,
        db=db,
        client_getter=lambda: None,
        executor=InlineExecutor(),
        reconciliation_engine=reconcile,
    )
    engine.order_manager = SuccessfulOrderManager()

    original_transition = engine._transition_order

    def exploding_transition(ctx, audit, order_id, from_status, to_status, extra=None):
        if from_status == "INFLIGHT" and to_status == "SUBMITTED":
            raise RuntimeError("STATE_WRITE_SPLIT_BRAIN")
        return original_transition(ctx, audit, order_id, from_status, to_status, extra)

    engine._transition_order = exploding_transition

    result = engine.submit_quick_bet(
        {
            "market_id": "1.120",
            "selection_id": 21,
            "price": 2.4,
            "stake": 7.0,
            "side": "BACK",
            "customer_ref": "SPLIT-1",
            "correlation_id": "CID-SPLIT-1",
            "simulation_mode": True,
            "event_key": "1.120:21:BACK",
        }
    )

    assert result["ok"] is False
    assert result["status"] == "AMBIGUOUS"
    assert result["outcome"] == "AMBIGUOUS"
    assert result["ambiguity_reason"] == "PERSISTED_NOT_CONFIRMED"
    assert result["simulation_mode"] is True
    assert result["event_key"] == "1.120:21:BACK"

    assert len(reconcile.calls) == 1
    assert reconcile.calls[0]["customer_ref"] == "SPLIT-1"
    assert reconcile.calls[0]["correlation_id"] == "CID-SPLIT-1"
    assert reconcile.calls[0]["ambiguity_reason"] == "PERSISTED_NOT_CONFIRMED"

    event_names = [name for name, _ in bus.events]
    assert "QUICK_BET_ROUTED" in event_names
    assert "QUICK_BET_AMBIGUOUS" in event_names

    audit_types = [event["type"] for event in result["audit"]["events"]]
    assert "SUBMIT_TRANSITION_FAILED" in audit_types
    assert "RECONCILE_ENQUEUED" in audit_types
    assert "FINAL_AMBIGUOUS" in audit_types