import pytest

from core.trading_engine import TradingEngine


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


class CountingOrderManager:
    def __init__(self):
        self.calls = 0

    def submit(self, payload):
        self.calls += 1
        return {"bet_id": f"BET-{self.calls}", "accepted": True}


@pytest.mark.integration
def test_reconcile_now_topic_is_subscribed_as_noop_and_does_not_submit_order():
    bus = FakeBus()
    db = FakeDB()
    om = CountingOrderManager()

    engine = TradingEngine(
        bus=bus,
        db=db,
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )
    engine.order_manager = om

    assert "RECONCILE_NOW" in bus.subscriptions

    handler = bus.subscriptions["RECONCILE_NOW"]
    result = handler({"customer_ref": "SHOULD-NOT-RUN"})

    assert result is None
    assert om.calls == 0
    assert db.orders == {}


@pytest.mark.integration
def test_recover_pending_topic_is_subscribed_as_noop_and_does_not_submit_order():
    bus = FakeBus()
    db = FakeDB()
    om = CountingOrderManager()

    engine = TradingEngine(
        bus=bus,
        db=db,
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )
    engine.order_manager = om

    assert "RECOVER_PENDING" in bus.subscriptions

    handler = bus.subscriptions["RECOVER_PENDING"]
    result = handler({"customer_ref": "SHOULD-NOT-RUN"})

    assert result is None
    assert om.calls == 0
    assert db.orders == {}