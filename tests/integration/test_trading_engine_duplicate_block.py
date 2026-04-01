import pytest


class FakeBus:
    def __init__(self):
        self.events = []
        self.subscriptions = {}

    def subscribe(self, event_name, handler):
        self.subscriptions[event_name] = handler

    def publish(self, event_name, payload=None):
        self.events.append((event_name, payload))


class FakeDB:
    def __init__(self):
        self.orders = {}
        self.audit_events = []
        self.seq = 0

    def is_ready(self):
        return True

    def insert_order(self, payload):
        self.seq += 1
        oid = f"ORD-{self.seq}"
        self.orders[oid] = dict(payload)
        return oid

    def update_order(self, order_id, update):
        self.orders.setdefault(order_id, {})
        self.orders[order_id].update(update)

    def insert_audit_event(self, event):
        self.audit_events.append(event)

    def load_pending_customer_refs(self):
        return []

    def load_pending_correlation_ids(self):
        return []

    def order_exists_inflight(self, *, customer_ref, correlation_id):
        return False


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
        return {"ok": True}


@pytest.mark.integration
def test_duplicate_request_is_blocked():
    from core.trading_engine import TradingEngine

    bus = FakeBus()
    engine = TradingEngine(
        bus=bus,
        db=FakeDB(),
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )

    om = CountingOrderManager()
    engine.order_manager = om

    payload = {
        "market_id": "1.100",
        "selection_id": 10,
        "price": 2.0,
        "size": 10.0,
        "side": "BACK",
        "customer_ref": "DUP1",
        "event_key": "1.100:10:BACK",
    }

    with engine._lock:
        engine._inflight_keys.add("DUP1")

    result = engine.submit_quick_bet(payload)

    assert result["ok"] is True
    assert result["status"] == "DUPLICATE_BLOCKED"
    assert om.calls == 0

    event_names = [x[0] for x in bus.events]
    assert "QUICK_BET_DUPLICATE" in event_names
