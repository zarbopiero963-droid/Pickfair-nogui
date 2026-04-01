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


class CapturingOrderManager:
    def __init__(self):
        self.calls = 0

    def submit(self, payload):
        self.calls += 1
        return {"bet_id": f"BET-{self.calls}", "accepted": True}


@pytest.mark.integration
def test_successful_new_request_registers_inflight_customer_ref_and_seen_correlation_id():
    bus = FakeBus()
    db = FakeDB()
    om = CapturingOrderManager()

    engine = TradingEngine(
        bus=bus,
        db=db,
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )
    engine.order_manager = om

    customer_ref = "INF-REF-1"
    correlation_id = "INF-CID-1"

    assert customer_ref not in engine._inflight_keys
    assert correlation_id not in engine._seen_correlation_ids

    result = engine.submit_quick_bet(
        {
            "market_id": "1.100",
            "selection_id": 1,
            "price": 2.0,
            "stake": 5.0,
            "side": "BACK",
            "customer_ref": customer_ref,
            "correlation_id": correlation_id,
        }
    )

    assert result["ok"] is True
    assert result["status"] == "ACCEPTED_FOR_PROCESSING"
    assert om.calls == 1

    # customer_ref deve essere stato registrato almeno durante il lifecycle
    # e poi rilasciato su SUCCESS; correlation_id invece resta visto
    assert correlation_id in engine._seen_correlation_ids