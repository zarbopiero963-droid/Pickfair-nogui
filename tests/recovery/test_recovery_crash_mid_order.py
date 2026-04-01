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


class ExplodingOrderManager:
    def submit(self, payload):
        raise RuntimeError("mid-order crash")


@pytest.mark.recovery
@pytest.mark.failure
def test_crash_mid_order_marks_saga_and_publishes_recovery_events():
    from core.trading_engine import TradingEngine

    bus = FakeBus()
    db = FakeDB()

    engine = TradingEngine(
        bus=bus,
        db=db,
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )
    engine.order_manager = ExplodingOrderManager()

    result = engine.submit_quick_bet(
        {
            "market_id": "1.111",
            "selection_id": 10,
            "price": 2.0,
            "stake": 5.0,
            "side": "BACK",
            "customer_ref": "CRASH-1",
        }
    )

    assert result["ok"] is False
    assert result["status"] == "FAILED"

    assert len(db.orders) == 1
    order = next(iter(db.orders.values()))
    assert order["status"] == "FAILED"
    assert order["customer_ref"] == "CRASH-1"
    assert "mid-order crash" in order["last_error"]

    names = [x[0] for x in bus.events]
    assert "QUICK_BET_ROUTED" in names
    assert "QUICK_BET_FAILED" in names
