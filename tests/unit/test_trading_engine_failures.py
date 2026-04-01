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
        raise RuntimeError("order manager exploded")


@pytest.mark.unit
@pytest.mark.failure
def test_invalid_payload_is_rejected_without_crash():
    from core.trading_engine import TradingEngine

    bus = FakeBus()
    engine = TradingEngine(
        bus=bus,
        db=FakeDB(),
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )

    handler = bus.subscriptions["REQ_QUICK_BET"]

    result = handler({"selectionId": None, "price": 0, "size": -1, "side": "BACK"})

    assert result["ok"] is False
    assert result["status"] == "FAILED"
    assert any(event == "QUICK_BET_FAILED" for event, _ in bus.events)


@pytest.mark.unit
@pytest.mark.failure
def test_crash_mid_order_publishes_async_failure_and_rollback_required():
    from core.trading_engine import TradingEngine

    bus = FakeBus()
    engine = TradingEngine(
        bus=bus,
        db=FakeDB(),
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )
    engine.order_manager = ExplodingOrderManager()

    result = engine.submit_quick_bet(
        {
            "market_id": "1.200",
            "selection_id": 22,
            "price": 2.2,
            "stake": 10.0,
            "side": "BACK",
            "customer_ref": "CR1",
        }
    )

    # With InlineExecutor, crash is immediate → result is FAILED
    assert result["ok"] is False
    assert result["status"] == "FAILED"

    event_names = [name for name, _ in bus.events]
    assert "QUICK_BET_ROUTED" in event_names
    assert "QUICK_BET_FAILED" in event_names


@pytest.mark.unit
@pytest.mark.failure
def test_failed_async_path_releases_inflight_key():
    from core.trading_engine import TradingEngine

    bus = FakeBus()
    engine = TradingEngine(
        bus=bus,
        db=FakeDB(),
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )
    engine.order_manager = ExplodingOrderManager()

    payload = {
        "market_id": "1.300",
        "selection_id": 33,
        "price": 2.5,
        "stake": 10.0,
        "side": "BACK",
        "customer_ref": "REL1",
    }

    result = engine.submit_quick_bet(payload)
    assert result["ok"] is False

    with engine._lock:
        assert "REL1" not in engine._inflight_keys
