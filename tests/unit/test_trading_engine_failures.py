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
    pass


class SyncExecutor:
    def submit(self, _name, fn, *args, **kwargs):
        return fn(*args, **kwargs)


class ExplodingOrderManager:
    def place_order(self, payload):
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
        executor=SyncExecutor(),
    )

    handler = bus.subscriptions["REQ_QUICK_BET"]

    result = handler({"selectionId": None, "price": 0, "size": -1, "side": "BACK"})

    assert result["ok"] is False
    assert result["status"] == "FAILED"
    assert any(event == "QUICK_BET_FAILED" for event, _ in bus.events)


@pytest.mark.unit
@pytest.mark.failure
def test_crash_mid_order_publishes_failure_and_rollback_required():
    from core.trading_engine import TradingEngine

    bus = FakeBus()
    engine = TradingEngine(
        bus=bus,
        db=FakeDB(),
        client_getter=lambda: None,
        executor=SyncExecutor(),
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

    assert result["ok"] is True
    event_names = [name for name, _ in bus.events]
    assert "QUICK_BET_ROUTED" in event_names
    assert "QUICK_BET_ROLLBACK_REQUIRED" in event_names


@pytest.mark.unit
@pytest.mark.failure
def test_failed_path_releases_inflight_key():
    from core.trading_engine import TradingEngine

    bus = FakeBus()
    engine = TradingEngine(
        bus=bus,
        db=FakeDB(),
        client_getter=lambda: None,
        executor=SyncExecutor(),
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
    dedup_key = "REL1"

    engine.submit_quick_bet(payload)

    with engine._lock:
        assert dedup_key not in engine._inflight_keys