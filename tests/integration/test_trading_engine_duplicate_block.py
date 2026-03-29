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


class InlineExecutor:
    def submit(self, _name, fn=None, *args, **kwargs):
        target = fn if fn is not None else _name
        return target(*args, **kwargs)


class CountingOrderManager:
    def __init__(self):
        self.calls = 0

    def place_order(self, payload):
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
    assert "QUICK_BET_DUPLICATE_BLOCKED" in event_names