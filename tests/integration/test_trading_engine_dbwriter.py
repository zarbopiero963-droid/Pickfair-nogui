import threading
import time

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


class FakeAsyncWriter:
    def __init__(self):
        self.items = []
        self.lock = threading.Lock()

    def submit(self, kind, payload):
        with self.lock:
            self.items.append((kind, dict(payload)))
        return True


class AsyncExecutor:
    def submit(self, _name, fn=None, *args, **kwargs):
        target = fn if fn is not None else _name
        t = threading.Thread(target=target, args=args, kwargs=kwargs, daemon=True)
        t.start()
        return t


class CapturingOrderManager:
    def __init__(self):
        self.calls = 0
        self.lock = threading.Lock()

    def place_order(self, payload):
        with self.lock:
            self.calls += 1
        return {"ok": True}


@pytest.mark.integration
def test_db_writer_integration_prefers_async_writer():
    from core.trading_engine import TradingEngine

    bus = FakeBus()
    writer = FakeAsyncWriter()

    engine = TradingEngine(
        bus=bus,
        db=FakeDB(),
        client_getter=lambda: None,
        executor=AsyncExecutor(),
        async_db_writer=writer,
    )
    engine.order_manager = CapturingOrderManager()

    result = engine.submit_quick_bet(
        {
            "market_id": "1.500",
            "selection_id": 50,
            "price": 2.0,
            "stake": 10.0,
            "side": "BACK",
            "customer_ref": "DBW1",
        }
    )

    assert result["ok"] is True
    assert result["status"] == "ACCEPTED_FOR_PROCESSING"

    time.sleep(0.2)

    assert writer.items[0][0] == "bet"
    assert writer.items[0][1]["customer_ref"] == "DBW1"