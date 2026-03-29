import threading
import time

import pytest


class FakeBus:
    def __init__(self):
        self.events = []
        self.subscriptions = {}
        self.lock = threading.Lock()

    def subscribe(self, event_name, handler):
        self.subscriptions[event_name] = handler

    def publish(self, event_name, payload=None):
        with self.lock:
            self.events.append((event_name, payload))


class FakeDB:
    pass


class AsyncExecutor:
    def submit(self, _name, fn=None, *args, **kwargs):
        target = fn if fn is not None else _name
        t = threading.Thread(target=target, args=args, kwargs=kwargs, daemon=True)
        t.start()
        return t


class SafeModeOn:
    def is_enabled(self):
        return True


class RiskBlocker:
    def allow(self, payload):
        return False


class RiskMutator:
    def process_request(self, payload):
        out = dict(payload)
        out["stake"] = 12.5
        return out


class CapturingOrderManager:
    def __init__(self):
        self.payloads = []
        self.lock = threading.Lock()

    def place_order(self, payload):
        with self.lock:
            self.payloads.append(dict(payload))
        return {"ok": True}


@pytest.mark.unit
@pytest.mark.failure
def test_safe_mode_blocks_order():
    from core.trading_engine import TradingEngine

    bus = FakeBus()
    engine = TradingEngine(
        bus=bus,
        db=FakeDB(),
        client_getter=lambda: None,
        executor=AsyncExecutor(),
        safe_mode=SafeModeOn(),
    )

    result = engine.submit_quick_bet(
        {
            "market_id": "1.400",
            "selection_id": 44,
            "price": 2.0,
            "stake": 5.0,
            "side": "BACK",
        }
    )

    assert result["ok"] is False
    assert result["status"] == "FAILED"
    assert any(name == "QUICK_BET_FAILED" for name, _ in bus.events)


@pytest.mark.unit
@pytest.mark.failure
def test_risk_middleware_can_block_order():
    from core.trading_engine import TradingEngine

    bus = FakeBus()
    engine = TradingEngine(
        bus=bus,
        db=FakeDB(),
        client_getter=lambda: None,
        executor=AsyncExecutor(),
        risk_middleware=RiskBlocker(),
    )

    result = engine.submit_quick_bet(
        {
            "market_id": "1.401",
            "selection_id": 45,
            "price": 2.0,
            "stake": 5.0,
            "side": "BACK",
        }
    )

    assert result["ok"] is False
    assert result["status"] == "FAILED"


@pytest.mark.unit
@pytest.mark.invariant
def test_risk_middleware_can_mutate_payload_before_async_order():
    from core.trading_engine import TradingEngine

    bus = FakeBus()
    order_manager = CapturingOrderManager()

    engine = TradingEngine(
        bus=bus,
        db=FakeDB(),
        client_getter=lambda: None,
        executor=AsyncExecutor(),
        risk_middleware=RiskMutator(),
    )
    engine.order_manager = order_manager

    result = engine.submit_quick_bet(
        {
            "market_id": "1.402",
            "selection_id": 46,
            "price": 2.0,
            "stake": 5.0,
            "side": "BACK",
            "customer_ref": "RM1",
        }
    )

    assert result["ok"] is True
    assert result["status"] == "ACCEPTED_FOR_PROCESSING"

    time.sleep(0.2)

    assert order_manager.payloads[0]["stake"] == 12.5