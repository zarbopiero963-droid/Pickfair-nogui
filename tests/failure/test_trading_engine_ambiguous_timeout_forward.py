import pytest

from core.trading_engine import TradingEngine


class TimeoutExecutor:
    def submit(self, *_):
        raise TimeoutError("submit timeout")


class DummyBus:
    def subscribe(self, *_):
        pass

    def publish(self, *_):
        pass


class DummyDB:
    def insert_order(self, payload):
        return "OID1"

    def update_order(self, *_):
        pass


def test_timeout_generates_ambiguous_with_reason():
    engine = TradingEngine(
        bus=DummyBus(),
        db=DummyDB(),
        client_getter=lambda: None,
        executor=TimeoutExecutor(),
    )

    engine._runtime_state = "READY"

    result = engine.submit_quick_bet(
        {
            "customer_ref": "C1",
            "price": 2.0,
        }
    )

    assert result["status"] == "AMBIGUOUS"
    assert result["ambiguity_reason"] == "SUBMIT_TIMEOUT"