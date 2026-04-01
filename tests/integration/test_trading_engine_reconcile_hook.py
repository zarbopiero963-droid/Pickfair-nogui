import pytest

from core.trading_engine import TradingEngine


class DummyReconcile:
    def __init__(self):
        self.called = False

    def enqueue(self, **kwargs):
        self.called = True


class TimeoutExecutor:
    def submit(self, *_):
        raise TimeoutError("timeout")


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


def test_reconcile_called_on_ambiguous():
    reconcile = DummyReconcile()

    engine = TradingEngine(
        bus=DummyBus(),
        db=DummyDB(),
        client_getter=lambda: None,
        executor=TimeoutExecutor(),
        reconciliation_engine=reconcile,
    )

    engine._runtime_state = "READY"

    engine.submit_quick_bet(
        {
            "customer_ref": "C1",
            "price": 2.0,
        }
    )

    assert reconcile.called is True