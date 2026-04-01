import pytest

from core.trading_engine import TradingEngine


class FailingExecutor:
    def submit(self, *_):
        raise TimeoutError("timeout")


class DummyBus:
    def subscribe(self, *_):
        pass

    def publish(self, *_):
        pass


class DummyDB:
    def __init__(self):
        self.data = {}

    def insert_order(self, payload):
        oid = "OID1"
        self.data[oid] = dict(payload)
        return oid

    def update_order(self, oid, update):
        self.data.setdefault(oid, {})
        self.data[oid].update(update)


def test_no_state_loss_on_partial_fail():
    db = DummyDB()

    engine = TradingEngine(
        bus=DummyBus(),
        db=db,
        client_getter=lambda: None,
        executor=FailingExecutor(),
    )

    engine._runtime_state = "READY"

    result = engine.submit_quick_bet(
        {
            "customer_ref": "C1",
            "price": 2.0,
        }
    )

    assert result["status"] == "AMBIGUOUS"

    stored = list(db.data.values())[0]
    assert stored["status"] in ("AMBIGUOUS", "FAILED", "INFLIGHT")