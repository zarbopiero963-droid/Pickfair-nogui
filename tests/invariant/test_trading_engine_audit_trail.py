import pytest

from core.trading_engine import TradingEngine


class DummyBus:
    def __init__(self):
        self.events = []
        self.subscribers = {}

    def subscribe(self, name, fn):
        self.subscribers[name] = fn

    def publish(self, name, payload):
        self.events.append((name, payload))


class DummyDB:
    def insert_order(self, payload):
        return "OID1"

    def update_order(self, *_):
        pass


def test_audit_trail_is_chained_and_indexed():
    bus = DummyBus()
    db = DummyDB()

    engine = TradingEngine(
        bus=bus,
        db=db,
        client_getter=lambda: None,
        executor=None,
    )

    engine._runtime_state = "READY"

    result = engine.submit_quick_bet(
        {
            "customer_ref": "C1",
            "price": 2.0,
        }
    )

    audit = result["audit"]
    events = audit["events"]

    assert len(events) > 0

    prev_id = None
    for i, ev in enumerate(events):
        assert ev["index"] == i
        assert ev["parent_event_id"] == prev_id
        prev_id = ev["event_id"]