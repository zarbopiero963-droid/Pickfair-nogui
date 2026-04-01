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


class FakeAsyncWriter:
    def __init__(self):
        self.items = []

    def is_ready(self):
        return True

    def write(self, event):
        self.items.append(event)


class CapturingOrderManager:
    def __init__(self):
        self.calls = 0

    def submit(self, payload):
        self.calls += 1
        return {"ok": True, "bet_id": f"BET-{self.calls}"}


@pytest.mark.integration
def test_db_writer_integration_prefers_async_writer():
    from core.trading_engine import TradingEngine

    bus = FakeBus()
    writer = FakeAsyncWriter()

    engine = TradingEngine(
        bus=bus,
        db=FakeDB(),
        client_getter=lambda: None,
        executor=InlineExecutor(),
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

    # async_db_writer.write() receives audit events
    assert len(writer.items) > 0
    assert writer.items[0]["correlation_id"] is not None
