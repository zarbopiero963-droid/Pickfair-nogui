import pytest

from core.trading_engine import TradingEngine


class DummyBus:
    def __init__(self):
        self.subscribers = {}
        self.events = []

    def subscribe(self, topic, handler):
        self.subscribers[topic] = handler

    def publish(self, name, payload):
        self.events.append((name, payload))


class DummyDB:
    def __init__(self):
        self.orders = {}
        self.seq = 0

    def insert_order(self, payload):
        self.seq += 1
        oid = f"OID{self.seq}"
        self.orders[oid] = dict(payload)
        return oid

    def update_order(self, order_id, update):
        self.orders.setdefault(order_id, {})
        self.orders[order_id].update(update)

    def order_exists_inflight(self, *, customer_ref, correlation_id):
        return False


class DummyOrderManager:
    def __init__(self):
        self.calls = 0

    def submit(self, payload):
        self.calls += 1
        return {"ok": True, "bet_id": f"B{self.calls}"}


class InlineExecutor:
    def submit(self, _name, fn):
        return fn()


@pytest.mark.invariant
def test_dedup_window_evicts_oldest_correlation_ids():
    bus = DummyBus()
    db = DummyDB()
    om = DummyOrderManager()

    engine = TradingEngine(
        bus=bus,
        db=db,
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )
    engine.order_manager = om
    engine._runtime_state = "READY"

    engine._max_seen_cid_size = 5
    engine._seen_cid_trim_to = 3

    for i in range(6):
        result = engine.submit_quick_bet(
            {
                "customer_ref": f"CUST-{i}",
                "correlation_id": f"CID-{i}",
                "price": 2.0,
            }
        )
        assert result["status"] == "ACCEPTED_FOR_PROCESSING"

    assert len(engine._seen_correlation_ids) <= 5
    assert len(engine._seen_cid_order) <= 5
    assert "CID-0" not in engine._seen_correlation_ids
    assert "CID-5" in engine._seen_correlation_ids


@pytest.mark.invariant
def test_repopulate_does_not_duplicate_seen_cid_deque():
    bus = DummyBus()

    class RepopDB(DummyDB):
        def load_pending_customer_refs(self):
            return ["A", "B"]

        def load_pending_correlation_ids(self):
            return ["CID-A", "CID-B", "CID-A"]

    db = RepopDB()

    engine = TradingEngine(
        bus=bus,
        db=db,
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )

    synced1 = engine._repopulate_inflight_from_db()
    synced2 = engine._repopulate_inflight_from_db()

    assert synced1 is True
    assert synced2 is True

    assert "CID-A" in engine._seen_correlation_ids
    assert "CID-B" in engine._seen_correlation_ids

    order_list = list(engine._seen_cid_order)
    assert order_list.count("CID-A") == 1
    assert order_list.count("CID-B") == 1