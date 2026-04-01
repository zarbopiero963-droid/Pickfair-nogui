import threading

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
    def __init__(self):
        self.orders = {}
        self.audit_events = []
        self.seq = 0
        self.lock = threading.Lock()

    def is_ready(self):
        return True

    def insert_order(self, payload):
        with self.lock:
            self.seq += 1
            oid = f"ORD-{self.seq}"
            self.orders[oid] = dict(payload)
            return oid

    def update_order(self, order_id, update):
        with self.lock:
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


class CountingOrderManager:
    def __init__(self):
        self.calls = 0
        self.lock = threading.Lock()

    def submit(self, payload):
        with self.lock:
            self.calls += 1
        return {"ok": True, "bet_id": f"BET-{self.calls}"}


@pytest.mark.chaos
@pytest.mark.core
@pytest.mark.concurrency
def test_many_parallel_quick_bets_do_not_crash_engine():
    from core.trading_engine import TradingEngine

    bus = FakeBus()
    db = FakeDB()

    engine = TradingEngine(
        bus=bus,
        db=db,
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )
    om = CountingOrderManager()
    engine.order_manager = om

    def worker(i):
        payload = {
            "market_id": f"1.{100+i}",
            "selection_id": i + 1,
            "price": 2.0,
            "size": 5.0,
            "side": "BACK",
            "customer_ref": f"C{i}",
            "event_key": f"1.{100+i}:{i}:BACK",
        }
        result = engine.submit_quick_bet(payload)
        assert result["ok"] is True
        assert result["status"] == "ACCEPTED_FOR_PROCESSING"

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert om.calls == 20
    assert len(db.orders) == 20


@pytest.mark.chaos
@pytest.mark.core
@pytest.mark.concurrency
def test_duplicate_burst_only_one_inflight_accepts_when_same_key_locked():
    from core.trading_engine import TradingEngine

    bus = FakeBus()
    db = FakeDB()

    engine = TradingEngine(
        bus=bus,
        db=db,
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )
    om = CountingOrderManager()
    engine.order_manager = om

    payload = {
        "market_id": "1.900",
        "selection_id": 90,
        "price": 2.0,
        "size": 5.0,
        "side": "BACK",
        "customer_ref": "SAMEKEY",
        "correlation_id": "SAME-CID",
        "event_key": "1.900:90:BACK",
    }

    with engine._lock:
        engine._inflight_keys.add("SAMEKEY")
        engine._seen_correlation_ids.add("SAME-CID")
        engine._seen_cid_order.append("SAME-CID")

    results = []

    def worker():
        results.append(engine.submit_quick_bet(payload))

    threads = [threading.Thread(target=worker) for _ in range(10)]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert all(r["status"] == "DUPLICATE_BLOCKED" for r in results)
    assert om.calls == 0
