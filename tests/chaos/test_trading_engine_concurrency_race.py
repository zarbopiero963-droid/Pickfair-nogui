import threading

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
        self._lock = threading.Lock()
        self._seq = 0

    def insert_order(self, payload):
        with self._lock:
            self._seq += 1
            oid = f"OID{self._seq}"
            self.orders[oid] = dict(payload)
            return oid

    def update_order(self, order_id, update):
        with self._lock:
            self.orders.setdefault(order_id, {})
            self.orders[order_id].update(update)


class CountingOrderManager:
    def __init__(self):
        self.calls = 0
        self.payloads = []
        self._lock = threading.Lock()

    def submit(self, payload):
        with self._lock:
            self.calls += 1
            self.payloads.append(dict(payload))
        return {"bet_id": f"B{self.calls}", "ok": True}


class InlineExecutor:
    def submit(self, _name, fn):
        return fn()


@pytest.mark.chaos
@pytest.mark.integration
def test_same_correlation_id_only_one_execution_under_race():
    bus = DummyBus()
    db = DummyDB()
    om = CountingOrderManager()

    engine = TradingEngine(
        bus=bus,
        db=db,
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )
    engine.order_manager = om
    engine._runtime_state = "READY"

    results = []
    errors = []

    def worker():
        try:
            result = engine.submit_quick_bet(
                {
                    "customer_ref": "RACE-CUST",
                    "correlation_id": "RACE-CID-1",
                    "price": 2.0,
                    "size": 10.0,
                    "side": "BACK",
                }
            )
            results.append(result)
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    assert len(results) == 20

    executed = [r for r in results if r["status"] == "ACCEPTED_FOR_PROCESSING"]
    duplicates = [r for r in results if r["status"] == "DUPLICATE_BLOCKED"]

    assert len(executed) == 1
    assert len(duplicates) == 19
    assert om.calls == 1
    assert len(db.orders) == 1


@pytest.mark.chaos
@pytest.mark.integration
def test_distinct_correlation_ids_all_execute_under_race():
    bus = DummyBus()
    db = DummyDB()
    om = CountingOrderManager()

    engine = TradingEngine(
        bus=bus,
        db=db,
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )
    engine.order_manager = om
    engine._runtime_state = "READY"

    results = []
    errors = []

    def worker(i: int):
        try:
            result = engine.submit_quick_bet(
                {
                    "customer_ref": f"RACE-CUST-{i}",
                    "correlation_id": f"RACE-CID-{i}",
                    "price": 2.0,
                    "size": 10.0,
                    "side": "BACK",
                }
            )
            results.append(result)
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    assert len(results) == 20
    assert all(r["status"] == "ACCEPTED_FOR_PROCESSING" for r in results)
    assert om.calls == 20
    assert len(db.orders) == 20