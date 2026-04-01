import pytest


class FakeBus:
    def __init__(self):
        self.subscriptions = {}
        self.events = []

    def subscribe(self, topic, handler):
        self.subscriptions[topic] = handler

    def publish(self, name, payload):
        self.events.append((name, payload))


class FakeDB:
    def __init__(self):
        self.orders = {}
        self.audit_events = []
        self.seq = 0

    def is_ready(self):
        return True

    def insert_order(self, payload):
        self.seq += 1
        order_id = f"ORD-{self.seq}"
        self.orders[order_id] = dict(payload)
        return order_id

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
        for row in self.orders.values():
            status = row.get("status")
            if status in {"INFLIGHT", "SUBMITTED", "AMBIGUOUS"}:
                if row.get("customer_ref") == customer_ref or row.get("correlation_id") == correlation_id:
                    return True
        return False


class InlineExecutor:
    def is_ready(self):
        return True

    def submit(self, _name, fn):
        return fn()


class ExplodingOrderManager:
    def submit(self, _payload):
        raise RuntimeError("simulated crash mid submit")


class ReconcileHook:
    def __init__(self):
        self.calls = []

    def is_ready(self):
        return True

    def enqueue(self, **kwargs):
        self.calls.append(kwargs)


@pytest.mark.chaos
def test_crash_mid_submit_goes_failed_and_preserves_audit():
    from core.trading_engine import TradingEngine

    bus = FakeBus()
    db = FakeDB()
    reconcile = ReconcileHook()

    engine = TradingEngine(
        bus=bus,
        db=db,
        client_getter=lambda: None,
        executor=InlineExecutor(),
        reconciliation_engine=reconcile,
    )
    engine.order_manager = ExplodingOrderManager()

    result = engine.submit_quick_bet(
        {
            "market_id": "1.500",
            "selection_id": 99,
            "price": 2.2,
            "stake": 12.0,
            "side": "BACK",
            "customer_ref": "CHAOS-KILL-1",
        }
    )

    assert result["ok"] is False
    assert result["status"] == "FAILED"
    assert result["reason"] == "SUBMIT_FAILED"

    assert len(db.orders) == 1
    order = next(iter(db.orders.values()))
    assert order["status"] == "FAILED"
    assert order["customer_ref"] == "CHAOS-KILL-1"
    assert order["last_error"] == "simulated crash mid submit"
    assert order["finalized"] is True

    event_names = [e["type"] for e in db.audit_events]
    assert "REQUEST_RECEIVED" in event_names
    assert "PERSIST_INFLIGHT" in event_names
    assert "SUBMIT_FAILED" in event_names
    assert "ORDER_TRANSITION" in event_names
    assert "FINAL_FAILURE" in event_names
    assert "FINALIZED" in event_names

    bus_names = [name for name, _ in bus.events]
    assert "QUICK_BET_ROUTED" in bus_names
    assert "QUICK_BET_FAILED" in bus_names

    assert reconcile.calls == []


@pytest.mark.chaos
def test_timeout_mid_submit_goes_ambiguous_and_enqueues_reconcile():
    from core.trading_engine import TradingEngine

    class TimeoutOrderManager:
        def submit(self, _payload):
            raise TimeoutError("broker timeout")

    bus = FakeBus()
    db = FakeDB()
    reconcile = ReconcileHook()

    engine = TradingEngine(
        bus=bus,
        db=db,
        client_getter=lambda: None,
        executor=InlineExecutor(),
        reconciliation_engine=reconcile,
    )
    engine.order_manager = TimeoutOrderManager()

    result = engine.submit_quick_bet(
        {
            "market_id": "1.501",
            "selection_id": 100,
            "price": 2.0,
            "stake": 10.0,
            "side": "BACK",
            "customer_ref": "CHAOS-TIMEOUT-1",
        }
    )

    assert result["ok"] is False
    assert result["status"] == "AMBIGUOUS"
    assert result["ambiguity_reason"] == "SUBMIT_TIMEOUT"

    assert len(db.orders) == 1
    order = next(iter(db.orders.values()))
    assert order["status"] == "AMBIGUOUS"
    assert order["ambiguity_reason"] == "SUBMIT_TIMEOUT"
    assert order["finalized"] is True

    assert len(reconcile.calls) == 1
    assert reconcile.calls[0]["customer_ref"] == "CHAOS-TIMEOUT-1"
    assert reconcile.calls[0]["ambiguity_reason"] == "SUBMIT_TIMEOUT"

    bus_names = [name for name, _ in bus.events]
    assert "QUICK_BET_ROUTED" in bus_names
    assert "QUICK_BET_AMBIGUOUS" in bus_names