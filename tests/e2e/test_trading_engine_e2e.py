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


class CapturingOrderManager:
    def __init__(self):
        self.payloads = []

    def submit(self, payload):
        self.payloads.append(dict(payload))
        return {"status": "SUCCESS", "bet_id": "BET-E2E-1"}


class ReconcileHook:
    def __init__(self):
        self.calls = []

    def is_ready(self):
        return True

    def enqueue(self, **kwargs):
        self.calls.append(kwargs)


@pytest.mark.e2e
def test_full_quick_bet_lifecycle_with_hooks_async():
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

    om = CapturingOrderManager()
    engine.order_manager = om

    payload = {
        "market_id": "1.200",
        "selection_id": 33,
        "price": 1.95,
        "size": 12.0,
        "side": "BACK",
        "customer_ref": "E2E1",
        "event_key": "1.200:33:BACK",
        "simulation_mode": True,
    }

    result = engine.submit_quick_bet(payload)

    assert result["ok"] is True
    assert result["status"] == "ACCEPTED_FOR_PROCESSING"
    assert result["simulation_mode"] is True

    assert len(om.payloads) == 1
    assert len(db.orders) == 1

    names = [x[0] for x in bus.events]
    assert "QUICK_BET_ROUTED" in names