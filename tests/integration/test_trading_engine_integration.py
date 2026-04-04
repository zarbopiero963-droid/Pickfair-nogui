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

    def get_order(self, order_id):
        return self.orders.get(order_id)


class InlineExecutor:
    def is_ready(self):
        return True

    def submit(self, _name, fn):
        return fn()


class FakeClient:
    def place_bet(self, **kwargs):
        return {
            "status": "SUCCESS",
            "marketId": kwargs.get("market_id", ""),
            "instructionReports": [{"status": "SUCCESS", "betId": "BET1"}],
            "simulated": False,
        }


class CapturingOrderManager:
    def __init__(self):
        self.payloads = []

    def submit(self, payload):
        self.payloads.append(dict(payload))
        return {"ok": True, "bet_id": "BET-INT-1"}


class ReconcileHook:
    def __init__(self):
        self.calls = []

    def is_ready(self):
        return True

    def enqueue(self, **kwargs):
        self.calls.append(kwargs)


@pytest.mark.integration
def test_quick_bet_happy_path_routes_logs_and_reconciles_async():
    from core.trading_engine import TradingEngine

    bus = FakeBus()
    db = FakeDB()
    reconcile = ReconcileHook()

    engine = TradingEngine(
        bus=bus,
        db=db,
        client_getter=lambda: FakeClient(),
        executor=InlineExecutor(),
        reconciliation_engine=reconcile,
    )

    om = CapturingOrderManager()
    engine.order_manager = om

    handler = bus.subscriptions["REQ_QUICK_BET"]

    payload = {
        "market_id": "1.100",
        "selection_id": 10,
        "price": 2.0,
        "size": 10.0,
        "side": "BACK",
        "customer_ref": "REF1",
        "event_key": "1.100:10:BACK",
    }

    result = handler(payload)

    assert result["ok"] is True
    assert result["status"] == "ACCEPTED_FOR_PROCESSING"
    assert result["is_terminal"] is False
    assert result["lifecycle_stage"] == "accepted"

    assert len(om.payloads) == 1
    assert len(db.orders) == 1

    order = next(iter(db.orders.values()))
    assert order["customer_ref"] == "REF1"

    event_names = [x[0] for x in bus.events]
    assert "QUICK_BET_ROUTED" in event_names
    assert "QUICK_BET_SUCCESS" not in event_names