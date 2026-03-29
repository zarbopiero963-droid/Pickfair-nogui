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
        self.saved = []

    def save_bet(self, **kwargs):
        self.saved.append(kwargs)


class SyncExecutor:
    def submit(self, _name, fn, *args, **kwargs):
        return fn(*args, **kwargs)


class CapturingOrderManager:
    def __init__(self):
        self.payloads = []

    def place_order(self, payload):
        self.payloads.append(dict(payload))
        return {"status": "SUCCESS"}


class ReconcileHook:
    def __init__(self):
        self.calls = []

    def on_order_submitted(self, payload):
        self.calls.append(dict(payload))


@pytest.mark.e2e
def test_full_quick_bet_lifecycle_with_hooks():
    from core.trading_engine import TradingEngine

    bus = FakeBus()
    db = FakeDB()
    reconcile = ReconcileHook()

    engine = TradingEngine(
        bus=bus,
        db=db,
        client_getter=lambda: None,
        executor=SyncExecutor(),
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
    assert len(db.saved) == 1
    assert len(reconcile.calls) == 1

    names = [x[0] for x in bus.events]
    assert "QUICK_BET_ROUTED" in names