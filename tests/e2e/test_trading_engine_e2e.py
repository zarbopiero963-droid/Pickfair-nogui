import threading
import time

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
        self.saved = []
        self.lock = threading.Lock()

    def save_bet(self, **kwargs):
        with self.lock:
            self.saved.append(kwargs)


class AsyncExecutor:
    def submit(self, _name, fn=None, *args, **kwargs):
        target = fn if fn is not None else _name
        t = threading.Thread(target=target, args=args, kwargs=kwargs, daemon=True)
        t.start()
        return t


class CapturingOrderManager:
    def __init__(self):
        self.payloads = []
        self.lock = threading.Lock()

    def place_order(self, payload):
        with self.lock:
            self.payloads.append(dict(payload))
        return {"status": "SUCCESS"}


class ReconcileHook:
    def __init__(self):
        self.calls = []

    def on_order_submitted(self, payload):
        self.calls.append(dict(payload))


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
        executor=AsyncExecutor(),
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

    time.sleep(0.2)

    assert len(om.payloads) == 1
    assert len(db.saved) == 1
    assert len(reconcile.calls) == 1

    names = [x[0] for x in bus.events]
    assert "QUICK_BET_ROUTED" in names
    assert "QUICK_BET_EXECUTION_STARTED" in names
    assert "QUICK_BET_EXECUTION_FINISHED" in names