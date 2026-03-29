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
        self.lock = threading.Lock()

    def place_order(self, payload):
        with self.lock:
            self.payloads.append(dict(payload))
        return {"ok": True}


class ReconcileHook:
    def __init__(self):
        self.calls = []

    def on_order_submitted(self, payload):
        self.calls.append(dict(payload))


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
        executor=AsyncExecutor(),
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

    time.sleep(0.2)

    assert len(om.payloads) == 1
    assert db.saved[0]["customer_ref"] == "REF1"
    assert len(reconcile.calls) == 1

    event_names = [x[0] for x in bus.events]
    assert "QUICK_BET_ROUTED" in event_names
    assert "QUICK_BET_EXECUTION_STARTED" in event_names
    assert "QUICK_BET_EXECUTION_FINISHED" in event_names