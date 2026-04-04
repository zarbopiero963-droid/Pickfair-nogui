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
        self.sagas = {}

    def create_order_saga(
        self,
        *,
        customer_ref,
        batch_id,
        event_key,
        table_id,
        market_id,
        selection_id,
        bet_type,
        price,
        stake,
        payload,
        status="PENDING",
    ):
        self.sagas[customer_ref] = {"status": status, "customer_ref": customer_ref}

    def update_order_saga(self, *, customer_ref, status, bet_id="", error_text=""):
        if customer_ref in self.sagas:
            self.sagas[customer_ref]["status"] = status
            self.sagas[customer_ref]["bet_id"] = bet_id
            self.sagas[customer_ref]["error_text"] = error_text

    def get_pending_sagas(self):
        return []


class AsyncExecutor:
    def submit(self, _name, fn=None, *args, **kwargs):
        target = fn if fn is not None else _name
        t = threading.Thread(target=target, args=args, kwargs=kwargs, daemon=True)
        t.start()
        return t


class FastOrderManager:
    def place_order(self, payload):
        return {"status": "SUCCESS", "instructionReports": [{"betId": "BET-OK"}]}


@pytest.mark.recovery
@pytest.mark.invariant
def test_no_duplicate_order_after_restart_when_key_already_restored():
    from core.trading_engine import TradingEngine

    bus = FakeBus()
    db = FakeDB()
    engine = TradingEngine(
        bus=bus,
        db=db,
        client_getter=lambda: None,
        executor=AsyncExecutor(),
    )
    engine.order_manager = FastOrderManager()

    with engine._lock:
        engine._inflight_keys.add("DUP-REC")

    result = engine.submit_quick_bet(
        {
            "market_id": "1.333",
            "selection_id": 7,
            "price": 2.0,
            "stake": 4.0,
            "side": "BACK",
            "customer_ref": "DUP-REC",
        }
    )

    assert result["status"] == "DUPLICATE_BLOCKED"


@pytest.mark.recovery
@pytest.mark.invariant
def test_non_normalized_async_executor_keeps_inflight_key():
    from core.trading_engine import TradingEngine

    bus = FakeBus()
    db = FakeDB()
    engine = TradingEngine(
        bus=bus,
        db=db,
        client_getter=lambda: None,
        executor=AsyncExecutor(),
    )
    engine.order_manager = FastOrderManager()

    result = engine.submit_quick_bet(
        {
            "market_id": "1.444",
            "selection_id": 8,
            "price": 2.0,
            "stake": 4.0,
            "side": "BACK",
            "customer_ref": "REL-OK",
        }
    )

    assert result["ok"] is True

    time.sleep(0.2)

    with engine._lock:
        assert "REL-OK" in engine._inflight_keys