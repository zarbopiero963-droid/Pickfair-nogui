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
        self.outbox = []
        self.saved_bets = []

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
        self.sagas[customer_ref] = {
            "customer_ref": customer_ref,
            "batch_id": batch_id,
            "event_key": event_key,
            "table_id": table_id,
            "market_id": market_id,
            "selection_id": selection_id,
            "bet_type": bet_type,
            "price": price,
            "stake": stake,
            "payload": payload,
            "status": status,
            "bet_id": "",
            "error_text": "",
        }

    def update_order_saga(self, *, customer_ref, status, bet_id="", error_text=""):
        if customer_ref in self.sagas:
            self.sagas[customer_ref]["status"] = status
            self.sagas[customer_ref]["bet_id"] = bet_id
            self.sagas[customer_ref]["error_text"] = error_text

    def get_pending_sagas(self):
        pending = []
        for row in self.sagas.values():
            if row["status"] in {"PENDING", "ACCEPTED", "EXECUTING", "ROLLBACK_REQUIRED"}:
                pending.append(dict(row))
        return pending

    def save_outbox_event(self, **kwargs):
        self.outbox.append(kwargs)

    def save_bet(self, **kwargs):
        self.saved_bets.append(kwargs)


class AsyncExecutor:
    def submit(self, _name, fn=None, *args, **kwargs):
        target = fn if fn is not None else _name
        t = threading.Thread(target=target, args=args, kwargs=kwargs, daemon=True)
        t.start()
        return t


class ExplodingOrderManager:
    def place_order(self, payload):
        raise RuntimeError("mid-order crash")


@pytest.mark.recovery
@pytest.mark.failure
def test_crash_mid_order_marks_saga_and_publishes_recovery_events():
    from core.trading_engine import TradingEngine

    bus = FakeBus()
    db = FakeDB()

    engine = TradingEngine(
        bus=bus,
        db=db,
        client_getter=lambda: None,
        executor=AsyncExecutor(),
    )
    engine.order_manager = ExplodingOrderManager()

    result = engine.submit_quick_bet(
        {
            "market_id": "1.111",
            "selection_id": 10,
            "price": 2.0,
            "stake": 5.0,
            "side": "BACK",
            "customer_ref": "CRASH-1",
        }
    )

    assert result["ok"] is True
    assert result["status"] == "ACCEPTED_FOR_PROCESSING"

    time.sleep(0.2)

    assert db.sagas["CRASH-1"]["status"] == "FAILED"
    assert "mid-order crash" in db.sagas["CRASH-1"]["error_text"]

    names = [x[0] for x in bus.events]
    assert "QUICK_BET_ROUTED" in names
    assert "QUICK_BET_ROLLBACK_REQUIRED" in names
    assert "QUICK_BET_FAILED" in names