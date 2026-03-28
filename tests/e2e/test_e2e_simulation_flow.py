import time

import pytest

from core.async_db_writer import AsyncDBWriter
from core.duplication_guard import DuplicationGuard
from core.event_bus import EventBus


class FakeSimulationDB:
    def __init__(self):
        self.saved_bets = []

    def save_bet(self, **payload):
        self.saved_bets.append(payload)

    def save_cashout_transaction(self, **payload):
        raise AssertionError("non previsto in questo test")

    def save_simulation_bet(self, **payload):
        self.saved_bets.append(payload)


class FakeSimulationBroker:
    def __init__(self):
        self.placed_orders = []

    def place_bet(self, **payload):
        self.placed_orders.append(payload)
        return {"status": "SUCCESS", "bet_id": "SIM-1"}


def wait_until(condition, timeout=3.0, interval=0.02):
    start = time.time()
    while time.time() - start < timeout:
        if condition():
            return True
        time.sleep(interval)
    return False


@pytest.mark.e2e
def test_fake_signal_to_simulation_order_to_db_write():
    bus = EventBus()
    guard = DuplicationGuard()
    db = FakeSimulationDB()
    writer = AsyncDBWriter(db)
    broker = FakeSimulationBroker()

    writer.start()

    def trading_engine_handler(payload):
        key = guard.build_event_key(payload)

        if hasattr(guard, "acquire"):
            if not guard.acquire(key):
                return
        else:
            if guard.is_duplicate(key):
                return
            guard.register(key)

        result = broker.place_bet(**payload)
        writer.submit(
            "simulation_bet",
            {
                "market_id": payload["market_id"],
                "selection_id": payload["selection_id"],
                "bet_type": payload["bet_type"],
                "stake": payload["stake"],
                "result_status": result["status"],
            },
        )

    bus.subscribe("REQ_QUICK_BET", trading_engine_handler)

    fake_telegram_order = {
        "market_id": "1.555",
        "selection_id": 999,
        "bet_type": "BACK",
        "stake": 15,
    }

    try:
        bus.publish("REQ_QUICK_BET", fake_telegram_order)

        ok = wait_until(lambda: len(broker.placed_orders) == 1 and len(db.saved_bets) == 1)
        assert ok, (
            "E2E simulation: il segnale deve produrre un ordine simulato "
            "e una scrittura DB coerente"
        )

        placed = broker.placed_orders[0]
        saved = db.saved_bets[0]

        assert placed["market_id"] == "1.555"
        assert placed["selection_id"] == 999
        assert saved["market_id"] == "1.555"
        assert saved["selection_id"] == 999
    finally:
        writer.stop()
