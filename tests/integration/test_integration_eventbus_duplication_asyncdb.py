import time

import pytest

from core.async_db_writer import AsyncDBWriter
from core.duplication_guard import DuplicationGuard
from core.event_bus import EventBus


class DummyDB:
    def __init__(self):
        self.saved_bets = []

    def save_bet(self, **payload):
        self.saved_bets.append(payload)

    def save_cashout_transaction(self, **payload):
        raise AssertionError("non previsto in questo test")

    def save_simulation_bet(self, **payload):
        raise AssertionError("non previsto in questo test")


def wait_until(condition, timeout=3.0, interval=0.02):
    start = time.time()
    while time.time() - start < timeout:
        if condition():
            return True
        time.sleep(interval)
    return False


@pytest.mark.integration
def test_eventbus_duplicationguard_asyncdb_work_together():
    bus = EventBus()
    guard = DuplicationGuard()
    db = DummyDB()
    writer = AsyncDBWriter(db)
    writer.start()

    processed = []

    def on_quick_bet(payload):
        key = guard.build_event_key(payload)

        if hasattr(guard, "acquire"):
            acquired = guard.acquire(key)
            if not acquired:
                return
        else:
            if guard.is_duplicate(key):
                return
            guard.register(key)

        processed.append(payload)
        writer.submit("bet", payload)

    bus.subscribe("REQ_QUICK_BET", on_quick_bet)

    payload = {
        "market_id": "1.123",
        "selection_id": 456,
        "bet_type": "BACK",
        "stake": 10,
    }

    try:
        bus.publish("REQ_QUICK_BET", payload)
        bus.publish("REQ_QUICK_BET", payload)

        ok = wait_until(lambda: len(processed) == 1 and len(db.saved_bets) == 1)
        assert ok, (
            "integrazione EventBus + DuplicationGuard + AsyncDBWriter: "
            "un ordine duplicato deve essere bloccato e un solo bet deve essere scritto"
        )
    finally:
        writer.stop()
