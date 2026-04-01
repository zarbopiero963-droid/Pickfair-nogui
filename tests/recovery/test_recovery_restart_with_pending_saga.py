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

    def is_ready(self):
        return True

    def load_pending_customer_refs(self):
        return ["PEND-1", "PEND-2"]

    def load_pending_correlation_ids(self):
        return ["CID-PEND-1", "CID-PEND-2"]

    def order_exists_inflight(self, *, customer_ref, correlation_id):
        return False


class InlineExecutor:
    def is_ready(self):
        return True

    def submit(self, _name, fn):
        return fn()


class RecoveryHook:
    def recover(self):
        return {"ok": True, "reason": None}

    def is_ready(self):
        return True


@pytest.mark.recovery
def test_restart_restores_inflight_keys_from_pending_sagas():
    from core.trading_engine import TradingEngine

    bus = FakeBus()
    db = FakeDB()

    engine = TradingEngine(
        bus=bus,
        db=db,
        client_getter=lambda: None,
        executor=InlineExecutor(),
        state_recovery=RecoveryHook(),
    )

    result = engine.recover_after_restart()

    assert result["ok"] is True
    assert result["status"] == "RECOVERY_TRIGGERED"
    assert result["ram_synced"] is True

    with engine._lock:
        assert "PEND-1" in engine._inflight_keys
        assert "PEND-2" in engine._inflight_keys
