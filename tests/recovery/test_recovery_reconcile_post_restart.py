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


class ReconcileHook:
    def __init__(self):
        self.calls = 0

    def notify_restart(self):
        self.calls += 1
        return {"triggered": True}

    def is_ready(self):
        return True

    def enqueue(self, **kwargs):
        pass


class RecoveryHook:
    def __init__(self):
        self.calls = 0

    def recover(self):
        self.calls += 1
        return {"ok": True, "reason": None}

    def is_ready(self):
        return True


@pytest.mark.recovery
@pytest.mark.integration
def test_restart_triggers_recovery_then_reconcile_hooks():
    from core.trading_engine import TradingEngine

    bus = FakeBus()
    db = FakeDB()
    reconcile = ReconcileHook()
    recovery = RecoveryHook()

    engine = TradingEngine(
        bus=bus,
        db=db,
        client_getter=lambda: None,
        executor=InlineExecutor(),
        reconciliation_engine=reconcile,
        state_recovery=recovery,
    )

    result = engine.recover_after_restart()

    assert result["ok"] is True
    assert result["status"] == "RECOVERY_TRIGGERED"
    assert recovery.calls == 1
    assert reconcile.calls == 1
