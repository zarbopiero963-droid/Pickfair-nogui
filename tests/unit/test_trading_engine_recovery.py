import pytest


class DummyBus:
    def __init__(self):
        self.subscriptions = {}
        self.events = []

    def subscribe(self, event_name, handler):
        self.subscriptions[event_name] = handler

    def publish(self, event_name, payload=None):
        self.events.append((event_name, payload))


class DummyDB:
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


class RecoveryHook:
    def __init__(self):
        self.calls = 0

    def recover(self):
        self.calls += 1
        return {"ok": True, "reason": None}

    def is_ready(self):
        return True


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


@pytest.mark.unit
@pytest.mark.recovery
def test_engine_can_be_recreated_after_failure():
    from core.trading_engine import TradingEngine

    e1 = TradingEngine(
        bus=DummyBus(),
        db=DummyDB(),
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )
    e2 = TradingEngine(
        bus=DummyBus(),
        db=DummyDB(),
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )

    assert e1 is not None
    assert e2 is not None


@pytest.mark.unit
@pytest.mark.recovery
def test_recover_after_restart_triggers_recovery_and_reconcile():
    from core.trading_engine import TradingEngine

    recovery = RecoveryHook()
    reconcile = ReconcileHook()

    engine = TradingEngine(
        bus=DummyBus(),
        db=DummyDB(),
        client_getter=lambda: None,
        executor=InlineExecutor(),
        reconciliation_engine=reconcile,
        state_recovery=recovery,
    )

    result = engine.recover_after_restart()

    assert result["ok"] is True
    assert result["status"] == "RECOVERY_TRIGGERED"
    assert "ram_synced" in result
    assert recovery.calls == 1
    assert reconcile.calls == 1
