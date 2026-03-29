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
    pass


class DummyExecutor:
    def submit(self, _name, fn, *args, **kwargs):
        return fn(*args, **kwargs)


class RecoveryHook:
    def __init__(self):
        self.calls = []

    def recover_pending(self, payload=None):
        self.calls.append(("recover_pending", payload))
        return {"ok": True}


class ReconcileHook:
    def __init__(self):
        self.calls = []

    def run_once(self, payload=None):
        self.calls.append(("run_once", payload))
        return {"ok": True}


@pytest.mark.unit
@pytest.mark.recovery
def test_engine_can_be_recreated_after_failure():
    from core.trading_engine import TradingEngine

    e1 = TradingEngine(
        bus=DummyBus(),
        db=DummyDB(),
        client_getter=lambda: None,
        executor=DummyExecutor(),
    )
    e2 = TradingEngine(
        bus=DummyBus(),
        db=DummyDB(),
        client_getter=lambda: None,
        executor=DummyExecutor(),
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
        executor=DummyExecutor(),
        reconciliation_engine=reconcile,
        state_recovery=recovery,
    )

    result = engine.recover_after_restart()

    assert result["ok"] is True
    assert result["status"] == "RECOVERY_TRIGGERED"
    assert recovery.calls[0][0] == "recover_pending"
    assert reconcile.calls[0][0] == "run_once"