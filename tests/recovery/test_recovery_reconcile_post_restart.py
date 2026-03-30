import threading
import time

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
    def get_pending_sagas(self):
        return []


class ReconcileHook:
    def __init__(self):
        self.calls = []

    def run_once(self, payload=None):
        self.calls.append(payload)
        return {"ok": True, "reconciled": 3}


class RecoveryHook:
    def __init__(self):
        self.calls = []

    def recover_pending(self, payload=None):
        self.calls.append(payload)
        return {"ok": True, "recovered": 2}


class AsyncExecutor:
    def submit(self, _name, fn=None, *args, **kwargs):
        target = fn if fn is not None else _name
        t = threading.Thread(target=target, args=args, kwargs=kwargs, daemon=True)
        t.start()
        return t


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
        executor=AsyncExecutor(),
        reconciliation_engine=reconcile,
        state_recovery=recovery,
    )

    result = engine.recover_after_restart()

    assert result["ok"] is True

    time.sleep(0.2)

    assert len(recovery.calls) == 1
    assert len(reconcile.calls) == 1
    assert recovery.calls[0]["source"] == "recover_after_restart"
    assert reconcile.calls[0]["source"] == "recover_after_restart"