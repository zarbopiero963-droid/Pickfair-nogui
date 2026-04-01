import pytest

from core.trading_engine import TradingEngine


class DummyBus:
    def __init__(self):
        self.subscribers = {}
        self.events = []

    def subscribe(self, topic, handler):
        self.subscribers[topic] = handler

    def publish(self, name, payload):
        self.events.append((name, payload))


class RecoveryHook:
    def __init__(self, ok=True):
        self.ok = ok
        self.calls = 0

    def recover(self):
        self.calls += 1
        return {"ok": self.ok, "reason": None}


class ReconcileHook:
    def __init__(self):
        self.calls = 0

    def notify_restart(self):
        self.calls += 1
        return {"triggered": True}


class DummyDB:
    def __init__(self):
        self.pending_refs = ["REC-CUST-1", "REC-CUST-2"]
        self.pending_cids = ["REC-CID-1", "REC-CID-2"]

    def load_pending_customer_refs(self):
        return list(self.pending_refs)

    def load_pending_correlation_ids(self):
        return list(self.pending_cids)

    def order_exists_inflight(self, *, customer_ref, correlation_id):
        return customer_ref in self.pending_refs or correlation_id in self.pending_cids


class InlineExecutor:
    def submit(self, _name, fn):
        return fn()


@pytest.mark.recovery
@pytest.mark.invariant
def test_recovery_repopulates_ram_and_returns_ram_synced():
    bus = DummyBus()
    recovery = RecoveryHook(ok=True)
    reconcile = ReconcileHook()
    db = DummyDB()

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
    assert result["ram_synced"] is True
    assert recovery.calls == 1
    assert reconcile.calls == 1

    assert "REC-CUST-1" in engine._inflight_keys
    assert "REC-CUST-2" in engine._inflight_keys
    assert "REC-CID-1" in engine._seen_correlation_ids
    assert "REC-CID-2" in engine._seen_correlation_ids


@pytest.mark.recovery
@pytest.mark.invariant
def test_recovered_pending_request_is_blocked_by_dedup():
    bus = DummyBus()
    recovery = RecoveryHook(ok=True)
    reconcile = ReconcileHook()
    db = DummyDB()

    engine = TradingEngine(
        bus=bus,
        db=db,
        client_getter=lambda: None,
        executor=InlineExecutor(),
        reconciliation_engine=reconcile,
        state_recovery=recovery,
    )
    engine._runtime_state = "READY"

    engine.recover_after_restart()

    result = engine.submit_quick_bet(
        {
            "customer_ref": "REC-CUST-1",
            "correlation_id": "NEW-CID",
            "price": 2.0,
        }
    )
    assert result["ok"] is True
    assert result["status"] == "DUPLICATE_BLOCKED"

    result2 = engine.submit_quick_bet(
        {
            "customer_ref": "NEW-CUST",
            "correlation_id": "REC-CID-1",
            "price": 2.0,
        }
    )
    assert result2["ok"] is True
    assert result2["status"] == "DUPLICATE_BLOCKED"


@pytest.mark.recovery
def test_recovery_failure_is_reported_correctly():
    bus = DummyBus()
    recovery = RecoveryHook(ok=False)
    reconcile = ReconcileHook()
    db = DummyDB()

    engine = TradingEngine(
        bus=bus,
        db=db,
        client_getter=lambda: None,
        executor=InlineExecutor(),
        reconciliation_engine=reconcile,
        state_recovery=recovery,
    )

    result = engine.recover_after_restart()

    assert result["ok"] is False
    assert result["status"] == "RECOVERY_FAILED"
    assert result["ram_synced"] is True