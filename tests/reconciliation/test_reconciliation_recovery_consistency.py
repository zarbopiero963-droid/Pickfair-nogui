from __future__ import annotations

from typing import Any

from core.reconciliation_engine import ReconciliationEngine, ReconcileConfig


class FakeDB:
    def __init__(self):
        self.logs: list[tuple[str, list[dict[str, Any]]]] = []

    def persist_decision_log(self, batch_id, entries):
        self.logs.append((batch_id, entries))

    def get_pending_sagas(self):
        return []

    def get_reconcile_marker(self, batch_id):
        return None

    def set_reconcile_marker(self, batch_id, value):
        return None


class FakeBus:
    def publish(self, name, payload):
        return None


class FakeBatchManager:
    def __init__(self):
        self.batch = {"batch_id": "B1", "market_id": "1.1", "status": "LIVE"}
        self.legs = [{"leg_index": 0, "status": "UNKNOWN", "customer_ref": "R1", "bet_id": "", "created_at_ts": 0}]

    def get_batch(self, batch_id):
        return dict(self.batch)

    def get_batch_legs(self, batch_id):
        return [dict(x) for x in self.legs]

    def update_leg_status(self, batch_id, leg_index, status, bet_id=None, raw_response=None, error_text=None):
        self.legs[leg_index]["status"] = status
        if bet_id:
            self.legs[leg_index]["bet_id"] = bet_id

    def recompute_batch_status(self, batch_id):
        return {"batch_id": batch_id, "status": "LIVE"}

    def release_runtime_artifacts(self, **kwargs):
        return None


class FakeClient:
    def get_current_orders(self, market_ids=None):
        return []


def test_restart_same_input_same_outcome():
    db = FakeDB()
    batch_manager = FakeBatchManager()

    eng1 = ReconciliationEngine(
        db=db,
        bus=FakeBus(),
        batch_manager=batch_manager,
        client_getter=lambda: FakeClient(),
        config=ReconcileConfig(max_convergence_cycles=2, convergence_sleep_secs=0.0),
    )
    r1 = eng1.reconcile_batch("B1")

    eng2 = ReconciliationEngine(
        db=db,
        bus=FakeBus(),
        batch_manager=batch_manager,
        client_getter=lambda: FakeClient(),
        config=ReconcileConfig(max_convergence_cycles=2, convergence_sleep_secs=0.0),
    )
    r2 = eng2.reconcile_batch("B1")

    assert r1["status"] == r2["status"]


def test_repeated_recovery_idempotent():
    db = FakeDB()
    batch_manager = FakeBatchManager()

    eng = ReconciliationEngine(
        db=db,
        bus=FakeBus(),
        batch_manager=batch_manager,
        client_getter=lambda: FakeClient(),
        config=ReconcileConfig(max_convergence_cycles=2, convergence_sleep_secs=0.0),
    )
    r1 = eng.reconcile_batch("B1")
    r2 = eng.reconcile_batch("B1")

    assert r1["ok"] is True
    assert r2["ok"] is True