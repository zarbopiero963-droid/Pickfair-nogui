from __future__ import annotations

import pytest

from core.reconciliation_engine import ReconciliationEngine, ReasonCode


class FakeBus:
    def publish(self, name, payload):
        return None


class FakeDB:
    def __init__(self, fail=False):
        self.fail = fail
        self.logs = []

    def persist_decision_log(self, batch_id, entries):
        if self.fail:
            raise RuntimeError("persist failed")
        self.logs.append((batch_id, entries))

    def get_pending_sagas(self):
        return []


class FakeBatchManager:
    def get_batch(self, batch_id):
        return {"batch_id": batch_id, "market_id": "1.1", "status": "LIVE"}

    def get_batch_legs(self, batch_id):
        return [{"leg_index": 0, "status": "UNKNOWN", "customer_ref": "R1", "bet_id": "", "created_at_ts": 0}]

    def recompute_batch_status(self, batch_id):
        return {"batch_id": batch_id, "status": "LIVE"}

    def release_runtime_artifacts(self, **kwargs):
        return None


class FakeClient:
    def get_current_orders(self, market_ids=None):
        return []


def test_decision_log_persisted():
    db = FakeDB()
    eng = ReconciliationEngine(
        db=db,
        bus=FakeBus(),
        batch_manager=FakeBatchManager(),
        client_getter=lambda: FakeClient(),
    )
    result = eng.reconcile_batch("B1")
    assert result["ok"] is True
    assert len(db.logs) >= 1
    assert db.logs[0][0] == "B1"


def test_decision_log_persist_failure_raises():
    db = FakeDB(fail=True)
    eng = ReconciliationEngine(
        db=db,
        bus=FakeBus(),
        batch_manager=FakeBatchManager(),
        client_getter=lambda: FakeClient(),
    )
    with pytest.raises(RuntimeError):
        eng._flush_decision_log("B1")


def test_logged_reason_codes_machine_readable():
    db = FakeDB()
    eng = ReconciliationEngine(
        db=db,
        bus=FakeBus(),
        batch_manager=FakeBatchManager(),
        client_getter=lambda: FakeClient(),
    )
    eng._log_decision(
        batch_id="B1",
        leg_index=0,
        case_classification="LOCAL_INFLIGHT_EXCHANGE_ABSENT",
        reason_code=ReasonCode.RESOLVED_UNKNOWN_TO_FAILED,
        local_status="UNKNOWN",
        exchange_status=None,
        resolved_status="FAILED",
        merge_winner="NONE",
    )
    assert eng.get_decision_log("B1")[0]["reason_code"] == ReasonCode.RESOLVED_UNKNOWN_TO_FAILED.value