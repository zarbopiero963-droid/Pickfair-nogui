import time

import pytest

from core.reconciliation_engine import ReconciliationEngine, ReasonCode


class FakeDB:
    def get_pending_sagas(self):
        return []

    def persist_decision_log(self, *args, **kwargs):
        pass


class FakeBatchManager:
    def __init__(self):
        self.batch = {"batch_id": "B-CHAOS", "market_id": "1.100", "status": "LIVE"}
        self.legs = [{
            "leg_index": 0,
            "status": "PLACED",
            "customer_ref": "REF-CHAOS",
            "bet_id": "",
            "selection_id": "1",
            "market_id": "1.100",
            "created_at_ts": time.time(),
        }]

    def get_batch(self, batch_id):
        return self.batch if batch_id == self.batch["batch_id"] else None

    def get_batch_legs(self, batch_id):
        return self.legs if batch_id == self.batch["batch_id"] else []

    def update_leg_status(self, *, batch_id, leg_index, status, **kwargs):
        _ = batch_id, kwargs
        self.legs[int(leg_index)]["status"] = status

    def recompute_batch_status(self, _batch_id):
        return self.batch

    def release_runtime_artifacts(self, **kwargs):
        _ = kwargs

    def mark_batch_failed(self, *_args, **_kwargs):
        self.batch["status"] = "FAILED"

    def get_open_batches(self):
        return [self.batch]


class FakeClient:
    def get_current_orders(self, **kwargs):
        _ = kwargs
        return []


@pytest.fixture
def engine():
    return ReconciliationEngine(
        db=FakeDB(),
        batch_manager=FakeBatchManager(),
        client_getter=lambda: FakeClient(),
    )


@pytest.fixture
def batch(engine):
    return {"batch_id": engine.batch_manager.batch["batch_id"]}


def test_no_double_execution(engine, batch):
    leg = engine.batch_manager.get_batch_legs(batch["batch_id"])[0]

    def fake_apply(*args, **kwargs):
        _ = args, kwargs
        return "MATCHED", ReasonCode.EXCHANGE_WINS_MATCHED, "EXCHANGE"

    engine._apply_merge_policy = fake_apply

    engine.reconcile_batch(batch["batch_id"])
    first = leg["status"]

    engine.reconcile_batch(batch["batch_id"])
    second = leg["status"]

    assert first == "MATCHED"
    assert second == "MATCHED"
