from __future__ import annotations

import random

import pytest

from core.reconciliation_engine import ReconciliationEngine, ReconcileConfig


class FakeDB:
    def get_pending_sagas(self):
        return []

    def persist_decision_log(self, batch_id, entries):
        return None

    def get_reconcile_marker(self, batch_id):
        return None

    def set_reconcile_marker(self, batch_id, value):
        return None


class FakeBus:
    def publish(self, name, payload):
        return None


class FakeBatchManager:
    def __init__(self, legs):
        self.batch = {"batch_id": "B1", "market_id": "1.111", "status": "LIVE"}
        self._legs = [dict(x) for x in legs]

    def get_batch(self, batch_id):
        return dict(self.batch)

    def get_batch_legs(self, batch_id):
        return [dict(x) for x in self._legs]

    def update_leg_status(
        self,
        batch_id,
        leg_index,
        status,
        bet_id=None,
        raw_response=None,
        error_text=None,
    ):
        for leg in self._legs:
            if int(leg.get("leg_index", -1)) == int(leg_index):
                leg["status"] = status
                if bet_id is not None:
                    leg["bet_id"] = bet_id
                if raw_response is not None:
                    leg["raw_response"] = raw_response
                if error_text is not None:
                    leg["error_text"] = error_text
                return
        raise AssertionError(f"leg_index {leg_index} not found")

    def recompute_batch_status(self, batch_id):
        return {"batch_id": batch_id, "status": "LIVE"}

    def release_runtime_artifacts(self, **kwargs):
        return None


class FakeClient:
    def __init__(self, orders):
        self.orders = orders

    def get_current_orders(self, market_ids=None):
        return [dict(x) for x in self.orders]


@pytest.mark.parametrize("seed", [1, 2, 3, 4, 5])
def test_same_state_same_result(seed):
    legs = [
        {"leg_index": 1, "status": "PLACED", "customer_ref": "B", "bet_id": "BID2"},
        {"leg_index": 0, "status": "UNKNOWN", "customer_ref": "A", "bet_id": "BID1", "created_at_ts": 0},
    ]
    orders = [
        {"customerOrderRef": "A", "betId": "BID1", "status": "EXECUTION_COMPLETE", "sizeMatched": 10, "sizeRemaining": 0},
        {"customerOrderRef": "B", "betId": "BID2", "status": "EXECUTABLE", "sizeMatched": 0, "sizeRemaining": 10},
    ]
    random.Random(seed).shuffle(legs)
    random.Random(seed + 100).shuffle(orders)

    eng = ReconciliationEngine(
        db=FakeDB(),
        bus=FakeBus(),
        batch_manager=FakeBatchManager(legs),
        client_getter=lambda: FakeClient(orders),
        config=ReconcileConfig(max_convergence_cycles=2, convergence_sleep_secs=0.0),
    )
    r1 = eng.reconcile_batch("B1")
    r2 = eng.reconcile_batch("B1")

    assert r1["ok"] is True
    assert r2["ok"] is True
    assert r1["status"] == r2["status"]


def test_fingerprint_independent_from_input_order():
    eng = ReconciliationEngine(
        db=FakeDB(),
        bus=FakeBus(),
        batch_manager=FakeBatchManager([]),
        client_getter=lambda: FakeClient([]),
    )

    legs1 = [
        {"leg_index": 1, "status": "PLACED", "customer_ref": "B", "bet_id": "2"},
        {"leg_index": 0, "status": "PLACED", "customer_ref": "A", "bet_id": "1"},
    ]
    legs2 = list(reversed(legs1))

    remote1 = [
        {"customerOrderRef": "B", "status": "EXECUTABLE", "sizeMatched": 0, "sizeRemaining": 10},
        {"customerOrderRef": "A", "status": "EXECUTION_COMPLETE", "sizeMatched": 10, "sizeRemaining": 0},
    ]
    remote2 = list(reversed(remote1))

    fp1 = eng._compute_fingerprint(legs1, remote1)
    fp2 = eng._compute_fingerprint(legs2, remote2)

    assert fp1 == fp2