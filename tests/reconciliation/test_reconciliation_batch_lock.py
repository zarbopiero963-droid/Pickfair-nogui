from __future__ import annotations

import threading
import time
from typing import Any, Dict, List

import pytest

from core.reconciliation_engine import ReconciliationEngine, ReconcileConfig, ReasonCode


class FakeBus:
    def __init__(self):
        self.events: List[tuple[str, Dict[str, Any]]] = []

    def publish(self, name: str, payload: Dict[str, Any]) -> None:
        self.events.append((name, dict(payload)))


class FakeDB:
    def __init__(self):
        self.persisted_decisions: List[tuple[str, List[Dict[str, Any]]]] = []

    def get_pending_sagas(self):
        return []

    def persist_decision_log(self, batch_id: str, entries: List[Dict[str, Any]]) -> None:
        self.persisted_decisions.append((batch_id, entries))

    def get_reconcile_marker(self, batch_id):
        return None

    def set_reconcile_marker(self, batch_id, value):
        return None


class FakeBatchManager:
    def __init__(self):
        self.batches = {
            "B1": {"batch_id": "B1", "market_id": "1.100", "status": "LIVE"},
            "B2": {"batch_id": "B2", "market_id": "1.200", "status": "LIVE"},
        }
        self.legs = {
            "B1": [{"leg_index": 0, "status": "PLACED", "customer_ref": "R1", "bet_id": "BET1"}],
            "B2": [{"leg_index": 0, "status": "PLACED", "customer_ref": "R2", "bet_id": "BET2"}],
        }
        self.calls: List[str] = []
        self.pause = 0.15

    def get_batch(self, batch_id: str):
        return self.batches.get(batch_id)

    def get_batch_legs(self, batch_id: str):
        self.calls.append(f"legs:{batch_id}")
        time.sleep(self.pause)
        return [dict(x) for x in self.legs.get(batch_id, [])]

    def recompute_batch_status(self, batch_id: str):
        return {"batch_id": batch_id, "status": "LIVE"}

    def get_open_batches(self):
        return list(self.batches.values())

    def release_runtime_artifacts(self, **kwargs):
        return None

    def update_leg_status(
        self,
        batch_id: str,
        leg_index: int,
        status: str,
        bet_id=None,
        raw_response=None,
        error_text=None,
    ):
        for leg in self.legs.get(batch_id, []):
            if int(leg.get("leg_index", -1)) == int(leg_index):
                leg["status"] = status
                if bet_id is not None:
                    leg["bet_id"] = bet_id
                if error_text is not None:
                    leg["error_text"] = error_text
                if raw_response is not None:
                    leg["raw_response"] = raw_response
                return
        raise AssertionError(f"leg {leg_index} not found in batch {batch_id}")


class FakeClient:
    def get_current_orders(self, market_ids=None):
        return []


@pytest.fixture
def engine():
    db = FakeDB()
    bus = FakeBus()
    batch_manager = FakeBatchManager()
    eng = ReconciliationEngine(
        db=db,
        bus=bus,
        batch_manager=batch_manager,
        client_getter=lambda: FakeClient(),
        config=ReconcileConfig(max_convergence_cycles=2, convergence_sleep_secs=0.0),
    )
    return eng


def test_same_batch_only_one_reconcile_enters(engine):
    results: List[Dict[str, Any]] = []

    def run():
        results.append(engine.reconcile_batch("B1"))

    t1 = threading.Thread(target=run)
    t2 = threading.Thread(target=run)

    t1.start()
    time.sleep(0.03)
    t2.start()
    t1.join()
    t2.join()

    assert len(results) == 2
    reason_codes = {r.get("reason_code") for r in results}
    assert ReasonCode.CONVERGED.value in reason_codes
    assert ReasonCode.RECONCILE_ALREADY_RUNNING.value in reason_codes


def test_different_batches_can_run_in_parallel(engine):
    results: List[Dict[str, Any]] = []

    def run(batch_id: str):
        results.append(engine.reconcile_batch(batch_id))

    t1 = threading.Thread(target=run, args=("B1",))
    t2 = threading.Thread(target=run, args=("B2",))

    start = time.time()
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    elapsed = time.time() - start

    assert len(results) == 2
    # margine più realistico per CI shared runner
    assert elapsed < 0.40


def test_lock_released_after_exception():
    db = FakeDB()
    bus = FakeBus()

    class ExplodingBatchManager(FakeBatchManager):
        def get_batch_legs(self, batch_id: str):
            raise RuntimeError("boom")

    eng = ReconciliationEngine(
        db=db,
        bus=bus,
        batch_manager=ExplodingBatchManager(),
        client_getter=lambda: FakeClient(),
        config=ReconcileConfig(max_convergence_cycles=1),
    )

    with pytest.raises(RuntimeError):
        eng.reconcile_batch("B1")

    # seconda chiamata: deve rientrare e rilanciare di nuovo boom,
    # non restare bloccata per lock zombie
    with pytest.raises(RuntimeError):
        eng.reconcile_batch("B1")