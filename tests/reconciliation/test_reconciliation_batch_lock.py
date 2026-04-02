from __future__ import annotations

import threading
import time
from typing import Any, Dict, List

from core.reconciliation_engine import ReasonCode, ReconciliationEngine


class FakeDB:
    def __init__(self):
        self.markers: Dict[str, Any] = {}
        self.persisted: List[tuple[str, list[dict[str, Any]]]] = []

    def persist_decision_log(self, batch_id, entries):
        self.persisted.append((batch_id, list(entries)))
        return None

    def get_pending_sagas(self):
        return []

    def get_reconcile_marker(self, batch_id):
        return self.markers.get(batch_id)

    def set_reconcile_marker(self, batch_id, value):
        self.markers[batch_id] = value
        return None


class FakeBus:
    def __init__(self):
        self.events: List[tuple[str, dict[str, Any]]] = []

    def publish(self, name, payload):
        self.events.append((name, payload))


class FakeClient:
    def get_current_orders(self, market_ids=None):
        return []


class FakeBatchManager:
    def __init__(self, sleep_secs: float = 0.20, raise_on_batch: str | None = None):
        self.sleep_secs = sleep_secs
        self.raise_on_batch = raise_on_batch

        self.entered: List[str] = []
        self.updated: List[tuple] = []
        self.released: List[str] = []

        self._batches: Dict[str, Dict[str, Any]] = {
            "B1": {"batch_id": "B1", "market_id": "1.1", "status": "LIVE"},
            "B2": {"batch_id": "B2", "market_id": "1.2", "status": "LIVE"},
        }
        self._legs: Dict[str, List[Dict[str, Any]]] = {
            # almeno una leg non terminale, così il motore non va in NO_LEGS
            "B1": [
                {
                    "leg_index": 0,
                    "status": "PLACED",
                    "customer_ref": "REF_B1",
                    "bet_id": "BET_B1",
                    "market_id": "1.1",
                    "selection_id": "101",
                    "created_at_ts": time.time(),
                }
            ],
            "B2": [
                {
                    "leg_index": 0,
                    "status": "PLACED",
                    "customer_ref": "REF_B2",
                    "bet_id": "BET_B2",
                    "market_id": "1.2",
                    "selection_id": "102",
                    "created_at_ts": time.time(),
                }
            ],
        }

    def get_batch(self, batch_id):
        return self._batches.get(batch_id)

    def get_batch_legs(self, batch_id):
        self.entered.append(batch_id)
        if self.raise_on_batch == batch_id:
            raise RuntimeError(f"boom:{batch_id}")
        time.sleep(self.sleep_secs)
        return [dict(x) for x in self._legs.get(batch_id, [])]

    def update_leg_status(
        self,
        batch_id,
        leg_index,
        status,
        bet_id=None,
        raw_response=None,
        error_text=None,
    ):
        self.updated.append(
            (batch_id, leg_index, status, bet_id, raw_response, error_text)
        )
        legs = self._legs.setdefault(batch_id, [])
        for leg in legs:
            if int(leg.get("leg_index", -1)) == int(leg_index):
                leg["status"] = status
                if bet_id is not None:
                    leg["bet_id"] = bet_id
                return None
        return None

    def recompute_batch_status(self, batch_id):
        return self._batches.get(batch_id, {"batch_id": batch_id, "status": "LIVE"})

    def mark_batch_failed(self, batch_id, reason=""):
        batch = self._batches.setdefault(
            batch_id, {"batch_id": batch_id, "market_id": "", "status": "LIVE"}
        )
        batch["status"] = "FAILED"
        batch["reason"] = reason
        return None

    def release_runtime_artifacts(
        self,
        batch_id,
        duplication_guard=None,
        table_manager=None,
        pnl=0.0,
    ):
        self.released.append(batch_id)
        return None

    def get_open_batches(self):
        return list(self._batches.values())


def make_engine(*, sleep_secs: float = 0.20, raise_on_batch: str | None = None):
    db = FakeDB()
    bus = FakeBus()
    batch_manager = FakeBatchManager(
        sleep_secs=sleep_secs,
        raise_on_batch=raise_on_batch,
    )
    engine = ReconciliationEngine(
        db=db,
        bus=bus,
        batch_manager=batch_manager,
        client_getter=lambda: FakeClient(),
    )
    return engine, db, bus, batch_manager


def test_single_execution_per_batch():
    engine, _db, _bus, batch_manager = make_engine(sleep_secs=0.20)
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
    assert ReasonCode.RECONCILE_ALREADY_RUNNING.value in reason_codes
    assert ReasonCode.CONVERGED.value in reason_codes

    assert batch_manager.entered.count("B1") == 1


def test_lock_released_after_exception():
    engine, _db, _bus, batch_manager = make_engine(
        sleep_secs=0.05,
        raise_on_batch="B1",
    )

    try:
        engine.reconcile_batch("B1")
    except RuntimeError as exc:
        assert "boom:B1" in str(exc)

    batch_manager.raise_on_batch = None
    result2 = engine.reconcile_batch("B1")

    assert result2["reason_code"] == ReasonCode.CONVERGED.value
    assert engine._lock_mgr.is_locked("B1") is False


def test_parallel_batches_can_run_in_parallel():
    engine, _db, _bus, _batch_manager = make_engine(sleep_secs=0.20)
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
    assert all(r["reason_code"] == ReasonCode.CONVERGED.value for r in results)

    assert elapsed < 0.60


def test_reentrant_same_batch_denied_or_skipped():
    engine, _db, _bus, batch_manager = make_engine(sleep_secs=0.20)
    results: List[Dict[str, Any]] = []

    def nested_call():
        results.append(engine.reconcile_batch("B1"))

    original_get_batch_legs = batch_manager.get_batch_legs
    nested_triggered = {"done": False}

    def wrapped_get_batch_legs(batch_id):
        if batch_id == "B1" and not nested_triggered["done"]:
            nested_triggered["done"] = True
            t = threading.Thread(target=nested_call)
            t.start()
            time.sleep(0.03)
            res = original_get_batch_legs(batch_id)
            t.join()
            return res
        return original_get_batch_legs(batch_id)

    batch_manager.get_batch_legs = wrapped_get_batch_legs

    outer = engine.reconcile_batch("B1")
    results.append(outer)

    assert len(results) == 2
    reason_codes = [r["reason_code"] for r in results]

    assert ReasonCode.RECONCILE_ALREADY_RUNNING.value in reason_codes
    assert ReasonCode.CONVERGED.value in reason_codes