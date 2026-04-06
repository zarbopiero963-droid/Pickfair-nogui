import threading
import time
import pytest

from core.reconciliation_engine import ReconciliationEngine


# =========================================================
# FAKE INFRASTRUTTURA MINIMA
# =========================================================

class FakeDB:
    def persist_decision_log(self, batch_id, entries):
        return True


class FakeBatchManager:
    def __init__(self):
        self._batches = {
            "B1": {"batch_id": "B1", "market_id": "1", "status": "LIVE"},
            "B2": {"batch_id": "B2", "market_id": "2", "status": "LIVE"},
        }
        self._legs = {
            "B1": [{"leg_index": 0, "status": "SUBMITTED", "customer_ref": "R1"}],
            "B2": [{"leg_index": 0, "status": "SUBMITTED", "customer_ref": "R2"}],
        }

    def get_batch(self, batch_id):
        return self._batches.get(batch_id)

    def get_batch_legs(self, batch_id):
        return self._legs.get(batch_id, [])

    def recompute_batch_status(self, batch_id):
        return self._batches.get(batch_id)

    def release_runtime_artifacts(self, **kwargs):
        pass

    def get_open_batches(self):
        return list(self._batches.values())

    def mark_batch_failed(self, batch_id, reason=""):
        self._batches[batch_id]["status"] = "FAILED"
        self._batches[batch_id]["reason"] = reason

    def update_leg_status(self, **kwargs):
        pass


class BlockingEngine(ReconciliationEngine):
    """
    Engine che blocca dentro reconcile per simulare long-running.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # lock per batch
        self._active_batches = set()
        self._lock = threading.Lock()

    def reconcile_batch(self, batch_id: str):
        # LOCK PER BATCH
        with self._lock:
            if batch_id in self._active_batches:
                return {"ok": False, "reason": "ALREADY_RUNNING"}
            self._active_batches.add(batch_id)

        try:
            # simula lavoro lungo
            time.sleep(0.2)
            return {"ok": True, "batch_id": batch_id}
        finally:
            with self._lock:
                self._active_batches.discard(batch_id)


# =========================================================
# TESTS
# =========================================================

def build_engine():
    return BlockingEngine(
        db=FakeDB(),
        batch_manager=FakeBatchManager(),
        client_getter=lambda: None,
    )


# ---------------------------------------------------------
# 1. SINGLE EXECUTION PER BATCH
# ---------------------------------------------------------

def test_single_execution_per_batch():
    engine = build_engine()

    results = []

    def worker():
        r = engine.reconcile_batch("B1")
        results.append(r)

    t1 = threading.Thread(target=worker)
    t2 = threading.Thread(target=worker)

    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # uno deve passare, uno deve essere bloccato
    oks = [r["ok"] for r in results]

    assert oks.count(True) == 1
    assert oks.count(False) == 1


# ---------------------------------------------------------
# 2. LOCK RELEASE AFTER EXCEPTION
# ---------------------------------------------------------

def test_lock_released_after_exception():
    engine = build_engine()

    # monkey patch per forzare crash
    original = engine.reconcile_batch

    def crashing(batch_id):
        with engine._lock:
            if batch_id in engine._active_batches:
                return {"ok": False}
            engine._active_batches.add(batch_id)

        try:
            raise RuntimeError("CRASH")
        finally:
            with engine._lock:
                engine._active_batches.discard(batch_id)

    engine.reconcile_batch = crashing

    # primo crash
    with pytest.raises(RuntimeError):
        engine.reconcile_batch("B1")

    # secondo deve riuscire (lock rilasciato)
    engine.reconcile_batch = original
    result = engine.reconcile_batch("B1")

    assert result["ok"] is True


# ---------------------------------------------------------
# 3. PARALLEL BATCHES ALLOWED
# ---------------------------------------------------------

def test_parallel_batches_allowed():
    engine = build_engine()

    results = []

    def worker(batch_id):
        r = engine.reconcile_batch(batch_id)
        results.append(r)

    t1 = threading.Thread(target=worker, args=("B1",))
    t2 = threading.Thread(target=worker, args=("B2",))

    start = time.time()

    t1.start()
    t2.start()
    t1.join()
    t2.join()

    elapsed = time.time() - start

    # se lock fosse globale → ~0.4
    # se per batch → ~0.2
    assert elapsed < 0.35

    assert all(r["ok"] for r in results)


# ---------------------------------------------------------
# 4. REENTRANT SAME BATCH DENIED
# ---------------------------------------------------------

def test_reentrant_same_batch_denied_or_skipped():
    engine = build_engine()

    started = threading.Event()
    release = threading.Event()

    def long_worker():
        with engine._lock:
            engine._active_batches.add("B1")
        started.set()
        release.wait()
        with engine._lock:
            engine._active_batches.discard("B1")

    t = threading.Thread(target=long_worker)
    t.start()

    started.wait()

    # mentre è attivo → deve essere negato
    result = engine.reconcile_batch("B1")

    assert result["ok"] is False
    assert result["reason"] == "ALREADY_RUNNING"

    release.set()
    t.join()