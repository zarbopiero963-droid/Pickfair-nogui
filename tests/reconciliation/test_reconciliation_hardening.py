import time
import pytest


# =========================================================
# FIXTURE MINIME REALI (no mocking fragile)
# =========================================================

class FakeDB:
    def __init__(self):
        self.markers = {}
        self.decisions = []

    def persist_decision_log(self, batch_id, entries):
        self.decisions.extend(entries)

    def set_reconcile_marker(self, batch_id, value):
        self.markers[batch_id] = value

    def get_reconcile_marker(self, batch_id):
        return self.markers.get(batch_id)

    def get_pending_sagas(self):
        return []


class FakeBatchManager:
    def __init__(self):
        self.batches = {}
        self.legs = {}

    def create_batch(self, batch_id):
        self.batches[batch_id] = {
            "batch_id": batch_id,
            "status": "LIVE",
            "market_id": "1.100"
        }
        self.legs[batch_id] = [
            {
                "leg_index": 0,
                "status": "PLACED",
                "customer_ref": "REF1",
                "bet_id": None,
                "selection_id": "1",
                "market_id": "1.100",
                "created_at_ts": time.time()
            }
        ]

    def get_batch(self, batch_id):
        return self.batches.get(batch_id)

    def get_batch_legs(self, batch_id):
        return self.legs.get(batch_id)

    def update_leg_status(self, batch_id, leg_index, status, **kwargs):
        self.legs[batch_id][leg_index]["status"] = status

    def recompute_batch_status(self, batch_id):
        return self.batches[batch_id]

    def release_runtime_artifacts(self, **kwargs):
        pass

    def get_open_batches(self):
        return list(self.batches.values())

    def mark_batch_failed(self, *args, **kwargs):
        pass


class FakeClient:
    def __init__(self, fail=False):
        self.fail = fail

    def get_current_orders(self, **kwargs):
        if self.fail:
            raise TimeoutError("timeout")
        return []


# =========================================================
# FIXTURE ENGINE
# =========================================================

@pytest.fixture
def engine():
    from core.reconciliation_engine import ReconciliationEngine

    db = FakeDB()
    bm = FakeBatchManager()
    bm.create_batch("B1")

    eng = ReconciliationEngine(
        db=db,
        batch_manager=bm,
        client_getter=lambda: FakeClient(),
    )

    return eng


@pytest.fixture
def batch(engine):
    return {"batch_id": "B1"}


# =========================================================
# 🔥 TEST 1 — CONVERGENZA REALE (mutation killer)
# =========================================================

def test_convergence_requires_no_changes(engine, batch):
    calls = {"count": 0}

    original = engine._apply_merge_policy

    def fake_apply(*args, **kwargs):
        calls["count"] += 1

        # prima iterazione cambia stato
        if calls["count"] == 1:
            return "MATCHED", engine.ReasonCode.EXCHANGE_WINS_MATCHED, "EXCHANGE"

        # dopo stabilizza
        return None, engine.ReasonCode.CONVERGED, "NONE"

    engine._apply_merge_policy = fake_apply

    engine.reconcile_batch(batch["batch_id"])

    # DEVE fare almeno 2 cicli
    assert calls["count"] >= 2


# =========================================================
# 🔥 TEST 2 — AUDIT FAIL-CLOSED (soldi veri)
# =========================================================

def test_audit_fail_closed_blocks_state_change(engine, batch):
    engine.cfg.audit_fail_closed = True

    def fail_persist(*args, **kwargs):
        return False

    engine._persist_decision_immediate = fail_persist

    with pytest.raises(RuntimeError):
        engine.reconcile_batch(batch["batch_id"])

    legs = engine.batch_manager.get_batch_legs(batch["batch_id"])

    # stato NON deve cambiare
    assert all(l["status"] != "MATCHED" for l in legs)


# =========================================================
# 🔥 TEST 3 — RETRY LOOP (no recursion)
# =========================================================

def test_retry_does_not_stack_overflow(engine):
    engine.client_getter = lambda: FakeClient(fail=True)
    engine.cfg.max_transient_retries = 10

    orders, reason = engine._fetch_current_orders_by_market("1.100")

    assert reason == engine.ReasonCode.TRANSIENT_ERROR


# =========================================================
# 🔥 TEST 4 — RECOVERY TTL (no zombie lock)
# =========================================================

def test_recovery_marker_expires(engine):
    now = time.time()

    engine.db.set_reconcile_marker("B1", now - 1000)
    engine.db.get_reconcile_marker = lambda b: now - 1000

    assert engine._has_recovery_marker("B1") is False


# =========================================================
# 🔥 TEST 5 — GHOST FALSE POSITIVE (soldi veri)
# =========================================================

def test_no_false_ghost_on_replaced_order(engine, batch):
    legs = engine.batch_manager.get_batch_legs(batch["batch_id"])

    remote_orders = [{
        "selectionId": legs[0]["selection_id"],
        "marketId": legs[0]["market_id"],
        "betId": "NEW123",
        "status": "EXECUTABLE",
    }]

    by_ref, by_bet, by_sel = engine._build_exchange_indices(remote_orders)

    ghosts = engine._detect_ghost_orders(
        batch["batch_id"],
        legs,
        remote_orders,
        by_ref,
        by_bet,
        by_sel
    )

    assert len(ghosts) == 0


# =========================================================
# 🔥 TEST 6 — IDEMPOTENCY HARD
# =========================================================

def test_idempotent_second_run(engine, batch):
    engine.reconcile_batch(batch["batch_id"])

    first = engine._reconcile_fingerprints[batch["batch_id"]]

    result = engine.reconcile_batch(batch["batch_id"])

    second = engine._reconcile_fingerprints[batch["batch_id"]]

    assert first == second
    assert result["reason_code"] == "IDEMPOTENT_SKIP"


# =========================================================
# 🔥 TEST 7 — NO INVALID TRANSITION
# =========================================================

def test_no_invalid_status_transition(engine, batch):
    leg = engine.batch_manager.get_batch_legs(batch["batch_id"])[0]
    leg["status"] = "MATCHED"

    result = engine.reconcile_batch(batch["batch_id"])

    # non deve tornare indietro
    assert leg["status"] == "MATCHED"


# =========================================================
# 🔥 TEST 8 — DECISION LOG CONSISTENTE
# =========================================================

def test_decision_log_written(engine, batch):
    engine.reconcile_batch(batch["batch_id"])

    log = engine.get_decision_log(batch["batch_id"])

    assert isinstance(log, list)
    assert all("reason_code" in e for e in log)


# =========================================================
# 🔥 TEST 9 — LOCK CONCURRENCY
# =========================================================

def test_reconcile_lock(engine, batch):
    engine._lock_mgr._batch_locks["B1"] = engine._lock_mgr._get_lock("B1")

    engine._lock_mgr._batch_locks["B1"].acquire()

    result = engine.reconcile_batch("B1")

    assert result["reason_code"] == "RECONCILE_ALREADY_RUNNING"

    engine._lock_mgr._batch_locks["B1"].release()


# =========================================================
# 🔥 TEST 10 — UNKNOWN RESOLUTION
# =========================================================

def test_unknown_resolves_to_failed(engine, batch):
    leg = engine.batch_manager.get_batch_legs(batch["batch_id"])[0]
    leg["status"] = "UNKNOWN"
    leg["created_at_ts"] = time.time() - 1000

    engine.reconcile_batch(batch["batch_id"])

    assert leg["status"] == "FAILED"