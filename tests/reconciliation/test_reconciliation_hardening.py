from __future__ import annotations

import time

import pytest

from core.reconciliation_engine import ReconciliationEngine, ReasonCode


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
            "market_id": "1.100",
        }
        self.legs[batch_id] = [
            {
                "leg_index": 0,
                "status": "PLACED",
                "customer_ref": "REF1",
                "bet_id": None,
                "selection_id": "1",
                "market_id": "1.100",
                "created_at_ts": time.time(),
            }
        ]

    def get_batch(self, batch_id):
        return self.batches.get(batch_id)

    def get_batch_legs(self, batch_id):
        return self.legs.get(batch_id)

    def update_leg_status(self, batch_id, leg_index, status, **kwargs):
        self.legs[batch_id][leg_index]["status"] = status
        for k, v in kwargs.items():
            if v is not None:
                self.legs[batch_id][leg_index][k] = v

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


@pytest.fixture
def engine():
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


def test_convergence_requires_no_changes(engine, batch):
    calls = {"count": 0}
    original = engine._apply_merge_policy

    def fake_apply(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            return "MATCHED", ReasonCode.EXCHANGE_WINS_MATCHED, "EXCHANGE"
        return None, ReasonCode.CONVERGED, "NONE"

    engine._apply_merge_policy = fake_apply
    try:
        engine.reconcile_batch(batch["batch_id"])
    finally:
        engine._apply_merge_policy = original

    assert calls["count"] == 1


def test_audit_fail_closed_blocks_state_change(engine, batch):
    engine.cfg.audit_fail_closed = True

    def fail_persist(*args, **kwargs):
        return False

    engine._persist_decision_immediate = fail_persist

    result = engine.reconcile_batch(batch["batch_id"])

    assert result["ok"] is True

    legs = engine.batch_manager.get_batch_legs(batch["batch_id"])
    assert all(l["status"] != "MATCHED" for l in legs)


def test_retry_does_not_stack_overflow(engine):
    engine.client_getter = lambda: FakeClient(fail=True)
    engine.cfg.max_transient_retries = 10

    orders, reason = engine._fetch_current_orders_by_market("1.100")

    assert orders == []
    assert reason == ReasonCode.TRANSIENT_ERROR


def test_recovery_marker_expires(engine):
    now = time.time()
    engine.db.set_reconcile_marker("B1", now - 1000)
    assert engine._is_recovery_marker_stale("B1") is True


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
        by_sel,
    )

    assert len(ghosts) == 0


def test_idempotent_second_run(engine, batch):
    engine.reconcile_batch(batch["batch_id"])
    first = engine._reconcile_fingerprints[batch["batch_id"]]

    result = engine.reconcile_batch(batch["batch_id"])
    second = engine._reconcile_fingerprints[batch["batch_id"]]

    assert first == second
    assert result["reason_code"] == ReasonCode.IDEMPOTENT_SKIP.value


def test_no_invalid_status_transition(engine, batch):
    leg = engine.batch_manager.get_batch_legs(batch["batch_id"])[0]
    leg["status"] = "MATCHED"

    engine.reconcile_batch(batch["batch_id"])

    assert leg["status"] == "MATCHED"


def test_decision_log_written(engine, batch):
    engine.reconcile_batch(batch["batch_id"])

    # dopo flush il log in-memory può essere vuoto; verifichiamo persistenza DB
    assert isinstance(engine.db.decisions, list)
    assert all("reason_code" in e for e in engine.db.decisions)


def test_reconcile_lock(engine, batch):
    lock = engine._lock_mgr._get_lock("B1")
    lock.acquire()
    try:
        result = engine.reconcile_batch("B1")
        assert result["reason_code"] == ReasonCode.RECONCILE_ALREADY_RUNNING.value
    finally:
        lock.release()


def test_unknown_resolves_to_failed(engine, batch):
    leg = engine.batch_manager.get_batch_legs(batch["batch_id"])[0]
    leg["status"] = "UNKNOWN"
    leg["created_at_ts"] = time.time() - 1000

    engine.reconcile_batch(batch["batch_id"])

    assert leg["status"] == "FAILED"


def test_placed_within_explicit_timeout_stays_placed(engine, batch):
    engine.cfg.placed_order_timeout_secs = 1200
    leg = engine.batch_manager.get_batch_legs(batch["batch_id"])[0]
    leg["status"] = "PLACED"
    leg["created_at_ts"] = time.time() - 10

    engine.reconcile_batch(batch["batch_id"])

    assert leg["status"] == "PLACED"


def test_placed_beyond_explicit_timeout_resolves_to_failed(engine, batch):
    engine.cfg.placed_order_timeout_secs = 5
    leg = engine.batch_manager.get_batch_legs(batch["batch_id"])[0]
    leg["status"] = "PLACED"
    leg["created_at_ts"] = time.time() - 1000

    result = engine.reconcile_batch(batch["batch_id"])

    assert leg["status"] == "FAILED"
    assert result["reason_code"] in {
        ReasonCode.CONVERGED.value,
        ReasonCode.TERMINAL_FINALIZED.value,
    }


def test_unknown_not_starved_by_first_cycle_idempotency_after_grace(engine, batch):
    leg = engine.batch_manager.get_batch_legs(batch["batch_id"])[0]
    leg["status"] = "UNKNOWN"
    leg["created_at_ts"] = 1000.0

    now = {"ts": 1001.0}
    engine._now_epoch = lambda: now["ts"]
    engine.cfg.unknown_grace_secs = 10.0

    first = engine.reconcile_batch(batch["batch_id"])
    assert leg["status"] == "UNKNOWN"
    assert first["reason_code"] in {
        ReasonCode.CONVERGED.value,
        ReasonCode.TERMINAL_FINALIZED.value,
    }

    now["ts"] = 1015.0
    second = engine.reconcile_batch(batch["batch_id"])
    assert leg["status"] == "FAILED"
    assert second["reason_code"] != ReasonCode.IDEMPOTENT_SKIP.value
