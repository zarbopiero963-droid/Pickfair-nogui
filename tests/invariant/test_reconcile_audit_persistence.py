from __future__ import annotations

import copy
import pytest

from core.reconciliation_engine import ReconciliationEngine, ReconcileConfig, ReasonCode


# =========================================================
# FAKES
# =========================================================

class StrictDB:
    """
    DB che può fallire per test audit critical.
    """

    def __init__(self, fail_persist=False):
        self.logs = {}
        self.fail_persist = fail_persist

    def persist_decision_log(self, batch_id, entries):
        if self.fail_persist:
            raise RuntimeError("AUDIT_PERSIST_FAILED")

        self.logs.setdefault(batch_id, []).extend(copy.deepcopy(entries))


class FakeBatchManager:
    def __init__(self):
        self.batch = {"batch_id": "B300", "market_id": "1.300", "status": "LIVE"}
        self.legs = [
            {"leg_index": 0, "status": "SUBMITTED", "customer_ref": "R1"}
        ]

    def get_batch(self, batch_id):
        return self.batch

    def get_batch_legs(self, batch_id):
        return self.legs

    def update_leg_status(self, **kwargs):
        self.legs[0]["status"] = kwargs["status"]

    def recompute_batch_status(self, batch_id):
        return self.batch

    def release_runtime_artifacts(self, **kwargs):
        pass


class AuditEngine(ReconciliationEngine):
    def _fetch_current_orders_by_market(self, *args, **kwargs):
        return []

    def _get_pending_saga_refs(self):
        return set()


# =========================================================
# TESTS
# =========================================================

def build_engine(fail=False):
    db = StrictDB(fail_persist=fail)
    bm = FakeBatchManager()

    engine = AuditEngine(
        db=db,
        batch_manager=bm,
        client_getter=lambda: None,
        config=ReconcileConfig(unknown_grace_secs=0),
    )

    return engine, db


# ---------------------------------------------------------
# 1. DECISION ALWAYS LOGGED
# ---------------------------------------------------------

def test_decision_logged_every_transition():
    engine, db = build_engine()

    engine.reconcile_batch("B300")

    assert "B300" in db.logs
    assert len(db.logs["B300"]) > 0


# ---------------------------------------------------------
# 2. AUDIT FAILURE MUST BREAK (CRITICAL MODE)
# ---------------------------------------------------------

def test_persist_failure_blocks_reconcile():
    engine, _ = build_engine(fail=True)

    with pytest.raises(RuntimeError):
        engine.reconcile_batch("B300")


# ---------------------------------------------------------
# 3. DECISION LOG CONSISTENCY
# ---------------------------------------------------------

def test_decision_log_consistency():
    engine, db = build_engine()

    engine.reconcile_batch("B300")

    logs = db.logs["B300"]

    # ogni entry deve avere struttura coerente
    for entry in logs:
        assert "reason_code" in entry
        assert "resolved_status" in entry


# ---------------------------------------------------------
# 4. REASON CODE VALIDITY
# ---------------------------------------------------------

def test_decision_reason_codes_valid():
    engine, db = build_engine()

    engine.reconcile_batch("B300")

    valid = {x.value for x in ReasonCode}

    for entry in db.logs["B300"]:
        assert entry["reason_code"] in valid


# ---------------------------------------------------------
# 5. FLUSH IS BATCH-SCOPED
# ---------------------------------------------------------

def test_flush_decision_log_per_batch_only():
    engine, db = build_engine()

    engine.reconcile_batch("B300")

    assert "B300" in db.logs
    assert len(db.logs.keys()) == 1