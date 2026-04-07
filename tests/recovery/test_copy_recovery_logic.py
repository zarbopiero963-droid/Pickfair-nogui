from __future__ import annotations

import time

import pytest

from core.reconciliation_engine import (
    IllegalTransitionError,
    ReasonCode,
    ReconciliationEngine,
    validate_leg_transition,
)
from tests.fixtures.fake_batch_manager import FakeBatchManager


class FakeDB:
    def persist_decision_log(self, batch_id, entries):
        return None

    def get_pending_sagas(self):
        return []

    def get_reconcile_marker(self, batch_id):
        return None

    def set_reconcile_marker(self, batch_id, value):
        return None


def make_engine():
    return ReconciliationEngine(
        db=FakeDB(),
        bus=None,
        batch_manager=FakeBatchManager(),
        client_getter=lambda: None,
    )


def test_recovery_logic_handles_inflight_ambiguous_and_partial_without_illegal_transitions():
    eng = make_engine()

    inflight_leg = {"leg_index": 0, "status": "PLACED", "created_at_ts": time.time()}
    new_status, reason, winner = eng._apply_merge_policy(
        batch_id="B-INFLIGHT",
        leg=inflight_leg,
        remote_order=None,
        saga_pending=True,
    )
    assert new_status is None
    assert reason == ReasonCode.LOCAL_WINS_SAGA_PENDING
    assert winner == "LOCAL"

    ambiguous_leg = {"leg_index": 1, "status": "UNKNOWN", "created_at_ts": time.time()}
    remote_matched = {"status": "EXECUTION_COMPLETE", "sizeMatched": 10.0}
    new_status, reason, winner = eng._apply_merge_policy(
        batch_id="B-AMB",
        leg=ambiguous_leg,
        remote_order=remote_matched,
        saga_pending=False,
    )
    assert new_status == "MATCHED"
    assert reason == ReasonCode.EXCHANGE_WINS_MATCHED
    assert winner == "EXCHANGE"

    partial_leg = {"leg_index": 2, "status": "PARTIAL", "customer_ref": "C-1"}
    remote_partial = {
        "customerOrderRef": "C-1",
        "status": "EXECUTABLE",
        "sizeMatched": 1.0,
        "sizeRemaining": 9.0,
    }
    new_status, reason, winner = eng._apply_merge_policy(
        batch_id="B-PARTIAL",
        leg=partial_leg,
        remote_order=remote_partial,
        saga_pending=False,
    )
    assert new_status is None
    assert reason == ReasonCode.IDEMPOTENT_SKIP
    assert winner == "NONE"


def test_unknown_ambiguity_stays_unresolved_until_evidence_arrives():
    eng = make_engine()
    leg = {
        "leg_index": 3,
        "status": "UNKNOWN",
        "created_at_ts": 0,
    }

    new_status, reason, winner = eng._apply_merge_policy(
        batch_id="B-HOLD",
        leg=leg,
        remote_order=None,
        saga_pending=False,
    )

    assert new_status is None
    assert reason == ReasonCode.LOCAL_WINS_SAGA_PENDING
    assert winner == "LOCAL"


def test_illegal_transition_is_blocked_for_recovery_state_machine():
    with pytest.raises(IllegalTransitionError):
        validate_leg_transition("MATCHED", "PLACED", batch_id="B-ILLEGAL", leg_index=9)
