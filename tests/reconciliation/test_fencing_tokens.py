"""
Tests for concrete fencing token enforcement in ReconciliationEngine.

Verifies:
- _next_fencing_token() returns monotonically increasing integers
- reconcile_batch() result includes fencing_token when enable_fencing_token=True
- get_active_fencing_token() returns the active token during reconcile
- get_active_fencing_token() returns None after reconcile completes
- assert_fencing_ownership() passes when token matches
- assert_fencing_ownership() raises FENCING_OWNERSHIP_LOST when token is stale
- No token when enable_fencing_token=False
"""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, List, Optional

import pytest

from core.reconciliation_engine import ReconcileConfig, ReconciliationEngine


# ===========================================================================
# Stubs
# ===========================================================================

class _DB:
    def __init__(self):
        self.markers: Dict[str, Any] = {}

    def persist_decision_log(self, batch_id, entries):
        return None

    def get_pending_sagas(self):
        return []

    def get_reconcile_marker(self, batch_id):
        return self.markers.get(batch_id)

    def set_reconcile_marker(self, batch_id, value):
        self.markers[batch_id] = value


class _Bus:
    def __init__(self):
        self.events: List = []

    def publish(self, name, payload=None):
        self.events.append((name, payload))


class _BatchManager:
    def __init__(self, sleep_secs: float = 0.0):
        self._sleep = sleep_secs
        # Use empty market_id so the engine skips exchange fetch
        self._batches: Dict[str, Dict] = {
            "B1": {"batch_id": "B1", "market_id": "", "status": "LIVE"},
        }
        self._legs: Dict[str, List] = {
            "B1": [{"leg_index": 0, "status": "UNKNOWN", "bet_id": "",
                    "customer_ref": "r1", "created_at_ts": time.time()}],
        }
        self.recomputed: List[str] = []

    # -- required contract --
    def get_batch(self, batch_id):
        if self._sleep:
            time.sleep(self._sleep)
        return self._batches.get(batch_id)

    def get_batch_legs(self, batch_id):
        return list(self._legs.get(batch_id, []))

    def update_leg_status(self, batch_id, leg_index, status, **_kw):
        return None

    def recompute_batch_status(self, batch_id):
        self.recomputed.append(batch_id)
        # Must return a dict (engine does new_batch.get("status"))
        return dict(self._batches.get(batch_id) or {"status": "LIVE"})

    def release_runtime_artifacts(self, batch_id):
        return None

    def mark_batch_failed(self, batch_id, reason=""):
        return None

    def get_open_batches(self):
        return list(self._batches.values())


def _make_engine(cfg=None, bm=None) -> ReconciliationEngine:
    cfg = cfg or ReconcileConfig(
        validate_batch_manager_contract=True,
        enable_fencing_token=True,
    )
    return ReconciliationEngine(
        db=_DB(),
        bus=_Bus(),
        batch_manager=bm or _BatchManager(),
        betfair_service=None,
        client_getter=None,
        config=cfg,
    )


# ===========================================================================
# Tests: _next_fencing_token monotonic
# ===========================================================================

@pytest.mark.unit
@pytest.mark.reconciliation
def test_fencing_token_monotonically_increasing():
    engine = _make_engine()
    tokens = [engine._next_fencing_token() for _ in range(5)]
    assert tokens == sorted(tokens), "tokens must be strictly increasing"
    assert len(set(tokens)) == 5, "tokens must be unique"


@pytest.mark.unit
@pytest.mark.reconciliation
def test_fencing_token_thread_safe():
    engine = _make_engine()
    results = []
    lock = threading.Lock()

    def grab():
        t = engine._next_fencing_token()
        with lock:
            results.append(t)

    threads = [threading.Thread(target=grab) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(results) == 20
    assert len(set(results)) == 20, "all tokens must be unique under concurrency"


# ===========================================================================
# Tests: token in reconcile result
# ===========================================================================

@pytest.mark.unit
@pytest.mark.reconciliation
def test_reconcile_result_includes_fencing_token():
    engine = _make_engine()
    result = engine.reconcile_batch("B1")
    assert "fencing_token" in result, "result must include fencing_token"
    assert isinstance(result["fencing_token"], int)
    assert result["fencing_token"] > 0


@pytest.mark.unit
@pytest.mark.reconciliation
def test_reconcile_no_token_when_disabled():
    cfg = ReconcileConfig(
        validate_batch_manager_contract=True,
        enable_fencing_token=False,
    )
    engine = _make_engine(cfg=cfg)
    result = engine.reconcile_batch("B1")
    assert result.get("fencing_token") is None


# ===========================================================================
# Tests: active token tracking
# ===========================================================================

@pytest.mark.unit
@pytest.mark.reconciliation
def test_active_fencing_token_cleared_after_reconcile():
    engine = _make_engine()
    engine.reconcile_batch("B1")
    # After completion, active token must be cleared
    assert engine.get_active_fencing_token("B1") is None


@pytest.mark.unit
@pytest.mark.reconciliation
def test_active_fencing_token_set_during_reconcile():
    """Token must be visible in _active_fencing_tokens while reconcile runs."""
    captured_token_during = []

    class SlowBM(_BatchManager):
        def get_batch(self, batch_id):
            # Capture the active token after the lock is acquired
            tok = engine.get_active_fencing_token(batch_id)
            captured_token_during.append(tok)
            return super().get_batch(batch_id)

    engine = _make_engine(bm=SlowBM())
    result = engine.reconcile_batch("B1")

    assert len(captured_token_during) >= 1
    assert captured_token_during[0] is not None, \
        "fencing token must be set during reconcile execution"
    assert captured_token_during[0] == result.get("fencing_token"), \
        "captured token must match result token"


# ===========================================================================
# Tests: assert_fencing_ownership
# ===========================================================================

@pytest.mark.unit
@pytest.mark.reconciliation
def test_assert_fencing_ownership_passes_for_valid_token():
    engine = _make_engine()
    # Manually set a token
    with engine._fencing_lock:
        engine._active_fencing_tokens["B1"] = 42

    # Must not raise
    engine.assert_fencing_ownership("B1", 42)


@pytest.mark.unit
@pytest.mark.reconciliation
def test_assert_fencing_ownership_raises_for_stale_token():
    engine = _make_engine()
    # Active token is 99, caller claims 42 (stale)
    with engine._fencing_lock:
        engine._active_fencing_tokens["B1"] = 99

    with pytest.raises(RuntimeError, match="FENCING_OWNERSHIP_LOST"):
        engine.assert_fencing_ownership("B1", 42)


@pytest.mark.unit
@pytest.mark.reconciliation
def test_assert_fencing_ownership_raises_when_no_active_token():
    engine = _make_engine()
    # No active token (reconcile not running)
    with pytest.raises(RuntimeError, match="FENCING_OWNERSHIP_LOST"):
        engine.assert_fencing_ownership("B1", 1)


# ===========================================================================
# Tests: runtime enforcement at critical mutation points
# ===========================================================================

@pytest.mark.unit
@pytest.mark.reconciliation
def test_fencing_stale_token_denied_before_mark_batch_failed():
    """Stale fencing token raises FENCING_OWNERSHIP_LOST before mark_batch_failed."""

    class NoLegsBM(_BatchManager):
        def get_batch_legs(self, batch_id):
            return []  # triggers the no-legs → mark_batch_failed path

    engine = _make_engine(bm=NoLegsBM())
    # Plant a different active token so the one we pass (42) is stale
    with engine._fencing_lock:
        engine._active_fencing_tokens["B1"] = 99

    with pytest.raises(RuntimeError, match="FENCING_OWNERSHIP_LOST"):
        engine._reconcile_batch_inner("B1", fencing_token=42)


@pytest.mark.unit
@pytest.mark.reconciliation
def test_fencing_stale_token_denied_before_transactional_leg_update():
    """Stale fencing token raises FENCING_OWNERSHIP_LOST before _transactional_leg_update.

    The default _BatchManager has B1 with one UNKNOWN leg and an empty market_id
    (no exchange fetch). The merge policy resolves UNKNOWN → FAILED immediately,
    triggering a state-change which calls _transactional_leg_update.
    """
    engine = _make_engine()
    # Plant a different active token so the one we pass (42) is stale
    with engine._fencing_lock:
        engine._active_fencing_tokens["B1"] = 99

    with pytest.raises(RuntimeError, match="FENCING_OWNERSHIP_LOST"):
        engine._reconcile_batch_inner("B1", fencing_token=42)


@pytest.mark.unit
@pytest.mark.reconciliation
def test_fencing_stale_token_denied_before_recompute_batch_status():
    """Stale fencing token raises FENCING_OWNERSHIP_LOST before recompute_batch_status.

    All legs are MATCHED (terminal) so the convergence loop produces no state
    changes and _transactional_leg_update is never called — the first fencing
    check reached is the one immediately before recompute_batch_status.
    """

    class TerminalLegsBM(_BatchManager):
        def __init__(self):
            super().__init__()
            self._legs["B1"] = [
                {"leg_index": 0, "status": "MATCHED", "bet_id": "123",
                 "customer_ref": "r1", "created_at_ts": time.time()}
            ]

    engine = _make_engine(bm=TerminalLegsBM())
    # Plant a different active token so the one we pass (42) is stale
    with engine._fencing_lock:
        engine._active_fencing_tokens["B1"] = 99

    with pytest.raises(RuntimeError, match="FENCING_OWNERSHIP_LOST"):
        engine._reconcile_batch_inner("B1", fencing_token=42)
