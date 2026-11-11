"""Gate A batch C: core/batch_lock_manager.py (_BatchLockManager)."""
from __future__ import annotations

import pytest

from core.batch_lock_manager import _BatchLockManager


def test_trg_628_b_0974():
    """ID: TRG-628-B-0974 / PF-B1-R2-BATCH-LOCK

    acquire() creates one lock per batch, tries a non-blocking acquire and
    yields the bool; on exit it releases ONLY if it acquired. A losing
    contender's exit must not release the winner's lock; an exception inside
    the winner's block still releases.
    """
    mgr = _BatchLockManager()
    with mgr.acquire("B1") as outer:
        assert outer is True
        assert mgr.is_locked("B1") is True
        assert "B1" in mgr._batch_owners
        with mgr.acquire("B1") as inner:
            assert inner is False                     # non-reentrant
        # the losing contender exited: the winner's lock is still held
        assert mgr.is_locked("B1") is True
        assert "B1" in mgr._batch_owners
        with mgr.acquire("B2") as other:              # different batch independent
            assert other is True
    assert mgr.is_locked("B1") is False
    assert mgr.is_locked("B2") is False
    assert mgr._batch_owners == {}
    # same lock object reused for the batch id
    assert mgr._get_lock("B1") is mgr._get_lock("B1")

    with pytest.raises(RuntimeError):
        with mgr.acquire("B3") as got:
            assert got is True
            raise RuntimeError("boom")
    assert mgr.is_locked("B3") is False
    assert "B3" not in mgr._batch_owners


def test_trg_628_b_0975_idle_batch_cleanup():
    """ID: TRG-628-B-0975 / PF-B1-R2-BATCH-CLEANUP

    Pins ONLY the uncontroversial part: cleanup_batch() drops the lock and
    owner entries of a batch from the registry (no memory leak). The
    "without held check" aspect is NOT pinned (NEEDS_OWNER_DECISION: the
    reconciliation engine calls cleanup_batch while still holding the batch
    lock, so a concurrent acquire after cleanup would get a fresh lock).
    """
    mgr = _BatchLockManager()
    with mgr.acquire("B1") as got:
        assert got is True
    mgr._batch_owners["B1"] = 123                     # stale owner entry
    assert "B1" in mgr._batch_locks
    mgr.cleanup_batch("B1")
    assert "B1" not in mgr._batch_locks
    assert "B1" not in mgr._batch_owners
    mgr.cleanup_batch("never-seen")                   # idempotent, no KeyError
