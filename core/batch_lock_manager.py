"""
Per-batch non-reentrant lock manager extracted from ReconciliationEngine.

Extracted from core/reconciliation_engine.py to reduce module size.
ReconciliationEngine imports _BatchLockManager from here; existing callers
that import ReconciliationEngine are unaffected (no public API change).
"""
from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Dict, Generator


class _BatchLockManager:
    """
    Per-batch non-reentrant lock with zombie protection.

    Guarantees:
      - Only one reconcile_batch() runs per batch_id at a time
      - Different batch_ids can reconcile in parallel
      - Exception during reconcile → lock always released
      - After crash/restart → no zombie locks (in-memory only,
        combined with recovery marker in DB for cross-process)
    """

    def __init__(self) -> None:
        self._global_lock = threading.Lock()
        self._batch_locks: Dict[str, threading.Lock] = {}
        self._batch_owners: Dict[str, int] = {}  # batch_id → thread id

    def _get_lock(self, batch_id: str) -> threading.Lock:
        with self._global_lock:
            if batch_id not in self._batch_locks:
                self._batch_locks[batch_id] = threading.Lock()
            return self._batch_locks[batch_id]

    @contextmanager
    def acquire(self, batch_id: str) -> Generator[bool, None, None]:
        """
        Context manager that yields True if lock acquired, False if
        the batch is already being reconciled by another thread.
        Lock is always released on exit.
        """
        lock = self._get_lock(batch_id)
        acquired = lock.acquire(blocking=False)
        if acquired:
            self._batch_owners[batch_id] = threading.get_ident()
        try:
            yield acquired
        finally:
            if acquired:
                self._batch_owners.pop(batch_id, None)
                lock.release()

    def is_locked(self, batch_id: str) -> bool:
        lock = self._get_lock(batch_id)
        if lock.acquire(blocking=False):
            lock.release()
            return False
        return True

    def cleanup_batch(self, batch_id: str) -> None:
        """Remove lock for a terminal batch to avoid memory leak."""
        with self._global_lock:
            self._batch_locks.pop(batch_id, None)
            self._batch_owners.pop(batch_id, None)
