from __future__ import annotations

import threading


class ExecutionGuard:
    """
    Protegge da:
    - doppio ordine
    - race condition
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._active = set()

    def acquire(self, key: str) -> bool:
        with self._lock:
            if key in self._active:
                return False
            self._active.add(key)
            return True

    def release(self, key: str) -> None:
        with self._lock:
            self._active.discard(key)

    def snapshot(self):
        return list(self._active)