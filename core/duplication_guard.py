from __future__ import annotations

import time
from datetime import datetime
from threading import RLock
from typing import Any, Dict, List


class DuplicationGuard:
    """
    Duplication Guard PRO

    - check + register atomico
    - TTL automatico
    - evita memory leak
    - supporta strategie diverse
    """

    def __init__(self, ttl_seconds: int = 120):
        self._lock = RLock()

        # key -> timestamp
        self._active: Dict[str, float] = {}

        # metadata debug
        self._registered_at: Dict[str, str] = {}

        self.ttl = int(ttl_seconds)

    # =========================================================
    # KEY BUILD
    # =========================================================
    def build_event_key(self, payload: Dict[str, Any]) -> str:
        payload = dict(payload or {})

        market_id = str(
            payload.get("market_id")
            or payload.get("marketId")
            or ""
        ).strip()

        selection_id = str(
            payload.get("selection_id")
            or payload.get("selectionId")
            or ""
        ).strip()

        bet_type = str(
            payload.get("bet_type")
            or payload.get("side")
            or "BACK"
        ).upper().strip()

        strategy = str(
            payload.get("strategy")
            or payload.get("source")
            or "default"
        ).lower().strip()

        return f"{market_id}:{selection_id}:{bet_type}:{strategy}"

    # =========================================================
    # ATOMIC CHECK + REGISTER
    # =========================================================
    def acquire(self, event_key: str) -> bool:
        """
        True → puoi eseguire ordine
        False → duplicato
        """
        key = str(event_key or "").strip()
        if not key:
            return False

        now = time.time()

        with self._lock:
            self._cleanup_locked(now)

            if key in self._active:
                return False

            self._active[key] = now
            self._registered_at[key] = datetime.utcnow().isoformat()

            return True


    def register_startup_order(self, payload: Dict[str, Any]) -> bool:
        """
        Seed della guardia da ordini già vivi su exchange al restart.
        True se registrato, False se payload insufficiente o già presente.
        """
        order = dict(payload or {})
        event_key = str(order.get("event_key") or "").strip()
        if not event_key:
            event_key = self.build_event_key(order)
        return self.acquire(event_key)

    # =========================================================
    # TWO-PHASE INTERFACE (is_duplicate / register)
    # Called by dutching_controller and other runtime paths.
    # =========================================================
    def is_duplicate(self, event_key: str) -> bool:
        """
        Returns True if the key is already active (i.e. IS a duplicate).
        Does NOT register the key.
        """
        key = str(event_key or "").strip()
        if not key:
            return False

        now = time.time()
        with self._lock:
            self._cleanup_locked(now)
            return key in self._active

    def register(self, event_key: str) -> None:
        """
        Register a key as active without atomically checking first.
        Idempotent: registering an already-active key refreshes its timestamp.
        """
        key = str(event_key or "").strip()
        if not key:
            return

        now = time.time()
        with self._lock:
            self._active[key] = now
            self._registered_at[key] = datetime.utcnow().isoformat()

    # =========================================================
    # RELEASE
    # =========================================================
    def release(self, event_key: str) -> None:
        key = str(event_key or "").strip()
        if not key:
            return

        with self._lock:
            self._active.pop(key, None)
            self._registered_at.pop(key, None)

    # =========================================================
    # CLEANUP TTL
    # =========================================================
    def _cleanup_locked(self, now: float):
        if not self._active:
            return

        expired = [
            k for k, ts in self._active.items()
            if (now - ts) > self.ttl
        ]

        for k in expired:
            self._active.pop(k, None)
            self._registered_at.pop(k, None)

    # =========================================================
    # SNAPSHOT
    # =========================================================
    def snapshot(self) -> Dict[str, Any]:
        now = time.time()

        with self._lock:
            self._cleanup_locked(now)

            keys: List[Dict[str, str]] = []

            for key in sorted(self._active):
                keys.append({
                    "event_key": key,
                    "registered_at": self._registered_at.get(key, ""),
                })

            return {
                "active_count": len(self._active),
                "active_keys": keys,
            }

    # =========================================================
    # CLEAR
    # =========================================================
    def clear(self):
        with self._lock:
            self._active.clear()
            self._registered_at.clear()