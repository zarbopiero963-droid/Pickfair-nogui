from __future__ import annotations

from datetime import datetime
from threading import RLock
from typing import Any, Dict, List, Set


class DuplicationGuard:
    """
    Anti-duplicazione ordini/eventi.

    Scopo:
    - impedire doppia esposizione tossica sullo stesso evento/runner
    - registrare event_key attivi
    - rilasciare la chiave quando l'ordine/posizione termina

    La chiave base è costruita da:
    - market_id
    - selection_id
    - bet_type

    Se vuoi più ampiezza puoi passare event_key già calcolato dal runtime.
    """

    def __init__(self):
        self._lock = RLock()
        self._active_keys: Set[str] = set()
        self._registered_at: Dict[str, str] = {}

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
            or payload.get("action")
            or "BACK"
        ).upper().strip()

        return f"{market_id}:{selection_id}:{bet_type}"

    # =========================================================
    # REGISTER / CHECK / RELEASE
    # =========================================================
    def register(self, event_key: str) -> None:
        key = str(event_key or "").strip()
        if not key:
            return

        with self._lock:
            self._active_keys.add(key)
            self._registered_at[key] = datetime.utcnow().isoformat()

    def release(self, event_key: str) -> None:
        key = str(event_key or "").strip()
        if not key:
            return

        with self._lock:
            self._active_keys.discard(key)
            self._registered_at.pop(key, None)

    def is_duplicate(self, event_key: str) -> bool:
        key = str(event_key or "").strip()
        if not key:
            return False

        with self._lock:
            return key in self._active_keys

    def clear(self) -> None:
        with self._lock:
            self._active_keys.clear()
            self._registered_at.clear()

    # =========================================================
    # SNAPSHOT
    # =========================================================
    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            keys: List[Dict[str, str]] = []
            for key in sorted(self._active_keys):
                keys.append(
                    {
                        "event_key": key,
                        "registered_at": self._registered_at.get(key, ""),
                    }
                )

            return {
                "active_count": len(self._active_keys),
                "active_keys": keys,
            }