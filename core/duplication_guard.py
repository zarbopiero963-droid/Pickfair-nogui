from __future__ import annotations

import time
from typing import Dict, Optional


class DuplicationGuard:
    def __init__(self, ttl_seconds: int = 6 * 60 * 60):
        self.ttl_seconds = int(ttl_seconds)
        self._active: Dict[str, float] = {}

    def cleanup(self) -> None:
        now = time.time()
        expired = [
            key for key, ts in self._active.items()
            if now - ts > self.ttl_seconds
        ]
        for key in expired:
            self._active.pop(key, None)

    def build_event_key(self, signal: dict) -> str:
        event = str(
            signal.get("event")
            or signal.get("match")
            or signal.get("event_name")
            or ""
        ).strip().lower()

        market_id = str(signal.get("market_id") or signal.get("marketId") or "").strip()
        market = str(
            signal.get("market")
            or signal.get("market_name")
            or signal.get("marketName")
            or signal.get("market_type")
            or ""
        ).strip().lower()

        selection_id = str(
            signal.get("selection_id")
            or signal.get("selectionId")
            or ""
        ).strip()

        selection = str(signal.get("selection") or signal.get("runner_name") or "").strip().lower()
        side = str(signal.get("bet_type") or signal.get("side") or signal.get("action") or "BACK").strip().upper()

        parts = [
            event,
            market_id or market,
            selection_id or selection,
            side,
        ]
        return "|".join([p for p in parts if p])

    def is_duplicate(self, event_key: str) -> bool:
        self.cleanup()
        return bool(event_key and event_key in self._active)

    def register(self, event_key: str) -> None:
        if event_key:
            self._active[event_key] = time.time()

    def release(self, event_key: str) -> None:
        if event_key:
            self._active.pop(event_key, None)

    def snapshot(self) -> dict:
        self.cleanup()
        return {
            "active_keys": list(self._active.keys()),
            "count": len(self._active),
            "ttl_seconds": self.ttl_seconds,
        }
