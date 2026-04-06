from __future__ import annotations

import threading
import time
from typing import Any, Dict, Optional


READY = "READY"
DEGRADED = "DEGRADED"
NOT_READY = "NOT_READY"


class HealthRegistry:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._components: Dict[str, Dict[str, Any]] = {}

    def set_component(
        self,
        name: str,
        status: str,
        *,
        reason: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        now = time.time()
        with self._lock:
            self._components[name] = {
                "name": name,
                "status": status,
                "reason": reason,
                "details": dict(details or {}),
                "updated_at": now,
            }

    def get_component(self, name: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            item = self._components.get(name)
            return dict(item) if item else None

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            components = {k: dict(v) for k, v in self._components.items()}

        states = [x["status"] for x in components.values()]
        if not states:
            overall = NOT_READY
        elif any(s == NOT_READY for s in states):
            overall = NOT_READY
        elif any(s == DEGRADED for s in states):
            overall = DEGRADED
        else:
            overall = READY

        return {
            "overall_status": overall,
            "components": components,
            "updated_at": time.time(),
        }
