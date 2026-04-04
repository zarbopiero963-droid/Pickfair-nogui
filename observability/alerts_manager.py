from __future__ import annotations

import threading
import time
import uuid
from typing import Any, Dict, List, Optional


class AlertsManager:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._alerts: Dict[str, Dict[str, Any]] = {}

    def upsert_alert(
        self,
        code: str,
        severity: str,
        message: str,
        *,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        now = time.time()
        with self._lock:
            existing = self._alerts.get(code)
            if existing:
                existing["severity"] = severity
                existing["message"] = message
                existing["details"] = dict(details or {})
                existing["last_seen_at"] = now
                existing["count"] += 1
                existing["active"] = True
                return

            self._alerts[code] = {
                "alert_id": str(uuid.uuid4()),
                "code": code,
                "severity": severity,
                "message": message,
                "details": dict(details or {}),
                "first_seen_at": now,
                "last_seen_at": now,
                "count": 1,
                "active": True,
            }

    def resolve_alert(self, code: str) -> None:
        with self._lock:
            item = self._alerts.get(code)
            if item:
                item["active"] = False
                item["resolved_at"] = time.time()

    def active_alerts(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [dict(v) for v in self._alerts.values() if v.get("active")]

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            items = [dict(v) for v in self._alerts.values()]
        return {
            "alerts": items,
            "active_count": sum(1 for x in items if x.get("active")),
            "updated_at": time.time(),
        }
