from __future__ import annotations

import logging
import threading
import time
import uuid
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class AlertsManager:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._alerts: Dict[str, Dict[str, Any]] = {}
        self._notifiers: List[Any] = []

    def register_notifier(self, fn) -> None:
        with self._lock:
            self._notifiers.append(fn)

    def upsert_alert(
        self,
        code: str,
        severity: str,
        message: str,
        *,
        source: str = "system",
        title: Optional[str] = None,
        description: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        now = time.time()

        with self._lock:
            existing = self._alerts.get(code)
            if existing:
                existing["severity"] = severity
                existing["message"] = message
                existing["title"] = title or message
                existing["description"] = description
                existing["source"] = source
                existing["details"] = dict(details or {})
                existing["last_seen_at"] = now
                existing["count"] += 1
                existing["active"] = True
                alert = dict(existing)
            else:
                alert = {
                    "alert_id": str(uuid.uuid4()),
                    "code": code,
                    "severity": severity,
                    "message": message,
                    "title": title or message,
                    "description": description,
                    "source": source,
                    "details": dict(details or {}),
                    "first_seen_at": now,
                    "last_seen_at": now,
                    "count": 1,
                    "active": True,
                }
                self._alerts[code] = dict(alert)

        self._notify_all(alert)
        return alert

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
            rows = [dict(v) for v in self._alerts.values()]
        return {
            "alerts": rows,
            "active_count": sum(1 for x in rows if x.get("active")),
            "updated_at": time.time(),
        }

    def _notify_all(self, alert: Dict[str, Any]) -> None:
        with self._lock:
            notifiers = list(self._notifiers)

        for fn in notifiers:
            try:
                fn(dict(alert))
            except Exception:
                logger.exception("AlertsManager notifier failed")
