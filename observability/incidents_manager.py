from __future__ import annotations

import threading
import time
import uuid
from typing import Any, Dict, List, Optional


class IncidentsManager:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._incidents: Dict[str, Dict[str, Any]] = {}

    def open_incident(
        self,
        code: str,
        title: str,
        severity: str,
        *,
        details: Optional[Dict[str, Any]] = None,
    ) -> str:
        now = time.time()
        with self._lock:
            existing = self._incidents.get(code)
            if existing and existing["status"] == "OPEN":
                return existing["incident_id"]

            incident_id = str(uuid.uuid4())
            self._incidents[code] = {
                "incident_id": incident_id,
                "code": code,
                "title": title,
                "severity": severity,
                "status": "OPEN",
                "opened_at": now,
                "closed_at": None,
                "details": dict(details or {}),
                "events": [],
            }
            return incident_id

    def add_event(self, code: str, message: str, *, details: Optional[Dict[str, Any]] = None) -> None:
        with self._lock:
            incident = self._incidents.get(code)
            if not incident:
                return
            incident["events"].append(
                {
                    "ts": time.time(),
                    "message": message,
                    "details": dict(details or {}),
                }
            )

    def close_incident(
        self,
        code: str,
        *,
        reason: str = "resolved",
        resolved_by: str = "system",
    ) -> None:
        """Close an open incident with structured resolution metadata.

        Parameters
        ----------
        code:
            The incident code to close.
        reason:
            Human-readable reason for resolution (e.g. ``"finding_cleared"``,
            ``"stale_alert_resolved"``).  Stored as ``resolution_reason``.
        resolved_by:
            Source that triggered the closure (e.g. ``"anomaly_reviewer"``,
            ``"correlation_reviewer"``).  Stored as ``resolved_by``.
        """
        now = time.time()
        with self._lock:
            incident = self._incidents.get(code)
            if not incident or incident["status"] != "OPEN":
                return
            incident["status"] = "CLOSED"
            incident["closed_at"] = now
            incident["resolution_reason"] = reason
            incident["resolved_by"] = resolved_by
            incident["events"].append(
                {
                    "ts": now,
                    "message": f"Incident closed: {reason}",
                    "details": {"resolved_by": resolved_by},
                }
            )

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            items = [dict(v) | {"events": list(v.get("events", []))} for v in self._incidents.values()]
        return {
            "incidents": items,
            "open_count": sum(1 for x in items if x["status"] == "OPEN"),
            "updated_at": time.time(),
        }
