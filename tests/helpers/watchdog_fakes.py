from __future__ import annotations

from typing import Any


class FakeAnomalyEngineSequence:
    """Deterministic anomaly engine for watchdog tests."""

    def __init__(self) -> None:
        self.step = 0

    def run(self, state: dict[str, Any]) -> list[dict[str, Any]]:
        self.step += 1
        if self.step == 1:
            return [
                {
                    "code": "STUCK_INFLIGHT",
                    "severity": "HIGH",
                    "description": "inflight not progressing",
                    "source": "watchdog",
                }
            ]
        return []

    def evaluate(self, context: dict[str, Any]) -> list[dict[str, Any]]:
        return self.run(context)


def normalize_alerts_snapshot(snapshot: dict[str, Any] | None) -> dict[str, list[dict[str, Any]]]:
    """Normalize snapshot payload to always expose {'alerts': [...]} shape."""

    raw_snapshot = snapshot if isinstance(snapshot, dict) else {}
    raw_alerts = raw_snapshot.get("alerts")
    if not isinstance(raw_alerts, list):
        raw_alerts = []

    normalized_alerts: list[dict[str, Any]] = []
    for item in raw_alerts:
        if not isinstance(item, dict):
            continue

        code = item.get("code")
        if code is None:
            continue

        normalized_alerts.append(
            {
                **item,
                "code": str(code),
                "active": bool(item.get("active", False)),
                "severity": str(item.get("severity", "")),
                "description": str(item.get("description", "")),
            }
        )

    return {"alerts": normalized_alerts}


def get_alert(snapshot: dict[str, Any] | None, code: str) -> dict[str, Any] | None:
    for alert in normalize_alerts_snapshot(snapshot)["alerts"]:
        if alert.get("code") == code:
            return alert
    return None
