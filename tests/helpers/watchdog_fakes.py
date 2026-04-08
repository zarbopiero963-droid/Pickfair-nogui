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
                    "severity": "warning",
                    "description": "inflight not progressing",
                    "source": "watchdog",
                }
            ]
        return []

    def evaluate(self, context: dict[str, Any]) -> list[dict[str, Any]]:
        return self.run(context)


def normalize_alerts_snapshot(snapshot: dict[str, Any] | None) -> dict[str, list[dict[str, Any]]]:
    """Normalize snapshot payload to always expose {'alerts': [...]} shape."""

    normalized = snapshot if isinstance(snapshot, dict) else {}
    alerts = normalized.get("alerts")
    if not isinstance(alerts, list):
        alerts = []

    normalized_alerts: list[dict[str, Any]] = []
    for item in alerts:
        if isinstance(item, dict):
            normalized_alerts.append(item)

    return {"alerts": normalized_alerts}


def get_alert(snapshot: dict[str, Any] | None, code: str) -> dict[str, Any] | None:
    for alert in normalize_alerts_snapshot(snapshot)["alerts"]:
        if alert.get("code") == code:
            return alert
    return None
