from __future__ import annotations

from observability.alerts_manager import AlertsManager
from observability.health_registry import HealthRegistry
from observability.incidents_manager import IncidentsManager
from observability.metrics_registry import MetricsRegistry
from observability.watchdog_service import WatchdogService


class _SnapshotStub:
    def collect_and_store(self):
        return None


class _TelegramProbe:
    def __init__(self, health: dict):
        self.health = dict(health)

    def set_health(self, health: dict) -> None:
        self.health = dict(health)

    def collect_health(self):
        return {"runtime": {"status": "READY", "reason": "ok", "details": {}}}

    def collect_metrics(self):
        return {}

    def collect_runtime_state(self):
        return {"telegram_health": dict(self.health)}


def _make_watchdog(probe: _TelegramProbe, alerts: AlertsManager | None = None) -> WatchdogService:
    return WatchdogService(
        probe=probe,
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts or AlertsManager(),
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        interval_sec=60.0,
    )


def _base_health(**overrides):
    base = {
        "state": "CONNECTED",
        "healthy": True,
        "degraded": False,
        "failed": False,
        "invariant_ok": True,
        "intentional_stop": False,
        "reconnect_in_progress": False,
        "last_error": "",
        "reconnect_attempts": 0,
        "active_alert_codes": [],
        "checked_at": "2026-04-15T00:00:00+00:00",
    }
    base.update(overrides)
    return base


def test_no_alert_when_telegram_healthy():
    probe = _TelegramProbe(_base_health())
    alerts = AlertsManager()
    watchdog = _make_watchdog(probe, alerts=alerts)

    watchdog._evaluate_alerts()

    assert not [a for a in alerts.active_alerts() if a.get("source") == "telegram_watchdog"]


def test_warning_when_reconnecting():
    probe = _TelegramProbe(_base_health(state="RECONNECTING", reconnect_in_progress=True, checked_at="2026-04-15T00:00:00+00:00"))
    alerts = AlertsManager()
    watchdog = _make_watchdog(probe, alerts=alerts)

    watchdog._evaluate_alerts()
    probe.set_health(_base_health(state="RECONNECTING", reconnect_in_progress=True, checked_at="2026-04-15T00:00:25+00:00"))
    watchdog._evaluate_alerts()

    row = next(a for a in alerts.active_alerts() if a["code"] == "TELEGRAM_RECONNECTING")
    assert row["severity"] == "warning"


def test_error_when_failed():
    probe = _TelegramProbe(_base_health(state="FAILED", failed=True, last_error="boom"))
    alerts = AlertsManager()
    watchdog = _make_watchdog(probe, alerts=alerts)

    watchdog._evaluate_alerts()

    row = next(a for a in alerts.active_alerts() if a["code"] == "TELEGRAM_FAILED")
    assert row["severity"] == "error"


def test_critical_when_invariant_broken():
    probe = _TelegramProbe(
        _base_health(
            state="CONNECTED",
            invariant_ok=False,
            active_alert_codes=["CONNECTED_REQUIRES_CLIENT_ALIVE"],
        )
    )
    alerts = AlertsManager()
    watchdog = _make_watchdog(probe, alerts=alerts)

    watchdog._evaluate_alerts()

    row = next(a for a in alerts.active_alerts() if a["code"] == "TELEGRAM_INVARIANT_BROKEN")
    assert row["severity"] == "critical"


def test_no_false_positive_during_startup():
    probe = _TelegramProbe(_base_health(state="CONNECTING", healthy=False, degraded=True, checked_at="2026-04-15T00:00:00+00:00"))
    alerts = AlertsManager()
    watchdog = _make_watchdog(probe, alerts=alerts)

    watchdog._evaluate_alerts()

    assert not [a for a in alerts.active_alerts() if a["code"] == "TELEGRAM_DISCONNECTED"]


def test_no_false_positive_during_reconnect_grace():
    probe = _TelegramProbe(_base_health(state="RECONNECTING", reconnect_in_progress=True, checked_at="2026-04-15T00:00:00+00:00"))
    alerts = AlertsManager()
    watchdog = _make_watchdog(probe, alerts=alerts)

    watchdog._evaluate_alerts()

    assert not [a for a in alerts.active_alerts() if a["code"] == "TELEGRAM_RECONNECTING"]


def test_stale_detection_and_escalation():
    probe = _TelegramProbe(
        _base_health(
            state="CONNECTED",
            healthy=False,
            degraded=True,
            invariant_ok=False,
            active_alert_codes=["STALE_RUNTIME"],
            checked_at="2026-04-15T00:02:00+00:00",
        )
    )
    alerts = AlertsManager()
    watchdog = _make_watchdog(probe, alerts=alerts)

    watchdog._evaluate_alerts()
    probe.set_health(
        _base_health(
            state="CONNECTED",
            healthy=False,
            degraded=True,
            invariant_ok=False,
            active_alert_codes=["STALE_RUNTIME"],
            checked_at="2026-04-15T00:03:00+00:00",
        )
    )
    watchdog._evaluate_alerts()

    row = next(a for a in alerts.active_alerts() if a["code"] == "TELEGRAM_STALE")
    assert row["severity"] == "warning"


def test_alert_deduplication():
    probe = _TelegramProbe(_base_health(state="FAILED", failed=True, checked_at="2026-04-15T00:00:00+00:00"))
    alerts = AlertsManager()
    watchdog = _make_watchdog(probe, alerts=alerts)

    watchdog._evaluate_alerts()
    watchdog._evaluate_alerts()

    failed_rows = [a for a in alerts.active_alerts() if a["code"] == "TELEGRAM_FAILED"]
    assert len(failed_rows) == 1
    assert failed_rows[0]["count"] >= 2


def test_alert_resolution_when_recovered():
    probe = _TelegramProbe(_base_health(state="FAILED", failed=True, checked_at="2026-04-15T00:00:00+00:00"))
    alerts = AlertsManager()
    watchdog = _make_watchdog(probe, alerts=alerts)

    watchdog._evaluate_alerts()
    probe.set_health(_base_health(state="CONNECTED", healthy=True, failed=False, checked_at="2026-04-15T00:01:00+00:00"))
    watchdog._evaluate_alerts()

    snapshot = alerts.snapshot()["alerts"]
    failed_row = next(a for a in snapshot if a["code"] == "TELEGRAM_FAILED")
    assert failed_row["active"] is False


def test_watchdog_does_not_affect_unrelated_alerts():
    probe = _TelegramProbe(_base_health(state="FAILED", failed=True, checked_at="2026-04-15T00:00:00+00:00"))
    alerts = AlertsManager()
    alerts.upsert_alert("SYSTEM_WARN", "warning", "keep", source="system")
    watchdog = _make_watchdog(probe, alerts=alerts)

    watchdog._evaluate_alerts()
    probe.set_health(_base_health(state="CONNECTED", checked_at="2026-04-15T00:01:00+00:00"))
    watchdog._evaluate_alerts()

    system_row = next(a for a in alerts.snapshot()["alerts"] if a["code"] == "SYSTEM_WARN")
    assert system_row["active"] is True
