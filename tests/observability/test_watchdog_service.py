from observability.alerts_manager import AlertsManager
from observability.health_registry import HealthRegistry
from observability.incidents_manager import IncidentsManager
from observability.metrics_registry import MetricsRegistry
from observability.watchdog_service import WatchdogService
from tests.helpers.watchdog_fakes import (
    FakeAnomalyEngineSequence,
    get_alert,
    normalize_alerts_snapshot,
)


class _ProbeStub:
    def collect_health(self):
        return {"runtime": {"status": "READY", "reason": "ok", "details": {}}}

    def collect_metrics(self):
        return {}

    def collect_runtime_state(self):
        return {}


class _SnapshotStub:
    def collect_and_store(self):
        return None


def test_watchdog_resolves_stale_anomaly_alert_without_touching_unrelated_alerts():
    alerts = AlertsManager()
    alerts.upsert_alert("SYSTEM_WARN", "warning", "keep me", source="system")

    watchdog = WatchdogService(
        probe=_ProbeStub(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        anomaly_engine=FakeAnomalyEngineSequence(),
        interval_sec=60.0,
    )

    watchdog._evaluate_anomalies()
    first_snapshot = normalize_alerts_snapshot(alerts.snapshot())

    first_stuck = get_alert(first_snapshot, "STUCK_INFLIGHT")
    assert first_stuck is not None
    assert first_stuck["active"] is True
    assert first_stuck["severity"] == "HIGH"

    first_system_warn = get_alert(first_snapshot, "SYSTEM_WARN")
    assert first_system_warn is not None
    assert first_system_warn["active"] is True
    assert first_system_warn.get("source") == "system"

    watchdog._evaluate_anomalies()
    second_snapshot = normalize_alerts_snapshot(alerts.snapshot())

    second_stuck = get_alert(second_snapshot, "STUCK_INFLIGHT")
    assert second_stuck is not None
    assert second_stuck["active"] is False

    second_system_warn = get_alert(second_snapshot, "SYSTEM_WARN")
    assert second_system_warn is not None
    assert second_system_warn["active"] is True
    assert second_system_warn.get("source") == "system"
