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


def _alerts_by_code(alerts: AlertsManager) -> dict[str, dict]:
    snapshot = normalize_alerts_snapshot(alerts.snapshot())
    return {a["code"]: a for a in snapshot["alerts"] if "code" in a}


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
    first = _alerts_by_code(alerts)
    assert "STUCK_INFLIGHT" in first
    assert first["STUCK_INFLIGHT"]["active"] is True
    assert first["SYSTEM_WARN"]["active"] is True

    first_stuck = get_alert(first_snapshot, "STUCK_INFLIGHT")
    assert first_stuck is not None
    assert first_stuck["active"] is True

    watchdog._evaluate_anomalies()
    second_snapshot = normalize_alerts_snapshot(alerts.snapshot())
    second = _alerts_by_code(alerts)
    assert "STUCK_INFLIGHT" in second
    assert second["STUCK_INFLIGHT"]["active"] is False
    assert second["SYSTEM_WARN"]["active"] is True

    second_stuck = get_alert(second_snapshot, "STUCK_INFLIGHT")
    assert second_stuck is not None
    assert second_stuck["active"] is False

    system_warn = get_alert(second_snapshot, "SYSTEM_WARN")
    assert system_warn is not None
    assert system_warn["active"] is True
