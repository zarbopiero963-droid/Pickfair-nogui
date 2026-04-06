from observability.alerts_manager import AlertsManager
from observability.health_registry import HealthRegistry
from observability.incidents_manager import IncidentsManager
from observability.metrics_registry import MetricsRegistry
from observability.watchdog_service import WatchdogService


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


class _AnomalyEngineSequence:
    def __init__(self):
        self.calls = 0

    def evaluate(self, context):
        self.calls += 1
        if self.calls == 1:
            return [{"code": "STUCK_INFLIGHT", "severity": "warning", "message": "stuck", "details": {}}]
        return []


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
        anomaly_engine=_AnomalyEngineSequence(),
        interval_sec=60.0,
    )

    watchdog._evaluate_anomalies()
    first = {a["code"]: a for a in alerts.snapshot()["alerts"]}
    assert first["STUCK_INFLIGHT"]["active"] is True
    assert first["SYSTEM_WARN"]["active"] is True

    watchdog._evaluate_anomalies()
    second = {a["code"]: a for a in alerts.snapshot()["alerts"]}
    assert second["STUCK_INFLIGHT"]["active"] is False
    assert second["SYSTEM_WARN"]["active"] is True
