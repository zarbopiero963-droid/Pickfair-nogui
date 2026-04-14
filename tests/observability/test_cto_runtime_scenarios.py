from observability.watchdog_service import WatchdogService
from observability.alerts_manager import AlertsManager
from observability.health_registry import HealthRegistry
from observability.incidents_manager import IncidentsManager
from observability.metrics_registry import MetricsRegistry


class _Probe:
    def collect_health(self):
        return {"runtime": {"status": "DEGRADED", "reason": "lag", "details": {}}}

    def collect_metrics(self):
        return {
            "stalled_ticks": 3,
            "completed_delta": 0,
            "missing_observability_sections": 2,
        }

    def collect_runtime_state(self):
        return {"alert_pipeline": {"enabled": True, "deliverable": False}, "component": "runtime"}

    def collect_forensics_evidence(self):
        return {}


class _Snapshot:
    def collect_and_store(self):
        return None


class _Anomaly:
    def evaluate(self, _):
        return [{"code": "STUCK_INFLIGHT", "severity": "high", "message": "stalled"}]


class _Forensics:
    def evaluate(self, _):
        return [{"code": "LOCAL_VS_REMOTE_MISMATCH", "severity": "high", "message": "mismatch"}]


def test_watchdog_tick_exposes_cto_findings_after_anomaly_and_forensics():
    alerts = AlertsManager()
    svc = WatchdogService(
        probe=_Probe(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=IncidentsManager(),
        snapshot_service=_Snapshot(),
        anomaly_engine=_Anomaly(),
        forensics_engine=_Forensics(),
    )
    svc.tick()
    active_codes = {a["code"] for a in alerts.active_alerts()}
    assert "STUCK_INFLIGHT" in active_codes
    assert "LOCAL_VS_REMOTE_MISMATCH" in active_codes
    assert any(code.startswith("CTO::") for code in active_codes)
