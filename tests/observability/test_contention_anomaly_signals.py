from __future__ import annotations

from observability.alerts_manager import AlertsManager
from observability.health_registry import HealthRegistry
from observability.incidents_manager import IncidentsManager
from observability.metrics_registry import MetricsRegistry
from observability.watchdog_service import WatchdogService


class _Probe:
    def collect_health(self):
        return {"runtime": {"status": "DEGRADED", "reason": "db-contention", "details": {}}}

    def collect_metrics(self):
        return {"stalled_ticks": 3, "completed_delta": 0, "db_lock_errors": 2, "ambiguous_submissions": 1, "network_timeout_count": 1}

    def collect_runtime_state(self):
        return {"alert_pipeline": {"enabled": True, "deliverable": False}, "component": "runtime"}

    def collect_forensics_evidence(self):
        return {"diagnostics_export": {"manifest_files": []}}

    def collect_reviewer_context(self):
        return {"db": {"contention_events": 2, "db_writer_backlog": 60}, "runtime_state": {"alert_pipeline": {"enabled": True, "deliverable": False}}}


class _Snapshot:
    def collect_and_store(self):
        return None


def test_contention_signal_flows_through_default_watchdog_path():
    alerts = AlertsManager()
    watchdog = WatchdogService(
        probe=_Probe(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=IncidentsManager(),
        snapshot_service=_Snapshot(),
    )
    watchdog.tick()
    codes = {a["code"] for a in alerts.active_alerts()}
    assert "DB_CONTENTION_DETECTED" in codes
    assert "CTO::CASCADE_FAILURE_RISK" in codes
