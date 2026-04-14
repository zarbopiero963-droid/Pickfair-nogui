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
            "db_lock_errors": 2,
            "network_timeout_count": 1,
            "ambiguous_submissions": 1,
            "writer_backlog": 60,
            "memory_growth_mb": 130,
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


def test_stuck_inflight_under_lag_and_cascade_pattern_named_coverage():
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
    codes = {a["code"] for a in alerts.active_alerts()}
    assert "CTO::STALLED_SYSTEM_DETECTED" in codes
    assert "CTO::CASCADE_FAILURE_RISK" in codes


def test_silent_failure_detected_from_enabled_undeliverable_pipeline_integrated():
    class _ProbeWithUndeliverable(_Probe):
        def collect_forensics_evidence(self):
            return {"diagnostics_export": {"manifest_files": []}}

    alerts = AlertsManager()
    svc = WatchdogService(
        probe=_ProbeWithUndeliverable(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=IncidentsManager(),
        snapshot_service=_Snapshot(),
        anomaly_engine=_Anomaly(),
        forensics_engine=_Forensics(),
    )
    svc.tick()
    codes = {a["code"] for a in alerts.active_alerts()}
    assert "CTO::SILENT_FAILURE_DETECTED" in codes
    assert "CTO::OBSERVABILITY_UNTRUSTED" in codes


def test_cto_alert_not_resolved_just_because_reviewer_is_in_cooldown():
    class _StableProbe(_Probe):
        def collect_forensics_evidence(self):
            return {"diagnostics_export": {"manifest_files": []}}

    alerts = AlertsManager()
    svc = WatchdogService(
        probe=_StableProbe(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=IncidentsManager(),
        snapshot_service=_Snapshot(),
        anomaly_engine=_Anomaly(),
        forensics_engine=_Forensics(),
    )
    svc.tick()
    first_codes = {a["code"] for a in alerts.active_alerts() if a.get("source") == "cto_reviewer"}
    assert "CTO::SILENT_FAILURE_DETECTED" in first_codes
    svc.tick()  # within reviewer cooldown, no new emission expected
    second_codes = {a["code"] for a in alerts.active_alerts() if a.get("source") == "cto_reviewer"}
    assert "CTO::SILENT_FAILURE_DETECTED" in second_codes


def test_cto_reviewer_pass_handles_runtime_probe_exception_without_aborting_tick():
    class _BrokenProbe(_Probe):
        def collect_runtime_state(self):
            raise RuntimeError("runtime probe failed")

    class _SnapshotCounter(_Snapshot):
        def __init__(self):
            self.calls = 0

        def collect_and_store(self):
            self.calls += 1

    snapshot = _SnapshotCounter()
    svc = WatchdogService(
        probe=_BrokenProbe(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=AlertsManager(),
        incidents_manager=IncidentsManager(),
        snapshot_service=snapshot,
        anomaly_engine=_Anomaly(),
        forensics_engine=_Forensics(),
    )
    svc.tick()
    assert snapshot.calls == 1
