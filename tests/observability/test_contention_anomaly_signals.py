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


def _watchdog_for(probe, alerts_manager=None):
    return WatchdogService(
        probe=probe,
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts_manager or AlertsManager(),
        incidents_manager=IncidentsManager(),
        snapshot_service=_Snapshot(),
    )


def test_contention_signal_flows_through_default_watchdog_path():
    alerts = AlertsManager()
    watchdog = _watchdog_for(_Probe(), alerts_manager=alerts)
    watchdog.tick()
    codes = {a["code"] for a in alerts.active_alerts()}
    assert "DB_CONTENTION_DETECTED" in codes
    assert "CTO::CASCADE_FAILURE_RISK" in codes


def test_canonical_contention_and_ambiguity_matrix_preserves_evidence_chain():
    class _MatrixProbe:
        def collect_health(self):
            return {"runtime": {"status": "DEGRADED", "reason": "writer-stall", "details": {"heartbeat_gap_sec": 45}}}

        def collect_metrics(self):
            return {
                "db_lock_errors": 3,
                "db_writer_queue_high_watermark": 150,
                "db_writer_seconds_since_last_write": 90,
                "heartbeat_age": 45,
                "last_heartbeat_age_sec": 45,
                "stalled_ticks": 4,
                "completed_delta": 0,
                "ambiguous_submissions": 2,
                "runtime_io_degraded_total": 3,
            }

        def collect_runtime_state(self):
            return {
                "component": "runtime-matrix",
                "mode": "simulation",
                "runtime_io": {"degraded_count": 3, "slow_count": 1},
                "alert_pipeline": {"enabled": True, "deliverable": False},
            }

        def collect_forensics_evidence(self):
            return {
                "diagnostics_export": {
                    "manifest_files": ["runtime_state.json", "metrics.json", "alerts.json"],
                    "contention": {"events": 3, "locked_transient": True},
                    "writer": {"backlog": 150, "stall_seconds": 90},
                }
            }

        def collect_reviewer_context(self):
            return {
                "db": {"contention_events": 3, "db_writer_backlog": 150, "locked_transient": True},
                "runtime_state": {
                    "alert_pipeline": {"enabled": True, "deliverable": False},
                    "runtime_io": {"degraded_count": 3},
                },
            }

    alerts = AlertsManager()
    watchdog = _watchdog_for(_MatrixProbe(), alerts_manager=alerts)

    watchdog.tick()
    active = {a["code"]: a for a in alerts.active_alerts()}

    assert "DB_CONTENTION_DETECTED" in active
    assert "CTO::CASCADE_FAILURE_RISK" in active
    assert "CTO::SILENT_FAILURE_DETECTED" in active

    risk_codes = {
        code
        for code, row in active.items()
        if code.startswith("CTO::") and row.get("severity") in {"high", "critical"}
    }
    assert risk_codes, "combined contention + ambiguity should escalate as operator actionable/critical"

    evidence = active["DB_CONTENTION_DETECTED"].get("details", {})
    assert evidence["contention_events"] == 3
    assert evidence["db_writer_backlog"] == 150
    assert evidence["db_writer_backlog_threshold"] == 50

    stalled = active["CTO::STALLED_SYSTEM_DETECTED"]["details"]
    assert stalled["key_metrics"]["stalled_ticks"] == 4
    assert stalled["key_metrics"]["completed_delta"] == 0.0

    cascade = active["CTO::CASCADE_FAILURE_RISK"]["details"]
    assert cascade["key_metrics"]["ambiguous_submissions"] == 2
    assert cascade["key_metrics"]["db_lock_errors"] == 3
