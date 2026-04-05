import json
import zipfile

from observability.alerts_manager import AlertsManager
from observability.diagnostic_bundle_builder import DiagnosticBundleBuilder
from observability.diagnostics_service import DiagnosticsService
from observability.health_registry import HealthRegistry
from observability.incidents_manager import IncidentsManager
from observability.metrics_registry import MetricsRegistry
from observability.watchdog_service import WatchdogService


class _ProbeNotReady:
    def collect_health(self):
        return {
            "runtime": {
                "status": "NOT_READY",
                "reason": "critical dependency missing",
                "details": {"missing": "database"},
            }
        }

    def collect_metrics(self):
        return {"memory_rss_mb": 920.0, "inflight_count": 77.0}


class _SnapshotStub:
    def collect_and_store(self):
        return None


class _RuntimeProbeStub:
    def collect_runtime_state(self):
        return {"mode": "simulation", "breadcrumbs": ["alerted", "degraded", "incident_open"]}



def _export_bundle(tmp_path, health, metrics, alerts, incidents):
    service = DiagnosticsService(
        builder=DiagnosticBundleBuilder(export_dir=str(tmp_path / "exports")),
        probe=_RuntimeProbeStub(),
        health_registry=health,
        metrics_registry=metrics,
        alerts_manager=alerts,
        incidents_manager=incidents,
        db=None,
        safe_mode=None,
        log_paths=[],
    )
    return service.export_bundle()


def test_not_ready_condition_generates_alert_and_incident_evidence(tmp_path):
    health = HealthRegistry()
    metrics = MetricsRegistry()
    alerts = AlertsManager()
    incidents = IncidentsManager()

    watchdog = WatchdogService(
        probe=_ProbeNotReady(),
        health_registry=health,
        metrics_registry=metrics,
        alerts_manager=alerts,
        incidents_manager=incidents,
        snapshot_service=_SnapshotStub(),
        interval_sec=60.0,
    )

    watchdog._tick()

    active_codes = {a["code"] for a in alerts.active_alerts()}
    assert "SYSTEM_NOT_READY" in active_codes
    assert "MEMORY_HIGH" in active_codes
    assert "INFLIGHT_HIGH" in active_codes

    incident_snapshot = incidents.snapshot()
    system_incidents = [i for i in incident_snapshot["incidents"] if i["code"] == "SYSTEM_NOT_READY"]
    assert system_incidents and system_incidents[0]["status"] == "OPEN"

    bundle_path = _export_bundle(tmp_path, health, metrics, alerts, incidents)
    with zipfile.ZipFile(bundle_path, "r") as zf:
        alerts_payload = json.loads(zf.read("alerts.json"))
        incidents_payload = json.loads(zf.read("incidents.json"))

    assert any(a["code"] == "SYSTEM_NOT_READY" and a["active"] for a in alerts_payload["alerts"])
    assert any(i["code"] == "SYSTEM_NOT_READY" and i["status"] == "OPEN" for i in incidents_payload["incidents"])


def test_diagnostics_export_preserves_ambiguous_and_duplicate_like_evidence(tmp_path):
    health = HealthRegistry()
    health.set_component("runtime", "DEGRADED", reason="signal drift")

    metrics = MetricsRegistry()
    metrics.inc("duplicate_events", 2)
    metrics.set_gauge("inflight_count", 51)

    alerts = AlertsManager()
    alerts.upsert_alert(
        "AMBIGUOUS_SIGNAL",
        "warning",
        "ambiguous state detected",
        details={"raw_states": ["READY", "DEGRADED"], "duplicate_correlation_ids": ["c-1", "c-1"]},
    )

    incidents = IncidentsManager()
    incidents.open_incident(
        "AMBIGUOUS_SIGNAL",
        "Ambiguous signal",
        "warning",
        details={"cause": "classification mismatch"},
    )
    incidents.add_event("AMBIGUOUS_SIGNAL", "duplicate evidence retained", details={"count": 2})

    bundle_path = _export_bundle(tmp_path, health, metrics, alerts, incidents)

    with zipfile.ZipFile(bundle_path, "r") as zf:
        runtime_state = json.loads(zf.read("runtime_state.json"))
        alerts_payload = json.loads(zf.read("alerts.json"))
        incidents_payload = json.loads(zf.read("incidents.json"))
        metrics_payload = json.loads(zf.read("metrics.json"))

    alert_row = next(a for a in alerts_payload["alerts"] if a["code"] == "AMBIGUOUS_SIGNAL")
    incident_row = next(i for i in incidents_payload["incidents"] if i["code"] == "AMBIGUOUS_SIGNAL")

    assert alert_row["details"]["duplicate_correlation_ids"] == ["c-1", "c-1"]
    assert incident_row["events"] and incident_row["events"][0]["details"]["count"] == 2
    assert "breadcrumbs" in runtime_state
    assert metrics_payload["counters"]["duplicate_events"] == 2
