import json
import zipfile

from observability.alerts_manager import AlertsManager
from observability.diagnostic_bundle_builder import DiagnosticBundleBuilder
from observability.diagnostics_service import DiagnosticsService
from observability.health_registry import HealthRegistry
from observability.incidents_manager import IncidentsManager
from observability.metrics_registry import MetricsRegistry
from observability.watchdog_service import WatchdogService


class _ProbeReadyButDegraded:
    def collect_health(self):
        return {
            "runtime": {"status": "READY", "reason": "ok", "details": {"loop": "running"}},
            "betfair_service": {"status": "DEGRADED", "reason": "latency", "details": {"ms": 1200}},
        }

    def collect_metrics(self):
        return {"memory_rss_mb": 128.0, "inflight_count": 3.0}


class _SnapshotStub:
    def __init__(self):
        self.calls = 0

    def collect_and_store(self):
        self.calls += 1


class _ProbeRuntimeState:
    def collect_runtime_state(self):
        return {"mode": "simulation", "state": "running"}


class _DbStub:
    def get_recent_orders_for_diagnostics(self, limit=200):
        return [{"order_id": "o-1", "status": "FAILED"}]

    def get_recent_audit_events_for_diagnostics(self, limit=500):
        return [{"event": "REQUEST_RECEIVED", "correlation_id": "c-1"}]



def test_watchdog_tick_produces_coherent_side_effects():
    health = HealthRegistry()
    metrics = MetricsRegistry()
    alerts = AlertsManager()
    incidents = IncidentsManager()
    snapshot = _SnapshotStub()

    watchdog = WatchdogService(
        probe=_ProbeReadyButDegraded(),
        health_registry=health,
        metrics_registry=metrics,
        alerts_manager=alerts,
        incidents_manager=incidents,
        snapshot_service=snapshot,
        interval_sec=60.0,
    )

    watchdog._tick()

    health_snapshot = health.snapshot()
    metrics_snapshot = metrics.snapshot()

    assert snapshot.calls == 1
    assert health_snapshot["components"]["runtime"]["status"] == "READY"
    assert health_snapshot["components"]["betfair_service"]["status"] == "DEGRADED"
    assert health_snapshot["overall_status"] == "DEGRADED"
    assert metrics_snapshot["gauges"]["memory_rss_mb"] == 128.0
    assert metrics_snapshot["gauges"]["inflight_count"] == 3.0


def test_health_overall_status_reflects_component_matrix_without_contradiction():
    health = HealthRegistry()

    assert health.snapshot()["overall_status"] == "NOT_READY"

    health.set_component("runtime", "READY")
    health.set_component("database", "READY")
    assert health.snapshot()["overall_status"] == "READY"

    health.set_component("betfair_service", "DEGRADED", reason="network")
    assert health.snapshot()["overall_status"] == "DEGRADED"

    health.set_component("shutdown_manager", "NOT_READY", reason="missing")
    assert health.snapshot()["overall_status"] == "NOT_READY"


def test_diagnostics_bundle_contains_required_forensics_artifacts(tmp_path):
    health = HealthRegistry()
    health.set_component("runtime", "READY")

    metrics = MetricsRegistry()
    metrics.set_gauge("inflight_count", 1.0)

    alerts = AlertsManager()
    alerts.upsert_alert("RUNTIME_WARN", "warning", "runtime warning")

    incidents = IncidentsManager()
    incidents.open_incident("RUNTIME_WARN", "Runtime warning", "warning")

    service = DiagnosticsService(
        builder=DiagnosticBundleBuilder(export_dir=str(tmp_path / "exports")),
        probe=_ProbeRuntimeState(),
        health_registry=health,
        metrics_registry=metrics,
        alerts_manager=alerts,
        incidents_manager=incidents,
        db=_DbStub(),
        safe_mode=None,
        log_paths=[],
    )

    bundle_path = service.export_bundle()

    with zipfile.ZipFile(bundle_path, "r") as zf:
        names = set(zf.namelist())
        assert "runtime_state.json" in names
        assert "health.json" in names
        assert "metrics.json" in names
        assert "alerts.json" in names
        assert "incidents.json" in names
        assert "recent_orders.json" in names
        assert "recent_audit.json" in names

        alerts_payload = json.loads(zf.read("alerts.json"))
        incidents_payload = json.loads(zf.read("incidents.json"))
        assert alerts_payload["active_count"] >= 1
        assert incidents_payload["open_count"] >= 1
