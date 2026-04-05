from pathlib import Path

from observability.alerts_manager import AlertsManager
from observability.diagnostic_bundle_builder import DiagnosticBundleBuilder
from observability.diagnostics_service import DiagnosticsService
from observability.health_registry import HealthRegistry
from observability.incidents_manager import IncidentsManager
from observability.metrics_registry import MetricsRegistry


class DummyProbe:
    def collect_runtime_state(self):
        return {"mode": "test"}


class DummyDb:
    def __init__(self):
        self.exports = []

    def register_diagnostics_export(self, path):
        self.exports.append(path)

    def get_recent_orders_for_diagnostics(self, limit=200):
        return [{"order_id": "O1", "status": "FAILED"}]

    def get_recent_audit_events_for_diagnostics(self, limit=500):
        return [{"type": "REQUEST_RECEIVED", "correlation_id": "C1"}]


def test_diagnostics_service_exports_bundle(tmp_path):
    builder = DiagnosticBundleBuilder(export_dir=str(tmp_path / "exports"))
    service = DiagnosticsService(
        builder=builder,
        probe=DummyProbe(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=AlertsManager(),
        incidents_manager=IncidentsManager(),
        db=DummyDb(),
        safe_mode=None,
        log_paths=[],
    )

    path = service.export_bundle()
    assert Path(path).exists()
    assert path.endswith(".zip")
