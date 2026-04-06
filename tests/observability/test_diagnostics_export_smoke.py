import zipfile
from pathlib import Path

import pytest
import json

from observability.alerts_manager import AlertsManager
from observability.diagnostic_bundle_builder import DiagnosticBundleBuilder
from observability.diagnostics_service import DiagnosticsService
from observability.health_registry import HealthRegistry
from observability.incidents_manager import IncidentsManager
from observability.metrics_registry import MetricsRegistry


class DummyProbe:
    def collect_runtime_state(self):
        return {"mode": "smoke"}


class DummyDb:
    def __init__(self):
        self.paths = []

    def register_diagnostics_export(self, path):
        self.paths.append(path)

    def get_recent_orders_for_diagnostics(self, limit=200):
        _ = limit
        return [{"id": "O1"}]

    def get_recent_audit_events_for_diagnostics(self, limit=500):
        _ = limit
        return [{"id": "A1"}]


@pytest.mark.smoke
def test_diagnostics_export_zip_contains_expected_payloads(tmp_path):
    db = DummyDb()
    service = DiagnosticsService(
        builder=DiagnosticBundleBuilder(export_dir=str(tmp_path / "exports")),
        probe=DummyProbe(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=AlertsManager(),
        incidents_manager=IncidentsManager(),
        db=db,
        safe_mode=None,
        log_paths=[],
    )

    path = service.export_bundle()
    assert Path(path).exists()
    assert db.paths and db.paths[-1] == path

    with zipfile.ZipFile(path, "r") as zf:
        names = set(zf.namelist())

    expected = {
        "manifest.json",
        "health.json",
        "metrics.json",
        "alerts.json",
        "incidents.json",
        "runtime_state.json",
        "safe_mode.json",
        "recent_orders.json",
        "recent_audit.json",
        "forensics_review.json",
        "thread_dump.txt",
        "logs_tail.txt",
    }
    assert expected.issubset(names)

    with zipfile.ZipFile(path, "r") as zf:
        payload = json.loads(zf.read("forensics_review.json"))
    assert "health_status" in payload
    assert "orders_count" in payload
