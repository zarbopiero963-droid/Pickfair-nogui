from pathlib import Path
import zipfile
import json

import pytest

from observability.alerts_manager import AlertsManager
from observability.diagnostic_bundle_builder import DiagnosticBundleBuilder
from observability.diagnostics_service import DiagnosticsService
from observability.health_registry import HealthRegistry
from observability.incidents_manager import IncidentsManager
from observability.metrics_registry import MetricsRegistry
from observability.watchdog_service import WatchdogService


class FlowProbe:
    def collect_runtime_state(self):
        return {"mode": "smoke-flow"}

    def collect_health(self):
        return {"engine": {"status": "READY", "reason": "ok", "details": {}}}

    def collect_metrics(self):
        return {"memory_rss_mb": 20.0, "inflight_count": 2.0}


class DummyDb:
    def __init__(self):
        self.exports = []

    def register_diagnostics_export(self, path):
        self.exports.append(path)

    def get_recent_orders_for_diagnostics(self, limit=200):
        _ = limit
        return [{"id": "O1", "status": "ok"}]

    def get_recent_audit_events_for_diagnostics(self, limit=500):
        _ = limit
        return [{"id": "A1", "type": "evt"}]


class SnapshotCollector:
    def __init__(self, db, probe, health, metrics, alerts, incidents):
        self.db = db
        self.probe = probe
        self.health = health
        self.metrics = metrics
        self.alerts = alerts
        self.incidents = incidents
        self.calls = 0

    def collect_and_store(self):
        self.calls += 1


@pytest.mark.smoke
def test_observability_full_flow_smoke(tmp_path):
    db = DummyDb()
    probe = FlowProbe()
    health = HealthRegistry()
    metrics = MetricsRegistry()
    alerts = AlertsManager()
    incidents = IncidentsManager()

    snapshot_collector = SnapshotCollector(db, probe, health, metrics, alerts, incidents)

    watchdog = WatchdogService(
        probe=probe,
        health_registry=health,
        metrics_registry=metrics,
        alerts_manager=alerts,
        incidents_manager=incidents,
        snapshot_service=snapshot_collector,
        interval_sec=0.01,
    )
    watchdog._tick()

    service = DiagnosticsService(
        builder=DiagnosticBundleBuilder(export_dir=str(tmp_path / "exports")),
        probe=probe,
        health_registry=health,
        metrics_registry=metrics,
        alerts_manager=alerts,
        incidents_manager=incidents,
        db=db,
        safe_mode=None,
        log_paths=[],
    )

    bundle = service.export_bundle()

    assert snapshot_collector.calls >= 1
    assert Path(bundle).exists()
    assert db.exports and db.exports[-1] == bundle
    assert health.snapshot()["overall_status"] in {"READY", "DEGRADED", "NOT_READY"}
    with zipfile.ZipFile(bundle, "r") as zf:
        assert "forensics_review.json" in set(zf.namelist())
        review = json.loads(zf.read("forensics_review.json"))
    assert "degraded_or_not_ready" in review
