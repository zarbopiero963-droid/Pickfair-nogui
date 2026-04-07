from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pytest

from core.trading_engine import STATUS_AMBIGUOUS
from observability.diagnostic_bundle_builder import DiagnosticBundleBuilder
from observability.diagnostics_service import DiagnosticsService


class _Probe:
    def collect_runtime_state(self):
        return {"mode": "resilience", "forensics": {"observability_snapshot_recent": False}, "orders": {"ambiguous": 2}}


class _Health:
    def snapshot(self):
        return {"overall_status": "DEGRADED"}


class _Metrics:
    def snapshot(self):
        return {"counters": {"quick_bet_ambiguous_total": 2}}


class _Alerts:
    def snapshot(self):
        return {"active_count": 1, "alerts": [{"code": "AMBIGUOUS_SPIKE", "active": True}]}


class _Incidents:
    def snapshot(self):
        return {"open_count": 1, "incidents": [{"code": "INC-RES", "status": "OPEN"}]}


class _Db:
    def __init__(self) -> None:
        self.exports = []

    def register_diagnostics_export(self, path: str) -> None:
        self.exports.append(path)

    def get_recent_orders_for_diagnostics(self, limit: int = 200):
        _ = limit
        return [{"order_id": "ORD-1", "status": STATUS_AMBIGUOUS}]

    def get_recent_audit_events_for_diagnostics(self, limit: int = 500):
        _ = limit
        return [{"type": "REQUEST_RECEIVED", "customer_ref": "RES-DIAG-1"}]


@pytest.mark.chaos
@pytest.mark.integration
def test_diagnostics_and_forensics_remain_truthful_after_recovery(tmp_path: Path) -> None:
    db = _Db()
    service = DiagnosticsService(
        builder=DiagnosticBundleBuilder(export_dir=str(tmp_path / "exports")),
        probe=_Probe(),
        health_registry=_Health(),
        metrics_registry=_Metrics(),
        alerts_manager=_Alerts(),
        incidents_manager=_Incidents(),
        db=db,
        safe_mode=None,
        log_paths=[],
    )
    bundle = service.export_bundle()

    with zipfile.ZipFile(bundle, "r") as zf:
        names = set(zf.namelist())
        required = {
            "health.json",
            "metrics.json",
            "alerts.json",
            "incidents.json",
            "runtime_state.json",
            "recent_orders.json",
            "recent_audit.json",
            "forensics_review.json",
        }
        assert required.issubset(names)

        health = json.loads(zf.read("health.json"))
        review = json.loads(zf.read("forensics_review.json"))
        assert health["overall_status"] == "DEGRADED"
        assert review
        assert review.get("degraded_or_not_ready") is True
        assert len(json.loads(zf.read("recent_orders.json"))) > 0
        assert len(json.loads(zf.read("recent_audit.json"))) > 0
