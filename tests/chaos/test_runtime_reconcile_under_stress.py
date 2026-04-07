from __future__ import annotations

import json
import zipfile
from pathlib import Path
from typing import Any, Dict, List

import pytest

from core.trading_engine import STATUS_AMBIGUOUS, STATUS_COMPLETED
from observability.diagnostic_bundle_builder import DiagnosticBundleBuilder
from observability.diagnostics_service import DiagnosticsService
from tests.integration.test_betfair_timeout_and_ghost_orders import (
    FakeClient,
    FlakyRemoteFetcher,
    ReconcilePassRunner,
    _make_engine,
    _payload,
)


class Probe:
    def collect_runtime_state(self) -> Dict[str, Any]:
        return {
            "mode": "chaos-test",
            "forensics": {"observability_snapshot_recent": False},
            "orders": {"ambiguous": 1},
        }


class Health:
    def snapshot(self) -> Dict[str, Any]:
        return {"overall_status": "DEGRADED"}


class Metrics:
    def snapshot(self) -> Dict[str, Any]:
        return {
            "counters": {"quick_bet_ambiguous_total": 1},
            "gauges": {"inflight_count": 1, "memory_rss_mb": 99},
        }


class Alerts:
    def snapshot(self) -> Dict[str, Any]:
        return {"active_count": 1, "alerts": [{"code": "AMBIGUOUS_SPIKE", "active": True}]}


class Incidents:
    def snapshot(self) -> Dict[str, Any]:
        return {"open_count": 1, "incidents": [{"code": "INC-CHAOS", "status": "OPEN"}]}


class DiagnosticsDb:
    def __init__(self) -> None:
        self.exports: List[str] = []

    def register_diagnostics_export(self, path: str) -> None:
        self.exports.append(path)

    def get_recent_orders_for_diagnostics(self, limit: int = 200) -> List[Dict[str, Any]]:
        _ = limit
        return [{"order_id": "ORD-1", "status": STATUS_AMBIGUOUS, "customer_ref": "CHAOS-REC-1"}]

    def get_recent_audit_events_for_diagnostics(self, limit: int = 500) -> List[Dict[str, Any]]:
        _ = limit
        return [{"type": "REQUEST_RECEIVED", "customer_ref": "CHAOS-REC-1"}]


@pytest.mark.chaos
@pytest.mark.integration
def test_reconcile_transient_failure_preserves_ambiguity() -> None:
    engine, db, _bus, _rec = _make_engine(client=FakeClient(error=TimeoutError("network timeout")))

    result = engine.submit_quick_bet(_payload("CHAOS-REC-1"))
    assert result["status"] == STATUS_AMBIGUOUS

    runner = ReconcilePassRunner(
        db,
        FlakyRemoteFetcher(
            outcomes=[
                TimeoutError("fetch timeout"),
                {"bet_id": "REMOTE-REC-1"},
            ]
        ),
    )

    first_pass = runner.run_once(customer_ref="CHAOS-REC-1")
    state_after_first = db.get_order(result["order_id"])
    second_pass = runner.run_once(customer_ref="CHAOS-REC-1")
    state_after_second = db.get_order(result["order_id"])

    assert first_pass is False
    assert state_after_first["status"] == STATUS_AMBIGUOUS
    assert second_pass is True
    assert state_after_second["status"] == STATUS_COMPLETED


@pytest.mark.chaos
@pytest.mark.integration
def test_diagnostics_bundle_preserves_chaos_evidence(tmp_path: Path) -> None:
    db = DiagnosticsDb()
    service = DiagnosticsService(
        builder=DiagnosticBundleBuilder(export_dir=str(tmp_path / "exports")),
        probe=Probe(),
        health_registry=Health(),
        metrics_registry=Metrics(),
        alerts_manager=Alerts(),
        incidents_manager=Incidents(),
        db=db,
        safe_mode=None,
        log_paths=[],
    )

    bundle = service.export_bundle()

    assert Path(bundle).exists()
    assert db.exports and db.exports[-1] == bundle

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

        review = json.loads(zf.read("forensics_review.json"))
        assert review.get("degraded_or_not_ready") is True
        assert len(json.loads(zf.read("recent_orders.json"))) >= 1
        assert len(json.loads(zf.read("recent_audit.json"))) >= 1
