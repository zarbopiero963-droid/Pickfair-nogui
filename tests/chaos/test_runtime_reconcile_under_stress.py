from __future__ import annotations

import json
import inspect
import zipfile
from pathlib import Path
from typing import Any, Dict, List

import pytest

from core.reconciliation_engine import ReconciliationEngine
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

    assert runner.fetcher.calls >= 2
    assert first_pass is False
    assert state_after_first["status"] == STATUS_AMBIGUOUS
    assert state_after_first["status"] != "FAILED"
    assert second_pass is True
    assert state_after_second["status"] == STATUS_COMPLETED


@pytest.mark.chaos
@pytest.mark.integration
def test_reconciliation_audited_path_has_no_while_true_or_sleep() -> None:
    fetch_src = inspect.getsource(ReconciliationEngine._fetch_current_orders_by_market)
    reconcile_src = inspect.getsource(ReconciliationEngine._reconcile_batch_inner)

    assert "while True" not in fetch_src
    assert "while True" not in reconcile_src

    class _DB:
        def persist_decision_log(self, *args, **kwargs):
            return None

        def get_pending_sagas(self):
            return []

        def get_reconcile_marker(self, _batch_id):
            return None

        def set_reconcile_marker(self, _batch_id, _value):
            return None

        def get_reconcile_remote_orders(self, *, batch_id, market_id):
            _ = (batch_id, market_id)
            return [
                {
                    "customerOrderRef": "R1",
                    "betId": "BET-R1",
                    "marketId": "1.1",
                    "selectionId": "10",
                    "status": "EXECUTION_COMPLETE",
                    "sizeMatched": 2.0,
                    "sizeRemaining": 0.0,
                }
            ]

    class _Batch:
        def get_batch(self, batch_id):
            return {"batch_id": batch_id, "market_id": "1.1", "status": "LIVE"}

        def get_batch_legs(self, _batch_id):
            return [{"leg_index": 0, "status": "SUBMITTED", "customer_ref": "R1"}]

        def update_leg_status(self, **kwargs):
            _ = kwargs
            return None

        def recompute_batch_status(self, batch_id):
            return {"batch_id": batch_id, "status": "LIVE"}

        def mark_batch_failed(self, *_args, **_kwargs):
            return None

        def get_open_batches(self):
            return []

        def release_runtime_artifacts(self, **kwargs):
            _ = kwargs
            return None

    class _Client:
        def get_current_orders(self, market_ids=None):
            _ = market_ids
            raise AssertionError("audited reconcile path must not call network fetch client")

    engine = ReconciliationEngine(
        db=_DB(),
        batch_manager=_Batch(),
        client_getter=lambda: _Client(),
    )
    setattr(engine.cfg, "audited_single_pass_mode", True)

    original_sleep = __import__("time").sleep

    def _boom(_seconds):
        raise AssertionError("time.sleep must not be called in audited single-pass mode")

    import time

    time.sleep = _boom
    try:
        result = engine.reconcile_batch("B1")
    finally:
        time.sleep = original_sleep

    assert result["ok"] is True


@pytest.mark.chaos
@pytest.mark.integration
def test_reconciliation_audited_path_requires_supplied_remote_snapshot() -> None:
    class _DB:
        def persist_decision_log(self, *args, **kwargs):
            return None

        def get_pending_sagas(self):
            return []

        def get_reconcile_marker(self, _batch_id):
            return None

        def set_reconcile_marker(self, _batch_id, _value):
            return None

    class _Batch:
        def get_batch(self, batch_id):
            return {"batch_id": batch_id, "market_id": "1.1", "status": "LIVE"}

        def get_batch_legs(self, _batch_id):
            return [{"leg_index": 0, "status": "SUBMITTED", "customer_ref": "R1"}]

        def update_leg_status(self, **kwargs):
            _ = kwargs
            return None

        def recompute_batch_status(self, batch_id):
            return {"batch_id": batch_id, "status": "LIVE"}

        def mark_batch_failed(self, *_args, **_kwargs):
            return None

        def get_open_batches(self):
            return []

        def release_runtime_artifacts(self, **kwargs):
            _ = kwargs
            return None

    engine = ReconciliationEngine(db=_DB(), batch_manager=_Batch(), client_getter=lambda: object())
    setattr(engine.cfg, "audited_single_pass_mode", True)
    result = engine.reconcile_batch("B1")
    assert result["ok"] is False
    assert result["reason_code"] == "FETCH_PERMANENT_FAILURE"
    assert result.get("operational_signal", {}).get("code") == "RECONCILE_REMOTE_INPUT_UNAVAILABLE"


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
        for filename in required:
            raw = zf.read(filename)
            assert raw
            parsed = json.loads(raw)
            assert isinstance(parsed, (dict, list))

        review = json.loads(zf.read("forensics_review.json"))
        assert review.get("degraded_or_not_ready") is True
        assert len(json.loads(zf.read("recent_orders.json"))) >= 1
        assert len(json.loads(zf.read("recent_audit.json"))) >= 1
