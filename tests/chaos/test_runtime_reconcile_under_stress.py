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
            raise TimeoutError("transient")

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
        _orders, reason = engine._fetch_current_orders_by_market("1.1")
    finally:
        time.sleep = original_sleep

    assert reason is not None


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


# ---------------------------------------------------------------------------
# F.4 — Audited path must not make network calls
# ---------------------------------------------------------------------------

@pytest.mark.chaos
@pytest.mark.integration
def test_audited_reconcile_path_makes_no_network_call_without_snapshot() -> None:
    """In audited single-pass mode without a remote_snapshot, the engine must NOT
    call the network client.  Instead it returns an explicit FETCH_PERMANENT_FAILURE
    signal so the caller knows the input is unavailable."""
    from core.reconciliation_engine import ReconciliationEngine
    from core.reconciliation_types import ReasonCode

    network_call_count = {"n": 0}

    class _TrackingClient:
        def get_current_orders(self, market_ids=None):
            network_call_count["n"] += 1
            return []

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
            return {"batch_id": batch_id, "market_id": "1.AUDITED-TEST", "status": "LIVE"}
        def get_batch_legs(self, _batch_id):
            return [{"leg_index": 0, "status": "SUBMITTED", "customer_ref": "R-AUDITED-1"}]
        def update_leg_status(self, **kwargs):
            return None
        def recompute_batch_status(self, batch_id):
            return {"batch_id": batch_id, "status": "LIVE"}
        def mark_batch_failed(self, *_args, **_kwargs):
            return None
        def get_open_batches(self):
            return []
        def release_runtime_artifacts(self, **kwargs):
            return None

    engine = ReconciliationEngine(
        db=_DB(),
        batch_manager=_Batch(),
        client_getter=lambda: _TrackingClient(),
    )
    setattr(engine.cfg, "audited_single_pass_mode", True)

    # Direct method call — no remote_snapshot provided
    orders, reason = engine._fetch_current_orders_by_market("1.AUDITED-TEST")

    assert network_call_count["n"] == 0, (
        "Audited path must NOT call the network client when no remote_snapshot is provided"
    )
    assert reason is not None, (
        "Audited path must return an explicit failure reason when no snapshot is available"
    )
    assert reason == ReasonCode.FETCH_PERMANENT_FAILURE, (
        "Explicit unavailable-input signal must be FETCH_PERMANENT_FAILURE"
    )


@pytest.mark.chaos
@pytest.mark.integration
def test_audited_reconcile_uses_provided_snapshot_not_network() -> None:
    """In audited single-pass mode with remote_snapshot provided, the engine consumes
    the snapshot directly — no network call is made."""
    from core.reconciliation_engine import ReconciliationEngine

    network_call_count = {"n": 0}

    class _TrackingClient:
        def get_current_orders(self, market_ids=None):
            network_call_count["n"] += 1
            return []

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
            return {"batch_id": batch_id, "market_id": "1.AUDITED-SNAP", "status": "LIVE"}
        def get_batch_legs(self, _batch_id):
            return []
        def update_leg_status(self, **kwargs):
            return None
        def recompute_batch_status(self, batch_id):
            return {"batch_id": batch_id, "status": "LIVE"}
        def mark_batch_failed(self, *_args, **_kwargs):
            return None
        def get_open_batches(self):
            return []
        def release_runtime_artifacts(self, **kwargs):
            return None

    engine_stub = ReconciliationEngine(
        db=_DB(),
        batch_manager=_Batch(),
        client_getter=lambda: _TrackingClient(),
    )
    setattr(engine_stub.cfg, "audited_single_pass_mode", True)

    snapshot = [{"customerOrderRef": "R1", "betId": "BET-1", "status": "EXECUTABLE"}]
    orders, reason = engine_stub._fetch_current_orders_by_market(
        "1.AUDITED-SNAP", remote_snapshot=snapshot
    )

    assert network_call_count["n"] == 0, (
        "Audited path must not call network when remote_snapshot is provided"
    )
    assert reason is None, "Provided snapshot must result in success (reason=None)"
    assert len(orders) == 1
    assert orders[0]["betId"] == "BET-1"


# ---------------------------------------------------------------------------
# F.5 — Full audited reconcile path: explicit no-sleep proof
# ---------------------------------------------------------------------------

@pytest.mark.chaos
@pytest.mark.integration
def test_full_audited_reconcile_batch_does_not_call_sleep() -> None:
    """Proves the full reconcile_batch() path in audited single-pass mode does NOT
    call time.sleep at any point.  sleep is monkeypatched to raise AssertionError
    if invoked, so any hidden sleep in the audited path will fail the test."""
    import time as _time
    from core.reconciliation_engine import ReconciliationEngine
    from core.reconciliation_types import ReconcileConfig

    original_sleep = _time.sleep

    def _forbidden_sleep(_seconds):
        raise AssertionError(
            "time.sleep must NOT be called in the audited reconcile path"
        )

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
            return {"batch_id": batch_id, "market_id": "1.SLEEP-PROOF", "status": "LIVE"}
        def get_batch_legs(self, _batch_id):
            return [{"leg_index": 0, "status": "SUBMITTED", "customer_ref": "R-SLEEP-1"}]
        def update_leg_status(self, **kwargs):
            return None
        def recompute_batch_status(self, batch_id):
            return {"batch_id": batch_id, "status": "LIVE"}
        def mark_batch_failed(self, *_args, **_kwargs):
            return None
        def get_open_batches(self):
            return []
        def release_runtime_artifacts(self, **kwargs):
            return None

    cfg = ReconcileConfig(
        validate_batch_manager_contract=True,
        audit_fail_closed=False,
        persist_recovery_marker=False,
        max_convergence_cycles=1,
        max_transient_retries=0,
        enable_fencing_token=True,
        enable_runtime_invariants=False,
        ghost_order_action="LOG_AND_FLAG",
    )
    engine = ReconciliationEngine(
        db=_DB(),
        batch_manager=_Batch(),
        config=cfg,
    )
    setattr(engine.cfg, "audited_single_pass_mode", True)

    # Pre-supply a remote snapshot so no network call is attempted
    remote_orders = [
        {
            "customerOrderRef": "R-SLEEP-1",
            "betId": "BET-SLEEP-1",
            "marketId": "1.SLEEP-PROOF",
            "selectionId": "10",
            "status": "EXECUTABLE",
            "sizeMatched": "0.0",
            "sizeRemaining": "5.0",
        }
    ]

    _time.sleep = _forbidden_sleep
    try:
        result = engine.reconcile_batch("BATCH-SLEEP-PROOF", remote_snapshot=remote_orders)
    finally:
        _time.sleep = original_sleep

    # The reconcile must complete without raising AssertionError (no sleep)
    # and produce a meaningful result
    assert result is not None
    assert "batch_id" in result
