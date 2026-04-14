from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from core.trading_engine import STATUS_AMBIGUOUS, STATUS_FAILED, TradingEngine
from observability.anomaly_engine import AnomalyEngine
from observability.anomaly_rules import DEFAULT_ANOMALY_RULES
from observability.cto_reviewer import CtoReviewer
from observability.diagnostic_bundle_builder import DiagnosticBundleBuilder
from observability.diagnostics_service import DiagnosticsService


class _DBLocked:
    def is_ready(self):
        return True

    def insert_order(self, payload):
        raise sqlite3.OperationalError("database is locked")


class _Bus:
    def subscribe(self, *_a, **_k):
        return None

    def publish(self, *_a, **_k):
        return None


class _Exec:
    def is_ready(self):
        return True

    def submit(self, _name, fn):
        return fn()


def test_sqlite_locked_transient_does_not_claim_false_success():
    engine = TradingEngine(
        bus=_Bus(),
        db=_DBLocked(),
        client_getter=lambda: object(),
        executor=_Exec(),
        reconciliation_engine=None,
    )
    result = engine.submit_quick_bet({"market_id": "1", "selection_id": 1, "price": 2.0, "size": 2.0, "side": "BACK"})
    assert result["status"] in {STATUS_FAILED, STATUS_AMBIGUOUS}
    assert result["status"] != "SUBMITTED"


@pytest.mark.chaos
def test_db_contention_plus_ambiguity_escalates_to_cascade_risk():
    anomaly = AnomalyEngine(DEFAULT_ANOMALY_RULES).evaluate(
        {
            "db": {"contention_events": 2, "lock_wait_ms": 350, "db_writer_backlog": 12},
            "runtime_state": {"alert_pipeline": {"enabled": True, "deliverable": False}},
            "metrics": {"gauges": {"queue_depth": 4, "worker_alive": 0, "completed_delta": 0, "heartbeat_age": 100}},
        }
    )
    reviewer = CtoReviewer(history_window=4, cooldown_sec=0)
    findings = reviewer.evaluate(
        {
            "metrics_snapshot": {"gauges": {"db_lock_errors": 2, "network_timeout_count": 1, "ambiguous_submissions": 1}},
            "anomaly_alerts": anomaly,
            "forensics_alerts": [],
            "incidents_snapshot": {"open_count": 1},
            "runtime_probe_state": {"alert_pipeline": {"enabled": True, "deliverable": False}},
            "diagnostics_bundle": {"available": False},
        }
    )
    names = {f["rule_name"] for f in findings}
    assert "CASCADE_FAILURE_RISK" in names


def test_contention_with_diagnostic_export_marks_evidence_sections(tmp_path: Path):
    class DB:
        def register_diagnostics_export(self, _):
            return None

        def get_recent_orders_for_diagnostics(self, limit=200):
            _ = limit
            raise sqlite3.OperationalError("database is locked")

        def get_recent_audit_events_for_diagnostics(self, limit=500):
            _ = limit
            raise sqlite3.OperationalError("database is locked")

    class Probe:
        def collect_runtime_state(self):
            return {"mode": "chaos"}

    class Snap:
        def snapshot(self):
            return {"overall_status": "DEGRADED", "components": []}

    class Alerts:
        def snapshot(self):
            return {"active_count": 1, "alerts": [{"code": "DB_CONTENTION_DETECTED", "active": True}]}

    class Inc:
        def snapshot(self):
            return {"open_count": 1, "incidents": [{"code": "INC-1", "status": "OPEN"}]}

    service = DiagnosticsService(
        builder=DiagnosticBundleBuilder(export_dir=str(tmp_path / "exports")),
        probe=Probe(),
        health_registry=Snap(),
        metrics_registry=Snap(),
        alerts_manager=Alerts(),
        incidents_manager=Inc(),
        db=DB(),
        safe_mode=None,
        log_paths=[],
    )
    bundle = service.export_bundle()
    assert Path(bundle).exists()
