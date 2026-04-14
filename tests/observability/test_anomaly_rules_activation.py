"""Tests for CTO anomaly rule activation in the runtime watchdog path.

Each of the 5 required anomaly rules is tested as a pure function (deterministic
input → deterministic output), then their activation is verified via the watchdog
_evaluate_anomalies() path when anomaly_enabled=True.

NO sleep, NO randomness, NO side effects, NO real-time dependency.
"""
from __future__ import annotations

import pytest

from observability.anomaly_rules import (
    db_contention_detected,
    event_fanout_incomplete,
    exposure_mismatch,
    financial_drift,
    ghost_order_detected,
    rule_stuck_inflight,
)
from observability.alerts_manager import AlertsManager
from observability.health_registry import HealthRegistry
from observability.incidents_manager import IncidentsManager
from observability.metrics_registry import MetricsRegistry
from observability.watchdog_service import WatchdogService


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _SnapshotStub:
    def collect_and_store(self) -> None:
        return None


class _BaseProbe:
    def collect_health(self):
        return {"runtime": {"status": "READY", "reason": "ok", "details": {}}}

    def collect_metrics(self):
        return {}

    def collect_runtime_state(self):
        return {}


def _make_watchdog(probe=None, anomaly_engine=None, **kwargs):
    defaults = dict(
        probe=probe or _BaseProbe(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=AlertsManager(),
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        anomaly_enabled=True,
        interval_sec=60.0,
    )
    if anomaly_engine is not None:
        defaults["anomaly_engine"] = anomaly_engine
    defaults.update(kwargs)
    return WatchdogService(**defaults)


# ===========================================================================
# A) ghost_order_detected — pure function tests
# ===========================================================================

def test_ghost_order_detected_fires_when_ghost_orders_count_positive():
    """ghost_order_detected fires when reconciliation confirms ghost_orders_count > 0."""
    context = {"runtime_state": {"reconcile": {"ghost_orders_count": 2}}}
    result = ghost_order_detected(context, {})
    assert result is not None
    assert result["code"] == "GHOST_ORDER_DETECTED"
    assert result["severity"] == "critical"
    assert result["details"]["ghost_orders_count"] == 2


def test_ghost_order_detected_does_not_fire_on_zero_count():
    """ghost_order_detected must NOT fire when ghost_orders_count == 0 (no false positive)."""
    context = {"runtime_state": {"reconcile": {"ghost_orders_count": 0}}}
    result = ghost_order_detected(context, {})
    assert result is None


def test_ghost_order_detected_does_not_fire_on_missing_reconcile():
    """ghost_order_detected must NOT fire when reconcile context is absent."""
    result = ghost_order_detected({}, {})
    assert result is None


def test_ghost_order_detected_is_pure_function():
    """Calling ghost_order_detected multiple times with the same input returns the same result."""
    context = {"runtime_state": {"reconcile": {"ghost_orders_count": 1}}}
    r1 = ghost_order_detected(context, {})
    r2 = ghost_order_detected(context, {})
    assert r1 is not None
    assert r2 is not None
    assert r1["code"] == r2["code"]
    assert r1["details"] == r2["details"]


# ===========================================================================
# B) exposure_mismatch — pure function tests
# ===========================================================================

def test_exposure_mismatch_fires_on_large_delta():
    """exposure_mismatch fires when |expected - actual| > tolerance."""
    context = {
        "risk": {
            "expected_exposure": 100.0,
            "actual_exposure": 200.0,
            "exposure_tolerance": 0.01,
        }
    }
    result = exposure_mismatch(context, {})
    assert result is not None
    assert result["code"] == "EXPOSURE_MISMATCH"
    assert result["details"]["expected_exposure"] == 100.0
    assert result["details"]["actual_exposure"] == 200.0
    assert result["details"]["difference"] == pytest.approx(100.0)


def test_exposure_mismatch_does_not_fire_when_within_tolerance():
    """exposure_mismatch must NOT fire when delta <= tolerance."""
    context = {
        "risk": {
            "expected_exposure": 100.0,
            "actual_exposure": 100.005,
            "exposure_tolerance": 0.01,
        }
    }
    result = exposure_mismatch(context, {})
    assert result is None


def test_exposure_mismatch_does_not_fire_on_missing_risk_context():
    """exposure_mismatch must NOT fire when risk context is absent (safe default)."""
    result = exposure_mismatch({}, {})
    assert result is None


def test_exposure_mismatch_is_pure_function():
    """Calling exposure_mismatch multiple times with same inputs returns same output."""
    context = {"risk": {"expected_exposure": 50.0, "actual_exposure": 100.0, "exposure_tolerance": 0.01}}
    r1 = exposure_mismatch(context, {})
    r2 = exposure_mismatch(context, {})
    assert r1 is not None and r2 is not None
    assert r1["code"] == r2["code"]


# ===========================================================================
# C) db_contention_detected — pure function tests
# ===========================================================================

def test_db_contention_detected_fires_on_contention_events():
    """db_contention_detected fires when contention_events > 0."""
    context = {
        "db": {"contention_events": 3, "lock_wait_ms": 0.0, "lock_wait_threshold_ms": 200.0}
    }
    result = db_contention_detected(context, {})
    assert result is not None
    assert result["code"] == "DB_CONTENTION_DETECTED"
    assert result["details"]["contention_events"] == 3


def test_db_contention_detected_fires_on_lock_wait_exceeded():
    """db_contention_detected fires when lock_wait_ms > threshold."""
    context = {
        "db": {"contention_events": 0, "lock_wait_ms": 500.0, "lock_wait_threshold_ms": 200.0}
    }
    result = db_contention_detected(context, {})
    assert result is not None
    assert result["code"] == "DB_CONTENTION_DETECTED"


def test_db_contention_detected_fires_on_writer_backlog():
    """db_contention_detected fires when db_writer_backlog >= threshold."""
    context = {
        "db": {
            "contention_events": 0,
            "lock_wait_ms": 0.0,
            "lock_wait_threshold_ms": 200.0,
            "db_writer_backlog": 60,
            "db_writer_backlog_threshold": 50,
        }
    }
    result = db_contention_detected(context, {})
    assert result is not None
    assert result["code"] == "DB_CONTENTION_DETECTED"


def test_db_contention_detected_does_not_fire_on_clean_state():
    """db_contention_detected must NOT fire when all metrics are within bounds."""
    context = {
        "db": {
            "contention_events": 0,
            "lock_wait_ms": 10.0,
            "lock_wait_threshold_ms": 200.0,
            "db_writer_backlog": 0,
            "db_writer_failed": 0,
            "db_writer_dropped": 0,
        }
    }
    result = db_contention_detected(context, {})
    assert result is None


def test_db_contention_detected_is_pure_function():
    """Same inputs always produce the same result."""
    context = {"db": {"contention_events": 5}}
    r1 = db_contention_detected(context, {})
    r2 = db_contention_detected(context, {})
    assert r1 is not None and r2 is not None
    assert r1["code"] == r2["code"]


# ===========================================================================
# D) event_fanout_incomplete — pure function tests
# ===========================================================================

def test_event_fanout_incomplete_fires_when_delivered_less_than_expected():
    """event_fanout_incomplete fires when delivered_fanout < expected_fanout."""
    context = {
        "event_bus": {"expected_fanout": 5, "delivered_fanout": 3}
    }
    result = event_fanout_incomplete(context, {})
    assert result is not None
    assert result["code"] == "EVENT_FANOUT_INCOMPLETE"
    assert result["details"]["expected_fanout"] == 5
    assert result["details"]["delivered_fanout"] == 3
    assert result["details"]["missing_fanout"] == 2


def test_event_fanout_incomplete_does_not_fire_when_fully_delivered():
    """event_fanout_incomplete must NOT fire when all subscribers received the event."""
    context = {
        "event_bus": {"expected_fanout": 4, "delivered_fanout": 4}
    }
    result = event_fanout_incomplete(context, {})
    assert result is None


def test_event_fanout_incomplete_does_not_fire_when_expected_is_zero():
    """event_fanout_incomplete must NOT fire when expected_fanout == 0 (no subscribers)."""
    context = {"event_bus": {"expected_fanout": 0, "delivered_fanout": 0}}
    result = event_fanout_incomplete(context, {})
    assert result is None


def test_event_fanout_incomplete_is_pure_function():
    """Same inputs always return the same finding."""
    context = {"event_bus": {"expected_fanout": 3, "delivered_fanout": 1}}
    r1 = event_fanout_incomplete(context, {})
    r2 = event_fanout_incomplete(context, {})
    assert r1 is not None and r2 is not None
    assert r1["details"]["missing_fanout"] == r2["details"]["missing_fanout"]


# ===========================================================================
# E) financial_drift — pure function tests
# ===========================================================================

def test_financial_drift_fires_on_balance_drift():
    """financial_drift fires when |ledger - venue| > drift_threshold."""
    context = {
        "financials": {
            "ledger_balance": 1000.0,
            "venue_balance": 900.0,
            "drift_threshold": 0.01,
        }
    }
    result = financial_drift(context, {})
    assert result is not None
    assert result["code"] == "FINANCIAL_DRIFT"
    assert result["severity"] == "critical"
    assert result["details"]["drift"] == pytest.approx(100.0)


def test_financial_drift_does_not_fire_within_threshold():
    """financial_drift must NOT fire when balance delta <= threshold."""
    context = {
        "financials": {
            "ledger_balance": 1000.0,
            "venue_balance": 1000.005,
            "drift_threshold": 0.01,
        }
    }
    result = financial_drift(context, {})
    assert result is None


def test_financial_drift_does_not_fire_on_empty_context():
    """financial_drift must NOT fire when financials context is absent."""
    result = financial_drift({}, {})
    assert result is None


def test_financial_drift_is_pure_function():
    """financial_drift produces the same output for the same input on repeated calls."""
    context = {
        "financials": {"ledger_balance": 500.0, "venue_balance": 400.0, "drift_threshold": 0.01}
    }
    r1 = financial_drift(context, {})
    r2 = financial_drift(context, {})
    assert r1 is not None and r2 is not None
    assert r1["code"] == r2["code"]
    assert r1["details"]["drift"] == r2["details"]["drift"]


def test_stuck_inflight_requires_explicit_runtime_timestamp() -> None:
    context = {
        "metrics": {"gauges": {"inflight_count": 55}},
        "runtime_state": {"require_explicit_ts": True},
        "recent_orders": [],
    }
    state: Dict[str, Any] = {}
    result = rule_stuck_inflight(context, state)
    assert result is not None
    assert result["code"] == "ANOMALY_INPUT_MISSING"
    assert result["severity"] == "critical"


# ===========================================================================
# F) Watchdog runtime activation — all 5 rules active via watchdog path
# ===========================================================================

def test_rules_NOT_active_when_anomaly_enabled_false():
    """When anomaly_enabled=False, no anomaly rules fire through the watchdog path."""

    class _AlwaysTriggerEngine:
        def evaluate(self, _ctx):
            return [
                {"code": "GHOST_ORDER_DETECTED", "severity": "critical", "description": "ghost", "details": {}},
                {"code": "EXPOSURE_MISMATCH", "severity": "warning", "description": "drift", "details": {}},
            ]

    alerts = AlertsManager()
    watchdog = _make_watchdog(
        anomaly_engine=_AlwaysTriggerEngine(),
        alerts_manager=alerts,
        anomaly_enabled=False,
    )

    watchdog._tick()
    assert watchdog.last_anomalies == [], "anomaly scan must be suppressed when anomaly_enabled=False"
    active_codes = {a["code"] for a in alerts.active_alerts()}
    assert "GHOST_ORDER_DETECTED" not in active_codes
    assert "EXPOSURE_MISMATCH" not in active_codes


def test_ghost_order_detected_active_in_watchdog_via_context():
    """ghost_order_detected fires through the default watchdog anomaly path
    when runtime_state.reconcile.ghost_orders_count > 0."""
    alerts = AlertsManager()

    class _GhostProbe(_BaseProbe):
        def collect_runtime_state(self):
            return {"reconcile": {"ghost_orders_count": 1, "event_key": "batch-42"}}

    watchdog = _make_watchdog(probe=_GhostProbe(), alerts_manager=alerts, anomaly_enabled=True)
    watchdog._evaluate_anomalies()

    codes = {a.get("code") for a in watchdog.last_anomalies}
    assert "GHOST_ORDER_DETECTED" in codes, (
        "ghost_order_detected must fire via watchdog when reconcile shows ghost_orders_count > 0"
    )
    alert_codes = {a["code"] for a in alerts.active_alerts() if a.get("source") == "anomaly"}
    assert "GHOST_ORDER_DETECTED" in alert_codes


def test_exposure_mismatch_active_in_watchdog_via_context_provider():
    """exposure_mismatch fires through the default watchdog anomaly path
    when risk context shows exposure delta > tolerance."""
    alerts = AlertsManager()

    watchdog = _make_watchdog(
        alerts_manager=alerts,
        anomaly_enabled=True,
        anomaly_context_provider=lambda: {
            "risk": {
                "expected_exposure": 100.0,
                "actual_exposure": 200.0,
                "exposure_tolerance": 0.01,
            }
        },
    )
    watchdog._evaluate_anomalies()

    codes = {a.get("code") for a in watchdog.last_anomalies}
    assert "EXPOSURE_MISMATCH" in codes, (
        "exposure_mismatch must fire via watchdog when risk exposure delta exceeds tolerance"
    )


def test_db_contention_active_in_watchdog_via_context_provider():
    """db_contention_detected fires through the default watchdog anomaly path
    when db context shows contention events."""
    alerts = AlertsManager()

    watchdog = _make_watchdog(
        alerts_manager=alerts,
        anomaly_enabled=True,
        anomaly_context_provider=lambda: {
            "db": {
                "contention_events": 5,
                "lock_wait_ms": 0.0,
                "lock_wait_threshold_ms": 200.0,
            }
        },
    )
    watchdog._evaluate_anomalies()

    codes = {a.get("code") for a in watchdog.last_anomalies}
    assert "DB_CONTENTION_DETECTED" in codes, (
        "db_contention_detected must fire via watchdog when contention events are present"
    )


def test_event_fanout_incomplete_active_in_watchdog_via_context_provider():
    """event_fanout_incomplete fires through the default watchdog anomaly path
    when event_bus context shows incomplete fanout."""
    alerts = AlertsManager()

    watchdog = _make_watchdog(
        alerts_manager=alerts,
        anomaly_enabled=True,
        anomaly_context_provider=lambda: {
            "event_bus": {"expected_fanout": 4, "delivered_fanout": 2}
        },
    )
    watchdog._evaluate_anomalies()

    codes = {a.get("code") for a in watchdog.last_anomalies}
    assert "EVENT_FANOUT_INCOMPLETE" in codes, (
        "event_fanout_incomplete must fire via watchdog when fanout is incomplete"
    )


def test_financial_drift_active_in_watchdog_via_context_provider():
    """financial_drift fires through the default watchdog anomaly path
    when financials context shows balance drift."""
    alerts = AlertsManager()

    watchdog = _make_watchdog(
        alerts_manager=alerts,
        anomaly_enabled=True,
        anomaly_context_provider=lambda: {
            "financials": {
                "ledger_balance": 1000.0,
                "venue_balance": 800.0,
                "drift_threshold": 0.01,
            }
        },
    )
    watchdog._evaluate_anomalies()

    codes = {a.get("code") for a in watchdog.last_anomalies}
    assert "FINANCIAL_DRIFT" in codes, (
        "financial_drift must fire via watchdog when balance drift exceeds threshold"
    )


def test_all_five_cto_rules_fire_in_single_watchdog_tick():
    """End-to-end proof: all 5 CTO anomaly rules fire in a single watchdog tick
    when the anomaly context contains all required trigger conditions.

    This is the canonical 'rules are active, not just present in catalog' proof.
    """
    alerts = AlertsManager()

    watchdog = _make_watchdog(
        alerts_manager=alerts,
        anomaly_enabled=True,
        anomaly_context_provider=lambda: {
            "runtime_state": {"reconcile": {"ghost_orders_count": 1}},
            "risk": {
                "expected_exposure": 100.0,
                "actual_exposure": 500.0,
                "exposure_tolerance": 0.01,
            },
            "db": {
                "contention_events": 2,
                "lock_wait_ms": 0.0,
                "lock_wait_threshold_ms": 200.0,
            },
            "event_bus": {"expected_fanout": 5, "delivered_fanout": 2},
            "financials": {
                "ledger_balance": 1000.0,
                "venue_balance": 500.0,
                "drift_threshold": 0.01,
            },
        },
    )

    watchdog._evaluate_anomalies()

    fired_codes = {a.get("code") for a in watchdog.last_anomalies}
    assert "GHOST_ORDER_DETECTED" in fired_codes, "ghost_order_detected must be active"
    assert "EXPOSURE_MISMATCH" in fired_codes, "exposure_mismatch must be active"
    assert "DB_CONTENTION_DETECTED" in fired_codes, "db_contention_detected must be active"
    assert "EVENT_FANOUT_INCOMPLETE" in fired_codes, "event_fanout_incomplete must be active"
    assert "FINANCIAL_DRIFT" in fired_codes, "financial_drift must be active"

    # All active findings must be surfaced as alerts
    active_anomaly_codes = {a["code"] for a in alerts.active_alerts() if a.get("source") == "anomaly"}
    assert "GHOST_ORDER_DETECTED" in active_anomaly_codes
    assert "EXPOSURE_MISMATCH" in active_anomaly_codes
    assert "DB_CONTENTION_DETECTED" in active_anomaly_codes
    assert "EVENT_FANOUT_INCOMPLETE" in active_anomaly_codes
    assert "FINANCIAL_DRIFT" in active_anomaly_codes


def test_no_false_positives_on_clean_context():
    """None of the 5 CTO rules fire on a completely clean operational context."""
    context = {
        "runtime_state": {"reconcile": {"ghost_orders_count": 0}},
        "risk": {
            "expected_exposure": 100.0,
            "actual_exposure": 100.0,
            "exposure_tolerance": 0.01,
        },
        "db": {
            "contention_events": 0,
            "lock_wait_ms": 0.0,
            "lock_wait_threshold_ms": 200.0,
            "db_writer_backlog": 0,
            "db_writer_failed": 0,
            "db_writer_dropped": 0,
        },
        "event_bus": {"expected_fanout": 4, "delivered_fanout": 4},
        "financials": {
            "ledger_balance": 1000.0,
            "venue_balance": 1000.0,
            "drift_threshold": 0.01,
        },
    }
    state = {}
    assert ghost_order_detected(context, {}) is None
    assert exposure_mismatch(context, state) is None
    assert db_contention_detected(context, {}) is None
    assert event_fanout_incomplete(context, {}) is None
    assert financial_drift(context, {}) is None
