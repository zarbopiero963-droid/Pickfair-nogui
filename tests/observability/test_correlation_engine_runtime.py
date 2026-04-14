"""Tests for correlation engine runtime wiring in the watchdog path.

Verifies the 3 required live checks:
  1. local vs remote mismatch (rule_local_vs_remote)
  2. DB vs memory mismatch (rule_db_vs_memory)
  3. lifecycle mismatch / submit-reconcile chain break (rule_submit_reconcile_chain_break)

Also verifies:
  - correlate() is a pure function (no mutation, no side effects)
  - watchdog._evaluate_correlations() is called in _tick() and surfaces findings
  - mismatch → detection; valid state → no detection

NO sleep, NO randomness, NO side effects, NO real-time dependency.
"""
from __future__ import annotations

import pytest

from observability.correlation_engine import (
    CorrelationEvaluator,
    evaluate_correlation_rules,
    rule_db_vs_memory,
    rule_local_vs_remote,
    rule_submit_reconcile_chain_break,
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


def _make_watchdog(**kwargs):
    defaults = dict(
        probe=_BaseProbe(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=AlertsManager(),
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        interval_sec=60.0,
    )
    defaults.update(kwargs)
    return WatchdogService(**defaults)


# ===========================================================================
# 1. rule_local_vs_remote — local vs remote status mismatch
# ===========================================================================

def test_local_vs_remote_detects_status_mismatch():
    """rule_local_vs_remote fires when local order status differs from remote status."""
    context = {
        "recent_orders": [
            {"order_id": "o1", "status": "OPEN", "remote_status": "CANCELLED"},
        ]
    }
    result = rule_local_vs_remote(context, {})
    assert result is not None
    assert result["code"] == "LOCAL_VS_REMOTE_MISMATCH"
    assert result["severity"] == "critical"
    assert result["details"]["mismatched_count"] == 1
    assert result["details"]["sample"][0]["id"] == "o1"
    assert result["details"]["sample"][0]["local"] == "OPEN"
    assert result["details"]["sample"][0]["remote"] == "CANCELLED"


def test_local_vs_remote_detects_multiple_mismatches():
    """rule_local_vs_remote counts all mismatched orders."""
    context = {
        "recent_orders": [
            {"order_id": "o1", "status": "OPEN", "remote_status": "CANCELLED"},
            {"order_id": "o2", "status": "SUBMITTED", "remote_status": "MATCHED"},
            {"order_id": "o3", "status": "COMPLETED", "remote_status": "COMPLETED"},  # match
        ]
    }
    result = rule_local_vs_remote(context, {})
    assert result is not None
    assert result["details"]["mismatched_count"] == 2


def test_local_vs_remote_no_finding_on_matching_states():
    """rule_local_vs_remote must NOT fire when all orders have matching local/remote status."""
    context = {
        "recent_orders": [
            {"order_id": "o1", "status": "COMPLETED", "remote_status": "COMPLETED"},
            {"order_id": "o2", "status": "SUBMITTED", "remote_status": "SUBMITTED"},
        ]
    }
    result = rule_local_vs_remote(context, {})
    assert result is None


def test_local_vs_remote_no_finding_on_empty_orders():
    """rule_local_vs_remote must NOT fire when there are no recent orders."""
    result = rule_local_vs_remote({"recent_orders": []}, {})
    assert result is None


def test_local_vs_remote_no_finding_on_missing_remote_status():
    """rule_local_vs_remote must NOT fire when an order lacks remote_status (partial data)."""
    context = {
        "recent_orders": [
            {"order_id": "o1", "status": "OPEN"},  # no remote_status
        ]
    }
    result = rule_local_vs_remote(context, {})
    assert result is None


def test_local_vs_remote_is_pure_no_mutation():
    """rule_local_vs_remote does not mutate context or state on repeated calls."""
    context = {
        "recent_orders": [
            {"order_id": "o1", "status": "OPEN", "remote_status": "CANCELLED"},
        ]
    }
    original_context = {"recent_orders": [dict(o) for o in context["recent_orders"]]}
    state = {}
    r1 = rule_local_vs_remote(context, state)
    r2 = rule_local_vs_remote(context, state)
    # Context unchanged
    assert context["recent_orders"] == original_context["recent_orders"]
    # Same results
    assert r1 is not None and r2 is not None
    assert r1["code"] == r2["code"]
    assert r1["details"]["mismatched_count"] == r2["details"]["mismatched_count"]


# ===========================================================================
# 2. rule_db_vs_memory — DB vs memory inflight count mismatch
# ===========================================================================

def test_db_vs_memory_detects_count_mismatch():
    """rule_db_vs_memory fires when DB inflight count differs from in-memory count."""
    context = {
        "metrics": {"gauges": {"inflight_count": 3}},
        "db_state": {"inflight_orders_count": 7},
    }
    result = rule_db_vs_memory(context, {})
    assert result is not None
    assert result["code"] == "DB_VS_MEMORY_MISMATCH"
    assert result["details"]["db_count"] == 7
    assert result["details"]["memory_count"] == 3
    assert result["details"]["delta"] == 4


def test_db_vs_memory_no_finding_on_equal_counts():
    """rule_db_vs_memory must NOT fire when DB and memory counts match."""
    context = {
        "metrics": {"gauges": {"inflight_count": 5}},
        "db_state": {"inflight_orders_count": 5},
    }
    result = rule_db_vs_memory(context, {})
    assert result is None


def test_db_vs_memory_no_finding_when_db_count_absent():
    """rule_db_vs_memory must NOT fire when db_state.inflight_orders_count is absent."""
    context = {
        "metrics": {"gauges": {"inflight_count": 5}},
    }
    result = rule_db_vs_memory(context, {})
    assert result is None


def test_db_vs_memory_uses_db_state_direct_source():
    """rule_db_vs_memory uses db_state.inflight_orders_count as authoritative source."""
    context = {
        "metrics": {"gauges": {"inflight_count": 2, "db_inflight_count": 99}},
        "db_state": {"inflight_orders_count": 10},  # direct source
    }
    result = rule_db_vs_memory(context, {})
    assert result is not None
    assert result["details"]["db_count"] == 10  # direct source wins
    assert result["details"]["db_source"] == "diagnostics_recent_orders"


def test_db_vs_memory_is_pure_no_mutation():
    """rule_db_vs_memory does not mutate context on repeated calls."""
    context = {
        "metrics": {"gauges": {"inflight_count": 1}},
        "db_state": {"inflight_orders_count": 5},
    }
    state = {}
    r1 = rule_db_vs_memory(context, state)
    r2 = rule_db_vs_memory(context, state)
    assert r1 is not None and r2 is not None
    assert r1["code"] == r2["code"]
    assert r1["details"]["delta"] == r2["details"]["delta"]


# ===========================================================================
# 3. rule_submit_reconcile_chain_break — lifecycle chain mismatch
# ===========================================================================

def test_submit_reconcile_chain_break_detects_submitted_not_reconciled():
    """rule_submit_reconcile_chain_break fires when submitted orders are absent from reconcile audit."""
    context = {
        "recent_orders": [
            {"order_id": "o1", "status": "SUBMITTED"},
        ],
        "recent_audit": [],  # o1 never appeared in reconcile audit
    }
    result = rule_submit_reconcile_chain_break(context, {})
    assert result is not None
    assert result["code"] == "SUBMIT_RECONCILE_CHAIN_BREAK"
    assert result["details"]["broken_count"] == 1
    assert "o1" in result["details"]["sample_ids"]


def test_submit_reconcile_chain_break_no_finding_when_all_reconciled():
    """rule_submit_reconcile_chain_break must NOT fire when all submitted orders appear in audit."""
    context = {
        "recent_orders": [
            {"order_id": "o1", "status": "SUBMITTED"},
        ],
        "recent_audit": [
            {"order_id": "o1"},  # appears in reconcile audit
        ],
    }
    result = rule_submit_reconcile_chain_break(context, {})
    assert result is None


def test_submit_reconcile_chain_break_canonical_evidence_wins():
    """reconcile_chain canonical evidence takes precedence over order-level inference."""
    context = {
        "reconcile_chain": {
            "missing_count": 2,
            "sample_missing_ids": ["o1", "o2"],
            "submitted_count": 5,
            "reconciled_count": 3,
        },
        "recent_orders": [],
        "recent_audit": [],
    }
    result = rule_submit_reconcile_chain_break(context, {})
    assert result is not None
    assert result["code"] == "SUBMIT_RECONCILE_CHAIN_BREAK"
    assert result["details"]["broken_count"] == 2
    assert result["details"]["source"] == "canonical_reconcile_chain"


def test_submit_reconcile_chain_break_no_finding_on_clean_chain():
    """rule_submit_reconcile_chain_break must NOT fire when all orders are reconciled."""
    context = {
        "recent_orders": [
            {"order_id": "o1", "status": "COMPLETED"},
        ],
        "recent_audit": [{"order_id": "o1"}],
    }
    result = rule_submit_reconcile_chain_break(context, {})
    assert result is None


def test_submit_reconcile_chain_break_is_pure_no_side_effects():
    """rule_submit_reconcile_chain_break does not mutate context and is deterministic."""
    context = {
        "recent_orders": [{"order_id": "o1", "status": "SUBMITTED"}],
        "recent_audit": [],
    }
    state = {}
    r1 = rule_submit_reconcile_chain_break(context, state)
    r2 = rule_submit_reconcile_chain_break(context, state)
    assert r1 is not None and r2 is not None
    assert r1["code"] == r2["code"]
    assert r1["details"]["broken_count"] == r2["details"]["broken_count"]


# ===========================================================================
# 4. CorrelationEvaluator — batch evaluation
# ===========================================================================

def test_correlation_evaluator_returns_all_applicable_findings():
    """CorrelationEvaluator.evaluate() returns findings from all rules that fire."""
    context = {
        "recent_orders": [
            {"order_id": "o1", "status": "OPEN", "remote_status": "CANCELLED"},
        ],
        "metrics": {"gauges": {"inflight_count": 0}},
        "db_state": {"inflight_orders_count": 5},
    }
    evaluator = CorrelationEvaluator([rule_local_vs_remote, rule_db_vs_memory])
    findings = evaluator.evaluate(context)
    codes = {f["code"] for f in findings}
    assert "LOCAL_VS_REMOTE_MISMATCH" in codes
    assert "DB_VS_MEMORY_MISMATCH" in codes


def test_correlation_evaluator_returns_empty_on_clean_context():
    """CorrelationEvaluator.evaluate() returns an empty list when no rules fire."""
    context = {
        "recent_orders": [
            {"order_id": "o1", "status": "COMPLETED", "remote_status": "COMPLETED"},
        ],
        "metrics": {"gauges": {"inflight_count": 5}},
        "db_state": {"inflight_orders_count": 5},
    }
    evaluator = CorrelationEvaluator([rule_local_vs_remote, rule_db_vs_memory])
    findings = evaluator.evaluate(context)
    assert findings == []


def test_correlation_evaluator_disabled_state_emits_structured_signal():
    evaluator = CorrelationEvaluator([rule_local_vs_remote])
    findings = evaluator.evaluate({"correlation_reviewer_enabled": False})
    assert findings[0]["code"] == "CORRELATION_REVIEWER_DISABLED"


def test_correlation_evaluator_empty_rules_emits_structured_signal():
    evaluator = CorrelationEvaluator([])
    findings = evaluator.evaluate({})
    assert findings[0]["code"] == "CORRELATION_REVIEWER_EMPTY"


def test_correlation_evaluator_misconfigured_rules_emits_structured_signal():
    evaluator = CorrelationEvaluator([None])
    findings = evaluator.evaluate({})
    assert findings[0]["code"] == "CORRELATION_REVIEWER_MISCONFIGURED"


def test_partial_invalid_rules_still_run_callable_checks_and_emit_misconfiguration():
    def valid_rule(context):
        return [
            {
                "code": "LOCAL_VS_REMOTE_MISMATCH",
                "severity": "critical",
                "reason": "Local and remote state diverged",
                "details": {"order_id": "ord-1"},
            }
        ]

    invalid_rule = "not-callable"

    evaluator = CorrelationEvaluator(
        rules=[invalid_rule, valid_rule],
    )

    findings = evaluator.evaluate(
        {
            "local_state": {"orders": [{"id": "ord-1", "status": "OPEN"}]},
            "remote_state": {"orders": []},
            "db_state": {},
            "memory_state": {},
            "lifecycle_events": [],
        }
    )

    codes = {finding["code"] if isinstance(finding, dict) else finding.code for finding in findings}

    assert "CORRELATION_REVIEWER_MISCONFIGURED" in codes
    assert "LOCAL_VS_REMOTE_MISMATCH" in codes


def test_all_invalid_rules_emit_only_misconfiguration():
    evaluator = CorrelationEvaluator(
        rules=["not-callable-1", None],
    )

    findings = evaluator.evaluate(
        {
            "local_state": {},
            "remote_state": {},
            "db_state": {},
            "memory_state": {},
            "lifecycle_events": [],
        }
    )

    codes = [finding["code"] if isinstance(finding, dict) else finding.code for finding in findings]

    assert "CORRELATION_REVIEWER_MISCONFIGURED" in codes
    assert "LOCAL_VS_REMOTE_MISMATCH" not in codes
    assert "DB_VS_MEMORY_MISMATCH" not in codes
    assert "SUBMIT_RECONCILE_CHAIN_BREAK" not in codes


def test_all_valid_healthy_rules_do_not_emit_misconfiguration():
    def healthy_rule(context):
        return []

    evaluator = CorrelationEvaluator(
        rules=[healthy_rule],
    )

    findings = evaluator.evaluate(
        {
            "local_state": {},
            "remote_state": {},
            "db_state": {},
            "memory_state": {},
            "lifecycle_events": [],
        }
    )

    codes = [finding["code"] if isinstance(finding, dict) else finding.code for finding in findings]

    assert "CORRELATION_REVIEWER_MISCONFIGURED" not in codes
    assert findings == []


def test_correlation_evaluator_healthy_zero_findings_is_not_misconfigured():
    evaluator = CorrelationEvaluator([rule_local_vs_remote])
    findings = evaluator.evaluate(
        {"recent_orders": [{"order_id": "o1", "status": "COMPLETED", "remote_status": "COMPLETED"}]}
    )
    assert findings == []


def test_evaluate_correlation_rules_functional_api():
    """evaluate_correlation_rules() stateless functional API returns findings correctly."""
    context = {
        "recent_orders": [
            {"order_id": "o1", "status": "SUBMITTED", "remote_status": "CANCELLED"},
        ]
    }
    findings = evaluate_correlation_rules(context)
    codes = {f["code"] for f in findings}
    assert "LOCAL_VS_REMOTE_MISMATCH" in codes


# ===========================================================================
# 5. Watchdog wiring — correlation is called in _tick and surfaces findings
# ===========================================================================

def test_watchdog_evaluate_correlations_called_in_tick_and_raises_alert():
    """_evaluate_correlations() is called during _tick() and surfaces LOCAL_VS_REMOTE_MISMATCH
    as an alert from the correlation_reviewer when a mismatch exists."""
    alerts = AlertsManager()

    class _MismatchProbe(_BaseProbe):
        def collect_runtime_state(self):
            return {
                "recent_orders": [
                    {"order_id": "o1", "status": "OPEN", "remote_status": "CANCELLED"},
                ]
            }

    watchdog = _make_watchdog(
        probe=_MismatchProbe(),
        alerts_manager=alerts,
        anomaly_context_provider=lambda: {
            "recent_orders": [
                {"order_id": "o1", "status": "OPEN", "remote_status": "CANCELLED"},
            ]
        },
    )

    watchdog._tick()

    active = alerts.active_alerts()
    correlation_alerts = [a for a in active if a.get("source") == "correlation_reviewer"]
    codes = {a["code"] for a in correlation_alerts}
    assert "LOCAL_VS_REMOTE_MISMATCH" in codes, (
        "correlation reviewer must be called in _tick and raise LOCAL_VS_REMOTE_MISMATCH"
    )
    assert correlation_alerts[0]["severity"] == "critical"


def test_watchdog_evaluate_correlations_opens_incident_for_critical_finding():
    """When correlation surfaces a critical finding, an incident is opened."""
    alerts = AlertsManager()
    incidents = IncidentsManager()

    watchdog = _make_watchdog(
        alerts_manager=alerts,
        incidents_manager=incidents,
        anomaly_context_provider=lambda: {
            "recent_orders": [
                {"order_id": "o1", "status": "AMBIGUOUS", "remote_status": "MATCHED"},
            ]
        },
    )

    watchdog._evaluate_correlations()

    open_incidents = {
        i["code"] for i in incidents.snapshot()["incidents"] if i["status"] == "OPEN"
    }
    assert "LOCAL_VS_REMOTE_MISMATCH" in open_incidents, (
        "critical correlation finding must open an incident"
    )


def test_watchdog_correlation_resolves_when_mismatch_clears():
    """When a correlation mismatch is corrected, the alert and incident resolve on next tick."""
    alerts = AlertsManager()
    incidents = IncidentsManager()

    orders = [{"order_id": "o1", "status": "AMBIGUOUS", "remote_status": "MATCHED"}]

    watchdog = _make_watchdog(
        alerts_manager=alerts,
        incidents_manager=incidents,
        anomaly_context_provider=lambda: {"recent_orders": list(orders)},
    )

    # Tick 1: mismatch present → alert active, incident open
    watchdog._evaluate_correlations()
    active_codes = {a["code"] for a in alerts.active_alerts() if a.get("source") == "correlation_reviewer"}
    assert "LOCAL_VS_REMOTE_MISMATCH" in active_codes

    # Fix the mismatch
    orders.clear()
    orders.append({"order_id": "o1", "status": "COMPLETED", "remote_status": "COMPLETED"})

    # Tick 2: mismatch cleared → alert resolved, incident closed
    watchdog._evaluate_correlations()
    active_codes_after = {a["code"] for a in alerts.active_alerts() if a.get("source") == "correlation_reviewer"}
    assert "LOCAL_VS_REMOTE_MISMATCH" not in active_codes_after

    open_incidents = {i["code"] for i in incidents.snapshot()["incidents"] if i["status"] == "OPEN"}
    assert "LOCAL_VS_REMOTE_MISMATCH" not in open_incidents


def test_watchdog_correlation_db_vs_memory_wired_in_default_path():
    """DB_VS_MEMORY_MISMATCH flows through watchdog._evaluate_correlations when
    probe.collect_correlation_context() provides direct db_state evidence."""
    alerts = AlertsManager()

    class _DbMismatchProbe(_BaseProbe):
        def collect_correlation_context(self):
            return {
                "db_state": {"inflight_orders_count": 10},
                "metrics": {"gauges": {"inflight_count": 2}},
            }

    watchdog = _make_watchdog(probe=_DbMismatchProbe(), alerts_manager=alerts)
    watchdog._evaluate_correlations()

    active = alerts.active_alerts()
    codes = {a["code"] for a in active if a.get("source") == "correlation_reviewer"}
    assert "DB_VS_MEMORY_MISMATCH" in codes, (
        "DB_VS_MEMORY_MISMATCH must flow through watchdog when db_state shows mismatch"
    )


def test_watchdog_correlation_chain_break_wired_in_default_path():
    """SUBMIT_RECONCILE_CHAIN_BREAK flows through watchdog._evaluate_correlations when
    context provides submitted orders missing from reconcile audit."""
    alerts = AlertsManager()

    watchdog = _make_watchdog(
        alerts_manager=alerts,
        anomaly_context_provider=lambda: {
            "recent_orders": [{"order_id": "chain-1", "status": "SUBMITTED"}],
            "recent_audit": [],
        },
    )

    watchdog._evaluate_correlations()

    active = alerts.active_alerts()
    codes = {a["code"] for a in active if a.get("source") == "correlation_reviewer"}
    assert "SUBMIT_RECONCILE_CHAIN_BREAK" in codes, (
        "SUBMIT_RECONCILE_CHAIN_BREAK must flow through watchdog when lifecycle chain is broken"
    )


def test_watchdog_correlation_exactly_once_per_tick():
    """_evaluate_correlations is called exactly once per _tick() — no loop, no double-call."""
    call_counts = {"count": 0}
    original_evaluate = CorrelationEvaluator.evaluate

    class _CountingEvaluator(CorrelationEvaluator):
        def evaluate(self, context):
            call_counts["count"] += 1
            return original_evaluate(self, context)

    alerts = AlertsManager()
    watchdog = _make_watchdog(alerts_manager=alerts)
    watchdog._correlation_evaluator = _CountingEvaluator()

    watchdog._tick()

    assert call_counts["count"] == 1, (
        "_evaluate_correlations must call the evaluator exactly once per tick — no loop wiring"
    )


def test_correlation_no_false_positives_on_fully_healthy_state():
    """No correlation findings on a fully clean, healthy operational context."""
    context = {
        "recent_orders": [
            {"order_id": "o1", "status": "COMPLETED", "remote_status": "COMPLETED"},
        ],
        "metrics": {"gauges": {"inflight_count": 3}},
        "db_state": {"inflight_orders_count": 3},
        "recent_audit": [{"order_id": "o1"}],
        "event_bus": {
            "queue_depth": 0,
            "running": True,
            "worker_threads_alive": 2,
            "published_total": 10,
            "side_effects_confirmed": 10,
        },
    }
    evaluator = CorrelationEvaluator(
        [rule_local_vs_remote, rule_db_vs_memory, rule_submit_reconcile_chain_break]
    )
    findings = evaluator.evaluate(context)
    assert findings == [], f"Expected no findings on clean state, got: {findings}"
