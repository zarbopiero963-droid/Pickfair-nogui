from __future__ import annotations

from typing import Any, Dict


RuleConfig = Dict[str, Dict[str, Any]]
AuditInput = Dict[str, Any]


def _to_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def build_anomaly_rule_config(audit_input: AuditInput) -> RuleConfig:
    """Build deterministic anomaly rule config from structured audit-like input.

    The resulting config is safe-mode only (disabled by default).
    """

    audit = audit_input or {}

    risk = audit.get("risk") or {}
    db = audit.get("db") or {}
    event_bus = audit.get("event_bus") or {}
    financials = audit.get("financials") or {}

    return {
        "ghost_order": {
            "enabled": False,
            "ghost_orders_count": _to_int(audit.get("ghost_orders_count"), 0),
        },
        "exposure_mismatch": {
            "enabled": False,
            "expected_exposure": _to_float(risk.get("expected_exposure"), 0.0),
            "actual_exposure": _to_float(risk.get("actual_exposure"), 0.0),
            "exposure_tolerance": _to_float(risk.get("exposure_tolerance"), 0.01),
        },
        "db_contention": {
            "enabled": False,
            "lock_wait_ms": _to_float(db.get("lock_wait_ms"), 0.0),
            "contention_events": _to_int(db.get("contention_events"), 0),
            "lock_wait_threshold_ms": _to_float(db.get("lock_wait_threshold_ms"), 200.0),
        },
        "event_fanout_incomplete": {
            "enabled": False,
            "expected_fanout": _to_int(event_bus.get("expected_fanout"), 0),
            "delivered_fanout": _to_int(event_bus.get("delivered_fanout"), 0),
        },
        "financial_drift": {
            "enabled": False,
            "ledger_balance": _to_float(financials.get("ledger_balance"), 0.0),
            "venue_balance": _to_float(financials.get("venue_balance"), 0.0),
            "drift_threshold": _to_float(financials.get("drift_threshold"), 0.01),
        },
    }
