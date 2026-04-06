from __future__ import annotations

from typing import Any, Dict, List


ForensicFinding = Dict[str, Any]


def _finding(code: str, severity: str, message: str, details: Dict[str, Any]) -> ForensicFinding:
    return {
        "code": code,
        "severity": severity,
        "message": message,
        "details": details,
    }


def rule_failed_but_remote_exists(context: Dict[str, Any], state: Dict[str, Any]) -> ForensicFinding | None:
    _ = state
    orders = context.get("recent_orders") or []
    for order in orders:
        status = str(order.get("status", "") or "").upper()
        remote = order.get("remote_bet_id") or order.get("exchange_order_id")
        if status in {"FAILED", "ERROR", "REJECTED"} and remote:
            return _finding(
                "FAILED_BUT_REMOTE_EXISTS",
                "critical",
                "Order marked failed but remote order id exists",
                {"order_id": order.get("order_id") or order.get("id"), "remote": remote, "status": status},
            )
    return None


def rule_finalized_without_audit_evidence(context: Dict[str, Any], state: Dict[str, Any]) -> ForensicFinding | None:
    _ = state
    orders = context.get("recent_orders") or []
    audit = context.get("recent_audit") or []
    audit_keys = {
        str(item.get("correlation_id") or item.get("order_id") or "")
        for item in audit
        if item.get("correlation_id") or item.get("order_id")
    }
    for order in orders:
        status = str(order.get("status", "") or "").upper()
        if status not in {"FINALIZED", "SETTLED", "COMPLETED", "SUCCESS"}:
            continue
        key = str(order.get("correlation_id") or order.get("order_id") or order.get("id") or "")
        if key and key not in audit_keys:
            return _finding(
                "FINALIZED_WITHOUT_AUDIT_EVIDENCE",
                "critical",
                "Finalized order has no matching audit evidence",
                {"order_key": key, "status": status},
            )
    return None


def rule_event_without_expected_side_effect(context: Dict[str, Any], state: Dict[str, Any]) -> ForensicFinding | None:
    _ = state
    orders = context.get("recent_orders") or []
    audit = context.get("recent_audit") or []
    order_keys = {
        str(item.get("correlation_id") or item.get("order_id") or item.get("id") or "")
        for item in orders
        if item.get("correlation_id") or item.get("order_id") or item.get("id")
    }

    for event in audit:
        ev_type = str(event.get("type", "") or "").upper()
        if ev_type not in {"REQUEST_RECEIVED", "ORDER_FINALIZED", "FINALIZED"}:
            continue
        key = str(event.get("correlation_id") or event.get("order_id") or "")
        if key and key not in order_keys:
            return _finding(
                "EVENT_WITHOUT_EXPECTED_SIDE_EFFECT",
                "warning",
                "Audit event has no expected order side effect",
                {"event_type": ev_type, "event_key": key},
            )
    return None


def rule_snapshot_without_runtime_evidence(context: Dict[str, Any], state: Dict[str, Any]) -> ForensicFinding | None:
    _ = state
    runtime_state = context.get("runtime_state") or {}
    metrics = context.get("metrics") or {}
    forensics = runtime_state.get("forensics") or {}
    snapshot_recent = bool(forensics.get("observability_snapshot_recent", False))
    gauges = (metrics.get("gauges") or {})
    runtime_readiness = runtime_state.get("trading_engine_readiness")

    if snapshot_recent and runtime_readiness in (None, {}, "") and not gauges:
        return _finding(
            "SNAPSHOT_WITHOUT_RUNTIME_EVIDENCE",
            "warning",
            "Recent snapshot exists but runtime evidence is empty",
            {"snapshot_recent": snapshot_recent, "has_gauges": False, "has_readiness": False},
        )
    return None


def rule_diagnostics_bundle_evidence_gap(context: Dict[str, Any], state: Dict[str, Any]) -> ForensicFinding | None:
    _ = state
    health = context.get("health") or {}
    alerts = context.get("alerts") or {}
    incidents = context.get("incidents") or {}
    orders = context.get("recent_orders") or []
    audit = context.get("recent_audit") or []

    overall = str(health.get("overall_status", "NOT_READY") or "NOT_READY")
    active_alerts = int(alerts.get("active_count", 0) or 0)
    open_incidents = int(incidents.get("open_count", 0) or 0)
    if (overall in {"DEGRADED", "NOT_READY"} or active_alerts > 0 or open_incidents > 0) and not orders and not audit:
        return _finding(
            "DIAGNOSTICS_BUNDLE_EVIDENCE_GAP",
            "critical",
            "Diagnostics evidence missing during degraded/alerted runtime",
            {"overall_status": overall, "active_alerts": active_alerts, "open_incidents": open_incidents},
        )
    return None


def rule_incident_without_supporting_alert(context: Dict[str, Any], state: Dict[str, Any]) -> ForensicFinding | None:
    _ = state
    alerts = context.get("alerts") or {}
    incidents = context.get("incidents") or {}

    active_codes = {str(a.get("code", "") or "") for a in (alerts.get("alerts") or []) if a.get("active")}
    for incident in incidents.get("incidents") or []:
        if str(incident.get("status", "")) != "OPEN":
            continue
        code = str(incident.get("code", "") or "")
        if code and code not in active_codes:
            return _finding(
                "INCIDENT_WITHOUT_SUPPORTING_ALERT",
                "warning",
                "Open incident has no supporting active alert",
                {"incident_code": code},
            )
    return None


def rule_alert_without_runtime_context(context: Dict[str, Any], state: Dict[str, Any]) -> ForensicFinding | None:
    _ = state
    alerts = context.get("alerts") or {}
    runtime_state = context.get("runtime_state") or {}

    active_alerts: List[Dict[str, Any]] = [a for a in (alerts.get("alerts") or []) if a.get("active")]
    has_runtime_context = any(k in runtime_state for k in ("mode", "pid", "trading_engine_readiness"))
    if active_alerts and not has_runtime_context:
        code = str(active_alerts[0].get("code", "UNKNOWN") or "UNKNOWN")
        return _finding(
            "ALERT_WITHOUT_RUNTIME_CONTEXT",
            "warning",
            "Active alert exists without runtime context",
            {"sample_alert_code": code},
        )
    return None


DEFAULT_FORENSICS_RULES = [
    rule_failed_but_remote_exists,
    rule_finalized_without_audit_evidence,
    rule_event_without_expected_side_effect,
    rule_snapshot_without_runtime_evidence,
    rule_diagnostics_bundle_evidence_gap,
    rule_incident_without_supporting_alert,
    rule_alert_without_runtime_context,
]
