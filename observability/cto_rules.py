from __future__ import annotations

from typing import Any, Dict, List


def _finding(*, rule_name: str, severity: str, short_explanation: str, key_metrics: Dict[str, Any], correlation_summary: str, suggested_action: str) -> Dict[str, Any]:
    return {
        "rule_name": rule_name,
        "severity": severity,
        "short_explanation": short_explanation,
        "key_metrics": dict(key_metrics),
        "correlation_summary": correlation_summary,
        "suggested_action": suggested_action,
    }


def evaluate_cto_rules(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    metrics = dict(payload.get("metrics") or {})
    anomalies = list(payload.get("anomaly_alerts") or [])
    forensics = list(payload.get("forensics_alerts") or [])
    incidents = dict(payload.get("incidents") or {})
    runtime_probe = dict(payload.get("runtime_probe") or {})
    diagnostics = dict(payload.get("diagnostics") or {})
    health = dict(payload.get("health") or {})

    findings: List[Dict[str, Any]] = []

    high_codes = {str(x.get("code") or "") for x in anomalies if str(x.get("severity", "")).lower() in {"high", "critical", "error"}}
    stalled_ticks = int(metrics.get("stalled_ticks", 0) or 0)
    backlog = float(metrics.get("writer_backlog", 0) or 0)
    memory_growth_mb = float(metrics.get("memory_growth_mb", 0) or 0)
    timeout_count = int(metrics.get("network_timeout_count", 0) or 0)
    contention = int(metrics.get("db_lock_errors", 0) or 0)
    observability_missing = int(metrics.get("missing_observability_sections", 0) or 0)

    if len(high_codes) >= 2 and int(metrics.get("repeated_high_ticks", 0) or 0) >= 2:
        findings.append(_finding(
            rule_name="RISK_ESCALATION_CHAIN",
            severity="critical",
            short_explanation="Multiple high-severity signals persisted across ticks",
            key_metrics={"repeated_high_ticks": int(metrics.get("repeated_high_ticks", 0) or 0), "high_codes": sorted([c for c in high_codes if c])},
            correlation_summary="Independent anomaly signals are escalating together",
            suggested_action="Escalate to on-call CTO runbook and freeze risky automation paths",
        ))

    pipeline = dict(runtime_probe.get("alert_pipeline") or {})
    if bool(pipeline.get("enabled")) and not bool(pipeline.get("deliverable", True)):
        findings.append(_finding(
            rule_name="SILENT_FAILURE_DETECTED",
            severity="high",
            short_explanation="Alert pipeline is enabled but not deliverable",
            key_metrics={"enabled": True, "deliverable": False, "reason": pipeline.get("reason", "undeliverable")},
            correlation_summary="Operator notification channel would silently drop alerts",
            suggested_action="Restore sender/chat routing immediately and verify delivery health checks",
        ))

    if bool(metrics.get("state_mismatch")) and int(incidents.get("open_count", 0) or 0) > 0:
        findings.append(_finding(
            rule_name="STATE_INCONSISTENCY_CRITICAL",
            severity="critical",
            short_explanation="Runtime state mismatch is present with active incidents",
            key_metrics={"state_mismatch": True, "open_incidents": int(incidents.get("open_count", 0) or 0)},
            correlation_summary="State invariants are broken while system is already unstable",
            suggested_action="Disable writes and run reconciliation before continuing trading",
        ))

    if stalled_ticks >= 2 and float(metrics.get("completed_delta", 0) or 0) <= 0:
        findings.append(_finding(
            rule_name="STALLED_SYSTEM_DETECTED",
            severity="high",
            short_explanation="Heartbeat/progress indicate system stall",
            key_metrics={"stalled_ticks": stalled_ticks, "completed_delta": float(metrics.get("completed_delta", 0) or 0)},
            correlation_summary="Components appear alive but meaningful work is not progressing",
            suggested_action="Investigate worker liveness and unblock event processing pipeline",
        ))

    if timeout_count > 0 and bool(metrics.get("ambiguous_submissions")):
        findings.append(_finding(
            rule_name="DATA_DRIFT_SUSPECTED",
            severity="high",
            short_explanation="Timeouts plus ambiguous submissions can create data drift",
            key_metrics={"network_timeout_count": timeout_count, "ambiguous_submissions": int(metrics.get("ambiguous_submissions", 0) or 0)},
            correlation_summary="Submit lifecycle is incomplete and may diverge local/remote truth",
            suggested_action="Run targeted reconciliation and mark unresolved orders ambiguous",
        ))

    if observability_missing > 0 or not diagnostics.get("available", True):
        findings.append(_finding(
            rule_name="OBSERVABILITY_UNTRUSTED",
            severity="high",
            short_explanation="Observability evidence is incomplete",
            key_metrics={"missing_observability_sections": observability_missing, "diagnostics_available": bool(diagnostics.get("available", True))},
            correlation_summary="Missing evidence weakens confidence in reviewer conclusions",
            suggested_action="Repair diagnostics export path and verify required sections are present",
        ))

    if contention > 0 and (timeout_count > 0 or bool(metrics.get("ambiguous_submissions"))):
        findings.append(_finding(
            rule_name="CASCADE_FAILURE_RISK",
            severity="critical",
            short_explanation="DB contention combines with runtime instability signals",
            key_metrics={"db_lock_errors": contention, "network_timeout_count": timeout_count, "ambiguous_submissions": int(metrics.get("ambiguous_submissions", 0) or 0)},
            correlation_summary="Layered failures can cascade into false-success/visibility gaps",
            suggested_action="Reduce load, clear lock contention, and run fail-closed reconcile checks",
        ))

    if backlog > 0 and memory_growth_mb >= 50:
        findings.append(_finding(
            rule_name="MEMORY_GROWTH_TREND",
            severity="warning" if memory_growth_mb < 100 else "high",
            short_explanation="Backlog growth is correlated with memory growth trend",
            key_metrics={"writer_backlog": backlog, "memory_growth_mb": memory_growth_mb},
            correlation_summary="Persistent queue pressure may degrade runtime over time",
            suggested_action="Drain backlog and inspect async writer throughput bottlenecks",
        ))

    return findings
