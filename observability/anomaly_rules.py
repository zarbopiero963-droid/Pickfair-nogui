from __future__ import annotations

from typing import Any, Dict


Anomaly = Dict[str, Any]
Context = Dict[str, Any]
State = Dict[str, Any]


def _anomaly(code: str, severity: str, message: str, details: Dict[str, Any]) -> Anomaly:
    return {
        "code": code,
        "severity": severity,
        "message": message,
        "details": details,
    }


def rule_ambiguous_spike(context: Context, state: State) -> Anomaly | None:
    counters = (context.get("metrics") or {}).get("counters") or {}
    total = int(counters.get("quick_bet_ambiguous_total", 0) or 0)
    prev = int(state.get("ambiguous_total", 0) or 0)
    state["ambiguous_total"] = total
    delta = total - prev
    if delta >= 3:
        return _anomaly(
            "AMBIGUOUS_SPIKE",
            "warning",
            "Ambiguous decisions spiked",
            {"delta": delta, "total": total},
        )
    return None


def rule_duplicate_block_spike(context: Context, state: State) -> Anomaly | None:
    counters = (context.get("metrics") or {}).get("counters") or {}
    total = int(counters.get("duplicate_blocked_total", 0) or 0)
    prev = int(state.get("duplicate_total", 0) or 0)
    state["duplicate_total"] = total
    delta = total - prev
    if delta >= 5:
        return _anomaly(
            "DUPLICATE_BLOCK_SPIKE",
            "warning",
            "Duplicate blocking spiked",
            {"delta": delta, "total": total},
        )
    return None


def rule_memory_growth_trend(context: Context, state: State) -> Anomaly | None:
    gauges = (context.get("metrics") or {}).get("gauges") or {}
    rss = float(gauges.get("memory_rss_mb", 0.0) or 0.0)
    samples = list(state.get("memory_samples", []))
    samples.append(rss)
    if len(samples) > 5:
        samples = samples[-5:]
    state["memory_samples"] = samples

    if len(samples) >= 4:
        growth = samples[-1] - samples[0]
        monotonic = all(samples[i] <= samples[i + 1] for i in range(len(samples) - 1))
        if monotonic and growth >= 200.0:
            return _anomaly(
                "MEMORY_GROWTH_TREND",
                "warning",
                "Memory RSS is trending upward",
                {"start_mb": samples[0], "end_mb": samples[-1], "growth_mb": growth},
            )
    return None


def rule_stuck_inflight(context: Context, state: State) -> Anomaly | None:
    gauges = (context.get("metrics") or {}).get("gauges") or {}
    inflight = float(gauges.get("inflight_count", 0.0) or 0.0)
    count = int(state.get("stuck_inflight_ticks", 0) or 0)
    if inflight >= 50:
        count += 1
    else:
        count = 0
    state["stuck_inflight_ticks"] = count

    if count >= 3:
        return _anomaly(
            "STUCK_INFLIGHT",
            "warning",
            "Inflight orders appear stuck",
            {"inflight_count": inflight, "consecutive_ticks": count},
        )
    return None


def rule_alert_pipeline_disabled(context: Context, state: State) -> Anomaly | None:
    del state
    runtime_state = context.get("runtime_state") or {}
    pipeline = runtime_state.get("alert_pipeline") or {}
    if bool(pipeline.get("alerts_enabled")) and not bool(pipeline.get("sender_available")):
        return _anomaly(
            "ALERT_PIPELINE_DISABLED",
            "critical",
            "Telegram alerts enabled but no sender is available",
            {
                "alerts_enabled": bool(pipeline.get("alerts_enabled")),
                "sender_available": bool(pipeline.get("sender_available")),
            },
        )
    return None


def rule_forensic_gap(context: Context, state: State) -> Anomaly | None:
    del state
    health = context.get("health") or {}
    runtime_state = context.get("runtime_state") or {}
    alerts = context.get("alerts") or {}

    overall = str(health.get("overall_status", "NOT_READY") or "NOT_READY")
    active_alerts = int(alerts.get("active_count", 0) or 0)
    forensics = runtime_state.get("forensics") or {}
    snapshots_recent = bool(forensics.get("observability_snapshot_recent", True))

    if (overall in {"DEGRADED", "NOT_READY"} or active_alerts > 0) and not snapshots_recent:
        return _anomaly(
            "FORENSIC_GAP",
            "warning",
            "No recent observability snapshot available during degraded runtime",
            {
                "overall_status": overall,
                "active_alerts": active_alerts,
                "observability_snapshot_recent": snapshots_recent,
            },
        )
    return None


# New anomaly rules (disabled by default).
def ghost_order_detected(context: Context, state: State) -> Anomaly | None:
    del state
    reconcile = (context.get("runtime_state") or {}).get("reconcile") or {}
    ghost_orders = int(reconcile.get("ghost_orders_count", 0) or 0)
    if ghost_orders > 0:
        return _anomaly(
            "GHOST_ORDER_DETECTED",
            "critical",
            "Ghost orders detected during reconciliation",
            {"ghost_orders_count": ghost_orders},
        )
    return None


def exposure_mismatch(context: Context, state: State) -> Anomaly | None:
    del state
    risk = context.get("risk") or {}
    expected = float(risk.get("expected_exposure", 0.0) or 0.0)
    actual = float(risk.get("actual_exposure", 0.0) or 0.0)
    tolerance = float(risk.get("exposure_tolerance", 0.01) or 0.01)
    diff = abs(expected - actual)
    if diff > tolerance:
        return _anomaly(
            "EXPOSURE_MISMATCH",
            "warning",
            "Risk exposure mismatch exceeds tolerance",
            {
                "expected_exposure": expected,
                "actual_exposure": actual,
                "difference": diff,
                "tolerance": tolerance,
            },
        )
    return None


def db_contention_detected(context: Context, state: State) -> Anomaly | None:
    del state
    db = context.get("db") or {}
    lock_wait_ms = float(db.get("lock_wait_ms", 0.0) or 0.0)
    contention_events = int(db.get("contention_events", 0) or 0)
    threshold_ms = float(db.get("lock_wait_threshold_ms", 200.0) or 200.0)

    if contention_events > 0 or lock_wait_ms > threshold_ms:
        return _anomaly(
            "DB_CONTENTION_DETECTED",
            "warning",
            "Database contention indicators exceeded threshold",
            {
                "lock_wait_ms": lock_wait_ms,
                "contention_events": contention_events,
                "lock_wait_threshold_ms": threshold_ms,
            },
        )
    return None


def event_fanout_incomplete(context: Context, state: State) -> Anomaly | None:
    del state
    event_bus = context.get("event_bus") or {}
    expected = int(event_bus.get("expected_fanout", 0) or 0)
    delivered = int(event_bus.get("delivered_fanout", 0) or 0)

    if expected > 0 and delivered < expected:
        return _anomaly(
            "EVENT_FANOUT_INCOMPLETE",
            "warning",
            "Event fanout did not reach all expected subscribers",
            {
                "expected_fanout": expected,
                "delivered_fanout": delivered,
                "missing_fanout": expected - delivered,
            },
        )
    return None


def financial_drift(context: Context, state: State) -> Anomaly | None:
    del state
    financials = context.get("financials") or {}
    ledger = float(financials.get("ledger_balance", 0.0) or 0.0)
    venue = float(financials.get("venue_balance", 0.0) or 0.0)
    threshold = float(financials.get("drift_threshold", 0.01) or 0.01)
    drift = abs(ledger - venue)

    if drift > threshold:
        return _anomaly(
            "FINANCIAL_DRIFT",
            "critical",
            "Financial balance drift exceeds threshold",
            {
                "ledger_balance": ledger,
                "venue_balance": venue,
                "drift": drift,
                "drift_threshold": threshold,
            },
        )
    return None


DEFAULT_ANOMALY_RULES = [
    rule_ambiguous_spike,
    rule_duplicate_block_spike,
    rule_memory_growth_trend,
    rule_stuck_inflight,
    rule_alert_pipeline_disabled,
    rule_forensic_gap,
]

DISABLED_ANOMALY_RULES = [
    ghost_order_detected,
    exposure_mismatch,
    db_contention_detected,
    event_fanout_incomplete,
    financial_drift,
]
