from __future__ import annotations

from collections import Counter
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
    if delta <= 0:
        return None

    deltas = list(state.get("duplicate_delta_history", []))
    deltas.append(delta)
    if len(deltas) > 6:
        deltas = deltas[-6:]
    state["duplicate_delta_history"] = deltas

    duplicate_signal = _collect_duplicate_signal(context)
    baseline = (sum(deltas[:-1]) / len(deltas[:-1])) if len(deltas) > 1 else 0.0
    strong_vs_baseline = len(deltas) >= 4 and delta >= 3 and delta >= max(3, int(baseline * 2))
    contextual_burst = (
        delta >= 3
        and (
            duplicate_signal["blocked_submit_streak"] >= 3
            or duplicate_signal["same_key_blocked_streak"] >= 2
            or duplicate_signal["recent_duplicate_blocked"] >= 4
        )
    )
    if delta >= 5 or strong_vs_baseline or contextual_burst:
        trigger = "flat_threshold" if delta >= 5 else ("baseline_deviation" if strong_vs_baseline else "contextual_burst")
        return _anomaly(
            "DUPLICATE_BLOCK_SPIKE",
            "warning",
            "Duplicate blocking spiked",
            {
                "delta": delta,
                "total": total,
                "trigger": trigger,
                "baseline_avg_delta": round(baseline, 2),
                "blocked_submit_streak": duplicate_signal["blocked_submit_streak"],
                "same_key_blocked_streak": duplicate_signal["same_key_blocked_streak"],
                "recent_duplicate_blocked": duplicate_signal["recent_duplicate_blocked"],
            },
        )
    return None


def _collect_duplicate_signal(context: Context) -> Dict[str, int]:
    runtime_state = context.get("runtime_state") or {}
    duplicate_runtime = runtime_state.get("duplicate_guard") or {}
    recent_orders = context.get("recent_orders") or []

    blocked_orders = [
        order for order in recent_orders
        if str(order.get("status", "")).upper() == "DUPLICATE_BLOCKED"
    ]
    blocked_submit_streak = int(duplicate_runtime.get("blocked_submit_streak", 0) or 0)
    same_key_blocked_streak = int(duplicate_runtime.get("same_key_blocked_streak", 0) or 0)

    tail_streak = 0
    for order in reversed(recent_orders):
        if str(order.get("status", "")).upper() != "DUPLICATE_BLOCKED":
            break
        tail_streak += 1
    blocked_submit_streak = max(blocked_submit_streak, tail_streak)

    key_counter: Counter[str] = Counter()
    for order in blocked_orders:
        key = str(order.get("event_key") or order.get("customer_ref") or "")
        if key:
            key_counter[key] += 1

    if key_counter:
        same_key_blocked_streak = max(same_key_blocked_streak, max(key_counter.values()))

    return {
        "blocked_submit_streak": blocked_submit_streak,
        "same_key_blocked_streak": same_key_blocked_streak,
        "recent_duplicate_blocked": len(blocked_orders),
    }


def rule_suspicious_duplicate_pattern(context: Context, state: State) -> Anomaly | None:
    duplicate_signal = _collect_duplicate_signal(context)
    repeated_ticks = int(state.get("duplicate_pattern_ticks", 0) or 0)

    evidence: list[str] = []
    if duplicate_signal["blocked_submit_streak"] >= 3:
        evidence.append("repeated_blocked_submits")
    if duplicate_signal["same_key_blocked_streak"] >= 3:
        evidence.append("repeated_same_key_blocks")
    if duplicate_signal["recent_duplicate_blocked"] >= 4:
        evidence.append("duplicate_burst_shape")

    suspicious = len(evidence) >= 2
    repeated_ticks = repeated_ticks + 1 if suspicious else 0
    state["duplicate_pattern_ticks"] = repeated_ticks

    if not suspicious:
        return None

    return _anomaly(
        "SUSPICIOUS_DUPLICATE_PATTERN",
        "warning",
        "Suspicious duplicate behavior observed with repeated runtime evidence",
        {
            "evidence": evidence,
            "blocked_submit_streak": duplicate_signal["blocked_submit_streak"],
            "same_key_blocked_streak": duplicate_signal["same_key_blocked_streak"],
            "recent_duplicate_blocked": duplicate_signal["recent_duplicate_blocked"],
            "consecutive_ticks": repeated_ticks,
        },
    )


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


def rule_service_stalled(context: Context, state: State) -> Anomaly | None:
    health = context.get("health") or {}
    components = (health.get("components") or {}) if isinstance(health, dict) else {}
    stalled = [name for name, comp in components.items()
               if isinstance(comp, dict) and comp.get("status") == "NOT_READY"]
    prev = list(state.get("stalled_components", []))
    state["stalled_components"] = stalled
    # Stalled if same components NOT_READY for 2+ consecutive ticks
    persistent = [c for c in stalled if c in prev]
    if persistent:
        return _anomaly("SERVICE_STALLED", "critical", "Services persistently stalled",
                        {"stalled_components": persistent, "consecutive_count": len(persistent)})
    return None


def rule_heartbeat_stale(context: Context, state: State) -> Anomaly | None:
    metrics = context.get("metrics") or {}
    gauges = (metrics.get("gauges") or {}) if isinstance(metrics, dict) else {}
    last_hb = float(gauges.get("last_heartbeat_age_sec", 0.0) or 0.0)
    threshold = 60.0
    if last_hb > threshold:
        return _anomaly("HEARTBEAT_STALE", "critical", "Heartbeat is stale",
                        {"last_heartbeat_age_sec": last_hb, "threshold_sec": threshold})
    return None


def rule_zombie_worker_suspected(context: Context, state: State) -> Anomaly | None:
    metrics = context.get("metrics") or {}
    gauges = (metrics.get("gauges") or {}) if isinstance(metrics, dict) else {}
    # A zombie worker holds a lock but makes no progress
    inflight = float(gauges.get("inflight_count", 0.0) or 0.0)
    completed_delta = float(gauges.get("completed_delta", 0.0) or 0.0)
    ticks = int(state.get("zombie_ticks", 0) or 0)
    if inflight > 0 and completed_delta == 0:
        ticks += 1
    else:
        ticks = 0
    state["zombie_ticks"] = ticks
    if ticks >= 5:
        return _anomaly("ZOMBIE_WORKER_SUSPECTED", "critical", "Worker may be zombie",
                        {"inflight_count": inflight, "no_progress_ticks": ticks})
    return None


def rule_queue_depth_liveness_mismatch(context: Context, state: State) -> Anomaly | None:
    del state
    metrics = context.get("metrics") or {}
    gauges = (metrics.get("gauges") or {}) if isinstance(metrics, dict) else {}
    queue_depth = float(gauges.get("queue_depth", 0.0) or 0.0)
    worker_alive = bool(gauges.get("worker_alive", True))
    if queue_depth > 0 and not worker_alive:
        return _anomaly("QUEUE_DEPTH_LIVENESS_MISMATCH", "critical",
                        "Queue has depth but no live worker",
                        {"queue_depth": queue_depth, "worker_alive": worker_alive})
    return None


def rule_ghost_order_suspected(context: Context, state: State) -> Anomaly | None:
    """Fires when evidence of a ghost order exists but is incomplete.

    Distinct from ghost_order_detected (confirmed via reconciliation).
    Severity is warning; progression to detected occurs when reconciliation
    confirms ghost_orders_count > 0 on the same event_key.
    Signals: unconfirmed_inflight_count present, or suspected_ghost_count > 0,
    or inflight orders with remote IDs in ambiguous state.
    """
    reconcile = (context.get("runtime_state") or {}).get("reconcile") or {}
    suspected = int(reconcile.get("suspected_ghost_count", 0) or 0)
    unconfirmed = int(reconcile.get("unconfirmed_inflight_count", 0) or 0)
    unconfirmed_age = float(reconcile.get("unconfirmed_inflight_age_sec", 0.0) or 0.0)
    age_threshold = float(reconcile.get("ghost_age_threshold_sec", 120.0) or 120.0)

    # Also check recent_orders for ambiguous orders with remote IDs (incomplete evidence)
    orders = context.get("recent_orders") or []
    ambiguous_with_remote = [
        o for o in orders
        if str(o.get("status", "")).upper() in {"AMBIGUOUS", "UNCERTAIN", "INFLIGHT"}
        and (o.get("remote_bet_id") or o.get("exchange_order_id"))
    ]

    confirmed = int(reconcile.get("ghost_orders_count", 0) or 0) > 0
    triggered = (
        suspected > 0
        or (unconfirmed > 0 and unconfirmed_age > age_threshold)
        or len(ambiguous_with_remote) > 0
    )

    # Progression support: once confirmed by reconciliation, suppression ensures
    # the reviewer naturally transitions from suspected -> detected.
    if confirmed:
        state["suspected_ticks"] = 0
        return None

    if triggered:
        prev = int(state.get("suspected_ticks", 0) or 0)
        event_key = str(reconcile.get("event_key", "") or "")
        state["suspected_ticks"] = prev + 1
        return _anomaly(
            "GHOST_ORDER_SUSPECTED",
            "warning",
            "Incomplete evidence of ghost order(s) — reconciliation confirmation pending",
            {
                "suspected_ghost_count": suspected,
                "unconfirmed_inflight_count": unconfirmed,
                "unconfirmed_inflight_age_sec": unconfirmed_age,
                "ambiguous_with_remote_count": len(ambiguous_with_remote),
                "consecutive_suspected_ticks": prev + 1,
                "event_key": event_key,
            },
        )
    state["suspected_ticks"] = 0
    return None


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


def rule_poison_pill_subscriber(context: Context, state: State) -> Anomaly | None:
    """Fires when a specific event bus subscriber has repeatedly raised exceptions.

    Distinct from event_fanout_incomplete (which counts missing deliveries).
    A poison-pill subscriber is one whose errors exceed a threshold, indicating
    it will consistently corrupt or skip processing for its event type.
    Severity is 'error' — worse than generic fanout incomplete (warning) because
    the subscriber is actively broken, not merely slow or delayed.
    """
    del state
    event_bus = context.get("event_bus") or {}
    subscriber_errors = event_bus.get("subscriber_errors") or {}
    threshold = int(event_bus.get("poison_pill_threshold", 3) or 3)

    poison_pills = {
        name: count
        for name, count in subscriber_errors.items()
        if int(count or 0) >= threshold
    }

    if poison_pills:
        worst = max(poison_pills, key=lambda k: poison_pills[k])
        return _anomaly(
            "POISON_PILL_SUBSCRIBER",
            "error",
            "Event bus subscriber repeatedly raising exceptions — poison-pill pattern",
            {
                "poison_pill_subscribers": poison_pills,
                "worst_subscriber": worst,
                "worst_error_count": poison_pills[worst],
                "threshold": threshold,
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
    rule_suspicious_duplicate_pattern,
    rule_memory_growth_trend,
    rule_stuck_inflight,
    rule_alert_pipeline_disabled,
    rule_forensic_gap,
    rule_service_stalled,
    rule_heartbeat_stale,
    rule_zombie_worker_suspected,
    rule_queue_depth_liveness_mismatch,
    rule_ghost_order_suspected,
    ghost_order_detected,
    exposure_mismatch,
    db_contention_detected,
    rule_poison_pill_subscriber,
    event_fanout_incomplete,
    financial_drift,
]

DISABLED_ANOMALY_RULES: list = []
