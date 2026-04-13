from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


DEFAULT_MATCH_FIELDS: Tuple[str, ...] = (
    "trace_id",
    "request_id",
    "session_id",
    "order_id",
    "user_id",
)


def correlate_events(
    events: Iterable[Mapping[str, Any]],
    *,
    timestamp_field: str = "ts",
    match_fields: Sequence[str] = DEFAULT_MATCH_FIELDS,
    window_seconds: int = 300,
) -> List[Dict[str, Any]]:
    """Correlate events by shared identity fields within a time window.

    This function is intentionally isolated and side-effect free; it consumes the
    input events and returns deterministic correlation clusters.
    """

    event_list = list(events)
    if not event_list:
        return []

    prepared: List[Dict[str, Any]] = []
    for index, event in enumerate(event_list):
        ts = event.get(timestamp_field)
        if not isinstance(ts, (int, float)):
            continue
        prepared.append({"index": index, "event": event, "ts": float(ts)})

    prepared.sort(key=lambda item: item["ts"])
    if not prepared:
        return []

    parents = list(range(len(prepared)))

    def find(node: int) -> int:
        while parents[node] != node:
            parents[node] = parents[parents[node]]
            node = parents[node]
        return node

    def union(left: int, right: int) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parents[right_root] = left_root

    for left in range(len(prepared)):
        for right in range(left + 1, len(prepared)):
            delta = prepared[right]["ts"] - prepared[left]["ts"]
            if delta > window_seconds:
                break
            for field in match_fields:
                left_value = prepared[left]["event"].get(field)
                if left_value is None or left_value == "":
                    continue
                if left_value == prepared[right]["event"].get(field):
                    union(left, right)
                    break

    groups: Dict[int, List[int]] = defaultdict(list)
    for idx in range(len(prepared)):
        groups[find(idx)].append(idx)

    correlations: List[Dict[str, Any]] = []
    for cluster_num, members in enumerate(groups.values(), start=1):
        if len(members) < 2:
            continue

        cluster_events = [prepared[item] for item in members]
        shared: Dict[str, Any] = {}
        for field in match_fields:
            values = {entry["event"].get(field) for entry in cluster_events}
            values.discard(None)
            values.discard("")
            if len(values) == 1:
                shared[field] = next(iter(values))

        correlations.append(
            {
                "cluster_id": f"corr-{cluster_num}",
                "event_indices": [entry["index"] for entry in cluster_events],
                "event_ids": [entry["event"].get("id") for entry in cluster_events],
                "start_ts": cluster_events[0]["ts"],
                "end_ts": cluster_events[-1]["ts"],
                "shared": shared,
            }
        )

    correlations.sort(key=lambda item: (item["start_ts"], item["cluster_id"]))
    return correlations


# ---------------------------------------------------------------------------
# Operational correlation rules
# ---------------------------------------------------------------------------

CorrelationFinding = Dict[str, Any]
CorrelationContext = Dict[str, Any]
CorrelationState = Dict[str, Any]


def _correlation_finding(code: str, severity: str, message: str, details: Dict[str, Any]) -> CorrelationFinding:
    return {"code": code, "severity": severity, "message": message, "details": details}


def rule_local_vs_remote(context: CorrelationContext, state: CorrelationState) -> Optional[CorrelationFinding]:
    orders = context.get("recent_orders") or []
    mismatched = [
        {"id": o.get("order_id") or o.get("id"), "local": o.get("status"), "remote": o.get("remote_status")}
        for o in orders
        if o.get("status") and o.get("remote_status") and o.get("status") != o.get("remote_status")
    ]
    if mismatched:
        return _correlation_finding("LOCAL_VS_REMOTE_MISMATCH", "critical",
            "Local order status does not match remote status",
            {"mismatched_count": len(mismatched), "sample": mismatched[:3]})
    return None


def rule_db_vs_memory(context: CorrelationContext, state: CorrelationState) -> Optional[CorrelationFinding]:
    metrics = (context.get("metrics") or {})
    gauges = (metrics.get("gauges") or {}) if isinstance(metrics, dict) else {}
    db_state = context.get("db_state") or {}
    direct_db_count = db_state.get("inflight_orders_count")
    db_count = int(direct_db_count if direct_db_count is not None else (gauges.get("db_inflight_count", -1) or -1))
    mem_count = int(gauges.get("inflight_count", 0) or 0)
    if db_count >= 0 and abs(db_count - mem_count) > 0:
        details: Dict[str, Any] = {
            "db_count": db_count,
            "memory_count": mem_count,
            "delta": abs(db_count - mem_count),
        }
        if direct_db_count is not None:
            details["db_source"] = "diagnostics_recent_orders"
        # Augment with DB write queue depth to distinguish expected write lag
        # from true DB/memory corruption — direct typed evidence from AsyncDBWriter.
        db_write_queue = context.get("db_write_queue") or {}
        write_queue_depth = db_write_queue.get("queue_depth")
        if write_queue_depth is not None:
            details["db_write_queue_depth"] = int(write_queue_depth)
        return _correlation_finding("DB_VS_MEMORY_MISMATCH", "warning",
            "DB inflight count differs from in-memory inflight count", details)
    return None


def rule_submit_reconcile_chain_break(context: CorrelationContext, state: CorrelationState) -> Optional[CorrelationFinding]:
    reconcile_chain = context.get("reconcile_chain") or {}
    missing_count = reconcile_chain.get("missing_count")
    finalized_missing_count = reconcile_chain.get("finalized_missing_count")
    submitted_count = int(reconcile_chain.get("submitted_count", 0) or 0)
    reconciled_count = int(reconcile_chain.get("reconciled_count", 0) or 0)
    sample_ids = list(reconcile_chain.get("sample_missing_ids") or [])[:3]
    sample_finalized_ids = list(reconcile_chain.get("sample_finalized_missing_ids") or [])[:3]
    has_canonical_evidence = (
        (missing_count is not None or finalized_missing_count is not None)
        and (
            int(missing_count or 0) > 0
            or int(finalized_missing_count or 0) > 0
            or submitted_count > 0
            or reconciled_count > 0
            or len(sample_ids) > 0
            or len(sample_finalized_ids) > 0
        )
    )
    if has_canonical_evidence:
        broken_count = int(missing_count or 0)
        finalized_broken_count = int(finalized_missing_count or 0)
        if broken_count > 0 or finalized_broken_count > 0:
            details = {
                "broken_count": broken_count,
                "sample_ids": sample_ids,
                "finalized_broken_count": finalized_broken_count,
                "sample_finalized_ids": sample_finalized_ids,
            }
            details["source"] = "canonical_reconcile_chain"
            return _correlation_finding("SUBMIT_RECONCILE_CHAIN_BREAK", "warning",
                "Submitted orders missing from reconciliation audit trail", details)
        return None

    orders = context.get("recent_orders") or []
    # Orders that are SUBMITTED but never appeared in reconciliation
    submitted_ids = {o.get("order_id") or o.get("id") for o in orders if str(o.get("status", "")).upper() == "SUBMITTED"}
    reconciled_ids = {r.get("order_id") or r.get("id") for r in (context.get("recent_audit") or [])}
    broken = [oid for oid in submitted_ids if oid and oid not in reconciled_ids]
    if len(broken) > 0:
        return _correlation_finding("SUBMIT_RECONCILE_CHAIN_BREAK", "warning",
            "Submitted orders missing from reconciliation audit trail",
            {"broken_count": len(broken), "sample_ids": broken[:3]})
    return None


def rule_event_side_effect_gap(context: CorrelationContext, state: CorrelationState) -> Optional[CorrelationFinding]:
    event_bus = context.get("event_bus") or {}
    # Prefer direct published_total (from EventBus.published_total_count) over loose
    # injected events_published gauge — direct evidence is authoritative.
    published = int(event_bus.get("published_total", event_bus.get("events_published", 0)) or 0)
    side_effects = int(event_bus.get("side_effects_confirmed", 0) or 0)
    prev_pub = int(state.get("prev_published", 0) or 0)
    prev_fx = int(state.get("prev_side_effects", 0) or 0)
    state["prev_published"] = published
    state["prev_side_effects"] = side_effects
    delta_pub = published - prev_pub
    delta_fx = side_effects - prev_fx
    if delta_pub > 0 and delta_fx < delta_pub:
        return _correlation_finding("EVENT_SIDE_EFFECT_GAP", "warning",
            "Published events exceed confirmed downstream side effects",
            {"events_published_delta": delta_pub, "side_effects_delta": delta_fx, "gap": delta_pub - delta_fx})
    return None


def rule_queue_depth_liveness(context: CorrelationContext, state: CorrelationState) -> Optional[CorrelationFinding]:
    metrics = context.get("metrics") or {}
    gauges = (metrics.get("gauges") or {}) if isinstance(metrics, dict) else {}
    # Prefer direct event_bus.queue_depth (from EventBus.queue_depth()) over loose
    # metrics gauge — direct typed evidence from the live queue object is authoritative.
    event_bus = context.get("event_bus") or {}
    direct_depth = event_bus.get("queue_depth")
    queue_depth = float(direct_depth if direct_depth is not None else gauges.get("queue_depth", 0.0) or 0.0)
    heartbeat_age = float(gauges.get("last_heartbeat_age_sec", 0.0) or 0.0)
    running = event_bus.get("running")
    workers_alive = event_bus.get("worker_threads_alive")
    # Strong direct contradiction: queue has work but dispatcher is not running
    # or has no alive worker threads.
    if queue_depth > 0 and (running is False or (isinstance(workers_alive, int) and workers_alive <= 0)):
        return _correlation_finding(
            "QUEUE_DEPTH_DISPATCHER_CONTRADICTION",
            "critical",
            "Queue has pending work but dispatcher liveness is down",
            {"queue_depth": queue_depth, "running": bool(running) if running is not None else None, "worker_threads_alive": workers_alive},
        )
    # Queue has depth but heartbeat is stale → liveness contradiction
    if queue_depth > 0 and heartbeat_age > 60.0:
        return _correlation_finding("QUEUE_DEPTH_LIVENESS_CONTRADICTION", "critical",
            "Queue has pending work but heartbeat is stale — worker may be dead",
            {"queue_depth": queue_depth, "heartbeat_age_sec": heartbeat_age})
    return None


DEFAULT_CORRELATION_RULES = [
    rule_local_vs_remote,
    rule_db_vs_memory,
    rule_submit_reconcile_chain_break,
    rule_event_side_effect_gap,
    rule_queue_depth_liveness,
]


class CorrelationEvaluator:
    def __init__(self, rules=None):
        self.rules = list(rules if rules is not None else DEFAULT_CORRELATION_RULES)
        self.state: Dict[str, Dict[str, Any]] = {}

    def evaluate(self, context: Dict[str, Any]) -> List[Dict[str, Any]]:
        findings = []
        for rule in self.rules:
            rule_name = getattr(rule, "__name__", "rule")
            rule_state = self.state.setdefault(rule_name, {})
            try:
                item = rule(context, rule_state)
            except Exception:
                item = None
            if item:
                findings.append(item)
        return findings


def evaluate_correlation_rules(context: Dict[str, Any], *, evaluator: "Optional[CorrelationEvaluator]" = None) -> List[Dict[str, Any]]:
    """Evaluate all default correlation rules against context. Returns list of findings."""
    if evaluator is not None:
        return evaluator.evaluate(context)
    return CorrelationEvaluator().evaluate(context)
