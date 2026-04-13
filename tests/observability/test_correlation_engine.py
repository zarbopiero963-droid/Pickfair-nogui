from observability.correlation_engine import (
    CorrelationEvaluator,
    DEFAULT_CORRELATION_RULES,
    correlate_events,
    evaluate_correlation_rules,
    rule_db_vs_memory,
    rule_event_side_effect_gap,
    rule_local_vs_remote,
    rule_queue_depth_liveness,
    rule_submit_reconcile_chain_break,
)


def test_correlate_events_groups_by_shared_identity_within_window():
    events = [
        {"id": "a", "ts": 1000, "request_id": "req-1", "user_id": "u-1"},
        {"id": "b", "ts": 1010, "request_id": "req-1", "user_id": "u-2"},
        {"id": "c", "ts": 1020, "request_id": "req-2", "user_id": "u-2"},
    ]

    correlations = correlate_events(events, window_seconds=60)

    assert len(correlations) == 1
    cluster = correlations[0]
    assert cluster["event_ids"] == ["a", "b", "c"]
    assert cluster["shared"] == {}


def test_correlate_events_respects_time_window():
    events = [
        {"id": "a", "ts": 1000, "trace_id": "t-1"},
        {"id": "b", "ts": 1405, "trace_id": "t-1"},
    ]

    correlations = correlate_events(events, window_seconds=300)

    assert correlations == []


def test_correlate_events_ignores_invalid_timestamps_and_does_not_mutate_inputs():
    events = [
        {"id": "a", "ts": "1000", "request_id": "req-1"},
        {"id": "b", "ts": 1005, "request_id": "req-1"},
        {"id": "c", "ts": 1006, "request_id": "req-1"},
    ]
    snapshot = [dict(item) for item in events]

    correlations = correlate_events(events)

    assert len(correlations) == 1
    assert correlations[0]["event_ids"] == ["b", "c"]
    assert events == snapshot


# ---------------------------------------------------------------------------
# Operational correlation rule tests
# ---------------------------------------------------------------------------

def test_default_correlation_rules_contains_all_five():
    assert len(DEFAULT_CORRELATION_RULES) == 5
    assert rule_local_vs_remote in DEFAULT_CORRELATION_RULES
    assert rule_db_vs_memory in DEFAULT_CORRELATION_RULES
    assert rule_submit_reconcile_chain_break in DEFAULT_CORRELATION_RULES
    assert rule_event_side_effect_gap in DEFAULT_CORRELATION_RULES
    assert rule_queue_depth_liveness in DEFAULT_CORRELATION_RULES


def test_rule_local_vs_remote_fires_on_mismatch():
    ctx = {"recent_orders": [
        {"order_id": "o1", "status": "OPEN", "remote_status": "CANCELLED"},
    ]}
    finding = rule_local_vs_remote(ctx, {})
    assert finding is not None
    assert finding["code"] == "LOCAL_VS_REMOTE_MISMATCH"
    assert finding["severity"] == "critical"


def test_rule_local_vs_remote_passes_on_match():
    ctx = {"recent_orders": [
        {"order_id": "o1", "status": "OPEN", "remote_status": "OPEN"},
    ]}
    assert rule_local_vs_remote(ctx, {}) is None


def test_rule_db_vs_memory_fires_on_delta():
    ctx = {"metrics": {"gauges": {"db_inflight_count": 5, "inflight_count": 3}}}
    finding = rule_db_vs_memory(ctx, {})
    assert finding is not None
    assert finding["code"] == "DB_VS_MEMORY_MISMATCH"


def test_rule_db_vs_memory_passes_when_equal():
    ctx = {"metrics": {"gauges": {"db_inflight_count": 3, "inflight_count": 3}}}
    assert rule_db_vs_memory(ctx, {}) is None


def test_rule_submit_reconcile_chain_break_fires_on_unreconciled():
    ctx = {
        "recent_orders": [{"order_id": "o1", "status": "SUBMITTED"}],
        "recent_audit": [],
    }
    finding = rule_submit_reconcile_chain_break(ctx, {})
    assert finding is not None
    assert finding["code"] == "SUBMIT_RECONCILE_CHAIN_BREAK"


def test_rule_submit_reconcile_chain_break_passes_when_reconciled():
    ctx = {
        "recent_orders": [{"order_id": "o1", "status": "SUBMITTED"}],
        "recent_audit": [{"order_id": "o1"}],
    }
    assert rule_submit_reconcile_chain_break(ctx, {}) is None


def test_rule_submit_reconcile_chain_break_prefers_canonical_block():
    ctx = {
        "reconcile_chain": {
            "missing_count": 2,
            "sample_missing_ids": ["o7", "o8"],
        },
        # fallback lists intentionally contradictory to prove canonical preference
        "recent_orders": [],
        "recent_audit": [{"order_id": "o7"}, {"order_id": "o8"}],
    }
    finding = rule_submit_reconcile_chain_break(ctx, {})
    assert finding is not None
    assert finding["code"] == "SUBMIT_RECONCILE_CHAIN_BREAK"
    assert finding["details"]["broken_count"] == 2
    assert finding["details"]["source"] == "canonical_reconcile_chain"


def test_rule_submit_reconcile_chain_break_uses_finalize_stage_from_canonical_block():
    ctx = {
        "reconcile_chain": {
            "missing_count": 0,
            "submitted_count": 2,
            "reconciled_count": 2,
            "finalized_missing_count": 1,
            "sample_finalized_missing_ids": ["o-final-1"],
        },
        "recent_orders": [],
        "recent_audit": [],
    }
    finding = rule_submit_reconcile_chain_break(ctx, {})
    assert finding is not None
    assert finding["code"] == "SUBMIT_RECONCILE_CHAIN_BREAK"
    assert finding["details"]["broken_count"] == 0
    assert finding["details"]["finalized_broken_count"] == 1
    assert finding["details"]["sample_finalized_ids"] == ["o-final-1"]
    assert finding["details"]["source"] == "canonical_reconcile_chain"


def test_rule_submit_reconcile_chain_break_falls_back_when_canonical_is_empty():
    ctx = {
        # Empty canonical block is allowed in default runtime; rule must still
        # derive from recent_orders/recent_audit when richer evidence exists there.
        "reconcile_chain": {"missing_count": 0, "submitted_count": 0, "reconciled_count": 0},
        "recent_orders": [{"order_id": "o1", "status": "SUBMITTED"}],
        "recent_audit": [],
    }
    finding = rule_submit_reconcile_chain_break(ctx, {})
    assert finding is not None
    assert finding["code"] == "SUBMIT_RECONCILE_CHAIN_BREAK"
    assert finding["details"]["broken_count"] == 1
    assert "source" not in finding["details"]


def test_rule_event_side_effect_gap_fires_on_gap():
    state = {"prev_published": 0, "prev_side_effects": 0}
    ctx = {"event_bus": {"events_published": 10, "side_effects_confirmed": 5}}
    finding = rule_event_side_effect_gap(ctx, state)
    assert finding is not None
    assert finding["code"] == "EVENT_SIDE_EFFECT_GAP"
    assert finding["details"]["gap"] == 5


def test_rule_event_side_effect_gap_passes_when_balanced():
    state = {"prev_published": 0, "prev_side_effects": 0}
    ctx = {"event_bus": {"events_published": 10, "side_effects_confirmed": 10}}
    assert rule_event_side_effect_gap(ctx, state) is None


def test_rule_queue_depth_liveness_fires_on_stale_heartbeat():
    ctx = {"metrics": {"gauges": {"queue_depth": 5.0, "last_heartbeat_age_sec": 90.0}}}
    finding = rule_queue_depth_liveness(ctx, {})
    assert finding is not None
    assert finding["code"] == "QUEUE_DEPTH_LIVENESS_CONTRADICTION"
    assert finding["severity"] == "critical"


def test_rule_queue_depth_liveness_passes_on_fresh_heartbeat():
    ctx = {"metrics": {"gauges": {"queue_depth": 5.0, "last_heartbeat_age_sec": 10.0}}}
    assert rule_queue_depth_liveness(ctx, {}) is None


def test_correlation_evaluator_stateful_across_calls():
    """CorrelationEvaluator preserves per-rule state across evaluate() calls."""
    evaluator = CorrelationEvaluator()

    ctx1 = {"event_bus": {"events_published": 10, "side_effects_confirmed": 5}}
    evaluator.evaluate(ctx1)

    ctx2 = {"event_bus": {"events_published": 10, "side_effects_confirmed": 5}}
    findings2 = evaluator.evaluate(ctx2)
    # No delta on second call → no gap finding
    gap_findings = [f for f in findings2 if f["code"] == "EVENT_SIDE_EFFECT_GAP"]
    assert gap_findings == []


def test_evaluate_correlation_rules_returns_findings():
    ctx = {
        "recent_orders": [{"order_id": "o1", "status": "OPEN", "remote_status": "CANCELLED"}],
        "metrics": {"gauges": {}},
        "event_bus": {},
        "recent_audit": [],
    }
    findings = evaluate_correlation_rules(ctx)
    codes = {f["code"] for f in findings}
    assert "LOCAL_VS_REMOTE_MISMATCH" in codes


def test_correlation_evaluator_no_crash_on_empty_context():
    evaluator = CorrelationEvaluator()
    findings = evaluator.evaluate({})
    assert isinstance(findings, list)


# ---------------------------------------------------------------------------
# Task: reviewer_strong_collectors — direct evidence preference
# ---------------------------------------------------------------------------

def test_rule_event_side_effect_gap_prefers_published_total_over_events_published():
    """When both published_total (direct) and events_published (loose) are present,
    published_total must be used — it is the authoritative source from EventBus."""
    state = {}
    ctx = {
        "event_bus": {
            "published_total": 20,       # direct evidence from EventBus
            "events_published": 5,        # stale/loose injected gauge
            "side_effects_confirmed": 10,
        }
    }
    finding = rule_event_side_effect_gap(ctx, state)
    assert finding is not None
    # delta should be 20-0 = 20, side_effects_delta = 10-0 = 10 → gap = 10
    assert finding["details"]["events_published_delta"] == 20
    assert finding["details"]["gap"] == 10


def test_rule_event_side_effect_gap_falls_back_to_events_published():
    """When published_total is absent, events_published is used as fallback."""
    state = {}
    ctx = {
        "event_bus": {
            "events_published": 8,
            "side_effects_confirmed": 3,
        }
    }
    finding = rule_event_side_effect_gap(ctx, state)
    assert finding is not None
    assert finding["details"]["events_published_delta"] == 8
    assert finding["details"]["gap"] == 5


def test_rule_queue_depth_liveness_prefers_direct_event_bus_depth():
    """When event_bus.queue_depth (direct) is present, it takes precedence over
    the loose metrics gauge — even if the gauge says zero."""
    ctx = {
        "event_bus": {"queue_depth": 8},
        "metrics": {
            "gauges": {
                "queue_depth": 0.0,           # gauge says no depth
                "last_heartbeat_age_sec": 90.0,
            }
        },
    }
    finding = rule_queue_depth_liveness(ctx, {})
    assert finding is not None, "direct queue_depth=8 must trigger the rule"
    assert finding["code"] == "QUEUE_DEPTH_LIVENESS_CONTRADICTION"
    assert finding["details"]["queue_depth"] == 8.0


def test_rule_queue_depth_liveness_falls_back_to_gauge_when_no_direct_depth():
    """When event_bus has no queue_depth, metrics gauge is used."""
    ctx = {
        "event_bus": {},
        "metrics": {"gauges": {"queue_depth": 5.0, "last_heartbeat_age_sec": 90.0}},
    }
    finding = rule_queue_depth_liveness(ctx, {})
    assert finding is not None
    assert finding["details"]["queue_depth"] == 5.0


def test_rule_db_vs_memory_includes_write_queue_depth_in_details():
    """When db_write_queue.queue_depth is available, it must appear in finding details
    to help distinguish expected write lag from true DB/memory corruption."""
    ctx = {
        "metrics": {"gauges": {"db_inflight_count": 5, "inflight_count": 3}},
        "db_write_queue": {"queue_depth": 12},
    }
    finding = rule_db_vs_memory(ctx, {})
    assert finding is not None
    assert finding["code"] == "DB_VS_MEMORY_MISMATCH"
    assert finding["details"]["db_write_queue_depth"] == 12


def test_rule_db_vs_memory_omits_write_queue_depth_when_absent():
    """When db_write_queue is not present, finding must still be emitted without crashing."""
    ctx = {"metrics": {"gauges": {"db_inflight_count": 5, "inflight_count": 3}}}
    finding = rule_db_vs_memory(ctx, {})
    assert finding is not None
    assert "db_write_queue_depth" not in finding["details"]


def test_rule_db_vs_memory_prefers_direct_db_state_over_gauge():
    ctx = {
        "metrics": {"gauges": {"db_inflight_count": 1, "inflight_count": 3}},
        "db_state": {"inflight_orders_count": 8},
    }
    finding = rule_db_vs_memory(ctx, {})
    assert finding is not None
    assert finding["details"]["db_count"] == 8
    assert finding["details"]["db_source"] == "diagnostics_recent_orders"


def test_rule_queue_depth_liveness_fires_when_dispatcher_down_with_pending_depth():
    ctx = {
        "event_bus": {"queue_depth": 6, "running": False, "worker_threads_alive": 0},
        "metrics": {"gauges": {"last_heartbeat_age_sec": 0.0}},
    }
    finding = rule_queue_depth_liveness(ctx, {})
    assert finding is not None
    assert finding["code"] == "QUEUE_DEPTH_DISPATCHER_CONTRADICTION"
    assert finding["severity"] == "critical"
