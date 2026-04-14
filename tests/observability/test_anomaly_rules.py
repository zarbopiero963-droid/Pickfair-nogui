from observability.anomaly_config_builder import build_anomaly_rule_config
from observability.anomaly_rules import (
    DEFAULT_ANOMALY_RULES,
    DISABLED_ANOMALY_RULES,
    db_contention_detected,
    event_fanout_incomplete,
    exposure_mismatch,
    financial_drift,
    financial_drift_detected,
    ghost_order_detected,
    rule_ghost_order_suspected,
    rule_heartbeat_stale,
    rule_poison_pill_subscriber,
    rule_queue_depth_liveness_mismatch,
    rule_service_stalled,
    rule_suspicious_duplicate_pattern,
    rule_zombie_worker_suspected,
)


def test_each_new_rule_triggers_on_bad_state():
    assert ghost_order_detected({"runtime_state": {"reconcile": {"ghost_orders_count": 1}}}, {})

    assert exposure_mismatch(
        {"risk": {"expected_exposure": 100.0, "actual_exposure": 110.0, "exposure_tolerance": 0.5}},
        {},
    )

    assert db_contention_detected(
        {"db": {"lock_wait_ms": 500.0, "contention_events": 0, "lock_wait_threshold_ms": 200.0}},
        {},
    )

    assert event_fanout_incomplete({"event_bus": {"expected_fanout": 5, "delivered_fanout": 3}}, {})

    assert financial_drift(
        {"financials": {"ledger_balance": 200.0, "venue_balance": 150.0, "drift_threshold": 1.0}},
        {},
    )


def test_each_new_rule_does_not_trigger_on_good_state():
    assert ghost_order_detected({"runtime_state": {"reconcile": {"ghost_orders_count": 0}}}, {}) is None

    assert exposure_mismatch(
        {"risk": {"expected_exposure": 100.0, "actual_exposure": 100.2, "exposure_tolerance": 0.5}},
        {},
    ) is None

    assert db_contention_detected(
        {"db": {"lock_wait_ms": 100.0, "contention_events": 0, "lock_wait_threshold_ms": 200.0}},
        {},
    ) is None

    assert event_fanout_incomplete({"event_bus": {"expected_fanout": 5, "delivered_fanout": 5}}, {}) is None

    assert financial_drift(
        {"financials": {"ledger_balance": 200.0, "venue_balance": 199.5, "drift_threshold": 1.0}},
        {},
    ) is None


def test_multiple_anomalies_can_be_returned_together():
    context = {
        "runtime_state": {"reconcile": {"ghost_orders_count": 2}},
        "risk": {"expected_exposure": 100.0, "actual_exposure": 120.0, "exposure_tolerance": 0.5},
        "db": {"lock_wait_ms": 250.0, "contention_events": 1, "lock_wait_threshold_ms": 200.0},
        "event_bus": {"expected_fanout": 4, "delivered_fanout": 1},
        "financials": {"ledger_balance": 1000.0, "venue_balance": 900.0, "drift_threshold": 1.0},
    }

    anomalies = [
        ghost_order_detected(context, {}),
        exposure_mismatch(context, {}),
        db_contention_detected(context, {}),
        event_fanout_incomplete(context, {}),
        financial_drift(context, {}),
    ]

    assert len([a for a in anomalies if a is not None]) == 5


def test_exposure_mismatch_supports_local_remote_runtime_fields():
    finding = exposure_mismatch(
        {"risk": {"local_exposure": 100.0, "remote_exposure": 104.0, "exposure_tolerance": 1.0}},
        {},
    )
    assert finding is not None
    assert finding["code"] == "EXPOSURE_MISMATCH"


def test_db_contention_detected_uses_db_write_queue_fallback_block():
    finding = db_contention_detected(
        {
            "db": {"lock_wait_ms": 0.0, "contention_events": 0, "lock_wait_threshold_ms": 200.0},
            "db_write_queue": {"queue_depth": 75, "failed": 0, "dropped": 0},
        },
        {},
    )
    assert finding is not None
    assert finding["code"] == "DB_CONTENTION_DETECTED"


def test_default_anomaly_rules_includes_all_critical_rules():
    """All previously disabled critical rules are now in DEFAULT_ANOMALY_RULES."""
    default_fns = set(DEFAULT_ANOMALY_RULES)
    assert ghost_order_detected in default_fns
    assert rule_ghost_order_suspected in default_fns
    assert exposure_mismatch in default_fns
    assert db_contention_detected in default_fns
    assert event_fanout_incomplete in default_fns
    assert rule_poison_pill_subscriber in default_fns
    assert financial_drift_detected in default_fns
    # New stall/zombie rules also default-on
    assert rule_service_stalled in default_fns
    assert rule_heartbeat_stale in default_fns
    assert rule_zombie_worker_suspected in default_fns
    assert rule_queue_depth_liveness_mismatch in default_fns


def test_disabled_anomaly_rules_is_empty():
    """DISABLED_ANOMALY_RULES must be empty — no rule should be silently disabled."""
    assert DISABLED_ANOMALY_RULES == []


def test_financial_drift_detected_alias_matches_primary_rule():
    ctx = {"financials": {"ledger_balance": 100.0, "venue_balance": 97.0, "drift_threshold": 0.1}}
    assert financial_drift_detected(ctx, {}) == financial_drift(ctx, {})


def test_rule_service_stalled_fires_on_persistent_not_ready():
    state = {"stalled_components": ["db"]}
    ctx = {"health": {"components": {"db": {"status": "NOT_READY"}}}}
    finding = rule_service_stalled(ctx, state)
    assert finding is not None
    assert finding["code"] == "SERVICE_STALLED"
    assert finding["severity"] == "critical"


def test_rule_service_stalled_does_not_fire_on_first_tick():
    state = {}
    ctx = {"health": {"components": {"db": {"status": "NOT_READY"}}}}
    finding = rule_service_stalled(ctx, state)
    assert finding is None  # first tick — not yet persistent


def test_rule_service_stalled_fires_on_hard_liveness_stall_without_history():
    state = {}
    ctx = {
        "health": {"components": {"db": {"status": "READY"}}},
        "metrics": {
            "gauges": {
                "queue_depth": 3.0,
                "completed_delta": 0.0,
                "worker_threads_alive": 0.0,
            }
        },
    }
    finding = rule_service_stalled(ctx, state)
    assert finding is not None
    assert finding["code"] == "SERVICE_STALLED"
    assert finding["details"]["hard_liveness_stall"] is True


def test_rule_heartbeat_stale_fires_when_stale():
    ctx = {"metrics": {"gauges": {"heartbeat_age": 120.0}}}
    finding = rule_heartbeat_stale(ctx, {})
    assert finding is not None
    assert finding["code"] == "HEARTBEAT_STALE"
    assert finding["severity"] == "critical"
    assert finding["details"]["heartbeat_age"] == 120.0


def test_rule_heartbeat_stale_passes_when_fresh():
    ctx = {"metrics": {"gauges": {"last_heartbeat_age_sec": 10.0}}}
    assert rule_heartbeat_stale(ctx, {}) is None


def test_rule_zombie_worker_fires_after_threshold():
    state = {"zombie_ticks": 4}
    ctx = {"metrics": {"gauges": {
        "inflight_count": 5,
        "completed_delta": 0,
        "worker_alive": False,
        "worker_threads_alive": 0,
    }}}
    finding = rule_zombie_worker_suspected(ctx, state)
    assert finding is not None
    assert finding["code"] == "ZOMBIE_WORKER_SUSPECTED"
    assert finding["severity"] == "critical"


def test_rule_zombie_worker_does_not_fire_on_progress():
    state = {"zombie_ticks": 4}
    ctx = {"metrics": {"gauges": {
        "inflight_count": 5,
        "completed_delta": 1,
        "worker_alive": False,
        "worker_threads_alive": 0,
    }}}
    assert rule_zombie_worker_suspected(ctx, state) is None
    assert state["zombie_ticks"] == 0


def test_rule_queue_depth_liveness_mismatch_fires():
    ctx = {"metrics": {"gauges": {"queue_depth": 10.0, "worker_threads_alive": 0}}}
    finding = rule_queue_depth_liveness_mismatch(ctx, {})
    assert finding is not None
    assert finding["code"] == "QUEUE_DEPTH_LIVENESS_MISMATCH"
    assert finding["severity"] == "critical"
    assert finding["details"]["worker_threads_alive"] == 0


def test_rule_queue_depth_liveness_mismatch_passes_when_worker_alive():
    ctx = {"metrics": {"gauges": {"queue_depth": 10.0, "worker_alive": True}}}
    assert rule_queue_depth_liveness_mismatch(ctx, {}) is None


def test_rule_duplicate_block_spike_fires_from_contextual_burst_not_only_flat_threshold():
    from observability.anomaly_rules import rule_duplicate_block_spike

    state = {"duplicate_total": 10, "duplicate_delta_history": [2, 2, 2]}
    ctx = {
        "metrics": {"counters": {"duplicate_blocked_total": 13}},
        "runtime_state": {"duplicate_guard": {"blocked_submit_streak": 3}},
    }
    finding = rule_duplicate_block_spike(ctx, state)
    assert finding is not None
    assert finding["code"] == "DUPLICATE_BLOCK_SPIKE"
    assert finding["details"]["trigger"] == "contextual_burst"


def test_rule_duplicate_block_spike_does_not_fire_for_small_delta_without_context():
    from observability.anomaly_rules import rule_duplicate_block_spike

    state = {"duplicate_total": 10, "duplicate_delta_history": [1, 1, 1]}
    ctx = {"metrics": {"counters": {"duplicate_blocked_total": 12}}}
    assert rule_duplicate_block_spike(ctx, state) is None


def test_rule_suspicious_duplicate_pattern_fires_for_same_key_blocked_submit_pattern():
    state = {}
    ctx = {
        "runtime_state": {"duplicate_guard": {"blocked_submit_streak": 4}},
        "recent_orders": [
            {"status": "DUPLICATE_BLOCKED", "event_key": "E-1"},
            {"status": "DUPLICATE_BLOCKED", "event_key": "E-1"},
            {"status": "DUPLICATE_BLOCKED", "event_key": "E-1"},
            {"status": "DUPLICATE_BLOCKED", "event_key": "E-2"},
        ],
    }
    finding = rule_suspicious_duplicate_pattern(ctx, state)
    assert finding is not None
    assert finding["code"] == "SUSPICIOUS_DUPLICATE_PATTERN"
    assert "repeated_blocked_submits" in finding["details"]["evidence"]
    assert "repeated_same_key_blocks" in finding["details"]["evidence"]


def test_rule_suspicious_duplicate_pattern_does_not_fire_on_isolated_duplicate_noise():
    state = {}
    ctx = {
        "runtime_state": {"duplicate_guard": {"blocked_submit_streak": 1}},
        "recent_orders": [
            {"status": "DUPLICATE_BLOCKED", "event_key": "E-1"},
            {"status": "SUBMITTED", "event_key": "E-2"},
            {"status": "DUPLICATE_BLOCKED", "event_key": "E-3"},
        ],
    }
    assert rule_suspicious_duplicate_pattern(ctx, state) is None


def test_rule_suspicious_duplicate_pattern_uses_newest_first_ordering_for_streak():
    state = {}
    ctx = {
        "recent_orders": [
            {"status": "DUPLICATE_BLOCKED", "event_key": "E-1"},  # newest
            {"status": "DUPLICATE_BLOCKED", "event_key": "E-1"},
            {"status": "SUBMITTED", "event_key": "E-2"},
            {"status": "DUPLICATE_BLOCKED", "event_key": "E-1"},  # older
            {"status": "DUPLICATE_BLOCKED", "event_key": "E-1"},
        ],
    }
    finding = rule_suspicious_duplicate_pattern(ctx, state)
    assert finding is not None
    assert finding["details"]["blocked_submit_streak"] == 2


def test_config_builder_generates_expected_rule_config_from_audit_input():
    audit_input = {
        "ghost_orders_count": 3,
        "risk": {"expected_exposure": "120.0", "actual_exposure": "119.8", "exposure_tolerance": "0.4"},
        "db": {"lock_wait_ms": "55.5", "contention_events": "2", "lock_wait_threshold_ms": "210"},
        "event_bus": {"expected_fanout": "7", "delivered_fanout": "6"},
        "financials": {"ledger_balance": "500.0", "venue_balance": "499.3", "drift_threshold": "1.2"},
    }

    assert build_anomaly_rule_config(audit_input) == {
        "ghost_order": {"enabled": False, "ghost_orders_count": 3},
        "exposure_mismatch": {
            "enabled": False,
            "expected_exposure": 120.0,
            "actual_exposure": 119.8,
            "exposure_tolerance": 0.4,
        },
        "db_contention": {
            "enabled": False,
            "lock_wait_ms": 55.5,
            "contention_events": 2,
            "lock_wait_threshold_ms": 210.0,
        },
        "event_fanout_incomplete": {"enabled": False, "expected_fanout": 7, "delivered_fanout": 6},
        "financial_drift": {
            "enabled": False,
            "ledger_balance": 500.0,
            "venue_balance": 499.3,
            "drift_threshold": 1.2,
        },
    }


# ---------------------------------------------------------------------------
# ghost_order_suspected — distinct from ghost_order_detected
# ---------------------------------------------------------------------------

def test_rule_ghost_order_suspected_fires_on_suspected_count():
    ctx = {"runtime_state": {"reconcile": {"suspected_ghost_count": 1}}}
    finding = rule_ghost_order_suspected(ctx, {})
    assert finding is not None
    assert finding["code"] == "GHOST_ORDER_SUSPECTED"
    assert finding["severity"] == "warning"


def test_rule_ghost_order_suspected_fires_on_aged_unconfirmed_inflight():
    ctx = {"runtime_state": {"reconcile": {
        "unconfirmed_inflight_count": 2,
        "unconfirmed_inflight_age_sec": 250.0,
        "ghost_age_threshold_sec": 120.0,
    }}}
    finding = rule_ghost_order_suspected(ctx, {})
    assert finding is not None
    assert finding["code"] == "GHOST_ORDER_SUSPECTED"


def test_rule_ghost_order_suspected_fires_on_ambiguous_with_remote():
    ctx = {"recent_orders": [
        {"order_id": "o1", "status": "AMBIGUOUS", "remote_bet_id": "ext-99"},
    ]}
    finding = rule_ghost_order_suspected(ctx, {})
    assert finding is not None
    assert finding["code"] == "GHOST_ORDER_SUSPECTED"
    assert finding["details"]["ambiguous_with_remote_count"] == 1


def test_rule_ghost_order_suspected_does_not_fire_on_clean_state():
    ctx = {"runtime_state": {"reconcile": {}}, "recent_orders": []}
    assert rule_ghost_order_suspected(ctx, {}) is None


def test_ghost_suspected_and_detected_are_distinguishable():
    """suspected and detected must emit different codes and severities."""
    ctx_suspected = {"runtime_state": {"reconcile": {"suspected_ghost_count": 1}}}
    ctx_detected = {"runtime_state": {"reconcile": {"ghost_orders_count": 1}}}

    suspected = rule_ghost_order_suspected(ctx_suspected, {})
    detected = ghost_order_detected(ctx_detected, {})

    assert suspected["code"] == "GHOST_ORDER_SUSPECTED"
    assert suspected["severity"] == "warning"
    assert detected["code"] == "GHOST_ORDER_DETECTED"
    assert detected["severity"] == "critical"
    assert suspected["code"] != detected["code"]


def test_ghost_order_suspected_tracks_consecutive_ticks():
    """State tracks how many consecutive ticks the suspicion has been active."""
    ctx = {"runtime_state": {"reconcile": {"suspected_ghost_count": 1}}}
    state = {}
    rule_ghost_order_suspected(ctx, state)
    rule_ghost_order_suspected(ctx, state)
    finding = rule_ghost_order_suspected(ctx, state)
    assert finding["details"]["consecutive_suspected_ticks"] == 3


def test_ghost_order_suspected_resets_ticks_on_clear():
    ctx_active = {"runtime_state": {"reconcile": {"suspected_ghost_count": 1}}}
    ctx_clear = {"runtime_state": {"reconcile": {}}, "recent_orders": []}
    state = {}
    rule_ghost_order_suspected(ctx_active, state)
    rule_ghost_order_suspected(ctx_clear, state)
    assert state["suspected_ticks"] == 0


def test_ghost_order_suspected_suppressed_once_detected_confirmed():
    state = {"suspected_ticks": 3}
    ctx = {"runtime_state": {"reconcile": {"suspected_ghost_count": 2, "ghost_orders_count": 1}}}
    assert rule_ghost_order_suspected(ctx, state) is None
    assert state["suspected_ticks"] == 0


# ---------------------------------------------------------------------------
# poison_pill_subscriber — distinct from event_fanout_incomplete
# ---------------------------------------------------------------------------

def test_rule_poison_pill_subscriber_fires_above_threshold():
    ctx = {"event_bus": {
        "subscriber_errors": {"on_signal": 5, "on_heartbeat": 1},
        "poison_pill_threshold": 3,
    }}
    finding = rule_poison_pill_subscriber(ctx, {})
    assert finding is not None
    assert finding["code"] == "POISON_PILL_SUBSCRIBER"
    assert finding["severity"] == "error"
    assert finding["details"]["worst_subscriber"] == "on_signal"


def test_rule_poison_pill_subscriber_does_not_fire_below_threshold():
    ctx = {"event_bus": {
        "subscriber_errors": {"on_signal": 2},
        "poison_pill_threshold": 3,
    }}
    assert rule_poison_pill_subscriber(ctx, {}) is None


def test_rule_poison_pill_subscriber_uses_default_threshold_of_3():
    ctx = {"event_bus": {"subscriber_errors": {"on_fill": 3}}}
    finding = rule_poison_pill_subscriber(ctx, {})
    assert finding is not None
    assert finding["code"] == "POISON_PILL_SUBSCRIBER"


def test_poison_pill_and_fanout_incomplete_are_distinguishable():
    """poison-pill subtype must be distinct from generic fanout incomplete."""
    ctx = {
        "event_bus": {
            "subscriber_errors": {"on_fill": 5},
            "expected_fanout": 3,
            "delivered_fanout": 2,
        }
    }
    poison = rule_poison_pill_subscriber(ctx, {})
    fanout = event_fanout_incomplete(ctx, {})

    assert poison["code"] == "POISON_PILL_SUBSCRIBER"
    assert fanout["code"] == "EVENT_FANOUT_INCOMPLETE"
    assert poison["severity"] == "error"      # worse — specific broken subscriber
    assert fanout["severity"] == "warning"    # generic delivery shortfall
