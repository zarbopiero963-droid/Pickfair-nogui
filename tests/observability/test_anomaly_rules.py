from observability.anomaly_rules import (
    DEFAULT_ANOMALY_RULES,
    DISABLED_ANOMALY_RULES,
    db_contention_detected,
    event_fanout_incomplete,
    exposure_mismatch,
    financial_drift,
    ghost_order_detected,
    rule_alert_pipeline_disabled,
    rule_ambiguous_spike,
    rule_duplicate_block_spike,
    rule_forensic_gap,
    rule_memory_growth_trend,
    rule_stuck_inflight,
)


def test_ambiguous_and_duplicate_spike_rules_fire_on_counter_delta():
    state_a = {}
    state_d = {}
    ctx1 = {"metrics": {"counters": {"quick_bet_ambiguous_total": 1, "duplicate_blocked_total": 2}}}
    assert rule_ambiguous_spike(ctx1, state_a) is None
    assert rule_duplicate_block_spike(ctx1, state_d) is None

    ctx2 = {"metrics": {"counters": {"quick_bet_ambiguous_total": 5, "duplicate_blocked_total": 9}}}
    amb = rule_ambiguous_spike(ctx2, state_a)
    dup = rule_duplicate_block_spike(ctx2, state_d)
    assert amb and amb["code"] == "AMBIGUOUS_SPIKE"
    assert dup and dup["code"] == "DUPLICATE_BLOCK_SPIKE"


def test_memory_growth_trend_and_stuck_inflight_rules():
    mem_state = {}
    in_state = {}

    for rss in (100, 180, 260, 340):
        item = rule_memory_growth_trend({"metrics": {"gauges": {"memory_rss_mb": rss}}}, mem_state)
    assert item and item["code"] == "MEMORY_GROWTH_TREND"

    assert rule_stuck_inflight({"metrics": {"gauges": {"inflight_count": 55}}}, in_state) is None
    assert rule_stuck_inflight({"metrics": {"gauges": {"inflight_count": 55}}}, in_state) is None
    stuck = rule_stuck_inflight({"metrics": {"gauges": {"inflight_count": 55}}}, in_state)
    assert stuck and stuck["code"] == "STUCK_INFLIGHT"


def test_alert_pipeline_disabled_and_forensic_gap_rules():
    pipeline = rule_alert_pipeline_disabled(
        {"runtime_state": {"alert_pipeline": {"alerts_enabled": True, "sender_available": False}}},
        {},
    )
    assert pipeline and pipeline["code"] == "ALERT_PIPELINE_DISABLED"

    forensic = rule_forensic_gap(
        {
            "health": {"overall_status": "DEGRADED"},
            "alerts": {"active_count": 1},
            "runtime_state": {"forensics": {"observability_snapshot_recent": False}},
        },
        {},
    )
    assert forensic and forensic["code"] == "FORENSIC_GAP"


def test_new_rules_are_disabled_by_default_but_available_for_opt_in():
    assert ghost_order_detected not in DEFAULT_ANOMALY_RULES
    assert exposure_mismatch not in DEFAULT_ANOMALY_RULES
    assert db_contention_detected not in DEFAULT_ANOMALY_RULES
    assert event_fanout_incomplete not in DEFAULT_ANOMALY_RULES
    assert financial_drift not in DEFAULT_ANOMALY_RULES
    assert DISABLED_ANOMALY_RULES == [
        ghost_order_detected,
        exposure_mismatch,
        db_contention_detected,
        event_fanout_incomplete,
        financial_drift,
    ]


def test_ghost_order_detected_rule():
    assert ghost_order_detected({}, {}) is None
    anomaly = ghost_order_detected({"runtime_state": {"reconcile": {"ghost_orders_count": 2}}}, {})
    assert anomaly and anomaly["code"] == "GHOST_ORDER_DETECTED"


def test_exposure_mismatch_rule():
    assert (
        exposure_mismatch(
            {"risk": {"expected_exposure": 100.0, "actual_exposure": 100.005, "exposure_tolerance": 0.01}},
            {},
        )
        is None
    )
    anomaly = exposure_mismatch(
        {"risk": {"expected_exposure": 100.0, "actual_exposure": 100.5, "exposure_tolerance": 0.01}},
        {},
    )
    assert anomaly and anomaly["code"] == "EXPOSURE_MISMATCH"


def test_db_contention_detected_rule():
    assert db_contention_detected({"db": {"lock_wait_ms": 50.0, "contention_events": 0}}, {}) is None
    anomaly_wait = db_contention_detected(
        {"db": {"lock_wait_ms": 250.0, "contention_events": 0, "lock_wait_threshold_ms": 200.0}},
        {},
    )
    assert anomaly_wait and anomaly_wait["code"] == "DB_CONTENTION_DETECTED"
    anomaly_events = db_contention_detected({"db": {"lock_wait_ms": 10.0, "contention_events": 1}}, {})
    assert anomaly_events and anomaly_events["code"] == "DB_CONTENTION_DETECTED"


def test_event_fanout_incomplete_rule():
    assert event_fanout_incomplete({"event_bus": {"expected_fanout": 3, "delivered_fanout": 3}}, {}) is None
    anomaly = event_fanout_incomplete({"event_bus": {"expected_fanout": 4, "delivered_fanout": 2}}, {})
    assert anomaly and anomaly["code"] == "EVENT_FANOUT_INCOMPLETE"


def test_financial_drift_rule():
    assert financial_drift({"financials": {"ledger_balance": 100.0, "venue_balance": 100.005}}, {}) is None
    anomaly = financial_drift(
        {"financials": {"ledger_balance": 100.0, "venue_balance": 99.0, "drift_threshold": 0.5}},
        {},
    )
    assert anomaly and anomaly["code"] == "FINANCIAL_DRIFT"
