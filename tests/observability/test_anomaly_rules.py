from observability.anomaly_rules import (
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
