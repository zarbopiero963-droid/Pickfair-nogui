from observability.forensics_rules import (
    rule_alert_without_runtime_context,
    rule_diagnostics_bundle_evidence_gap,
    rule_event_without_expected_side_effect,
    rule_failed_but_remote_exists,
    rule_finalized_without_audit_evidence,
    rule_incident_without_supporting_alert,
    rule_snapshot_without_runtime_evidence,
)


def test_failed_but_remote_exists_and_finalized_without_audit_evidence_rules():
    failed = rule_failed_but_remote_exists(
        {"recent_orders": [{"order_id": "O1", "status": "FAILED", "remote_bet_id": "R1"}]},
        {},
    )
    assert failed and failed["code"] == "FAILED_BUT_REMOTE_EXISTS"

    finalized = rule_finalized_without_audit_evidence(
        {
            "recent_orders": [{"order_id": "O2", "status": "FINALIZED", "correlation_id": "C2"}],
            "recent_audit": [{"type": "REQUEST_RECEIVED", "correlation_id": "C1"}],
        },
        {},
    )
    assert finalized and finalized["code"] == "FINALIZED_WITHOUT_AUDIT_EVIDENCE"


def test_event_without_side_effect_snapshot_gap_and_bundle_gap_rules():
    event_gap = rule_event_without_expected_side_effect(
        {
            "recent_orders": [{"order_id": "O1", "correlation_id": "C1"}],
            "recent_audit": [{"type": "FINALIZED", "correlation_id": "C9"}],
        },
        {},
    )
    assert event_gap and event_gap["code"] == "EVENT_WITHOUT_EXPECTED_SIDE_EFFECT"

    metric_gap = rule_event_without_expected_side_effect(
        {
            "metrics": {"counters": {"quick_bet_finalized_total": 2}},
            "recent_orders": [],
            "recent_audit": [],
        },
        {},
    )
    assert metric_gap and metric_gap["code"] == "EVENT_WITHOUT_EXPECTED_SIDE_EFFECT"

    snap_gap = rule_snapshot_without_runtime_evidence(
        {
            "runtime_state": {"forensics": {"observability_snapshot_recent": True}},
            "metrics": {"gauges": {}},
        },
        {},
    )
    assert snap_gap and snap_gap["code"] == "SNAPSHOT_WITHOUT_RUNTIME_EVIDENCE"

    bundle_gap = rule_diagnostics_bundle_evidence_gap(
        {
            "health": {"overall_status": "DEGRADED"},
            "alerts": {"active_count": 1},
            "incidents": {"open_count": 1},
            "recent_orders": [],
            "recent_audit": [],
        },
        {},
    )
    assert bundle_gap and bundle_gap["code"] == "DIAGNOSTICS_BUNDLE_EVIDENCE_GAP"

    manifest_gap = rule_diagnostics_bundle_evidence_gap(
        {
            "health": {"overall_status": "DEGRADED"},
            "alerts": {"active_count": 0},
            "incidents": {"open_count": 0},
            "recent_orders": [{"id": "O1"}],
            "recent_audit": [{"id": "A1"}],
            "diagnostics_export": {"manifest_files": ["health.json", "metrics.json"]},
        },
        {},
    )
    assert manifest_gap and manifest_gap["code"] == "DIAGNOSTICS_BUNDLE_EVIDENCE_GAP"


def test_finalize_requires_success_not_terminal():
    state = {}
    baseline = {"metrics": {"counters": {"quick_bet_finalized_total": 1}}, "recent_orders": [], "recent_audit": []}
    rule_event_without_expected_side_effect(baseline, state)
    no_success = rule_event_without_expected_side_effect(
        {
            "metrics": {"counters": {"quick_bet_finalized_total": 2}},
            "recent_orders": [{"status": "FAILED"}],
            "recent_audit": [{"type": "ORDER_FAILED"}],
        },
        state,
    )
    assert no_success is None


def test_finalize_counter_delta_required():
    state = {}
    first = rule_event_without_expected_side_effect(
        {
            "metrics": {"counters": {"quick_bet_finalized_total": 2}},
            "recent_orders": [{"status": "FINALIZED"}],
            "recent_audit": [],
        },
        state,
    )
    second = rule_event_without_expected_side_effect(
        {
            "metrics": {"counters": {"quick_bet_finalized_total": 2}},
            "recent_orders": [{"status": "FINALIZED"}],
            "recent_audit": [],
        },
        state,
    )
    assert first is None
    assert second is None


def test_failed_terminals_do_not_trigger_rule():
    state = {}
    rule_event_without_expected_side_effect(
        {"metrics": {"counters": {"quick_bet_finalized_total": 1}}, "recent_orders": [], "recent_audit": []},
        state,
    )
    item = rule_event_without_expected_side_effect(
        {
            "metrics": {"counters": {"quick_bet_finalized_total": 2}},
            "recent_orders": [{"status": "FAILED"}],
            "recent_audit": [{"type": "FAILED"}],
        },
        state,
    )
    assert item is None


def test_success_finalize_triggers_check():
    state = {}
    rule_event_without_expected_side_effect(
        {"metrics": {"counters": {"quick_bet_finalized_total": 1}}, "recent_orders": [], "recent_audit": []},
        state,
    )
    triggered = rule_event_without_expected_side_effect(
        {
            "metrics": {"counters": {"quick_bet_finalized_total": 2}},
            "recent_orders": [{"status": "FINALIZED"}],
            "recent_audit": [],
        },
        state,
    )
    assert triggered and triggered["code"] == "EVENT_WITHOUT_EXPECTED_SIDE_EFFECT"


def test_success_finalize_without_side_effect_detected():
    state = {}
    rule_event_without_expected_side_effect(
        {"metrics": {"counters": {"quick_bet_finalized_total": 3}}, "recent_orders": [], "recent_audit": []},
        state,
    )
    item = rule_event_without_expected_side_effect(
        {
            "metrics": {"counters": {"quick_bet_finalized_total": 4}},
            "recent_orders": [],
            "recent_audit": [{"type": "FINALIZED"}],
        },
        state,
    )
    assert item and item["code"] == "EVENT_WITHOUT_EXPECTED_SIDE_EFFECT"


def test_counter_flat_no_retrigger():
    state = {}
    rule_event_without_expected_side_effect(
        {"metrics": {"counters": {"quick_bet_finalized_total": 2}}, "recent_orders": [], "recent_audit": []},
        state,
    )
    flat = rule_event_without_expected_side_effect(
        {
            "metrics": {"counters": {"quick_bet_finalized_total": 2}},
            "recent_orders": [{"status": "FINALIZED"}],
            "recent_audit": [],
        },
        state,
    )
    assert flat is None


def test_mixed_terminals_only_success_counts():
    state = {}
    rule_event_without_expected_side_effect(
        {"metrics": {"counters": {"quick_bet_finalized_total": 5}}, "recent_orders": [], "recent_audit": []},
        state,
    )
    item = rule_event_without_expected_side_effect(
        {
            "metrics": {"counters": {"quick_bet_finalized_total": 6}},
            "recent_orders": [{"status": "FAILED"}, {"status": "AMBIGUOUS"}, {"status": "COMPLETED"}],
            "recent_audit": [{"type": "ORDER_FAILED"}],
        },
        state,
    )
    assert item and item["code"] == "EVENT_WITHOUT_EXPECTED_SIDE_EFFECT"


def test_incident_without_alert_and_alert_without_runtime_context_rules():
    incident_gap = rule_incident_without_supporting_alert(
        {
            "alerts": {"alerts": [{"code": "A1", "active": True}]},
            "incidents": {"incidents": [{"code": "I1", "status": "OPEN"}]},
        },
        {},
    )
    assert incident_gap and incident_gap["code"] == "INCIDENT_WITHOUT_SUPPORTING_ALERT"

    context_gap = rule_alert_without_runtime_context(
        {
            "alerts": {"alerts": [{"code": "X1", "active": True}]},
            "runtime_state": {},
        },
        {},
    )
    assert context_gap and context_gap["code"] == "ALERT_WITHOUT_RUNTIME_CONTEXT"
