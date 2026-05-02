from observability.forensics_engine import ForensicsEngine
from observability.forensics_rules import (
    DEFAULT_FORENSICS_RULES,
    rule_event_without_expected_side_effect,
)


def test_eventbus_subscriber_failure_evidence_preserved_in_forensics_context():
    engine = ForensicsEngine(DEFAULT_FORENSICS_RULES)
    context = {
        "event_bus": {
            "subscriber_errors": {"poisoned_subscriber": 4},
            "published_total": 8,
            "delivered_total": 4,
        },
        "recent_orders": [],
        "recent_audit": [],
        "alerts": {"active_count": 0, "alerts": []},
        "incidents": {"open_count": 0, "incidents": []},
        "health": {"overall_status": "READY"},
        "runtime_state": {"mode": "headless"},
        "metrics": {"counters": {}, "gauges": {}},
    }

    findings = engine.evaluate(context)

    assert findings == []
    # Contract proof: forensics evaluation must not mutate/drop EventBus evidence.
    assert context["event_bus"]["subscriber_errors"]["poisoned_subscriber"] == 4
    assert context["event_bus"]["published_total"] == 8
    assert context["event_bus"]["delivered_total"] == 4


def test_partial_fanout_evidence_distinguishable_via_existing_side_effect_rule():
    state = {}
    # first pass seeds rule state
    assert (
        rule_event_without_expected_side_effect(
            {
                "metrics": {"counters": {"quick_bet_finalized_total": 10}},
                "recent_orders": [],
                "recent_audit": [],
                "event_bus": {
                    "subscriber_errors": {"handler_b": 1},
                    "published_total": 10,
                    "delivered_total": 9,
                },
            },
            state,
        )
        is None
    )

    finding = rule_event_without_expected_side_effect(
        {
            "metrics": {"counters": {"quick_bet_finalized_total": 11}},
            "recent_orders": [],
            "recent_audit": [],
            "event_bus": {
                "subscriber_errors": {"handler_b": 2},
                "published_total": 11,
                "delivered_total": 10,
            },
        },
        state,
    )

    assert finding is not None
    assert finding["code"] == "EVENT_WITHOUT_EXPECTED_SIDE_EFFECT"
    assert finding["severity"] == "warning"


def test_no_silent_evidence_loss_for_eventbus_fault_payload_through_forensics_engine():
    engine = ForensicsEngine(DEFAULT_FORENSICS_RULES)
    context = {
        "event_bus": {
            "subscriber_errors": {"broken_subscriber": 3},
            "published_total": 6,
            "delivered_total": 3,
            "dispatch_failures": [{"subscriber": "broken_subscriber", "error": "RuntimeError"}],
        },
        "health": {"overall_status": "DEGRADED"},
        "alerts": {"active_count": 1, "alerts": [{"code": "POISON_PILL_SUBSCRIBER", "active": True}]},
        "incidents": {"open_count": 1, "incidents": [{"code": "POISON_PILL_SUBSCRIBER", "status": "OPEN"}]},
        "runtime_state": {"mode": "headless"},
        "recent_orders": [],
        "recent_audit": [],
        "metrics": {"counters": {}, "gauges": {}},
    }

    _ = engine.evaluate(context)

    # Preserve enough context for downstream CTO/anomaly/diagnostics consumers.
    assert context["event_bus"]["subscriber_errors"]["broken_subscriber"] == 3
    assert context["event_bus"]["published_total"] == 6
    assert context["event_bus"]["delivered_total"] == 3
    assert context["event_bus"]["dispatch_failures"][0]["subscriber"] == "broken_subscriber"
