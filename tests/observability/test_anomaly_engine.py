from observability.anomaly_engine import AnomalyEngine
from observability.anomaly_rules import DEFAULT_ANOMALY_RULES
from observability.anomaly_rules import ghost_order_detected



def test_anomaly_engine_evaluates_rules_and_returns_expected_codes():
    engine = AnomalyEngine(DEFAULT_ANOMALY_RULES)

    context = {
        "health": {"overall_status": "DEGRADED"},
        "metrics": {
            "counters": {
                "quick_bet_ambiguous_total": 4,
                "duplicate_blocked_total": 7,
            },
            "gauges": {
                "memory_rss_mb": 100,
                "inflight_count": 55,
            },
        },
        "alerts": {"active_count": 1},
        "runtime_state": {
            "alert_pipeline": {"alerts_enabled": True, "sender_available": False},
            "forensics": {"observability_snapshot_recent": False},
        },
    }

    anomalies_first = engine.evaluate(context)
    codes_first = {a["code"] for a in anomalies_first}
    assert {"AMBIGUOUS_SPIKE", "DUPLICATE_BLOCK_SPIKE", "ALERT_PIPELINE_DISABLED", "FORENSIC_GAP"}.issubset(
        codes_first
    )

    for rss in (180, 260, 340):
        context["metrics"]["gauges"]["memory_rss_mb"] = rss
        anomalies = engine.evaluate(context)

    codes = {a["code"] for a in anomalies}
    assert "MEMORY_GROWTH_TREND" in codes
    assert "STUCK_INFLIGHT" in codes


def test_anomaly_engine_progresses_ghost_suspected_to_detected():
    engine = AnomalyEngine(DEFAULT_ANOMALY_RULES)
    suspected_context = {"runtime_state": {"reconcile": {"suspected_ghost_count": 1, "event_key": "order-1"}}}
    detected_context = {"runtime_state": {"reconcile": {"ghost_orders_count": 2, "event_key": "order-1"}}}

    first = engine.evaluate(suspected_context)
    assert "GHOST_ORDER_SUSPECTED" in {a["code"] for a in first}

    anomalies = engine.evaluate(detected_context)
    codes = {a["code"] for a in anomalies}
    assert "GHOST_ORDER_SUSPECTED" not in codes
    assert "GHOST_ORDER_DETECTED" in codes
    detected = next(a for a in anomalies if a["code"] == "GHOST_ORDER_DETECTED")
    assert detected["details"]["progressed_from"] == "GHOST_ORDER_SUSPECTED"


def test_anomaly_engine_does_not_mark_progression_without_suspected_evidence():
    engine = AnomalyEngine([ghost_order_detected])
    anomalies = engine.evaluate({"runtime_state": {"reconcile": {"ghost_orders_count": 1}}})
    assert len(anomalies) == 1
    detected = anomalies[0]
    assert detected["code"] == "GHOST_ORDER_DETECTED"
    assert "progressed_from" not in detected.get("details", {})


def test_anomaly_engine_emits_structured_suspicious_duplicate_pattern():
    engine = AnomalyEngine(DEFAULT_ANOMALY_RULES)
    context = {
        "metrics": {"counters": {"duplicate_blocked_total": 3}},
        "runtime_state": {"duplicate_guard": {"blocked_submit_streak": 4}},
        "recent_orders": [
            {"status": "DUPLICATE_BLOCKED", "event_key": "evt-a"},
            {"status": "DUPLICATE_BLOCKED", "event_key": "evt-a"},
            {"status": "DUPLICATE_BLOCKED", "event_key": "evt-a"},
            {"status": "DUPLICATE_BLOCKED", "event_key": "evt-b"},
        ],
    }
    anomalies = engine.evaluate(context)
    suspicious = next(a for a in anomalies if a["code"] == "SUSPICIOUS_DUPLICATE_PATTERN")
    assert suspicious["severity"] == "warning"
    assert isinstance(suspicious["details"]["evidence"], list)
