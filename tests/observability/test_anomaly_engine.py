from observability.anomaly_engine import AnomalyEngine
from observability.anomaly_rules import DEFAULT_ANOMALY_RULES



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
    context = {
        "runtime_state": {
            "reconcile": {
                "suspected_ghost_count": 1,
                "ghost_orders_count": 2,
                "event_key": "order-1",
            }
        }
    }
    anomalies = engine.evaluate(context)
    codes = {a["code"] for a in anomalies}
    assert "GHOST_ORDER_SUSPECTED" not in codes
    assert "GHOST_ORDER_DETECTED" in codes
    detected = next(a for a in anomalies if a["code"] == "GHOST_ORDER_DETECTED")
    assert detected["details"]["progressed_from"] == "GHOST_ORDER_SUSPECTED"
