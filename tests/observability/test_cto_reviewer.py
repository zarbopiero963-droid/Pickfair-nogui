from observability.cto_reviewer import CtoReviewer


def _payload(**overrides):
    base = {
        "now_ts": 1000,
        "health_snapshot": {"overall_status": "DEGRADED"},
        "metrics_snapshot": {"gauges": {"stalled_ticks": 2, "completed_delta": 0, "missing_observability_sections": 1, "repeated_high_ticks": 2}},
        "anomaly_alerts": [{"code": "A", "severity": "high"}, {"code": "B", "severity": "critical"}],
        "forensics_alerts": [],
        "incidents_snapshot": {"open_count": 1},
        "runtime_probe_state": {"component": "engine", "alert_pipeline": {"enabled": True, "deliverable": False}},
        "diagnostics_bundle": {"available": False},
    }
    base.update(overrides)
    return base


def test_history_eviction_and_escalation():
    reviewer = CtoReviewer(history_window=3, cooldown_sec=0)
    f1 = reviewer.evaluate(_payload(now_ts=1))
    f2 = reviewer.evaluate(_payload(now_ts=2))
    f3 = reviewer.evaluate(_payload(now_ts=3))
    assert f1 and f2 and f3
    assert max(x["history_size"] for x in f3) == 3
    assert any(x["evidence_count"] >= 3 for x in f3)
    reviewer.evaluate(_payload(now_ts=4))
    out = reviewer.evaluate(_payload(now_ts=5))
    assert max(x["history_size"] for x in out) == 3


def test_cooldown_suppresses_duplicate_same_context():
    reviewer = CtoReviewer(history_window=4, cooldown_sec=60)
    first = reviewer.evaluate(_payload(now_ts=10))
    second = reviewer.evaluate(_payload(now_ts=20))
    assert first
    assert second == []


def test_distinct_context_not_collapsed_and_payload_fields_present():
    reviewer = CtoReviewer(history_window=4, cooldown_sec=60)
    reviewer.evaluate(_payload(now_ts=100, runtime_probe_state={"component": "engine-a", "alert_pipeline": {"enabled": True, "deliverable": False}}))
    out = reviewer.evaluate(_payload(now_ts=101, runtime_probe_state={"component": "engine-b", "alert_pipeline": {"enabled": True, "deliverable": False}}))
    assert out
    for row in out:
        assert row["rule_name"]
        assert row["reasoning_payload"]["anomaly_alert_count"] >= 0
        assert row["suggested_action"]
