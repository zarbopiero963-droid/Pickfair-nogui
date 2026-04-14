from observability.cto_reviewer import CtoReviewer


def test_chaos_repeated_stall_escalates_and_cooldown_applies():
    reviewer = CtoReviewer(history_window=5, cooldown_sec=0)
    payload = {
        "now_ts": 1,
        "health_snapshot": {"overall_status": "DEGRADED"},
        "metrics_snapshot": {"gauges": {"stalled_ticks": 3, "completed_delta": 0, "writer_backlog": 50, "memory_growth_mb": 110}},
        "anomaly_alerts": [{"code": "STALL", "severity": "high"}, {"code": "LAG", "severity": "high"}],
        "forensics_alerts": [],
        "incidents_snapshot": {"open_count": 1},
        "runtime_probe_state": {"component": "chaos", "alert_pipeline": {"enabled": True, "deliverable": False}},
        "diagnostics_bundle": {"available": False},
    }
    first = reviewer.evaluate(payload)
    second = reviewer.evaluate({**payload, "now_ts": 2})
    third = reviewer.evaluate({**payload, "now_ts": 3})

    assert any(x["rule_name"] == "STALLED_SYSTEM_DETECTED" for x in first)
    escalated = [x for x in third if x["rule_name"] == "STALLED_SYSTEM_DETECTED"]
    assert escalated and escalated[0]["severity"] in {"high", "critical"}
    assert any(x["rule_name"] == "MEMORY_GROWTH_TREND" for x in second)
    assert any(x["rule_name"] in {"OBSERVABILITY_UNTRUSTED", "CASCADE_FAILURE_RISK"} for x in first)
    assert any(x["rule_name"] == "SILENT_FAILURE_DETECTED" for x in first)
