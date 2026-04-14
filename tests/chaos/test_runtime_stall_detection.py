from __future__ import annotations

from observability.anomaly_engine import AnomalyEngine
from observability.anomaly_rules import DEFAULT_ANOMALY_RULES
from observability.cto_reviewer import CtoReviewer


def test_stalled_system_with_heartbeat_gap_emits_operator_signal():
    anomalies = AnomalyEngine(DEFAULT_ANOMALY_RULES).evaluate(
        {
            "metrics": {"gauges": {"queue_depth": 6, "worker_alive": 0, "completed_delta": 0, "heartbeat_age": 130}},
            "runtime_state": {},
        }
    )
    codes = {a["code"] for a in anomalies}
    assert {"SERVICE_STALLED", "HEARTBEAT_STALE"} & codes


def test_cto_reviewer_escalates_repeated_stall_evidence():
    reviewer = CtoReviewer(history_window=4, cooldown_sec=0)
    payload = {
        "metrics_snapshot": {"gauges": {"stalled_ticks": 4, "completed_delta": 0, "repeated_high_ticks": 3}},
        "anomaly_alerts": [{"code": "SERVICE_STALLED", "severity": "high"}, {"code": "HEARTBEAT_STALE", "severity": "high"}],
        "forensics_alerts": [],
        "incidents_snapshot": {"open_count": 1},
        "runtime_probe_state": {"alert_pipeline": {"enabled": True, "deliverable": False}},
        "diagnostics_bundle": {"available": False},
    }
    reviewer.evaluate({**payload, "now_ts": 1})
    reviewer.evaluate({**payload, "now_ts": 2})
    findings = reviewer.evaluate({**payload, "now_ts": 3})
    stalled = [f for f in findings if f["rule_name"] == "STALLED_SYSTEM_DETECTED"]
    assert stalled and stalled[0]["severity"] in {"high", "critical"}
