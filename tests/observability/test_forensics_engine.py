from observability.forensics_engine import ForensicsEngine
from observability.forensics_rules import DEFAULT_FORENSICS_RULES


def test_forensics_engine_returns_correlated_findings():
    engine = ForensicsEngine(DEFAULT_FORENSICS_RULES)

    context = {
        "health": {"overall_status": "DEGRADED"},
        "metrics": {"gauges": {}},
        "alerts": {"active_count": 1, "alerts": [{"code": "ALERT_A", "active": True}]},
        "incidents": {"open_count": 1, "incidents": [{"code": "INC_A", "status": "OPEN"}]},
        "runtime_state": {"forensics": {"observability_snapshot_recent": True}},
        "recent_orders": [
            {"order_id": "O1", "status": "FAILED", "remote_bet_id": "R1"},
            {"order_id": "O2", "status": "FINALIZED", "correlation_id": "C2"},
        ],
        "recent_audit": [{"type": "REQUEST_RECEIVED", "correlation_id": "C1"}],
    }

    findings = engine.evaluate(context)
    codes = {f["code"] for f in findings}

    assert "FAILED_BUT_REMOTE_EXISTS" in codes
    assert "FINALIZED_WITHOUT_AUDIT_EVIDENCE" in codes
    assert "EVENT_WITHOUT_EXPECTED_SIDE_EFFECT" in codes
    assert "INCIDENT_WITHOUT_SUPPORTING_ALERT" in codes
