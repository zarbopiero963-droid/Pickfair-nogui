from observability.forensics_engine import ForensicsEngine
from observability.forensics_rules import DEFAULT_FORENSICS_RULES


def test_forensics_engine_returns_correlated_findings():
    engine = ForensicsEngine(DEFAULT_FORENSICS_RULES)

    context = {
        "health": {"overall_status": "DEGRADED"},
        "metrics": {"gauges": {}, "counters": {"quick_bet_finalized_total": 1}},
        "alerts": {"active_count": 1, "alerts": [{"code": "ALERT_A", "active": True}]},
        "incidents": {"open_count": 1, "incidents": [{"code": "INC_A", "status": "OPEN"}]},
        "runtime_state": {"forensics": {"observability_snapshot_recent": True}},
        "recent_orders": [
            {"order_id": "O1", "status": "FAILED", "remote_bet_id": "R1"},
            {"order_id": "O2", "status": "FINALIZED", "correlation_id": "C2"},
            {"order_id": "O3", "status": "FINALIZED", "correlation_id": "C3"},
        ],
        "recent_audit": [{"type": "REQUEST_RECEIVED", "correlation_id": "C2"}],
        "diagnostics_export": {"manifest_files": ["health.json"]},
    }

    baseline = engine.evaluate(context)
    baseline_codes = {f["code"] for f in baseline}
    assert "EVENT_WITHOUT_EXPECTED_SIDE_EFFECT" not in baseline_codes
    assert "FAILED_BUT_REMOTE_EXISTS" in baseline_codes
    assert "FINALIZED_WITHOUT_AUDIT_EVIDENCE" in baseline_codes

    context["metrics"]["counters"]["quick_bet_finalized_total"] = 2
    context["recent_orders"] = []
    context["recent_audit"] = []
    findings = engine.evaluate(context)
    codes = {f["code"] for f in findings}

        ],
        "recent_audit": [{"type": "REQUEST_RECEIVED", "correlation_id": "C1"}],
        "diagnostics_export": {"manifest_files": ["health.json"]},
    }

    findings = engine.evaluate(context)
    codes = {f["code"] for f in findings}

    assert "FAILED_BUT_REMOTE_EXISTS" in codes
    assert "FINALIZED_WITHOUT_AUDIT_EVIDENCE" in codes
    assert "EVENT_WITHOUT_EXPECTED_SIDE_EFFECT" in codes
    assert "INCIDENT_WITHOUT_SUPPORTING_ALERT" in codes
    assert "DIAGNOSTICS_BUNDLE_EVIDENCE_GAP" in codes
