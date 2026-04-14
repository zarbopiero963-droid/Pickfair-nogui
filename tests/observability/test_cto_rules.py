from observability.cto_rules import evaluate_cto_rules


def _base_payload():
    return {
        "health": {"overall_status": "DEGRADED"},
        "metrics": {},
        "anomaly_alerts": [],
        "forensics_alerts": [],
        "incidents": {"open_count": 0},
        "runtime_probe": {"alert_pipeline": {"enabled": False, "deliverable": True}},
        "diagnostics": {"available": True},
    }


def test_each_cto_rule_emits_required_fields_and_actions():
    p = _base_payload()
    p["metrics"] = {
        "repeated_high_ticks": 2,
        "state_mismatch": True,
        "stalled_ticks": 2,
        "completed_delta": 0,
        "network_timeout_count": 1,
        "ambiguous_submissions": 1,
        "missing_observability_sections": 1,
        "db_lock_errors": 1,
        "writer_backlog": 10,
        "memory_growth_mb": 120,
    }
    p["runtime_probe"] = {"alert_pipeline": {"enabled": True, "deliverable": False}}
    p["anomaly_alerts"] = [{"code": "A", "severity": "high"}, {"code": "B", "severity": "critical"}]
    p["incidents"] = {"open_count": 1}
    p["diagnostics"] = {"available": False}

    findings = evaluate_cto_rules(p)
    names = {f["rule_name"] for f in findings}
    assert {
        "RISK_ESCALATION_CHAIN",
        "SILENT_FAILURE_DETECTED",
        "STATE_INCONSISTENCY_CRITICAL",
        "STALLED_SYSTEM_DETECTED",
        "DATA_DRIFT_SUSPECTED",
        "OBSERVABILITY_UNTRUSTED",
        "CASCADE_FAILURE_RISK",
        "MEMORY_GROWTH_TREND",
    } <= names
    for row in findings:
        assert row["severity"]
        assert row["short_explanation"]
        assert isinstance(row["key_metrics"], dict)
        assert row["correlation_summary"]
        assert row["suggested_action"]


def test_rules_stay_quiet_with_insufficient_evidence():
    findings = evaluate_cto_rules(_base_payload())
    assert findings == []
