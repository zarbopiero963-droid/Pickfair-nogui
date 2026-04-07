from __future__ import annotations

import pytest

from observability.anomaly_engine import AnomalyEngine
from observability.anomaly_rules import DEFAULT_ANOMALY_RULES
from observability.forensics_engine import ForensicsEngine
from observability.forensics_rules import DEFAULT_FORENSICS_RULES


@pytest.mark.chaos
@pytest.mark.integration
def test_diagnostics_forensics_context_survives_restart_reconstruction() -> None:
    anomaly_engine = AnomalyEngine(DEFAULT_ANOMALY_RULES)
    forensics_engine = ForensicsEngine(DEFAULT_FORENSICS_RULES)

    persisted_context = {
        "health": {"overall_status": "DEGRADED"},
        "metrics": {
            "counters": {"quick_bet_ambiguous_total": 5, "quick_bet_finalized_total": 1},
            "gauges": {"inflight_count": 1},
        },
        "alerts": {"active_count": 1, "alerts": [{"code": "AMBIGUOUS_SPIKE", "active": True}]},
        "incidents": {"open_count": 1, "incidents": [{"code": "INC-CS-1", "status": "OPEN"}]},
        "runtime_state": {"forensics": {"observability_snapshot_recent": False}},
        "recent_orders": [{"order_id": "ORD-CS-1", "status": "AMBIGUOUS"}],
        "recent_audit": [{"type": "REQUEST_RECEIVED", "order_id": "ORD-CS-1"}],
        "diagnostics_export": {"manifest_files": ["health.json", "metrics.json", "alerts.json"]},
    }

    restarted_context = dict(persisted_context)
    restarted_context["runtime_state"] = {"forensics": {"observability_snapshot_recent": False}}

    anomalies = anomaly_engine.evaluate(restarted_context)
    findings = forensics_engine.evaluate(restarted_context)

    assert restarted_context["health"]
    assert restarted_context["metrics"]
    assert restarted_context["alerts"]
    assert restarted_context["incidents"]
    assert restarted_context["runtime_state"]
    assert restarted_context["recent_orders"]
    assert restarted_context["recent_audit"]

    anomaly_codes = {a["code"] for a in anomalies}
    finding_codes = {f["code"] for f in findings}
    assert "AMBIGUOUS_SPIKE" in anomaly_codes
    assert "FORENSIC_GAP" in anomaly_codes
    assert "DIAGNOSTICS_BUNDLE_EVIDENCE_GAP" in finding_codes
