from __future__ import annotations

from typing import Any, Dict

import pytest

from observability.alerts_manager import AlertsManager
from observability.health_registry import HealthRegistry
from observability.incidents_manager import IncidentsManager
from observability.metrics_registry import MetricsRegistry
from observability.watchdog_service import WatchdogService


class _Snapshot:
    def collect_and_store(self) -> None:
        return None


@pytest.mark.integration
def test_timeout_ambiguity_reviewer_governance_open_resolve_reopen_without_orphans() -> None:
    state = {
        "runtime_state": {
            "reconcile": {"ghost_orders_count": 1},
            "alert_pipeline": {"alerts_enabled": True, "sender_available": True, "deliverable": True, "last_delivery_ok": True},
        },
        "recent_orders": [{"order_id": "T-AMB-1", "status": "AMBIGUOUS", "remote_status": "MATCHED", "remote_final_status": "SETTLED_WIN"}],
    }

    class _Probe:
        def collect_health(self) -> Dict[str, Any]:
            return {"runtime": {"status": "READY", "reason": "ok", "details": {}}}

        def collect_metrics(self) -> Dict[str, float]:
            return {"inflight_count": 1.0, "last_heartbeat_age_sec": 2.0}

        def collect_runtime_state(self) -> Dict[str, Any]:
            payload = dict(state["runtime_state"])
            payload["recent_orders"] = [dict(item) for item in state["recent_orders"]]
            return payload

        def collect_correlation_context(self) -> Dict[str, Any]:
            return {"recent_orders": [dict(item) for item in state["recent_orders"]]}

        def collect_reviewer_context(self) -> Dict[str, Any]:
            return {"recent_orders": [dict(item) for item in state["recent_orders"]]}

    alerts = AlertsManager()
    incidents = IncidentsManager()
    watchdog = WatchdogService(
        probe=_Probe(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=incidents,
        snapshot_service=_Snapshot(),
        interval_sec=60.0,
    )

    watchdog.tick()
    wave1 = [
        row for row in alerts.active_alerts()
        if row.get("source") == "reviewer_governance"
        and (row.get("details") or {}).get("incident_class") == "execution_consistency_incident"
    ]
    assert len(wave1) == 1
    gov_code = wave1[0]["code"]
    assert wave1[0]["details"]["normalized_severity"] == "critical"
    assert "LOCAL_VS_REMOTE_MISMATCH" in wave1[0]["details"]["triggering_finding_codes"]

    state["runtime_state"]["reconcile"] = {"ghost_orders_count": 0}
    state["recent_orders"] = [{"order_id": "T-AMB-1", "status": "COMPLETED", "remote_status": "COMPLETED", "remote_final_status": "SETTLED_WIN"}]
    watchdog.tick()
    assert gov_code not in {row["code"] for row in alerts.active_alerts()}

    state["runtime_state"]["reconcile"] = {"ghost_orders_count": 1}
    state["recent_orders"] = [{"order_id": "T-AMB-1", "status": "AMBIGUOUS", "remote_status": "MATCHED", "remote_final_status": "SETTLED_WIN"}]
    watchdog.tick()
    wave2 = [
        row for row in alerts.active_alerts()
        if row.get("source") == "reviewer_governance"
        and (row.get("details") or {}).get("incident_class") == "execution_consistency_incident"
    ]
    assert len(wave2) == 1
    assert wave2[0]["code"] == gov_code

    state["runtime_state"]["reconcile"] = {"ghost_orders_count": 0}
    state["recent_orders"] = [{"order_id": "T-AMB-1", "status": "COMPLETED", "remote_status": "COMPLETED", "remote_final_status": "SETTLED_WIN"}]
    watchdog.tick()

    active_codes = {row["code"] for row in alerts.active_alerts()}
    assert gov_code not in active_codes
    assert all(
        not (
            row.get("status") == "OPEN"
            and (row.get("details") or {}).get("incident_class") == "execution_consistency_incident"
        )
        for row in incidents.snapshot()["incidents"]
    )
