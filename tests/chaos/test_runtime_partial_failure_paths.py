from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest

from core.trading_engine import STATUS_AMBIGUOUS, STATUS_COMPLETED, STATUS_SUBMITTED, TradingEngine
from observability.anomaly_engine import AnomalyEngine
from observability.anomaly_rules import DEFAULT_ANOMALY_RULES
from observability.alerts_manager import AlertsManager
from observability.forensics_engine import ForensicsEngine
from observability.forensics_rules import DEFAULT_FORENSICS_RULES
from observability.health_registry import HealthRegistry
from observability.incidents_manager import IncidentsManager
from observability.metrics_registry import MetricsRegistry
from observability.watchdog_service import WatchdogService


class FakeBus:
    def __init__(self) -> None:
        self.events: List[tuple[str, Dict[str, Any]]] = []

    def subscribe(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    def publish(self, event_name: str, payload: Optional[Dict[str, Any]] = None) -> None:
        self.events.append((event_name, payload or {}))


class FakeDB:
    def __init__(self) -> None:
        self.orders: Dict[str, Dict[str, Any]] = {}
        self.audit_events: List[Dict[str, Any]] = []
        self.next_id = 1

    def is_ready(self) -> bool:
        return True

    def insert_order(self, payload: Dict[str, Any]) -> str:
        oid = f"ORD-{self.next_id}"
        self.next_id += 1
        self.orders[oid] = dict(payload)
        return oid

    def update_order(self, order_id: str, update: Dict[str, Any]) -> None:
        self.orders.setdefault(order_id, {})
        self.orders[order_id].update(dict(update))

    def get_order(self, order_id: str) -> Dict[str, Any]:
        return dict(self.orders[order_id])

    def insert_audit_event(self, event: Dict[str, Any]) -> None:
        self.audit_events.append(dict(event))

    def load_pending_customer_refs(self) -> List[str]:
        return []

    def load_pending_correlation_ids(self) -> List[str]:
        return []

    def order_exists_inflight(self, *, customer_ref: Optional[str], correlation_id: Optional[str]) -> bool:
        return False


class InlineExecutor:
    def is_ready(self) -> bool:
        return True

    def submit(self, _name: str, fn: Any) -> Any:
        return fn()


class FakeClient:
    def __init__(self, response: Any) -> None:
        self.response = response

    def place_bet(self, **_payload: Any) -> Any:
        return self.response


def _payload(customer_ref: str) -> Dict[str, Any]:
    return {
        "market_id": "1.100",
        "selection_id": 10,
        "price": 2.0,
        "size": 5.0,
        "side": "BACK",
        "customer_ref": customer_ref,
        "event_key": "1.100:10:BACK",
    }


@pytest.mark.chaos
@pytest.mark.integration
def test_partial_failure_does_not_claim_success() -> None:
    db = FakeDB()
    bus = FakeBus()
    engine = TradingEngine(
        bus=bus,
        db=db,
        client_getter=lambda: FakeClient(response={"unexpected": "shape"}),
        executor=InlineExecutor(),
    )

    result = engine.submit_quick_bet(_payload("PARTIAL-CHAOS-1"))

    assert result["status"] == "ACCEPTED_FOR_PROCESSING"
    assert result["status"] != STATUS_COMPLETED

    order = db.get_order(result["order_id"])
    assert order["status"] == STATUS_SUBMITTED
    assert order.get("bet_id") is None
    assert order.get("remote_bet_id") is None
    assert bool(order.get("correlation_id"))

    event_names = [name for name, _payload in bus.events]
    assert "QUICK_BET_SUCCESS" not in event_names

    retry = engine.submit_quick_bet(_payload("PARTIAL-CHAOS-1"))
    assert retry["status"] == "DUPLICATE_BLOCKED"
    active_orders = [o for o in db.orders.values() if o.get("status") != "DUPLICATE_BLOCKED"]
    assert len(active_orders) == 1

    unresolved = [o for o in db.orders.values() if o.get("status") == STATUS_SUBMITTED]
    assert len(unresolved) == 1


@pytest.mark.chaos
@pytest.mark.integration
def test_partial_failure_preserves_operator_facing_evidence() -> None:
    anomaly_engine = AnomalyEngine(DEFAULT_ANOMALY_RULES)
    forensics_engine = ForensicsEngine(DEFAULT_FORENSICS_RULES)

    context = {
        "health": {"overall_status": "DEGRADED"},
        "metrics": {
            "counters": {"quick_bet_ambiguous_total": 4, "quick_bet_finalized_total": 1},
            "gauges": {"memory_rss_mb": 180, "inflight_count": 1},
        },
        "alerts": {"active_count": 1, "alerts": [{"code": "AMBIGUOUS_SPIKE", "active": True}]},
        "incidents": {"open_count": 1, "incidents": [{"code": "INC-1", "status": "OPEN"}]},
        "runtime_state": {
            "forensics": {"observability_snapshot_recent": False},
            "alert_pipeline": {"alerts_enabled": True, "sender_available": False},
        },
        "recent_orders": [{"order_id": "O-1", "status": STATUS_AMBIGUOUS}],
        "recent_audit": [{"type": "REQUEST_RECEIVED", "order_id": "O-1"}],
        "diagnostics_export": {"manifest_files": ["health.json"]},
    }

    anomalies = anomaly_engine.evaluate(context)
    findings = forensics_engine.evaluate(context)
    anomaly_codes = {a["code"] for a in anomalies}
    finding_codes = {f["code"] for f in findings}

    assert "AMBIGUOUS_SPIKE" in anomaly_codes
    assert "FORENSIC_GAP" in anomaly_codes
    assert "SUSPICIOUS_DUPLICATE_PATTERN" not in anomaly_codes
    assert "DIAGNOSTICS_BUNDLE_EVIDENCE_GAP" in finding_codes


@pytest.mark.chaos
@pytest.mark.integration
def test_timeout_ambiguity_contradiction_alert_lifecycle_through_reviewer() -> None:
    state = {
        "recent_orders": [
            {"order_id": "O-TIMEOUT-1", "status": STATUS_AMBIGUOUS, "remote_status": "MATCHED"},
        ],
        "event_bus": {"queue_depth": 2, "running": False, "worker_threads_alive": 0},
    }

    class _Probe:
        def collect_health(self) -> Dict[str, Any]:
            return {"runtime": {"status": "READY", "reason": "ok", "details": {}}}

        def collect_metrics(self) -> Dict[str, float]:
            return {"inflight_count": 1.0, "last_heartbeat_age_sec": 2.0}

        def collect_runtime_state(self) -> Dict[str, Any]:
            return {"recent_orders": list(state["recent_orders"])}

        def collect_correlation_context(self) -> Dict[str, Any]:
            return {
                "recent_orders": list(state["recent_orders"]),
                "event_bus": dict(state["event_bus"]),
            }

    class _Snapshot:
        def collect_and_store(self) -> None:
            return None

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

    watchdog._evaluate_correlations()
    first_codes = {a["code"] for a in alerts.active_alerts() if a.get("source") == "correlation_reviewer"}
    assert "LOCAL_VS_REMOTE_MISMATCH" in first_codes
    assert "QUEUE_DEPTH_DISPATCHER_CONTRADICTION" in first_codes
    first_open_incidents = {
        i["code"] for i in incidents.snapshot()["incidents"] if i.get("status") == "OPEN"
    }
    assert "LOCAL_VS_REMOTE_MISMATCH" in first_open_incidents
    assert "QUEUE_DEPTH_DISPATCHER_CONTRADICTION" in first_open_incidents

    state["recent_orders"] = [{"order_id": "O-TIMEOUT-1", "status": STATUS_COMPLETED, "remote_status": STATUS_COMPLETED}]
    state["event_bus"] = {"queue_depth": 0, "running": True, "worker_threads_alive": 1}

    watchdog._evaluate_correlations()
    second_codes = {a["code"] for a in alerts.active_alerts() if a.get("source") == "correlation_reviewer"}
    assert "LOCAL_VS_REMOTE_MISMATCH" not in second_codes
    assert "QUEUE_DEPTH_DISPATCHER_CONTRADICTION" not in second_codes
    second_open_incidents = {
        i["code"] for i in incidents.snapshot()["incidents"] if i.get("status") == "OPEN"
    }
    assert "LOCAL_VS_REMOTE_MISMATCH" not in second_open_incidents
    assert "QUEUE_DEPTH_DISPATCHER_CONTRADICTION" not in second_open_incidents


@pytest.mark.chaos
@pytest.mark.integration
def test_repeated_contention_cycles_produce_stable_grouped_governance_incident() -> None:
    state = {
        "event_bus": {"events_published": 20, "side_effects_confirmed": 10},
        "db_write_queue": {"queue_depth": 7},
        "gauges": {"db_inflight_count": 9, "inflight_count": 5},
        "diagnostics_export": {"manifest_files": []},
        "runtime_state": {
            "alert_pipeline": {"alerts_enabled": True, "sender_available": True, "deliverable": True, "last_delivery_ok": True},
            "forensics": {"observability_snapshot_recent": False},
        },
    }

    class _Probe:
        def collect_health(self):
            return {"runtime": {"status": "READY", "reason": "ok", "details": {}}}

        def collect_metrics(self):
            return {}

        def collect_runtime_state(self):
            return dict(state["runtime_state"])

        def collect_correlation_context(self):
            return {
                "event_bus": dict(state["event_bus"]),
                "db_write_queue": dict(state["db_write_queue"]),
                "metrics": {"gauges": dict(state["gauges"])},
            }

        def collect_forensics_evidence(self):
            return {"diagnostics_export": dict(state["diagnostics_export"])}

    class _Snapshot:
        def collect_and_store(self):
            return None

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

    for _ in range(3):
        watchdog.tick()
    grouped = [a for a in alerts.active_alerts() if a.get("source") == "reviewer_governance" and "observability_evidence_incident" in a["code"]]
    assert len(grouped) == 1
    assert grouped[0]["details"]["incident_class"] == "observability_evidence_incident"

    state["event_bus"] = {"events_published": 20, "side_effects_confirmed": 20}
    state["gauges"] = {"db_inflight_count": 5, "inflight_count": 5}
    state["diagnostics_export"] = {"manifest_files": ["health.json"]}
    state["runtime_state"]["forensics"] = {"observability_snapshot_recent": True}
    watchdog.tick()
    assert grouped[0]["code"] not in {a["code"] for a in alerts.active_alerts()}
