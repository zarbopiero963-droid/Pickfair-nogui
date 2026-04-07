from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest

from core.trading_engine import STATUS_AMBIGUOUS, STATUS_COMPLETED, STATUS_SUBMITTED, TradingEngine
from observability.anomaly_engine import AnomalyEngine
from observability.anomaly_rules import DEFAULT_ANOMALY_RULES
from observability.forensics_engine import ForensicsEngine
from observability.forensics_rules import DEFAULT_FORENSICS_RULES


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

    event_names = [name for name, _payload in bus.events]
    assert "QUICK_BET_SUCCESS" not in event_names


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
    assert "DIAGNOSTICS_BUNDLE_EVIDENCE_GAP" in finding_codes
