from __future__ import annotations

import threading
from typing import Any, Dict, List, Optional

import pytest
from requests.exceptions import ReadTimeout

from core.trading_engine import (
    AMBIGUITY_SUBMIT_TIMEOUT,
    STATUS_AMBIGUOUS,
    STATUS_DUPLICATE_BLOCKED,
    STATUS_FAILED,
    STATUS_INFLIGHT,
    STATUS_SUBMITTED,
    TradingEngine,
)


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
        refs: List[str] = []
        for row in self.orders.values():
            if row.get("status") in {STATUS_INFLIGHT, STATUS_SUBMITTED, STATUS_AMBIGUOUS}:
                refs.append(str(row.get("customer_ref")))
        return refs

    def load_pending_correlation_ids(self) -> List[str]:
        cids: List[str] = []
        for row in self.orders.values():
            if row.get("status") in {STATUS_INFLIGHT, STATUS_SUBMITTED, STATUS_AMBIGUOUS}:
                cids.append(str(row.get("correlation_id")))
        return cids

    def order_exists_inflight(self, *, customer_ref: Optional[str], correlation_id: Optional[str]) -> bool:
        for row in self.orders.values():
            if row.get("status") not in {STATUS_INFLIGHT, STATUS_SUBMITTED, STATUS_AMBIGUOUS}:
                continue
            if customer_ref and row.get("customer_ref") == customer_ref:
                return True
            if correlation_id and row.get("correlation_id") == correlation_id:
                return True
        return False

    def find_duplicate_order(self, *, customer_ref: Optional[str], correlation_id: Optional[str]) -> Optional[str]:
        for oid, row in self.orders.items():
            if customer_ref and row.get("customer_ref") == customer_ref:
                return oid
            if correlation_id and row.get("correlation_id") == correlation_id:
                return oid
        return None


class InlineExecutor:
    def is_ready(self) -> bool:
        return True

    def submit(self, _name: str, fn: Any) -> Any:
        return fn()


class FakeClient:
    def __init__(self, *, error: Exception | None = None) -> None:
        self.error = error

    def place_bet(self, **_payload: Any) -> Any:
        if self.error is not None:
            raise self.error
        return {"bet_id": "BET-1"}


class FakeReconcileQueue:
    def __init__(self) -> None:
        self.enqueued: List[Dict[str, Any]] = []

    def is_ready(self) -> bool:
        return True

    def enqueue(self, **kwargs: Any) -> None:
        self.enqueued.append(dict(kwargs))


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


def _make_engine(*, client: FakeClient) -> tuple[TradingEngine, FakeDB, FakeBus, FakeReconcileQueue]:
    db = FakeDB()
    bus = FakeBus()
    rec = FakeReconcileQueue()
    engine = TradingEngine(
        bus=bus,
        db=db,
        client_getter=lambda: client,
        executor=InlineExecutor(),
        reconciliation_engine=rec,
    )
    return engine, db, bus, rec


@pytest.mark.chaos
@pytest.mark.integration
def test_submit_timeout_becomes_ambiguous_not_failed() -> None:
    engine, db, bus, rec = _make_engine(client=FakeClient(error=TimeoutError("network timeout")))

    result = engine.submit_quick_bet(_payload("TIMEOUT-CHAOS-1"))
    assert result["status"] == STATUS_AMBIGUOUS
    assert result["status"] != STATUS_FAILED
    assert result["ambiguity_reason"] == AMBIGUITY_SUBMIT_TIMEOUT

    order = db.get_order(result["order_id"])
    assert order["status"] == STATUS_AMBIGUOUS
    assert "TIMEOUT-CHAOS-1" in engine._inflight_keys
    assert len(rec.enqueued) == 1

    event_names = [name for name, _ in bus.events]
    assert "QUICK_BET_FAILED" not in event_names


@pytest.mark.chaos
@pytest.mark.integration
def test_retry_after_timeout_does_not_duplicate_order() -> None:
    engine, db, bus, _rec = _make_engine(client=FakeClient(error=TimeoutError("submit timeout")))

    first = engine.submit_quick_bet(_payload("RETRY-CHAOS-1"))
    second = engine.submit_quick_bet(_payload("RETRY-CHAOS-1"))

    assert first["status"] == STATUS_AMBIGUOUS
    assert second["status"] == STATUS_DUPLICATE_BLOCKED

    accepted_or_ambiguous = [
        o for o in db.orders.values() if o.get("status") not in {STATUS_DUPLICATE_BLOCKED}
    ]
    assert len(accepted_or_ambiguous) == 1
    effective_exposure = sum(float(order.get("payload", {}).get("size", 0.0)) for order in accepted_or_ambiguous)
    assert effective_exposure == float(_payload("RETRY-CHAOS-1")["size"])

    published_names = [name for name, _payload in bus.events]
    assert "QUICK_BET_DUPLICATE" in published_names


@pytest.mark.chaos
@pytest.mark.integration
def test_concurrent_submit_race_is_deduplicated() -> None:
    for idx in range(5):
        engine, db, _bus, _rec = _make_engine(client=FakeClient(error=TimeoutError("submit timeout")))
        results: List[Dict[str, Any]] = []

        def worker() -> None:
            results.append(engine.submit_quick_bet(_payload(f"RACE-CHAOS-{idx}")))

        t1 = threading.Thread(target=worker)
        t2 = threading.Thread(target=worker)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        statuses = [r["status"] for r in results]
        assert STATUS_AMBIGUOUS in statuses
        assert STATUS_DUPLICATE_BLOCKED in statuses

        non_duplicate_orders = [o for o in db.orders.values() if o.get("status") != STATUS_DUPLICATE_BLOCKED]
        assert len(non_duplicate_orders) == 1


@pytest.mark.chaos
@pytest.mark.integration
def test_submit_readtimeout_becomes_ambiguous_not_failed() -> None:
    engine, db, _bus, _rec = _make_engine(client=FakeClient(error=ReadTimeout("read timeout")))
    result = engine.submit_quick_bet(_payload("READ-TIMEOUT-1"))
    assert result["status"] == STATUS_AMBIGUOUS
    assert result["status"] != STATUS_FAILED
    order = db.get_order(result["order_id"])
    assert order["status"] == STATUS_AMBIGUOUS
