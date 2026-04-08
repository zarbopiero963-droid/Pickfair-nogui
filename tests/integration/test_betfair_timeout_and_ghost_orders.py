from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest

from core.trading_engine import (
    AMBIGUITY_SUBMIT_TIMEOUT,
    STATUS_AMBIGUOUS,
    STATUS_COMPLETED,
    STATUS_DUPLICATE_BLOCKED,
    STATUS_FAILED,
    STATUS_INFLIGHT,
    STATUS_SUBMITTED,
    TradingEngine,
)
from tests.helpers.fake_exchange import FakeExchange


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
    def __init__(self, *, error: Exception | None = None, response: Any = None, exchange: FakeExchange | None = None) -> None:
        self.error = error
        self.response = {"bet_id": "BET-1"} if response is None else response
        self.exchange = exchange
        self.calls: List[Dict[str, Any]] = []

    def place_bet(self, **payload: Any) -> Any:
        self.calls.append(dict(payload))
        if self.error is not None:
            raise self.error
        if self.exchange is not None:
            row = self.exchange.place_order(payload)
            return {"bet_id": row["bet_id"]}
        return self.response




class GhostReconciler:
    """Test-only reconciler to emulate remote evidence becoming available over passes."""

    def __init__(self, db: FakeDB, remote_by_ref: Dict[str, Dict[str, Any]]) -> None:
        self.db = db
        self.remote_by_ref = remote_by_ref

    def resolve_once(self, *, customer_ref: str) -> bool:
        remote = self.remote_by_ref.get(customer_ref)
        if not remote:
            return False

        for oid, row in self.db.orders.items():
            if row.get("customer_ref") != customer_ref:
                continue
            if row.get("status") == STATUS_DUPLICATE_BLOCKED:
                continue
            self.db.update_order(
                oid,
                {
                    "status": STATUS_COMPLETED,
                    "outcome": "SUCCESS",
                    "remote_bet_id": remote.get("bet_id"),
                    "finalized": True,
                },
            )
            return True
        return False


class FlakyRemoteFetcher:
    def __init__(self, outcomes: List[Any]) -> None:
        self.outcomes = list(outcomes)
        self.calls = 0

    def fetch(self) -> Any:
        self.calls += 1
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class ReconcilePassRunner:
    def __init__(self, db: FakeDB, fetcher: FlakyRemoteFetcher) -> None:
        self.db = db
        self.fetcher = fetcher

    def run_once(self, *, customer_ref: str) -> bool:
        try:
            remote = self.fetcher.fetch()
        except Exception:
            return False

        if not remote:
            return False

        for oid, row in self.db.orders.items():
            if row.get("customer_ref") == customer_ref and row.get("status") == STATUS_AMBIGUOUS:
                self.db.update_order(
                    oid,
                    {
                        "status": STATUS_COMPLETED,
                        "outcome": "SUCCESS",
                        "remote_bet_id": remote.get("bet_id"),
                        "finalized": True,
                    },
                )
                return True
        return False


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


def _make_engine(*, exchange: FakeExchange | None = None, client: FakeClient | None = None) -> tuple[TradingEngine, FakeDB, FakeBus, FakeReconcileQueue]:
    db = FakeDB()
    bus = FakeBus()
    rec = FakeReconcileQueue()
    selected_client = client or FakeClient(exchange=exchange)
    engine = TradingEngine(
        bus=bus,
        db=db,
        client_getter=lambda: selected_client,
        executor=InlineExecutor(),
        reconciliation_engine=rec,
    )
    return engine, db, bus, rec


@pytest.mark.integration
def test_submit_timeout_becomes_ambiguous_and_remote_order_exists() -> None:
    exchange = FakeExchange(duplicate_mode="single_exposure")
    exchange.force_timeout_on_next_submit()
    engine, db, _bus, rec = _make_engine(exchange=exchange, client=FakeClient(exchange=exchange))

    result = engine.submit_quick_bet(_payload("TIMEOUT-1"))

    assert result["status"] == STATUS_AMBIGUOUS
    assert result["ambiguity_reason"] == AMBIGUITY_SUBMIT_TIMEOUT
    assert result["status"] != STATUS_FAILED

    order = db.get_order(result["order_id"])
    assert order["status"] == STATUS_AMBIGUOUS
    assert "TIMEOUT-1" in engine._inflight_keys
    assert len(rec.enqueued) == 1

    remote = exchange.get_current_orders(customer_ref="TIMEOUT-1")
    assert len(remote) == 1
    assert remote[0]["status"] in {"EXECUTABLE", "PARTIALLY_MATCHED", "MATCHED"}


@pytest.mark.integration
def test_timeout_retry_has_no_double_exposure_and_reconcile_finds_ghost() -> None:
    exchange = FakeExchange(duplicate_mode="single_exposure")
    exchange.force_timeout_on_next_submit()
    engine, db, bus, _rec = _make_engine(exchange=exchange, client=FakeClient(exchange=exchange))

    first = engine.submit_quick_bet(_payload("GHOST-1"))
    second = engine.submit_quick_bet(_payload("GHOST-1"))

    assert first["status"] == STATUS_AMBIGUOUS
    assert second["status"] == STATUS_DUPLICATE_BLOCKED

    remote = exchange.get_current_orders(customer_ref="GHOST-1")
    assert len(remote) == 1

    for oid, row in db.orders.items():
        if row.get("customer_ref") == "GHOST-1" and row.get("status") != STATUS_DUPLICATE_BLOCKED:
            db.update_order(
                oid,
                {
                    "status": STATUS_COMPLETED,
                    "outcome": "SUCCESS",
                    "remote_bet_id": remote[0]["bet_id"],
                    "finalized": True,
                },
            )

    non_duplicate_orders = [o for o in db.orders.values() if o.get("status") != STATUS_DUPLICATE_BLOCKED]
    assert len(non_duplicate_orders) == 1
    assert non_duplicate_orders[0]["status"] == STATUS_COMPLETED
    assert non_duplicate_orders[0]["remote_bet_id"] == remote[0]["bet_id"]
    assert non_duplicate_orders[0]["status"] != STATUS_FAILED

    ambiguous_events = [payload for name, payload in bus.events if name == "QUICK_BET_AMBIGUOUS"]
    assert len(ambiguous_events) == 1


@pytest.mark.integration
def test_partial_fill_simulation_cancel_replace_and_reconcile_convergence() -> None:
    exchange = FakeExchange(duplicate_mode="return_existing")
    exchange.seed_liquidity(market_id="1.100", selection_id=10, side="LAY", size=2.0)
    engine, db, _bus, _rec = _make_engine(exchange=exchange, client=FakeClient(exchange=exchange))

    result = engine.submit_quick_bet(_payload("PARTIAL-1"))
    assert result["status"] in {STATUS_SUBMITTED, "ACCEPTED_FOR_PROCESSING"}

    remote = exchange.get_current_orders(customer_ref="PARTIAL-1")
    assert len(remote) == 1
    order_id = remote[0]["order_id"]
    assert remote[0]["status"] == "PARTIALLY_MATCHED"

    exchange.replace_order(order_id, new_price=2.2)
    replaced = exchange.get_current_orders(customer_ref="PARTIAL-1")[0]
    assert replaced["price"] == 2.2

    exchange.cancel_order(order_id)
    cancelled = exchange.get_current_orders(customer_ref="PARTIAL-1")[0]
    assert cancelled["status"] == "CANCELLED"

    exchange.advance_fill(order_id, new_status="MATCHED")
    matched = exchange.get_current_orders(customer_ref="PARTIAL-1")[0]
    assert matched["status"] == "MATCHED"
    assert matched["matched_size"] == pytest.approx(5.0)

    persisted = [row for row in db.orders.values() if row.get("customer_ref") == "PARTIAL-1"]
    assert len(persisted) == 1
    assert persisted[0]["status"] != STATUS_FAILED
