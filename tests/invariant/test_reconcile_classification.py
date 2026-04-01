from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

import pytest

from core.reconciliation_engine import ReconcileConfig, ReconciliationEngine


# ============================================================
# FAKES
# ============================================================

class FakeBus:
    def __init__(self) -> None:
        self.events: List[tuple[str, Dict[str, Any]]] = []

    def publish(self, name: str, payload: Dict[str, Any]) -> None:
        self.events.append((name, deepcopy(payload)))


class FakeDB:
    def __init__(self) -> None:
        self.pending_sagas: List[Dict[str, Any]] = []
        self.persisted_logs: Dict[str, List[Dict[str, Any]]] = {}

    def get_pending_sagas(self) -> List[Dict[str, Any]]:
        return deepcopy(self.pending_sagas)

    def persist_decision_log(self, batch_id: str, entries: List[Dict[str, Any]]) -> None:
        self.persisted_logs.setdefault(batch_id, [])
        self.persisted_logs[batch_id].extend(deepcopy(entries))

    def find_legs_by_customer_ref(self, customer_ref: str) -> List[Dict[str, Any]]:
        return []

    def find_legs_by_bet_id(self, bet_id: str) -> List[Dict[str, Any]]:
        return []

    def find_batches_by_market_id(self, market_id: str) -> List[Dict[str, Any]]:
        return []


class FakeBatchManager:
    def __init__(self) -> None:
        self.batches: Dict[str, Dict[str, Any]] = {}
        self.batch_legs: Dict[str, List[Dict[str, Any]]] = {}
        self.update_calls: List[Dict[str, Any]] = []

    def add_batch(
        self,
        *,
        batch_id: str,
        market_id: str,
        status: str,
        legs: List[Dict[str, Any]],
    ) -> None:
        self.batches[batch_id] = {
            "batch_id": batch_id,
            "market_id": market_id,
            "status": status,
        }
        self.batch_legs[batch_id] = deepcopy(legs)

    def get_batch(self, batch_id: str) -> Optional[Dict[str, Any]]:
        batch = self.batches.get(batch_id)
        return deepcopy(batch) if batch else None

    def get_batch_legs(self, batch_id: str) -> List[Dict[str, Any]]:
        return deepcopy(self.batch_legs.get(batch_id, []))

    def get_open_batches(self) -> List[Dict[str, Any]]:
        return [deepcopy(v) for v in self.batches.values()]

    def update_leg_status(
        self,
        *,
        batch_id: str,
        leg_index: int,
        status: str,
        bet_id: Optional[str] = None,
        raw_response: Optional[Dict[str, Any]] = None,
        error_text: Optional[str] = None,
    ) -> None:
        for leg in self.batch_legs[batch_id]:
            if int(leg["leg_index"]) == int(leg_index):
                leg["status"] = status
                if bet_id is not None:
                    leg["bet_id"] = bet_id
                if raw_response is not None:
                    leg["raw_response"] = deepcopy(raw_response)
                if error_text is not None:
                    leg["error_text"] = error_text
                self.update_calls.append(
                    {
                        "batch_id": batch_id,
                        "leg_index": leg_index,
                        "status": status,
                        "bet_id": bet_id,
                        "error_text": error_text,
                    }
                )
                return
        raise KeyError(f"leg_index {leg_index} not found")

    def recompute_batch_status(self, batch_id: str) -> Dict[str, Any]:
        legs = self.batch_legs[batch_id]
        statuses = {str(l.get("status") or "").upper() for l in legs}

        if all(s == "MATCHED" for s in statuses):
            self.batches[batch_id]["status"] = "EXECUTED"
        elif any(s == "PARTIAL" for s in statuses):
            self.batches[batch_id]["status"] = "PARTIAL"
        elif any(s == "FAILED" for s in statuses):
            self.batches[batch_id]["status"] = "FAILED"
        else:
            self.batches[batch_id]["status"] = "LIVE"

        return deepcopy(self.batches[batch_id])

    def mark_batch_failed(self, batch_id: str, reason: str = "") -> None:
        self.batches[batch_id]["status"] = "FAILED"
        self.batches[batch_id]["reason"] = reason

    def mark_batch_rollback_pending(self, batch_id: str, reason: str = "") -> None:
        self.batches[batch_id]["status"] = "ROLLBACK_PENDING"
        self.batches[batch_id]["reason"] = reason

    def update_batch_status(self, batch_id: str, status: str, notes: str = "") -> None:
        self.batches[batch_id]["status"] = status
        self.batches[batch_id]["notes"] = notes

    def release_runtime_artifacts(
        self,
        *,
        batch_id: str,
        duplication_guard=None,
        table_manager=None,
        pnl: float = 0.0,
    ) -> None:
        return None


class FakeClient:
    def __init__(self, orders_by_market: Optional[Dict[str, List[Dict[str, Any]]]] = None) -> None:
        self.orders_by_market = orders_by_market or {}

    def get_current_orders(self, market_ids: List[str]) -> List[Dict[str, Any]]:
        return deepcopy(self.orders_by_market.get(market_ids[0], []))


# ============================================================
# HELPERS
# ============================================================

def make_engine(
    *,
    db: FakeDB,
    batch_manager: FakeBatchManager,
    client: FakeClient,
) -> ReconciliationEngine:
    return ReconciliationEngine(
        db=db,
        bus=FakeBus(),
        batch_manager=batch_manager,
        client_getter=lambda: client,
        config=ReconcileConfig(
            max_convergence_cycles=3,
            convergence_sleep_secs=0.0,
            max_transient_retries=1,
            unknown_grace_secs=0.0,
        ),
    )


def leg(
    *,
    leg_index: int,
    status: str,
    customer_ref: str,
    bet_id: str = "",
    selection_id: int = 1001,
    created_at_ts: float = 0.0,
) -> Dict[str, Any]:
    return {
        "leg_index": leg_index,
        "status": status,
        "customer_ref": customer_ref,
        "bet_id": bet_id,
        "selection_id": selection_id,
        "created_at_ts": created_at_ts,
    }


def remote_order(
    *,
    customer_ref: str,
    bet_id: str,
    status: str,
    size_matched: float = 10.0,
    size_remaining: float = 0.0,
    selection_id: int = 1001,
) -> Dict[str, Any]:
    return {
        "customerOrderRef": customer_ref,
        "betId": bet_id,
        "status": status,
        "sizeMatched": size_matched,
        "sizeRemaining": size_remaining,
        "selectionId": selection_id,
    }


# ============================================================
# AREA 7 — CASE CLASSIFICATION
# ============================================================

def test_local_inflight_exchange_absent():
    """
    Invariant:
      local inflight + exchange absent => caso LOCAL_INFLIGHT_EXCHANGE_ABSENT

    Mutation che deve fallire:
      swap case o trattarlo come split_state
    """
    db = FakeDB()
    bm = FakeBatchManager()
    bm.add_batch(
        batch_id="C1",
        market_id="1.700",
        status="LIVE",
        legs=[leg(leg_index=0, status="PLACED", customer_ref="REF-C1")],
    )
    client = FakeClient(orders_by_market={"1.700": []})

    engine = make_engine(db=db, batch_manager=bm, client=client)
    result = engine.reconcile_batch("C1")

    assert result["ok"] is True
    persisted = db.persisted_logs.get("C1", [])
    assert persisted
    assert any(
        entry["case_classification"] == "LOCAL_INFLIGHT_EXCHANGE_ABSENT"
        for entry in persisted
    )


def test_local_ambiguous_exchange_matched():
    """
    Invariant:
      local ambiguous/inflight + exchange matched => exchange vince

    Mutation che deve fallire:
      far vincere local oppure classificare come ghost
    """
    db = FakeDB()
    bm = FakeBatchManager()
    bm.add_batch(
        batch_id="C2",
        market_id="1.701",
        status="LIVE",
        legs=[leg(leg_index=0, status="UNKNOWN", customer_ref="REF-C2")],
    )
    client = FakeClient(
        orders_by_market={
            "1.701": [
                remote_order(
                    customer_ref="REF-C2",
                    bet_id="BET-C2",
                    status="EXECUTION_COMPLETE",
                    size_matched=10.0,
                    size_remaining=0.0,
                )
            ]
        }
    )

    engine = make_engine(db=db, batch_manager=bm, client=client)
    result = engine.reconcile_batch("C2")

    legs = bm.get_batch_legs("C2")
    assert result["ok"] is True
    assert legs[0]["status"] == "MATCHED"

    persisted = db.persisted_logs.get("C2", [])
    assert any(
        entry["case_classification"] == "LOCAL_AMBIGUOUS_EXCHANGE_MATCHED"
        for entry in persisted
    )


def test_local_absent_exchange_present():
    """
    Invariant:
      local absent + exchange present => ghost

    Mutation che deve fallire:
      ignorare il caso o classificarlo come split_state innocuo
    """
    db = FakeDB()
    bm = FakeBatchManager()
    bm.add_batch(
        batch_id="C3",
        market_id="1.702",
        status="LIVE",
        legs=[],
    )
    client = FakeClient(
        orders_by_market={
            "1.702": [
                remote_order(
                    customer_ref="REF-GHOST",
                    bet_id="BET-GHOST",
                    status="EXECUTION_COMPLETE",
                    size_matched=10.0,
                    size_remaining=0.0,
                )
            ]
        }
    )

    engine = make_engine(db=db, batch_manager=bm, client=client)
    ghosts = engine._detect_ghost_orders("C3", [], client.get_current_orders(["1.702"]))

    assert len(ghosts) == 1
    assert ghosts[0]["customer_ref"] == "REF-GHOST"

    persisted = engine.get_decision_log("C3")
    assert any(
        entry["case_classification"] == "LOCAL_ABSENT_EXCHANGE_PRESENT"
        for entry in persisted
    )


def test_split_state():
    """
    Invariant:
      local inflight + remote present ma non matched => split_state

    Mutation che deve fallire:
      classificazione errata come inflight_exchange_absent o matched
    """
    db = FakeDB()
    bm = FakeBatchManager()
    bm.add_batch(
        batch_id="C4",
        market_id="1.703",
        status="LIVE",
        legs=[leg(leg_index=0, status="PLACED", customer_ref="REF-C4")],
    )
    client = FakeClient(
        orders_by_market={
            "1.703": [
                remote_order(
                    customer_ref="REF-C4",
                    bet_id="BET-C4",
                    status="EXECUTABLE",
                    size_matched=4.0,
                    size_remaining=6.0,
                )
            ]
        }
    )

    engine = make_engine(db=db, batch_manager=bm, client=client)
    result = engine.reconcile_batch("C4")

    legs = bm.get_batch_legs("C4")
    assert result["ok"] is True
    assert legs[0]["status"] == "PARTIAL"

    persisted = db.persisted_logs.get("C4", [])
    assert any(
        entry["case_classification"] == "SPLIT_STATE"
        for entry in persisted
    )


@pytest.mark.parametrize(
    ("local_status", "remote_present", "remote_status", "saga_pending", "expected"),
    [
        ("PLACED", False, None, False, "LOCAL_INFLIGHT_EXCHANGE_ABSENT"),
        ("UNKNOWN", True, "MATCHED", False, "LOCAL_AMBIGUOUS_EXCHANGE_MATCHED"),
        ("ABSENT", True, "MATCHED", False, "LOCAL_ABSENT_EXCHANGE_PRESENT"),
        ("PLACED", True, "PARTIAL", False, "SPLIT_STATE"),
    ],
)
def test_classify_case_matrix(local_status, remote_present, remote_status, saga_pending, expected):
    """
    Invariant:
      la classificazione pura deve ritornare il caso corretto per ogni combinazione canonica

    Mutation che deve fallire:
      swap case, if invertito, precedence errata
    """
    remote = {"status": remote_status} if remote_present else None

    got = ReconciliationEngine._classify_case(
        local_status=local_status,
        remote_order=remote,
        remote_status=remote_status,
        saga_pending=saga_pending,
    )

    assert got == expected