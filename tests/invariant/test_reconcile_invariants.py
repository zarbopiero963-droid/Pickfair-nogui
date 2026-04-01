from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

import pytest

from core.reconciliation_engine import ReconcileConfig, ReconciliationEngine, ReasonCode


# ============================================================
# FAKES
# ============================================================

VALID_LEG_STATUSES = {
    "CREATED",
    "SUBMITTED",
    "PLACED",
    "PARTIAL",
    "UNKNOWN",
    "MATCHED",
    "FAILED",
    "CANCELLED",
    "ROLLED_BACK",
    "LAPSED",
    "VOIDED",
}


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


class FakeBatchManager:
    def __init__(self) -> None:
        self.batches: Dict[str, Dict[str, Any]] = {}
        self.batch_legs: Dict[str, List[Dict[str, Any]]] = {}
        self.update_calls: List[Dict[str, Any]] = []
        self.release_calls: List[Dict[str, Any]] = []
        self.fail_on_invalid_status = True

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
        if self.fail_on_invalid_status and status not in VALID_LEG_STATUSES:
            raise AssertionError(f"invalid leg status produced by reconcile: {status}")

        self.update_calls.append(
            {
                "batch_id": batch_id,
                "leg_index": leg_index,
                "status": status,
                "bet_id": bet_id,
                "error_text": error_text,
            }
        )

        legs = self.batch_legs[batch_id]
        for leg in legs:
            if int(leg["leg_index"]) == int(leg_index):
                leg["status"] = status
                if bet_id is not None:
                    leg["bet_id"] = bet_id
                if raw_response is not None:
                    leg["raw_response"] = deepcopy(raw_response)
                if error_text is not None:
                    leg["error_text"] = error_text
                return

        raise KeyError(f"leg_index {leg_index} not found in batch {batch_id}")

    def recompute_batch_status(self, batch_id: str) -> Dict[str, Any]:
        legs = self.batch_legs[batch_id]
        statuses = {str(leg.get("status") or "").upper() for leg in legs}

        if statuses and statuses <= {"MATCHED"}:
            self.batches[batch_id]["status"] = "EXECUTED"
        elif statuses and statuses <= {"FAILED", "CANCELLED", "LAPSED", "VOIDED", "ROLLED_BACK"}:
            self.batches[batch_id]["status"] = "FAILED"
        elif "PARTIAL" in statuses:
            self.batches[batch_id]["status"] = "PARTIAL"
        else:
            self.batches[batch_id]["status"] = "OPEN"

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
        self.release_calls.append({"batch_id": batch_id, "pnl": pnl})


class FakeClient:
    def __init__(self, orders_by_market: Optional[Dict[str, List[Dict[str, Any]]]] = None) -> None:
        self.orders_by_market = orders_by_market or {}
        self.calls: List[str] = []

    def get_current_orders(self, market_ids: List[str]) -> List[Dict[str, Any]]:
        market_id = market_ids[0]
        self.calls.append(market_id)
        return deepcopy(self.orders_by_market.get(market_id, []))


# ============================================================
# HELPERS
# ============================================================

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
    size_matched: float,
    size_remaining: float,
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


def make_engine(
    *,
    db: Optional[FakeDB] = None,
    batch_manager: Optional[FakeBatchManager] = None,
    client: Optional[FakeClient] = None,
) -> ReconciliationEngine:
    return ReconciliationEngine(
        db=db or FakeDB(),
        bus=FakeBus(),
        batch_manager=batch_manager or FakeBatchManager(),
        client_getter=lambda: client or FakeClient(),
        config=ReconcileConfig(
            max_convergence_cycles=3,
            convergence_sleep_secs=0.0,
            max_transient_retries=1,
            transient_retry_base_delay=0.0,
            transient_retry_max_delay=0.0,
            unknown_grace_secs=0.0,
        ),
    )


# ============================================================
# AREA 10 — INVARIANTS GLOBALI
# ============================================================

def test_no_invalid_status_transition():
    """
    Cosa uccide:
      reconcile che produce stato illegale.

    Invariant coperto:
      solo stati leg validi.

    Mutation che deve fallire:
      scrivere stato non ammesso.
    """
    db = FakeDB()
    batch_manager = FakeBatchManager()
    batch_manager.add_batch(
        batch_id="B-INV-1",
        market_id="1.111",
        status="OPEN",
        legs=[
            leg(
                leg_index=0,
                status="UNKNOWN",
                customer_ref="REF-INV-1",
                created_at_ts=0.0,
            )
        ],
    )
    client = FakeClient(
        orders_by_market={
            "1.111": [
                remote_order(
                    customer_ref="REF-INV-1",
                    bet_id="BET-INV-1",
                    status="EXECUTION_COMPLETE",
                    size_matched=10.0,
                    size_remaining=0.0,
                )
            ]
        }
    )
    engine = make_engine(db=db, batch_manager=batch_manager, client=client)

    result = engine.reconcile_batch("B-INV-1")
    assert result["ok"] is True

    for update in batch_manager.update_calls:
        assert update["status"] in VALID_LEG_STATUSES

    final_legs = batch_manager.batch_legs["B-INV-1"]
    for item in final_legs:
        assert item["status"] in VALID_LEG_STATUSES


def test_all_terminal_batches_released():
    """
    Cosa uccide:
      cleanup saltato su batch terminali.

    Invariant coperto:
      release runtime artifacts sempre sui terminali.

    Mutation che deve fallire:
      saltare _release.
    """
    db = FakeDB()
    batch_manager = FakeBatchManager()
    batch_manager.add_batch(
        batch_id="B-INV-2",
        market_id="1.222",
        status="OPEN",
        legs=[
            leg(
                leg_index=0,
                status="UNKNOWN",
                customer_ref="REF-INV-2",
                created_at_ts=0.0,
            )
        ],
    )
    client = FakeClient(
        orders_by_market={
            "1.222": [
                remote_order(
                    customer_ref="REF-INV-2",
                    bet_id="BET-INV-2",
                    status="EXECUTION_COMPLETE",
                    size_matched=10.0,
                    size_remaining=0.0,
                )
            ]
        }
    )
    engine = make_engine(db=db, batch_manager=batch_manager, client=client)

    result = engine.reconcile_batch("B-INV-2")
    assert result["ok"] is True
    assert result["status"] == "EXECUTED"

    assert batch_manager.release_calls == [{"batch_id": "B-INV-2", "pnl": 0.0}]


def test_no_orphan_legs():
    """
    Cosa uccide:
      leg lasciate incoerenti o staccate dal batch.

    Invariant coperto:
      nessuna leg orfana dopo reconcile.

    Mutation che deve fallire:
      aggiornare batch senza allineare legs.
    """
    db = FakeDB()
    batch_manager = FakeBatchManager()
    original_legs = [
        leg(
            leg_index=0,
            status="UNKNOWN",
            customer_ref="REF-INV-3A",
            selection_id=111,
            created_at_ts=0.0,
        ),
        leg(
            leg_index=1,
            status="UNKNOWN",
            customer_ref="REF-INV-3B",
            selection_id=222,
            created_at_ts=0.0,
        ),
    ]
    batch_manager.add_batch(
        batch_id="B-INV-3",
        market_id="1.333",
        status="OPEN",
        legs=original_legs,
    )
    client = FakeClient(
        orders_by_market={
            "1.333": [
                remote_order(
                    customer_ref="REF-INV-3A",
                    bet_id="BET-INV-3A",
                    status="EXECUTION_COMPLETE",
                    size_matched=10.0,
                    size_remaining=0.0,
                    selection_id=111,
                ),
                remote_order(
                    customer_ref="REF-INV-3B",
                    bet_id="BET-INV-3B",
                    status="EXECUTABLE",
                    size_matched=4.0,
                    size_remaining=6.0,
                    selection_id=222,
                ),
            ]
        }
    )
    engine = make_engine(db=db, batch_manager=batch_manager, client=client)

    result = engine.reconcile_batch("B-INV-3")
    assert result["ok"] is True

    final_legs = batch_manager.batch_legs["B-INV-3"]
    assert len(final_legs) == 2

    final_indexes = {int(x["leg_index"]) for x in final_legs}
    assert final_indexes == {0, 1}

    final_refs = {str(x["customer_ref"]) for x in final_legs}
    assert final_refs == {"REF-INV-3A", "REF-INV-3B"}

    for row in final_legs:
        assert row["status"] in VALID_LEG_STATUSES
        assert row["customer_ref"]
        assert "leg_index" in row

    assert "B-INV-3" in batch_manager.batches
    assert batch_manager.batches["B-INV-3"]["market_id"] == "1.333"


def test_decision_log_complete():
    """
    Cosa uccide:
      reconcile conclude senza set minimo di decision entries.

    Invariant coperto:
      ogni batch riconciliato ha log sufficiente a spiegare l’esito.

    Mutation che deve fallire:
      mancare entry terminale o entry di merge.
    """
    db = FakeDB()
    batch_manager = FakeBatchManager()
    batch_manager.add_batch(
        batch_id="B-INV-4",
        market_id="1.444",
        status="OPEN",
        legs=[
            leg(
                leg_index=0,
                status="UNKNOWN",
                customer_ref="REF-INV-4",
                created_at_ts=0.0,
            )
        ],
    )
    client = FakeClient(
        orders_by_market={
            "1.444": [
                remote_order(
                    customer_ref="REF-INV-4",
                    bet_id="BET-INV-4",
                    status="EXECUTION_COMPLETE",
                    size_matched=10.0,
                    size_remaining=0.0,
                )
            ]
        }
    )
    engine = make_engine(db=db, batch_manager=batch_manager, client=client)

    result = engine.reconcile_batch("B-INV-4")
    assert result["ok"] is True

    persisted = db.persisted_logs.get("B-INV-4", [])
    assert persisted, "decision log must be persisted for reconciled batch"

    # Deve esserci almeno una entry che spiega il merge.
    merge_entries = [
        e for e in persisted
        if e.get("reason_code") in {
            ReasonCode.EXCHANGE_WINS_MATCHED.value,
            ReasonCode.EXCHANGE_WINS_PARTIAL.value,
            ReasonCode.EXCHANGE_WINS_CANCELLED.value,
            ReasonCode.EXCHANGE_WINS_LAPSED.value,
            ReasonCode.LOCAL_WINS_SAGA_PENDING.value,
            ReasonCode.RESOLVED_UNKNOWN_TO_FAILED.value,
        }
    ]
    assert merge_entries, "missing merge decision entry"

    # Ogni entry deve essere strutturalmente spiegabile.
    required_keys = {
        "timestamp",
        "batch_id",
        "leg_index",
        "case_classification",
        "reason_code",
        "local_status",
        "exchange_status",
        "resolved_status",
        "merge_winner",
        "details",
    }
    for entry in persisted:
        assert required_keys <= set(entry.keys())

    # L'esito finale deve essere coerente con almeno una decisione persistita.
    final_leg_status = batch_manager.batch_legs["B-INV-4"][0]["status"]
    assert any(e["resolved_status"] == final_leg_status for e in persisted)