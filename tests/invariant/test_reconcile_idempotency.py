from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

import pytest

from core.reconciliation_engine import ReconcileConfig, ReconciliationEngine, ReasonCode


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


class FakeBatchManager:
    def __init__(self) -> None:
        self.batches: Dict[str, Dict[str, Any]] = {}
        self.batch_legs: Dict[str, List[Dict[str, Any]]] = {}
        self.update_calls: List[Dict[str, Any]] = []
        self.release_calls: List[Dict[str, Any]] = []

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
        self.update_calls.append(
            {
                "batch_id": batch_id,
                "leg_index": leg_index,
                "status": status,
                "bet_id": bet_id,
                "error_text": error_text,
            }
        )
        for leg in self.batch_legs[batch_id]:
            if int(leg["leg_index"]) == int(leg_index):
                leg["status"] = status
                if bet_id is not None:
                    leg["bet_id"] = bet_id
                if raw_response is not None:
                    leg["raw_response"] = deepcopy(raw_response)
                if error_text is not None:
                    leg["error_text"] = error_text
                return
        raise KeyError(f"leg_index {leg_index} not found")

    def recompute_batch_status(self, batch_id: str) -> Dict[str, Any]:
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


def build_world() -> tuple[FakeDB, FakeBatchManager, FakeClient, ReconciliationEngine]:
    db = FakeDB()
    batch_manager = FakeBatchManager()
    batch_manager.add_batch(
        batch_id="B-IDEMP-1",
        market_id="1.100",
        status="OPEN",
        legs=[
            leg(
                leg_index=0,
                status="UNKNOWN",
                customer_ref="REF-IDEMP-1",
                created_at_ts=0.0,
            )
        ],
    )
    client = FakeClient(
        orders_by_market={
            "1.100": [
                remote_order(
                    customer_ref="REF-IDEMP-1",
                    bet_id="BET-1",
                    status="EXECUTION_COMPLETE",
                    size_matched=10.0,
                    size_remaining=0.0,
                )
            ]
        }
    )
    engine = make_engine(db=db, batch_manager=batch_manager, client=client)
    return db, batch_manager, client, engine


# ============================================================
# AREA 9 — IDEMPOTENCY
# ============================================================

def test_idempotent_skip():
    """
    Cosa uccide:
      secondo reconcile identico che rifà lavoro.

    Invariant coperto:
      fingerprint identico = skip.

    Mutation che deve fallire:
      rimuovere _reconcile_fingerprints.
    """
    db, batch_manager, client, engine = build_world()

    first = engine.reconcile_batch("B-IDEMP-1")
    assert first["ok"] is True
    assert client.calls == ["1.100"]
    assert len(batch_manager.update_calls) == 1
    assert batch_manager.batch_legs["B-IDEMP-1"][0]["status"] == "MATCHED"

    second = engine.reconcile_batch("B-IDEMP-1")
    assert second["ok"] is True
    assert second["reason_code"] == ReasonCode.IDEMPOTENT_SKIP.value

    # second run skipped before doing work again
    assert client.calls == ["1.100"]
    assert len(batch_manager.update_calls) == 1


def test_state_not_changed_on_second_run():
    """
    Cosa uccide:
      doppio reconcile che cambia lo stato senza nuovi input.

    Invariant coperto:
      secondo run non altera nulla.

    Mutation che deve fallire:
      riapplicare merge ogni volta.
    """
    db, batch_manager, client, engine = build_world()

    first = engine.reconcile_batch("B-IDEMP-1")
    assert first["ok"] is True

    state_after_first = deepcopy(batch_manager.batch_legs["B-IDEMP-1"])
    updates_after_first = deepcopy(batch_manager.update_calls)
    fingerprint_after_first = engine._reconcile_fingerprints["B-IDEMP-1"]

    second = engine.reconcile_batch("B-IDEMP-1")
    assert second["ok"] is True
    assert second["reason_code"] == ReasonCode.IDEMPOTENT_SKIP.value

    assert batch_manager.batch_legs["B-IDEMP-1"] == state_after_first
    assert batch_manager.update_calls == updates_after_first
    assert engine._reconcile_fingerprints["B-IDEMP-1"] == fingerprint_after_first


def test_decision_log_not_duplicated_on_skip():
    """
    Cosa uccide:
      skip che comunque duplica decision log.

    Invariant coperto:
      run idempotente non crea nuove decisioni inutili.

    Mutation che deve fallire:
      loggare anche nel path skip non previsto.
    """
    db, batch_manager, client, engine = build_world()

    # primo giro: genera decisioni e poi flush su DB
    first = engine.reconcile_batch("B-IDEMP-1")
    assert first["ok"] is True

    persisted_first = deepcopy(db.persisted_logs.get("B-IDEMP-1", []))
    assert len(persisted_first) >= 1

    # nessun log in-memory rimasto per quel batch dopo flush
    assert engine.get_decision_log("B-IDEMP-1") == []

    # secondo giro: skip idempotente, non deve aggiungere nuovi decision log persistiti
    second = engine.reconcile_batch("B-IDEMP-1")
    assert second["ok"] is True
    assert second["reason_code"] == ReasonCode.IDEMPOTENT_SKIP.value

    persisted_second = deepcopy(db.persisted_logs.get("B-IDEMP-1", []))
    assert persisted_second == persisted_first

    # anche in-memory non deve accumulare nuovo rumore
    assert engine.get_decision_log("B-IDEMP-1") == []