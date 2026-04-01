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
    """
    Minimal DB fake with persisted decision logs + pending sagas.
    """

    def __init__(self) -> None:
        self.pending_sagas: List[Dict[str, Any]] = []
        self.persisted_logs: Dict[str, List[Dict[str, Any]]] = {}
        self.legs_by_customer_ref: Dict[str, List[Dict[str, Any]]] = {}
        self.legs_by_bet_id: Dict[str, List[Dict[str, Any]]] = {}
        self.batches_by_market_id: Dict[str, List[Dict[str, Any]]] = {}

    def get_pending_sagas(self) -> List[Dict[str, Any]]:
        return deepcopy(self.pending_sagas)

    def persist_decision_log(self, batch_id: str, entries: List[Dict[str, Any]]) -> None:
        self.persisted_logs.setdefault(batch_id, [])
        self.persisted_logs[batch_id].extend(deepcopy(entries))

    def find_legs_by_customer_ref(self, customer_ref: str) -> List[Dict[str, Any]]:
        return deepcopy(self.legs_by_customer_ref.get(customer_ref, []))

    def find_legs_by_bet_id(self, bet_id: str) -> List[Dict[str, Any]]:
        return deepcopy(self.legs_by_bet_id.get(bet_id, []))

    def find_batches_by_market_id(self, market_id: str) -> List[Dict[str, Any]]:
        return deepcopy(self.batches_by_market_id.get(market_id, []))


class FakeBatchManager:
    """
    Persisted in-memory batch manager shared across engine restarts.
    """

    def __init__(self) -> None:
        self.batches: Dict[str, Dict[str, Any]] = {}
        self.batch_legs: Dict[str, List[Dict[str, Any]]] = {}
        self.release_calls: List[Dict[str, Any]] = []
        self.rollback_pending_calls: List[str] = []
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
        out = []
        for batch in self.batches.values():
            if str(batch.get("status") or "").upper() not in {
                "EXECUTED", "ROLLED_BACK", "FAILED", "CANCELLED"
            }:
                out.append(deepcopy(batch))
        return out

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
        elif any(s == "FAILED" for s in statuses) and all(
            s in {"FAILED", "MATCHED", "CANCELLED", "LAPSED", "VOIDED"} for s in statuses
        ):
            self.batches[batch_id]["status"] = "FAILED"
        elif any(s == "PARTIAL" for s in statuses):
            self.batches[batch_id]["status"] = "PARTIAL"
        elif any(s == "UNKNOWN" for s in statuses):
            self.batches[batch_id]["status"] = "LIVE"
        else:
            self.batches[batch_id]["status"] = "LIVE"

        return deepcopy(self.batches[batch_id])

    def mark_batch_failed(self, batch_id: str, reason: str = "") -> None:
        self.batches[batch_id]["status"] = "FAILED"
        self.batches[batch_id]["reason"] = reason

    def mark_batch_rollback_pending(self, batch_id: str, reason: str = "") -> None:
        self.rollback_pending_calls.append(batch_id)
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

    def get_current_orders(self, market_ids: List[str]) -> List[Dict[str, Any]]:
        market_id = market_ids[0]
        return deepcopy(self.orders_by_market.get(market_id, []))


class CrashOnceBatchManager(FakeBatchManager):
    def __init__(self, crash_batch_id: str, crash_leg_index: int) -> None:
        super().__init__()
        self.crash_batch_id = crash_batch_id
        self.crash_leg_index = crash_leg_index
        self.has_crashed = False

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
        if (
            batch_id == self.crash_batch_id
            and int(leg_index) == int(self.crash_leg_index)
            and not self.has_crashed
        ):
            self.has_crashed = True
            raise RuntimeError("CRASH_MID_RECONCILE")
        super().update_leg_status(
            batch_id=batch_id,
            leg_index=leg_index,
            status=status,
            bet_id=bet_id,
            raw_response=raw_response,
            error_text=error_text,
        )


# ============================================================
# HELPERS
# ============================================================

def make_engine(
    *,
    db: FakeDB,
    batch_manager: FakeBatchManager,
    client: FakeClient,
    bus: Optional[FakeBus] = None,
    config: Optional[ReconcileConfig] = None,
) -> ReconciliationEngine:
    return ReconciliationEngine(
        db=db,
        bus=bus or FakeBus(),
        batch_manager=batch_manager,
        client_getter=lambda: client,
        config=config or ReconcileConfig(
            max_convergence_cycles=4,
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
    created_at_ts: float = 0.0,
    selection_id: int = 1001,
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
# AREA 6 — RECOVERY CONSISTENCY
# ============================================================

def test_restart_same_result():
    """
    Cosa uccide:
      inconsistenza tra prima esecuzione e restart con stesso stato persistito

    Invariant coperto:
      restart idempotente: stesso DB + stesso stato exchange => stesso risultato finale

    Mutation che deve fallire:
      stato non persistito / fingerprint o merge dipendente dalla memoria del processo
    """
    db = FakeDB()
    bm = FakeBatchManager()
    bm.add_batch(
        batch_id="B1",
        market_id="1.100",
        status="LIVE",
        legs=[leg(leg_index=0, status="UNKNOWN", customer_ref="REF-1")],
    )
    client = FakeClient(
        orders_by_market={
            "1.100": [
                remote_order(
                    customer_ref="REF-1",
                    bet_id="BET-1",
                    status="EXECUTION_COMPLETE",
                    size_matched=10.0,
                    size_remaining=0.0,
                )
            ]
        }
    )

    engine1 = make_engine(db=db, batch_manager=bm, client=client)
    result1 = engine1.reconcile_batch("B1")

    engine2 = make_engine(db=db, batch_manager=bm, client=client)
    result2 = engine2.reconcile_batch("B1")

    assert result1["ok"] is True
    assert result2["ok"] is True
    assert result1["status"] == "EXECUTED"
    assert result2["status"] == "EXECUTED"
    assert bm.get_batch("B1")["status"] == "EXECUTED"


def test_double_recovery_no_side_effect():
    """
    Cosa uccide:
      doppio reconcile che modifica di nuovo batch e legs già riconciliati

    Invariant coperto:
      seconda esecuzione sullo stesso stato non produce side effects ulteriori

    Mutation che deve fallire:
      duplicate update o release duplicato
    """
    db = FakeDB()
    bm = FakeBatchManager()
    bm.add_batch(
        batch_id="B2",
        market_id="1.200",
        status="LIVE",
        legs=[leg(leg_index=0, status="UNKNOWN", customer_ref="REF-2")],
    )
    client = FakeClient(
        orders_by_market={
            "1.200": [
                remote_order(
                    customer_ref="REF-2",
                    bet_id="BET-2",
                    status="EXECUTION_COMPLETE",
                    size_matched=10.0,
                    size_remaining=0.0,
                )
            ]
        }
    )

    engine = make_engine(db=db, batch_manager=bm, client=client)

    result1 = engine.reconcile_batch("B2")
    updates_after_first = len(bm.update_calls)
    releases_after_first = len(bm.release_calls)

    result2 = engine.reconcile_batch("B2")
    updates_after_second = len(bm.update_calls)
    releases_after_second = len(bm.release_calls)

    assert result1["status"] == "EXECUTED"
    assert result2["status"] == "EXECUTED"
    assert updates_after_second == updates_after_first
    assert releases_after_second == releases_after_first


def test_partial_state_recovery():
    """
    Cosa uccide:
      recovery che perde o schiaccia PARTIAL in MATCHED/FAILED

    Invariant coperto:
      stato partial gestito correttamente e preservato

    Mutation che deve fallire:
      skip partial oppure convertire partial in matched automaticamente
    """
    db = FakeDB()
    bm = FakeBatchManager()
    bm.add_batch(
        batch_id="B3",
        market_id="1.300",
        status="LIVE",
        legs=[leg(leg_index=0, status="UNKNOWN", customer_ref="REF-3")],
    )
    client = FakeClient(
        orders_by_market={
            "1.300": [
                remote_order(
                    customer_ref="REF-3",
                    bet_id="BET-3",
                    status="EXECUTABLE",
                    size_matched=4.0,
                    size_remaining=6.0,
                )
            ]
        }
    )

    engine = make_engine(db=db, batch_manager=bm, client=client)
    result = engine.reconcile_batch("B3")

    legs = bm.get_batch_legs("B3")
    assert result["ok"] is True
    assert bm.get_batch("B3")["status"] == "PARTIAL"
    assert legs[0]["status"] == "PARTIAL"
    assert legs[0]["bet_id"] == "BET-3"


def test_crash_mid_reconcile():
    """
    Cosa uccide:
      stato incoerente dopo crash a metà update

    Invariant coperto:
      dopo un crash a metà, una nuova esecuzione porta comunque a stato stabile corretto

    Mutation che deve fallire:
      stato intermedio perso / engine che non riesce a ripartire dopo eccezione
    """
    db = FakeDB()
    bm = CrashOnceBatchManager(crash_batch_id="B4", crash_leg_index=0)
    bm.add_batch(
        batch_id="B4",
        market_id="1.400",
        status="LIVE",
        legs=[leg(leg_index=0, status="UNKNOWN", customer_ref="REF-4")],
    )
    client = FakeClient(
        orders_by_market={
            "1.400": [
                remote_order(
                    customer_ref="REF-4",
                    bet_id="BET-4",
                    status="EXECUTION_COMPLETE",
                    size_matched=10.0,
                    size_remaining=0.0,
                )
            ]
        }
    )

    engine = make_engine(db=db, batch_manager=bm, client=client)

    with pytest.raises(RuntimeError, match="CRASH_MID_RECONCILE"):
        engine.reconcile_batch("B4")

    # Second run must recover and converge correctly.
    result = engine.reconcile_batch("B4")

    assert result["ok"] is True
    assert result["status"] == "EXECUTED"
    assert bm.get_batch("B4")["status"] == "EXECUTED"
    assert bm.get_batch_legs("B4")[0]["status"] == "MATCHED"


def test_reconcile_after_recovery():
    """
    Cosa uccide:
      pipeline invertita o incoerente tra stato pending saga e reconcile reale

    Invariant coperto:
      dopo recovery di stato pending/inflight, reconcile porta il batch nello stato vero exchange

    Mutation che deve fallire:
      invertire ordine recovery -> reconcile oppure ignorare pending sagas
    """
    db = FakeDB()
    db.pending_sagas = [{"customer_ref": "REF-5"}]

    bm = FakeBatchManager()
    bm.add_batch(
        batch_id="B5",
        market_id="1.500",
        status="LIVE",
        legs=[leg(leg_index=0, status="PLACED", customer_ref="REF-5")],
    )
    client = FakeClient(
        orders_by_market={
            "1.500": [
                remote_order(
                    customer_ref="REF-5",
                    bet_id="BET-5",
                    status="CANCELLED",
                    size_matched=0.0,
                    size_remaining=0.0,
                )
            ]
        }
    )

    engine = make_engine(db=db, batch_manager=bm, client=client)
    result = engine.reconcile_batch("B5")

    legs = bm.get_batch_legs("B5")
    assert result["ok"] is True
    assert legs[0]["status"] == "CANCELLED"
    assert bm.get_batch("B5")["status"] in {"LIVE", "FAILED", "EXECUTED", "PARTIAL", "CANCELLED"}
    assert any(
        entry["reason_code"] == ReasonCode.EXCHANGE_WINS_CANCELLED.value
        for entry in db.persisted_logs.get("B5", [])
    )