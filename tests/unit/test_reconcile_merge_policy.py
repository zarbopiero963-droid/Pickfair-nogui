from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

import pytest

from core.reconciliation_engine import ReasonCode, ReconcileConfig, ReconciliationEngine


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
        return [deepcopy(batch) for batch in self.batches.values() if batch.get("status") not in {"MATCHED", "CANCELLED", "LAPSED", "FAILED"}]

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
    db: Optional[FakeDB] = None,
    batch_manager: Optional[FakeBatchManager] = None,
    client: Optional[FakeClient] = None,
    unknown_grace_secs: float = 0.0,
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
            unknown_grace_secs=unknown_grace_secs,
        ),
    )


def leg(
    *,
    leg_index: int = 0,
    status: str,
    customer_ref: str = "REF-1",
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
    customer_ref: str = "REF-1",
    bet_id: str = "BET-1",
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
# AREA 8 — MERGE POLICY
# ============================================================

@pytest.mark.parametrize(
    ("exchange_order", "expected_status", "expected_reason"),
    [
        (remote_order(status="EXECUTION_COMPLETE", size_matched=10.0, size_remaining=0.0), "MATCHED", ReasonCode.EXCHANGE_WINS_MATCHED),
        (remote_order(status="CANCELLED"), "CANCELLED", ReasonCode.EXCHANGE_WINS_CANCELLED),
        (remote_order(status="LAPSED"), "LAPSED", ReasonCode.EXCHANGE_WINS_LAPSED),
    ],
)
def test_exchange_wins_terminal(exchange_order, expected_status, expected_reason):
    """
    Cosa uccide:
      locale che sovrascrive terminale exchange.

    Invariant coperto:
      exchange vince su terminali definitivi.

    Mutation che deve fallire:
      far vincere local su MATCHED/CANCELLED/LAPSED.
    """
    engine = make_engine()
    local_leg = leg(status="PLACED", customer_ref="REF-X")

    new_status, reason, winner = engine._apply_merge_policy(
        batch_id="B1",
        leg=local_leg,
        remote_order=exchange_order,
        saga_pending=False,
    )

    assert new_status == expected_status
    assert reason == expected_reason
    assert winner == "EXCHANGE"


def test_local_wins_saga_pending():
    """
    Cosa uccide:
      exchange che schiaccia saga ancora pending.

    Invariant coperto:
      local pending può trattenere update finché policy lo consente.

    Mutation che deve fallire:
      exchange wins sempre.
    """
    db = FakeDB()
    db.pending_sagas = [{"customer_ref": "REF-PENDING"}]
    engine = make_engine(db=db)

    local_leg = leg(
        status="PLACED",
        customer_ref="REF-PENDING",
        created_at_ts=9999999999.0,  # giovane, non da risolvere a FAILED
    )

    new_status, reason, winner = engine._apply_merge_policy(
        batch_id="B2",
        leg=local_leg,
        remote_order=None,
        saga_pending=True,
    )

    assert new_status is None
    assert reason == ReasonCode.LOCAL_WINS_SAGA_PENDING
    assert winner == "LOCAL"


def test_unknown_resolves_to_failed():
    """
    Cosa uccide:
      UNKNOWN eterno.

    Invariant coperto:
      dopo grace period si risolve deterministicamente.

    Mutation che deve fallire:
      lasciare UNKNOWN.
    """
    engine = make_engine(unknown_grace_secs=0.0)

    local_leg = leg(
        status="UNKNOWN",
        customer_ref="REF-U",
        created_at_ts=1.0,
    )

    new_status, reason, winner = engine._apply_merge_policy(
        batch_id="B3",
        leg=local_leg,
        remote_order=None,
        saga_pending=False,
    )

    assert new_status == "FAILED"
    assert reason == ReasonCode.RESOLVED_UNKNOWN_TO_FAILED
    assert winner == "NONE"


def test_partial_updates_correctly():
    """
    Cosa uccide:
      partial convertito a matched senza base.

    Invariant coperto:
      partial resta partial se i dati exchange sono partial.

    Mutation che deve fallire:
      promuovere sempre a matched.
    """
    engine = make_engine()

    local_leg = leg(status="PLACED", customer_ref="REF-PART")
    remote = remote_order(
        customer_ref="REF-PART",
        status="EXECUTABLE",
        size_matched=4.0,
        size_remaining=6.0,
    )

    new_status, reason, winner = engine._apply_merge_policy(
        batch_id="B4",
        leg=local_leg,
        remote_order=remote,
        saga_pending=False,
    )

    assert new_status == "PARTIAL"
    assert reason == ReasonCode.EXCHANGE_WINS_PARTIAL
    assert winner == "EXCHANGE"


def test_terminal_local_no_change():
    """
    Cosa uccide:
      batch già terminale mutato di nuovo.

    Invariant coperto:
      locale terminale coerente non deve essere toccato.

    Mutation che deve fallire:
      riscrivere stato terminale inutilmente.
    """
    engine = make_engine()

    local_leg = leg(status="MATCHED", customer_ref="REF-T")
    remote = remote_order(
        customer_ref="REF-T",
        status="EXECUTION_COMPLETE",
        size_matched=10.0,
        size_remaining=0.0,
    )

    new_status, reason, winner = engine._apply_merge_policy(
        batch_id="B5",
        leg=local_leg,
        remote_order=remote,
        saga_pending=False,
    )

    assert new_status is None
    assert reason == ReasonCode.IDEMPOTENT_SKIP
    assert winner == "NONE"
