from __future__ import annotations

import copy
from typing import Any, Dict, List, Optional

import pytest
from unittest.mock import MagicMock

from order_manager import (
    OrderManager,
    OrderStatus,
    VALID_TRANSITIONS,
    TERMINAL_STATES,
    validate_transition,
    InvalidTransitionError,
)


class FakeBus:
    def __init__(self) -> None:
        self.events: List[tuple[str, dict]] = []

    def publish(self, name: str, payload: dict) -> None:
        self.events.append((name, copy.deepcopy(payload)))

    def names(self) -> List[str]:
        return [name for name, _ in self.events]


class FakeDB:
    def __init__(self) -> None:
        self._store: Dict[str, Dict[str, Any]] = {}
        self._by_logical_key: Dict[str, Dict[str, Any]] = {}

    def create_order_saga(self, *, customer_ref: str, **kwargs: Any) -> None:
        record = {"customer_ref": customer_ref, **kwargs}
        self._store[customer_ref] = record
        logical_key = kwargs.get("logical_key")
        if logical_key:
            self._by_logical_key[logical_key] = record

    def get_order_saga(self, customer_ref: str) -> Optional[Dict[str, Any]]:
        return self._store.get(customer_ref)

    def get_order_saga_by_logical_key(self, logical_key: str) -> Optional[Dict[str, Any]]:
        return self._by_logical_key.get(logical_key)

    def update_order_saga(self, *, customer_ref: str, **kwargs: Any) -> None:
        if customer_ref not in self._store:
            return
        self._store[customer_ref].update(kwargs)
        logical_key = self._store[customer_ref].get("logical_key")
        if logical_key and logical_key in self._by_logical_key:
            self._by_logical_key[logical_key].update(kwargs)


@pytest.fixture
def db() -> FakeDB:
    return FakeDB()


def _payload(**overrides: Any) -> Dict[str, Any]:
    base = {
        "market_id": "1.234567890",
        "selection_id": 12345,
        "bet_type": "BACK",
        "price": 2.5,
        "stake": 10.0,
        "customer_ref": "REF-001",
        "simulation_mode": False,
    }
    base.update(overrides)
    return base


def _ok_response(size_matched: float = 10.0, bet_id: str = "BET001") -> Dict[str, Any]:
    return {
        "status": "SUCCESS",
        "instructionReports": [
            {"status": "SUCCESS", "betId": bet_id, "sizeMatched": size_matched}
        ],
    }


def _partial_response(size_matched: float = 4.0, bet_id: str = "BET002") -> Dict[str, Any]:
    return {
        "status": "SUCCESS",
        "instructionReports": [
            {"status": "SUCCESS", "betId": bet_id, "sizeMatched": size_matched}
        ],
    }


def _make_om(
    *,
    db: Optional[FakeDB] = None,
    bus: Optional[FakeBus] = None,
    client: Any = None,
) -> OrderManager:
    return OrderManager(
        db=db or FakeDB(),
        bus=bus or FakeBus(),
        client_getter=lambda: client,
        sleep_fn=lambda _: None,
    )


class TestTransitionMatrix:
    def test_all_allowed_transitions_pass(self) -> None:
        for source, targets in VALID_TRANSITIONS.items():
            for target in targets:
                validate_transition(source, target)

    def test_terminal_states_have_no_outgoing(self) -> None:
        for terminal in TERMINAL_STATES:
            assert VALID_TRANSITIONS[terminal] == frozenset()

    def test_blocked_transitions_raise(self) -> None:
        all_states = set(OrderStatus)
        for source, allowed in VALID_TRANSITIONS.items():
            blocked = all_states - allowed - {source}
            for target in blocked:
                with pytest.raises(InvalidTransitionError):
                    validate_transition(source, target)

    def test_ambiguous_is_terminal(self) -> None:
        assert OrderStatus.AMBIGUOUS in TERMINAL_STATES

    def test_pending_to_ambiguous_allowed(self) -> None:
        validate_transition(OrderStatus.PENDING, OrderStatus.AMBIGUOUS)

    @pytest.mark.parametrize(
        "target",
        [s for s in OrderStatus if s not in TERMINAL_STATES],
    )
    def test_ambiguous_cannot_transition_out(self, target: OrderStatus) -> None:
        with pytest.raises(InvalidTransitionError):
            validate_transition(OrderStatus.AMBIGUOUS, target)


class TestLifecycleAndPartial:
    def test_partial_fill_persists_remaining_size(self, db: FakeDB) -> None:
        bus = FakeBus()
        client = MagicMock()
        client.place_bet = MagicMock(return_value=_partial_response(4.0))

        om = _make_om(db=db, bus=bus, client=client)
        result = om.place_order(_payload(customer_ref="PART-1", stake=10.0))

        assert result["ok"] is True
        assert result["status"] == OrderStatus.PARTIALLY_MATCHED.value
        assert result["matched_size"] == 4.0
        assert result["remaining_size"] == 6.0

        saga = db.get_order_saga("PART-1")
        assert saga is not None
        assert saga["status"] == OrderStatus.PARTIALLY_MATCHED.value
        assert saga["matched_size"] == 4.0
        assert saga["remaining_size"] == 6.0
        assert "QUICK_BET_PARTIAL" in bus.names()

    def test_terminal_state_cannot_reopen_via_rollback(self, db: FakeDB) -> None:
        client = MagicMock()
        client.place_bet = MagicMock(return_value=_ok_response())

        om = _make_om(db=db, client=client)
        om.place_order(_payload(customer_ref="TERM-1"))

        with pytest.raises(InvalidTransitionError):
            om.mark_rollback_pending("TERM-1")

    def test_ambiguous_state_cannot_rollback(self, db: FakeDB) -> None:
        client = MagicMock()
        client.place_bet = MagicMock(side_effect=RuntimeError("PROCESSED_WITH_ERRORS"))

        om = _make_om(db=db, client=client)
        om.place_order(_payload(customer_ref="AMB-TERM-1"))

        saga = db.get_order_saga("AMB-TERM-1")
        assert saga is not None
        assert saga["status"] == OrderStatus.AMBIGUOUS.value

        with pytest.raises(InvalidTransitionError):
            om.mark_rollback_pending("AMB-TERM-1")