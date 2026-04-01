from __future__ import annotations

import copy
from typing import Any, Dict, List, Optional

import pytest
from unittest.mock import MagicMock

from order_manager import OrderManager, OrderStatus, InvalidTransitionError


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


@pytest.fixture
def bus() -> FakeBus:
    return FakeBus()


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


def _make_om(*, db: FakeDB, bus: FakeBus, client: Any) -> OrderManager:
    return OrderManager(
        db=db,
        bus=bus,
        client_getter=lambda: client,
        sleep_fn=lambda _: None,
    )


class TestCancelFlow:
    def test_cancel_happy_path(self, db: FakeDB, bus: FakeBus) -> None:
        client = MagicMock()
        client.place_bet = MagicMock(
            return_value={
                "status": "SUCCESS",
                "instructionReports": [
                    {"status": "SUCCESS", "betId": "BET002", "sizeMatched": 3.0}
                ],
            }
        )
        client.cancel_orders = MagicMock(
            return_value={
                "status": "SUCCESS",
                "instructionReports": [
                    {"status": "SUCCESS", "sizeCancelled": 7.0}
                ],
            }
        )

        om = _make_om(db=db, bus=bus, client=client)
        om.place_order(_payload(customer_ref="CAN-1", stake=10.0))
        result = om.cancel_order(customer_ref="CAN-1", bet_id="BET002", market_id="1.234567890")

        assert result["ok"] is True
        assert result["status"] == OrderStatus.CANCELLED.value
        assert db.get_order_saga("CAN-1")["status"] == OrderStatus.CANCELLED.value
        assert "QUICK_BET_CANCELLED" in bus.names()

    def test_cancel_from_terminal_raises(self, db: FakeDB, bus: FakeBus) -> None:
        client = MagicMock()
        client.place_bet = MagicMock(
            return_value={
                "status": "SUCCESS",
                "instructionReports": [
                    {"status": "SUCCESS", "betId": "BET003", "sizeMatched": 10.0}
                ],
            }
        )

        om = _make_om(db=db, bus=bus, client=client)
        om.place_order(_payload(customer_ref="CAN-TERM-1"))

        with pytest.raises(InvalidTransitionError):
            om.cancel_order(customer_ref="CAN-TERM-1", bet_id="BET003", market_id="1.234567890")


class TestReplaceFlow:
    def test_replace_updates_same_saga_row(self, db: FakeDB, bus: FakeBus) -> None:
        client = MagicMock()
        client.place_bet = MagicMock(
            return_value={
                "status": "SUCCESS",
                "instructionReports": [
                    {"status": "SUCCESS", "betId": "OLD-BET", "sizeMatched": 0}
                ],
            }
        )
        client.replace_orders = MagicMock(
            return_value={
                "status": "SUCCESS",
                "instructionReports": [
                    {"status": "SUCCESS", "betId": "NEW-BET"}
                ],
            }
        )

        om = _make_om(db=db, bus=bus, client=client)
        om.place_order(_payload(customer_ref="REP-1"))
        result = om.replace_order(
            customer_ref="REP-1",
            bet_id="OLD-BET",
            market_id="1.234567890",
            new_price=3.0,
        )

        assert result["ok"] is True
        assert result["status"] == OrderStatus.PLACED.value
        assert len(db._store) == 1
        assert db.get_order_saga("REP-1")["bet_id"] == "NEW-BET"
        assert "QUICK_BET_REPLACED" in bus.names()

    def test_residual_exposure_after_partial(self, db: FakeDB, bus: FakeBus) -> None:
        client = MagicMock()
        client.place_bet = MagicMock(
            return_value={
                "status": "SUCCESS",
                "instructionReports": [
                    {"status": "SUCCESS", "betId": "BET-RES", "sizeMatched": 6.0}
                ],
            }
        )

        om = _make_om(db=db, bus=bus, client=client)
        om.place_order(_payload(customer_ref="RES-1", stake=10.0))

        exposure = om.get_residual_exposure("RES-1")
        assert exposure["status"] == OrderStatus.PARTIALLY_MATCHED.value
        assert exposure["matched_size"] == 6.0
        assert exposure["remaining_size"] == 4.0