from __future__ import annotations

import copy
from typing import Any, Dict, List, Optional

import pytest
from unittest.mock import MagicMock

from order_manager import OrderManager, DuplicateOrderError


class FakeBus:
    def __init__(self) -> None:
        self.events: List[tuple[str, dict]] = []

    def publish(self, name: str, payload: dict) -> None:
        self.events.append((name, copy.deepcopy(payload)))


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


def _ok_response() -> Dict[str, Any]:
    return {
        "status": "SUCCESS",
        "instructionReports": [
            {"status": "SUCCESS", "betId": "BET001", "sizeMatched": 10.0}
        ],
    }


def _fail_response() -> Dict[str, Any]:
    return {
        "status": "FAILURE",
        "instructionReports": [
            {"status": "FAILURE", "betId": "", "sizeMatched": 0}
        ],
    }


def _make_om(*, db: FakeDB, client: Any) -> OrderManager:
    return OrderManager(
        db=db,
        bus=FakeBus(),
        client_getter=lambda: client,
        sleep_fn=lambda _: None,
    )


class TestCustomerRefIdempotency:
    def test_duplicate_customer_ref_rejected(self, db: FakeDB) -> None:
        client = MagicMock()
        client.place_bet = MagicMock(return_value=_ok_response())
        om = _make_om(db=db, client=client)

        om.place_order(_payload(customer_ref="DUP-REF-1"))

        with pytest.raises(DuplicateOrderError):
            om.place_order(_payload(customer_ref="DUP-REF-1"))

        assert client.place_bet.call_count == 1


class TestLogicalKeyIdempotency:
    def test_same_logical_order_different_customer_ref_blocked(self, db: FakeDB) -> None:
        client = MagicMock()
        client.place_bet = MagicMock(return_value=_ok_response())
        om = _make_om(db=db, client=client)

        om.place_order(_payload(customer_ref="LOG-A"))

        with pytest.raises(DuplicateOrderError, match="Logical order"):
            om.place_order(_payload(customer_ref="LOG-B"))

        assert client.place_bet.call_count == 1

    def test_different_price_is_different_logical_key(self, db: FakeDB) -> None:
        client = MagicMock()
        client.place_bet = MagicMock(return_value=_ok_response())
        om = _make_om(db=db, client=client)

        om.place_order(_payload(customer_ref="P-A", price=2.5))
        om.place_order(_payload(customer_ref="P-B", price=3.0))

        assert client.place_bet.call_count == 2

    def test_resubmit_after_failed_is_allowed(self, db: FakeDB) -> None:
        client = MagicMock()
        client.place_bet = MagicMock(return_value=_fail_response())
        om = _make_om(db=db, client=client)

        first = om.place_order(_payload(customer_ref="FAIL-A"))
        assert first["ok"] is False
        assert first["status"] == "FAILED"

        client.place_bet = MagicMock(return_value=_ok_response())

        second = om.place_order(_payload(customer_ref="FAIL-B"))
        assert second["ok"] is True
        assert second["status"] == "MATCHED"