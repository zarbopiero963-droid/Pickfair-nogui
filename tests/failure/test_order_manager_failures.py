from __future__ import annotations

import copy
from typing import Any, Dict, List, Optional

import pytest
from unittest.mock import MagicMock

from order_manager import (
    OrderManager,
    OrderStatus,
    ErrorClass,
    ReasonCode,
    RetryPolicy,
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


def _make_om(*, db: FakeDB, bus: FakeBus, client: Any, retry: Optional[RetryPolicy] = None) -> OrderManager:
    return OrderManager(
        db=db,
        bus=bus,
        client_getter=lambda: client,
        retry_policy=retry or RetryPolicy(max_attempts=1),
        sleep_fn=lambda _: None,
    )


class TestRetryAndPermanentFailure:
    def test_transient_retried_then_success(self, db: FakeDB, bus: FakeBus) -> None:
        client = MagicMock()
        client.place_bet = MagicMock(
            side_effect=[
                ConnectionError("TIMEOUT connection reset"),
                {
                    "status": "SUCCESS",
                    "instructionReports": [
                        {"status": "SUCCESS", "betId": "BET-OK", "sizeMatched": 10.0}
                    ],
                },
            ]
        )

        om = _make_om(db=db, bus=bus, client=client, retry=RetryPolicy(max_attempts=2))
        result = om.place_order(_payload(customer_ref="RET-1"))

        assert result["ok"] is True
        assert result["status"] == "MATCHED"
        assert client.place_bet.call_count == 2

    def test_transient_exhausted_fails(self, db: FakeDB, bus: FakeBus) -> None:
        client = MagicMock()
        client.place_bet = MagicMock(side_effect=ConnectionError("TIMEOUT"))

        om = _make_om(db=db, bus=bus, client=client, retry=RetryPolicy(max_attempts=3))
        result = om.place_order(_payload(customer_ref="RET-2"))

        assert result["ok"] is False
        assert result["status"] == "FAILED"
        assert result["reason_code"] == ReasonCode.RETRY_EXHAUSTED.value

    def test_permanent_error_not_retried(self, db: FakeDB, bus: FakeBus) -> None:
        client = MagicMock()
        client.place_bet = MagicMock(side_effect=RuntimeError("INSUFFICIENT_FUNDS"))

        om = _make_om(db=db, bus=bus, client=client, retry=RetryPolicy(max_attempts=3))
        result = om.place_order(_payload(customer_ref="PERM-1"))

        assert result["ok"] is False
        assert result["status"] == "FAILED"
        assert result["error_class"] == ErrorClass.PERMANENT.value
        assert client.place_bet.call_count == 1


class TestAmbiguousPaths:
    def test_ambiguous_exception_goes_to_ambiguous_state(self, db: FakeDB, bus: FakeBus) -> None:
        client = MagicMock()
        client.place_bet = MagicMock(side_effect=RuntimeError("PROCESSED_WITH_ERRORS something"))

        om = _make_om(db=db, bus=bus, client=client, retry=RetryPolicy(max_attempts=3))
        result = om.place_order(_payload(customer_ref="AMB-EXC-1"))

        assert result["ok"] is False
        assert result["status"] == OrderStatus.AMBIGUOUS.value
        assert result["error_class"] == ErrorClass.AMBIGUOUS.value
        assert result["reason_code"] == ReasonCode.AMBIGUOUS_OUTCOME.value
        assert "QUICK_BET_AMBIGUOUS" in bus.names()
        assert "QUICK_BET_FAILED" not in bus.names()

        saga = db.get_order_saga("AMB-EXC-1")
        assert saga is not None
        assert saga["status"] == OrderStatus.AMBIGUOUS.value

    def test_ambiguous_response_path_goes_to_ambiguous_state(self, db: FakeDB, bus: FakeBus) -> None:
        client = MagicMock()
        client.place_bet = MagicMock(
            return_value={
                "status": "PROCESSED_WITH_ERRORS",
                "instructionReports": [
                    {"status": "WEIRD_STATUS", "betId": "", "sizeMatched": 0}
                ],
            }
        )

        om = _make_om(db=db, bus=bus, client=client)
        result = om.place_order(_payload(customer_ref="AMB-RESP-1"))

        assert result["ok"] is False
        assert result["status"] == OrderStatus.AMBIGUOUS.value
        assert result["reason_code"] == ReasonCode.AMBIGUOUS_OUTCOME.value
        assert "QUICK_BET_AMBIGUOUS" in bus.names()

        saga = db.get_order_saga("AMB-RESP-1")
        assert saga is not None
        assert saga["status"] == OrderStatus.AMBIGUOUS.value