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
    ValidationError,
    classify_error,
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


def _make_om(*, client: Any) -> OrderManager:
    return OrderManager(
        db=FakeDB(),
        bus=FakeBus(),
        client_getter=lambda: client,
        sleep_fn=lambda _: None,
    )


class TestContractShape:
    SUCCESS_RESULT_KEYS = {
        "ok",
        "status",
        "customer_ref",
        "bet_id",
        "matched_size",
        "remaining_size",
        "reason_code",
        "response",
    }

    @staticmethod
    def test_success_contract_shape() -> None:
        """Successful placement returns the stable success contract keys."""
        client = MagicMock()
        client.place_bet = MagicMock(
            return_value={
                "status": "SUCCESS",
                "instructionReports": [
                    {"status": "SUCCESS", "betId": "BET001", "sizeMatched": 10.0}
                ],
            }
        )
        om = _make_om(client=client)

        result = om.place_order(_payload(customer_ref="CONTRACT-SUCCESS"))

        if set(result.keys()) != TestContractShape.SUCCESS_RESULT_KEYS:
            pytest.fail("success contract keys mismatch")
        assert result["ok"] is True
        assert result["status"] == OrderStatus.MATCHED.value

    @staticmethod
    def test_failure_contract_shape() -> None:
        """Permanent broker errors return the stable failure contract keys."""
        client = MagicMock()
        client.place_bet = MagicMock(side_effect=RuntimeError("INSUFFICIENT_FUNDS"))
        om = _make_om(client=client)

        result = om.place_order(_payload(customer_ref="CONTRACT-FAIL"))

        assert set(result.keys()) == {
            "ok",
            "status",
            "customer_ref",
            "error",
            "error_class",
            "reason_code",
        }
        assert result["ok"] is False
        assert result["status"] == OrderStatus.FAILED.value
        assert result["error_class"] == ErrorClass.PERMANENT.value

    @staticmethod
    def test_ambiguous_contract_shape() -> None:
        """Ambiguous transport outcomes map to the ambiguous contract shape."""
        client = MagicMock()
        client.place_bet = MagicMock(side_effect=RuntimeError("PROCESSED_WITH_ERRORS"))
        om = _make_om(client=client)

        result = om.place_order(_payload(customer_ref="CONTRACT-AMB"))

        assert set(result.keys()) == {
            "ok",
            "status",
            "customer_ref",
            "error",
            "error_class",
            "reason_code",
        }
        assert result["ok"] is False
        assert result["status"] == OrderStatus.AMBIGUOUS.value
        assert result["reason_code"] == ReasonCode.AMBIGUOUS_OUTCOME.value

    @staticmethod
    def test_no_bet_id_fails_closed() -> None:
        """A success response without betId is treated as failed."""
        response = {
            "status": "SUCCESS",
            "instructionReports": [{"status": "SUCCESS", "betId": "", "sizeMatched": 0.0}],
        }
        client = MagicMock(place_bet=MagicMock(return_value=response))
        om = _make_om(client=client)

        result = om.place_order(_payload(customer_ref="CONTRACT-NO-BETID"))

        assert result["ok"] is False
        assert result["status"] == OrderStatus.FAILED.value
        if result["remaining_size"] is not None:
            pytest.fail("remaining_size must be None for fail-closed outcome")
        if result["reason_code"] != ReasonCode.BROKER_REJECTED.value:
            pytest.fail("reason_code must be BROKER_REJECTED for fail-closed outcome")
        last_event = om.bus.events[-1][1]
        if last_event["remaining_size"] is not None:
            pytest.fail("event remaining_size must be None for fail-closed outcome")


class TestValidation:
    @staticmethod
    def test_missing_market_id_raises() -> None:
        """Missing required market_id is rejected early."""
        om = _make_om(client=MagicMock())

        with pytest.raises(ValidationError, match="market_id"):
            om.place_order({"stake": 10, "price": 2.0, "selection_id": 1})

    @staticmethod
    def test_invalid_price_raises() -> None:
        """Price below exchange minimum fails validation."""
        om = _make_om(client=MagicMock())

        with pytest.raises(ValidationError, match="price"):
            om.place_order({
                "market_id": "1.2",
                "selection_id": 1,
                "price": 1.0,
                "stake": 10,
            })


class TestErrorClassification:
    @staticmethod
    @pytest.mark.parametrize(
        ("code", "expected"),
        [
            ("TIMEOUT", ErrorClass.TRANSIENT),
            ("SERVICE_UNAVAILABLE", ErrorClass.TRANSIENT),
            ("INSUFFICIENT_FUNDS", ErrorClass.PERMANENT),
            ("MARKET_NOT_OPEN_FOR_BETTING", ErrorClass.PERMANENT),
            ("UNKNOWN", ErrorClass.AMBIGUOUS),
            ("PROCESSED_WITH_ERRORS", ErrorClass.AMBIGUOUS),
        ],
    )
    def test_reason_code_mapping(code: str, expected: ErrorClass) -> None:
        """Maps broker/transport error codes to stable error classes."""
        assert classify_error(code) == expected

    @staticmethod
    def test_conn_error_transient() -> None:
        """Classify ConnectionError as transient."""
        assert classify_error("", ConnectionError("x")) == ErrorClass.TRANSIENT

    @staticmethod
    def test_unknown_code_ambiguous() -> None:
        """Unknown broker codes are treated as ambiguous."""
        assert classify_error("NEVER_SEEN") == ErrorClass.AMBIGUOUS
