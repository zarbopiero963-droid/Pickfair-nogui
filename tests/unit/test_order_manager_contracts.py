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
    def test_success_contract_shape(self) -> None:
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

        assert set(result.keys()) == {
            "ok",
            "status",
            "customer_ref",
            "bet_id",
            "matched_size",
            "remaining_size",
            "reason_code",
            "response",
        }
        assert result["ok"] is True
        assert result["status"] == OrderStatus.MATCHED.value

    def test_failure_contract_shape(self) -> None:
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

    def test_ambiguous_contract_shape(self) -> None:
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


class TestValidation:
    def test_missing_market_id_raises(self) -> None:
        om = _make_om(client=MagicMock())

        with pytest.raises(ValidationError, match="market_id"):
            om.place_order({"stake": 10, "price": 2.0, "selection_id": 1})

    def test_invalid_price_raises(self) -> None:
        om = _make_om(client=MagicMock())

        with pytest.raises(ValidationError, match="price"):
            om.place_order({
                "market_id": "1.2",
                "selection_id": 1,
                "price": 1.0,
                "stake": 10,
            })


class TestErrorClassification:
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
    def test_reason_code_mapping(self, code: str, expected: ErrorClass) -> None:
        assert classify_error(code) == expected

    def test_connection_error_is_transient(self) -> None:
        assert classify_error("", ConnectionError("x")) == ErrorClass.TRANSIENT

    def test_unknown_string_is_ambiguous(self) -> None:
        assert classify_error("NEVER_SEEN") == ErrorClass.AMBIGUOUS