from __future__ import annotations

from typing import Any, Dict, Optional

from unittest.mock import MagicMock

from order_manager import OrderManager, OrderStatus


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


def _make_om(*, db: FakeDB, client: Any) -> OrderManager:
    return OrderManager(
        db=db,
        bus=None,
        client_getter=lambda: client,
        sleep_fn=lambda _: None,
    )


class TestRestartPreservesLifecycle:
    def test_new_instance_sees_existing_partial_state(self) -> None:
        db = FakeDB()
        client = MagicMock()
        client.place_bet = MagicMock(
            return_value={
                "status": "SUCCESS",
                "instructionReports": [
                    {"status": "SUCCESS", "betId": "BET-RST", "sizeMatched": 3.0}
                ],
            }
        )

        om1 = _make_om(db=db, client=client)
        om1.place_order(_payload(customer_ref="RST-1", stake=10.0))

        om2 = _make_om(db=db, client=client)
        exposure = om2.get_residual_exposure("RST-1")

        assert exposure["status"] == OrderStatus.PARTIALLY_MATCHED.value
        assert exposure["matched_size"] == 3.0
        assert exposure["remaining_size"] == 7.0

    def test_unknown_customer_ref_returns_none_exposure(self) -> None:
        db = FakeDB()
        om = _make_om(db=db, client=MagicMock())

        exposure = om.get_residual_exposure("UNKNOWN-REF")

        assert exposure["customer_ref"] == "UNKNOWN-REF"
        assert exposure["remaining_size"] is None