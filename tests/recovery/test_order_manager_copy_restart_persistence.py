from __future__ import annotations

from typing import Any, Dict, Optional
from unittest.mock import MagicMock

import pytest

from order_manager import DuplicateOrderError, OrderManager, OrderStatus


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


def _payload(**overrides: Any) -> Dict[str, Any]:
    base = {
        "market_id": "1.777",
        "selection_id": 700,
        "bet_type": "BACK",
        "price": 2.2,
        "stake": 10.0,
        "customer_ref": "COPY-REF-1",
    }
    base.update(overrides)
    return base


def _make_manager(db: FakeDB, client: Any) -> OrderManager:
    return OrderManager(db=db, bus=None, client_getter=lambda: client, sleep_fn=lambda _: None)


def test_order_manager_restart_resumes_without_duplicate_submission_and_keeps_pending_visibility():
    db = FakeDB()
    client = MagicMock()
    client.place_bet = MagicMock(
        return_value={
            "status": "SUCCESS",
            "instructionReports": [{"status": "SUCCESS", "betId": "BET-1", "sizeMatched": 0.0}],
        }
    )

    manager_before = _make_manager(db, client)
    manager_before.place_order(_payload())

    saved = db.get_order_saga("COPY-REF-1")
    assert saved is not None
    saved["status"] = OrderStatus.PENDING.value

    manager_after = _make_manager(db, client)
    assert manager_after._current_status("COPY-REF-1") == OrderStatus.PENDING

    with pytest.raises(DuplicateOrderError):
        manager_after.place_order(_payload(customer_ref="COPY-REF-2"))

    assert client.place_bet.call_count == 1
