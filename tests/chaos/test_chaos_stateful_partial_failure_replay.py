from __future__ import annotations

import pytest

from core.trading_engine import STATUS_COMPLETED, STATUS_DUPLICATE_BLOCKED, STATUS_SUBMITTED
from tests.integration.test_betfair_timeout_and_ghost_orders import FakeClient, _make_engine, _payload


@pytest.mark.chaos
@pytest.mark.integration
def test_partial_failure_replay_remains_unresolved_and_exactly_once() -> None:
    engine, db, bus, _rec = _make_engine(client=FakeClient(response={"malformed": True}))

    first = engine.submit_quick_bet(_payload("CS-PFR-1"))
    assert first["status"] == "ACCEPTED_FOR_PROCESSING"
    assert first["status"] != STATUS_COMPLETED

    order_before = db.get_order(first["order_id"])
    assert order_before["status"] == STATUS_SUBMITTED

    restarted, _db2, _bus2, _rec2 = _make_engine(client=FakeClient(response={"malformed": True}))
    restarted.db = db
    restarted._inflight_keys = set()
    restarted._repopulate_inflight_from_db()

    second = restarted.submit_quick_bet(_payload("CS-PFR-1"))
    assert second["status"] == STATUS_DUPLICATE_BLOCKED

    order_after = db.get_order(first["order_id"])
    assert order_after["status"] == STATUS_SUBMITTED

    all_non_duplicate = [o for o in db.orders.values() if o.get("status") != STATUS_DUPLICATE_BLOCKED]
    assert len(all_non_duplicate) == 1

    published = [name for name, _payload in bus.events]
    assert "QUICK_BET_SUCCESS" not in published
