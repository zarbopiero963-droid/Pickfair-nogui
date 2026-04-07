from __future__ import annotations

import pytest

from core.trading_engine import STATUS_AMBIGUOUS, STATUS_DUPLICATE_BLOCKED
from tests.integration.test_betfair_timeout_and_ghost_orders import FakeClient, _make_engine, _payload


@pytest.mark.chaos
@pytest.mark.integration
def test_dedup_keys_persist_after_restart_and_allow_new_logical_request() -> None:
    engine, db, _bus, _rec = _make_engine(client=FakeClient(error=TimeoutError("submit timeout")))

    first = engine.submit_quick_bet(_payload("CS-DAR-1"))
    assert first["status"] == STATUS_AMBIGUOUS

    restarted, _db2, _bus2, _rec2 = _make_engine(client=FakeClient(error=TimeoutError("submit timeout")))
    restarted.db = db
    restarted._inflight_keys = set()
    restarted._repopulate_inflight_from_db()

    dup = restarted.submit_quick_bet(_payload("CS-DAR-1"))
    assert dup["status"] == STATUS_DUPLICATE_BLOCKED

    new_logical = restarted.submit_quick_bet(_payload("CS-DAR-2"))
    assert new_logical["status"] == STATUS_AMBIGUOUS

    non_duplicate = [o for o in db.orders.values() if o.get("status") != STATUS_DUPLICATE_BLOCKED]
    assert len(non_duplicate) == 2
