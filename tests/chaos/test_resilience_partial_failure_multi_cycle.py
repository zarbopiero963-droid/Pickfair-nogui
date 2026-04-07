from __future__ import annotations

import pytest

from core.trading_engine import STATUS_DUPLICATE_BLOCKED, STATUS_SUBMITTED
from tests.integration.test_betfair_timeout_and_ghost_orders import FakeClient, _make_engine, _payload


@pytest.mark.chaos
@pytest.mark.integration
def test_partial_failure_multi_cycle_preserves_unresolved_truth_until_evidence() -> None:
    engine1, db, _bus, _rec = _make_engine(client=FakeClient(response={"unexpected": "shape"}))
    partial = engine1.submit_quick_bet(_payload("RES-PARTIAL-1"))
    assert db.get_order(partial["order_id"])["status"] == STATUS_SUBMITTED

    # Cycle 2 after restart must not sanitize prior unresolved/submitted state.
    engine2, _db2, _bus2, _rec2 = _make_engine(client=FakeClient(error=TimeoutError("submit timeout")))
    engine2.db = db
    retry = engine2.submit_quick_bet(_payload("RES-PARTIAL-1"))

    assert retry["status"] == STATUS_DUPLICATE_BLOCKED
    persisted = db.get_order(partial["order_id"])
    assert persisted["status"] == STATUS_SUBMITTED
    assert persisted.get("outcome") != "SUCCESS"
