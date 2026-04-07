from __future__ import annotations

import pytest

from core.trading_engine import STATUS_AMBIGUOUS, STATUS_COMPLETED, STATUS_DUPLICATE_BLOCKED
from tests.integration.test_betfair_timeout_and_ghost_orders import (
    FakeClient,
    FlakyRemoteFetcher,
    ReconcilePassRunner,
    _make_engine,
    _payload,
)


@pytest.mark.chaos
@pytest.mark.integration
def test_exactly_once_logical_order_across_timeout_retry_restart_reconcile() -> None:
    engine1, db, _bus1, _rec1 = _make_engine(client=FakeClient(error=TimeoutError("submit timeout")))
    first = engine1.submit_quick_bet(_payload("RES-EO-1"))
    second = engine1.submit_quick_bet(_payload("RES-EO-1"))

    assert first["status"] == STATUS_AMBIGUOUS
    assert second["status"] == STATUS_DUPLICATE_BLOCKED

    engine2, _db2, _bus2, _rec2 = _make_engine(client=FakeClient(error=TimeoutError("submit timeout")))
    engine2.db = db
    engine2._inflight_keys = set(engine1._inflight_keys)
    third = engine2.submit_quick_bet(_payload("RES-EO-1"))
    assert third["status"] == STATUS_DUPLICATE_BLOCKED

    runner = ReconcilePassRunner(db, FlakyRemoteFetcher([{"bet_id": "REMOTE-EO-1"}]))
    assert runner.run_once(customer_ref="RES-EO-1") is True

    effective = [
        row
        for row in db.orders.values()
        if row.get("customer_ref") == "RES-EO-1" and row.get("status") != STATUS_DUPLICATE_BLOCKED
    ]
    assert len(effective) == 1
    assert effective[0]["status"] == STATUS_COMPLETED
