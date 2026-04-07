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
def test_replay_is_deterministic_and_does_not_emit_extra_effective_orders() -> None:
    engine, db, _bus, _rec = _make_engine(client=FakeClient(error=TimeoutError("submit timeout")))
    first = engine.submit_quick_bet(_payload("RES-REPLAY-1"))
    assert first["status"] == STATUS_AMBIGUOUS

    runner = ReconcilePassRunner(db, FlakyRemoteFetcher([{"bet_id": "REMOTE-REP-1"}]))
    assert runner.run_once(customer_ref="RES-REPLAY-1") is True
    final1 = db.get_order(first["order_id"])

    replay_runner = ReconcilePassRunner(db, FlakyRemoteFetcher([{"bet_id": "REMOTE-REP-1"}]))
    assert replay_runner.run_once(customer_ref="RES-REPLAY-1") is False
    final2 = db.get_order(first["order_id"])

    assert final1["status"] == STATUS_COMPLETED
    assert final1["status"] == final2["status"]
    assert final1.get("remote_bet_id") == final2.get("remote_bet_id")

    non_blocked = [o for o in db.orders.values() if o.get("status") != STATUS_DUPLICATE_BLOCKED]
    assert len(non_blocked) == 1
