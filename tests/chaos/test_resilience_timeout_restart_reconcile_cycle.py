from __future__ import annotations

import pytest

from core.trading_engine import STATUS_AMBIGUOUS, STATUS_COMPLETED, STATUS_DUPLICATE_BLOCKED, STATUS_FAILED
from tests.integration.test_betfair_timeout_and_ghost_orders import (
    FakeClient,
    FlakyRemoteFetcher,
    ReconcilePassRunner,
    _make_engine,
    _payload,
)


@pytest.mark.chaos
@pytest.mark.integration
def test_timeout_ambiguous_restart_reconcile_finalize_cycle_is_reconstructible() -> None:
    engine1, db, _bus, _rec = _make_engine(client=FakeClient(error=TimeoutError("submit timeout")))
    first = engine1.submit_quick_bet(_payload("RES-TIMEOUT-1"))

    assert first["status"] == STATUS_AMBIGUOUS
    assert first["status"] != STATUS_FAILED

    # Restart with persisted DB: unresolved truth must survive restart.
    engine2, _db2, _bus2, _rec2 = _make_engine(client=FakeClient(response={"bet_id": "REMOTE-1"}))
    engine2.db = db

    order_before_evidence = db.get_order(first["order_id"])
    assert order_before_evidence["status"] == STATUS_AMBIGUOUS

    runner = ReconcilePassRunner(db, FlakyRemoteFetcher([None, {"bet_id": "REMOTE-1"}]))
    assert runner.run_once(customer_ref="RES-TIMEOUT-1") is False
    assert db.get_order(first["order_id"])["status"] == STATUS_AMBIGUOUS

    assert runner.run_once(customer_ref="RES-TIMEOUT-1") is True
    final_order = db.get_order(first["order_id"])
    assert final_order["status"] == STATUS_COMPLETED
    assert final_order["remote_bet_id"] == "REMOTE-1"

    # Reconstructible trail + no duplicate effective order.
    assert len(db.audit_events) > 0
    non_blocked = [o for o in db.orders.values() if o.get("status") != STATUS_DUPLICATE_BLOCKED]
    assert len(non_blocked) == 1
