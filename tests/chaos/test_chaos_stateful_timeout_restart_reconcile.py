from __future__ import annotations

import pytest

from core.trading_engine import STATUS_AMBIGUOUS, STATUS_COMPLETED, STATUS_DUPLICATE_BLOCKED, STATUS_FAILED
from tests.integration.test_betfair_timeout_and_ghost_orders import (
    FakeClient,
    GhostReconciler,
    _make_engine,
    _payload,
)


@pytest.mark.chaos
@pytest.mark.integration
def test_timeout_ambiguous_survives_restart_then_reconciles_once() -> None:
    engine, db, _bus, _rec = _make_engine(client=FakeClient(error=TimeoutError("submit timeout")))

    first = engine.submit_quick_bet(_payload("CS-TRR-1"))
    assert first["status"] == STATUS_AMBIGUOUS
    assert first["status"] not in {STATUS_COMPLETED, STATUS_FAILED}

    restarted, _db2, _bus2, _rec2 = _make_engine(client=FakeClient(error=TimeoutError("submit timeout")))
    restarted.db = db
    restarted._inflight_keys = set()
    restarted._repopulate_inflight_from_db()

    duplicate = restarted.submit_quick_bet(_payload("CS-TRR-1"))
    assert duplicate["status"] == STATUS_DUPLICATE_BLOCKED

    state_after_restart = db.get_order(first["order_id"])
    assert state_after_restart["status"] == STATUS_AMBIGUOUS

    resolver = GhostReconciler(db, {"CS-TRR-1": {"bet_id": "REMOTE-CS-1"}})
    assert resolver.resolve_once(customer_ref="CS-TRR-1") is True

    final = db.get_order(first["order_id"])
    assert final["status"] == STATUS_COMPLETED
    assert final["remote_bet_id"] == "REMOTE-CS-1"
    assert final["outcome"] == "SUCCESS"

    effective = [o for o in db.orders.values() if o.get("status") != STATUS_DUPLICATE_BLOCKED]
    assert len(effective) == 1
    assert db.audit_events
