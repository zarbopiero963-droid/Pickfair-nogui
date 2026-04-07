from __future__ import annotations

import pytest

from core.trading_engine import STATUS_AMBIGUOUS, STATUS_COMPLETED, STATUS_FAILED
from tests.integration.test_betfair_timeout_and_ghost_orders import (
    FakeClient,
    FlakyRemoteFetcher,
    ReconcilePassRunner,
    _make_engine,
    _payload,
)


@pytest.mark.chaos
@pytest.mark.integration
def test_reconcile_multi_pass_converges_without_illegal_transition() -> None:
    engine, db, _bus, _rec = _make_engine(client=FakeClient(error=TimeoutError("submit timeout")))

    submit = engine.submit_quick_bet(_payload("CS-RMP-1"))
    assert submit["status"] == STATUS_AMBIGUOUS

    runner = ReconcilePassRunner(
        db,
        FlakyRemoteFetcher(
            outcomes=[
                TimeoutError("transient fetch timeout"),
                None,
                {"bet_id": "REMOTE-CS-RMP-1"},
            ]
        ),
    )

    assert runner.run_once(customer_ref="CS-RMP-1") is False
    assert db.get_order(submit["order_id"])["status"] == STATUS_AMBIGUOUS

    assert runner.run_once(customer_ref="CS-RMP-1") is False
    assert db.get_order(submit["order_id"])["status"] == STATUS_AMBIGUOUS

    assert runner.run_once(customer_ref="CS-RMP-1") is True
    final = db.get_order(submit["order_id"])
    assert final["status"] == STATUS_COMPLETED
    assert final["status"] != STATUS_FAILED
