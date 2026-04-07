from __future__ import annotations

import pytest

from core.trading_engine import STATUS_AMBIGUOUS, STATUS_COMPLETED
from tests.integration.test_betfair_timeout_and_ghost_orders import (
    FakeClient,
    FlakyRemoteFetcher,
    ReconcilePassRunner,
    _make_engine,
    _payload,
)


@pytest.mark.chaos
@pytest.mark.integration
def test_reconcile_converges_after_multiple_retries_once_evidence_exists() -> None:
    engine, db, _bus, _rec = _make_engine(client=FakeClient(error=TimeoutError("network timeout")))
    res = engine.submit_quick_bet(_payload("RES-RETRY-1"))
    assert res["status"] == STATUS_AMBIGUOUS

    fetcher = FlakyRemoteFetcher([TimeoutError("fetch timeout"), None, {"bet_id": "REMOTE-R1"}])
    runner = ReconcilePassRunner(db, fetcher)

    assert runner.run_once(customer_ref="RES-RETRY-1") is False
    assert db.get_order(res["order_id"])["status"] == STATUS_AMBIGUOUS
    assert runner.run_once(customer_ref="RES-RETRY-1") is False
    assert db.get_order(res["order_id"])["status"] == STATUS_AMBIGUOUS
    assert runner.run_once(customer_ref="RES-RETRY-1") is True
    assert fetcher.calls > 1
    assert db.get_order(res["order_id"])["status"] == STATUS_COMPLETED
