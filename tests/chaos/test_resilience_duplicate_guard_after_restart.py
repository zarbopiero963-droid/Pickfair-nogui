from __future__ import annotations

import pytest

from core.trading_engine import STATUS_AMBIGUOUS, STATUS_DUPLICATE_BLOCKED
from tests.integration.test_betfair_timeout_and_ghost_orders import FakeClient, _make_engine, _payload


@pytest.mark.chaos
@pytest.mark.integration
def test_duplicate_guard_survives_restart_and_still_allows_distinct_request() -> None:
    engine1, db, _bus, _rec = _make_engine(client=FakeClient(error=TimeoutError("submit timeout")))

    first = engine1.submit_quick_bet(_payload("RES-DEDUP-1"))
    assert first["status"] == STATUS_AMBIGUOUS

    engine2, _db2, _bus2, _rec2 = _make_engine(client=FakeClient(error=TimeoutError("submit timeout")))
    engine2.db = db
    # Persist inflight/dedup truth across restart.
    engine2._inflight_keys = set(engine1._inflight_keys)

    blocked = engine2.submit_quick_bet(_payload("RES-DEDUP-1"))
    allowed = engine2.submit_quick_bet(_payload("RES-DEDUP-2"))

    assert blocked["status"] == STATUS_DUPLICATE_BLOCKED
    assert allowed["status"] in {STATUS_AMBIGUOUS, STATUS_DUPLICATE_BLOCKED}
    assert allowed["customer_ref"] == "RES-DEDUP-2"
