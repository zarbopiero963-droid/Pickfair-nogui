from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from core.trading_engine import STATUS_DUPLICATE_BLOCKED
from tests.integration.test_betfair_timeout_and_ghost_orders import FakeClient, _make_engine, _payload


@pytest.mark.chaos
@pytest.mark.integration
def test_concurrent_submit_then_restart_preserves_dedup_integrity() -> None:
    engine, db, _bus, _rec = _make_engine(client=FakeClient(error=TimeoutError("submit timeout")))
    payload = _payload("CS-CRI-1")

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _i: engine.submit_quick_bet(payload), [0, 1]))

    statuses = {r["status"] for r in results}
    assert STATUS_DUPLICATE_BLOCKED in statuses

    effective = [o for o in db.orders.values() if o.get("status") != STATUS_DUPLICATE_BLOCKED]
    assert len(effective) == 1

    restarted, _db2, _bus2, _rec2 = _make_engine(client=FakeClient(error=TimeoutError("submit timeout")))
    restarted.db = db
    restarted._inflight_keys = set()
    restarted._repopulate_inflight_from_db()

    after_restart = restarted.submit_quick_bet(payload)
    assert after_restart["status"] == STATUS_DUPLICATE_BLOCKED

    effective_after = [o for o in db.orders.values() if o.get("status") != STATUS_DUPLICATE_BLOCKED]
    assert len(effective_after) == 1
