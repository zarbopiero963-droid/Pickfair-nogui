from __future__ import annotations

import pytest

from core.trading_engine import STATUS_AMBIGUOUS, STATUS_DUPLICATE_BLOCKED
from tests.integration.test_betfair_timeout_and_ghost_orders import FakeClient, _make_engine, _payload


@pytest.mark.chaos
@pytest.mark.integration
def test_copy_pattern_and_normal_metadata_paths_remain_isolated() -> None:
    engine, db, _bus, _rec = _make_engine(client=FakeClient(error=TimeoutError("submit timeout")))

    copy_payload = {**_payload("RES-COPY-1"), "source_type": "copy", "origin": "COPY"}
    pattern_payload = {**_payload("RES-PATTERN-1"), "source_type": "pattern", "origin": "PATTERN"}
    normal_payload = {**_payload("RES-NORMAL-1"), "source_type": "manual", "origin": "NORMAL"}

    r_copy = engine.submit_quick_bet(copy_payload)
    r_pattern = engine.submit_quick_bet(pattern_payload)
    r_normal = engine.submit_quick_bet(normal_payload)

    assert r_copy["status"] == STATUS_AMBIGUOUS
    assert r_pattern["status"] == STATUS_AMBIGUOUS
    assert r_normal["status"] == STATUS_AMBIGUOUS

    o_copy = db.get_order(r_copy["order_id"])
    o_pattern = db.get_order(r_pattern["order_id"])
    o_normal = db.get_order(r_normal["order_id"])

    assert o_copy["customer_ref"] != o_pattern["customer_ref"]
    assert o_pattern["customer_ref"] != o_normal["customer_ref"]

    dup_copy = engine.submit_quick_bet(copy_payload)
    dup_pattern = engine.submit_quick_bet(pattern_payload)
    assert dup_copy["status"] == STATUS_DUPLICATE_BLOCKED
    assert dup_pattern["status"] == STATUS_DUPLICATE_BLOCKED
