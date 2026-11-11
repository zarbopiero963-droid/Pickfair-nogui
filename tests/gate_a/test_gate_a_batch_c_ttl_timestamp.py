"""Gate A batch C: direct_unmatched_ttl.py placed timestamp parsing."""
from __future__ import annotations

import time
from datetime import datetime, timezone

import pytest

import direct_unmatched_ttl as ttl


@pytest.fixture
def non_utc_local_tz(monkeypatch):
    """Force a non-UTC local zone so a naive datetime would be misread as local."""
    if not hasattr(time, "tzset"):
        pytest.skip("time.tzset not available")
    monkeypatch.setenv("TZ", "Asia/Tokyo")
    time.tzset()
    yield
    monkeypatch.undo()
    time.tzset()


def test_trg_628_b_1022(non_utc_local_tz):
    """ID: TRG-628-B-1022 / PF-B1-R2-TTL-TIMESTAMP

    _placed_epoch falls back to ISO placedDate (also when placed_epoch is
    malformed); the 'Z' suffix and naive datetimes are both treated as UTC,
    independent of the process local timezone.
    """
    expected = datetime(2026, 6, 27, 12, 0, tzinfo=timezone.utc).timestamp()
    assert ttl._placed_epoch({"placedDate": "2026-06-27T12:00:00Z"}) == expected
    assert ttl._placed_epoch({"placedDate": "2026-06-27T12:00:00"}) == expected          # naive => UTC
    assert ttl._placed_epoch({"placed_date": "2026-06-27T12:00:00"}) == expected         # snake alias
    assert ttl._placed_epoch({"placedDate": "2026-06-27T14:00:00+02:00"}) == expected    # explicit offset kept
    assert ttl._placed_epoch({"placed_epoch": "nan", "placedDate": "2026-06-27T12:00:00"}) == expected
    assert ttl._placed_epoch({"placed_epoch": 42.0, "placedDate": "2026-06-27T12:00:00"}) == 42.0
    assert ttl._placed_epoch({"placedDate": "garbage"}) is None

    # End-to-end through the public selector: a naive placedDate 10 minutes old
    # with a 300s TTL is expired; 1 minute old is not.
    row = {"betId": "B", "marketId": "1.1", "sizeMatched": 0.0, "sizeRemaining": 2.0,
           "placedDate": "2026-06-27T12:00:00"}
    assert ttl.select_expired_unmatched(current_orders=[row], now_epoch=expected + 600,
                                        ttl_seconds=300.0, direct_bet_ids={"B"}) == [
        {"market_id": "1.1", "bet_id": "B"}]
    assert ttl.select_expired_unmatched(current_orders=[row], now_epoch=expected + 60,
                                        ttl_seconds=300.0, direct_bet_ids={"B"}) == []
