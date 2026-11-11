"""Gate A — T2b batch b1, componente direct_unmatched_ttl (select_expired_unmatched reale, funzione pura)."""
from __future__ import annotations

from direct_unmatched_ttl import select_expired_unmatched

NOW = 1_000_000.0


def _row(bet_id, market="1.1", matched=0.0, remaining=5.0, placed=NOW - 100.0):
    return {"betId": bet_id, "marketId": market, "sizeMatched": matched, "sizeRemaining": remaining,
            "placed_epoch": placed}


def test_trg_628_b_1019():
    """ID: TRG-628-B-1019 / PF-B1-R2-TTL-ALLOWLIST"""
    rows = [_row("D1"), _row("C1"), _row("D2", market="1.2"), _row("D1")]
    out = select_expired_unmatched(current_orders=rows, now_epoch=NOW, ttl_seconds=30,
                                   direct_bet_ids={"D1", "D2"})
    assert out == [{"market_id": "1.1", "bet_id": "D1"}, {"market_id": "1.2", "bet_id": "D2"}]
    scoped = select_expired_unmatched(current_orders=rows, now_epoch=NOW, ttl_seconds=30,
                                      direct_bet_ids={"D1", "D2"}, scope_market_ids=["1.2"])
    assert scoped == [{"market_id": "1.2", "bet_id": "D2"}]
    for empty in (None, set(), [""]):
        assert select_expired_unmatched(current_orders=rows, now_epoch=NOW, ttl_seconds=30,
                                        direct_bet_ids=empty) == []


def test_trg_628_b_1020():
    """ID: TRG-628-B-1020 / PF-B1-R2-TTL-PARTIAL-EXCLUDE"""
    rows = [_row("P1", matched=1.0, remaining=4.0), _row("Z1", matched=0.0, remaining=0.0),
            _row("N1", matched=float("nan")), _row("U1", matched=0.0, remaining=5.0),
            _row("M1", matched=None)]
    out = select_expired_unmatched(current_orders=rows, now_epoch=NOW, ttl_seconds=30,
                                   direct_bet_ids={"P1", "Z1", "N1", "U1", "M1"})
    assert out == [{"market_id": "1.1", "bet_id": "U1"}]


def test_trg_628_b_1021():
    """ID: TRG-628-B-1021 / PF-B1-R2-TTL-AGE"""
    rows = [_row("EQ", placed=NOW - 30.0), _row("GT", placed=NOW - 30.001), _row("LT", placed=NOW - 29.0),
            _row("NA", placed=None)]
    out = select_expired_unmatched(current_orders=rows, now_epoch=NOW, ttl_seconds=30,
                                   direct_bet_ids={"EQ", "GT", "LT", "NA"})
    assert out == [{"market_id": "1.1", "bet_id": "GT"}]
