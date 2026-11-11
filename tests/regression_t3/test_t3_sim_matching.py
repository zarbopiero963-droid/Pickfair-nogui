"""T3 regressions: SIM matching engine / book side mapping (offline, no network).

Rows: PF-B1-R2-MATCH-CONSUME-PRICE, PF-I04B-03.
"""
from __future__ import annotations

import pytest

from core.simulation_matching_engine import SimulationMatchingEngine
from core.simulation_order_book import SimulationOrderBook
from core.simulation_state import SimulationState
from simulation_broker import SimulationBroker

MARKET = "1.900"
SEL = 7


def _engine_book(back_size: float = 100.0, lay_size: float = 100.0):
    book = SimulationOrderBook()
    book.update_market_book(MARKET, {
        "marketId": MARKET,
        "runners": [{
            "selectionId": SEL,
            "ex": {
                "availableToBack": [{"price": 2.0, "size": back_size}],
                "availableToLay": [{"price": 2.02, "size": lay_size}],
            },
        }],
    })
    # Defaults of the engine for slippage (1 tick); queue ahead disabled so the
    # whole level is matchable and the oracle is exact.
    engine = SimulationMatchingEngine(order_book=book, state=SimulationState(), queue_ahead_ratio=0.0)
    return engine, book


@pytest.mark.xfail(strict=True, reason="PF-B1-R2-MATCH-CONSUME-PRICE: consume_liquidity riceve il prezzo "
                                       "di fill aggiustato dallo slippage e non trova il livello (uguaglianza)")
@pytest.mark.parametrize("side,price,book_key,level_price", [
    ("BACK", 2.0, "availableToBack", 2.0),
    ("LAY", 2.02, "availableToLay", 2.02),
])
def test_pf_b1_r2_match_consume_price_level_consumed_with_slippage(side, price, book_key, level_price):
    """ID: PF-B1-R2-MATCH-CONSUME-PRICE

    Oracle: a 10.0 fill against a 100.0 level must leave 90.0 on the matched
    book level, even when the engine reports a slippage-adjusted fill price.
    """
    engine, book = _engine_book()
    res = engine.submit_order(bet_id="B1", market_id=MARKET, selection_id=SEL,
                              side=side, price=price, size=10.0)
    assert res.matched_size == pytest.approx(10.0)
    ladder = book.get_market_book(MARKET)["runners"][0]["ex"][book_key]
    assert ladder == [{"price": level_price, "size": pytest.approx(90.0)}]


def _broker_one_side(side_with_liquidity: str) -> SimulationBroker:
    broker = SimulationBroker(starting_balance=100.0)
    full = [{"price": 2.0, "size": 50.0}]
    ex = {"availableToBack": full if side_with_liquidity == "availableToBack" else [],
          "availableToLay": full if side_with_liquidity == "availableToLay" else []}
    broker.update_market_book(MARKET, {"marketId": MARKET, "status": "OPEN", "runners": [
        {"selectionId": SEL, "status": "ACTIVE", "ex": ex}]})
    return broker


@pytest.mark.parametrize("side,only_liquidity", [
    ("BACK", "availableToLay"),
    ("LAY", "availableToBack"),
])
def test_pf_i04b_03_empty_own_side_never_matches_opposite_side(side, only_liquidity):
    """ID: PF-I04B-03

    T-04B-03 (book vuoto sul lato eseguibile): BACK consumes only
    availableToBack, LAY only availableToLay (F6/P25). With the executable
    side empty and 50.0 @ 2.0 on the other side, the order rests unmatched:
    matched 0, status EXECUTABLE, zero fills, opposite ladder untouched.
    """
    broker = _broker_one_side(only_liquidity)
    broker.place_bet(market_id=MARKET, selection_id=SEL, side=side, price=2.0, size=5.0)
    (order,) = broker.state.orders.values()
    assert order.matched_size == 0.0
    assert order.status == "EXECUTABLE"
    assert broker.state.fills == []
    runner = broker.get_market_book(MARKET)["runners"][0]
    assert runner["ex"][only_liquidity] == [{"price": 2.0, "size": 50.0}]
