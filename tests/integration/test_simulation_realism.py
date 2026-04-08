from core.pnl_engine import PnLEngine
from core.simulation_matching_engine import SimulationMatchingEngine
from core.simulation_order_book import SimulationOrderBook
from core.simulation_state import SimulationState


def _seed_book(book: SimulationOrderBook, market_id: str = "1.234", selection_id: int = 11):
    book.update_market_book(
        market_id,
        {
            "marketId": market_id,
            "runners": [
                {
                    "selectionId": selection_id,
                    "ex": {
                        "availableToBack": [
                            {"price": 2.0, "size": 4.0},
                            {"price": 1.99, "size": 10.0},
                        ],
                        "availableToLay": [
                            {"price": 2.02, "size": 4.0},
                            {"price": 2.04, "size": 10.0},
                        ],
                    },
                }
            ],
        },
    )


def test_order_is_not_fully_matched_when_queue_ahead_blocks_volume():
    book = SimulationOrderBook()
    state = SimulationState()
    _seed_book(book)

    engine = SimulationMatchingEngine(order_book=book, state=state, queue_ahead_ratio=0.5, slippage_ticks=0)

    res = engine.submit_order(
        bet_id="b1",
        market_id="1.234",
        selection_id=11,
        side="BACK",
        price=2.10,
        size=8.0,
    )

    assert res.status == "PARTIAL"
    assert res.matched_size == 7.0
    assert res.remaining_size == 1.0


def test_slippage_worse_execution_under_constrained_liquidity():
    book = SimulationOrderBook()
    state = SimulationState()
    _seed_book(book)

    engine = SimulationMatchingEngine(order_book=book, state=state, queue_ahead_ratio=0.0, slippage_ticks=1)

    res = engine.submit_order(
        bet_id="b2",
        market_id="1.234",
        selection_id=11,
        side="BACK",
        price=2.10,
        size=2.0,
    )

    assert res.status == "EXECUTION_COMPLETE"
    assert res.average_matched_price < 2.02


def test_partial_fill_remains_supported():
    book = SimulationOrderBook()
    state = SimulationState()
    _seed_book(book)

    engine = SimulationMatchingEngine(order_book=book, state=state, queue_ahead_ratio=0.75, slippage_ticks=0)

    res = engine.submit_order(
        bet_id="b3",
        market_id="1.234",
        selection_id=11,
        side="BACK",
        price=2.10,
        size=4.0,
    )

    assert res.status == "PARTIAL"
    assert 0.0 < res.matched_size < 4.0


def test_pnl_reflects_pessimistic_fill_vs_naive_immediate_fill():
    book = SimulationOrderBook()
    state = SimulationState()
    _seed_book(book)

    engine = SimulationMatchingEngine(order_book=book, state=state, queue_ahead_ratio=0.0, slippage_ticks=2)
    res = engine.submit_order(
        bet_id="b4",
        market_id="1.234",
        selection_id=11,
        side="BACK",
        price=2.10,
        size=2.0,
    )

    pnl = PnLEngine(bus=None, commission_pct=0.0)
    market_book = {
        "marketId": "1.234",
        "runners": [
            {
                "selectionId": 11,
                "ex": {
                    "availableToBack": [{"price": 2.00, "size": 100.0}],
                    "availableToLay": [{"price": 1.95, "size": 100.0}],
                },
            }
        ],
    }

    pessimistic_pos = {
        "selection_id": 11,
        "side": "BACK",
        "price": res.average_matched_price,
        "stake": res.matched_size,
    }
    naive_pos = {
        "selection_id": 11,
        "side": "BACK",
        "price": 2.02,
        "stake": res.matched_size,
    }

    pessimistic_pnl = pnl._calc(pessimistic_pos, market_book)
    naive_pnl = pnl._calc(naive_pos, market_book)

    assert pessimistic_pnl < naive_pnl
