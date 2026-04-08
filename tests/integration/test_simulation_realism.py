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
from tests.helpers.fake_market import FakeMarket


def test_no_instant_fill_without_sufficient_opposing_liquidity() -> None:
    market = FakeMarket()
    market.seed_selection(
        selection_id=101,
        back_levels=[(2.0, 20.0)],
        lay_levels=[(2.02, 5.0), (2.04, 5.0)],
    )

    oid = market.place_resting_order(selection_id=101, side="BACK", price=2.02, size=8.0)
    before = market.snapshot_order(oid)

    assert before["matched_size"] == 0.0
    assert before["status"] == "RESTING"

    market.advance_tick(101, opposing_traded_size=5.0)
    after = market.snapshot_order(oid)

    assert after["matched_size"] == 0.0
    assert after["status"] == "RESTING"


def test_partial_fill_when_only_part_of_size_exists() -> None:
    market = FakeMarket()
    market.seed_selection(
        selection_id=202,
        back_levels=[(1.99, 10.0)],
        lay_levels=[(2.0, 3.0)],
    )

    oid = market.place_resting_order(selection_id=202, side="BACK", price=2.0, size=5.0)
    market.advance_tick(202, opposing_traded_size=6.0)
    snap = market.snapshot_order(oid)

    assert snap["matched_size"] == 3.0
    assert snap["remaining_size"] == 2.0
    assert snap["status"] == "PARTIALLY_MATCHED"
    assert snap["average_fill_price"] == 2.0


def test_pessimistic_weighted_fill_across_levels_when_enabled() -> None:
    market = FakeMarket()
    market.seed_selection(
        selection_id=303,
        back_levels=[(2.06, 30.0)],
        lay_levels=[(2.0, 2.0), (2.02, 2.0), (2.04, 2.0)],
    )

    oid = market.place_resting_order(
        selection_id=303,
        side="BACK",
        price=2.0,
        size=6.0,
        allow_worse_fill=True,
    )
    market.advance_tick(303, opposing_traded_size=12.0)
    snap = market.snapshot_order(oid)

    assert snap["matched_size"] == 6.0
    assert snap["status"] == "MATCHED"
    assert snap["fills"] == [(2.0, 2.0), (2.02, 2.0), (2.04, 2.0)]
    assert snap["average_fill_price"] == 2.02


def test_unmatched_remainder_persists_until_liquidity_is_added() -> None:
    market = FakeMarket()
    market.seed_selection(selection_id=404, back_levels=[], lay_levels=[])

    oid = market.place_resting_order(selection_id=404, side="BACK", price=3.0, size=4.0)
    market.advance_tick(404, opposing_traded_size=10.0)
    snap_without_liq = market.snapshot_order(oid)

    assert snap_without_liq["matched_size"] == 0.0
    assert snap_without_liq["remaining_size"] == 4.0
    assert snap_without_liq["status"] == "RESTING"

    market.add_liquidity(selection_id=404, side="LAY", price=3.0, size=4.0)
    market.advance_tick(404, opposing_traded_size=4.0)
    snap_with_liq = market.snapshot_order(oid)

    assert snap_with_liq["matched_size"] == 4.0
    assert snap_with_liq["remaining_size"] == 0.0
    assert snap_with_liq["status"] == "MATCHED"
