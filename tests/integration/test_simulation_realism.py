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
