import pytest

from core.position_ledger import PositionLedger


def test_same_side_multiple_fills_updates_weighted_average_correctly():
    ledger = PositionLedger(market_id="1.100", runner_id=10)

    ledger.apply_fill(fill_id="f1", side="BACK", price=2.0, size=100.0)
    ledger.apply_fill(fill_id="f2", side="BACK", price=3.0, size=50.0)

    snap = ledger.snapshot()
    assert snap.open_side == "BACK"
    assert snap.open_size == pytest.approx(150.0)
    assert snap.avg_entry_price == pytest.approx((2.0 * 100.0 + 3.0 * 50.0) / 150.0)
    assert snap.realized_pnl == pytest.approx(0.0)


def test_back_then_lay_partial_close_realizes_only_closed_portion_and_keeps_residual():
    ledger = PositionLedger(market_id="1.101", runner_id=11)

    ledger.apply_fill(fill_id="f1", side="BACK", price=2.0, size=100.0)
    out = ledger.apply_fill(fill_id="f2", side="LAY", price=1.5, size=40.0)

    assert out["realized_delta"] == pytest.approx((2.0 - 1.5) * 40.0)

    snap = ledger.snapshot()
    assert snap.open_side == "BACK"
    assert snap.open_size == pytest.approx(60.0)
    assert snap.avg_entry_price == pytest.approx(2.0)
    assert snap.realized_pnl == pytest.approx((2.0 - 1.5) * 40.0)


def test_lay_then_back_partial_close_realizes_only_closed_portion_and_keeps_residual():
    ledger = PositionLedger(market_id="1.102", runner_id=12)

    ledger.apply_fill(fill_id="f1", side="LAY", price=2.0, size=100.0)
    out = ledger.apply_fill(fill_id="f2", side="BACK", price=2.5, size=40.0)

    assert out["realized_delta"] == pytest.approx((2.5 - 2.0) * 40.0)

    snap = ledger.snapshot()
    assert snap.open_side == "LAY"
    assert snap.open_size == pytest.approx(60.0)
    assert snap.avg_entry_price == pytest.approx(2.0)
    assert snap.realized_pnl == pytest.approx((2.5 - 2.0) * 40.0)


def test_full_close_zeroes_residual_open_size():
    ledger = PositionLedger(market_id="1.103", runner_id=13)

    ledger.apply_fill(fill_id="f1", side="BACK", price=2.0, size=50.0)
    ledger.apply_fill(fill_id="f2", side="LAY", price=1.8, size=50.0)

    snap = ledger.snapshot()
    assert snap.open_side == ""
    assert snap.open_size == pytest.approx(0.0)
    assert snap.avg_entry_price == pytest.approx(0.0)
    assert snap.exposure == pytest.approx(0.0)
    assert snap.liability == pytest.approx(0.0)


def test_realized_and_unrealized_stay_distinct():
    ledger = PositionLedger(market_id="1.104", runner_id=14)

    ledger.apply_fill(fill_id="f1", side="BACK", price=2.0, size=100.0)
    ledger.apply_fill(fill_id="f2", side="LAY", price=1.5, size=20.0)

    before_mtm = ledger.snapshot()
    assert before_mtm.realized_pnl == pytest.approx((2.0 - 1.5) * 20.0)
    assert before_mtm.unrealized_pnl == pytest.approx(0.0)

    after_mtm = ledger.mark_to_market(mark_price=1.8)
    # residual BACK 80 with avg 2.0 -> unrealized = (2.0 - 1.8) * 80
    assert after_mtm.realized_pnl == pytest.approx((2.0 - 1.5) * 20.0)
    assert after_mtm.unrealized_pnl == pytest.approx((2.0 - 1.8) * 80.0)


def test_duplicate_fill_id_does_not_duplicate_realization():
    ledger = PositionLedger(market_id="1.105", runner_id=15)

    ledger.apply_fill(fill_id="f1", side="BACK", price=2.0, size=100.0)
    first = ledger.apply_fill(fill_id="f2", side="LAY", price=1.5, size=20.0)
    second = ledger.apply_fill(fill_id="f2", side="LAY", price=1.5, size=20.0)

    assert first["applied"] is True
    assert first["duplicate"] is False
    assert first["realized_delta"] == pytest.approx((2.0 - 1.5) * 20.0)

    assert second["applied"] is False
    assert second["duplicate"] is True
    assert second["realized_delta"] == pytest.approx(0.0)

    snap = ledger.snapshot()
    assert snap.realized_pnl == pytest.approx((2.0 - 1.5) * 20.0)
    assert snap.open_size == pytest.approx(80.0)


def test_residual_exposure_and_liability_semantics_for_back_and_lay():
    back = PositionLedger(market_id="1.106", runner_id=16)
    back.apply_fill(fill_id="b1", side="BACK", price=3.0, size=10.0)
    back_snap = back.snapshot()
    assert back_snap.exposure == pytest.approx(10.0)
    assert back_snap.liability == pytest.approx(10.0)

    lay = PositionLedger(market_id="1.107", runner_id=17)
    lay.apply_fill(fill_id="l1", side="LAY", price=3.0, size=10.0)
    lay_snap = lay.snapshot()
    assert lay_snap.exposure == pytest.approx(20.0)
    assert lay_snap.liability == pytest.approx(20.0)
