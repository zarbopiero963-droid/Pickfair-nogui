import pytest

from dutching import calculate_cashout, calculate_dutching_stakes, dynamic_cashout_single


@pytest.mark.unit
@pytest.mark.failure
def test_dutching_empty_odds():
    result = calculate_dutching_stakes([], 100)

    assert result["stakes"] == []
    assert result["profits"] == []
    assert result["net_profits"] == []
    assert result["book_pct"] == 0.0


@pytest.mark.unit
@pytest.mark.failure
def test_dutching_impossible_odds_all_invalid():
    result = calculate_dutching_stakes([0.0, -2.0, 1.0], 100)

    assert result["stakes"] == []
    assert result["profits"] == []
    assert result["net_profits"] == []
    assert "error" in result


@pytest.mark.unit
@pytest.mark.failure
def test_cashout_with_odds_below_equal_one():
    result = calculate_cashout(100, 0.9, 1.2, "BACK")

    assert result["cashout_stake"] == 0.0
    assert result["side_to_place"] == "LAY"


@pytest.mark.unit
@pytest.mark.failure
def test_dynamic_cashout_negative_stake_is_rejected_to_zero():
    result = dynamic_cashout_single(
        matched_stake=-10,
        matched_price=2.0,
        current_price=1.5,
        side="BACK",
    )

    assert result["cashout_stake"] == 0.0
    assert result["profit_if_win"] == 0.0
    assert result["profit_if_lose"] == 0.0


@pytest.mark.unit
@pytest.mark.failure
def test_dynamic_cashout_invalid_side_degrades_to_back():
    result = dynamic_cashout_single(
        matched_stake=100,
        matched_price=2.0,
        current_price=1.5,
        side="???",
    )

    assert result["side_to_place"] == "LAY"
    assert result["cashout_stake"] > 0


@pytest.mark.unit
@pytest.mark.failure
def test_dynamic_cashout_invalid_prices_return_zeroed_result():
    result = dynamic_cashout_single(
        matched_stake=100,
        matched_price="bad",
        current_price=None,
        side="BACK",
    )

    assert result["cashout_stake"] == 0.0
    assert result["green_up"] == 0.0
    assert result["net_profit"] == 0.0