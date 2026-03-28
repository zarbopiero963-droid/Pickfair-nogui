import pytest

from dutching import calculate_cashout, calculate_dutching_stakes, dynamic_cashout_single


@pytest.mark.unit
@pytest.mark.core
@pytest.mark.invariant
def test_dutching_five_outcomes_exact_score_style_spread():
    odds = [6.0, 7.0, 8.0, 9.0, 10.0]
    result = calculate_dutching_stakes(odds, 20, commission=4.5)

    assert len(result["stakes"]) == 5
    assert len(result["profits"]) == 5
    assert len(result["net_profits"]) == 5
    assert abs(sum(result["stakes"]) - 20.0) < 0.02
    assert max(result["net_profits"]) - min(result["net_profits"]) < 0.10


@pytest.mark.unit
@pytest.mark.core
@pytest.mark.invariant
def test_dynamic_cashout_back_legacy_kwargs():
    result = dynamic_cashout_single(
        back_stake=100,
        back_price=2.0,
        lay_price=1.5,
        side="BACK",
    )

    assert result["cashout_stake"] > 0
    assert result["side_to_place"] == "LAY"
    assert result["lay_stake"] == result["cashout_stake"]
    assert abs(result["profit_if_win"] - result["profit_if_lose"]) < 0.05


@pytest.mark.unit
@pytest.mark.core
@pytest.mark.invariant
def test_dynamic_cashout_lay_legacy_kwargs():
    result = dynamic_cashout_single(
        lay_stake=100,
        lay_price=3.0,
        back_price=2.0,
        side="LAY",
    )

    assert result["cashout_stake"] > 0
    assert result["side_to_place"] == "BACK"
    assert result["back_stake"] == result["cashout_stake"]
    assert abs(result["profit_if_win"] - result["profit_if_lose"]) < 0.05


@pytest.mark.unit
@pytest.mark.core
@pytest.mark.failure
def test_dynamic_cashout_invalid_odds_returns_zeroed_result():
    result = dynamic_cashout_single(
        matched_stake=100,
        matched_price=1.0,
        current_price=1.5,
        side="BACK",
    )

    assert result["cashout_stake"] == 0.0
    assert result["green_up"] == 0.0
    assert result["net_profit"] == 0.0


@pytest.mark.unit
@pytest.mark.core
@pytest.mark.invariant
def test_default_invalid_side_falls_back_to_back():
    result = calculate_cashout(100, 2.0, 1.5, "INVALID")

    assert result["side_to_place"] == "LAY"