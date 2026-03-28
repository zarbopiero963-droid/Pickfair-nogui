import pytest

from dutching import calculate_cashout, calculate_dutching_stakes, dynamic_cashout_single


@pytest.mark.chaos
@pytest.mark.core
@pytest.mark.invariant
def test_many_outcomes_under_rounding_pressure_stay_consistent():
    odds = [2.11, 3.37, 4.89, 6.43, 8.77, 12.25, 15.9]
    result = calculate_dutching_stakes(odds, 123.45)

    assert len(result["stakes"]) == len(odds)
    assert abs(sum(result["stakes"]) - 123.45) < 0.02
    assert max(result["profits"]) - min(result["profits"]) < 0.2


@pytest.mark.chaos
@pytest.mark.core
@pytest.mark.failure
def test_extreme_cashout_prices_do_not_crash():
    result = dynamic_cashout_single(
        matched_stake=100,
        matched_price=1000,
        current_price=1.02,
        side="BACK",
    )

    assert result["cashout_stake"] > 0
    assert result["side_to_place"] == "LAY"


@pytest.mark.chaos
@pytest.mark.core
@pytest.mark.invariant
def test_invalid_side_and_invalid_numbers_degrade_safely():
    result = calculate_cashout("bad", "oops", None, side="???")

    assert result["cashout_stake"] == 0.0
    assert result["side_to_place"] == "LAY"