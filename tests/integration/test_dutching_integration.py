import pytest

from dutching import calculate_cashout, calculate_dutching_stakes, dynamic_cashout_single


@pytest.mark.integration
def test_dutching_then_cashout_flow_back():
    dutch = calculate_dutching_stakes([3.0, 4.0, 5.0], 60)

    assert len(dutch["stakes"]) == 3

    first_stake = dutch["stakes"][0]
    cash = dynamic_cashout_single(
        matched_stake=first_stake,
        matched_price=3.0,
        current_price=2.4,
        side="BACK",
    )

    assert cash["cashout_stake"] > 0
    assert cash["side_to_place"] == "LAY"


@pytest.mark.integration
def test_cashout_function_and_dynamic_cashout_are_consistent():
    a = calculate_cashout(100, 2.0, 1.5, "BACK")
    b = dynamic_cashout_single(
        matched_stake=100,
        matched_price=2.0,
        current_price=1.5,
        side="BACK",
        commission=0,
    )

    assert abs(a["cashout_stake"] - b["cashout_stake"]) < 0.01
    assert abs(a["profit_if_win"] - b["profit_if_win"]) < 0.05
    assert abs(a["profit_if_lose"] - b["profit_if_lose"]) < 0.05