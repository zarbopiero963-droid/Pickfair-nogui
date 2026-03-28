import pytest

from dutching import calculate_cashout, calculate_dutching_stakes, dynamic_cashout_single


@pytest.mark.e2e
def test_full_exact_score_style_flow():
    odds = [6.0, 7.5, 9.0, 11.0, 13.0]
    total_stake = 20.0

    dutch = calculate_dutching_stakes(odds, total_stake)

    assert len(dutch["stakes"]) == 5
    assert abs(sum(dutch["stakes"]) - total_stake) < 0.02

    selected_stake = dutch["stakes"][2]
    selected_odds = odds[2]

    cash = dynamic_cashout_single(
        matched_stake=selected_stake,
        matched_price=selected_odds,
        current_price=7.0,
        side="BACK",
    )

    assert cash["cashout_stake"] > 0
    assert cash["side_to_place"] == "LAY"

    final_cash = calculate_cashout(selected_stake, selected_odds, 7.0, "BACK")
    assert final_cash["cashout_stake"] > 0