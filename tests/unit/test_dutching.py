import pytest

from dutching import (
    _apply_commission,
    _d,
    _round_step,
    calculate_cashout,
    calculate_dutching_stakes,
    dynamic_cashout_single,
)


@pytest.mark.unit
@pytest.mark.guardrail
def test_decimal_parser_accepts_comma_and_dot():
    assert _d("1,50") == _d("1.50")
    assert _d(None) == _d("0")
    assert _d("x") == _d("0")


@pytest.mark.unit
@pytest.mark.guardrail
def test_round_step_two_places():
    assert str(_round_step(_d("1.234"))) == "1.23"
    assert str(_round_step(_d("1.235"))) == "1.24"


@pytest.mark.unit
@pytest.mark.guardrail
def test_apply_commission_only_on_positive_profit():
    assert _apply_commission(_d("10"), 5) == _d("9.5")
    assert _apply_commission(_d("-10"), 5) == _d("-10")
    assert _apply_commission(_d("10"), 0) == _d("10")


@pytest.mark.unit
@pytest.mark.guardrail
def test_calculate_dutching_stakes_equalized_profit_basic():
    result = calculate_dutching_stakes([2.0, 4.0], 100)

    assert len(result["stakes"]) == 2
    assert len(result["profits"]) == 2
    assert abs(sum(result["stakes"]) - 100.0) < 0.01
    assert abs(result["profits"][0] - result["profits"][1]) < 0.05


@pytest.mark.unit
@pytest.mark.guardrail
def test_calculate_dutching_invalid_odds():
    result = calculate_dutching_stakes([2.0, 1.0, 3.0], 100)

    assert result["stakes"] == []
    assert result["profits"] == []
    assert result["error"] == "Invalid odds <= 1.0"


@pytest.mark.unit
@pytest.mark.guardrail
def test_calculate_dutching_invalid_total_stake():
    result = calculate_dutching_stakes([2.0, 3.0], 0)

    assert result["stakes"] == []
    assert result["profits"] == []
    assert result["book_pct"] == 0.0
    assert result["avg_profit"] == 0.0


@pytest.mark.unit
@pytest.mark.guardrail
def test_calculate_cashout_back_equal_profit():
    result = calculate_cashout(100, 2.0, 1.5, "BACK")

    assert result["cashout_stake"] > 0
    assert result["side_to_place"] == "LAY"
    assert abs(result["profit_if_win"] - result["profit_if_lose"]) < 0.05
    assert result["guaranteed_profit"] == min(
        result["profit_if_win"], result["profit_if_lose"]
    )


@pytest.mark.unit
@pytest.mark.guardrail
def test_calculate_cashout_lay_equal_profit():
    result = calculate_cashout(100, 3.0, 2.0, "LAY")

    assert result["cashout_stake"] > 0
    assert result["side_to_place"] == "BACK"
    assert abs(result["profit_if_win"] - result["profit_if_lose"]) < 0.05


@pytest.mark.unit
@pytest.mark.guardrail
def test_calculate_cashout_invalid_inputs():
    result = calculate_cashout(0, 2.0, 1.5, "BACK")

    assert result["cashout_stake"] == 0.0
    assert result["profit_if_win"] == 0.0
    assert result["profit_if_lose"] == 0.0
    assert result["guaranteed_profit"] == 0.0