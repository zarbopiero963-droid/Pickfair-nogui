import pytest
import math

from dutching import (
    _apply_commission,
    _d,
    _round_step,
    calculate_cashout,
    calculate_dutching_stakes,
    dynamic_cashout_single,
)
from dutching_cache import cached_dutching_stakes, get_dutching_cache


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
    result = calculate_dutching_stakes([2.0, 4.0], 100, commission=0)

    assert len(result["stakes"]) == 2
    assert len(result["profits"]) == 2
    assert len(result["net_profits"]) == 2
    assert abs(sum(result["stakes"]) - 100.0) < 0.01
    assert abs(result["profits"][0] - result["profits"][1]) < 0.05


@pytest.mark.unit
@pytest.mark.guardrail
def test_calculate_dutching_invalid_odds():
    result = calculate_dutching_stakes([2.0, 1.0, 3.0], 100)

    assert result["stakes"] == []
    assert result["profits"] == []
    assert result["net_profits"] == []
    assert result["error"] == "Invalid odds <= 1.0"


@pytest.mark.unit
@pytest.mark.guardrail
def test_calculate_dutching_invalid_total_stake():
    result = calculate_dutching_stakes([2.0, 3.0], 0)

    assert result["stakes"] == []
    assert result["profits"] == []
    assert result["net_profits"] == []
    assert result["book_pct"] == 0.0
    assert result["avg_profit"] == 0.0
    assert result["avg_net_profit"] == 0.0


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


@pytest.mark.unit
@pytest.mark.guardrail
def test_dutching_commission_reduces_net_profits_only():
    result = calculate_dutching_stakes([2.5, 3.5, 4.5], 60, commission=4.5)

    assert len(result["profits"]) == 3
    assert len(result["net_profits"]) == 3

    for gross, net in zip(result["profits"], result["net_profits"]):
        assert net <= gross + 1e-9


@pytest.mark.unit
@pytest.mark.guardrail
def test_dutching_helper_output_is_not_realized_settlement_authority_contract():
    result = calculate_dutching_stakes([2.4, 3.6, 5.2], 75, commission=4.5)
    assert "settlement_source" not in result
    assert "settlement_kind" not in result
    assert "settlement_basis" not in result
    assert "commission_pct" not in result


@pytest.mark.unit
@pytest.mark.guardrail
def test_dutching_helper_rejects_non_italy_commission_when_commission_enabled():
    with pytest.raises(ValueError):
        calculate_dutching_stakes([2.4, 3.6, 5.2], 75, commission=5.0)

    with pytest.raises(ValueError):
        dynamic_cashout_single(
            matched_stake=100,
            matched_price=2.0,
            current_price=1.5,
            side="BACK",
            commission=5.0,
        )


@pytest.mark.unit
@pytest.mark.guardrail
def test_dutching_zero_commission_mode_is_explicit_gross_only_preview():
    result = calculate_dutching_stakes([2.4, 3.6, 5.2], 75, commission=0.0)
    assert len(result["profits"]) == len(result["net_profits"]) == 3
    for gross, net in zip(result["profits"], result["net_profits"]):
        assert net == gross
    assert "settlement_source" not in result
    assert "settlement_kind" not in result
    assert "settlement_basis" not in result


@pytest.mark.unit
@pytest.mark.guardrail
def test_dutching_dynamic_cashout_surface_is_helper_only_not_canonical_settlement_contract():
    result = dynamic_cashout_single(
        matched_stake=100.0,
        matched_price=2.0,
        current_price=1.6,
        commission=4.5,
        side="BACK",
    )
    for forbidden_key in (
        "settlement_source",
        "settlement_kind",
        "settlement_basis",
        "settlement_authority",
        "settlement_validation",
        "settlement_acceptance",
        "commission_pct",
    ):
        assert forbidden_key not in result


@pytest.mark.unit
@pytest.mark.guardrail
def test_dutching_equal_profit_is_tolerance_bounded_with_italy_commission_reference():
    result = calculate_dutching_stakes([3.0, 4.0, 6.0], 120, commission=4.5)

    assert len(result["stakes"]) == 3
    assert abs(sum(result["stakes"]) - 120.0) < 0.01
    assert all(math.isfinite(x) for x in result["net_profits"])
    # Equalized net-profit expectation is explicit and fail-closed.
    spread = max(result["net_profits"]) - min(result["net_profits"])
    assert spread <= 0.10


@pytest.mark.unit
@pytest.mark.guardrail
def test_dutching_rounding_tolerance_is_explicitly_bounded():
    result = calculate_dutching_stakes([2.17, 3.41, 5.35], 37, commission=4.5)

    assert len(result["net_profits"]) == 3
    assert all(math.isfinite(x) for x in result["net_profits"])
    # Tight-but-realistic tolerance that allows cent rounding only.
    assert max(result["net_profits"]) - min(result["net_profits"]) <= 0.15


@pytest.mark.unit
@pytest.mark.guardrail
def test_dutching_unprofitable_books_are_reported_honestly_not_forced_positive():
    # Book > 100% means non-profitable dutch; tests must lock honesty semantics.
    result = calculate_dutching_stakes([1.9, 1.9], 100, commission=4.5)

    assert result["book_pct"] > 100.0
    assert len(result["net_profits"]) == 2
    assert max(result["net_profits"]) < 0.0


@pytest.mark.unit
@pytest.mark.guardrail
def test_dutching_cache_hit_avoids_recompute():
    cache = get_dutching_cache()
    cache.clear()

    calls = {"n": 0}

    def fake_calc(selections, total_stake, bet_type, commission):
        calls["n"] += 1
        return ([{"selectionId": 1, "stake": total_stake}], 1.0, 90.0)

    selections = [{"selectionId": 1, "price": 2.0, "side": "BACK"}]
    first = cached_dutching_stakes(fake_calc, selections, 10.0, "BACK", 4.5)
    second = cached_dutching_stakes(fake_calc, selections, 10.0, "BACK", 4.5)

    assert first == second
    assert calls["n"] == 1
    assert cache.get_stats()["hits"] >= 1


@pytest.mark.unit
@pytest.mark.guardrail
def test_dutching_cache_key_separates_back_and_lay():
    cache = get_dutching_cache()
    cache.clear()

    calls = {"n": 0}

    def fake_calc(selections, total_stake, bet_type, commission):
        calls["n"] += 1
        return ([{"selectionId": 1, "stake": total_stake, "side": bet_type}], 1.0, 90.0)

    back_sel = [{"selectionId": 1, "price": 2.0, "effectiveType": "BACK"}]
    lay_sel = [{"selectionId": 1, "price": 2.0, "effectiveType": "LAY"}]

    cached_dutching_stakes(fake_calc, back_sel, 10.0, "BACK", 4.5)
    cached_dutching_stakes(fake_calc, lay_sel, 10.0, "LAY", 4.5)

    assert calls["n"] == 2
