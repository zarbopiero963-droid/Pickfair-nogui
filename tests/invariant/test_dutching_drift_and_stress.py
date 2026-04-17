import math
import random
import pytest


@pytest.mark.invariant
def test_dutching_repeated_same_input_is_stable():
    from dutching import calculate_dutching_stakes

    odds = [2.25, 3.75, 5.5]
    outputs = []

    for _ in range(2000):
        result = calculate_dutching_stakes(
            odds=odds,
            total_stake=250.0,
            commission=4.5,
            equalize=True,
            commission_aware=True,
        )
        outputs.append(
            (
                tuple(round(x, 8) for x in result["stakes"]),
                tuple(round(x, 8) for x in result["profits"]),
                tuple(round(x, 8) for x in result["net_profits"]),
                round(result["book_pct"], 8),
                round(result["avg_profit"], 8),
                round(result["avg_net_profit"], 8),
            )
        )

    first = outputs[0]
    assert all(x == first for x in outputs)


@pytest.mark.invariant
def test_dutching_total_stake_is_preserved_under_rounding():
    from dutching import calculate_dutching_stakes

    result = calculate_dutching_stakes(
        odds=[2.0, 3.0, 4.0, 6.0],
        total_stake=100.0,
        commission=4.5,
        equalize=True,
        commission_aware=True,
    )

    assert result["stakes"]
    assert abs(sum(result["stakes"]) - 100.0) <= 0.011


@pytest.mark.chaos
def test_dutching_numeric_stress_extreme_odds_stays_finite():
    from dutching import calculate_dutching_stakes

    extreme_sets = [
        [1.01, 1000.0],
        [1.02, 500.0, 750.0],
        [1.5, 2.0, 100.0, 1000.0],
        [50.0, 75.0, 120.0],
    ]

    for odds in extreme_sets:
        result = calculate_dutching_stakes(
            odds=odds,
            total_stake=1000000.0,
            commission=4.5,
            equalize=True,
            commission_aware=True,
        )

        assert result["stakes"]
        assert all(math.isfinite(x) for x in result["stakes"])
        assert all(math.isfinite(x) for x in result["profits"])
        assert all(math.isfinite(x) for x in result["net_profits"])
        assert all(x >= 0.0 for x in result["stakes"])
        assert math.isfinite(result["book_pct"])
        assert math.isfinite(result["avg_profit"])
        assert math.isfinite(result["avg_net_profit"])


@pytest.mark.chaos
def test_dutching_randomized_stress_no_negative_or_nan():
    from dutching import calculate_dutching_stakes

    rng = random.Random(2026)

    for _ in range(4000):
        n = rng.randint(2, 8)
        odds = [rng.uniform(1.01, 1000.0) for _ in range(n)]
        total_stake = rng.uniform(0.01, 1_000_000.0)
        # Generic helper-stress surface: keep commission in policy-allowed
        # helper modes (4.5 for Italy-facing commission-aware math, 0.0 for
        # explicit non-policy/gross helper mode).
        commission = 4.5 if rng.random() < 0.5 else 0.0

        result = calculate_dutching_stakes(
            odds=odds,
            total_stake=total_stake,
            commission=commission,
            equalize=True,
            commission_aware=True,
        )

        assert len(result["stakes"]) == n
        assert all(math.isfinite(x) for x in result["stakes"])
        assert all(x >= 0.0 for x in result["stakes"])
        assert abs(sum(result["stakes"]) - total_stake) <= 0.05


@pytest.mark.invariant
def test_dynamic_cashout_single_balanced_output_is_finite():
    from dutching import dynamic_cashout_single

    result = dynamic_cashout_single(
        matched_stake=100.0,
        matched_price=2.5,
        current_price=2.0,
        commission=4.5,
        side="BACK",
    )

    for key in [
        "cashout_stake",
        "green_up",
        "net_profit",
        "profit_if_win",
        "profit_if_lose",
    ]:
        assert math.isfinite(result[key])

    assert result["side_to_place"] == "LAY"
    assert result["cashout_stake"] >= 0.0


@pytest.mark.chaos
def test_dynamic_cashout_single_numeric_stress_extremes():
    from dutching import dynamic_cashout_single

    cases = [
        (0.01, 1.01, 1000.0, "BACK"),
        (1000000.0, 1000.0, 1.01, "BACK"),
        (500000.0, 50.0, 75.0, "LAY"),
        (0.5, 500.0, 1.02, "LAY"),
    ]

    for stake, matched_price, current_price, side in cases:
        result = dynamic_cashout_single(
            matched_stake=stake,
            matched_price=matched_price,
            current_price=current_price,
            commission=4.5,
            side=side,
        )

        for key in [
            "cashout_stake",
            "green_up",
            "net_profit",
            "profit_if_win",
            "profit_if_lose",
        ]:
            assert math.isfinite(result[key])


@pytest.mark.invariant
def test_calculate_cashout_repeat_same_input_is_stable():
    from dutching import calculate_cashout

    values = [
        calculate_cashout(100.0, 2.5, 2.0, "BACK")
        for _ in range(2000)
    ]

    first = values[0]
    assert all(v == first for v in values)


@pytest.mark.chaos
def test_calculate_cashout_extreme_inputs_no_nan():
    from dutching import calculate_cashout

    cases = [
        (0.01, 1.01, 1000.0, "BACK"),
        (1000000.0, 1000.0, 1.01, "BACK"),
        (1000000.0, 1000.0, 1.01, "LAY"),
        (0.1, 500.0, 2.0, "LAY"),
    ]

    for stake, entry, current, side in cases:
        result = calculate_cashout(stake, entry, current, side)
        for value in result.values():
            if isinstance(value, (int, float)):
                assert math.isfinite(value)
