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


@pytest.mark.invariant
@pytest.mark.parametrize(
    "side,odds",
    [
        ("BACK", [2.25, 3.75, 5.5]),
        ("LAY", [1.5, 1.6, 1.7]),
    ],
)
def test_dispatcher_repeat_same_input_is_deterministic_for_back_and_lay(side, odds):
    from dutching import calculate_dutching

    selections = [
        {"selectionId": idx + 1, "price": price, "side": side}
        for idx, price in enumerate(odds)
    ]
    outputs = [calculate_dutching(selections, total_stake=250.0, commission=4.5) for _ in range(800)]

    first = outputs[0]
    assert all(out == first for out in outputs)


@pytest.mark.invariant
def test_dispatcher_lay_mode_is_explicit_and_not_silent_back_reuse():
    from dutching import calculate_dutching

    selections = [
        {"selectionId": 1, "price": 1.5, "side": "LAY"},
        {"selectionId": 2, "price": 1.6, "side": "LAY"},
        {"selectionId": 3, "price": 1.7, "side": "LAY"},
    ]
    results, _avg_profit, _book_pct, _avg_net_profit = calculate_dutching(
        selections,
        total_stake=120.0,
        commission=4.5,
    )

    total_stake = sum(float(item["stake"]) for item in results)
    assert len(results) == 3
    assert all(item["side"] == "LAY" for item in results)
    assert all(item["dutchingModel"] == "LAY_EQUAL_PROFIT_FIXED_TOTAL_STAKE" for item in results)
    for item in results:
        expected_liability = round(float(item["stake"]) * (float(item["price"]) - 1.0), 2)
        expected_gross = total_stake - (float(item["stake"]) * float(item["price"]))
        assert float(item["liability"]) == pytest.approx(expected_liability, abs=0.01)
        assert float(item["profitIfWins"]) == pytest.approx(expected_gross, abs=0.02)


@pytest.mark.invariant
def test_dispatcher_mixed_back_lay_contract_is_fail_closed_under_stress():
    from dutching import calculate_dutching

    rng = random.Random(777)
    for _ in range(300):
        n = rng.randint(2, 6)
        selections = []
        for idx in range(n):
            side = "BACK" if idx % 2 == 0 else "LAY"
            selections.append(
                {
                    "selectionId": idx + 1,
                    "price": round(rng.uniform(1.2, 8.0), 2),
                    "side": side,
                }
            )
        with pytest.raises(ValueError):
            calculate_dutching(selections, total_stake=100.0, commission=4.5)


@pytest.mark.invariant
def test_dispatcher_lay_liability_internal_consistency_and_floor():
    from dutching import calculate_dutching

    books = [
        [1.5, 1.6, 1.7],
        [2.1, 2.3, 2.8],
        [3.0, 4.0, 6.0],
        [8.0, 12.0, 20.0],
    ]
    for odds in books:
        selections = [
            {"selectionId": idx + 1, "price": price, "side": "LAY"}
            for idx, price in enumerate(odds)
        ]
        results, _avg_profit, _book_pct, _avg_net_profit = calculate_dutching(
            selections,
            total_stake=200.0,
            commission=4.5,
        )
        liabilities = [float(item["liability"]) for item in results]
        assert all("liability" in item for item in results)
        assert all(l >= 0.0 for l in liabilities)
        assert sum(liabilities) >= max(liabilities)
        for item in results:
            base = float(item["stake"]) * max(0.0, float(item["price"]) - 1.0)
            assert float(item["liability"]) + 0.01 >= base


@pytest.mark.invariant
def test_dispatcher_lay_rounding_spread_bounded_across_representative_books():
    from dutching import calculate_dutching

    cases = [
        [1.5, 1.6, 1.7],
        [2.0, 2.4, 3.2, 4.8],
        [3.0, 4.0, 6.0],
        [7.5, 8.5, 10.5, 12.0],
    ]
    for odds in cases:
        selections = [
            {"selectionId": idx + 1, "price": price, "side": "LAY"}
            for idx, price in enumerate(odds)
        ]
        results, _avg_profit, _book_pct, _avg_net_profit = calculate_dutching(
            selections,
            total_stake=150.0,
            commission=4.5,
        )
        gross = [float(item["profitIfWins"]) for item in results]
        net = [float(item["profitIfWinsNet"]) for item in results]
        assert max(gross) - min(gross) <= 0.25
        assert max(net) - min(net) <= 0.25


@pytest.mark.invariant
def test_dispatcher_lay_profitability_honesty_for_profitable_and_unprofitable_books():
    from dutching import calculate_dutching

    profitable = [
        {"selectionId": 1, "price": 1.5, "side": "LAY"},
        {"selectionId": 2, "price": 1.6, "side": "LAY"},
    ]
    unprofitable = [
        {"selectionId": 1, "price": 3.0, "side": "LAY"},
        {"selectionId": 2, "price": 4.0, "side": "LAY"},
        {"selectionId": 3, "price": 6.0, "side": "LAY"},
    ]

    prof_results, *_ = calculate_dutching(profitable, total_stake=120.0, commission=4.5)
    loss_results, *_ = calculate_dutching(unprofitable, total_stake=120.0, commission=4.5)

    assert min(float(item["profitIfWins"]) for item in prof_results) > 0.0
    assert max(float(item["profitIfWins"]) for item in loss_results) < 0.0


@pytest.mark.invariant
def test_dispatcher_direct_call_falsy_side_fallback_remains_stable_under_repetition():
    from dutching import calculate_dutching

    selections = [
        {"selectionId": 1, "price": 1.5, "side": "", "effectiveType": "LAY"},
        {"selectionId": 2, "price": 1.6, "side": None, "effectiveType": "LAY"},
        {"selectionId": 3, "price": 1.7, "side": 0, "effectiveType": "LAY"},
    ]

    values = [calculate_dutching(selections, total_stake=90.0, commission=4.5) for _ in range(400)]

    first = values[0]
    assert all(v == first for v in values)
    rows = first[0]
    assert all(str(item.get("side")) == "LAY" for item in rows)
    assert all(item["dutchingModel"] == "LAY_EQUAL_PROFIT_FIXED_TOTAL_STAKE" for item in rows)
