import pytest

from core.money_management import RoserpinaMoneyManagement
from pnl_engine import PnLEngine
from core.system_state import RoserpinaConfig
from dutching import calculate_dutching_stakes


@pytest.mark.unit
def test_mm_non_finite_inputs_fail_closed():
    mm = RoserpinaMoneyManagement(RoserpinaConfig())
    decision = mm.calculate(
        signal={"price": 2.0},
        bankroll_current=float("nan"),
        equity_peak=float("inf"),
        current_total_exposure=float("nan"),
        event_current_exposure=float("inf"),
        table={"id": 1, "loss": float("nan")},
    )
    assert decision.approved is False
    assert decision.recommended_stake == 0.0


@pytest.mark.unit
def test_mm_safe_float_non_finite_default_falls_back_to_zero():
    mm = RoserpinaMoneyManagement(RoserpinaConfig())
    assert mm._safe_float("x", float("inf")) == 0.0


@pytest.mark.unit
def test_dutching_rejects_non_finite_odds_or_stake():
    bad_odds = calculate_dutching_stakes([2.0, float("inf")], 100.0)
    bad_stake = calculate_dutching_stakes([2.0, 3.0], float("nan"))

    assert bad_odds["stakes"] == []
    assert bad_odds["error"] == "Invalid odds <= 1.0"
    assert bad_stake["stakes"] == []


@pytest.mark.unit
@pytest.mark.parametrize(
    "odds",
    [
        ["NaN", "2.0"],
        ["nan", "2.0"],
        [" INF ", "2.0"],
        ["+infinity", "2.0"],
        ["-INF", "2.0"],
    ],
)
def test_dutching_rejects_non_finite_string_odds(odds):
    result = calculate_dutching_stakes(odds, "100.0")
    assert result["stakes"] == []
    assert "Invalid odds" in (result.get("error") or "")


@pytest.mark.unit
@pytest.mark.parametrize("stake", ["NaN", "nan", " INF ", "+infinity", "-INF"])
def test_dutching_rejects_non_finite_string_stake(stake):
    result = calculate_dutching_stakes(["2.0", "3.0"], stake)
    assert result["stakes"] == []


@pytest.mark.unit
def test_dutching_valid_allocation_preserved():
    result = calculate_dutching_stakes([2.0, 4.0], 100.0, commission=0.0)
    assert len(result["stakes"]) == 2
    assert abs(sum(result["stakes"]) - 100.0) < 0.01


@pytest.mark.unit
def test_pnl_rejects_non_finite_preview_inputs():
    engine = PnLEngine(commission_pct=4.5)
    with pytest.raises(ValueError):
        engine.calculate_position_pnl(
            market_id="1.1",
            selection_id=1,
            side="BACK",
            entry_price=float("nan"),
            exit_price=2.0,
            size=10.0,
        )


@pytest.mark.unit
@pytest.mark.parametrize(
    "price,size",
    [
        (float("nan"), 1.0),
        (2.0, float("inf")),
        (2.0, float("-inf")),
    ],
)
def test_settlement_pnl_rejects_non_finite_inputs(price, size):
    engine = PnLEngine(commission_pct=4.5)
    with pytest.raises(ValueError):
        engine.calculate_settlement_pnl(side="BACK", price=price, size=size, won=True)


@pytest.mark.unit
@pytest.mark.parametrize(
    "entry_price,entry_size,hedge_price",
    [
        (float("nan"), 1.0, 2.0),
        (1.5, float("inf"), 2.0),
        (1.5, 1.0, float("-inf")),
    ],
)
def test_green_up_size_rejects_non_finite_inputs(entry_price, entry_size, hedge_price):
    engine = PnLEngine(commission_pct=4.5)
    with pytest.raises(ValueError):
        engine.calculate_green_up_size(
            entry_side="BACK",
            entry_price=entry_price,
            entry_size=entry_size,
            hedge_price=hedge_price,
        )


@pytest.mark.unit
def test_pnl_engine_rejects_non_finite_commission_pct():
    with pytest.raises(ValueError):
        PnLEngine(commission_pct=float("nan")).calculate_position_pnl(
            market_id="1.1",
            selection_id=1,
            side="BACK",
            entry_price=2.0,
            exit_price=2.2,
            size=10.0,
        )


@pytest.mark.unit
def test_pnl_valid_output_preserved():
    engine = PnLEngine(commission_pct=4.5)
    result = engine.calculate_position_pnl(
        market_id="1.1",
        selection_id=1,
        side="BACK",
        entry_price=2.0,
        exit_price=3.0,
        size=10.0,
    )
    assert result.gross_pnl == 5.0
    assert result.commission_amount == pytest.approx(0.225)
    assert result.net_pnl == pytest.approx(4.775)
