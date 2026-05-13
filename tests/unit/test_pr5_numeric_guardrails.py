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
def test_dutching_rejects_non_finite_odds_or_stake():
    bad_odds = calculate_dutching_stakes([2.0, float("inf")], 100.0)
    bad_stake = calculate_dutching_stakes([2.0, 3.0], float("nan"))

    assert bad_odds["stakes"] == []
    assert bad_odds["error"] == "Invalid odds <= 1.0"
    assert bad_stake["stakes"] == []


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
