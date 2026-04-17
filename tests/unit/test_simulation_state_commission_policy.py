import pytest

from core.simulation_state import SimulationState


def _open_position(state: SimulationState, bet_id: str = "b1") -> None:
    state.add_position(
        bet_id=bet_id,
        market_id="1.999",
        selection_id=77,
        side="BACK",
        price=2.0,
        size=10.0,
    )


def test_simulation_state_settle_position_applies_4p5_commission_on_positive_pnl():
    state = SimulationState(starting_balance=1000.0, commission_pct=4.5)
    _open_position(state)

    settled = state.settle_position("b1", 200.0)

    assert settled is not None
    assert settled.status == "SETTLED"
    assert settled.realized_pnl == pytest.approx(191.0)
    assert state.realized_pnl == pytest.approx(191.0)


def test_simulation_state_settle_position_applies_zero_commission_on_negative_pnl():
    state = SimulationState(starting_balance=1000.0, commission_pct=4.5)
    _open_position(state)

    settled = state.settle_position("b1", -100.0)

    assert settled is not None
    assert settled.status == "SETTLED"
    assert settled.realized_pnl == pytest.approx(-100.0)
    assert state.realized_pnl == pytest.approx(-100.0)


def test_simulation_state_settle_position_applies_zero_commission_on_zero_pnl():
    state = SimulationState(starting_balance=1000.0, commission_pct=4.5)
    _open_position(state)

    settled = state.settle_position("b1", 0.0)

    assert settled is not None
    assert settled.realized_pnl == 0.0
    assert state.realized_pnl == 0.0


def test_simulation_state_settle_position_rejects_non_italy_commission_pct():
    state = SimulationState(starting_balance=1000.0, commission_pct=5.0)
    _open_position(state)

    with pytest.raises(ValueError):
        state.settle_position("b1", 200.0)


def test_simulation_state_settle_position_rejects_zero_commission_pct_fail_closed():
    state = SimulationState(starting_balance=1000.0, commission_pct=4.5)
    state.commission_pct = 0.0
    _open_position(state)

    with pytest.raises(ValueError):
        state.settle_position("b1", 200.0)


def test_simulation_state_settle_position_rejects_non_italy_pct_even_on_losses():
    state = SimulationState(starting_balance=1000.0, commission_pct=3.0)
    _open_position(state)

    with pytest.raises(ValueError):
        state.settle_position("b1", -50.0)
