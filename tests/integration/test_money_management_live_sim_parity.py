import pytest

from core.money_management import RoserpinaMoneyManagement
from core.system_state import RoserpinaConfig, RiskProfile


@pytest.mark.integration
@pytest.mark.parametrize("simulation_mode", [False, True])
def test_money_management_same_stake_and_cycle_semantics(simulation_mode):
    cfg = RoserpinaConfig(
        target_profit_cycle_pct=3.0,
        max_single_bet_pct=20.0,
        max_total_exposure_pct=50.0,
        max_event_exposure_pct=25.0,
        risk_profile=RiskProfile.BALANCED,
        allow_recovery=True,
    )
    mm = RoserpinaMoneyManagement(cfg)
    decision = mm.calculate(
        signal={"price": 2.5, "simulation_mode": simulation_mode, "copy_meta": {"master_id": "x"}, "pattern_meta": {"pattern_id": "p"}},
        bankroll_current=1000,
        equity_peak=1000,
        current_total_exposure=10,
        event_current_exposure=2,
        table={"table_id": 2, "loss_amount": 30.0, "in_recovery": True},
    )

    assert decision.approved is True
    assert decision.recommended_stake > 0
    assert decision.table_id == 2


@pytest.mark.integration
def test_money_management_live_sim_parity_no_simplified_sim_sizing():
    cfg = RoserpinaConfig(max_stake_abs=11.0, max_single_bet_pct=80.0, target_profit_cycle_pct=10.0)
    mm = RoserpinaMoneyManagement(cfg)
    live = mm.calculate(signal={"odds": 3.0, "simulation_mode": False}, bankroll_current=100, equity_peak=100, current_total_exposure=0, event_current_exposure=0, table={"id": 1, "loss": 200})
    sim = mm.calculate(signal={"odds": 3.0, "simulation_mode": True}, bankroll_current=100, equity_peak=100, current_total_exposure=0, event_current_exposure=0, table={"id": 1, "loss": 200})

    assert live.recommended_stake == sim.recommended_stake == 11.0
    assert live.reason == sim.reason
    assert live.metadata.keys() == sim.metadata.keys()


@pytest.mark.integration
@pytest.mark.parametrize("simulation_mode", [False, True])
def test_money_management_exposure_limit_block_reason_is_live_sim_parity(simulation_mode):
    cfg = RoserpinaConfig(
        max_total_exposure_pct=20.0,  # 20 on bankroll=100
        max_single_bet_pct=50.0,
        max_event_exposure_pct=50.0,
        target_profit_cycle_pct=10.0,
    )
    mm = RoserpinaMoneyManagement(cfg)
    decision = mm.calculate(
        signal={"price": 2.0, "simulation_mode": simulation_mode},
        bankroll_current=100.0,
        equity_peak=100.0,
        current_total_exposure=19.0,
        event_current_exposure=0.0,
        table={"id": 7, "loss": 20.0},
    )

    assert decision.approved is False
    assert decision.reason == "supera_max_total_exposure"
    assert decision.recommended_stake == 0.0
