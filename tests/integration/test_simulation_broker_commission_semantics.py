import math
import pytest

from pnl_engine import PnLEngine
from simulation_broker import SimulationBroker


@pytest.mark.integration
def test_simulation_realized_commission_is_applied_only_on_positive_winnings():
    engine = PnLEngine(commission_pct=4.5)

    won = engine.calculate_settlement_pnl(
        side="BACK",
        price=3.0,
        size=100.0,
        won=True,
    )

    assert math.isfinite(won["gross_pnl"])
    assert math.isfinite(won["commission_amount"])
    assert math.isfinite(won["net_pnl"])
    assert won["gross_pnl"] == 200.0
    assert won["commission_amount"] == 9.0
    assert won["net_pnl"] == 191.0


@pytest.mark.integration
def test_simulation_realized_commission_is_zero_on_losses():
    engine = PnLEngine(commission_pct=4.5)

    lost = engine.calculate_settlement_pnl(
        side="BACK",
        price=3.0,
        size=100.0,
        won=False,
    )

    assert math.isfinite(lost["gross_pnl"])
    assert math.isfinite(lost["commission_amount"])
    assert math.isfinite(lost["net_pnl"])
    assert lost["gross_pnl"] == -100.0
    assert lost["commission_amount"] == 0.0
    assert lost["net_pnl"] == -100.0


@pytest.mark.integration
def test_simulation_broker_snapshot_exposes_realized_commission_accounting_contract():
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)
    snap = broker.snapshot()

    # Fail-closed expectation lock for phase 1A: simulation-facing accounting
    # must expose realized fields so drift cannot stay implicit.
    assert "realized_pnl" in snap
    assert "realized_commission" in snap
