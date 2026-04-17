import math
import pytest

from simulation_broker import SimulationBroker


@pytest.mark.integration
def test_simulation_realized_commission_is_applied_only_on_positive_winnings():
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)
    won = broker.record_realized_settlement(200.0)

    assert math.isfinite(won["gross_pnl"])
    assert math.isfinite(won["commission_amount"])
    assert math.isfinite(won["net_pnl"])
    assert won["gross_pnl"] == 200.0
    assert won["commission_amount"] == 9.0
    assert won["net_pnl"] == 191.0
    assert won["commission_pct"] == 4.5
    assert won["settlement_source"] == "simulation_broker"
    assert won["settlement_kind"] == "realized_settlement"
    assert won["pnl"] == won["net_pnl"]


@pytest.mark.integration
def test_simulation_realized_commission_is_zero_on_losses():
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)
    lost = broker.record_realized_settlement(-100.0)

    assert math.isfinite(lost["gross_pnl"])
    assert math.isfinite(lost["commission_amount"])
    assert math.isfinite(lost["net_pnl"])
    assert lost["gross_pnl"] == -100.0
    assert lost["commission_amount"] == 0.0
    assert lost["net_pnl"] == -100.0
    assert lost["commission_pct"] == 4.5
    assert lost["settlement_source"] == "simulation_broker"
    assert lost["settlement_kind"] == "realized_settlement"


@pytest.mark.integration
def test_simulation_broker_snapshot_exposes_realized_commission_accounting_contract():
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)
    broker.record_realized_settlement(200.0)
    broker.record_realized_settlement(-100.0)
    snap = broker.snapshot()

    # Fail-closed expectation lock: simulation-facing realized accounting is explicit.
    assert "realized_pnl" in snap
    assert "realized_commission" in snap
    assert "last_settlement" in snap
    # Commission is market-net scoped on the default/global market key:
    # gross path +200 then -100 => market-net +100 => commission 4.5
    assert snap["realized_pnl"] == 95.5
    assert snap["realized_commission"] == 4.5
    assert snap["last_settlement"]["gross_pnl"] == -100.0
    # Refund leg: later negative gross reduced prior market-net commission basis.
    assert snap["last_settlement"]["commission_amount"] == -4.5
    assert snap["last_settlement"]["net_pnl"] == -95.5
    assert snap["last_settlement"]["commission_pct"] == 4.5
    assert snap["last_settlement"]["settlement_source"] == "simulation_broker"
    assert snap["last_settlement"]["settlement_kind"] == "realized_settlement"


@pytest.mark.integration
def test_simulation_broker_enforces_explicit_betfair_italy_commission_policy():
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=5.0)
    with pytest.raises(ValueError):
        broker.record_realized_settlement(100.0)


@pytest.mark.integration
def test_simulation_same_market_multi_leg_commission_is_market_net_positive_once():
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)

    # Same market, two realized legs:
    # +100 gross then -40 gross => market-net gross +60
    first = broker.record_realized_settlement(100.0, market_id="1.777")
    second = broker.record_realized_settlement(-40.0, market_id="1.777")

    expected_market_net_gross = 60.0
    expected_market_commission = expected_market_net_gross * 0.045
    expected_market_net = expected_market_net_gross - expected_market_commission

    assert first["market_id"] == "1.777"
    assert second["market_id"] == "1.777"
    assert second["market_net_gross"] == expected_market_net_gross
    assert second["market_commission_amount_total"] == expected_market_commission
    assert broker.state.realized_commission == expected_market_commission
    assert broker.state.realized_pnl == expected_market_net
    assert broker.state.balance == 1000.0 + expected_market_net


@pytest.mark.integration
def test_simulation_same_market_multi_leg_net_loss_has_zero_commission():
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)

    # Same market, two realized legs:
    # +30 gross then -50 gross => market-net gross -20 => zero commission.
    broker.record_realized_settlement(30.0, market_id="1.778")
    second = broker.record_realized_settlement(-50.0, market_id="1.778")

    assert second["market_id"] == "1.778"
    assert second["market_net_gross"] == -20.0
    assert second["market_commission_amount_total"] == 0.0
    assert broker.state.realized_commission == 0.0
    assert broker.state.realized_pnl == -20.0
    assert broker.state.balance == pytest.approx(980.0)


@pytest.mark.integration
def test_simulation_market_net_overcharge_detector_differs_from_per_leg_commission():
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)

    # If commission were charged per positive leg only:
    # +100 leg => 4.50 commission, -40 leg => 0.00 commission => total 4.50
    # Market-net rule must be commission on (+100-40)=+60 => 2.70.
    broker.record_realized_settlement(100.0, market_id="1.779")
    broker.record_realized_settlement(-40.0, market_id="1.779")

    per_leg_commission = 100.0 * 0.045
    market_net_commission = 60.0 * 0.045

    assert per_leg_commission == pytest.approx(4.5)
    assert market_net_commission == pytest.approx(2.7)
    assert broker.state.realized_commission == pytest.approx(market_net_commission)
    assert broker.state.realized_commission < per_leg_commission
