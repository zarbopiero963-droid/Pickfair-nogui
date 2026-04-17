import math
import pytest

from core.pnl_engine import PnLEngine as EventDrivenPnLEngine
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
    assert won["settlement_basis"] == "market_net_realized"
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
    assert lost["settlement_basis"] == "market_net_realized"


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
    assert snap["realized_pnl"] == 91.0
    assert snap["realized_commission"] == 9.0
    assert snap["last_settlement"]["gross_pnl"] == -100.0
    assert snap["last_settlement"]["commission_amount"] == 0.0
    assert snap["last_settlement"]["net_pnl"] == -100.0
    assert snap["last_settlement"]["commission_pct"] == 4.5
    assert snap["last_settlement"]["settlement_source"] == "simulation_broker"
    assert snap["last_settlement"]["settlement_kind"] == "realized_settlement"
    assert snap["last_settlement"]["settlement_basis"] == "market_net_realized"


@pytest.mark.integration
def test_simulation_broker_enforces_explicit_betfair_italy_commission_policy():
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=5.0)
    with pytest.raises(ValueError):
        broker.record_realized_settlement(100.0)


@pytest.mark.integration
def test_live_and_sim_realized_contracts_share_market_net_settlement_basis():
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)
    sim_settlement = broker.record_realized_settlement(50.0)

    class _Bus:
        def __init__(self) -> None:
            self.events: list[tuple[str, dict]] = []

        def subscribe(self, *_args, **_kwargs):
            return None

        def publish(self, topic, payload):
            self.events.append((topic, dict(payload or {})))

    bus = _Bus()
    engine = EventDrivenPnLEngine(bus=bus, commission_pct=4.5)
    engine._close(
        {
            "event_key": "evt-live-contract",
            "table_id": 1,
            "batch_id": "batch-live-contract",
        },
        {
            "gross_pnl": 50.0,
            "commission_amount": 2.25,
            "net_pnl": 47.75,
            "commission_pct": 4.5,
            "settlement_source": "core_pnl_engine",
        },
    )
    live_events = [payload for topic, payload in bus.events if topic == "RUNTIME_CLOSE_POSITION"]
    assert len(live_events) == 1
    live_settlement = live_events[0]

    assert sim_settlement["settlement_kind"] == live_settlement["settlement_kind"] == "realized_settlement"
    assert sim_settlement["settlement_basis"] == live_settlement["settlement_basis"] == "market_net_realized"
