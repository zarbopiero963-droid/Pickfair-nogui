import pytest

from simulation_broker import SimulationBroker


@pytest.mark.integration
def test_simulation_broker_cashout_contract_class_parity():
    broker = SimulationBroker()
    broker.update_market_book({"marketId": "1.5", "runners": [{"selectionId": 10, "ex": {"availableToLay": [{"price": 1.9, "size": 10.0}], "availableToBack": [{"price": 1.8, "size": 10.0}]}}]})

    open_leg = broker.place_bet(market_id="1.5", selection_id=10, side="BACK", price=2.0, size=3.0)
    cashout_leg = broker.place_bet(market_id="1.5", selection_id=10, side="LAY", price=1.8, size=3.0)

    assert open_leg["status"] == cashout_leg["status"] == "SUCCESS"
    assert set(open_leg.keys()) == set(cashout_leg.keys())
    assert open_leg["instructionReports"][0]["status"] == cashout_leg["instructionReports"][0]["status"]
