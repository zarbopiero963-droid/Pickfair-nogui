import pytest

from simulation_broker import SimulationBroker


@pytest.mark.integration
def test_simulation_broker_dutching_group_parity_and_determinism():
    broker = SimulationBroker()
    broker.update_market_book({
        "marketId": "1.6",
        "runners": [
            {"selectionId": 1, "ex": {"availableToLay": [{"price": 3.0, "size": 50.0}]}},
            {"selectionId": 2, "ex": {"availableToLay": [{"price": 4.0, "size": 50.0}]}},
        ],
    })

    instructions = [{"selection_id": 1, "side": "BACK", "price": 3.0, "stake": 10.0}, {"selection_id": 2, "side": "BACK", "price": 4.0, "stake": 8.0}]
    r1 = broker.place_orders(market_id="1.6", instructions=instructions, batch_id="BATCH-1", event_key="EVT")
    r2 = broker.place_orders(market_id="1.6", instructions=instructions, batch_id="BATCH-1", event_key="EVT")

    assert r1["status"] == r2["status"] == "SUCCESS"
    assert len(r1["instructionReports"]) == len(r2["instructionReports"]) == 2
    assert [x["status"] for x in r1["instructionReports"]] == [x["status"] for x in r2["instructionReports"]]
