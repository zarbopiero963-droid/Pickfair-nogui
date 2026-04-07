import pytest

from simulation_broker import SimulationBroker


@pytest.mark.integration
def test_simulation_broker_copy_open_close_semantics_parity():
    broker = SimulationBroker()
    broker.update_market_book({"marketId": "1.4", "runners": [{"selectionId": 5, "ex": {"availableToLay": [{"price": 2.0, "size": 10.0}]}}]})

    opened = broker.place_bet(market_id="1.4", selection_id=5, side="BACK", price=2.0, size=2.0, customer_ref="copy-1", event_key="evt-copy")
    bet_id = opened["instructionReports"][0]["betId"]
    closed = broker.cancel_orders(market_id="1.4", instructions=[{"betId": bet_id}], customer_ref="copy-1")

    assert opened["instructionReports"][0]["status"] == "SUCCESS"
    assert closed["instructionReports"][0]["status"] in {"SUCCESS", "FAILURE"}
    assert opened["simulated"] is True and closed["simulated"] is True
