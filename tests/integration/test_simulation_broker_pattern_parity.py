import pytest

from simulation_broker import SimulationBroker


@pytest.mark.integration
def test_simulation_broker_pattern_semantic_parity_against_live_contract_fake():
    broker = SimulationBroker(partial_fill_enabled=True)
    broker.update_market_book({
        "marketId": "1.3",
        "runners": [{"selectionId": 9, "ex": {"availableToBack": [{"price": 2.0, "size": 3.0}]}}],
    })

    # Semantica Betfair (#383, F6): un BACK a quota 1.9 si abbina sul miglior
    # availableToBack (2.0) e ne prende il prezzo; la liquidita' e' 3 su 5.
    sim = broker.place_bet(market_id="1.3", selection_id=9, side="BACK", price=1.9, size=5.0, event_key="e", customer_ref="c")
    live_fake = {"status": "SUCCESS", "marketId": "1.3", "instructionReports": [{"status": "SUCCESS", "betId": "LIVE-1", "sizeMatched": 3.0, "averagePriceMatched": 2.0}]}

    assert sim["status"] == live_fake["status"]
    assert set(sim.keys()) == set(live_fake.keys()) | {"simulated"}
    assert sim["instructionReports"][0]["status"] == live_fake["instructionReports"][0]["status"]
    assert sim["instructionReports"][0]["sizeMatched"] == live_fake["instructionReports"][0]["sizeMatched"]
