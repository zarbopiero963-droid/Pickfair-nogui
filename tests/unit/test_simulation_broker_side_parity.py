"""P15 / #426: the simulation order boundary must agree with the live client."""

import pytest

from simulation_broker import SimulationBroker


@pytest.mark.parametrize("side", [None, "", "  ", "SELL", "backk", 0, False])
def test_invalid_side_cannot_create_a_simulated_order(side):
    broker = SimulationBroker()
    before = broker.get_account_funds()

    with pytest.raises(RuntimeError, match="^INVALID_SIDE$"):
        broker.place_bet(
            market_id="1.100", selection_id=10, side=side,
            price=2.0, size=1.0,
        )

    assert broker.state.orders == {}
    assert broker.get_account_funds() == before


@pytest.mark.parametrize("side,expected", [("back", "BACK"), (" Lay ", "LAY")])
def test_valid_side_is_normalized_before_recording(side, expected):
    broker = SimulationBroker()

    result = broker.place_bet(
        market_id="1.100", selection_id=10, side=side,
        price=2.0, size=1.0,
    )

    bet_id = result["instructionReports"][0]["betId"]
    assert broker.state.orders[bet_id].side == expected


@pytest.mark.parametrize("instruction", [
    {"selectionId": 10, "price": 2.0, "size": 1.0},
    {"selectionId": 10, "side": None, "price": 2.0, "size": 1.0},
    {"selectionId": 10, "side": "SELL", "price": 2.0, "size": 1.0},
    {"selectionId": 10, "side": "LAY", "bet_type": "BACK", "price": 2.0, "size": 1.0},
    {"selectionId": 10, "side": "SELL", "bet_type": "BACK", "price": 2.0, "size": 1.0},
])
def test_invalid_batch_instruction_has_no_order_or_implicit_back(instruction):
    broker = SimulationBroker()
    response = broker.place_orders(
        market_id="1.100", instructions=[instruction],
    )

    assert response["instructionReports"] == [{
        "status": "FAILURE", "betId": "", "sizeMatched": 0.0,
        "averagePriceMatched": 0.0,
    }]
    assert broker.state.orders == {}


@pytest.mark.parametrize("instruction,expected", [
    ({"side": "", "bet_type": "BACK"}, "BACK"),
    ({"side": "  ", "bet_type": " lay "}, "LAY"),
    ({"side": None, "bet_type": "BACK"}, "BACK"),
    ({"side": "lay", "bet_type": "LAY"}, "LAY"),
])
def test_batch_accepts_one_unambiguous_side_from_either_alias(instruction, expected):
    broker = SimulationBroker()
    response = broker.place_orders(market_id="1.100", instructions=[{
        "selectionId": 10, "price": 2.0, "size": 1.0, **instruction,
    }])

    order_id = response["instructionReports"][0]["betId"]
    assert order_id in broker.state.orders
    assert broker.state.orders[order_id].side == expected


def test_invalid_batch_leg_does_not_block_independent_valid_leg():
    broker = SimulationBroker()
    response = broker.place_orders(market_id="1.100", instructions=[
        {"selectionId": 10, "side": "SELL", "price": 2.0, "size": 1.0},
        {"selectionId": 10, "side": " lay ", "price": 2.0, "size": 1.0},
    ])

    assert [report["status"] for report in response["instructionReports"]] == [
        "FAILURE", "FAILURE",
    ]  # No book is seeded, so a valid LAY remains unmatched.
    assert response["instructionReports"][0]["betId"] == ""
    valid_id = response["instructionReports"][1]["betId"]
    assert valid_id in broker.state.orders
    assert broker.state.orders[valid_id].side == "LAY"
    assert len(broker.state.orders) == 1
