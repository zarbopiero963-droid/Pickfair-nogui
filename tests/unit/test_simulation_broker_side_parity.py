"""P15 / #426: the simulation order boundary must agree with the live client."""

import pytest

from cashout_executor import CashoutExecutor
from core.order_router import OrderRouter
from order_manager import OrderManager
from simulation_broker import SimulationBroker


class _LooksLikeBack:
    def __str__(self):
        return "BACK"


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
    {"selectionId": 10, "side": _LooksLikeBack(), "price": 2.0, "size": 1.0},
    {"selectionId": 10, "side": 0, "bet_type": "BACK", "price": 2.0, "size": 1.0},
    {"selectionId": 10, "side": _LooksLikeBack(), "bet_type": "BACK", "price": 2.0, "size": 1.0},
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


def test_real_paper_callers_pass_valid_sides_and_reject_missing_side():
    """Exercise the actual router, manager and cashout with a real PAPER broker."""
    class SagaDB:
        def __init__(self):
            self.sagas = {}
            self.by_logical = {}

        def create_order_saga(self, **kwargs):
            self.sagas[kwargs["customer_ref"]] = kwargs
            self.by_logical[kwargs["logical_key"]] = kwargs

        def get_order_saga(self, customer_ref):
            return self.sagas.get(customer_ref)

        def get_order_saga_by_logical_key(self, logical_key):
            return self.by_logical.get(logical_key)

        def update_order_saga(self, **kwargs):
            self.sagas[kwargs["customer_ref"]].update(kwargs)

    class PaperService:
        def get_client(self):
            return broker

        def is_simulation_mode(self):
            return True

    class Bus:
        def __init__(self):
            self.events = []

        def publish(self, name, payload):
            self.events.append((name, payload))

    broker = SimulationBroker()
    router = OrderRouter(PaperService())
    routed = router.place({
        "market_id": "1.1", "selection_id": 7, "bet_type": "LAY",
        "price": 2.0, "stake": 1.0,
    })
    assert routed["placed"] is True
    assert broker.state.orders[routed["bet_id"]].side == "LAY"

    before = len(broker.state.orders)
    with pytest.raises(RuntimeError, match="^INVALID_SIDE$"):
        router.place({
            "market_id": "1.1", "selection_id": 8, "bet_type": None,
            "price": 2.0, "stake": 1.0,
        })
    assert len(broker.state.orders) == before

    broker.update_market_book({
        "marketId": "1.1", "runners": [{"selectionId": 9, "ex": {
            "availableToLay": [{"price": 1.9, "size": 10.0}],
            "availableToBack": [{"price": 1.8, "size": 10.0}],
        }}],
    })
    manager = OrderManager(db=SagaDB(), client_getter=lambda: broker,
                           sleep_fn=lambda _: None)
    managed = manager.place_order({
        "market_id": "1.1", "selection_id": 9, "bet_type": "BACK",
        "price": 2.0, "stake": 1.0, "simulation_mode": True,
        "customer_ref": "P15-ROUTE",
    })
    assert managed["ok"] is True
    assert broker.state.orders[managed["bet_id"]].side == "BACK"

    bus = Bus()
    CashoutExecutor(bus, router).on_cmd_execute_cashout({
        "market_id": "1.1", "selection_id": 9, "side": "LAY",
        "price": 1.8, "stake": 1.0, "green_up": 0.0,
    })
    assert len(bus.events) == 1
    assert bus.events[0][0] == "CASHOUT_SUCCESS"
    assert broker.state.orders[bus.events[0][1]["bet_id"]].side == "LAY"
