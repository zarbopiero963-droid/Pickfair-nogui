from simulation_broker import SimulationBroker, SimulationState


def _book(market_id="1.100", selection_id=10, lay_price=2.0, lay_size=10.0):
    return {
        "marketId": market_id,
        "runners": [
            {
                "selectionId": selection_id,
                "ex": {
                    "availableToBack": [{"price": 1.99, "size": 10.0}],
                    "availableToLay": [{"price": lay_price, "size": lay_size}],
                },
            }
        ],
    }


def test_load_from_dict_invalid_numeric_values_do_not_crash():
    state = SimulationState(starting_balance=1000.0, commission_pct=4.5)
    state.load_from_dict(
        {
            "starting_balance": None,
            "balance": "oops",
            "orders": {
                "b1": {
                    "bet_id": "b1",
                    "market_id": "1.1",
                    "selection_id": "bad",
                    "price": "nan?",
                    "size": None,
                }
            },
            "position_ledgers": {
                "1.1::x": {
                    "market_id": "1.1",
                    "runner_id": "bad",
                    "snapshot": {"open_side": "BACK", "open_size": "x", "avg_entry_price": "y"},
                }
            },
        }
    )
    assert state.balance == state.starting_balance
    assert state.orders["b1"].selection_id == 0
    assert state.orders["b1"].size == 0.0
    assert "1.1::x" not in state.position_ledgers


def test_partial_fill_then_cancel_keeps_state_consistent():
    broker = SimulationBroker(starting_balance=1000.0, partial_fill_enabled=True, consume_liquidity=True)
    broker.update_market_book(_book(lay_size=2.0))

    placed = broker.place_bet(market_id="1.100", selection_id=10, side="BACK", price=2.0, size=5.0)
    bet_id = placed["instructionReports"][0]["betId"]

    order = broker.state.orders[bet_id]
    assert order.matched_size == 2.0
    assert order.status == "EXECUTABLE"

    cancel = broker.cancel_orders(market_id="1.100", instructions=[{"betId": bet_id}])
    assert cancel["instructionReports"][0]["status"] == "SUCCESS"

    current = broker.list_current_orders(market_ids=["1.100"])["currentOrders"][0]
    assert current["status"] == "CANCELLED"
    assert current["sizeMatched"] == 2.0
    assert current["sizeRemaining"] == 3.0


def test_place_orders_with_none_selection_id_does_not_crash_and_fails_match():
    broker = SimulationBroker()
    broker.update_market_book(_book())

    out = broker.place_orders(
        market_id="1.100",
        instructions=[{"selectionId": None, "side": "BACK", "price": 2.0, "size": 1.0}],
    )

    report = out["instructionReports"][0]
    assert report["status"] == "FAILURE"
    order = broker.state.orders[report["betId"]]
    assert order.selection_id == 0
    assert order.status == "EXECUTABLE"


def test_place_orders_with_malformed_inputs_do_not_crash_or_move_funds():
    broker = SimulationBroker()
    broker.update_market_book(_book())
    before = broker.get_account_funds()

    out = broker.place_orders(
        market_id="1.100",
        instructions=[
            {"selectionId": "bad", "side": "BACK", "price": 2.0, "size": 1.0},
            {"selection_id": None, "selectionId": "bad", "side": "BACK", "price": "bad", "size": 1.0},
            {"selectionId": 10, "side": "BACK", "price": 2.0, "size": "bad"},
            {"selectionId": 10, "side": "BACK", "price": 2.0, "stake": "bad"},
        ],
    )

    for report in out["instructionReports"]:
        assert report["status"] == "FAILURE"
        order = broker.state.orders[report["betId"]]
        assert order.matched_size == 0.0
        assert order.status == "EXECUTABLE"

    after = broker.get_account_funds()
    assert after["available"] == before["available"]
    assert after["exposure"] == before["exposure"]
