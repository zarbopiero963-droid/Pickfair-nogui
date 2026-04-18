import pytest

from simulation_broker import SimulationBroker


def _set_book(broker: SimulationBroker, *, market_id: str, selection_id: int, back_price: float, lay_price: float, size: float = 1000.0):
    broker.update_market_book(
        {
            "marketId": market_id,
            "runners": [
                {
                    "selectionId": selection_id,
                    "ex": {
                        "availableToBack": [{"price": float(back_price), "size": float(size)}],
                        "availableToLay": [{"price": float(lay_price), "size": float(size)}],
                    },
                }
            ],
        }
    )


def _find_open_position(snapshot: dict, market_id: str, selection_id: int) -> dict:
    for row in snapshot.get("open_positions", []):
        if row.get("market_id") == market_id and int(row.get("runner_id") or 0) == int(selection_id):
            return row
    return {}


@pytest.mark.integration
def test_simulation_weighted_average_fills_use_authoritative_position_ledger():
    broker = SimulationBroker(partial_fill_enabled=False)

    _set_book(broker, market_id="1.900", selection_id=77, back_price=1.99, lay_price=2.0)
    broker.place_bet(market_id="1.900", selection_id=77, side="BACK", price=3.0, size=40.0)

    _set_book(broker, market_id="1.900", selection_id=77, back_price=2.99, lay_price=3.0)
    broker.place_bet(market_id="1.900", selection_id=77, side="BACK", price=3.5, size=20.0)

    snap = broker.snapshot()
    pos = _find_open_position(snap, "1.900", 77)

    assert pos["open_side"] == "BACK"
    assert pos["open_size"] == pytest.approx(60.0)
    assert pos["avg_entry_price"] == pytest.approx((2.0 * 40.0 + 3.0 * 20.0) / 60.0)


@pytest.mark.integration
def test_simulation_partial_close_and_residual_exposure_are_ledger_driven():
    broker = SimulationBroker(partial_fill_enabled=False)

    _set_book(broker, market_id="1.901", selection_id=78, back_price=1.99, lay_price=2.0)
    broker.place_bet(market_id="1.901", selection_id=78, side="BACK", price=3.0, size=40.0)

    _set_book(broker, market_id="1.901", selection_id=78, back_price=2.99, lay_price=3.0)
    broker.place_bet(market_id="1.901", selection_id=78, side="BACK", price=3.5, size=20.0)

    _set_book(broker, market_id="1.901", selection_id=78, back_price=1.5, lay_price=1.6)
    broker.place_bet(market_id="1.901", selection_id=78, side="LAY", price=1.5, size=20.0)

    snap = broker.snapshot()
    pos = _find_open_position(snap, "1.901", 78)
    avg = (2.0 * 40.0 + 3.0 * 20.0) / 60.0
    realized_expected = (avg - 1.5) * 20.0

    assert pos["open_side"] == "BACK"
    assert pos["open_size"] == pytest.approx(40.0)
    assert pos["exposure"] == pytest.approx(40.0)
    assert pos["liability"] == pytest.approx(40.0)
    assert pos["realized_pnl"] == pytest.approx(realized_expected)


@pytest.mark.integration
def test_simulation_realized_and_unrealized_remain_separate_on_market_updates():
    broker = SimulationBroker(partial_fill_enabled=False)

    _set_book(broker, market_id="1.902", selection_id=79, back_price=1.99, lay_price=2.0)
    broker.place_bet(market_id="1.902", selection_id=79, side="BACK", price=2.2, size=50.0)

    _set_book(broker, market_id="1.902", selection_id=79, back_price=1.5, lay_price=1.6)
    broker.place_bet(market_id="1.902", selection_id=79, side="LAY", price=1.5, size=10.0)

    # refresh unrealized on residual BACK size=40 with mark lay=1.8
    _set_book(broker, market_id="1.902", selection_id=79, back_price=1.79, lay_price=1.8)

    snap = broker.snapshot()
    pos = _find_open_position(snap, "1.902", 79)

    assert pos["realized_pnl"] == pytest.approx((2.0 - 1.5) * 10.0)
    assert pos["unrealized_pnl"] == pytest.approx((2.0 - 1.8) * 40.0)
    assert snap["realized_pnl"] == pytest.approx(0.0)
    assert snap["unrealized_pnl"] == pytest.approx((2.0 - 1.8) * 40.0)


@pytest.mark.integration
def test_simulation_duplicate_fill_application_is_idempotent_for_balance_and_realization():
    broker = SimulationBroker(partial_fill_enabled=False)

    first = broker._apply_fill_to_position_ledger(
        fill_id="dup-1",
        market_id="1.903",
        selection_id=80,
        side="BACK",
        price=2.0,
        size=30.0,
    )
    balance_after_first = broker.state.balance

    second = broker._apply_fill_to_position_ledger(
        fill_id="dup-1",
        market_id="1.903",
        selection_id=80,
        side="BACK",
        price=2.0,
        size=30.0,
    )

    assert first["applied"] is True
    assert second["applied"] is False
    assert broker.state.balance == pytest.approx(balance_after_first)

    snap = broker.snapshot()
    pos = _find_open_position(snap, "1.903", 80)
    assert pos["open_size"] == pytest.approx(30.0)
    assert pos["realized_pnl"] == pytest.approx(0.0)
