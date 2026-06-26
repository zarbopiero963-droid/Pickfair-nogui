"""Tests for SimulationBroker.replace_orders (B8 / Fase 1.5, SIM/LIVE parity)."""

import pytest

from simulation_broker import SimOrder, SimulationBroker


def _executable_order(bet_id="OLD", market="1.100", price=2.0, size=5.0):
    return SimOrder(
        bet_id=bet_id, market_id=market, selection_id=7, side="BACK",
        price=price, size=size, status="EXECUTABLE",
    )


@pytest.mark.unit
def test_replace_changes_price_and_returns_new_bet_id():
    b = SimulationBroker()
    b.state.orders["OLD"] = _executable_order()

    out = b.replace_orders(market_id="1.100", bet_id="OLD", new_price=3.5)

    rep = out["instructionReports"][0]
    assert rep["status"] == "SUCCESS"
    new_id = rep["betId"]
    # Betfair semantics: old order cancelled, a NEW order placed at the new price.
    assert new_id and new_id != "OLD"
    assert b.state.orders["OLD"].status == "CANCELLED"
    assert new_id in b.state.orders
    assert b.state.orders[new_id].price == 3.5


@pytest.mark.unit
def test_replace_unknown_bet_returns_failure_report():
    b = SimulationBroker()
    out = b.replace_orders(market_id="1.100", bet_id="NOPE", new_price=3.0)
    assert out["instructionReports"][0]["status"] == "FAILURE"


@pytest.mark.unit
def test_replace_non_executable_returns_failure_report():
    b = SimulationBroker()
    b.state.orders["DONE"] = _executable_order(bet_id="DONE")
    b.state.orders["DONE"].status = "EXECUTION_COMPLETE"
    out = b.replace_orders(market_id="1.100", bet_id="DONE", new_price=3.0)
    assert out["instructionReports"][0]["status"] == "FAILURE"


@pytest.mark.unit
def test_replace_invalid_price_returns_failure_report():
    b = SimulationBroker()
    b.state.orders["OLD"] = _executable_order()
    out = b.replace_orders(market_id="1.100", bet_id="OLD", new_price=1.0)
    assert out["instructionReports"][0]["status"] == "FAILURE"
    # The old order is left untouched on an invalid replace.
    assert b.state.orders["OLD"].status == "EXECUTABLE"


@pytest.mark.unit
def test_replace_response_shape_matches_live_contract():
    # order_manager reads instructionReports[0].{status,betId}; match that shape.
    b = SimulationBroker()
    b.state.orders["OLD"] = _executable_order()
    out = b.replace_orders(market_id="1.100", bet_id="OLD", new_price=3.0)
    assert {"status", "instructionReports"}.issubset(out)
    assert "status" in out["instructionReports"][0]
    assert "betId" in out["instructionReports"][0]


@pytest.mark.unit
def test_replace_market_mismatch_returns_failure_and_leaves_order():
    # A valid bet id with the WRONG market must not replace the order (parity
    # with the live API and the sim cancel path).
    b = SimulationBroker()
    b.state.orders["OLD"] = _executable_order(market="1.100")
    out = b.replace_orders(market_id="9.999", bet_id="OLD", new_price=3.0)
    assert out["instructionReports"][0]["status"] == "FAILURE"
    assert b.state.orders["OLD"].status == "EXECUTABLE"
