import pytest

from core import trading_engine
from order_manager import LIFECYCLE_CONTRACT, ORDER_STATUS_EVENT_MAP, TERMINAL_LIFECYCLE_EVENTS


@pytest.mark.integration
def test_lifecycle_contract_consistency():
    accepted = LIFECYCLE_CONTRACT["ACCEPTED"]
    filled = LIFECYCLE_CONTRACT["FILLED"]
    failed = LIFECYCLE_CONTRACT["FAILED"]
    ambiguous = LIFECYCLE_CONTRACT["AMBIGUOUS"]

    # same semantics across modules: accepted == submitted/placed, not terminal
    assert accepted["order_status"] == "PLACED"
    assert accepted["trading_engine_status"] == trading_engine.STATUS_ACCEPTED_FOR_PROCESSING
    assert accepted["event"] == "QUICK_BET_ACCEPTED"
    assert accepted["terminal"] is False

    # no mismatch ACCEPTED vs PLACED vs SUCCESS
    assert accepted["order_status"] != "SUCCESS"
    assert filled["outcome"] == trading_engine.OUTCOME_SUCCESS
    assert trading_engine._STATUS_TO_OUTCOME[trading_engine.STATUS_COMPLETED] == trading_engine.OUTCOME_SUCCESS

    # terminal states consistent everywhere
    assert filled["trading_engine_status"] == trading_engine.STATUS_COMPLETED
    assert failed["trading_engine_status"] == trading_engine.STATUS_FAILED
    assert ambiguous["trading_engine_status"] == trading_engine.STATUS_AMBIGUOUS
    assert trading_engine.STATUS_COMPLETED in trading_engine._TERMINAL_STATES
    assert trading_engine.STATUS_FAILED in trading_engine._TERMINAL_STATES
    assert trading_engine.STATUS_AMBIGUOUS in trading_engine._TERMINAL_STATES

    # event mapping consistent
    assert ORDER_STATUS_EVENT_MAP["PLACED"] == "QUICK_BET_ACCEPTED"
    assert ORDER_STATUS_EVENT_MAP["PARTIALLY_MATCHED"] == "QUICK_BET_PARTIAL"
    assert ORDER_STATUS_EVENT_MAP["MATCHED"] == "QUICK_BET_FILLED"
    assert ORDER_STATUS_EVENT_MAP["FAILED"] == "QUICK_BET_FAILED"
    assert ORDER_STATUS_EVENT_MAP["AMBIGUOUS"] == "QUICK_BET_AMBIGUOUS"

    assert "QUICK_BET_FILLED" in TERMINAL_LIFECYCLE_EVENTS
    assert "QUICK_BET_FAILED" in TERMINAL_LIFECYCLE_EVENTS
    assert "QUICK_BET_AMBIGUOUS" in TERMINAL_LIFECYCLE_EVENTS
    assert "QUICK_BET_ACCEPTED" not in TERMINAL_LIFECYCLE_EVENTS
    assert "QUICK_BET_PARTIAL" not in TERMINAL_LIFECYCLE_EVENTS
