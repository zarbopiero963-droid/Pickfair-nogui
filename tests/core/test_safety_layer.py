import math

import pytest

from core.safety_layer import (
    MarketSanityError,
    PayloadValidationError,
    RiskInvariantError,
    SafetyLayer,
    assert_live_gate_or_refuse,
)


def _valid_quick_bet_payload():
    return {
        "market_id": "1.234",
        "selection_id": 123,
        "bet_type": "BACK",
        "price": 2.5,
        "stake": 5.0,
    }


def test_validate_quick_bet_request_happy_path_returns_true():
    sl = SafetyLayer()
    assert sl.validate_quick_bet_request(_valid_quick_bet_payload()) is True


def test_validate_quick_bet_request_missing_required_field_fails_closed():
    sl = SafetyLayer()
    payload = _valid_quick_bet_payload()
    payload.pop("market_id")

    with pytest.raises(PayloadValidationError) as exc:
        sl.validate_quick_bet_request(payload)
    assert "market_id" in str(exc.value)


def test_validate_quick_bet_request_invalid_numeric_boundaries_fail_closed():
    sl = SafetyLayer()

    with pytest.raises(RiskInvariantError):
        sl.validate_quick_bet_request({**_valid_quick_bet_payload(), "stake": -1.0})

    with pytest.raises(MarketSanityError):
        sl.validate_quick_bet_request({**_valid_quick_bet_payload(), "price": 1.0})

    for bad_price in (math.inf, -math.inf, math.nan):
        with pytest.raises(MarketSanityError):
            sl.validate_quick_bet_request({**_valid_quick_bet_payload(), "price": bad_price})

    for bad_stake in (math.inf, -math.inf, math.nan):
        with pytest.raises(RiskInvariantError):
            sl.validate_quick_bet_request({**_valid_quick_bet_payload(), "stake": bad_stake})

    with pytest.raises(PayloadValidationError) as exc:
        sl.validate_quick_bet_request({**_valid_quick_bet_payload(), "price": None})
    assert "price" in str(exc.value)


def test_assert_live_gate_or_refuse_is_deterministic_and_fail_closed():
    kwargs = dict(
        execution_mode="LIVE",
        live_enabled=True,
        live_readiness_ok=False,
        kill_switch=False,
    )

    decision1 = assert_live_gate_or_refuse(**kwargs)
    decision2 = assert_live_gate_or_refuse(**kwargs)

    assert decision1 == decision2
    assert decision1.allowed is False
    assert decision1.effective_execution_mode == "SIMULATION"
    assert decision1.reason_code == "live_readiness_not_ok"


def test_assert_live_gate_or_refuse_input_mutation_does_not_change_result():
    mode = ["live"]
    decision = assert_live_gate_or_refuse(
        execution_mode=mode[0],
        live_enabled=True,
        live_readiness_ok=True,
        kill_switch=False,
    )

    mode[0] = "SIMULATION"

    assert decision.allowed is True
    assert decision.effective_execution_mode == "LIVE"
    assert decision.reason_code == "live_allowed"


def test_safety_layer_instances_do_not_leak_watchdog_state():
    first = SafetyLayer()
    second = SafetyLayer()

    first.register_watchdog("core-loop", timeout_sec=0.5)

    assert "core-loop" in first.get_watchdog_status()
    assert "core-loop" not in second.get_watchdog_status()


def test_watchdog_stale_signal_transitions_to_triggered_without_private_internals():
    now = [100.0]
    sl = SafetyLayer(clock=lambda: now[0])
    sl.register_watchdog("pulse", timeout_sec=0.5)
    now[0] = 101.0
    sl.check_watchdogs()

    status = sl.get_watchdog_status()["pulse"]
    assert status["triggered"] is True
    assert "timeout" in status["last_error"].lower()
