"""
Tests for circuit breaker runtime wiring.

Verifies that BetfairClient and TradingEngine correctly integrate with
CircuitBreaker so that repeated failures open the circuit and subsequent
calls are rejected without hitting the external service.
"""

from __future__ import annotations

from typing import Any, Dict, Optional
from unittest.mock import MagicMock, patch

import pytest

from circuit_breaker import CircuitBreaker, State


# ===========================================================================
# BetfairClient wiring
# ===========================================================================

class _FakeSession:
    """Minimal requests.Session stand-in."""

    def __init__(self, *, raise_on_post=None, status_code=200, response_body=None):
        self._raise = raise_on_post
        self._status = status_code
        self._body = response_body or [{"jsonrpc": "2.0", "result": {"ok": True}, "id": 1}]

    def post(self, url, **kwargs):
        if self._raise is not None:
            raise self._raise
        resp = MagicMock()
        resp.status_code = self._status
        resp.json.return_value = self._body
        resp.raise_for_status = lambda: None
        return resp


def _make_client(session=None) -> Any:
    from betfair_client import BetfairClient
    client = BetfairClient(
        username="u",
        app_key="app",
        cert_pem="/tmp/c.pem",
        key_pem="/tmp/k.pem",
        session=session or _FakeSession(),
        max_retries=0,
    )
    client.session_token = "tok"
    return client


@pytest.mark.unit
def test_betfair_client_has_api_breaker():
    client = _make_client()
    assert isinstance(client._api_breaker, CircuitBreaker)


@pytest.mark.unit
def test_betfair_client_breaker_open_raises_immediately():
    client = _make_client()
    client._api_breaker.state = State.OPEN
    client._api_breaker.opened_at = 1e9  # far future effectively

    import time
    client._api_breaker.opened_at = time.time()

    with pytest.raises(RuntimeError, match="CIRCUIT_BREAKER_OPEN"):
        client._post_jsonrpc("http://x", "SomeMethod", {})


@pytest.mark.unit
def test_betfair_client_network_failures_trip_breaker():
    from requests.exceptions import Timeout

    session = _FakeSession(raise_on_post=Timeout())
    client = _make_client(session=session)
    # Threshold is 5; fire 5 failures
    for _ in range(5):
        try:
            client._post_jsonrpc("http://x", "SomeMethod", {})
        except RuntimeError:
            pass

    assert client._api_breaker.is_open(), "breaker must open after 5 failures"


@pytest.mark.unit
def test_betfair_client_session_expired_does_not_trip_breaker():
    """SESSION_EXPIRED is an auth issue, not a network failure — must NOT trip CB."""
    body = [{"jsonrpc": "2.0", "error": "INVALID_SESSION", "id": 1}]
    session = _FakeSession(response_body=body)
    client = _make_client(session=session)

    for _ in range(6):  # more than threshold
        client.session_token = "tok"  # reset token so NOT_AUTHENTICATED doesn't fire first
        try:
            client._post_jsonrpc("http://x", "SomeMethod", {})
        except RuntimeError as e:
            assert "SESSION_EXPIRED" in str(e)

    assert not client._api_breaker.is_open(), "SESSION_EXPIRED must NOT trip CB"


@pytest.mark.unit
def test_betfair_client_success_resets_failure_count():
    from requests.exceptions import Timeout

    session = _FakeSession(raise_on_post=Timeout())
    client = _make_client(session=session)
    # Record 2 failures (below threshold of 5)
    for _ in range(2):
        try:
            client._post_jsonrpc("http://x", "SomeMethod", {})
        except RuntimeError:
            pass

    assert client._api_breaker.failures == 2

    # Now simulate success
    good_session = _FakeSession()
    client.session = good_session
    client._post_jsonrpc("http://x", "SomeMethod", {})
    assert client._api_breaker.failures == 0, "success must reset failure counter"


# ===========================================================================
# TradingEngine wiring
# ===========================================================================

class _MockBus:
    def subscribe(self, *a, **kw): pass
    def publish(self, *a, **kw): pass


class _MockDB:
    def insert_order(self, *a, **kw): return None
    def insert_audit_event(self, *a, **kw): return None
    def get_settings(self): return {}
    def get_pending_sagas(self): return []


class _MockClientGetter:
    def __call__(self): return None


def _make_engine():
    from core.trading_engine import TradingEngine
    engine = TradingEngine(
        bus=_MockBus(),
        db=_MockDB(),
        client_getter=_MockClientGetter(),
        executor=None,
    )
    return engine


@pytest.mark.unit
def test_trading_engine_has_order_submission_breaker():
    engine = _make_engine()
    assert isinstance(engine._order_submission_breaker, CircuitBreaker)


@pytest.mark.unit
def test_trading_engine_breaker_open_blocks_live_order():
    import time
    engine = _make_engine()
    engine._order_submission_breaker.state = State.OPEN
    engine._order_submission_breaker.opened_at = time.time()

    # Set up a fake runtime that allows LIVE
    rt = MagicMock()
    rt.get_effective_execution_mode.return_value = "LIVE"
    rt.is_live_allowed.return_value = True
    engine.runtime_controller = rt

    fake_client = MagicMock()
    fake_client.place_order.return_value = {"status": "ok"}
    engine.betfair_client = fake_client

    with pytest.raises(RuntimeError, match="ORDER_SUBMISSION_CIRCUIT_BREAKER_OPEN"):
        engine._submit_to_order_path(
            _make_ctx(engine),
            {"market_id": "1.1", "selection_id": 123, "side": "BACK", "price": 2.0, "size": 10.0},
        )

    # live_client.place_order must NOT have been called
    fake_client.place_order.assert_not_called()


@pytest.mark.unit
def test_trading_engine_order_failures_trip_breaker():
    engine = _make_engine()

    rt = MagicMock()
    rt.get_effective_execution_mode.return_value = "LIVE"
    rt.is_live_allowed.return_value = True
    engine.runtime_controller = rt

    fake_client = MagicMock()
    fake_client.place_order.side_effect = RuntimeError("TIMEOUT")
    engine.betfair_client = fake_client

    ctx = _make_ctx(engine)
    payload = {"market_id": "1.1", "selection_id": 123, "side": "BACK", "price": 2.0, "size": 10.0}

    for _ in range(3):
        try:
            engine._submit_to_order_path(ctx, payload)
        except RuntimeError:
            pass

    assert engine._order_submission_breaker.is_open(), "breaker must open after 3 failures"


@pytest.mark.unit
def test_trading_engine_successful_order_resets_breaker_failures():
    engine = _make_engine()

    rt = MagicMock()
    rt.get_effective_execution_mode.return_value = "LIVE"
    rt.is_live_allowed.return_value = True
    engine.runtime_controller = rt

    fake_client = MagicMock()
    fake_client.place_order.return_value = {"status": "ok"}
    engine.betfair_client = fake_client

    # Manually put 2 failures on the breaker
    engine._order_submission_breaker.failures = 2

    ctx = _make_ctx(engine)
    payload = {"market_id": "1.1", "selection_id": 123, "side": "BACK", "price": 2.0, "size": 10.0}
    engine._submit_to_order_path(ctx, payload)

    assert engine._order_submission_breaker.failures == 0, \
        "successful order must reset breaker failure count"


@pytest.mark.unit
def test_trading_engine_ok_false_non_session_trips_breaker():
    """Regression: ok=False that is NOT a session error must still be counted as a
    breaker failure. Previously these fell through to record_success(), defeating
    the circuit breaker for exchange/network errors reported via ok=False."""
    engine = _make_engine()

    rt = MagicMock()
    rt.get_effective_execution_mode.return_value = "LIVE"
    rt.is_live_allowed.return_value = True
    engine.runtime_controller = rt

    fake_client = MagicMock()
    # place_order returns ok=False with a non-session error (e.g. market suspended)
    fake_client.place_order.return_value = {"ok": False, "error": "MARKET_SUSPENDED"}
    engine.betfair_client = fake_client

    ctx = _make_ctx(engine)
    payload = {"market_id": "1.1", "selection_id": 123, "side": "BACK", "price": 2.0, "size": 10.0}

    breaker = engine._order_submission_breaker
    threshold = breaker.max_failures

    for _ in range(threshold):
        result = engine._submit_to_order_path(ctx, payload)
        assert result == {"ok": False, "error": "MARKET_SUSPENDED"}, \
            "ok=False result must be returned to caller"

    assert breaker.is_open(), \
        "breaker must open after repeated ok=False non-session failures"


@pytest.mark.unit
def test_trading_engine_ok_false_non_session_returns_result_to_caller():
    """ok=False non-session failures count as breaker failures but do NOT raise —
    the error dict is returned so the caller can inspect it."""
    engine = _make_engine()

    rt = MagicMock()
    rt.get_effective_execution_mode.return_value = "LIVE"
    rt.is_live_allowed.return_value = True
    engine.runtime_controller = rt

    fake_client = MagicMock()
    fake_client.place_order.return_value = {"ok": False, "error": "NETWORK_TIMEOUT"}
    engine.betfair_client = fake_client

    result = engine._submit_to_order_path(_make_ctx(engine),
        {"market_id": "1.2", "selection_id": 99, "side": "LAY", "price": 3.0, "size": 5.0})

    assert result == {"ok": False, "error": "NETWORK_TIMEOUT"}
    assert engine._order_submission_breaker.failures == 1


@pytest.mark.unit
def test_trading_engine_ok_true_still_records_success():
    """ok=True responses still record a breaker success (no regression)."""
    engine = _make_engine()

    rt = MagicMock()
    rt.get_effective_execution_mode.return_value = "LIVE"
    rt.is_live_allowed.return_value = True
    engine.runtime_controller = rt

    fake_client = MagicMock()
    fake_client.place_order.return_value = {"ok": True, "bet_id": "BET-1"}
    engine.betfair_client = fake_client

    # Seed 2 failures first
    engine._order_submission_breaker.failures = 2

    engine._submit_to_order_path(_make_ctx(engine),
        {"market_id": "1.3", "selection_id": 77, "side": "BACK", "price": 2.0, "size": 10.0})

    assert engine._order_submission_breaker.failures == 0, \
        "ok=True must still call record_success and reset failure count"


# ===========================================================================
# Helpers
# ===========================================================================

def _make_ctx(engine: Any) -> Any:
    """Create a minimal _ExecutionContext using the engine's factory."""
    from core.trading_engine import _ExecutionContext
    import uuid
    return _ExecutionContext(
        correlation_id=str(uuid.uuid4()),
        customer_ref="test-ref",
        created_at=0.0,
    )
