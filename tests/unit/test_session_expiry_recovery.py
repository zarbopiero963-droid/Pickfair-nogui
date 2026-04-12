"""
Tests for Betfair session expiry detection, blocking, and bounded recovery.

Verifies:
- handle_session_expiry() marks session invalid and blocks live ops
- is_live_usable() returns False when session invalid
- ensure_connected() raises when session invalid and live requested
- bounded re-auth: succeeds on first attempt if password available
- bounded re-auth: stays blocked if re-auth fails
- max re-auth attempts not exceeded (loop protection)
- session invalid flag cleared after successful re-auth
"""

import pytest

from services.betfair_service import BetfairService


# ===========================================================================
# Stubs
# ===========================================================================

class _SettingsWithPassword:
    def __init__(self, password="test_pass"):
        self._pw = password

    def load_betfair_config(self):
        class Cfg:
            username = "user"
            app_key = "key"
            certificate = "cert"
            private_key = "pk"
        return Cfg()

    def load_password(self):
        return self._pw


class _SettingsNoPassword:
    def load_betfair_config(self):
        class Cfg:
            username = "user"
            app_key = "key"
            certificate = "cert"
            private_key = "pk"
        return Cfg()

    def load_password(self):
        return ""


class _FakeClient:
    def __init__(self, *, login_raises=False):
        self.session_token = "tok"
        self.session_expiry = ""
        self.connected = True
        self.login_calls = 0
        self._raises = login_raises

    def login(self, password):
        self.login_calls += 1
        if self._raises:
            raise RuntimeError("LOGIN_FAILED")
        self.session_token = "new_tok"
        self.connected = True
        return {"session_token": True, "expiry": ""}


def _make_service(password="test_pass"):
    return BetfairService(_SettingsWithPassword(password=password))


def _make_service_no_password():
    return BetfairService(_SettingsNoPassword())


# ===========================================================================
# Tests: session invalid state
# ===========================================================================

@pytest.mark.unit
@pytest.mark.guardrail
def test_handle_session_expiry_marks_invalid():
    svc = _make_service()
    svc.connected = True

    svc.handle_session_expiry("SESSION_EXPIRED")

    assert svc.is_session_invalid is True
    assert svc.connected is False


@pytest.mark.unit
@pytest.mark.guardrail
def test_is_live_usable_false_when_session_invalid():
    svc = _make_service()
    svc.connected = True
    svc.simulation_mode = False
    svc._session_invalid = True

    assert svc.is_live_usable() is False


@pytest.mark.unit
@pytest.mark.guardrail
def test_is_live_usable_true_when_connected_and_session_valid():
    svc = _make_service()
    svc.connected = True
    svc.simulation_mode = False
    svc._session_invalid = False

    from unittest.mock import MagicMock
    svc.client = MagicMock()

    assert svc.is_live_usable() is True


@pytest.mark.unit
@pytest.mark.guardrail
def test_ensure_connected_raises_if_session_invalid_and_live():
    svc = _make_service()
    svc._session_invalid = True
    svc.simulation_mode = False

    with pytest.raises(RuntimeError, match="LIVE_BLOCKED_SESSION_INVALID"):
        svc.ensure_connected(simulation_mode=False)


@pytest.mark.unit
@pytest.mark.guardrail
def test_ensure_connected_does_not_raise_if_simulation():
    svc = _make_service()
    svc._session_invalid = True
    svc.simulation_mode = True

    from unittest.mock import MagicMock
    svc.simulation_broker = MagicMock()
    svc.connected = True

    # Must not raise for simulation mode
    result = svc.ensure_connected(simulation_mode=True)
    assert result is not None


# ===========================================================================
# Tests: re-auth recovery
# ===========================================================================

@pytest.mark.unit
@pytest.mark.guardrail
def test_handle_session_expiry_recovers_if_password_available(monkeypatch):
    svc = _make_service(password="secret")

    fake_client = _FakeClient()

    def fake_connect_live(password=None, force=False):
        fake_client.login(password)
        svc.client = fake_client
        svc.connected = True
        svc.simulation_mode = False
        return {"connected": True}

    monkeypatch.setattr(svc, "_connect_live", fake_connect_live)

    result = svc.handle_session_expiry("SESSION_EXPIRED")

    assert result["recovered"] is True
    assert result["reauth_attempted"] is True
    assert svc.is_session_invalid is False
    assert svc._reauth_attempts == 0  # reset after success


@pytest.mark.unit
@pytest.mark.guardrail
def test_handle_session_expiry_stays_blocked_if_reauth_fails(monkeypatch):
    svc = _make_service(password="secret")

    def fake_connect_live(password=None, force=False):
        raise RuntimeError("CONNECTION_FAILED")

    monkeypatch.setattr(svc, "_connect_live", fake_connect_live)

    result = svc.handle_session_expiry("SESSION_EXPIRED")

    assert result["recovered"] is False
    assert result["reauth_attempted"] is True
    # Must remain blocked
    assert svc.is_session_invalid is True


@pytest.mark.unit
@pytest.mark.guardrail
def test_handle_session_expiry_stays_blocked_if_no_password():
    svc = _make_service_no_password()

    result = svc.handle_session_expiry("SESSION_EXPIRED")

    assert result["recovered"] is False
    assert result["reauth_attempted"] is False
    assert svc.is_session_invalid is True


@pytest.mark.unit
@pytest.mark.guardrail
def test_handle_session_expiry_max_attempts_not_exceeded(monkeypatch):
    """After max re-auth attempts, further expiry calls must not retry."""
    svc = _make_service(password="secret")

    reauth_calls = []

    def fake_connect_live(password=None, force=False):
        reauth_calls.append(1)
        raise RuntimeError("FAIL")

    monkeypatch.setattr(svc, "_connect_live", fake_connect_live)

    # First call: attempts re-auth (fails)
    svc.handle_session_expiry("SESSION_EXPIRED")
    first_count = len(reauth_calls)

    # Second call: must NOT attempt re-auth (max exhausted)
    svc.handle_session_expiry("SESSION_EXPIRED_AGAIN")
    second_count = len(reauth_calls)

    assert first_count == 1, "first call must attempt re-auth"
    assert second_count == 1, "second call must NOT retry (max exhausted)"
    assert svc.is_session_invalid is True
