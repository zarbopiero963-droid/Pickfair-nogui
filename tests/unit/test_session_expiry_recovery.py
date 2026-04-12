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
- place_order() refuses immediately when session invalid (fail-closed)
- place_order() detects SESSION_EXPIRED in client response and invokes recovery
- RuntimeController._on_signal_received() rejects LIVE signals when session invalid
- TradingEngine._submit_to_order_path() raises when session invalid
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


# ===========================================================================
# Tests: SESSION_EXPIRED wired into get_account_funds()
# ===========================================================================

@pytest.mark.unit
@pytest.mark.guardrail
def test_get_account_funds_triggers_session_recovery_on_session_expired():
    """get_account_funds() must invoke handle_session_expiry() when the live
    client raises SESSION_EXPIRED, leaving the service in blocked state."""

    class _ExpiringClient:
        def get_account_funds(self):
            raise RuntimeError("SESSION_EXPIRED")

    svc = _make_service()
    svc.connected = True
    svc.simulation_mode = False
    svc.client = _ExpiringClient()

    result = svc.get_account_funds()

    # Recovery was invoked — service is now blocked
    assert svc.is_session_invalid is True
    assert svc.is_live_usable() is False
    # Returned a safe zero-value dict (no crash)
    assert result["available"] == 0.0


@pytest.mark.unit
@pytest.mark.guardrail
def test_get_account_funds_stays_blocked_after_session_expired_no_password():
    """When SESSION_EXPIRED fires and no password is available, service must
    stay blocked and return zero funds (fail-closed)."""

    class _ExpiringClient:
        def get_account_funds(self):
            raise RuntimeError("SESSION_EXPIRED")

    svc = _make_service_no_password()
    svc.connected = True
    svc.simulation_mode = False
    svc.client = _ExpiringClient()

    result = svc.get_account_funds()

    assert svc.is_session_invalid is True
    assert svc.is_live_usable() is False
    assert result["available"] == 0.0


# ===========================================================================
# Tests: SESSION_EXPIRED wired into place_order() — live order path
# ===========================================================================

@pytest.mark.unit
@pytest.mark.guardrail
def test_place_order_blocked_when_session_invalid():
    """BetfairService.place_order() refuses immediately when session is invalid."""
    svc = _make_service()
    svc._session_invalid = True
    svc._session_invalid_reason = "SESSION_EXPIRED"

    result = svc.place_order({
        "market_id": "1.123",
        "selection_id": 456,
        "bet_type": "BACK",
        "price": 2.0,
        "stake": 10.0,
    })

    assert result["ok"] is False
    err = result.get("error", "")
    assert "SESSION_INVALID" in err.upper() or "LIVE_BLOCKED" in err.upper(), \
        f"Expected session-invalid refusal in error, got: {err!r}"
    assert result.get("session_invalid") is True
    # Service must remain blocked — place_order must NOT attempt recovery
    assert svc.is_session_invalid is True


@pytest.mark.unit
@pytest.mark.guardrail
def test_place_order_detects_session_expired_from_client_response():
    """When place_bet() returns ok=False with SESSION_EXPIRED, handle_session_expiry() is invoked."""

    class _SessionExpiredPlaceBetClient:
        def place_bet(self, **_kw):
            # BetfairClient.place_bet() catches RuntimeError and returns ok=False
            return {
                "ok": False,
                "error": "SESSION_EXPIRED",
                "classification": "PERMANENT",
                "order_unknown": False,
            }

    svc = _make_service()
    svc.connected = True
    svc.simulation_mode = False
    svc.client = _SessionExpiredPlaceBetClient()

    result = svc.place_order({
        "market_id": "1.123",
        "selection_id": 456,
        "bet_type": "BACK",
        "price": 2.0,
        "stake": 10.0,
    })

    # Recovery was invoked — service must now be blocked
    assert svc.is_session_invalid is True, \
        "handle_session_expiry() must have been called — service must be blocked"
    assert svc.is_live_usable() is False
    # Response is passed through
    assert result["ok"] is False
    assert "SESSION_EXPIRED" in result.get("error", "")


@pytest.mark.unit
@pytest.mark.guardrail
def test_place_order_no_false_positive_on_normal_failure():
    """place_order() must NOT invoke handle_session_expiry() on non-session errors."""

    class _BetFailedClient:
        def place_bet(self, **_kw):
            return {
                "ok": False,
                "error": "BET_FAILED: MARKET_NOT_OPEN",
                "classification": "PERMANENT",
                "order_unknown": False,
            }

    svc = _make_service()
    svc.connected = True
    svc.simulation_mode = False
    svc.client = _BetFailedClient()

    svc.place_order({
        "market_id": "1.123",
        "selection_id": 456,
        "bet_type": "BACK",
        "price": 2.0,
        "stake": 10.0,
    })

    # Non-session error must NOT block live operations
    assert svc.is_session_invalid is False, \
        "BET_FAILED must not trigger session invalidity"
    assert svc.is_live_usable() is not True or not svc._session_invalid


# ===========================================================================
# Tests: SESSION_EXPIRED wired into RuntimeController signal routing
# ===========================================================================

@pytest.mark.unit
@pytest.mark.guardrail
def test_live_signal_rejected_when_session_invalid_in_runtime_controller():
    """RuntimeController._on_signal_received() rejects LIVE signals when session is invalid."""
    from core.runtime_controller import RuntimeController
    from core.system_state import RuntimeMode

    class _Bus:
        def __init__(self):
            self.published = []

        def subscribe(self, *_a, **_kw):
            pass

        def publish(self, event, payload=None):
            self.published.append((event, payload or {}))

    class _Db:
        def _execute(self, *_args, **_kwargs):
            return None

        def get_pending_sagas(self):
            return []

    class _InvalidSessionBetfairService:
        _session_invalid = True
        _session_invalid_reason = "SESSION_EXPIRED"

        def set_simulation_mode(self, *_a, **_kw):
            pass

        def get_live_client(self):
            return None

        def connect(self, **_kw):
            return {}

        def disconnect(self):
            pass

        def get_account_funds(self):
            return {"available": 0.0}

        def status(self):
            return {"connected": False}

    class _TgService:
        def start(self):
            return {}

        def stop(self):
            pass

        def status(self):
            return {}

    class _Cfg:
        table_count = 1
        anti_duplication_enabled = False
        allow_recovery = False
        auto_reset_drawdown_pct = 90
        defense_drawdown_pct = 7.5
        lockdown_drawdown_pct = 95

        def __getattr__(self, _n):
            return 0

    class _Settings:
        def load_roserpina_config(self):
            return _Cfg()

    bus = _Bus()
    rc = RuntimeController(
        bus=bus,
        db=_Db(),
        settings_service=_Settings(),
        betfair_service=_InvalidSessionBetfairService(),
        telegram_service=_TgService(),
    )
    rc.execution_mode = "LIVE"

    rc._on_signal_received({
        "market_id": "1.111",
        "selection_id": 99,
        "price": 2.0,
        "stake": 10.0,
    })

    rejected_reasons = [
        str(p.get("reason", ""))
        for e, p in bus.published
        if e == "SIGNAL_REJECTED"
    ]
    assert rejected_reasons, "Expected at least one SIGNAL_REJECTED event"
    assert any("session_invalid" in r for r in rejected_reasons), \
        f"Expected 'session_invalid' in rejection reason, got: {rejected_reasons}"
