"""
Tests for emergency_stop() / reset_emergency() path in RuntimeController.

Verifies:
- emergency_stop() sets _emergency_stopped flag
- All subsequent live order signals are refused
- live_enabled is forced to False
- EMERGENCY_STOP_TRIGGERED event is published
- cancel-all is attempted and partial failures don't silently resume trading
- reset_emergency() clears the flag
"""

import pytest

from core.runtime_controller import RuntimeController


# ===========================================================================
# Stubs
# ===========================================================================

class _Events:
    def __init__(self):
        self.published = []

    def subscribe(self, *_a, **_kw):
        pass

    def publish(self, event: str, payload=None):
        self.published.append((event, payload or {}))

    def events(self):
        return [e for e, _ in self.published]

    def last(self, event: str):
        for e, p in reversed(self.published):
            if e == event:
                return p
        return None


class _FakeSaga:
    def __init__(self, market_id, bet_id, customer_ref, status="PLACED"):
        self.data = {
            "market_id": market_id,
            "bet_id": bet_id,
            "customer_ref": customer_ref,
            "status": status,
        }


class _Db:
    def __init__(self, sagas=None):
        self._sagas = sagas or []

    def _execute(self, *_args, **_kwargs):
        return None

    def get_pending_sagas(self):
        return [s.data for s in self._sagas]


class _LiveClient:
    def __init__(self, raises=False):
        self.cancel_calls = []
        self.raises = raises

    def cancel_orders(self, *, market_id, bet_ids=None, **_kw):
        if self.raises:
            raise RuntimeError("Betfair unavailable")
        self.cancel_calls.append({"market_id": market_id, "bet_ids": bet_ids})
        return {"ok": True, "market_id": market_id, "status": "SUCCESS", "cancelled_count": 1}


class _BetfairService:
    def __init__(self, live_client=None):
        self._client = live_client or _LiveClient()

    def set_simulation_mode(self, *_a, **_kw):
        pass

    def get_live_client(self):
        return self._client

    def connect(self, **_kw):
        return {"ok": True}

    def disconnect(self):
        pass

    def get_account_funds(self):
        return {"available": 100.0}

    def status(self):
        return {"connected": True}


class _TelegramService:
    def start(self):
        return {"ok": True}

    def stop(self):
        pass

    def status(self):
        return {"connected": True}


class _Config:
    table_count = 2
    anti_duplication_enabled = False
    allow_recovery = False
    auto_reset_drawdown_pct = 90
    defense_drawdown_pct = 7.5
    lockdown_drawdown_pct = 95

    def __getattr__(self, _n):
        return 0


class _SettingsService:
    def load_roserpina_config(self):
        return _Config()


def _make_rc(bus=None, db=None, betfair=None, sagas=None):
    bus = bus or _Events()
    db = db or _Db(sagas)
    betfair = betfair or _BetfairService()
    return RuntimeController(
        bus=bus,
        db=db,
        settings_service=_SettingsService(),
        betfair_service=betfair,
        telegram_service=_TelegramService(),
    ), bus


# ===========================================================================
# Tests
# ===========================================================================

@pytest.mark.unit
@pytest.mark.safety
def test_emergency_stop_sets_flag():
    rc, _ = _make_rc()
    assert rc.is_emergency_stopped is False

    result = rc.emergency_stop(reason="test_reason")

    assert rc.is_emergency_stopped is True
    assert result["emergency_stopped"] is True
    assert result["reason"] == "test_reason"


@pytest.mark.unit
@pytest.mark.safety
def test_emergency_stop_forces_live_disabled():
    rc, _ = _make_rc()
    rc.live_enabled = True
    rc.execution_mode = "LIVE"

    rc.emergency_stop()

    assert rc.live_enabled is False
    assert rc.execution_mode == "SIMULATION"


@pytest.mark.unit
@pytest.mark.safety
def test_emergency_stop_publishes_event():
    rc, bus = _make_rc()

    rc.emergency_stop(reason="manual_trigger")

    assert "EMERGENCY_STOP_TRIGGERED" in bus.events()
    payload = bus.last("EMERGENCY_STOP_TRIGGERED")
    assert payload["reason"] == "manual_trigger"
    assert payload["emergency_stopped"] is True


@pytest.mark.unit
@pytest.mark.safety
def test_emergency_stop_rejects_subsequent_signals():
    rc, bus = _make_rc()
    rc._emergency_stopped = False

    rc.emergency_stop()

    # Simulate a signal arriving after emergency stop
    rc._on_signal_received({
        "market_id": "1.111",
        "selection_id": 99,
        "execution_mode": "LIVE",
    })

    rejected = [p for e, p in bus.published if e == "SIGNAL_REJECTED"]
    assert len(rejected) >= 1
    assert any("emergency_stop_active" in str(r.get("reason", "")) for r in rejected)


@pytest.mark.unit
@pytest.mark.safety
def test_emergency_stop_attempts_cancel_open_orders():
    live_client = _LiveClient()
    sagas = [
        _FakeSaga("1.111", "bet_aaa", "ref1"),
        _FakeSaga("1.222", "bet_bbb", "ref2"),
    ]
    rc, _ = _make_rc(
        betfair=_BetfairService(live_client=live_client),
        sagas=sagas,
    )

    result = rc.emergency_stop()

    assert result["pending_count"] == 2
    assert result["markets_attempted"] == 2
    # Both markets must have been attempted
    attempted = {c["market_id"] for c in live_client.cancel_calls}
    assert "1.111" in attempted
    assert "1.222" in attempted


@pytest.mark.unit
@pytest.mark.safety
def test_emergency_stop_partial_cancel_failure_does_not_resume_trading():
    """Even if cancel_orders raises, emergency stop must remain active."""
    live_client = _LiveClient(raises=True)
    sagas = [_FakeSaga("1.111", "bet_aaa", "ref1")]
    rc, bus = _make_rc(
        betfair=_BetfairService(live_client=live_client),
        sagas=sagas,
    )

    result = rc.emergency_stop()

    # Emergency flag must still be set despite cancel failure
    assert rc.is_emergency_stopped is True
    assert result["emergency_stopped"] is True
    assert result["cancel_error_count"] > 0

    # Subsequent signals still refused
    rc._on_signal_received({"market_id": "1.111", "selection_id": 9})
    rejected = [p for e, p in bus.published if e == "SIGNAL_REJECTED"]
    assert any("emergency_stop_active" in str(r.get("reason", "")) for r in rejected)


@pytest.mark.unit
@pytest.mark.safety
def test_emergency_stop_no_open_orders():
    rc, bus = _make_rc(sagas=[])

    result = rc.emergency_stop()

    assert result["pending_count"] == 0
    assert result["cancelled_count"] == 0
    assert "EMERGENCY_STOP_TRIGGERED" in bus.events()


@pytest.mark.unit
@pytest.mark.safety
def test_reset_emergency_clears_flag():
    rc, bus = _make_rc()
    rc.emergency_stop()
    assert rc.is_emergency_stopped is True

    rc.reset_emergency()

    assert rc.is_emergency_stopped is False
    assert "EMERGENCY_STOP_RESET" in bus.events()


@pytest.mark.unit
@pytest.mark.safety
def test_signal_allowed_after_reset_emergency_and_restart():
    """After reset_emergency(), signals are gated normally (not by emergency stop)."""
    rc, bus = _make_rc()
    rc.emergency_stop()
    rc.reset_emergency()

    assert rc.is_emergency_stopped is False


@pytest.mark.unit
@pytest.mark.safety
def test_emergency_stop_cancel_orders_called_without_bet_id_kwarg():
    """cancel_orders must be called with market_id only (no bet_id= arg).

    Regression: the old call used bet_id="" which does not exist on
    BetfairClient.cancel_orders; that raised AttributeError silently,
    leaving open orders on the exchange.
    """
    live_client = _LiveClient()
    sagas = [_FakeSaga("1.999", "bet_xyz", "refA")]
    rc, _ = _make_rc(
        betfair=_BetfairService(live_client=live_client),
        sagas=sagas,
    )

    rc.emergency_stop()

    assert len(live_client.cancel_calls) == 1
    call = live_client.cancel_calls[0]
    assert call["market_id"] == "1.999"
    # bet_ids should be None (cancel-all, no explicit bet list)
    assert call["bet_ids"] is None


@pytest.mark.unit
@pytest.mark.safety
def test_betfair_client_cancel_orders_exists():
    """BetfairClient must define cancel_orders so emergency_stop never gets AttributeError."""
    from betfair_client import BetfairClient
    assert callable(getattr(BetfairClient, "cancel_orders", None)), \
        "BetfairClient must implement cancel_orders()"
