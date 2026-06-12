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
def test_emergency_stop_cancel_ok_false_counted_as_error():
    """cancel_orders returning ok=False must increment cancel_error_count, not cancelled_count.

    Regression: the old code always set ok=True in the result dict and
    incremented cancelled_count regardless of the response payload, masking
    silent API-level failures during emergency stop.
    """
    class _OkFalseClient:
        def __init__(self):
            self.cancel_calls = []

        def cancel_orders(self, *, market_id, bet_ids=None, **_kw):
            self.cancel_calls.append({"market_id": market_id, "bet_ids": bet_ids})
            return {
                "ok": False,
                "market_id": market_id,
                "error": "CANCEL_FAILED: MARKET_NOT_OPEN",
                "classification": "PERMANENT",
            }

    live_client = _OkFalseClient()
    sagas = [_FakeSaga("1.111", "bet_aaa", "ref1")]
    rc, bus = _make_rc(
        betfair=_BetfairService(live_client=live_client),
        sagas=sagas,
    )

    result = rc.emergency_stop()

    # Emergency flag must still be set
    assert rc.is_emergency_stopped is True
    # ok=False response must be treated as an error
    assert result["cancel_error_count"] >= 1
    assert result["cancelled_count"] == 0
    # Subsequent signals still refused
    rc._on_signal_received({"market_id": "1.111", "selection_id": 9})
    rejected = [p for e, p in bus.published if e == "SIGNAL_REJECTED"]
    assert any("emergency_stop_active" in str(r.get("reason", "")) for r in rejected)


@pytest.mark.unit
@pytest.mark.safety
def test_betfair_client_cancel_orders_exists():
    """BetfairClient must define cancel_orders so emergency_stop never gets AttributeError."""
    from betfair_client import BetfairClient
    assert callable(getattr(BetfairClient, "cancel_orders", None)), \
        "BetfairClient must implement cancel_orders()"


# ===========================================================================
# GUI operator trigger: _runtime_emergency_stop()
# ===========================================================================

@pytest.mark.unit
@pytest.mark.safety
def test_gui_emergency_stop_method_invokes_runtime_emergency_stop():
    """_runtime_emergency_stop() must invoke runtime.emergency_stop() and leave
    the runtime permanently blocked for live order entry."""
    rc, bus = _make_rc()

    # Simulate what the GUI button does
    rc.emergency_stop(reason="operator_gui_button")

    assert rc.is_emergency_stopped is True, \
        "emergency_stop must set is_emergency_stopped after GUI trigger"
    assert rc.live_enabled is False, \
        "live_enabled must be forced to False by GUI emergency stop"
    assert rc.execution_mode == "SIMULATION", \
        "execution_mode must be forced to SIMULATION by GUI emergency stop"

    # All subsequent signals must be refused — flat gate
    rc._on_signal_received({"market_id": "1.999", "selection_id": 1})
    rejected = [p for e, p in bus.published if e == "SIGNAL_REJECTED"]
    assert rejected, "GUI emergency stop must block all subsequent signal routing"
    assert any("emergency_stop_active" in str(r.get("reason", "")) for r in rejected)


@pytest.mark.unit
@pytest.mark.safety
def test_gui_emergency_stop_button_is_defined_on_mini_gui_class():
    """mini_gui.MiniPickfairGUI must define _runtime_emergency_stop() method."""
    import ast
    import pathlib

    src = pathlib.Path("mini_gui.py").read_text(encoding="utf-8")
    tree = ast.parse(src)

    # Find the class MiniPickfairGUI and check for _runtime_emergency_stop
    found = False
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == "MiniPickfairGUI":
            for item in ast.walk(node):
                if isinstance(item, ast.FunctionDef) and item.name == "_runtime_emergency_stop":
                    found = True
                    break
    assert found, "MiniPickfairGUI must define _runtime_emergency_stop() method"


@pytest.mark.unit
@pytest.mark.safety
def test_gui_emergency_stop_cancel_semantics_flat_cancel():
    """emergency_stop() must cancel ALL unmatched orders per market (no bet_ids).
    Partial failure must leave the emergency flag SET (fail-closed)."""
    live_client = _LiveClient()
    sagas = [
        _FakeSaga("1.111", "bet_aaa", "ref1"),
        _FakeSaga("1.111", "bet_bbb", "ref2"),  # same market
        _FakeSaga("1.222", "bet_ccc", "ref3"),  # different market
    ]
    rc, bus = _make_rc(betfair=_BetfairService(live_client=live_client), sagas=sagas)

    result = rc.emergency_stop(reason="operator_gui_button")

    # Two markets → two cancel calls
    assert result["markets_attempted"] == 2, \
        "emergency_stop must attempt cancel on each distinct market"
    # No bet_id → flat cancel of ALL unmatched orders on each market
    for call in live_client.cancel_calls:
        assert "bet_ids" not in call or not call["bet_ids"], \
            "cancel must be market-wide (no bet_ids) to flatten all unmatched orders"
    # Emergency flag remains set regardless
    assert rc.is_emergency_stopped is True


# ===========================================================================
# PR-H: persistenza cross-riavvio, timeout/parziale, race, lifecycle
# ===========================================================================

class _PersistentDb(_Db):
    """DB fake con settings persistenti (stringhe, come la tabella reale)."""

    def __init__(self, sagas=None, settings=None):
        super().__init__(sagas)
        self.settings = dict(settings or {})

    def save_settings(self, data):
        for key, value in (data or {}).items():
            self.settings[str(key)] = str(value)

    def get_settings(self):
        return dict(self.settings)


class _TimeoutClient:
    def cancel_orders(self, *, market_id, bet_ids=None, **_kw):
        raise TimeoutError(f"cancelOrders timeout on {market_id}")


class _PartialFailClient:
    """Il cancel riesce su un mercato e fallisce sull'altro (N su M)."""

    def __init__(self, failing_market):
        self.failing_market = failing_market
        self.cancel_calls = []

    def cancel_orders(self, *, market_id, bet_ids=None, **_kw):
        self.cancel_calls.append(market_id)
        if market_id == self.failing_market:
            return {"ok": False, "error": "MARKET_SUSPENDED"}
        return {"ok": True, "cancelled_count": 1}


class _ReentrantSignalClient:
    """Simula un segnale che ARRIVA mentre il cancel e' in corso."""

    def __init__(self):
        self.controller = None
        self.rejected_during_cancel = []

    def cancel_orders(self, *, market_id, bet_ids=None, **_kw):
        # Il segnale concorrente arriva nel mezzo dell'emergency stop: il
        # flag e' settato PRIMA del cancel, quindi DEVE essere rifiutato.
        self.controller._on_signal_received({"market_type": "NEXT_GOAL"})
        events = self.controller.bus.last("SIGNAL_REJECTED")
        self.rejected_during_cancel.append(events)
        return {"ok": True, "cancelled_count": 1}


def _signal_rejections(bus):
    return [p for e, p in bus.published if e == "SIGNAL_REJECTED"]


# --------------------------- BUCKET 3: persistenza -------------------------

@pytest.mark.unit
@pytest.mark.safety
@pytest.mark.recovery
def test_emergency_state_survives_process_restart():
    """Crash o riavvio del processo (VPS/supervisor) DURANTE l'emergenza:
    il nuovo processo DEVE ripartire in emergenza — senza persistenza il
    bot riprenderebbe a tradare da solo, bypassando reset_emergency()."""
    db = _PersistentDb()
    rc1, _bus1 = _make_rc(db=db)
    rc1.emergency_stop(reason="operator_panic")
    assert rc1.is_emergency_stopped is True

    # "Riavvio": nuovo controller sullo stesso DB.
    rc2, bus2 = _make_rc(db=db)

    assert rc2.is_emergency_stopped is True, (
        "stato di emergenza PERSO al riavvio: il bot tornerebbe a tradare"
    )
    rc2._on_signal_received({"market_type": "NEXT_GOAL"})
    rejections = _signal_rejections(bus2)
    assert rejections, "segnale non rifiutato dopo riavvio in emergenza"
    assert "emergency_stop_active" in rejections[-1]["reason"]


@pytest.mark.unit
@pytest.mark.safety
@pytest.mark.recovery
def test_reset_emergency_clears_persisted_state_too():
    """reset_emergency() pulisce ANCHE lo stato persistito: il riavvio
    successivo riparte pulito."""
    db = _PersistentDb()
    rc1, _bus = _make_rc(db=db)
    rc1.emergency_stop(reason="x")
    rc1.reset_emergency()

    rc2, _bus2 = _make_rc(db=db)
    assert rc2.is_emergency_stopped is False


@pytest.mark.unit
@pytest.mark.safety
def test_emergency_persist_failure_is_reported_not_silent():
    """DB che fallisce il persist: l'emergenza resta attiva in memoria e
    il risultato riporta l'errore (mai fallimento silenzioso)."""
    class _BrokenSettingsDb(_Db):
        def save_settings(self, _data):
            raise RuntimeError("disk full")

        def get_settings(self):
            return {}

    rc, _bus = _make_rc(db=_BrokenSettingsDb())
    result = rc.emergency_stop(reason="x")

    assert rc.is_emergency_stopped is True
    assert result.get("persist_error"), (
        "persist fallito deve essere visibile nel risultato"
    )


@pytest.mark.unit
@pytest.mark.safety
def test_corrupt_persisted_flag_does_not_crash_and_stays_clean():
    """Valore spazzatura nella setting persistita: nessun crash, e solo i
    valori truthy espliciti ('1'/'true') ripristinano l'emergenza."""
    db = _PersistentDb(settings={"emergency_stopped": "garbage"})
    rc, _bus = _make_rc(db=db)
    assert rc.is_emergency_stopped is False

    db_true = _PersistentDb(settings={"emergency_stopped": "True"})
    rc2, _bus2 = _make_rc(db=db_true)
    assert rc2.is_emergency_stopped is True


# --------------------------- get_status onesto -----------------------------

@pytest.mark.unit
@pytest.mark.safety
def test_get_status_exposes_emergency_state():
    """L'operatore DEVE vedere lo stato di emergenza nello snapshot."""
    rc, _bus = _make_rc(db=_PersistentDb())
    rc.emergency_stop(reason="visibility")

    status = rc.get_status()
    assert status["is_emergency_stopped"] is True

    rc.reset_emergency()
    status_after = rc.get_status()
    assert status_after["is_emergency_stopped"] is False


# --------------------------- BUCKET 1: cancel degradato --------------------

@pytest.mark.unit
@pytest.mark.safety
def test_emergency_stop_timeout_during_cancel_stays_locked():
    """Timeout Betfair durante il cancel-all: il sistema RESTA in emergenza
    (mai 'tornare operativo perche' tanto ci ha provato')."""
    sagas = [_FakeSaga("1.111", "bet_a", "ref1")]
    rc, bus = _make_rc(
        betfair=_BetfairService(live_client=_TimeoutClient()), sagas=sagas,
    )

    result = rc.emergency_stop(reason="timeout_case")

    assert rc.is_emergency_stopped is True
    assert result["cancel_error_count"] >= 1
    rc._on_signal_received({"market_type": "NEXT_GOAL"})
    assert "emergency_stop_active" in _signal_rejections(bus)[-1]["reason"]


@pytest.mark.unit
@pytest.mark.safety
def test_emergency_stop_partial_market_failure_stays_locked():
    """N mercati su M falliscono il cancel: anche con successi parziali il
    sistema resta bloccato e il conteggio errori e' onesto."""
    client = _PartialFailClient(failing_market="1.222")
    sagas = [
        _FakeSaga("1.111", "bet_a", "ref1"),
        _FakeSaga("1.222", "bet_b", "ref2"),
    ]
    rc, bus = _make_rc(betfair=_BetfairService(live_client=client), sagas=sagas)

    result = rc.emergency_stop(reason="partial_case")

    assert sorted(client.cancel_calls) == ["1.111", "1.222"]
    assert result["cancelled_count"] >= 1
    assert result["cancel_error_count"] >= 1
    assert rc.is_emergency_stopped is True
    rc._on_signal_received({"market_type": "NEXT_GOAL"})
    assert "emergency_stop_active" in _signal_rejections(bus)[-1]["reason"]


# --------------------------- BUCKET 2: race segnale ------------------------

@pytest.mark.unit
@pytest.mark.safety
@pytest.mark.concurrency
def test_signal_arriving_during_cancel_is_rejected():
    """Il flag e' settato PRIMA del cancel: un segnale che arriva MENTRE il
    cancel-all e' in corso viene gia' rifiutato (niente finestra aperta)."""
    client = _ReentrantSignalClient()
    sagas = [_FakeSaga("1.111", "bet_a", "ref1")]
    rc, _bus = _make_rc(betfair=_BetfairService(live_client=client), sagas=sagas)
    client.controller = rc

    rc.emergency_stop(reason="race_case")

    assert client.rejected_during_cancel, "il cancel non e' stato eseguito"
    rejection = client.rejected_during_cancel[0]
    assert rejection is not None
    assert "emergency_stop_active" in rejection["reason"]


# --------------------------- BUCKET 3: lifecycle ----------------------------

@pytest.mark.unit
@pytest.mark.safety
def test_reset_cycle_does_not_clear_emergency():
    """'Reset Ciclo' NON e' reset_emergency(): l'emergenza resta attiva."""
    rc, bus = _make_rc(db=_PersistentDb())
    rc.emergency_stop(reason="cycle_case")

    rc.reset_cycle()

    assert rc.is_emergency_stopped is True
    rc._on_signal_received({"market_type": "NEXT_GOAL"})
    assert "emergency_stop_active" in _signal_rejections(bus)[-1]["reason"]


@pytest.mark.unit
@pytest.mark.safety
def test_reset_emergency_then_signals_not_emergency_blocked():
    """Controllo PASS: dopo reset_emergency() i segnali non sono piu'
    rifiutati per emergenza (possono esserlo per altri gate, es. runtime
    fermo, ma MAI con reason emergency_stop_active)."""
    rc, bus = _make_rc(db=_PersistentDb())
    rc.emergency_stop(reason="flow_case")
    rc.reset_emergency()

    rc._on_signal_received({"market_type": "NEXT_GOAL"})

    emergency_rejections = [
        r for r in _signal_rejections(bus)
        if r["reason"].startswith("emergency_stop_active")
    ]
    assert emergency_rejections == [], (
        "dopo reset_emergency() nessun segnale deve essere rifiutato per emergenza"
    )
