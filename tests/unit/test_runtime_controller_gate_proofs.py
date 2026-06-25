"""Proof fail-closed del control-path RuntimeController (test_suite_proof_runtime_controller).

Phase 0 dedup (verificato): il control-path e la safety layer sono GIA' coperti
in modo forte (emergency-stop lifecycle, deploy-gate fail-closed, live-switch
fail-closed, matrice live-gate completa in test_live_gate_fail_closed_matrix.py,
live-readiness blockers, bankroll/daily-loss, pause/resume parity, cycle
recovery). In particolare `core/safety_layer.assert_live_gate_or_refuse` ha gia'
una matrice esaustiva: qui NON si duplica.

Qui SOLO i rami fail-closed deterministici ancora non asseriti in isolamento,
caratterizzati sul comportamento REALE (probe eseguito prima di scrivere):

1. is_live_allowed(): con tutti gli altri gate aperti, `live_enabled=False`
   da solo basta a negare il live (ramo isolato, prima non asserito da solo).
2. is_live_allowed(): se get_deploy_gate_status() SOLLEVA, il choke point
   fa fail-closed (False) senza propagare, e l'effective mode demota a
   SIMULATION (questo ramo di eccezione non era coperto).
3. _on_signal_received(): un segnale LIVE con deploy gate che nega viene
   rifiutato con reason che inizia per 'deploy_gate_no_go:' (la causa del gate
   e' incapsulata nella reason).
4. _on_signal_received(): un segnale LIVE con sessione Betfair invalida viene
   rifiutato con reason esatta 'session_invalid_live_blocked'.
5. _risk_allows_auto_trade(): l'auto-trade da settlement (che NON passa da
   _on_signal_received) e' negato con 'runtime_not_active' se il runtime non
   e' ACTIVE.
6. _risk_allows_auto_trade(): negato con 'desk_lockdown' quando il desk e' in
   LOCKDOWN; approvato ('risk_approved') nello stato normale.

Tutto pure-method / minimal-stub e deterministico (nessun thread/sleep/IO);
nessuna modifica al codice di produzione (core/runtime_controller.py e
core/safety_layer.py sono file critici: solo test).
"""
from __future__ import annotations

import pytest

from core.runtime_controller import RuntimeController
from core.system_state import DeskMode, RuntimeMode

# ---------------------------------------------------------------------------
# Stub minimi (i metodi senza self sono @staticmethod per non introdurre
# antipattern; quelli che usano self restano metodi d'istanza).
# ---------------------------------------------------------------------------

class _Config:
    """Config Roserpina minima con default inerti per gli attributi non noti."""

    table_count = 1
    anti_duplication_enabled = False
    allow_recovery = False
    auto_reset_drawdown_pct = 90
    defense_drawdown_pct = 7.5
    lockdown_drawdown_pct = 95

    def __getattr__(self, _name):
        return 0


class _Settings:
    """settings_service finto."""

    @staticmethod
    def load_roserpina_config():
        return _Config()


class _Bus:
    """Event bus finto che registra gli eventi pubblicati."""

    def __init__(self):
        self.published = []

    @staticmethod
    def subscribe(*_args, **_kwargs):
        return None

    def publish(self, event, payload=None):
        self.published.append((event, payload or {}))


class _Db:
    """DB finto inerte."""

    @staticmethod
    def _execute(*_args, **_kwargs):
        return None

    @staticmethod
    def get_pending_sagas():
        return []


class _Betfair:
    """BetfairService finto; _session_invalid pilotabile per i test."""

    def __init__(self, session_invalid=False, reason=""):
        self._session_invalid = session_invalid
        self._session_invalid_reason = reason

    @staticmethod
    def set_simulation_mode(*_args, **_kwargs):
        return None

    @staticmethod
    def get_live_client():
        return None

    @staticmethod
    def connect(**_kwargs):
        return {}

    @staticmethod
    def disconnect():
        return None

    @staticmethod
    def get_account_funds():
        return {"available": 0.0}

    @staticmethod
    def status():
        return {"connected": False}


class _Telegram:
    """TelegramService finto inerte."""

    @staticmethod
    def start():
        return {}

    @staticmethod
    def stop():
        return None

    @staticmethod
    def status():
        return {}


def _make_rc(betfair=None):
    """Costruisce un RuntimeController con servizi finti (nessun lifecycle)."""
    bus = _Bus()
    rc = RuntimeController(
        bus=bus,
        db=_Db(),
        settings_service=_Settings(),
        betfair_service=betfair or _Betfair(),
        telegram_service=_Telegram(),
    )
    return rc, bus


def _rejection_reasons(bus):
    """Estrae le reason degli eventi SIGNAL_REJECTED pubblicati."""
    return [p.get("reason", "") for event, p in bus.published if event == "SIGNAL_REJECTED"]


_LIVE_SIGNAL = {"market_id": "1.111", "selection_id": 99, "price": 2.0, "stake": 10.0}


# ---------------------------------------------------------------------------
# 1-2. is_live_allowed(): rami fail-closed isolati
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.guardrail
def test_is_live_allowed_false_when_live_enabled_is_false_alone():
    """Con emergenza spenta, kill-switch spento, execution_mode=LIVE e deploy
    gate che concederebbe, il solo `live_enabled=False` nega comunque il live."""
    rc, _ = _make_rc()
    rc._emergency_stopped = False
    rc._is_kill_switch_active = lambda: False
    rc.execution_mode = "LIVE"
    rc.live_readiness_ok = True
    rc.get_deploy_gate_status = lambda **_k: {"allowed": True, "readiness": "READY"}

    rc.live_enabled = False
    assert rc.is_live_allowed() is False
    # controprova: riabilitando live, lo stesso choke point concede
    rc.live_enabled = True
    assert rc.is_live_allowed() is True


@pytest.mark.unit
@pytest.mark.guardrail
def test_is_live_allowed_fails_closed_when_deploy_gate_raises():
    """Se get_deploy_gate_status() SOLLEVA, il choke point fa fail-closed
    (False) senza propagare l'eccezione, e l'effective mode demota a
    SIMULATION."""
    rc, _ = _make_rc()
    rc._emergency_stopped = False
    rc._is_kill_switch_active = lambda: False
    rc.execution_mode = "LIVE"
    rc.live_enabled = True
    rc.live_readiness_ok = True

    def _raise(**_kwargs):
        raise RuntimeError("deploy gate boom")

    rc.get_deploy_gate_status = _raise

    assert rc.is_live_allowed() is False
    assert rc.get_effective_execution_mode() == "SIMULATION"


# ---------------------------------------------------------------------------
# 3-4. _on_signal_received(): reason di rifiuto sul percorso LIVE
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.guardrail
def test_live_signal_rejected_with_deploy_gate_no_go_reason():
    """Un segnale LIVE con deploy gate che nega viene rifiutato con reason che
    inizia per 'deploy_gate_no_go:' e incapsula la causa del gate."""
    rc, bus = _make_rc()
    rc._emergency_stopped = False
    rc._is_kill_switch_active = lambda: False
    rc.mode = RuntimeMode.ACTIVE
    rc.execution_mode = "LIVE"
    rc.live_enabled = False  # fa negare il deploy gate
    rc.live_readiness_ok = True

    rc._on_signal_received(dict(_LIVE_SIGNAL))

    reasons = _rejection_reasons(bus)
    assert reasons, "atteso almeno un SIGNAL_REJECTED"
    assert reasons[0].startswith("deploy_gate_no_go:")
    # la reason del gate e' incapsulata (non vuota dopo il prefisso)
    assert reasons[0].split("deploy_gate_no_go:", 1)[1].strip()


@pytest.mark.unit
@pytest.mark.guardrail
def test_live_signal_rejected_when_session_invalid():
    """Un segnale LIVE con sessione Betfair invalida viene rifiutato con la
    reason esatta 'session_invalid_live_blocked' (fail-closed lato sessione)."""
    rc, bus = _make_rc(betfair=_Betfair(session_invalid=True, reason="SESSION_EXPIRED"))
    rc._emergency_stopped = False
    rc.mode = RuntimeMode.ACTIVE
    rc.execution_mode = "LIVE"
    rc.live_enabled = True
    rc.live_readiness_ok = True

    rc._on_signal_received(dict(_LIVE_SIGNAL))

    assert "session_invalid_live_blocked" in _rejection_reasons(bus)


# ---------------------------------------------------------------------------
# 5-6. _risk_allows_auto_trade(): gate dell'auto-trade da settlement
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.guardrail
def test_auto_trade_blocked_when_runtime_not_active():
    """L'auto-trade da settlement (che NON passa da _on_signal_received) e'
    negato con 'runtime_not_active' se il runtime non e' ACTIVE."""
    rc, _ = _make_rc()
    rc._emergency_stopped = False
    rc.mode = RuntimeMode.STOPPED

    allowed, reason = rc._risk_allows_auto_trade()
    assert allowed is False
    assert reason == "runtime_not_active"


@pytest.mark.unit
@pytest.mark.guardrail
def test_auto_trade_blocked_on_desk_lockdown_and_approved_when_normal():
    """L'auto-trade e' negato con 'desk_lockdown' quando il desk e' in LOCKDOWN,
    e approvato ('risk_approved') nello stato normale."""
    rc, _ = _make_rc()
    rc._emergency_stopped = False
    rc.mode = RuntimeMode.ACTIVE

    rc._desk_mode = lambda: DeskMode.LOCKDOWN
    assert rc._risk_allows_auto_trade() == (False, "desk_lockdown")

    rc._desk_mode = lambda: DeskMode.NORMAL
    assert rc._risk_allows_auto_trade() == (True, "risk_approved")
