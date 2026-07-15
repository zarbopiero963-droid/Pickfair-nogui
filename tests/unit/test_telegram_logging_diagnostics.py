"""Diagnostica di logging Telegram — copre i 5 punti "silent-failure".

Contesto (decisione owner): se Telegram ha un problema, prima NON ne restava
mai il motivo nei log. Questo PR aggiunge log diagnostici fail-safe (solo
righe di log, nessun cambio di comportamento) su:

1. ``telegram_listener.mark_failed`` — ogni fallimento terminale (timeout,
   sessione non autorizzata, disconnessione inattesa, runtime_error,
   reconnect_failed) ora logga ERROR col reason.
2. ``telegram_listener._runtime_main`` — il crash del thread runtime ora
   preserva il traceback (``logger.exception``).
3. ``telegram_listener._set_state`` — ogni transizione di stato ora logga INFO.
4. ``telegram_listener.begin/end_reconnect_attempt`` — tentativi ed esito
   della riconnessione ora loggati.
5. ``services.telegram_service`` — decisioni autoheal ENTER_FAILED_LOCKOUT
   (recovery sospeso) e SCHEDULE_RESTART ora loggate.

Ogni test verifica SIA che il log esca sul path giusto (PASS) SIA che il
comportamento runtime resti invariato (BLOCK): lo stato transita ancora, i
contatori restano coerenti, e i segreti non finiscono nei log.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

import pytest

from recovery.telegram_autoheal import (
    TelegramAutohealAction,
    TelegramAutohealDecision,
    TelegramAutohealPolicy,
    TelegramFailureClass,
)
from services.telegram_service import TelegramService
from telegram_listener import TelegramListener


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _listener(**overrides):
    return TelegramListener(api_id=1, api_hash="x", **overrides)


def _messages(caplog, needle):
    return [r.getMessage() for r in caplog.records if needle in r.getMessage()]


# ---------------------------------------------------------------------------
# 1. mark_failed — ERROR col reason
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_mark_failed_logs_error_with_reason(caplog):
    listener = _listener()
    with caplog.at_level(logging.DEBUG, logger="telegram_listener"):
        listener.mark_failed("connect_timeout")

    hits = _messages(caplog, "[TelegramListener] mark_failed")
    assert hits, "mark_failed non ha loggato il fallimento terminale"
    assert any("connect_timeout" in m for m in hits)
    assert any(r.levelno == logging.ERROR for r in caplog.records
               if "mark_failed" in r.getMessage())
    # BLOCK: comportamento invariato (il fix #400 e la state machine restano).
    assert listener.state == "FAILED"
    assert listener.active_network_resources == 0
    assert listener.running is False


@pytest.mark.unit
def test_mark_failed_empty_reason_has_fallback(caplog):
    listener = _listener()
    with caplog.at_level(logging.DEBUG, logger="telegram_listener"):
        listener.mark_failed("")
    hits = _messages(caplog, "[TelegramListener] mark_failed")
    assert hits and any("listener_failure" in m for m in hits)


# ---------------------------------------------------------------------------
# 2. runtime crash — traceback preservato
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_runtime_crash_logs_traceback_and_marks_failed(caplog):
    listener = _listener()

    async def _boom():
        raise RuntimeError("kaboom")

    listener._runtime_async = _boom  # type: ignore[assignment]
    with caplog.at_level(logging.DEBUG, logger="telegram_listener"):
        listener._runtime_main()

    crash = [r for r in caplog.records if "runtime thread crashed" in r.getMessage()]
    assert crash, "il crash del thread runtime non e' stato loggato"
    # Traceback preservato nel messaggio (redatto): lo stack del crash e' visibile.
    assert any("Traceback" in r.getMessage() and "kaboom" in r.getMessage() for r in crash)
    # BLOCK: il crash sfocia comunque in mark_failed col reason runtime_error.
    assert listener.state == "FAILED"
    assert any("runtime_error" in m for m in _messages(caplog, "mark_failed"))


@pytest.mark.unit
def test_runtime_intentional_stop_does_not_log_crash(caplog):
    listener = _listener()
    listener.intentional_stop = True

    async def _boom():
        raise RuntimeError("kaboom")

    listener._runtime_async = _boom  # type: ignore[assignment]
    with caplog.at_level(logging.DEBUG, logger="telegram_listener"):
        listener._runtime_main()
    # BLOCK: uno stop intenzionale non deve generare un falso allarme di crash.
    assert not [r for r in caplog.records if "runtime thread crashed" in r.getMessage()]
    assert listener.state != "FAILED"


# ---------------------------------------------------------------------------
# 3. _set_state — transizioni loggate
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_set_state_logs_transition(caplog):
    listener = _listener()  # stato iniziale CREATED
    with caplog.at_level(logging.DEBUG, logger="telegram_listener"):
        listener._set_state("CONNECTING")
    assert _messages(caplog, "stato CREATED -> CONNECTING")


@pytest.mark.unit
def test_set_state_no_log_when_unchanged(caplog):
    listener = _listener()
    with caplog.at_level(logging.DEBUG, logger="telegram_listener"):
        listener._set_state("CREATED")  # nessuna transizione reale
    # BLOCK: nessun rumore di log su re-set dello stesso stato.
    assert not _messages(caplog, "stato CREATED -> CREATED")


# ---------------------------------------------------------------------------
# 4. reconnect begin/end — loggati
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_reconnect_begin_and_success_logged(caplog):
    listener = _listener()
    with caplog.at_level(logging.DEBUG, logger="telegram_listener"):
        started = listener.begin_reconnect_attempt()
        assert started is True
        listener.end_reconnect_attempt(success=True)

    assert _messages(caplog, "reconnect tentativo #1 avviato")
    assert _messages(caplog, "reconnect riuscito")
    # BLOCK: contatore e stato coerenti col contratto esistente.
    assert listener.reconnect_attempts == 1
    assert listener.state == "STOPPED"


@pytest.mark.unit
def test_reconnect_failure_logs_via_mark_failed(caplog):
    listener = _listener()
    with caplog.at_level(logging.DEBUG, logger="telegram_listener"):
        listener.begin_reconnect_attempt()
        listener.end_reconnect_attempt(success=False, error="reconnect_failed")
    assert any("reconnect_failed" in m for m in _messages(caplog, "mark_failed"))
    assert listener.state == "FAILED"


# ---------------------------------------------------------------------------
# GUARD: nessun segreto nei log di fallimento
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_failure_log_does_not_leak_secrets(caplog):
    listener = _listener(session_string="SECRET_SESSION_STRING", bot_token="123:SECRET_BOT_TOKEN")
    with caplog.at_level(logging.DEBUG, logger="telegram_listener"):
        listener.mark_failed("session_not_authorized")
        listener._set_state("CONNECTING")
    blob = "\n".join(r.getMessage() for r in caplog.records)
    assert "SECRET_SESSION_STRING" not in blob
    assert "SECRET_BOT_TOKEN" not in blob


@pytest.mark.unit
def test_runtime_error_reason_redacts_leaked_secret(caplog):
    """Il path reale del leak: un'eccezione runtime NON controllata che ingloba
    session string / bot token / chat-id finisce in mark_failed(runtime_error)
    e nel traceback. Entrambi devono uscire REDATTI."""
    listener = _listener(
        session_string="SUPERSECRETSESSION42",
        bot_token="9988:SECRETBOTTOKEN",
    )
    listener.set_monitored_chats([-1009998887])

    async def _boom():
        raise RuntimeError(
            "connect fallito sess=SUPERSECRETSESSION42 token=9988:SECRETBOTTOKEN chat=-1009998887"
        )

    listener._runtime_async = _boom  # type: ignore[assignment]
    with caplog.at_level(logging.DEBUG, logger="telegram_listener"):
        listener._runtime_main()

    blob = "\n".join(r.getMessage() for r in caplog.records)
    # Nessun segreto/identificativo in chiaro né nel reason né nel traceback.
    assert "SUPERSECRETSESSION42" not in blob
    assert "9988:SECRETBOTTOKEN" not in blob
    assert "SECRETBOTTOKEN" not in blob
    assert "-1009998887" not in blob
    assert "[REDACTED]" in blob
    # BLOCK: il fallimento resta osservabile (reason presente, stato FAILED).
    assert any("runtime_error" in m for m in _messages(caplog, "mark_failed"))
    assert listener.state == "FAILED"


# ---------------------------------------------------------------------------
# 5. autoheal (service) — lockout / restart loggati
# ---------------------------------------------------------------------------
@dataclass
class _TelegramCfg:
    enabled: bool = True
    api_id: str = "123"
    api_hash: str = "hash"
    session_string: str = "sess"
    monitored_chat_ids: list | None = None


class _Settings:
    def __init__(self, cfg):
        self.cfg = cfg

    def load_telegram_config(self):
        if self.cfg.monitored_chat_ids is None:
            self.cfg.monitored_chat_ids = [1001]
        return self.cfg


class _DB:
    def save_received_signal(self, payload):
        _ = payload


class _Bus:
    def publish(self, topic, payload):
        _ = topic, payload


class _FakeTelethonClient:
    def __init__(self, *_a, **_k):
        self._disconnected = None

    async def connect(self):
        import asyncio as _aio
        self._disconnected = _aio.Event()

    async def is_user_authorized(self):
        return True

    def add_event_handler(self, callback, event_filter=None):
        _ = callback, event_filter

    def is_connected(self):
        return self._disconnected is not None and not self._disconnected.is_set()

    async def run_until_disconnected(self):
        await self._disconnected.wait()

    async def disconnect(self):
        if self._disconnected is not None:
            self._disconnected.set()


class _FakePolicy(TelegramAutohealPolicy):
    """Policy deterministica: eredita i parametri reali (lockout_sec,
    restart_window_sec, ...) e sovrascrive solo evaluate() per ritornare la
    decisione data."""

    def __init__(self, decision):
        super().__init__()
        self._decision = decision

    def now(self):
        return 1000.0

    def evaluate(self, snapshot, history):
        _ = snapshot, history
        return self._decision


def _svc():
    return TelegramService(
        settings_service=_Settings(_TelegramCfg()),
        db=_DB(),
        bus=_Bus(),
        client_factory=lambda *_a: _FakeTelethonClient(),
        connect_timeout=5.0,
    )


@pytest.mark.unit
def test_autoheal_enter_lockout_logs_error(caplog):
    svc = _svc()
    svc.start()
    try:
        svc._autoheal_policy = _FakePolicy(
            TelegramAutohealDecision(
                action=TelegramAutohealAction.ENTER_FAILED_LOCKOUT,
                reason="restart_budget_exhausted",
                failure_class=TelegramFailureClass.RESTART_BUDGET_EXHAUSTED,
                recovery_allowed=False,
            )
        )
        with caplog.at_level(logging.DEBUG, logger="services.telegram_service"):
            svc.evaluate_autoheal(
                checked_at_ts=1000.0,
                startup_grace_active=False,
                reconnect_grace_active=False,
                failure_escalated=True,
            )
    finally:
        svc.stop()

    hits = _messages(caplog, "ENTER_FAILED_LOCKOUT")
    assert hits, "l'ingresso in lockout (recovery sospeso) non e' stato loggato"
    assert any("restart_budget_exhausted" in m for m in hits)
    assert any(r.levelno == logging.ERROR for r in caplog.records
               if "ENTER_FAILED_LOCKOUT" in r.getMessage())
    # BLOCK: lo stato di lockout resta effettivamente attivo.
    assert svc._lockout_active is True


@pytest.mark.unit
def test_autoheal_schedule_restart_logs_warning(caplog):
    svc = _svc()
    svc.start()
    try:
        svc._autoheal_policy = _FakePolicy(
            TelegramAutohealDecision(
                action=TelegramAutohealAction.SCHEDULE_RESTART,
                reason="disconnect_recoverable",
                failure_class=TelegramFailureClass.RECOVERABLE_DISCONNECT,
                recovery_allowed=True,
            )
        )
        with caplog.at_level(logging.DEBUG, logger="services.telegram_service"):
            outcome = svc.run_autoheal_once(
                checked_at_ts=1000.0,
                startup_grace_active=False,
                reconnect_grace_active=False,
                failure_escalated=True,
            )
    finally:
        svc.stop()

    hits = _messages(caplog, "SCHEDULE_RESTART")
    assert hits, "il restart automatico non e' stato loggato"
    assert any("disconnect_recoverable" in m for m in hits)
    # BLOCK: l'azione decisa resta SCHEDULE_RESTART (log additivo, non altera).
    assert outcome["action"] == "SCHEDULE_RESTART"
