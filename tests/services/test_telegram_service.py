from __future__ import annotations

from dataclasses import dataclass

import pytest

from services.telegram_service import TelegramService


class _FakeTelethonClient:
    """Client fake con l'interfaccia async richiesta dal runtime del listener."""

    def __init__(self, hang_connect: bool = False):
        self.hang_connect = hang_connect
        self._disconnected = None

    async def connect(self):
        if self.hang_connect:
            import asyncio as _aio
            await _aio.Event().wait()
        import asyncio as _aio
        self._disconnected = _aio.Event()

    async def is_user_authorized(self):
        return True

    def add_event_handler(self, callback, event_filter=None):
        pass

    def is_connected(self):
        return self._disconnected is not None and not self._disconnected.is_set()

    async def run_until_disconnected(self):
        await self._disconnected.wait()

    async def disconnect(self):
        if self._disconnected is not None:
            self._disconnected.set()


def _svc(cfg=None, *, hang_connect=False, connect_timeout=5.0, db=None, bus=None):
    return TelegramService(
        settings_service=_Settings(cfg or _TelegramCfg()),
        db=db or _DB(),
        bus=bus or _Bus(),
        client_factory=lambda *_a: _FakeTelethonClient(hang_connect=hang_connect),
        connect_timeout=connect_timeout,
    )


@dataclass
class _TelegramCfg:
    enabled: bool = True
    api_id: str = "123"
    api_hash: str = "hash"
    session_string: str = "sess"
    monitored_chat_ids: list[int] | None = None


class _Settings:
    def __init__(self, cfg: _TelegramCfg):
        self.cfg = cfg

    def load_telegram_config(self):
        cfg = self.cfg
        if cfg.monitored_chat_ids is None:
            cfg.monitored_chat_ids = [1001]
        return cfg


class _DB:
    def __init__(self):
        self.saved = []

    def save_received_signal(self, payload):
        self.saved.append(dict(payload))


class _Bus:
    def __init__(self):
        self.events = []

    def publish(self, topic, payload):
        self.events.append((topic, dict(payload or {})))


@pytest.mark.unit
def test_telegram_service_start_reaches_connected_with_live_client():
    svc = _svc()

    start = svc.start()
    status = svc.status()

    assert start["started"] is True
    assert start["state"] == "CONNECTED"
    assert start["connected"] is True
    assert status["state"] == "CONNECTED"
    assert status["running"] is True
    assert status["connected"] is True
    assert status["listener_started"] is True
    assert status["active_network_resources"] == 1
    assert "reconnect_attempts" in status
    svc.stop()


@pytest.mark.unit
def test_telegram_service_start_is_idempotent_and_does_not_duplicate_listener_setup():
    svc = _svc()
    first = svc.start()
    first_listener = svc.listener
    first_handlers = svc.status()["handlers_registered"]

    second = svc.start()
    second_listener = svc.listener
    second_handlers = svc.status()["handlers_registered"]

    assert first["started"] is True
    assert second["started"] is True
    assert first_listener is not None
    assert second_listener is not None
    assert first_handlers == second_handlers


@pytest.mark.unit
def test_telegram_service_stop_is_idempotent_and_marks_intentional_stop():
    svc = _svc()
    svc.start()

    svc.stop()
    status_after_first_stop = svc.status()

    svc.stop()
    status_after_second_stop = svc.status()

    assert status_after_first_stop["state"] == "STOPPED"
    assert status_after_first_stop["intentional_stop"] is True
    assert status_after_first_stop["connected"] is False
    assert status_after_second_stop["state"] == "STOPPED"
    assert status_after_second_stop["intentional_stop"] is True
    assert status_after_second_stop["connected"] is False


@pytest.mark.unit
def test_telegram_service_restart_is_controlled_and_bounded():
    svc = _svc()
    svc.start()
    svc._set_state("FAILED")

    result = svc.restart()
    status = svc.status()

    assert result["started"] is True
    assert status["state"] == "CONNECTED"
    assert status["reconnect_attempts"] >= 1
    assert status["reconnect_in_progress"] is False
    svc.stop()


@pytest.mark.unit
def test_telegram_listener_callback_failure_is_isolated_and_status_remains_coherent():
    svc = _svc()
    svc.start()
    listener = svc.listener
    assert listener is not None

    def _boom(*_args, **_kwargs):
        raise RuntimeError("boom")

    listener.set_callbacks(on_signal=svc._handle_signal, on_status=_boom)

    listener._emit_status("INFO", "hello")
    status = svc.status()

    assert status["state"] == "CONNECTED"
    assert status["connected"] is True
    svc.stop()


@pytest.mark.unit
def test_telegram_service_invalid_config_fails_closed():
    cfg = _TelegramCfg(enabled=True, api_id="", api_hash="")
    svc = TelegramService(settings_service=_Settings(cfg), db=_DB(), bus=_Bus())

    with pytest.raises(RuntimeError):
        svc.start()

    status = svc.status()
    assert status["state"] == "FAILED"
    assert "incompleta" in status["last_error"].lower()


@pytest.mark.unit
def test_telegram_service_exposes_probe_snapshot():
    svc = _svc()
    svc.start()

    snapshot = svc.runtime_snapshot()

    assert snapshot["state"] == "CONNECTED"
    assert snapshot["listener_started"] is True
    # Verita' runtime: un solo handler Telethon registrato (invariant guard)
    assert snapshot["handlers_registered"] == 1
    assert snapshot["active_network_resources"] == 1
    assert snapshot["client_alive"] is True
    svc.stop()


@pytest.mark.unit
def test_telegram_service_health_status_is_probe_friendly():
    svc = _svc()
    svc.start()
    health = svc.health_status(checked_at="2026-04-15T00:00:10+00:00")

    assert set(
        (
            "state",
            "healthy",
            "degraded",
            "failed",
            "last_error",
            "reconnect_attempts",
            "reconnect_in_progress",
            "last_successful_message_ts",
            "handlers_registered",
            "client_alive",
            "intentional_stop",
            "invariant_ok",
            "active_alert_codes",
            "checked_at",
        )
    ) <= set(health)
    assert health["state"] == "CONNECTED"
    assert health["checked_at"] == "2026-04-15T00:00:10+00:00"
    svc.stop()


@pytest.mark.unit
def test_telegram_service_redundant_start_is_idempotent_and_preserves_listener_state():
    svc = _svc()
    assert svc.start()["started"] is True
    first_listener = svc.listener
    assert first_listener is not None
    first_listener.last_successful_message_ts = "2026-04-15T00:00:00+00:00"

    second = svc.start()
    second_listener = svc.listener
    status = svc.status()

    assert second["reason"] == "already_running"
    assert second_listener is first_listener
    assert status["state"] == "CONNECTED"
    assert status["connected"] is True
    assert status["last_successful_message_ts"] == "2026-04-15T00:00:00+00:00"
    svc.stop()


@pytest.mark.unit
def test_restart_action_is_idempotent():
    svc = _svc()
    svc.start()
    svc._set_state("FAILED")

    first = svc.restart()
    svc._restart_in_progress = True
    second = svc.restart()

    assert first["started"] is True
    assert second["started"] is False
    assert second["reason"] == "restart_in_progress"


@pytest.mark.unit
def test_restart_reaches_connected_with_live_runtime():
    svc = _svc()
    svc.start()
    svc._set_state("FAILED")

    result = svc.restart()
    status = svc.status()

    assert result["started"] is True
    assert status["connected"] is True
    assert status["state"] == "CONNECTED"
    svc.stop()


@pytest.mark.unit
def test_intentional_stop_never_restarts():
    svc = _svc()
    svc.start()
    svc.stop()

    outcome = svc.run_autoheal_once(
        checked_at_ts=1000.0,
        startup_grace_active=False,
        reconnect_grace_active=False,
        failure_escalated=True,
    )

    assert outcome["action"] == "NO_ACTION"
    assert svc.status()["state"] == "STOPPED"


@pytest.mark.unit
def test_reboot_like_service_reconstruction_preserves_session_path_without_false_connected_state():
    settings_cfg = _TelegramCfg(session_string="persisted-session")

    first = _svc(settings_cfg)
    first_start = first.start()
    assert first_start["started"] is True
    first.listener.last_successful_message_ts = "2026-04-15T00:00:00+00:00"
    first.stop()

    reconstructed = _svc(settings_cfg)
    second_start = reconstructed.start()
    second_status = reconstructed.status()

    assert second_start["started"] is True
    assert second_status["state"] == "CONNECTED"
    assert second_status["connected"] is True
    assert second_status["handlers_registered"] == 2
    reconstructed.stop()


@pytest.mark.unit
def test_dirty_stop_and_intentional_stop_are_distinguishable_for_restart_paths():
    dirty = _svc()
    dirty.start()
    assert dirty.listener is not None
    dirty.handlers_registered = 1
    dirty.listener.handlers_registered = 1

    dirty.listener._set_state("FAILED")
    dirty.listener.reconnect_in_progress = False
    dirty.listener.last_error = "process_crashed"
    dirty_status = dirty.status()
    dirty_autoheal = dirty.run_autoheal_once(
        checked_at_ts=2_000_000_000.0,
        startup_grace_active=False,
        reconnect_grace_active=False,
        failure_escalated=True,
    )

    intentional = _svc()
    intentional.start()
    intentional.stop()
    intentional_autoheal = intentional.run_autoheal_once(
        checked_at_ts=1200.0,
        startup_grace_active=False,
        reconnect_grace_active=False,
        failure_escalated=True,
    )

    assert dirty_status["state"] == "FAILED"
    assert dirty_autoheal["action"] == "SCHEDULE_RESTART"
    assert dirty_autoheal["restart_result"]["started"] is True
    assert dirty.status()["state"] == "CONNECTED"
    assert dirty.status()["intentional_stop"] is False
    assert intentional_autoheal["action"] == "NO_ACTION"
    dirty.stop()


@pytest.mark.unit
def test_restart_during_unstable_lifecycle_phase_is_safely_suppressed():
    svc = _svc(hang_connect=True, connect_timeout=0.2)
    svc.start()

    suppressed = svc.restart()

    assert suppressed["started"] is False
    assert suppressed["reason"] == "connection_in_progress"
    assert svc.status()["state"] == "CONNECTING"


@pytest.mark.unit
def test_repeated_crash_recovery_cycles_remain_idempotent_without_duplicate_handlers():
    svc = _svc()
    svc.start()
    svc.handlers_registered = 1

    for i in range(3):
        assert svc.listener is not None
        svc.listener.handlers_registered = 1
        svc.listener._set_state("FAILED")
        svc.listener.reconnect_in_progress = False
        svc.listener.last_error = f"crash-{i}"
        action = svc.run_autoheal_once(
            checked_at_ts=2_000_000_000.0 + (i * 25.0),
            startup_grace_active=False,
            reconnect_grace_active=False,
            failure_escalated=True,
        )
        assert action["action"] == "SCHEDULE_RESTART"
        status = svc.status()
        assert status["handlers_registered"] == 2
        assert status["state"] == "CONNECTED"
        assert status["reconnect_attempts"] == i + 1
        assert svc._restart_in_progress is False
    svc.stop()
