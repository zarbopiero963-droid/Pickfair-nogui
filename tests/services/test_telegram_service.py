from __future__ import annotations

from dataclasses import dataclass

import pytest

from services.telegram_service import TelegramService


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
def test_telegram_service_exposes_explicit_lifecycle_truth_without_fake_connected():
    svc = TelegramService(settings_service=_Settings(_TelegramCfg()), db=_DB(), bus=_Bus())

    start = svc.start()
    status = svc.status()

    assert start["started"] is True
    assert start["state"] == "CONNECTING"
    assert start["connected"] is False
    assert status["state"] == "CONNECTING"
    assert status["running"] is True
    assert status["connected"] is False
    assert status["listener_started"] is True
    assert status["active_network_resources"] == 0
    assert "reconnect_attempts" in status


@pytest.mark.unit
def test_telegram_service_start_is_idempotent_and_does_not_duplicate_listener_setup():
    svc = TelegramService(settings_service=_Settings(_TelegramCfg()), db=_DB(), bus=_Bus())
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
    svc = TelegramService(settings_service=_Settings(_TelegramCfg()), db=_DB(), bus=_Bus())
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
    svc = TelegramService(settings_service=_Settings(_TelegramCfg()), db=_DB(), bus=_Bus())
    svc.start()

    result = svc.restart()
    status = svc.status()

    assert result["started"] is True
    assert status["state"] == "CONNECTING"
    assert status["reconnect_attempts"] >= 1
    assert status["reconnect_in_progress"] is False


@pytest.mark.unit
def test_telegram_listener_callback_failure_is_isolated_and_status_remains_coherent():
    svc = TelegramService(settings_service=_Settings(_TelegramCfg()), db=_DB(), bus=_Bus())
    svc.start()
    listener = svc.listener
    assert listener is not None

    def _boom(*_args, **_kwargs):
        raise RuntimeError("boom")

    listener.set_callbacks(on_signal=svc._handle_signal, on_status=_boom)

    listener._emit_status("INFO", "hello")
    status = svc.status()

    assert status["state"] in {"CONNECTING", "STOPPED", "FAILED"}
    assert status["connected"] is False


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
    svc = TelegramService(settings_service=_Settings(_TelegramCfg()), db=_DB(), bus=_Bus())
    svc.start()

    snapshot = svc.runtime_snapshot()

    assert snapshot["state"] == "CONNECTING"
    assert snapshot["listener_started"] is True
    assert snapshot["handlers_registered"] == 2
    assert snapshot["active_network_resources"] == 0
    assert snapshot["client_alive"] is False


@pytest.mark.unit
def test_telegram_service_health_status_is_probe_friendly():
    svc = TelegramService(settings_service=_Settings(_TelegramCfg()), db=_DB(), bus=_Bus())
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
    assert health["state"] == "CONNECTING"
    assert health["checked_at"] == "2026-04-15T00:00:10+00:00"


@pytest.mark.unit
def test_telegram_service_redundant_start_is_idempotent_and_preserves_listener_state():
    svc = TelegramService(settings_service=_Settings(_TelegramCfg()), db=_DB(), bus=_Bus())
    first = svc.start()
    first_listener = svc.listener
    assert first_listener is not None
    first_listener.last_successful_message_ts = "2026-04-15T00:00:00+00:00"

    second = svc.start()
    second_listener = svc.listener
    status = svc.status()

    assert second["reason"] == "already_running"
    assert second_listener is first_listener
    assert status["state"] == "CONNECTING"
    assert status["connected"] is False
    assert status["last_successful_message_ts"] == "2026-04-15T00:00:00+00:00"
