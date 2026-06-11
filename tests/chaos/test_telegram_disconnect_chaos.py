from __future__ import annotations

from dataclasses import dataclass

import pytest

from recovery.telegram_autoheal import (
    TelegramAutohealHistory,
    TelegramAutohealPolicy,
    TelegramAutohealSnapshot,
)
from services.telegram_service import TelegramService


class _FakeTelethonClient:
    def __init__(self):
        self._disconnected = None

    async def connect(self):
        import asyncio as _aio
        self._disconnected = _aio.Event()

    async def is_user_authorized(self):
        return True

    def add_event_handler(self, callback, event_filter=None):
        """No-op: il fake accetta la registrazione senza usare il filtro."""

    def is_connected(self):
        return self._disconnected is not None and not self._disconnected.is_set()

    async def run_until_disconnected(self):
        await self._disconnected.wait()

    async def disconnect(self):
        if self._disconnected is not None:
            self._disconnected.set()


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
        if self.cfg.monitored_chat_ids is None:
            self.cfg.monitored_chat_ids = [1001]
        return self.cfg


class _DB:
    def save_received_signal(self, payload):
        _ = payload


class _Bus:
    def publish(self, topic, payload):
        _ = topic, payload


@pytest.mark.chaos
def test_disconnect_storm_keeps_recovery_deterministic_and_idempotent():
    svc = TelegramService(
        settings_service=_Settings(_TelegramCfg()),
        db=_DB(),
        bus=_Bus(),
        client_factory=lambda *_a: _FakeTelethonClient(),
        connect_timeout=5.0,
    )
    svc.start()

    actions = []
    for cycle in range(4):
        checked_at = 2_000_000_000.0 + (cycle * 40.0)
        assert svc.listener is not None
        svc.handlers_registered = 1
        svc.listener.handlers_registered = 1
        svc.listener._set_state("FAILED")
        svc.listener.reconnect_in_progress = False
        svc.listener.last_error = f"disconnect-cycle-{cycle}"
        outcome = svc.run_autoheal_once(
            checked_at_ts=checked_at,
            startup_grace_active=False,
            reconnect_grace_active=False,
            failure_escalated=True,
        )
        actions.append(outcome["action"])

        status = svc.status()
        assert status["handlers_registered"] == 2
        assert status["state"] == "CONNECTED"
        assert status["reconnect_attempts"] == cycle + 1
        assert svc._restart_in_progress is False

    assert actions == ["SCHEDULE_RESTART", "SCHEDULE_RESTART", "SCHEDULE_RESTART", "SCHEDULE_RESTART"]
    svc.stop()


@pytest.mark.chaos
def test_disconnect_storm_respects_cooldown_then_allows_recovery_after_time_advance():
    policy = TelegramAutohealPolicy(max_restarts_in_window=2, restart_window_sec=300, restart_cooldown_sec=30)
    snapshot = TelegramAutohealSnapshot(
        state="FAILED",
        invariant_ok=True,
        active_alert_codes=tuple(),
        reconnect_attempts=1,
        restart_attempts_total=0,
        restart_in_progress=False,
        intentional_stop=False,
        startup_grace_active=False,
        reconnect_grace_active=False,
        lockout_active=False,
        last_error_category="disconnect",
        failure_escalated=True,
        listener_stale=False,
        now_ts=1000.0,
    )
    first = policy.evaluate(snapshot, TelegramAutohealHistory(restart_timestamps=(900.0,), lockout_since_ts=None))

    suppressed = policy.evaluate(
        TelegramAutohealSnapshot(**{**snapshot.__dict__, "now_ts": 1010.0}),
        TelegramAutohealHistory(restart_timestamps=(995.0,), lockout_since_ts=None),
    )
    resumed = policy.evaluate(
        TelegramAutohealSnapshot(**{**snapshot.__dict__, "now_ts": 1045.0}),
        TelegramAutohealHistory(restart_timestamps=(995.0,), lockout_since_ts=None),
    )
    lockout = policy.evaluate(
        TelegramAutohealSnapshot(**{**snapshot.__dict__, "now_ts": 1060.0, "reconnect_attempts": 2}),
        TelegramAutohealHistory(restart_timestamps=(1000.0, 1040.0), lockout_since_ts=None),
    )

    assert first.action.name == "SCHEDULE_RESTART"
    assert suppressed.action.name == "SUPPRESS_RESTART"
    assert suppressed.reason == "restart_cooldown_active"
    assert resumed.action.name == "SCHEDULE_RESTART"
    assert lockout.action.name == "ENTER_FAILED_LOCKOUT"
