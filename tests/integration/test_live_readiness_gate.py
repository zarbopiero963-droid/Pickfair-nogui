import pytest

from core.runtime_controller import RuntimeController


class _Bus:
    def __init__(self):
        self.events = []
        self.subscriptions = {}

    def subscribe(self, event_name, handler):
        self.subscriptions.setdefault(event_name, []).append(handler)

    def publish(self, event_name, payload=None):
        self.events.append((event_name, payload))


class _Db:
    def _execute(self, *_args, **_kwargs):
        return None


class _Settings:
    def __init__(self, live_enabled=False, live_ready=False):
        self._live_enabled = live_enabled
        self._live_ready = live_ready

    def load_roserpina_config(self):
        class Cfg:
            table_count = 2
            anti_duplication_enabled = False
            allow_recovery = False
            auto_reset_drawdown_pct = 90
            defense_drawdown_pct = 7.5
            lockdown_drawdown_pct = 99

            def __getattr__(self, _name):
                return 0

        return Cfg()

    def load_live_enabled(self):
        return self._live_enabled

    def load_live_readiness_ok(self):
        return self._live_ready


class _Betfair:
    def __init__(self):
        self.connect_calls = []

    def set_simulation_mode(self, _enabled):
        return None

    def connect(self, password=None, simulation_mode=False):
        self.connect_calls.append((password, simulation_mode))
        return {"ok": True, "simulation_mode": simulation_mode}

    def get_account_funds(self):
        return {"available": 100.0}

    def status(self):
        return {"connected": True}

    def disconnect(self):
        return None


class _Telegram:
    def start(self):
        return {"ok": True}

    def stop(self):
        return None

    def status(self):
        return {"connected": True}


class _KillSwitch:
    def __init__(self, enabled):
        self._enabled = enabled

    def is_enabled(self):
        return self._enabled


def _make_runtime(*, live_enabled=False, live_ready=False, kill_switch=False):
    return RuntimeController(
        bus=_Bus(),
        db=_Db(),
        settings_service=_Settings(live_enabled=live_enabled, live_ready=live_ready),
        betfair_service=_Betfair(),
        telegram_service=_Telegram(),
        safe_mode=_KillSwitch(kill_switch),
    )


def test_default_startup_is_not_accidentally_live_ready():
    rc = _make_runtime(live_enabled=False, live_ready=False)

    result = rc.start()

    assert result["started"] is True
    assert rc.execution_mode == "SIMULATION"
    assert rc.live_enabled is False
    assert rc.live_readiness_ok is False


def test_runtime_incomplete_readiness_is_false():
    rc = _make_runtime(live_enabled=True, live_ready=True)

    result = rc.start(execution_mode="LIVE", live_enabled=True, live_readiness_ok="UNKNOWN")

    assert result["refused"] is True
    assert result["reason_code"] == "live_readiness_not_ok"
    assert rc.execution_mode == "SIMULATION"


def test_missing_live_dependency_readiness_loader_fails_closed():
    class _SettingsNoReadiness(_Settings):
        pass

    _SettingsNoReadiness.load_live_readiness_ok = None
    rc = RuntimeController(
        bus=_Bus(),
        db=_Db(),
        settings_service=_SettingsNoReadiness(live_enabled=True, live_ready=True),
        betfair_service=_Betfair(),
        telegram_service=_Telegram(),
        safe_mode=_KillSwitch(False),
    )

    result = rc.start(execution_mode="LIVE", live_enabled=True)

    assert result["refused"] is True
    assert result["reason_code"] == "live_readiness_not_ok"
    assert rc.betfair_service.connect_calls == []


def test_kill_switch_active_forces_not_ready_for_live():
    rc = _make_runtime(live_enabled=True, live_ready=True, kill_switch=True)

    result = rc.start(execution_mode="LIVE", live_enabled=True)

    assert result["refused"] is True
    assert result["reason_code"] == "kill_switch_active"
    assert rc.execution_mode == "SIMULATION"


def test_valid_live_ready_state_allows_live():
    rc = _make_runtime(live_enabled=True, live_ready=True)

    result = rc.start(execution_mode="LIVE", live_enabled=True)

    assert result["started"] is True
    assert rc.execution_mode == "LIVE"
    assert rc.live_enabled is True
    assert rc.live_readiness_ok is True


@pytest.mark.parametrize("bad_mode", [None, "", "paper", "unknown"]) 
def test_invalid_or_missing_execution_mode_context_is_not_approved(bad_mode):
    rc = _make_runtime(live_enabled=True, live_ready=True)

    result = rc.start(execution_mode=bad_mode, live_enabled=True)

    assert result["started"] is True
    assert rc.execution_mode == "SIMULATION"
    assert rc.last_execution_gate_reason in {"simulation_mode_forced", "invalid_or_missing_execution_mode"}
