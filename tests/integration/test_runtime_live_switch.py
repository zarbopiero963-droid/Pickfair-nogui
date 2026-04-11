import pytest

from core.runtime_controller import RuntimeController


class _Bus:
    def subscribe(self, *_args, **_kwargs):
        return None

    def publish(self, *_args, **_kwargs):
        return None


class _Db:
    def _execute(self, *_args, **_kwargs):
        return None


class _Settings:
    def __init__(self, live_enabled=False, live_ready=False):
        self._live_enabled = live_enabled
        self._live_ready = live_ready

    def load_roserpina_config(self):
        class Cfg:
            table_count = 1
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
    def status(self):
        return {"connected": True}


class _Telegram:
    def status(self):
        return {"connected": True}


class _SafeMode:
    def __init__(self, enabled=False):
        self._enabled = enabled

    def is_enabled(self):
        return self._enabled


def _mk_runtime(*, execution_mode="SIMULATION", live_enabled=False, live_ready=False, kill_switch=False):
    rc = RuntimeController(
        bus=_Bus(),
        db=_Db(),
        settings_service=_Settings(live_enabled=live_enabled, live_ready=live_ready),
        betfair_service=_Betfair(),
        telegram_service=_Telegram(),
        safe_mode=_SafeMode(kill_switch),
    )
    rc.execution_mode = execution_mode
    rc.live_enabled = bool(live_enabled)
    rc.live_readiness_ok = bool(live_ready)
    return rc


@pytest.mark.integration
def test_default_mode_is_simulation():
    rc = _mk_runtime()
    assert rc.get_effective_execution_mode() == "SIMULATION"


@pytest.mark.integration
def test_live_requested_but_not_enabled_returns_simulation():
    rc = _mk_runtime(execution_mode="LIVE", live_enabled=False, live_ready=True)
    assert rc.get_effective_execution_mode() == "SIMULATION"


@pytest.mark.integration
def test_live_enabled_and_ready_returns_live():
    rc = _mk_runtime(execution_mode="LIVE", live_enabled=True, live_ready=True)
    assert rc.get_effective_execution_mode() == "LIVE"


@pytest.mark.integration
def test_kill_switch_forces_simulation():
    rc = _mk_runtime(execution_mode="LIVE", live_enabled=True, live_ready=True, kill_switch=True)
    assert rc.get_effective_execution_mode() == "SIMULATION"


@pytest.mark.integration
def test_unknown_readiness_forces_simulation():
    rc = _mk_runtime(execution_mode="LIVE", live_enabled=True, live_ready=True)
    rc.get_deploy_gate_status = lambda **_kwargs: {
        "allowed": True,
        "readiness": "UNKNOWN",
        "effective_execution_mode": "LIVE",
    }
    assert rc.get_effective_execution_mode() == "SIMULATION"
