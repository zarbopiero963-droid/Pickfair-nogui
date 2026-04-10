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


class _Betfair:
    def __init__(self):
        self.connect_calls = 0

    def set_simulation_mode(self, *_args, **_kwargs):
        return None

    def connect(self, **_kwargs):
        self.connect_calls += 1
        return {"ok": True}

    def get_account_funds(self):
        return {"available": 1.0}

    def status(self):
        return {"connected": True}


class _Telegram:
    def start(self):
        return {"ok": True}

    def status(self):
        return {"connected": True}


class _SettingsPartial:
    def load_roserpina_config(self):
        class Cfg:
            table_count = 1
            anti_duplication_enabled = False
            allow_recovery = False
            auto_reset_drawdown_pct = 90
            defense_drawdown_pct = 7.5
            lockdown_drawdown_pct = 95

            def __getattr__(self, _name):
                return 0

        return Cfg()


def _runtime(settings):
    return RuntimeController(
        bus=_Bus(),
        db=_Db(),
        settings_service=settings,
        betfair_service=_Betfair(),
        telegram_service=_Telegram(),
    )


def test_partial_config_fails_closed():
    rc = _runtime(_SettingsPartial())

    result = rc.start(execution_mode="LIVE")

    assert result["refused"] is True
    assert result["reason_code"] == "live_not_enabled"
    assert rc.betfair_service.connect_calls == 0


def test_missing_readiness_dependency_fails_closed():
    class _SettingsNoReadiness(_SettingsPartial):
        def load_live_enabled(self):
            return True

    rc = _runtime(_SettingsNoReadiness())

    result = rc.start(execution_mode="LIVE")

    assert result["refused"] is True
    assert result["reason_code"] == "live_readiness_not_ok"
    assert rc.betfair_service.connect_calls == 0


def test_missing_setting_service_value_fails_closed():
    class _SettingsMissing(_SettingsPartial):
        def load_live_enabled(self):
            raise RuntimeError("missing value")

    rc = _runtime(_SettingsMissing())

    result = rc.start(execution_mode="LIVE")

    assert result["refused"] is True
    assert result["reason_code"] == "live_not_enabled"
    assert rc.betfair_service.connect_calls == 0


@pytest.mark.parametrize("malformed_mode", ["", "prod", "LiVe!", 123])
def test_malformed_execution_mode_fails_closed(malformed_mode):
    class _SettingsReady(_SettingsPartial):
        def load_live_enabled(self):
            return True

        def load_live_readiness_ok(self):
            return True

    rc = _runtime(_SettingsReady())

    result = rc.start(execution_mode=malformed_mode, live_enabled=True)

    assert result["started"] is True
    assert rc.execution_mode == "SIMULATION"
    assert rc.betfair_service.connect_calls == 1
