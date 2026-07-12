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


def test_execution_mode_authoritative_over_simulation_mode_flag():
    # Prova lato controller (richiesta Fable, PR #350): `execution_mode` e'
    # AUTORITATIVO in RuntimeController.start. Un `simulation_mode`
    # conflittuale (False = intento LIVE) NON deve portare il runtime in
    # LIVE quando execution_mode dice SIMULATION: il fallback su
    # simulation_mode si applica SOLO con execution_mode assente (None).
    class _SettingsReady(_SettingsPartial):
        def load_live_enabled(self):
            return True

        def load_live_readiness_ok(self):
            return True

    rc = _runtime(_SettingsReady())

    result = rc.start(
        execution_mode="SIMULATION",
        simulation_mode=False,
        live_enabled=True,
    )

    assert result["started"] is True
    assert rc.execution_mode == "SIMULATION"


def test_production_runtime_always_exposes_kill_switch():
    # Invariante di contratto (#350/GPT): il RuntimeController di produzione
    # DEVE esporre emergency_stop e reset_emergency. Il ramo GUI "kill-switch
    # non disponibile" e' puramente difensivo e irraggiungibile col runtime
    # reale; questa invariante lo garantisce a livello di classe.
    assert callable(getattr(RuntimeController, "emergency_stop", None))
    assert callable(getattr(RuntimeController, "reset_emergency", None))


def test_lockdown_survives_start_until_reset_emergency():
    # Prova lato controller (Fable, PR #350): dopo emergency_stop il runtime
    # e' in LOCKDOWN e un successivo start NON lo riapre — il choke point
    # is_live_allowed() e l'entry-gate del TradingEngine
    # (_daily_loss_entry_blocked) restano chiusi. Riapre SOLO reset_emergency().
    class _SettingsReady(_SettingsPartial):
        def load_live_enabled(self):
            return True

        def load_live_readiness_ok(self):
            return True

    rc = _runtime(_SettingsReady())

    rc.emergency_stop(reason="mode_sync_failed_fail_closed")
    assert rc.is_emergency_stopped is True
    assert rc._daily_loss_entry_blocked() is True
    assert rc.is_live_allowed() is False

    # tentare start (anche LIVE esplicito) NON deve riaprire il lockdown
    rc.start(execution_mode="LIVE", live_enabled=True)
    assert rc.is_emergency_stopped is True
    assert rc._daily_loss_entry_blocked() is True
    assert rc.is_live_allowed() is False

    # unica via di recovery: reset_emergency esplicito
    rc.reset_emergency()
    assert rc.is_emergency_stopped is False


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
