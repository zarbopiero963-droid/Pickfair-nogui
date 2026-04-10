from core.runtime_controller import RuntimeController
from core.system_state import RuntimeMode


class _Bus:
    def subscribe(self, *_args, **_kwargs):
        return None

    def publish(self, *_args, **_kwargs):
        return None


class _Db:
    def _execute(self, *_args, **_kwargs):
        return None


class _Settings:
    def __init__(self, live_ready=True):
        self.live_ready = live_ready

    def load_roserpina_config(self):
        class _Cfg:
            table_count = 1
            anti_duplication_enabled = False
            allow_recovery = False
            auto_reset_drawdown_pct = 90
            defense_drawdown_pct = 7.5
            lockdown_drawdown_pct = 99

            def __getattr__(self, _name):
                return 0

        return _Cfg()

    def load_live_enabled(self):
        return True

    def load_live_readiness_ok(self):
        return self.live_ready


class _Betfair:
    def __init__(self, connected=True):
        self.connected = connected

    def connect(self, **_kwargs):
        return {"ok": True}

    def set_simulation_mode(self, *_args, **_kwargs):
        return None

    def status(self):
        return {"connected": self.connected}

    def get_account_funds(self):
        return {"available": 50.0}


class _Telegram:
    def __init__(self, connected=True):
        self.connected = connected

    def start(self):
        return {"ok": True}

    def status(self):
        return {"connected": self.connected}


def _runtime(*, live_ready=True, betfair_connected=True, telegram_connected=True):
    return RuntimeController(
        bus=_Bus(),
        db=_Db(),
        settings_service=_Settings(live_ready=live_ready),
        betfair_service=_Betfair(connected=betfair_connected),
        telegram_service=_Telegram(connected=telegram_connected),
    )


def test_blocker_reporting_is_accurate_for_startup_failure_state():
    rc = _runtime(live_ready=True)
    rc.last_error = "startup exploded"

    readiness = rc.evaluate_live_readiness(execution_mode="LIVE", live_enabled=True)

    assert readiness["ready"] is False
    assert "RUNTIME_STARTUP_FAILED" in readiness["blockers"]
    assert readiness["details"]["runtime_state"]["startup_failed"] is True


def test_unknown_runtime_state_fails_closed():
    rc = _runtime(live_ready=True)
    rc.mode = "BROKEN_STATE"

    readiness = rc.evaluate_live_readiness(execution_mode="LIVE", live_enabled=True)

    assert readiness["ready"] is False
    assert readiness["level"] == "NOT_READY"
    assert "UNKNOWN_STATE" in readiness["blockers"]


def test_contradictory_runtime_state_fails_closed():
    rc = _runtime(live_ready=True)
    rc.mode = RuntimeMode.ACTIVE
    rc.simulation_mode = True

    readiness = rc.evaluate_live_readiness(execution_mode="LIVE", live_enabled=True)

    assert readiness["ready"] is False
    assert "CONTRADICTORY_STATE" in readiness["blockers"]


def test_half_started_state_fails_closed_with_specific_blocker():
    rc = _runtime(live_ready=True, betfair_connected=False, telegram_connected=True)
    rc.mode = RuntimeMode.ACTIVE

    readiness = rc.evaluate_live_readiness(execution_mode="LIVE", live_enabled=True)

    assert readiness["ready"] is False
    assert "RUNTIME_HALF_STARTED" in readiness["blockers"]
