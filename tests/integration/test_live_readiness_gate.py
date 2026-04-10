from core.runtime_controller import RuntimeController


class _Bus:
    def __init__(self):
        self.events = []

    def subscribe(self, *_args, **_kwargs):
        return None

    def publish(self, event_name, payload=None):
        self.events.append((event_name, payload))


class _Db:
    def _execute(self, *_args, **_kwargs):
        return None


class _Settings:
    def __init__(self, *, live_enabled=True, live_ready=True):
        self.live_enabled = live_enabled
        self.live_ready = live_ready

    def load_roserpina_config(self):
        class _Cfg:
            table_count = 2
            anti_duplication_enabled = False
            allow_recovery = False
            auto_reset_drawdown_pct = 99
            defense_drawdown_pct = 7.5
            lockdown_drawdown_pct = 99

            def __getattr__(self, _name):
                return 0

        return _Cfg()

    def load_live_enabled(self):
        return self.live_enabled

    def load_live_readiness_ok(self):
        return self.live_ready


class _Betfair:
    def __init__(self, *, connected=True, has_connect=True):
        self.connected = connected
        self.connect_calls = []
        if has_connect:
            self.connect = self._connect

    def _connect(self, **kwargs):
        self.connect_calls.append(kwargs)
        return {"ok": True}

    def set_simulation_mode(self, _enabled):
        return None

    def status(self):
        return {"connected": self.connected}

    def get_account_funds(self):
        return {"available": 100.0}


class _Telegram:
    def __init__(self, *, connected=True):
        self.connected = connected

    def start(self):
        return {"ok": True}

    def status(self):
        return {"connected": self.connected}


class _KillSwitch:
    def __init__(self, enabled):
        self.enabled = enabled

    def is_enabled(self):
        return self.enabled


def _runtime(*, live_enabled=True, live_ready=True, kill_switch=False, has_connect=True):
    return RuntimeController(
        bus=_Bus(),
        db=_Db(),
        settings_service=_Settings(live_enabled=live_enabled, live_ready=live_ready),
        betfair_service=_Betfair(has_connect=has_connect),
        telegram_service=_Telegram(),
        safe_mode=_KillSwitch(kill_switch),
    )


def test_default_minimal_startup_is_not_accidentally_live_ready():
    rc = _runtime(live_enabled=False, live_ready=False)

    readiness = rc.evaluate_live_readiness(execution_mode="LIVE", live_enabled=False)

    assert readiness["ready"] is False
    assert readiness["level"] == "NOT_READY"
    assert "LIVE_NOT_ENABLED" in readiness["blockers"]


def test_incomplete_runtime_reports_runtime_init_blocker():
    rc = _runtime(live_enabled=True, live_ready=True)
    rc.reconciliation_engine = None

    readiness = rc.evaluate_live_readiness(execution_mode="LIVE", live_enabled=True)

    assert readiness["ready"] is False
    assert "RUNTIME_NOT_INITIALIZED" in readiness["blockers"]


def test_missing_live_dependency_reports_dependency_blocker():
    rc = _runtime(live_enabled=True, live_ready=True, has_connect=False)

    readiness = rc.evaluate_live_readiness(execution_mode="LIVE", live_enabled=True)

    assert readiness["ready"] is False
    assert "LIVE_DEPENDENCY_MISSING" in readiness["blockers"]


def test_kill_switch_active_reports_blocker():
    rc = _runtime(live_enabled=True, live_ready=True, kill_switch=True)

    readiness = rc.evaluate_live_readiness(execution_mode="LIVE", live_enabled=True)

    assert readiness["ready"] is False
    assert "KILL_SWITCH_ACTIVE" in readiness["blockers"]


def test_valid_live_ready_runtime_is_approved_for_live():
    rc = _runtime(live_enabled=True, live_ready=True)

    readiness = rc.evaluate_live_readiness(execution_mode="LIVE", live_enabled=True)
    result = rc.start(execution_mode="LIVE", live_enabled=True)

    assert readiness["ready"] is True
    assert readiness["level"] == "READY"
    assert readiness["blockers"] == []
    assert rc.is_live_readiness_ok(execution_mode="LIVE", live_enabled=True) is True
    assert result["started"] is True
    assert rc.execution_mode == "LIVE"


def test_live_requested_with_readiness_false_is_refused():
    rc = _runtime(live_enabled=True, live_ready=False)

    result = rc.start(execution_mode="LIVE", live_enabled=True)

    assert result["refused"] is True
    assert result["reason_code"] == "live_readiness_not_ok"
    assert "LIVE_READINESS_FLAG_NOT_OK" in result["readiness"]["blockers"]
