from core.runtime_controller import RuntimeController


class _Bus:
    def __init__(self):
        self.events = []

    def subscribe(self, *_args, **_kwargs):
        return None

    def publish(self, event, payload=None, *_args, **_kwargs):
        self.events.append((event, payload))


class _Db:
    def _execute(self, *_args, **_kwargs):
        return None


class _Settings:
    def __init__(self, *, live_enabled=True, readiness_ok=True):
        self._live_enabled = live_enabled
        self._readiness_ok = readiness_ok

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

    def load_live_enabled(self):
        return self._live_enabled

    def load_live_readiness_ok(self):
        return self._readiness_ok


class _Betfair:
    def __init__(self):
        self.connect_calls = 0

    def set_simulation_mode(self, *_args, **_kwargs):
        return None

    def connect(self, **_kwargs):
        self.connect_calls += 1
        return {"ok": True}

    def get_account_funds(self):
        return {"available": 0.0}

    def status(self):
        return {"connected": True}


class _Telegram:
    def start(self):
        return {"ok": True}

    def status(self):
        return {"connected": True}


class _Probe:
    def __init__(self, report):
        self._report = report

    def get_live_readiness_report(self):
        return self._report


class _SafeMode:
    def __init__(self, enabled=False):
        self._enabled = enabled

    def is_enabled(self):
        return self._enabled


def _runtime(*, readiness_level="READY", blockers=None, kill_switch=False, live_enabled=True, readiness_ok=True):
    bus = _Bus()
    rc = RuntimeController(
        bus=bus,
        db=_Db(),
        settings_service=_Settings(live_enabled=live_enabled, readiness_ok=readiness_ok),
        betfair_service=_Betfair(),
        telegram_service=_Telegram(),
        safe_mode=_SafeMode(kill_switch),
    )
    rc.runtime_probe = _Probe(
        {
            "ready": readiness_level == "READY" and not blockers,
            "level": readiness_level,
            "blockers": list(blockers or []),
        }
    )
    rc.enforce_probe_readiness_gate = True
    return rc


def test_live_requested_but_not_ready_is_no_go():
    rc = _runtime(readiness_level="NOT_READY")

    result = rc.start(execution_mode="LIVE", live_enabled=True, live_readiness_ok=True)

    assert result["refused"] is True
    assert result["reason_code"] == "live_readiness_not_ok"
    assert result["deploy_gate_reason_code"] == "DEPLOY_BLOCKED_NOT_READY"
    assert rc.betfair_service.connect_calls == 0


def test_live_requested_and_ready_is_go():
    rc = _runtime(readiness_level="READY")

    result = rc.start(execution_mode="LIVE", live_enabled=True, live_readiness_ok=True)

    assert result["started"] is True
    assert rc.execution_mode == "LIVE"
    assert rc.betfair_service.connect_calls == 1


def test_live_requested_with_blockers_is_no_go():
    rc = _runtime(readiness_level="READY", blockers=[{"code": "X"}])

    result = rc.start(execution_mode="LIVE", live_enabled=True, live_readiness_ok=True)

    assert result["refused"] is True
    assert result["reason_code"] == "live_readiness_not_ok"
    assert result["deploy_gate_reason_code"] == "DEPLOY_BLOCKED_BLOCKERS_PRESENT"
    assert rc.betfair_service.connect_calls == 0


def test_live_requested_with_kill_switch_is_no_go():
    rc = _runtime(readiness_level="READY", kill_switch=True)

    result = rc.start(execution_mode="LIVE", live_enabled=True, live_readiness_ok=True)

    assert result["refused"] is True
    assert result["reason_code"] == "kill_switch_active"
    assert result["deploy_gate_reason_code"] == "DEPLOY_BLOCKED_KILL_SWITCH"
    assert rc.betfair_service.connect_calls == 0


def test_simulation_mode_always_allowed():
    rc = _runtime(readiness_level="NOT_READY", blockers=[{"code": "X"}], readiness_ok=False)

    result = rc.start(execution_mode="SIMULATION", live_enabled=False, live_readiness_ok=False)

    assert result["started"] is True
    assert rc.execution_mode == "SIMULATION"
    assert rc.betfair_service.connect_calls == 1
