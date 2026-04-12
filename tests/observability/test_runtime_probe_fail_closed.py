7from core.runtime_controller import RuntimeController


class _Bus:
    def subscribe(self, *_args, **_kwargs):
        return None

    def publish(self, *_args, **_kwargs):
        return None


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


class _ProbeMissingReadiness:
    def get_live_readiness_report(self):
        return {"level": "READY", "blockers": []}


class _ProbeFailure:
    def get_live_readiness_report(self):
        raise RuntimeError("probe failed")


class _ProbeMalformed:
    def get_live_readiness_report(self):
        return {"ready": "yes", "level": "READY", "blockers": []}


class _ProbeReady:
    def get_live_readiness_report(self):
        return {"ready": True, "level": "READY", "blockers": []}


def _runtime(settings):
    rc = RuntimeController(
        bus=_Bus(),
        db=_Db(),
        settings_service=settings,
        betfair_service=_Betfair(),
        telegram_service=_Telegram(),
    )
    rc.enforce_probe_readiness_gate = True
    return rc


def test_missing_readiness_fails_closed_no_go():
    rc = _runtime(_Settings(live_enabled=True, readiness_ok=True))
    rc.runtime_probe = _ProbeMissingReadiness()

    result = rc.start(execution_mode="LIVE", live_enabled=True, live_readiness_ok=True)

    assert result["refused"] is False 
    assert result["reason_code"] == "live_readiness_not_ok"
    assert result["deploy_gate_reason_code"] == "DEPLOY_BLOCKED_NOT_READY"
    assert rc.betfair_service.connect_calls == 0


def test_probe_failure_fails_closed_no_go():
    rc = _runtime(_Settings(live_enabled=True, readiness_ok=True))
    rc.runtime_probe = _ProbeFailure()

    result = rc.start(execution_mode="LIVE", live_enabled=True, live_readiness_ok=True)

    assert result["refused"] is True
    assert result["reason_code"] == "live_readiness_not_ok"
    assert result["deploy_gate_reason_code"] == "DEPLOY_BLOCKED_NOT_READY"
    assert rc.betfair_service.connect_calls == 0


def test_malformed_state_fails_closed_no_go():
    rc = _runtime(_Settings(live_enabled=True, readiness_ok=True))
    rc.mode = object()
    rc.runtime_probe = _ProbeReady()

    result = rc.start(execution_mode="LIVE", live_enabled=True, live_readiness_ok=True)

    assert result["refused"] is True
    assert result["reason_code"] == "live_readiness_not_ok"
    assert result["deploy_gate_reason_code"] in {
        "DEPLOY_BLOCKED_BLOCKERS_PRESENT",
        "DEPLOY_BLOCKED_INVALID_STATE",
    }
    assert rc.betfair_service.connect_calls == 0


def test_inconsistent_config_fails_closed_no_go():
    rc = _runtime(_Settings(live_enabled=False, readiness_ok=True))
    rc.runtime_probe = _ProbeReady()

    result = rc.start(execution_mode="LIVE", live_enabled=False, live_readiness_ok=True)

    assert result["refused"] is True
    assert result["reason_code"] == "live_not_enabled"
    assert result["deploy_gate_reason_code"] == "DEPLOY_BLOCKED_BLOCKERS_PRESENT"
    assert rc.betfair_service.connect_calls == 0


def test_probe_payload_malformed_fails_closed_no_go():
    rc = _runtime(_Settings(live_enabled=True, readiness_ok=True))
    rc.runtime_probe = _ProbeMalformed()

    result = rc.start(execution_mode="LIVE", live_enabled=True, live_readiness_ok=True)

    assert result["refused"] is True
    assert result["reason_code"] == "live_readiness_not_ok"
    assert result["deploy_gate_reason_code"] == "DEPLOY_BLOCKED_NOT_READY"
    assert rc.betfair_service.connect_calls == 0
