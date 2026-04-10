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
    def __init__(self, *, ready=True):
        self._ready = ready

    def load_roserpina_config(self):
        class Cfg:
            table_count = 1

            def __getattr__(self, _name):
                return 0

        return Cfg()

    def load_live_readiness_ok(self):
        return self._ready


class _Betfair:
    def set_simulation_mode(self, _enabled):
        return None

    def connect(self, **_kwargs):
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


class _SafeMode:
    def __init__(self, enabled):
        self._enabled = enabled

    def is_enabled(self):
        return self._enabled


class _Probe:
    def __init__(self, report):
        self._report = report

    def get_live_readiness_report(self):
        return self._report


def _make_runtime(*, ready=True, safe_mode=False, betfair_service=None):
    return RuntimeController(
        bus=_Bus(),
        db=_Db(),
        settings_service=_Settings(ready=ready),
        betfair_service=betfair_service or _Betfair(),
        telegram_service=_Telegram(),
        safe_mode=_SafeMode(safe_mode),
    )


@pytest.mark.parametrize("readiness_signal", ["UNKNOWN", "maybe", "unexpected"])
def test_runtime_incomplete_or_unknown_signal_blocks_live(readiness_signal):
    rc = _make_runtime(ready=True)

    readiness = rc.evaluate_live_readiness(
        execution_mode="LIVE",
        live_enabled=True,
        live_readiness_ok=readiness_signal,
    )

    assert readiness["ready"] is False
    assert "LIVE_READINESS_FLAG_NOT_OK" in readiness["blockers"]


class _BetfairMissingConnect:
    def status(self):
        return {"connected": True}


def test_missing_live_dependency_reports_blocker_from_runtime_layer():
    rc = _make_runtime(ready=True, betfair_service=_BetfairMissingConnect())

    readiness = rc.evaluate_live_readiness(
        execution_mode="LIVE",
        live_enabled=True,
        live_readiness_ok=True,
    )

    assert readiness["ready"] is False
    assert "LIVE_DEPENDENCY_MISSING" in readiness["blockers"]


def test_contradictory_state_live_requested_but_not_enabled_reports_blocker():
    rc = _make_runtime(ready=True)

    readiness = rc.evaluate_live_readiness(
        execution_mode="LIVE",
        live_enabled=False,
        live_readiness_ok=True,
    )

    assert readiness["ready"] is False
    assert "LIVE_NOT_ENABLED" in readiness["blockers"]
    assert "CONTRADICTORY_STATE" in readiness["blockers"]


@pytest.mark.parametrize("mode", [None, "", "garbage"])
def test_malformed_execution_context_fails_closed(mode):
    rc = _make_runtime(ready=True)

    readiness = rc.evaluate_live_readiness(
        execution_mode=mode,
        live_enabled=True,
        live_readiness_ok=True,
    )

    assert readiness["ready"] is False
    assert readiness["level"] in {"DEGRADED", "NOT_READY"}
    if mode not in (None, ""):
        assert "INVALID_EXECUTION_MODE" in readiness["blockers"]


def test_kill_switch_active_blocks_live_with_correct_blockers():
    rc = _make_runtime(ready=True, safe_mode=True)

    readiness = rc.evaluate_live_readiness(
        execution_mode="LIVE",
        live_enabled=True,
        live_readiness_ok=True,
    )

    assert readiness["ready"] is False
    assert "KILL_SWITCH_ACTIVE" in readiness["blockers"]
    assert "SAFE_MODE_BLOCKING" in readiness["blockers"]


def test_startup_error_reports_fail_closed_blocker():
    rc = _make_runtime(ready=True)
    rc.last_error = "boom"

    readiness = rc.evaluate_live_readiness(
        execution_mode="LIVE",
        live_enabled=True,
        live_readiness_ok=True,
    )

    assert readiness["ready"] is False
    assert "STARTUP_FAILED" in readiness["blockers"]


def test_runtime_missing_readiness_signal_from_settings_fails_closed():
    rc = _make_runtime(ready=False)

    readiness = rc.evaluate_live_readiness(
        execution_mode="LIVE",
        live_enabled=True,
        live_readiness_ok=None,
    )

    assert readiness["ready"] is False
    assert "LIVE_READINESS_FLAG_NOT_OK" in readiness["blockers"]


@pytest.mark.parametrize(
    ("probe_report", "probe_reason"),
    [
        ({"ready": True, "level": "NOPE", "blockers": []}, "probe_report_level_not_ready"),
        ({"ready": True, "level": "READY", "blockers": ["X"]}, "probe_report_has_blockers"),
        ({"ready": True, "level": "READY", "blockers": {"bad": "shape"}}, "probe_report_blockers_not_list"),
        ({"ready": False, "level": "READY", "blockers": []}, "probe_report_ready_false"),
    ],
)
def test_live_start_with_contradictory_or_malformed_probe_payloads_fails_closed(probe_report, probe_reason):
    rc = _make_runtime(ready=True)
    rc.enforce_probe_readiness_gate = True
    rc.runtime_probe = _Probe(probe_report)

    result = rc.start(execution_mode="LIVE", live_enabled=True, live_readiness_ok=True)

    assert result["ok"] is False
    assert result["started"] is False
    assert result["refused"] is True
    assert result["effective_execution_mode"] == "SIMULATION"
    assert result["readiness"]["probe_ok"] is False
    assert result["readiness"]["details"]["probe"]["reason"] == probe_reason
    assert rc.execution_mode == "SIMULATION"
