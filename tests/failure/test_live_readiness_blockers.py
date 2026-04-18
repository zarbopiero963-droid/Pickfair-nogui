import pytest

from core.runtime_controller import RuntimeController
from core.system_state import RoserpinaConfig


class _Bus:
    def subscribe(self, *_args, **_kwargs):
        return None

    def publish(self, *_args, **_kwargs):
        return None


class _Db:
    def __init__(self, key_source="unknown"):
        class _Cipher:
            def __init__(self, src):
                self.key_source = src

        self._cipher = _Cipher(key_source)

    def _execute(self, *_args, **_kwargs):
        return None


class _Settings:
    def __init__(self, *, ready=True, strict_live_key_source_required=False, config=None):
        self._ready = ready
        self._strict_live_key_source_required = strict_live_key_source_required
        self._config = config

    def load_roserpina_config(self):
        if self._config is not None:
            return self._config
        return RoserpinaConfig(table_count=1)

    def load_live_readiness_ok(self):
        return self._ready

    def load_strict_live_key_source_required(self):
        return self._strict_live_key_source_required


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


def _make_runtime(
    *,
    ready=True,
    safe_mode=False,
    betfair_service=None,
    key_source="unknown",
    strict_live_key_source_required=False,
    config=None,
):
    return RuntimeController(
        bus=_Bus(),
        db=_Db(key_source=key_source),
        settings_service=_Settings(
            ready=ready,
            strict_live_key_source_required=strict_live_key_source_required,
            config=config,
        ),
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


def test_live_readiness_with_explicit_hard_stop_config_passes_without_hard_stop_blockers():
    rc = _make_runtime(
        ready=True,
        config=RoserpinaConfig(
            table_count=1,
            max_daily_loss=100.0,
            max_drawdown_hard_stop_pct=20.0,
            max_open_exposure=200.0,
        ),
    )

    readiness = rc.evaluate_live_readiness(
        execution_mode="LIVE",
        live_enabled=True,
        live_readiness_ok=True,
    )

    assert "LIVE_HARD_STOP_CONFIG_MISSING" not in readiness["blockers"]
    assert "LIVE_HARD_STOP_CONFIG_INVALID" not in readiness["blockers"]


def test_live_readiness_missing_hard_stop_config_from_implicit_defaults_reports_missing_blocker():
    rc = _make_runtime(
        ready=True,
        config=RoserpinaConfig(table_count=1),
    )

    readiness = rc.evaluate_live_readiness(
        execution_mode="LIVE",
        live_enabled=True,
        live_readiness_ok=True,
    )

    assert readiness["ready"] is False
    assert "LIVE_HARD_STOP_CONFIG_MISSING" in readiness["blockers"]
    assert set(readiness["details"]["hard_stop_config_state"]["missing_fields"]) == {
        "max_daily_loss",
        "max_drawdown_hard_stop_pct",
        "max_open_exposure",
    }


def test_live_readiness_missing_daily_loss_reports_explicit_blocker():
    rc = _make_runtime(
        ready=True,
        config=RoserpinaConfig(
            table_count=1,
            max_daily_loss=None,
            max_drawdown_hard_stop_pct=20.0,
            max_open_exposure=200.0,
        ),
    )

    readiness = rc.evaluate_live_readiness(
        execution_mode="LIVE",
        live_enabled=True,
        live_readiness_ok=True,
    )

    assert readiness["ready"] is False
    assert "LIVE_HARD_STOP_CONFIG_MISSING" in readiness["blockers"]
    assert "max_daily_loss" in readiness["details"]["hard_stop_config_state"]["missing_fields"]


def test_live_readiness_missing_open_exposure_reports_explicit_blocker():
    rc = _make_runtime(
        ready=True,
        config=RoserpinaConfig(
            table_count=1,
            max_daily_loss=100.0,
            max_drawdown_hard_stop_pct=20.0,
            max_open_exposure=None,
        ),
    )

    readiness = rc.evaluate_live_readiness(
        execution_mode="LIVE",
        live_enabled=True,
        live_readiness_ok=True,
    )

    assert readiness["ready"] is False
    assert "LIVE_HARD_STOP_CONFIG_MISSING" in readiness["blockers"]
    assert "max_open_exposure" in readiness["details"]["hard_stop_config_state"]["missing_fields"]


@pytest.mark.parametrize(
    ("max_daily_loss", "max_drawdown_hard_stop_pct", "max_open_exposure"),
    [
        (0.0, 20.0, 200.0),
        (100.0, 0.0, 200.0),
        (100.0, 101.0, 200.0),
        (100.0, 20.0, -1.0),
    ],
)
def test_live_readiness_invalid_hard_stop_values_report_explicit_blocker(
    max_daily_loss,
    max_drawdown_hard_stop_pct,
    max_open_exposure,
):
    rc = _make_runtime(
        ready=True,
        config=RoserpinaConfig(
            table_count=1,
            max_daily_loss=max_daily_loss,
            max_drawdown_hard_stop_pct=max_drawdown_hard_stop_pct,
            max_open_exposure=max_open_exposure,
        ),
    )

    readiness = rc.evaluate_live_readiness(
        execution_mode="LIVE",
        live_enabled=True,
        live_readiness_ok=True,
    )

    assert readiness["ready"] is False
    assert "LIVE_HARD_STOP_CONFIG_INVALID" in readiness["blockers"]
    assert readiness["details"]["hard_stop_config_state"]["invalid_fields"]


@pytest.mark.parametrize(
    ("max_daily_loss", "max_drawdown_hard_stop_pct", "max_open_exposure"),
    [
        (float("nan"), 20.0, 200.0),
        (100.0, float("nan"), 200.0),
        (100.0, 20.0, float("nan")),
        (float("inf"), 20.0, 200.0),
        (100.0, float("-inf"), 200.0),
        (100.0, 20.0, float("inf")),
    ],
)
def test_live_readiness_non_finite_hard_stop_values_are_invalid(
    max_daily_loss,
    max_drawdown_hard_stop_pct,
    max_open_exposure,
):
    rc = _make_runtime(
        ready=True,
        config=RoserpinaConfig(
            table_count=1,
            max_daily_loss=max_daily_loss,
            max_drawdown_hard_stop_pct=max_drawdown_hard_stop_pct,
            max_open_exposure=max_open_exposure,
        ),
    )

    readiness = rc.evaluate_live_readiness(
        execution_mode="LIVE",
        live_enabled=True,
        live_readiness_ok=True,
    )

    assert readiness["ready"] is False
    assert "LIVE_HARD_STOP_CONFIG_INVALID" in readiness["blockers"]
    assert readiness["details"]["hard_stop_config_state"]["invalid_fields"]


def test_non_live_readiness_does_not_emit_hard_stop_blockers_even_if_config_invalid():
    rc = _make_runtime(
        ready=True,
        config=RoserpinaConfig(
            table_count=1,
            max_daily_loss=0.0,
            max_drawdown_hard_stop_pct=0.0,
            max_open_exposure=0.0,
        ),
    )

    readiness = rc.evaluate_live_readiness(
        execution_mode="SIMULATION",
        live_enabled=False,
        live_readiness_ok=True,
    )

    assert "LIVE_HARD_STOP_CONFIG_MISSING" not in readiness["blockers"]
    assert "LIVE_HARD_STOP_CONFIG_INVALID" not in readiness["blockers"]


@pytest.mark.parametrize("unsafe_key_source", ["ephemeral", "unknown"])
@pytest.mark.parametrize("strict_live_key_source_required", [False, True])
def test_live_key_source_blocks_unsafe_sources_even_when_strict_flag_is_off(
    unsafe_key_source,
    strict_live_key_source_required,
):
    rc = _make_runtime(
        ready=True,
        key_source=unsafe_key_source,
        strict_live_key_source_required=strict_live_key_source_required,
    )

    readiness = rc.evaluate_live_readiness(
        execution_mode="LIVE",
        live_enabled=True,
        live_readiness_ok=True,
    )

    assert readiness["ready"] is False
    assert "LIVE_KEY_SOURCE_UNSAFE" in readiness["blockers"]
    ks = readiness["details"]["key_source_state"]
    assert ks["strict_live_key_source_required"] is True
    assert ks["configured_strict_live_key_source_required"] is strict_live_key_source_required
    assert ks["key_source"] == unsafe_key_source
    assert ks["passed"] is False


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
