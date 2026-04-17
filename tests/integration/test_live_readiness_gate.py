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
    def __init__(self, key_source="unknown"):
        class _Cipher:
            def __init__(self, src):
                self.key_source = src

        self._cipher = _Cipher(key_source)

    def _execute(self, *_args, **_kwargs):
        return None


class _Settings:
    def __init__(self, live_enabled=False, live_ready=False, strict_live_key_source_required=False):
        self._live_enabled = live_enabled
        self._live_ready = live_ready
        self._strict_live_key_source_required = strict_live_key_source_required

    def load_roserpina_config(self):
        class Cfg:
            table_count = 2
            anti_duplication_enabled = False
            allow_recovery = False
            auto_reset_drawdown_pct = 90
            defense_drawdown_pct = 7.5
            lockdown_drawdown_pct = 99
            max_daily_loss = 100.0
            max_drawdown_hard_stop_pct = 20.0
            max_open_exposure = 250.0

            def __getattr__(self, _name):
                return 0

        return Cfg()

    def load_live_enabled(self):
        return self._live_enabled

    def load_live_readiness_ok(self):
        return self._live_ready

    def load_strict_live_key_source_required(self):
        return self._strict_live_key_source_required


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


class _Probe:
    def __init__(self, report):
        self._report = report

    def get_live_readiness_report(self):
        return self._report



def _make_runtime(
    *,
    live_enabled=False,
    live_ready=False,
    kill_switch=False,
    betfair_service=None,
    key_source="unknown",
    strict_live_key_source_required=False,
):
    return RuntimeController(
        bus=_Bus(),
        db=_Db(key_source=key_source),
        settings_service=_Settings(
            live_enabled=live_enabled,
            live_ready=live_ready,
            strict_live_key_source_required=strict_live_key_source_required,
        ),
        betfair_service=betfair_service or _Betfair(),
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


class _BetfairMissingConnect:
    def set_simulation_mode(self, _enabled):
        return None

    def get_account_funds(self):
        return {"available": 100.0}

    def status(self):
        return {"connected": True}


def test_missing_live_dependency_object_path_fails_closed():
    rc = _make_runtime(
        live_enabled=True,
        live_ready=True,
        betfair_service=_BetfairMissingConnect(),
    )

    result = rc.start(execution_mode="LIVE", live_enabled=True)

    assert result["refused"] is True
    assert result["reason_code"] == "live_readiness_not_ok"
    assert "LIVE_DEPENDENCY_MISSING" in result["readiness"]["blockers"]


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


def test_key_source_ephemeral_does_not_change_behavior_when_strict_flag_off():
    rc = _make_runtime(
        live_enabled=True,
        live_ready=True,
        key_source="ephemeral",
        strict_live_key_source_required=False,
    )

    readiness = rc.evaluate_live_readiness(
        execution_mode="LIVE",
        live_enabled=True,
        live_readiness_ok=True,
    )

    assert readiness["ready"] is True
    assert readiness["details"]["key_source_state"]["strict_live_key_source_required"] is False
    assert "LIVE_KEY_SOURCE_UNSAFE" not in readiness["blockers"]


@pytest.mark.parametrize("allowed_key_source", ["env", "file_existing", "file_generated"])
def test_strict_key_source_on_allows_live_for_safe_sources(allowed_key_source):
    rc = _make_runtime(
        live_enabled=True,
        live_ready=True,
        key_source=allowed_key_source,
        strict_live_key_source_required=True,
    )

    readiness = rc.evaluate_live_readiness(
        execution_mode="LIVE",
        live_enabled=True,
        live_readiness_ok=True,
    )

    assert readiness["ready"] is True
    assert readiness["details"]["key_source_state"]["key_source"] == allowed_key_source
    assert readiness["details"]["key_source_state"]["strict_live_key_source_required"] is True
    assert "LIVE_KEY_SOURCE_UNSAFE" not in readiness["blockers"]


@pytest.mark.parametrize("bad_mode", [None, "", "paper", "unknown"])
def test_invalid_or_missing_execution_mode_context_is_not_approved(bad_mode):
    rc = _make_runtime(live_enabled=True, live_ready=True)

    result = rc.start(execution_mode=bad_mode, live_enabled=True)

    assert result["started"] is True
    assert rc.execution_mode == "SIMULATION"
    assert rc.last_execution_gate_reason in {"simulation_mode_forced", "invalid_or_missing_execution_mode"}


@pytest.mark.parametrize(
    ("probe_report", "probe_reason", "_case_id"),
    [
        (None, "probe_report_malformed", "probe-report-none"),
        ("not-a-dict", "probe_report_malformed", "probe-report-string"),
        (["also", "not", "a", "dict"], "probe_report_malformed", "probe-report-list"),
        ({"level": "READY", "blockers": []}, "probe_report_missing_required_fields", "probe-missing-ready"),
        ({"ready": True, "blockers": []}, "probe_report_missing_required_fields", "probe-missing-level"),
        ({"ready": "yes", "level": "READY", "blockers": []}, "probe_report_ready_not_bool", "probe-ready-not-bool"),
        ({"ready": True, "level": "GO", "blockers": []}, "probe_report_level_not_ready", "probe-ready-level-not-ready"),
        ({"ready": True, "level": "READY", "blockers": "BLOCKER"}, "probe_report_blockers_not_list", "probe-blockers-not-list"),
        ({"ready": True, "level": "READY", "blockers": ["BLOCKER_X"]}, "probe_report_has_blockers", "probe-level-ready-blockers-present"),
        (
            {"ready": True, "level": "READY", "blockers": [{"kind": "MALFORMED_BLOCKER"}]},
            "probe_report_has_blockers",
            "probe-ready-true-malformed-blockers-payload",
        ),
        (
            {"ready": False, "level": "READY", "blockers": []},
            "probe_report_ready_false",
            "probe-contradictory-ready-false-level-ready",
        ),
        (
            {"ready": False, "level": "READY", "blockers": ["CONTRADICTORY_STATE"]},
            "probe_report_has_blockers",
            "probe-contradictory-ready-false-with-blocker",
        ),
    ],
)
def test_live_start_refused_when_probe_report_is_malformed(probe_report, probe_reason, _case_id):
    rc = _make_runtime(live_enabled=True, live_ready=True)
    rc.enforce_probe_readiness_gate = True
    rc.runtime_probe = _Probe(probe_report)

    first = rc.start(execution_mode="LIVE", live_enabled=True, live_readiness_ok=True)
    second = rc.start(execution_mode="LIVE", live_enabled=True, live_readiness_ok=True)

    for result in (first, second):
        assert result["ok"] is False
        assert result["refused"] is True
        assert result["started"] is False
        assert result["reason_code"] == "live_readiness_not_ok"
        assert result["effective_execution_mode"] == "SIMULATION"
        assert result["readiness"]["details"]["probe"]["report"] == (
            probe_report if isinstance(probe_report, dict) else {}
        )
        assert result["readiness"]["probe_ok"] is False
        assert result["readiness"]["details"]["probe"]["reason"] == probe_reason

    assert rc.execution_mode == "SIMULATION"
    assert rc.mode.name == "STOPPED"
    assert rc.betfair_service.connect_calls == []
