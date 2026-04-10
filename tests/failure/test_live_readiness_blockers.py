import pytest

from core.safety_layer import assert_live_gate_or_refuse


def test_missing_runtime_component_blocks_live_fail_closed():
    decision = assert_live_gate_or_refuse(
        execution_mode="LIVE",
        live_enabled=True,
        live_readiness_ok=None,
        kill_switch=False,
    )

    assert decision.allowed is False
    assert decision.effective_execution_mode == "SIMULATION"
    assert decision.reason_code == "live_readiness_not_ok"


@pytest.mark.parametrize("readiness_signal", ["UNKNOWN", "maybe", "unexpected"])
def test_unknown_readiness_signal_blocks_live(readiness_signal):
    from core.runtime_controller import RuntimeController

    class _Settings:
        def load_roserpina_config(self):
            class Cfg:
                table_count = 1

                def __getattr__(self, _name):
                    return 0

            return Cfg()

        def load_live_readiness_ok(self):
            return readiness_signal

    class _Bus:
        def subscribe(self, *_args, **_kwargs):
            return None

        def publish(self, *_args, **_kwargs):
            return None

    class _Db:
        def _execute(self, *_args, **_kwargs):
            return None

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

    rc = RuntimeController(
        bus=_Bus(),
        db=_Db(),
        settings_service=_Settings(),
        betfair_service=_Betfair(),
        telegram_service=_Telegram(),
    )

    result = rc.start(execution_mode="LIVE", live_enabled=True)

    assert result["refused"] is True
    assert result["reason_code"] == "live_readiness_not_ok"


def test_contradictory_state_live_requested_but_not_enabled():
    decision = assert_live_gate_or_refuse(
        execution_mode="LIVE",
        live_enabled=False,
        live_readiness_ok=True,
        kill_switch=False,
    )

    assert decision.allowed is False
    assert decision.reason_code == "live_not_enabled"


@pytest.mark.parametrize(
    "mode,enabled,ready",
    [
        (None, True, True),
        ("", True, True),
        ("garbage", True, True),
        ("LIVE", None, True),
        ("LIVE", True, None),
    ],
)
def test_malformed_config_or_context_fails_closed(mode, enabled, ready):
    decision = assert_live_gate_or_refuse(
        execution_mode=mode,
        live_enabled=enabled,
        live_readiness_ok=ready,
        kill_switch=False,
    )

    assert decision.allowed is False
    assert decision.effective_execution_mode == "SIMULATION"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"execution_mode": "LIVE", "live_enabled": True, "live_readiness_ok": False, "kill_switch": False},
        {"execution_mode": "LIVE", "live_enabled": False, "live_readiness_ok": True, "kill_switch": False},
        {"execution_mode": "LIVE", "live_enabled": True, "live_readiness_ok": True, "kill_switch": True},
        {"execution_mode": "BROKEN", "live_enabled": True, "live_readiness_ok": True, "kill_switch": False},
    ],
)
def test_fail_closed_always_when_any_blocker_present(kwargs):
    decision = assert_live_gate_or_refuse(**kwargs)

    assert decision.allowed is False
    assert decision.effective_execution_mode == "SIMULATION"


def _make_runtime_controller(*, safe_mode=None):
    from core.runtime_controller import RuntimeController

    class _Settings:
        def load_roserpina_config(self):
            class Cfg:
                table_count = 1

                def __getattr__(self, _name):
                    return 0

            return Cfg()

        def load_live_readiness_ok(self):
            return True

    class _Bus:
        def subscribe(self, *_args, **_kwargs):
            return None

    class _Db:
        def _execute(self, *_args, **_kwargs):
            return None

    class _Betfair:
        def connect(self, **_kwargs):
            return {"ok": True}

        def status(self):
            return {"connected": True}

    class _Telegram:
        def status(self):
            return {"connected": True}

    return RuntimeController(
        bus=_Bus(),
        db=_Db(),
        settings_service=_Settings(),
        betfair_service=_Betfair(),
        telegram_service=_Telegram(),
        safe_mode=safe_mode,
    )


def test_runtime_controller_uses_standardized_blocker_codes():
    rc = _make_runtime_controller()
    rc.mode = "BROKEN_MODE"
    rc.last_error = "boom"

    readiness = rc.evaluate_live_readiness(execution_mode="LIVE", live_enabled=True, live_readiness_ok=True)

    assert "READINESS_SIGNAL_UNKNOWN" in readiness["blockers"]
    assert "STARTUP_FAILED" in readiness["blockers"]
    assert "RUNTIME_STARTUP_FAILED" not in readiness["blockers"]
    assert "UNKNOWN_STATE" not in readiness["blockers"]


def test_runtime_probe_uses_standardized_blocker_codes_for_safe_mode_and_dependencies():
    from observability.runtime_probe import RuntimeProbe

    class _SafeMode:
        def is_enabled(self):
            return True

    probe = RuntimeProbe(safe_mode=_SafeMode(), betfair_service=None)
    report = probe.get_live_readiness_report()
    blocker_codes = {item["code"] for item in report["blockers"]}
    degraded_codes = {item["code"] for item in report["details"]["degraded"]}

    assert "SAFE_MODE_BLOCKING" in degraded_codes
    assert "LIVE_DEPENDENCY_MISSING" in blocker_codes
