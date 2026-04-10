import pytest


class FakeVar:
    def __init__(self, value=None):
        self._value = value

    def set(self, value):
        self._value = value

    def get(self):
        return self._value


class FakeRuntime:
    def __init__(self, readiness):
        self._readiness = readiness

    def evaluate_live_readiness(self, **kwargs):
        return self._readiness


def _make_app(readiness, execution_mode="LIVE", live_enabled=True, kill_switch=False):
    import mini_gui

    app = mini_gui.MiniPickfairGUI.__new__(mini_gui.MiniPickfairGUI)
    app.runtime = FakeRuntime(readiness)
    app.execution_mode_var = FakeVar(execution_mode)
    app.live_enabled_var = FakeVar(live_enabled)
    app.kill_switch_var = FakeVar(kill_switch)
    app.live_readiness_level_var = FakeVar("UNKNOWN")
    app.live_readiness_blockers_var = FakeVar("")
    app.live_control_state_var = FakeVar("")
    app.live_requested_mode_var = FakeVar("SIMULATION")
    app.live_effective_status_var = FakeVar("UNKNOWN")
    app.live_last_decision_var = FakeVar("Last decision: N/A")
    app.live_last_reason_var = FakeVar("Last reason: N/A")
    return app


@pytest.mark.integration
def test_ready_state_displays_distinctly():
    import mini_gui

    app = _make_app({"level": "READY", "ready": True, "blockers": []})

    mini_gui.MiniPickfairGUI._refresh_live_control_plane_status(app, {})

    assert app.live_readiness_level_var.get() == "READY"
    assert app.live_effective_status_var.get() == "LIVE_ACTIVE"
    assert app.live_control_state_var.get() == "LIVE active"
    assert app.live_last_decision_var.get() == "Last decision: GO"


@pytest.mark.integration
@pytest.mark.parametrize("level", ["DEGRADED", "NOT_READY", "UNKNOWN"])
def test_non_ready_levels_are_distinguishable(level):
    import mini_gui

    app = _make_app({"level": level, "ready": False, "blockers": ["BLOCKER_A"]})

    mini_gui.MiniPickfairGUI._refresh_live_control_plane_status(app, {})

    assert app.live_readiness_level_var.get() == level
    assert app.live_readiness_level_var.get() != "READY"


@pytest.mark.integration
def test_blocker_list_matches_readiness_report():
    import mini_gui

    blockers = ["BLOCKER_A", "BLOCKER_B"]
    app = _make_app({"level": "NOT_READY", "ready": False, "blockers": blockers})

    mini_gui.MiniPickfairGUI._refresh_live_control_plane_status(app, {})

    blockers_text = app.live_readiness_blockers_var.get()
    assert "BLOCKER_A" in blockers_text
    assert "BLOCKER_B" in blockers_text


@pytest.mark.integration
def test_live_requested_with_blockers_shows_blocked_live():
    import mini_gui

    app = _make_app({"level": "NOT_READY", "ready": False, "blockers": ["RUNTIME_NOT_INITIALIZED"]})

    mini_gui.MiniPickfairGUI._refresh_live_control_plane_status(app, {})

    assert app.live_effective_status_var.get() == "LIVE_REQUESTED_BLOCKED"
    assert app.live_control_state_var.get() == "LIVE requested but blocked"
    assert app.live_readiness_level_var.get() == "NOT_READY"
    assert app.live_last_decision_var.get() == "Last decision: NO-GO"
    assert app.live_last_reason_var.get() == "Last reason: RUNTIME_NOT_INITIALIZED"


@pytest.mark.integration
def test_simulation_mode_stays_safe_with_na_decision():
    import mini_gui

    app = _make_app(
        {"level": "DEGRADED", "ready": False, "blockers": ["SIMULATION_MODE"]},
        execution_mode="SIMULATION",
        live_enabled=False,
    )

    mini_gui.MiniPickfairGUI._refresh_live_control_plane_status(app, {})

    assert app.live_effective_status_var.get() == "SAFE_MODE"
    assert app.live_last_decision_var.get() == "Last decision: N/A"
    assert app.live_last_reason_var.get() == "Last reason: SIMULATION_MODE"
