from __future__ import annotations

import tkinter as tk

import pytest

from tests.helpers.fake_settings import FakeSettingsService
from ui.live_control_plane import (
    LiveControlPlane,
    LiveReadinessReport,
    MODE_LIVE,
    MODE_SIMULATION,
    STATUS_LIVE_BLOCKED,
    STATUS_LIVE_REQUESTED_BLOCKED,
)


@pytest.fixture
def root():
    try:
        rt = tk.Tk()
        rt.withdraw()
    except tk.TclError as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"tk unavailable: {exc}")
    yield rt
    rt.destroy()


def test_default_state_is_simulation(root):
    panel = LiveControlPlane(
        root,
        settings_service=FakeSettingsService(),
        readiness_provider=lambda: LiveReadinessReport(ready=True, level="READY", blockers=[], details={}),
    )

    assert panel.execution_mode_var.get() == MODE_SIMULATION
    assert panel.effective_status_var.get() != "LIVE_ACTIVE"


def test_live_request_does_not_imply_live_active(root):
    settings = FakeSettingsService(
        {
            "execution_mode": MODE_LIVE,
            "live_enabled": False,
            "kill_switch": False,
        }
    )
    panel = LiveControlPlane(
        root,
        settings_service=settings,
        readiness_provider=lambda: LiveReadinessReport(ready=True, level="READY", blockers=[], details={}),
    )

    assert panel.requested_mode_var.get() == MODE_LIVE
    assert panel.effective_status_var.get() == STATUS_LIVE_REQUESTED_BLOCKED
    assert "NO-GO" in panel.last_decision_var.get()


def test_kill_switch_blocks_live(root):
    settings = FakeSettingsService(
        {
            "execution_mode": MODE_LIVE,
            "live_enabled": True,
            "kill_switch": True,
        }
    )
    panel = LiveControlPlane(
        root,
        settings_service=settings,
        readiness_provider=lambda: LiveReadinessReport(ready=True, level="READY", blockers=[], details={}),
    )

    assert panel.effective_status_var.get() == STATUS_LIVE_BLOCKED
    assert "KILL_SWITCH_ACTIVE" in panel.last_decision_var.get()


def test_blockers_visible(root):
    blockers = ["LIVE_DEPENDENCY_MISSING", "RUNTIME_NOT_INITIALIZED", "KILL_SWITCH_ACTIVE"]
    panel = LiveControlPlane(
        root,
        settings_service=FakeSettingsService({"execution_mode": MODE_LIVE, "live_enabled": True}),
        readiness_provider=lambda: LiveReadinessReport(ready=False, level="NOT_READY", blockers=blockers, details={}),
    )

    items = panel.blockers_listbox.get(0, tk.END)
    assert list(items) == blockers
