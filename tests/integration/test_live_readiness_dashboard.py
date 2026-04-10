from __future__ import annotations

import tkinter as tk

import pytest

from tests.helpers.fake_settings import FakeSettingsService
from ui.live_control_plane import (
    LiveControlPlane,
    LiveReadinessReport,
    MODE_LIVE,
    STATUS_LIVE_BLOCKED,
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


def test_ready_not_ready_unknown_visible(root):
    settings = FakeSettingsService({"execution_mode": MODE_LIVE, "live_enabled": True, "kill_switch": False})

    ready_panel = LiveControlPlane(
        root,
        settings_service=settings,
        readiness_provider=lambda: LiveReadinessReport(ready=True, level="READY", blockers=[], details={}),
    )
    assert ready_panel.readiness_level_var.get() == "READY"

    not_ready_panel = LiveControlPlane(
        root,
        settings_service=settings,
        readiness_provider=lambda: LiveReadinessReport(
            ready=False,
            level="NOT_READY",
            blockers=["LIVE_DEPENDENCY_MISSING"],
            details={},
        ),
    )
    assert not_ready_panel.readiness_level_var.get() == "NOT_READY"

    unknown_panel = LiveControlPlane(root, settings_service=settings, readiness_provider=None)
    assert unknown_panel.readiness_level_var.get() == "UNKNOWN"


def test_unknown_is_not_ready(root):
    panel = LiveControlPlane(
        root,
        settings_service=FakeSettingsService({"execution_mode": MODE_LIVE, "live_enabled": True, "kill_switch": False}),
        readiness_provider=None,
    )

    assert panel.readiness_level_var.get() == "UNKNOWN"
    assert panel.effective_status_var.get() == STATUS_LIVE_BLOCKED
    assert "GO" not in panel.last_decision_var.get()


def test_blockers_and_last_decision(root):
    blockers = ["LIVE_DEPENDENCY_MISSING", "RUNTIME_NOT_INITIALIZED"]
    panel = LiveControlPlane(
        root,
        settings_service=FakeSettingsService({"execution_mode": MODE_LIVE, "live_enabled": True, "kill_switch": False}),
        readiness_provider=lambda: LiveReadinessReport(
            ready=False,
            level="NOT_READY",
            blockers=blockers,
            details={"source": "test"},
        ),
    )

    assert list(panel.blockers_listbox.get(0, tk.END)) == blockers
    assert panel.last_decision_var.get() == "NO-GO: READINESS_BLOCKED"
