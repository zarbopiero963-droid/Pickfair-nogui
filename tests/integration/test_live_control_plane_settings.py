from __future__ import annotations

import tkinter as tk

import pytest

from tests.helpers.fake_settings import FakeSettingsService
from ui.live_control_plane import LiveControlPlane, LiveReadinessReport, MODE_LIVE, MODE_SIMULATION


@pytest.fixture
def root():
    try:
        rt = tk.Tk()
        rt.withdraw()
    except tk.TclError as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"tk unavailable: {exc}")
    yield rt
    rt.destroy()


def test_persistence_works(root):
    settings = FakeSettingsService()
    panel = LiveControlPlane(
        root,
        settings_service=settings,
        readiness_provider=lambda: LiveReadinessReport(ready=False, level="NOT_READY", blockers=["X"], details={}),
    )

    panel.execution_mode_var.set(MODE_LIVE)
    panel.live_enabled_var.set(True)
    panel.kill_switch_var.set(False)
    panel._on_controls_changed()

    assert settings.get("execution_mode") == MODE_LIVE
    assert settings.get("live_enabled") is True
    assert settings.get("kill_switch") is False


def test_reload_works(root):
    settings = FakeSettingsService(
        {
            "execution_mode": MODE_LIVE,
            "live_enabled": True,
            "kill_switch": False,
        }
    )
    panel = LiveControlPlane(
        root,
        settings_service=settings,
        readiness_provider=lambda: LiveReadinessReport(ready=False, level="NOT_READY", blockers=["X"], details={}),
    )

    settings.set("execution_mode", MODE_SIMULATION)
    settings.set("live_enabled", False)
    settings.set("kill_switch", True)
    panel.reload_settings()

    assert panel.execution_mode_var.get() == MODE_SIMULATION
    assert panel.live_enabled_var.get() is False
    assert panel.kill_switch_var.get() is True


def test_invalid_config_falls_back_to_safe(root):
    settings = FakeSettingsService(
        {
            "execution_mode": "BROKEN",
            "live_enabled": "not-a-bool",
            "kill_switch": "not-a-bool",
        }
    )
    panel = LiveControlPlane(
        root,
        settings_service=settings,
        readiness_provider=lambda: LiveReadinessReport(ready=True, level="READY", blockers=[], details={}),
    )

    assert panel.execution_mode_var.get() == MODE_SIMULATION
    assert panel.live_enabled_var.get() is False
    assert panel.kill_switch_var.get() is False
    assert panel.effective_status_var.get() == "SAFE_MODE"
