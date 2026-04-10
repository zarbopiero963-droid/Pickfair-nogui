import pytest

from services.setting_service import SettingsService


class FakeDB:
    def __init__(self, initial=None):
        self.data = dict(initial or {})

    def get_settings(self):
        return dict(self.data)

    def save_settings(self, payload):
        self.data.update(payload or {})


@pytest.mark.integration
def test_execution_mode_persists_across_reload():
    db = FakeDB()
    svc = SettingsService(db)

    svc.save_live_control_plane({"execution_mode": "LIVE", "live_enabled": False, "kill_switch": False})
    reloaded = SettingsService(db).load_live_control_plane()

    assert reloaded["execution_mode"] == "LIVE"


@pytest.mark.integration
def test_live_enabled_persists_across_reload():
    db = FakeDB()
    svc = SettingsService(db)

    svc.save_live_control_plane({"execution_mode": "SIMULATION", "live_enabled": True, "kill_switch": False})
    reloaded = SettingsService(db).load_live_control_plane()

    assert reloaded["live_enabled"] is True


@pytest.mark.integration
def test_kill_switch_persists_across_reload():
    db = FakeDB()
    svc = SettingsService(db)

    svc.save_live_control_plane({"execution_mode": "SIMULATION", "live_enabled": False, "kill_switch": True})
    reloaded = SettingsService(db).load_live_control_plane()

    assert reloaded["kill_switch"] is True


@pytest.mark.integration
def test_missing_settings_fall_back_safely():
    db = FakeDB(initial={})
    svc = SettingsService(db)

    loaded = svc.load_live_control_plane()

    assert loaded == {
        "execution_mode": "SIMULATION",
        "live_enabled": False,
        "kill_switch": False,
    }


@pytest.mark.integration
def test_malformed_settings_fail_closed():
    db = FakeDB(initial={"execution_mode": "banana", "live_enabled": "not_bool", "kill_switch": "nan"})
    svc = SettingsService(db)

    loaded = svc.load_live_control_plane()

    assert loaded["execution_mode"] == "SIMULATION"
    assert loaded["live_enabled"] is False
    assert loaded["kill_switch"] is False
