import pytest

from services.settings_service import SettingsService


class InMemoryDB:
    def __init__(self, initial=None):
        self._settings = dict(initial or {})

    def get_settings(self):
        return dict(self._settings)

    def save_settings(self, payload):
        self._settings.update(dict(payload or {}))


@pytest.mark.integration
def test_execution_mode_persists_across_reload():
    db = InMemoryDB()
    svc = SettingsService(db)

    svc.save_execution_settings(execution_mode="LIVE", live_enabled=False, kill_switch=False)

    reloaded = SettingsService(db)
    assert reloaded.load_execution_settings()["execution_mode"] == "LIVE"


@pytest.mark.integration
def test_live_enabled_persists_across_reload():
    db = InMemoryDB()
    svc = SettingsService(db)

    svc.save_execution_settings(execution_mode="SIMULATION", live_enabled=True, kill_switch=False)

    reloaded = SettingsService(db)
    assert reloaded.load_execution_settings()["live_enabled"] is True


@pytest.mark.integration
def test_kill_switch_persists_across_reload():
    db = InMemoryDB()
    svc = SettingsService(db)

    svc.save_execution_settings(execution_mode="SIMULATION", live_enabled=False, kill_switch=True)

    reloaded = SettingsService(db)
    assert reloaded.load_execution_settings()["kill_switch"] is True


@pytest.mark.integration
def test_missing_settings_fall_back_safely():
    svc = SettingsService(InMemoryDB())

    loaded = svc.load_execution_settings()

    assert loaded["execution_mode"] == "SIMULATION"
    assert loaded["live_enabled"] is False
    assert loaded["kill_switch"] is False


@pytest.mark.integration
def test_malformed_settings_fail_closed_never_false_live():
    svc = SettingsService(
        InMemoryDB(
            {
                "execution_mode": "danger",
                "live_enabled": "not-bool",
                "kill_switch": "not-bool",
            }
        )
    )

    loaded = svc.load_execution_settings()

    assert loaded["execution_mode"] == "SIMULATION"
    assert loaded["live_enabled"] is False
    assert loaded["kill_switch"] is False
