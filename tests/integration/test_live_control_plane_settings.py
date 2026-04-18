import pytest

from core.system_state import RoserpinaConfig
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


@pytest.mark.integration
def test_save_roserpina_config_preserves_persisted_hard_stop_values_when_omitted():
    db = InMemoryDB(
        {
            "roserpina.max_daily_loss": 125.0,
            "roserpina.max_drawdown_hard_stop_pct": 20.0,
            "roserpina.max_open_exposure": 300.0,
        }
    )
    svc = SettingsService(db)

    # Omitted hard-stop fields remain dataclass defaults (None).
    svc.save_roserpina_config(RoserpinaConfig(table_count=3))

    reloaded = SettingsService(db).load_roserpina_config()
    assert reloaded.max_daily_loss == 125.0
    assert reloaded.max_drawdown_hard_stop_pct == 20.0
    assert reloaded.max_open_exposure == 300.0


@pytest.mark.integration
def test_save_roserpina_config_updates_hard_stop_values_when_explicit():
    db = InMemoryDB(
        {
            "roserpina.max_daily_loss": 125.0,
            "roserpina.max_drawdown_hard_stop_pct": 20.0,
            "roserpina.max_open_exposure": 300.0,
        }
    )
    svc = SettingsService(db)

    svc.save_roserpina_config(
        RoserpinaConfig(
            table_count=3,
            max_daily_loss=150.0,
            max_drawdown_hard_stop_pct=25.0,
            max_open_exposure=350.0,
        )
    )

    reloaded = SettingsService(db).load_roserpina_config()
    assert reloaded.max_daily_loss == 150.0
    assert reloaded.max_drawdown_hard_stop_pct == 25.0
    assert reloaded.max_open_exposure == 350.0
