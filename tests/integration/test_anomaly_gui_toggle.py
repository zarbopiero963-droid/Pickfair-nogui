from headless_main import HeadlessApp
from services.setting_service import SettingsService


class _MemoryDb:
    def __init__(self):
        self.settings = {}

    def get_settings(self):
        return dict(self.settings)

    def save_settings(self, data):
        for key, value in (data or {}).items():
            self.settings[key] = value


def test_persisted_toggle_reload_preserves_all_three_flags():
    db = _MemoryDb()
    writer = SettingsService(db)
    writer.save_anomaly_toggles(
        anomaly_enabled=True,
        anomaly_alerts_enabled=True,
        anomaly_actions_enabled=False,
    )

    reader = SettingsService(db)
    assert reader.load_anomaly_toggles() == {
        "anomaly_enabled": True,
        "anomaly_alerts_enabled": True,
        "anomaly_actions_enabled": False,
    }


def test_missing_settings_fallback_is_safe_off():
    service = SettingsService(_MemoryDb())
    assert service.load_anomaly_toggles() == {
        "anomaly_enabled": False,
        "anomaly_alerts_enabled": False,
        "anomaly_actions_enabled": False,
    }


def test_headless_mode_loads_toggles_safely_without_gui():
    app = HeadlessApp()
    app.settings_service = None
    assert app._load_anomaly_toggles() == {
        "anomaly_enabled": False,
        "anomaly_alerts_enabled": False,
        "anomaly_actions_enabled": False,
    }
