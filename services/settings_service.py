from __future__ import annotations

from services.setting_service import SettingsService as LegacySettingsService


class SettingsService(LegacySettingsService):
    """
    Compatibility wrapper around the legacy `setting_service` module.
    Adds settings helpers used by headless/watchdog wiring.
    """

    ANOMALY_ENABLED_KEY = "anomaly_enabled"
    ANOMALY_ALERTS_ENABLED_KEY = "anomaly_alerts_enabled"
    ANOMALY_ACTIONS_ENABLED_KEY = "anomaly_actions_enabled"

    def load_anomaly_enabled(self) -> bool:
        data = self.get_all_settings()
        return self._b(data, self.ANOMALY_ENABLED_KEY, False)

    def save_anomaly_enabled(self, enabled: bool) -> None:
        self.save_settings({self.ANOMALY_ENABLED_KEY: int(bool(enabled))})

    def load_anomaly_alerts_enabled(self) -> bool:
        data = self.get_all_settings()
        return self._b(data, self.ANOMALY_ALERTS_ENABLED_KEY, False)

    def save_anomaly_alerts_enabled(self, enabled: bool) -> None:
        self.save_settings({self.ANOMALY_ALERTS_ENABLED_KEY: int(bool(enabled))})

    def load_anomaly_actions_enabled(self) -> bool:
        data = self.get_all_settings()
        return self._b(data, self.ANOMALY_ACTIONS_ENABLED_KEY, False)

    def save_anomaly_actions_enabled(self, enabled: bool) -> None:
        self.save_settings({self.ANOMALY_ACTIONS_ENABLED_KEY: int(bool(enabled))})
