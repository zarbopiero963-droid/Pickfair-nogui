from __future__ import annotations

from services.setting_service import SettingsService as LegacySettingsService


class SettingsService(LegacySettingsService):
    """
    Compatibility wrapper around the legacy `setting_service` module.
    Adds settings helpers used by headless/watchdog wiring.
    """

    ANOMALY_ENABLED_KEY = "anomaly_enabled"

    def load_anomaly_enabled(self) -> bool:
        data = self.get_all_settings()
        return self._b(data, self.ANOMALY_ENABLED_KEY, False)

    def save_anomaly_enabled(self, enabled: bool) -> None:
        self.save_settings({self.ANOMALY_ENABLED_KEY: int(bool(enabled))})
