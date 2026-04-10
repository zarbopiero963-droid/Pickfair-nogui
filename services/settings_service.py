from __future__ import annotations

from typing import Any, Dict

from services.setting_service import SettingsService as LegacySettingsService


class SettingsService(LegacySettingsService):
    """Compat service con gate espliciti SIM/LIVE fail-closed."""

    def load_execution_settings(self) -> Dict[str, Any]:
        data = self.get_all_settings()
        mode_raw = str(data.get("execution_mode", "SIMULATION") or "SIMULATION").strip().upper()
        execution_mode = mode_raw if mode_raw in {"SIMULATION", "LIVE"} else "SIMULATION"
        return {
            "execution_mode": execution_mode,
            "live_enabled": self._b(data, "live_enabled", False),
        }

    def save_execution_settings(self, *, execution_mode: str, live_enabled: bool) -> None:
        mode_raw = str(execution_mode or "SIMULATION").strip().upper()
        mode = mode_raw if mode_raw in {"SIMULATION", "LIVE"} else "SIMULATION"
        self.save_settings(
            {
                "execution_mode": mode,
                "live_enabled": int(bool(live_enabled)),
            }
        )

    def load_execution_mode(self) -> str:
        return str(self.load_execution_settings().get("execution_mode", "SIMULATION"))

    def load_live_enabled(self) -> bool:
        return bool(self.load_execution_settings().get("live_enabled", False))

    def load_live_readiness_ok(self) -> bool:
        # Default hard fail-closed: LIVE readiness non è implicita.
        data = self.get_all_settings()
        return self._b(data, "live_readiness_ok", False)

