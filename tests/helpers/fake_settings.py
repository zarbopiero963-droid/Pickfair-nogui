from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any


_TRUE_VALUES = {True, 1, "1", "true", "TRUE", "True", "yes", "on"}
_FALSE_VALUES = {False, 0, "0", "false", "FALSE", "False", "no", "off"}


class FakeSettingsService:
    """In-memory deterministic fake for settings/config test scenarios."""

    def __init__(self, initial_state: Mapping[str, Any] | None = None) -> None:
        self._store: dict[str, Any] = dict(initial_state or {})

    @classmethod
    def from_state(cls, state: Mapping[str, Any]) -> "FakeSettingsService":
        if not isinstance(state, Mapping):
            raise TypeError("state must be a mapping")
        return cls(initial_state=state)

    def export_state(self) -> dict[str, Any]:
        return deepcopy(self._store)

    def snapshot(self) -> dict[str, Any]:
        return self.export_state()

    def get(self, key: str, default: Any = None) -> Any:
        if not isinstance(key, str):
            raise TypeError("key must be a str")
        return self._store.get(key, default)

    def set(self, key: str, value: Any) -> Any:
        if not isinstance(key, str):
            raise TypeError("key must be a str")
        self._store[key] = value
        return value

    def get_bool(self, key: str, default: bool = False) -> bool:
        raw = self.get(key, default)
        if raw in _TRUE_VALUES:
            return True
        if raw in _FALSE_VALUES:
            return False
        if isinstance(raw, str):
            norm = raw.strip()
            if norm in _TRUE_VALUES:
                return True
            if norm in _FALSE_VALUES:
                return False
        raise ValueError(f"unsupported bool value for {key!r}: {raw!r}")

    def set_bool(self, key: str, value: Any) -> bool:
        if value in _TRUE_VALUES:
            normalized = True
        elif value in _FALSE_VALUES:
            normalized = False
        else:
            raise ValueError(f"unsupported bool value for {key!r}: {value!r}")
        self.set(key, normalized)
        return normalized

    def load_telegram_config_row(self) -> dict[str, Any]:
        return {
            "alerts_enabled": self.get_bool("anomaly_alerts_enabled", False),
            "anomaly_enabled": self.get_bool("anomaly_enabled", False),
            "anomaly_actions_enabled": self.get_bool("anomaly_actions_enabled", False),
        }
