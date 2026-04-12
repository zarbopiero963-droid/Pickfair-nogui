"""
Shared type-conversion helpers.

These functions were previously copy-pasted across BetfairClient,
Database, OrderManager, MoneyManagement, SafetyLayer and others.
Import from here instead of re-defining.
"""

from __future__ import annotations

import json
from typing import Any, TypeVar

_T = TypeVar("_T")


def safe_float(value: Any, default: float = 0.0) -> float:
    """Return ``float(value)`` or ``default`` on None / empty / error."""
    try:
        if value in (None, ""):
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def safe_int(value: Any, default: int = 0) -> int:
    """Return ``int(value)`` or ``default`` on None / empty / error.

    Deliberately strict: float strings like ``"123.9"`` are rejected and
    return ``default`` rather than being silently truncated to ``123``.
    Callers that receive genuine float values (e.g. ``3.0`` from JSON)
    should cast with ``int(float(x))`` before calling, or use
    ``safe_float`` and convert the result themselves.
    """
    try:
        if value in (None, ""):
            return int(default)
        return int(value)
    except Exception:
        return int(default)


def safe_side(value: Any) -> str:
    """Normalise a bet side string to ``"BACK"`` or ``"LAY"``."""
    s = str(value or "BACK").upper().strip()
    return s if s in {"BACK", "LAY"} else "BACK"


def safe_str(value: Any, default: str = "") -> str:
    """Return ``str(value)`` or ``default`` on None / error."""
    try:
        if value is None:
            return default
        return str(value)
    except Exception:
        return default


def safe_bool_int(value: Any, default: bool = False) -> int:
    """Return ``1`` or ``0`` from a truthy/bool/string value.

    Accepts bool, int, or string representations (``"true"``, ``"1"``,
    ``"yes"``, ``"on"``).  Returns ``int(default)`` on ``None``.
    """
    if value is None:
        return int(bool(default))
    if isinstance(value, bool):
        return int(value)
    return int(str(value).strip().lower() in {"1", "true", "yes", "on"})


def safe_json_dumps(value: Any) -> str:
    """Serialize *value* to a JSON string; return ``"{}"`` on any error."""
    try:
        return json.dumps(value if value is not None else {}, ensure_ascii=False)
    except Exception:
        return "{}"


def safe_json_loads(value: Any, default: _T) -> Any:
    """Deserialize *value* from JSON; return *default* on missing / error."""
    if not value:
        return default
    try:
        return json.loads(value)
    except Exception:
        return default
