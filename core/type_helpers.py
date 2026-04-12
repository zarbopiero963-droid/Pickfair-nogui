"""
Shared type-conversion helpers.

These functions were previously copy-pasted across BetfairClient,
Database, OrderManager, MoneyManagement, SafetyLayer and others.
Import from here instead of re-defining.
"""

from __future__ import annotations

from typing import Any


def safe_float(value: Any, default: float = 0.0) -> float:
    """Return ``float(value)`` or ``default`` on None / empty / error."""
    try:
        if value in (None, ""):
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def safe_int(value: Any, default: int = 0) -> int:
    """Return ``int(float(value))`` or ``default`` on None / empty / error.

    Uses float intermediate so "3.0" → 3 works correctly.
    """
    try:
        if value in (None, ""):
            return int(default)
        return int(float(value))
    except Exception:
        return int(default)


def safe_side(value: Any) -> str:
    """Normalise a bet side string to ``"BACK"`` or ``"LAY"``."""
    s = str(value or "BACK").upper().strip()
    return s if s in {"BACK", "LAY"} else "BACK"
