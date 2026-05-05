"""Telegram payload sanitizer with deterministic credential redaction."""

from __future__ import annotations

import re
from typing import Any

REDACTED = "[REDACTED]"
TELEGRAM_SENSITIVE_KEYS = {
    "token",
    "auth_token",
    "access_token",
    "bearer",
    "user_session",
    "session",
    "session_token",
    "api_key",
    "secret",
    "password",
    "authorization",
    "auth",
    "refresh_token",
    "bot_token",
    "client_secret",
    "private_key",
    "api_secret",
    "authorization_header",
}
TELEGRAM_SENSITIVE_KEY_FRAGMENTS = {
    "token",
    "secret",
    "password",
    "auth",
    "bearer",
    "key",
    "session",
    "api",
}


def _key_parts(key: str) -> list[str]:
    """Split a key into lowercase parts using common separators."""
    return [p for p in re.split(r"[\s_.-]+", str(key or "").lower()) if p]


def _has_sensitive_part_combo(parts: list[str]) -> bool:
    """Return True when two or more sensitive parts are present."""
    return sum(1 for part in parts if part in TELEGRAM_SENSITIVE_KEY_FRAGMENTS) >= 2


def _is_sensitive_key(key: str) -> bool:
    """Detect sensitive keys using exact and part-aware matching."""
    key_l = str(key or "").lower()
    parts = _key_parts(key_l)
    return key_l in TELEGRAM_SENSITIVE_KEYS or (
        len(parts) > 1 and _has_sensitive_part_combo(parts)
    )


def _sanitize_mapping(value: dict[Any, Any]) -> dict[Any, Any]:
    """Return sanitized copy of a mapping."""
    out: dict[Any, Any] = {}
    for k, value_item in value.items():
        if _is_sensitive_key(str(k)):
            out[k] = REDACTED
        else:
            out[k] = sanitize_telegram_payload(value_item)
    return out


def _sanitize_list(values: list[Any]) -> list[Any]:
    """Return sanitized copy of a list."""
    return [sanitize_telegram_payload(value_item) for value_item in values]


def _sanitize_tuple(values: tuple[Any, ...]) -> tuple[Any, ...]:
    """Return sanitized copy of a tuple."""
    return tuple(sanitize_telegram_payload(value_item) for value_item in values)


def sanitize_telegram_payload(value: Any) -> Any:
    """Recursively sanitize dict/list/tuple payload structures."""
    if isinstance(value, dict):
        return _sanitize_mapping(value)
    if isinstance(value, list):
        return _sanitize_list(value)
    if isinstance(value, tuple):
        return _sanitize_tuple(value)
    return value
