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
}
_CREDENTIAL_PARTS = {"token", "secret", "password", "key", "session", "auth", "bearer", "api"}


def _key_parts(key: str) -> list[str]:
    return [p for p in re.split(r"[\s_.-]+", str(key or "").lower()) if p]


def _has_sensitive_fragment(parts: list[str]) -> bool:
    return any(part in TELEGRAM_SENSITIVE_KEY_FRAGMENTS for part in parts)


def _has_sensitive_part_combo(parts: list[str]) -> bool:
    return sum(1 for part in parts if part in _CREDENTIAL_PARTS) >= 2


def _is_sensitive_key(key: str) -> bool:
    key_l = str(key or "").lower()
    if key_l in TELEGRAM_SENSITIVE_KEYS:
        return True
    parts = _key_parts(key_l)
    if not parts:
        return False
    if len(parts) <= 1:
        return False
    if not _has_sensitive_fragment(parts):
        return False
    return _has_sensitive_part_combo(parts)


def _sanitize_mapping(value: dict[Any, Any]) -> dict[Any, Any]:
    out: dict[Any, Any] = {}
    for k, v in value.items():
        if _is_sensitive_key(str(k)):
            out[k] = REDACTED
        else:
            out[k] = sanitize_telegram_payload(v)
    return out


def _sanitize_list(values: list[Any]) -> list[Any]:
    return [sanitize_telegram_payload(v) for v in values]


def _sanitize_tuple(values: tuple[Any, ...]) -> tuple[Any, ...]:
    return tuple(sanitize_telegram_payload(v) for v in values)


def sanitize_telegram_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return _sanitize_mapping(value)
    if isinstance(value, list):
        return _sanitize_list(value)
    if isinstance(value, tuple):
        return _sanitize_tuple(value)
    return value
