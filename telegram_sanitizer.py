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
_CREDENTIAL_PARTS = {"token", "secret", "password", "key", "session", "auth", "bearer"}
_CREDENTIAL_QUALIFIERS = {
    "refresh",
    "bot",
    "client",
    "private",
    "api",
    "auth",
    "access",
    "session",
    "user",
    "authorization",
}


def _is_sensitive_key(key: str) -> bool:
    key_l = str(key or "").lower()
    if key_l in TELEGRAM_SENSITIVE_KEYS:
        return True
    parts = [p for p in re.split(r"[\s_.-]+", key_l) if p]
    if len(parts) <= 1:
        return False
    if not any(part in TELEGRAM_SENSITIVE_KEY_FRAGMENTS for part in parts):
        return False
    if (parts[-1] in _CREDENTIAL_PARTS and any(p in _CREDENTIAL_QUALIFIERS for p in parts[:-1])) or (
        parts[0] in _CREDENTIAL_PARTS and any(p in _CREDENTIAL_QUALIFIERS for p in parts[1:])
    ):
        return True
    return False


def sanitize_telegram_payload(value: Any) -> Any:
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            key = str(k)
            if _is_sensitive_key(key):
                out[k] = REDACTED
            else:
                out[k] = sanitize_telegram_payload(v)
        return out
    if isinstance(value, list):
        return [sanitize_telegram_payload(v) for v in value]
    if isinstance(value, tuple):
        return tuple(sanitize_telegram_payload(v) for v in value)
    return value
