from __future__ import annotations

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


def _is_sensitive_key(key: str) -> bool:
    key_l = str(key or "").lower()
    if key_l in TELEGRAM_SENSITIVE_KEYS:
        return True
    return any(fragment in key_l for fragment in TELEGRAM_SENSITIVE_KEY_FRAGMENTS)


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
