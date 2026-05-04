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
}


def sanitize_telegram_payload(value: Any) -> Any:
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            key = str(k)
            if key.lower() in TELEGRAM_SENSITIVE_KEYS:
                out[k] = REDACTED
            else:
                out[k] = sanitize_telegram_payload(v)
        return out
    if isinstance(value, list):
        return [sanitize_telegram_payload(v) for v in value]
    if isinstance(value, tuple):
        return tuple(sanitize_telegram_payload(v) for v in value)
    return value
