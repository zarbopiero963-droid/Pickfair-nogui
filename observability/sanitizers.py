from __future__ import annotations

from typing import Any


SENSITIVE_KEYS = {
    "password",
    "passwd",
    "secret",
    "token",
    "session_token",
    "app_key",
    "certificate",
    "cert",
    "private_key",
    "authorization",
    "cookie",
    "telegram_token",
    "api_key",
    "ssoid",
}


def sanitize_value(value: Any) -> Any:
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            if str(k).lower() in SENSITIVE_KEYS:
                out[k] = "***REDACTED***"
            else:
                out[k] = sanitize_value(v)
        return out

    if isinstance(value, list):
        return [sanitize_value(x) for x in value]

    if isinstance(value, tuple):
        return tuple(sanitize_value(x) for x in value)

    return value
