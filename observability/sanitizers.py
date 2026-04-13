from __future__ import annotations

from typing import Any

# Exact-match sensitive keys (case-insensitive)
SENSITIVE_KEYS: frozenset = frozenset({
    "password",
    "passwd",
    "secret",
    "token",
    "session_token",
    "session_string",
    "app_key",
    "certificate",
    "cert",
    "private_key",
    "authorization",
    "cookie",
    "telegram_token",
    "api_key",
    "api_hash",
    "api_id",
    "ssoid",
})

# Suffix-based matching: any key whose last component (after the last dot)
# is in this set is also considered sensitive.  Handles dot-notation keys
# like "telegram.api_hash", "telegram.session_string", etc.
_SENSITIVE_KEY_SUFFIXES: frozenset = frozenset({
    "password",
    "passwd",
    "secret",
    "token",
    "session_token",
    "session_string",
    "app_key",
    "certificate",
    "cert",
    "private_key",
    "authorization",
    "cookie",
    "api_key",
    "api_hash",
    "api_id",
})

_REDACTED = "***REDACTED***"


def _is_sensitive_key(key: str) -> bool:
    """Return True if key should be redacted."""
    lower = str(key).lower()
    if lower in SENSITIVE_KEYS:
        return True
    # Dot-notation suffix check: "telegram.api_hash" → suffix "api_hash"
    suffix = lower.rsplit(".", 1)[-1]
    return suffix in _SENSITIVE_KEY_SUFFIXES


def sanitize_value(value: Any) -> Any:
    """Recursively redact sensitive fields in dicts/lists/tuples."""
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            if _is_sensitive_key(str(k)):
                out[k] = _REDACTED
            else:
                out[k] = sanitize_value(v)
        return out

    if isinstance(value, list):
        return [sanitize_value(x) for x in value]

    if isinstance(value, tuple):
        return tuple(sanitize_value(x) for x in value)

    return value


def sanitize_dict(data: dict) -> dict:
    """Convenience wrapper: always returns a dict."""
    result = sanitize_value(data)
    return result if isinstance(result, dict) else {}
