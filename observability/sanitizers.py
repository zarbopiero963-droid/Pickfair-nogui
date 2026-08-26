from __future__ import annotations

from typing import Any

# Rilevamento chiavi sensibili delegato al predicato UNICO condiviso
# (core.redaction), lo stesso usato da telegram_sanitizer: i due path di
# log non possono più divergere. I nomi storici SENSITIVE_KEYS /
# _SENSITIVE_KEY_SUFFIXES restano esportati (compat: importati dai test) e
# puntano all'unione condivisa.
from core.redaction import (
    SENSITIVE_KEY_SUFFIXES as _SENSITIVE_KEY_SUFFIXES,
    SENSITIVE_KEYS_EXACT as SENSITIVE_KEYS,
    is_sensitive_key,
)

_REDACTED = "***REDACTED***"


def _is_sensitive_key(key: str) -> bool:
    """Return True if key should be redacted (predicato condiviso)."""
    return is_sensitive_key(key)


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
