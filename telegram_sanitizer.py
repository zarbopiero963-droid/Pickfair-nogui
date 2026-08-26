"""Telegram payload sanitizer with deterministic credential redaction.

Il rilevamento delle chiavi sensibili è delegato al predicato UNICO condiviso
`core.redaction.is_sensitive_key` — lo stesso usato da
`observability.sanitizers` — così i due path di log non possono più divergere e
lasciar trapelare un segreto su un path mentre l'altro lo oscura.
"""

from __future__ import annotations

from typing import Any

from core.redaction import is_sensitive_key

REDACTED = "[REDACTED]"


def _is_sensitive_key(key: str) -> bool:
    """Compat interna: delega al predicato condiviso."""
    return is_sensitive_key(key)


def _sanitize_mapping(value: dict[Any, Any]) -> dict[Any, Any]:
    """Return sanitized copy of a mapping."""
    out: dict[Any, Any] = {}
    for k, value_item in value.items():
        if is_sensitive_key(str(k)):
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
