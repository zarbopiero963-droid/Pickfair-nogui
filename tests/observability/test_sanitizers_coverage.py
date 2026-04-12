"""
Tests for sensitive-data sanitization coverage.

Verifies:
- All known secret field names are redacted
- Dot-notation keys (telegram.api_hash, etc.) are redacted
- Nested dicts are recursively sanitized
- Non-sensitive fields are NOT redacted
- Lists and tuples are recursively sanitized
- sanitize_dict() wrapper works
- Telegram alert details are sanitized before rendering
"""

import pytest

from observability.sanitizers import (
    SENSITIVE_KEYS,
    _SENSITIVE_KEY_SUFFIXES,
    _is_sensitive_key,
    sanitize_dict,
    sanitize_value,
)


_REDACTED = "***REDACTED***"


# ===========================================================================
# _is_sensitive_key
# ===========================================================================

@pytest.mark.unit
@pytest.mark.guardrail
@pytest.mark.parametrize("key", [
    "password", "PASSWORD", "Password",
    "api_hash", "API_HASH",
    "api_id", "API_ID",
    "session_token", "SESSION_TOKEN",
    "session_string", "SESSION_STRING",
    "private_key", "PRIVATE_KEY",
    "certificate", "CERTIFICATE",
    "app_key",
    "secret",
    "token",
])
def test_is_sensitive_key_exact_match(key):
    assert _is_sensitive_key(key) is True, f"{key!r} must be sensitive"


@pytest.mark.unit
@pytest.mark.guardrail
@pytest.mark.parametrize("key", [
    "telegram.api_hash",
    "telegram.api_id",
    "telegram.session_string",
    "betfair.password",
    "betfair.certificate",
    "betfair.private_key",
    "betfair.session_token",
])
def test_is_sensitive_key_dot_notation(key):
    assert _is_sensitive_key(key) is True, f"Dot-notation key {key!r} must be sensitive"


@pytest.mark.unit
@pytest.mark.guardrail
@pytest.mark.parametrize("key", [
    "username", "market_id", "selection_id",
    "stake", "price", "event_name",
    "phone_number", "chat_id", "alerts_enabled",
    "simulation_mode", "execution_mode",
])
def test_is_not_sensitive_key(key):
    assert _is_sensitive_key(key) is False, f"{key!r} must NOT be sensitive"


# ===========================================================================
# sanitize_value
# ===========================================================================

@pytest.mark.unit
@pytest.mark.guardrail
def test_sanitize_password_field():
    data = {"username": "user", "password": "hunter2"}
    result = sanitize_value(data)
    assert result["password"] == _REDACTED
    assert result["username"] == "user"


@pytest.mark.unit
@pytest.mark.guardrail
def test_sanitize_telegram_dot_notation_fields():
    data = {
        "telegram.api_id": "12345678",
        "telegram.api_hash": "abcdef1234",
        "telegram.session_string": "1BAA_FAKE",
        "telegram.alerts_enabled": True,
        "telegram.phone_number": "+39331234",
    }
    result = sanitize_value(data)
    assert result["telegram.api_id"] == _REDACTED
    assert result["telegram.api_hash"] == _REDACTED
    assert result["telegram.session_string"] == _REDACTED
    # Non-secret fields must NOT be redacted
    assert result["telegram.alerts_enabled"] is True
    assert result["telegram.phone_number"] == "+39331234"


@pytest.mark.unit
@pytest.mark.guardrail
def test_sanitize_nested_dict():
    data = {
        "outer": "ok",
        "nested": {
            "password": "secret",
            "market_id": "1.111",
        },
    }
    result = sanitize_value(data)
    assert result["outer"] == "ok"
    assert result["nested"]["password"] == _REDACTED
    assert result["nested"]["market_id"] == "1.111"


@pytest.mark.unit
@pytest.mark.guardrail
def test_sanitize_list_of_dicts():
    data = [
        {"password": "p1", "user": "u1"},
        {"api_hash": "h1", "source": "telegram"},
    ]
    result = sanitize_value(data)
    assert result[0]["password"] == _REDACTED
    assert result[0]["user"] == "u1"
    assert result[1]["api_hash"] == _REDACTED
    assert result[1]["source"] == "telegram"


@pytest.mark.unit
@pytest.mark.guardrail
def test_sanitize_value_passthrough_for_primitives():
    assert sanitize_value(42) == 42
    assert sanitize_value("plaintext") == "plaintext"
    assert sanitize_value(None) is None


@pytest.mark.unit
@pytest.mark.guardrail
def test_sanitize_dict_convenience():
    data = {"password": "x", "username": "u"}
    result = sanitize_dict(data)
    assert isinstance(result, dict)
    assert result["password"] == _REDACTED
    assert result["username"] == "u"


# ===========================================================================
# SENSITIVE_KEYS completeness checks
# ===========================================================================

@pytest.mark.unit
@pytest.mark.guardrail
def test_sensitive_keys_includes_known_secret_fields():
    required = {
        "password", "api_hash", "api_id", "session_token",
        "session_string", "private_key", "certificate", "token", "secret",
    }
    missing = required - SENSITIVE_KEYS
    assert not missing, f"SENSITIVE_KEYS is missing required fields: {missing}"


# ===========================================================================
# Telegram alert formatting sanitizes details
# ===========================================================================

@pytest.mark.unit
@pytest.mark.guardrail
def test_telegram_alert_format_redacts_secrets_in_details():
    """Details dict with secret fields must be sanitized in formatted alert text."""
    from services.telegram_alerts_service import TelegramAlertsService

    svc = TelegramAlertsService.__new__(TelegramAlertsService)

    alert = {
        "severity": "ERROR",
        "code": "AUTH_FAIL",
        "title": "Auth failure",
        "source": "betfair",
        "details": {
            "password": "supersecret",
            "api_hash": "hash123",
            "market_id": "1.111",
        },
    }
    settings = {"alert_format_rich": True}

    text = svc._format_alert_text(alert, settings)

    assert "supersecret" not in text, "password must be redacted in alert text"
    assert "hash123" not in text, "api_hash must be redacted in alert text"
    assert _REDACTED in text, "redaction marker must appear in alert text"
    # Non-secret field should still be present
    assert "1.111" in text, "market_id should NOT be redacted"
