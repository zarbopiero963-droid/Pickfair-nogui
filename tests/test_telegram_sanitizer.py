"""
Tests for telegram_sanitizer.sanitize_telegram_payload.

Covers: all sensitive keys, case insensitivity, recursive structures,
non-sensitive passthrough, primitives, immutability, edge cases.
"""
import pytest

from telegram_sanitizer import (
    REDACTED,
    TELEGRAM_SENSITIVE_KEYS,
    sanitize_telegram_payload,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ALL_SENSITIVE_KEYS = sorted(TELEGRAM_SENSITIVE_KEYS)


# ---------------------------------------------------------------------------
# Primitives and scalar passthrough
# ---------------------------------------------------------------------------


def test_sanitize_none_returns_none():
    assert sanitize_telegram_payload(None) is None


def test_sanitize_int_returns_int():
    assert sanitize_telegram_payload(42) == 42


def test_sanitize_float_returns_float():
    assert sanitize_telegram_payload(3.14) == 3.14


def test_sanitize_bool_true_returns_true():
    assert sanitize_telegram_payload(True) is True


def test_sanitize_bool_false_returns_false():
    assert sanitize_telegram_payload(False) is False


def test_sanitize_string_returns_string_unchanged():
    assert sanitize_telegram_payload("hello") == "hello"


def test_sanitize_empty_string_returns_empty_string():
    assert sanitize_telegram_payload("") == ""


# ---------------------------------------------------------------------------
# Empty containers
# ---------------------------------------------------------------------------


def test_sanitize_empty_dict_returns_empty_dict():
    result = sanitize_telegram_payload({})
    assert result == {}


def test_sanitize_empty_list_returns_empty_list():
    assert sanitize_telegram_payload([]) == []


def test_sanitize_empty_tuple_returns_empty_tuple():
    assert sanitize_telegram_payload(()) == ()


# ---------------------------------------------------------------------------
# All 11 sensitive keys individually (lowercase)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("key", _ALL_SENSITIVE_KEYS)
def test_sanitize_each_sensitive_key_is_redacted(key):
    result = sanitize_telegram_payload({key: "sensitive_value"})
    assert result[key] == REDACTED


@pytest.mark.parametrize("key", _ALL_SENSITIVE_KEYS)
def test_sanitize_each_sensitive_key_uppercase_is_redacted(key):
    upper_key = key.upper()
    result = sanitize_telegram_payload({upper_key: "sensitive_value"})
    assert result[upper_key] == REDACTED


@pytest.mark.parametrize("key", _ALL_SENSITIVE_KEYS)
def test_sanitize_each_sensitive_key_mixed_case_is_redacted(key):
    mixed_key = key.capitalize()
    result = sanitize_telegram_payload({mixed_key: "sensitive_value"})
    assert result[mixed_key] == REDACTED


# ---------------------------------------------------------------------------
# Non-sensitive keys are passed through unchanged
# ---------------------------------------------------------------------------


def test_sanitize_non_sensitive_key_passes_through():
    result = sanitize_telegram_payload({"market_id": "1.234", "selection": "home"})
    assert result["market_id"] == "1.234"
    assert result["selection"] == "home"


def test_sanitize_partial_match_key_is_not_redacted():
    # "not_a_token" contains "token" as substring but is not in sensitive set
    result = sanitize_telegram_payload({"not_a_token": "value"})
    assert result["not_a_token"] == "value"


def test_sanitize_token_prefix_key_is_not_redacted():
    # "token_extra" should not be redacted
    result = sanitize_telegram_payload({"token_extra": "value"})
    assert result["token_extra"] == "value"


def test_sanitize_secret_prefix_key_is_not_redacted():
    result = sanitize_telegram_payload({"secret_sauce": "recipe"})
    assert result["secret_sauce"] == "recipe"


# ---------------------------------------------------------------------------
# Immutability: original input must not be mutated
# ---------------------------------------------------------------------------


def test_sanitize_dict_does_not_mutate_original():
    original = {"token": "abc", "market_id": "1.11"}
    _ = sanitize_telegram_payload(original)
    assert original["token"] == "abc"
    assert original["market_id"] == "1.11"


def test_sanitize_list_does_not_mutate_original():
    original = [{"password": "pw"}, {"ok": "val"}]
    _ = sanitize_telegram_payload(original)
    assert original[0]["password"] == "pw"


def test_sanitize_nested_dict_does_not_mutate_original():
    original = {"outer": {"secret": "s", "data": "d"}}
    _ = sanitize_telegram_payload(original)
    assert original["outer"]["secret"] == "s"


# ---------------------------------------------------------------------------
# Recursive structures: list
# ---------------------------------------------------------------------------


def test_sanitize_list_of_dicts_redacts_sensitive_keys():
    data = [{"token": "t1"}, {"market_id": "1.5"}, {"password": "pw"}]
    result = sanitize_telegram_payload(data)
    assert result[0]["token"] == REDACTED
    assert result[1]["market_id"] == "1.5"
    assert result[2]["password"] == REDACTED


def test_sanitize_list_preserves_order():
    data = [1, 2, 3, "a", None]
    assert sanitize_telegram_payload(data) == [1, 2, 3, "a", None]


def test_sanitize_nested_list_in_dict():
    data = {"items": [{"api_key": "k"}, {"name": "x"}]}
    result = sanitize_telegram_payload(data)
    assert result["items"][0]["api_key"] == REDACTED
    assert result["items"][1]["name"] == "x"


# ---------------------------------------------------------------------------
# Recursive structures: tuple
# ---------------------------------------------------------------------------


def test_sanitize_tuple_returns_tuple():
    result = sanitize_telegram_payload((1, 2, 3))
    assert result == (1, 2, 3)
    assert isinstance(result, tuple)


def test_sanitize_tuple_of_dicts_redacts_sensitive_keys():
    data = ({"token": "t"}, {"safe": "ok"})
    result = sanitize_telegram_payload(data)
    assert isinstance(result, tuple)
    assert result[0]["token"] == REDACTED
    assert result[1]["safe"] == "ok"


def test_sanitize_tuple_in_dict_value():
    data = {"payload": ({"session": "s"}, {"x": 1})}
    result = sanitize_telegram_payload(data)
    assert result["payload"][0]["session"] == REDACTED
    assert result["payload"][1]["x"] == 1


# ---------------------------------------------------------------------------
# Deeply nested structures
# ---------------------------------------------------------------------------


def test_sanitize_deeply_nested_dict():
    data = {
        "level1": {
            "level2": {
                "level3": {
                    "authorization": "deep_secret",
                    "safe_key": "safe_val",
                }
            }
        }
    }
    result = sanitize_telegram_payload(data)
    assert result["level1"]["level2"]["level3"]["authorization"] == REDACTED
    assert result["level1"]["level2"]["level3"]["safe_key"] == "safe_val"


def test_sanitize_list_of_lists_with_sensitive():
    data = [[{"bearer": "b"}], [{"x": 1}]]
    result = sanitize_telegram_payload(data)
    assert result[0][0]["bearer"] == REDACTED
    assert result[1][0]["x"] == 1


# ---------------------------------------------------------------------------
# Integer (non-string) dict keys
# ---------------------------------------------------------------------------


def test_sanitize_integer_keys_that_are_not_sensitive_pass_through():
    # str(1) = "1" which is not in TELEGRAM_SENSITIVE_KEYS
    data = {1: "one", 2: "two"}
    result = sanitize_telegram_payload(data)
    assert result[1] == "one"
    assert result[2] == "two"


# ---------------------------------------------------------------------------
# Sensitive key with a complex (non-string) value is still redacted
# ---------------------------------------------------------------------------


def test_sanitize_sensitive_key_with_dict_value_is_redacted():
    # Even if the value is itself a dict, the key match triggers REDACTED
    data = {"token": {"sub_key": "sub_val"}}
    result = sanitize_telegram_payload(data)
    assert result["token"] == REDACTED


def test_sanitize_sensitive_key_with_list_value_is_redacted():
    data = {"password": [1, 2, 3]}
    result = sanitize_telegram_payload(data)
    assert result["password"] == REDACTED


def test_sanitize_sensitive_key_with_none_value_is_redacted():
    data = {"secret": None}
    result = sanitize_telegram_payload(data)
    assert result["secret"] == REDACTED


# ---------------------------------------------------------------------------
# Mixed sensitive and non-sensitive in same dict
# ---------------------------------------------------------------------------


def test_sanitize_mixed_keys_dict():
    data = {
        "token": "tok",
        "market_id": "1.99",
        "password": "pw",
        "event_name": "match",
        "api_key": "key",
        "price": 2.5,
    }
    result = sanitize_telegram_payload(data)
    assert result["token"] == REDACTED
    assert result["password"] == REDACTED
    assert result["api_key"] == REDACTED
    assert result["market_id"] == "1.99"
    assert result["event_name"] == "match"
    assert result["price"] == 2.5


# ---------------------------------------------------------------------------
# REDACTED constant value
# ---------------------------------------------------------------------------


def test_redacted_constant_is_expected_string():
    assert REDACTED == "[REDACTED]"


# ---------------------------------------------------------------------------
# Regression: sanitize is idempotent (already-redacted value stays redacted)
# ---------------------------------------------------------------------------


def test_sanitize_already_redacted_value_stays_redacted():
    # If a value is already "[REDACTED]" for a sensitive key, it remains so
    data = {"token": "[REDACTED]"}
    result = sanitize_telegram_payload(data)
    assert result["token"] == REDACTED


def test_sanitize_non_sensitive_key_with_redacted_string_passes_through():
    # Non-sensitive key whose value happens to be "[REDACTED]" stays unchanged
    data = {"market_id": "[REDACTED]"}
    result = sanitize_telegram_payload(data)
    assert result["market_id"] == "[REDACTED]"


# ---------------------------------------------------------------------------
# Boundary: TELEGRAM_SENSITIVE_KEYS contains exactly the documented members
# ---------------------------------------------------------------------------


def test_telegram_sensitive_keys_contains_all_documented_members():
    expected = {
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
    assert TELEGRAM_SENSITIVE_KEYS == expected


# ---------------------------------------------------------------------------
# Negative: keys that look similar but are not in the sensitive set
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "key",
    [
        "tokenize",
        "tokens",
        "passwords",
        "secrets",
        "api_keys",
        "auth",
        "authorized",
        "sessions",
        "bearer_token",
    ],
)
def test_sanitize_similar_but_non_sensitive_keys_pass_through(key):
    result = sanitize_telegram_payload({key: "value"})
    assert result[key] == "value"
