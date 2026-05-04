from telegram_module import _sanitize_telegram_payload


def test_telegram_module_sanitizes_nested_raw_signal_sensitive_values_without_mutation():
    raw_signal = {
        "raw_text": "operator text",
        "token": "t1",
        "nested": {
            "Authorization": "Bearer abc",
            "user_session": "u1",
            "list": [{"access_token": "a1"}, {"market_id": "1.22"}],
        },
    }
    out = _sanitize_telegram_payload(raw_signal)

    assert out is not raw_signal
    assert out["token"] == "[REDACTED]"
    assert out["nested"]["Authorization"] == "[REDACTED]"
    assert out["nested"]["user_session"] == "[REDACTED]"
    assert out["nested"]["list"][0]["access_token"] == "[REDACTED]"
    assert out["nested"]["list"][1]["market_id"] == "1.22"
    assert out["raw_text"] == "operator text"
    assert raw_signal["token"] == "t1"
    assert raw_signal["nested"]["Authorization"] == "Bearer abc"


def test_telegram_module_sanitize_empty_signal_dict_returns_empty_dict():
    out = _sanitize_telegram_payload({})
    assert out == {}


def test_telegram_module_sanitize_none_signal_returns_none():
    assert _sanitize_telegram_payload(None) is None


def test_telegram_module_sanitize_tuple_in_signal_is_sanitized():
    signal = {"data": ({"password": "pw"}, {"safe": "ok"})}
    out = _sanitize_telegram_payload(signal)
    assert isinstance(out["data"], tuple)
    assert out["data"][0]["password"] == "[REDACTED]"
    assert out["data"][1]["safe"] == "ok"


def test_telegram_module_sanitize_partial_key_match_not_redacted():
    # "not_a_token" contains "token" but is not in sensitive key set
    signal = {"not_a_token": "value", "market_id": "1.5"}
    out = _sanitize_telegram_payload(signal)
    assert out["not_a_token"] == "value"
    assert out["market_id"] == "1.5"


def test_telegram_module_sanitize_all_sensitive_keys_in_flat_signal():
    signal = {
        "token": "t",
        "auth_token": "at",
        "access_token": "ac",
        "bearer": "b",
        "user_session": "us",
        "session": "s",
        "session_token": "st",
        "api_key": "ak",
        "secret": "sc",
        "password": "pw",
        "authorization": "az",
        "safe_field": "safe_value",
    }
    out = _sanitize_telegram_payload(signal)
    for key in (
        "token", "auth_token", "access_token", "bearer", "user_session",
        "session", "session_token", "api_key", "secret", "password", "authorization",
    ):
        assert out[key] == "[REDACTED]", f"Expected {key} to be redacted"
    assert out["safe_field"] == "safe_value"


def test_telegram_module_sanitize_signal_data_list_of_dicts():
    signal_data = [{"api_key": "k1"}, {"event": "goal"}, {"secret": "s1"}]
    out = _sanitize_telegram_payload(signal_data)
    assert out[0]["api_key"] == "[REDACTED]"
    assert out[1]["event"] == "goal"
    assert out[2]["secret"] == "[REDACTED]"


def test_telegram_module_sanitize_returns_new_dict_not_same_reference():
    signal = {"market_id": "1.0", "stake": 10}
    out = _sanitize_telegram_payload(signal)
    assert out is not signal
    assert out == signal


def test_telegram_module_sanitize_sensitive_key_case_insensitive_mixed():
    signal = {
        "TOKEN": "upper",
        "Password": "mixed",
        "SECRET": "allup",
    }
    out = _sanitize_telegram_payload(signal)
    assert out["TOKEN"] == "[REDACTED]"
    assert out["Password"] == "[REDACTED]"
    assert out["SECRET"] == "[REDACTED]"
