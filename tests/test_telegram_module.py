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
