from telegram_sanitizer import sanitize_telegram_payload
from telegram_module import TelegramModule


def test_telegram_sanitizer_redacts_nested_and_compound_keys_without_mutation():
    raw_signal = {
        "raw_text": "operator text",
        "refresh_token": "r1",
        "auth": "auth-only",
        "bearer_token": "bearer",
        "secret_key": "sec-key",
        "prefix_api_key_id": "api-key-id",
        "internal_session_token_value": "sess-token",
        "nested": {
            "Authorization": "Bearer abc",
            "user_session": "u1",
            "list": [
                {"access_token": "a1", "client_secret": "cs"},
                {"bot_token": "bt", "api_secret": "as", "private_key": "pk", "market_id": "1.22"},
            ],
            "token_count": 17,
            "tokenizer": "keep-tokenizer",
            "author": "keep-author",
            "authored_by": "keep-authored-by",
            "keyboard": "keep-keyboard",
            "monkey": "keep-monkey",
            "jockey": "keep-jockey",
        },
        "runner_name": "Runner",
        "selection_id": 123,
        "tuple_payload": (
            {"api_secret": "tuple-secret", "auth_token": "tuple-auth", "author": "tuple-author"},
            "plain-value",
        ),
    }
    out = sanitize_telegram_payload(raw_signal)

    assert out is not raw_signal
    assert out["refresh_token"] == "[REDACTED]"
    assert out["auth"] == "[REDACTED]"
    assert out["bearer_token"] == "[REDACTED]"
    assert out["secret_key"] == "[REDACTED]"
    assert out["prefix_api_key_id"] == "[REDACTED]"
    assert out["internal_session_token_value"] == "[REDACTED]"
    assert out["nested"]["Authorization"] == "[REDACTED]"
    assert out["nested"]["user_session"] == "[REDACTED]"
    assert out["nested"]["list"][0]["access_token"] == "[REDACTED]"
    assert out["nested"]["list"][0]["client_secret"] == "[REDACTED]"
    assert out["nested"]["list"][1]["bot_token"] == "[REDACTED]"
    assert out["nested"]["list"][1]["api_secret"] == "[REDACTED]"
    assert out["nested"]["list"][1]["private_key"] == "[REDACTED]"
    assert out["nested"]["list"][1]["market_id"] == "1.22"
    assert out["nested"]["token_count"] == 17
    assert out["nested"]["tokenizer"] == "keep-tokenizer"
    assert out["nested"]["author"] == "keep-author"
    assert out["nested"]["authored_by"] == "keep-authored-by"
    assert out["nested"]["keyboard"] == "keep-keyboard"
    assert out["nested"]["monkey"] == "keep-monkey"
    assert out["nested"]["jockey"] == "keep-jockey"
    assert out["runner_name"] == "Runner"
    assert out["selection_id"] == 123
    assert out["tuple_payload"][0]["api_secret"] == "[REDACTED]"
    assert out["tuple_payload"][0]["auth_token"] == "[REDACTED]"
    assert out["tuple_payload"][0]["author"] == "tuple-author"
    assert out["tuple_payload"][1] == "plain-value"
    assert out["raw_text"] == "operator text"
    assert raw_signal["refresh_token"] == "r1"


class _DbStub:
    def __init__(self):
        self.saved = []

    def save_received_signal(self, payload):
        self.saved.append(payload)


class _Host(TelegramModule):
    def __init__(self):
        self.db = _DbStub()


def test_safe_db_save_received_signal_defensively_sanitizes_input_signal():
    host = _Host()
    raw = {
        "token": "tok",
        "authorization_header": "Bearer z",
        "runner_name": "Runner A",
        "selection_id": 123,
    }

    host._safe_db_save_received_signal(
        selection="Runner A",
        action="BACK",
        price="2.5",
        stake="1.0",
        status="RECEIVED",
        signal=raw,
    )

    saved = host.db.saved[0]
    assert saved["token"] == "[REDACTED]"
    assert saved["authorization_header"] == "[REDACTED]"
    assert saved["runner_name"] == "Runner A"
    assert saved["selection_id"] == 123
    assert raw["token"] == "tok"
    assert raw["authorization_header"] == "Bearer z"
