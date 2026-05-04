import unittest

from telegram_module import TelegramModule
from telegram_sanitizer import sanitize_telegram_payload


class TelegramSanitizerTests(unittest.TestCase):
    """Focused sanitizer and defensive DB-save tests for PR2A."""

    @staticmethod
    def _build_signal():
        return {
            "raw_text": "operator text",
            "refresh_token": "rv",
            "auth": "av",
            "bearer_token": "bv",
            "secret_key": "skv",
            "prefix_api_key_id": "akid",
            "internal_session_token_value": "isv",
            "nested": {
                "Authorization": "bearer-value",
                "user_session": "uv",
                "list": [
                    {"access_token": "a1", "client_secret": "c1"},
                    {"bot_token": "b1", "api_secret": "s1", "private_key": "p1", "market_id": "1.22"},
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

    def test_sanitizer_redacts_credentials_and_preserves_diagnostics(self):
        raw_signal = self._build_signal()
        out = sanitize_telegram_payload(raw_signal)

        self.assertIsNot(out, raw_signal)
        for key in ("refresh_token", "auth", "bearer_token", "secret_key", "prefix_api_key_id", "internal_session_token_value"):
            self.assertEqual(out[key], "[REDACTED]")

        self.assertEqual(out["nested"]["Authorization"], "[REDACTED]")
        self.assertEqual(out["nested"]["user_session"], "[REDACTED]")
        self.assertEqual(out["nested"]["list"][0]["access_token"], "[REDACTED]")
        self.assertEqual(out["nested"]["list"][0]["client_secret"], "[REDACTED]")
        self.assertEqual(out["nested"]["list"][1]["bot_token"], "[REDACTED]")
        self.assertEqual(out["nested"]["list"][1]["api_secret"], "[REDACTED]")
        self.assertEqual(out["nested"]["list"][1]["private_key"], "[REDACTED]")

        for key, expected in (
            ("token_count", 17),
            ("tokenizer", "keep-tokenizer"),
            ("author", "keep-author"),
            ("authored_by", "keep-authored-by"),
            ("keyboard", "keep-keyboard"),
            ("monkey", "keep-monkey"),
            ("jockey", "keep-jockey"),
        ):
            self.assertEqual(out["nested"][key], expected)
        self.assertEqual(out["nested"]["list"][1]["market_id"], "1.22")
        self.assertEqual(out["runner_name"], "Runner")
        self.assertEqual(out["selection_id"], 123)
        self.assertEqual(out["raw_text"], "operator text")

    def test_sanitizer_handles_tuple_recursion(self):
        raw_signal = self._build_signal()
        out = sanitize_telegram_payload(raw_signal)
        self.assertEqual(out["tuple_payload"][0]["api_secret"], "[REDACTED]")
        self.assertEqual(out["tuple_payload"][0]["auth_token"], "[REDACTED]")
        self.assertEqual(out["tuple_payload"][0]["author"], "tuple-author")
        self.assertEqual(out["tuple_payload"][1], "plain-value")
        self.assertEqual(raw_signal["refresh_token"], "rv")


class _DbStub:
    def __init__(self):
        self.saved = []

    def save_received_signal(self, payload):
        self.saved.append(payload)


class _Host(TelegramModule):
    def __init__(self):
        self.db = _DbStub()


class TelegramModuleDbSaveTests(unittest.TestCase):
    """Defensive save path remains sanitized and non-mutating."""

    def test_defensive_save_sanitizes_without_mutation(self):
        host = _Host()
        raw = {
            "token": "tv",
            "authorization_header": "bh",
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
        self.assertEqual(saved["token"], "[REDACTED]")
        self.assertEqual(saved["authorization_header"], "[REDACTED]")
        self.assertEqual(saved["runner_name"], "Runner A")
        self.assertEqual(saved["selection_id"], 123)
        self.assertEqual(raw["token"], "tv")
        self.assertEqual(raw["authorization_header"], "bh")
