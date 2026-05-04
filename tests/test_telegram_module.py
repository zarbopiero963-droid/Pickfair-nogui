"""PR2A sanitizer and defensive DB-save characterization tests."""

import unittest

from telegram_module import TelegramModule
from telegram_sanitizer import sanitize_telegram_payload


class TelegramSanitizerTests(unittest.TestCase):
    """Focused sanitizer and defensive DB-save tests for PR2A."""

    @staticmethod
    def _sv(tag: str) -> str:
        """Return deterministic non-secret value payloads."""
        return f"value-{tag}"

    @staticmethod
    def _build_nested():
        """Build nested payload with sensitive and non-sensitive fields."""
        return {
            "Authorization": "bearer-value",
            "user_session": "uv",
            "list": [
                {"access_token": "value-access-token", "client_secret": "value-client-secret"},
                {
                    "bot_token": "value-bot-token",
                    "api_secret": "value-api-secret",
                    "private_key": "value-private-key",
                    "market_id": "1.22",
                },
            ],
            "token_count": 17,
            "tokenizer": "keep-tokenizer",
            "author": "keep-author",
            "authored_by": "keep-authored-by",
            "keyboard": "keep-keyboard",
            "monkey": "keep-monkey",
            "jockey": "keep-jockey",
        }

    @staticmethod
    def _build_tuple_payload():
        """Build tuple payload for recursive tuple sanitization coverage."""
        return (
            {
                "api_secret": TelegramSanitizerTests._sv("tuple-api-secret"),
                "auth_token": TelegramSanitizerTests._sv("tuple-auth"),
                "author": "tuple-author",
            },
            "plain-value",
        )

    @classmethod
    def _build_signal(cls):
        """Build full signal fixture used across sanitizer tests."""
        return {
            "raw_text": "operator text",
            "refresh_token": cls._sv("refresh"),
            "auth": cls._sv("auth"),
            "bearer_token": cls._sv("bearer"),
            "secret_key": cls._sv("secret-key"),
            "prefix_api_key_id": cls._sv("api-key-id"),
            "internal_session_token_value": cls._sv("session-token"),
            "nested": cls._build_nested(),
            "runner_name": "Runner",
            "selection_id": 123,
            "tuple_payload": cls._build_tuple_payload(),
        }

    def test_redacts_and_keeps(self):
        """Credential-like keys are redacted and safe diagnostics kept."""
        raw_signal = self._build_signal()
        out = sanitize_telegram_payload(raw_signal)

        self.assertIsNot(out, raw_signal)
        sensitive_top_level_keys = (
            "refresh_token",
            "auth",
            "bearer_token",
            "secret_key",
            "prefix_api_key_id",
            "internal_session_token_value",
        )
        for key in sensitive_top_level_keys:
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

    def test_tuple_recursion(self):
        """Tuple recursion redacts sensitive dict fields and keeps safe values."""
        raw_signal = self._build_signal()
        out = sanitize_telegram_payload(raw_signal)
        self.assertEqual(out["tuple_payload"][0]["api_secret"], "[REDACTED]")
        self.assertEqual(out["tuple_payload"][0]["auth_token"], "[REDACTED]")
        self.assertEqual(out["tuple_payload"][0]["author"], "tuple-author")
        self.assertEqual(out["tuple_payload"][1], "plain-value")
        self.assertEqual(raw_signal["refresh_token"], self._sv("refresh"))


class _DbStub:
    def __init__(self):
        self.saved = []

    def save_received_signal(self, payload):
        self.saved.append(payload)


class _Host(TelegramModule):
    def __init__(self):
        self.database = _DbStub()

    @property
    def db(self):
        return self.database


class TelegramModuleDbSaveTests(unittest.TestCase):
    """Defensive save path remains sanitized and non-mutating."""

    def test_defensive_save_sanitizes(self):
        """Defensive save sanitizes sensitive fields and keeps original input."""
        host = _Host()
        raw = {
            "token": "value-token",
            "authorization_header": "value-authz-header",
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
        self.assertEqual(raw["token"], "value-token")
        self.assertEqual(raw["authorization_header"], "value-authz-header")
