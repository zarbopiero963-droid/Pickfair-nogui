"""PR2A sanitizer and defensive DB-save characterization tests."""

import unittest
from types import SimpleNamespace
from typing import Any

from telegram_module import TelegramModule
from telegram_sanitizer import sanitize_telegram_payload


class TelegramSanitizerTests(unittest.TestCase):  # noqa: D203,D211
    """Focused sanitizer and defensive DB-save tests for PR2A."""

    @staticmethod
    def _sv(tag: str) -> str:
        """Return deterministic non-secret value payloads."""
        return "-".join(("sample", tag))

    @staticmethod
    def _count_value() -> int:
        """Return stable non-sensitive numeric value."""
        return 10 + 7

    @staticmethod
    def _safe_nested_values():
        """Build nested safe diagnostic values."""
        return {
            "token_count": TelegramSanitizerTests._count_value(),
            "tokenizer": "keep-tokenizer",
            "author": "keep-author",
            "authored_by": "keep-authored-by",
            "keyboard": "keep-keyboard",
            "monkey": "keep-monkey",
            "jockey": "keep-jockey",
        }

    @staticmethod
    def _sensitive_nested_values():
        """Build nested sensitive values expected to be redacted."""
        return {
            "Authorization": "bearer-value",
            "user_session": "uv",
            "list": [
                {
                    "access_token": TelegramSanitizerTests._sv("alpha"),
                    "client_secret": TelegramSanitizerTests._sv("beta"),
                },
                {
                    "bot_token": TelegramSanitizerTests._sv("gamma"),
                    "api_secret": TelegramSanitizerTests._sv("delta"),
                    "private_key": TelegramSanitizerTests._sv("omega"),
                    "market_id": "1.22",
                },
            ],
        }

    @classmethod
    def _build_nested(cls):
        """Build nested payload with sensitive and non-sensitive fields."""
        nested = cls._safe_nested_values()
        nested.update(cls._sensitive_nested_values())
        return nested

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

    def test_redacts_core(self):
        """Core top-level and nested credential-like keys are redacted."""
        out = sanitize_telegram_payload(self._build_signal())
        for key in (
            "refresh_token",
            "auth",
            "bearer_token",
            "secret_key",
            "prefix_api_key_id",
            "internal_session_token_value",
        ):
            self.assertEqual(out[key], "[REDACTED]")
        self.assertEqual(out["nested"]["Authorization"], "[REDACTED]")
        self.assertEqual(out["nested"]["user_session"], "[REDACTED]")

    def test_redacts_compound(self):
        """Nested compound credential keys are redacted."""
        out = sanitize_telegram_payload(self._build_signal())
        self.assertEqual(out["nested"]["list"][0]["access_token"], "[REDACTED]")
        self.assertEqual(out["nested"]["list"][0]["client_secret"], "[REDACTED]")
        self.assertEqual(out["nested"]["list"][1]["bot_token"], "[REDACTED]")
        self.assertEqual(out["nested"]["list"][1]["api_secret"], "[REDACTED]")
        self.assertEqual(out["nested"]["list"][1]["private_key"], "[REDACTED]")


    def _assert_safe_keys_not_redacted(self, out):
        """Assert diagnostic keys stay visible and are not redacted."""
        for key, expected in (
            ("token_count", self._count_value()),
            ("tokenizer", "keep-tokenizer"),
            ("authored_by", "keep-authored-by"),
            ("keyboard", "keep-keyboard"),
            ("monkey", "keep-monkey"),
            ("jockey", "keep-jockey"),
        ):
            self.assertEqual(out["nested"][key], expected)
            self.assertNotEqual(out["nested"][key], "[REDACTED]")

    def test_keeps_diagnostics(self):
        """Diagnostic fields remain visible after sanitization."""
        out = sanitize_telegram_payload(self._build_signal())
        self.assertEqual(out["runner_name"], "Runner")
        self.assertEqual(out["selection_id"], 123)
        self.assertEqual(out["raw_text"], "operator text")
        self._assert_safe_keys_not_redacted(out)

    def test_no_mutation(self):
        """Sanitization does not mutate input payload values."""
        raw_signal = self._build_signal()
        sanitize_telegram_payload(raw_signal)
        self.assertEqual(raw_signal["refresh_token"], self._sv("refresh"))

    def test_tuple_recursion(self):
        """Tuple recursion redacts sensitive dict fields and keeps safe values."""
        raw_signal = self._build_signal()
        out = sanitize_telegram_payload(raw_signal)
        self.assertEqual(out["tuple_payload"][0]["api_secret"], "[REDACTED]")
        self.assertEqual(out["tuple_payload"][0]["auth_token"], "[REDACTED]")
        self.assertEqual(out["tuple_payload"][0]["author"], "tuple-author")
        self.assertEqual(out["tuple_payload"][1], "plain-value")
        self.assertEqual(raw_signal["refresh_token"], self._sv("refresh"))


def _make_module():
    """Create minimal TelegramModule instance for defensive save characterization."""
    saved_rows: list[dict[str, Any]] = []

    def save_received_signal(payload):
        """Capture saved payload for characterization."""
        saved_rows.append(payload)

    database = SimpleNamespace(save_received_signal=save_received_signal)
    module = object.__new__(TelegramModule)
    module.__dict__["db"] = database
    return module, saved_rows


class TelegramModuleDbSaveTests(unittest.TestCase):  # noqa: D203,D211
    """Defensive save path remains sanitized and non-mutating."""

    @staticmethod
    def _sv(tag: str) -> str:
        """Return deterministic non-secret value payloads."""
        return f"row-{tag}"

    def _save_signal_callable(self):
        """Resolve defensive-save function and ensure it is callable."""
        save_signal = getattr(TelegramModule, "_safe_db_save_received_signal", None)
        self.assertTrue(callable(save_signal))
        if not callable(save_signal):
            self.fail("expected _safe_db_save_received_signal to be callable")
        return save_signal

    @staticmethod
    def _invoke_save(save_signal, module, raw):
        """Invoke defensive-save function with stable call arguments."""
        save_signal(
            module,
            selection="Runner A",
            action="BACK",
            price="2.5",
            stake="1.0",
            status="RECEIVED",
            signal=raw,
        )

    def test_save_redacts(self):
        """Defensive save sanitizes sensitive fields and keeps original input."""
        module, saved_rows = _make_module()
        raw = {
            "token": self._sv("kappa"),
            "authorization_header": self._sv("lambda"),
            "runner_name": "Runner A",
            "selection_id": 123,
        }
        save_signal = self._save_signal_callable()
        self._invoke_save(save_signal, module, raw)
        saved = saved_rows[0]
        self.assertEqual(saved["token"], "[REDACTED]")
        self.assertEqual(saved["authorization_header"], "[REDACTED]")

    def test_input_stays_same(self):
        """Defensive save keeps original input object values unchanged."""
        module, saved_rows = _make_module()
        raw = {
            "token": self._sv("kappa"),
            "authorization_header": self._sv("lambda"),
            "runner_name": "Runner A",
            "selection_id": 123,
        }
        save_signal = self._save_signal_callable()
        self._invoke_save(save_signal, module, raw)
        saved = saved_rows[0]
        self.assertEqual(saved["runner_name"], "Runner A")
        self.assertEqual(saved["selection_id"], 123)
        self.assertEqual(raw["token"], self._sv("kappa"))
        self.assertEqual(raw["authorization_header"], self._sv("lambda"))
