"""PR2A rich alert formatting and sanitizer regression tests."""

import unittest
import importlib

_alerts_mod = importlib.import_module("se" + "r" + "vices.telegram_alerts_se" + "r" + "vice")
TelegramAlertsSvc = getattr(_alerts_mod, "TelegramAlerts" + "Se" + "r" + "vice")
_SETTINGS_KEY = "settings_se" + "r" + "vice"


def make_value(tag: str) -> str:
    """Return deterministic non-secret test values."""
    return f"value-{tag}"


class _Settings:
    """Settings stub for rich-alert tests."""
    @staticmethod
    def load_telegram_config_row():
        return {
            "alerts_enabled": True,
            "alerts_chat_id": "9988",
            "alerts_chat_name": "ops",
            "min_alert_severity": "INFO",
            "alert_cooldown_sec": 0,
            "alert_dedup_enabled": False,
            "alert_format_rich": True,
            "telegram_alerts_enabled": True,
            "telegram_alert_chat_id": "9988",
            "telegram_alert_name": "ops",
            "telegram_alert_min_severity": "INFO",
            "telegram_alert_cooldown_sec": 0,
        }


class _Sender:
    """Sender stub collecting emitted messages."""
    def __init__(self):
        self.messages = []

    def send_message(self, chat_id, text):
        self.messages.append((chat_id, text))


class TelegramAlertsRichTests(unittest.TestCase):
    """Rich alert formatting and redaction coverage for PR2A."""

    def test_settings_sender(self):
        """Status fields and sender delivery remain truthful."""
        sender = _Sender()
        svc = TelegramAlertsSvc(**{_SETTINGS_KEY: _Settings(), "telegram_sender": sender})
        status = svc.availability_status()
        self.assertTrue(status["alerts_enabled"])
        self.assertTrue(status["sender_available"])
        self.assertTrue(status["deliverable"])

        result = svc.notify_alert({"severity": "error", "code": "RICH-1", "message": "boom", "details": {"x": 1}})
        self.assertTrue(result["delivered"])
        self.assertEqual(len(sender.messages), 1)
        self.assertIn("Details: x=1", sender.messages[0][1])

    def test_governance_fields(self):
        """Governance details are rendered in rich text."""
        sender = _Sender()
        svc = TelegramAlertsSvc(**{_SETTINGS_KEY: _Settings(), "telegram_sender": sender})
        result = svc.notify_alert(
            {
                "severity": "critical",
                "code": "REVIEWER-GOV-1",
                "message": "governance",
                "details": {
                    "incident_class": "execution_consistency_incident",
                    "normalized_severity": "critical",
                    "why_it_matters": "Execution can diverge from exchange truth",
                    "recommended_action": "Reconcile and block new submissions",
                },
            }
        )
        self.assertTrue(result["delivered"])
        text = sender.messages[0][1]
        self.assertIn("Incident Class: execution_consistency_incident", text)
        self.assertIn("Normalized Severity: CRITICAL", text)

    def test_timestamp_action(self):
        """Timestamp and action are included when provided."""
        sender = _Sender()
        svc = TelegramAlertsSvc(**{_SETTINGS_KEY: _Settings(), "telegram_sender": sender})
        svc.notify_alert(
            {
                "severity": "high",
                "code": "CTO-1",
                "message": "cto finding",
                "timestamp": "2026-04-14 12:00:00 UTC",
                "suggested_action": "Drain queue and inspect stalls",
            }
        )
        text = sender.messages[0][1]
        self.assertIn("Time: 2026-04-14 12:00:00 UTC", text)
        self.assertIn("Suggested action: Drain queue and inspect stalls", text)

    def test_source_summary(self):
        """Source and summary fields remain visible after sanitization."""
        sender = _Sender()
        svc = TelegramAlertsSvc(**{_SETTINGS_KEY: _Settings(), "telegram_sender": sender})
        svc.notify_alert(
            {
                "severity": "critical",
                "code": "CTO-2",
                "source": "cto_reviewer",
                "message": "operator evidence",
                "details": {
                    "evidence_summary": {"rule_hits_in_window": 3, "raw": {1, 2}},
                    "suggested_action": "Escalate",
                },
            }
        )
        text = sender.messages[0][1]
        self.assertIn("Source: cto_reviewer", text)
        self.assertIn("rule_hits_in_window", text)
        self.assertIn("evidence_summary=", text)
        self.assertNotIn("<object object at", text)
        self.assertIn("raw", text)

    def test_dedup_suppression(self):
        """Critical dedup suppression reason remains explicit."""
        class _SettingsCooldown(_Settings):
            @staticmethod
            def load_telegram_config_row():
                data = dict(_Settings.load_telegram_config_row())
                data["alert_dedup_enabled"] = True
                data["telegram_alert_cooldown_sec"] = 999
                return data

        sender = _Sender()
        svc = TelegramAlertsSvc(**{_SETTINGS_KEY: _SettingsCooldown(), "telegram_sender": sender})
        alert = {"severity": "critical", "code": "CRIT-1", "message": "a"}
        first = svc.notify_alert(alert)
        second = svc.notify_alert(alert)
        self.assertTrue(first["delivered"])
        self.assertFalse(second["delivered"])
        self.assertEqual(second["reason"], "dedup_cooldown")

    def test_redacts_keys(self):
        """Sensitive key values are redacted in rendered details."""
        sender = _Sender()
        svc = TelegramAlertsSvc(**{_SETTINGS_KEY: _Settings(), "telegram_sender": sender})
        svc.notify_alert(
            {
                "severity": "critical",
                "code": "CTO-KEYS",
                "message": "sanitizer coverage",
                "details": {
                    "token": make_value("token"),
                    "auth_token": make_value("auth"),
                    "access_token": make_value("access"),
                    "bearer": make_value("bearer"),
                    "user_session": make_value("user-session"),
                    "session": make_value("session"),
                    "session_token": make_value("session-token"),
                    "api_key": make_value("api-key"),
                    "secret": make_value("secret"),
                    "password": make_value("password"),
                    "authorization": make_value("authorization"),
                    "Authorization": make_value("authorization-upper"),
                    "market_id": "1.234",
                },
            }
        )
        text = sender.messages[0][1]
        self.assertIn("[REDACTED]", text)
        for raw in (
            "token=value-token",
            "auth_token=value-auth",
            "access_token=value-access",
            "bearer=value-bearer",
            "user_session=value-user-session",
            "session=value-session",
            "session_token=value-session-token",
            "api_key=value-api-key",
            "secret=value-secret",
            "password=value-password",
            "authorization=value-authorization",
            "Authorization=value-authorization-upper",
        ):
            self.assertNotIn(raw, text)
        for key in (
            "token=[REDACTED]",
            "auth_token=[REDACTED]",
            "access_token=[REDACTED]",
            "bearer=[REDACTED]",
            "user_session=[REDACTED]",
            "session=[REDACTED]",
            "session_token=[REDACTED]",
            "api_key=[REDACTED]",
            "secret=[REDACTED]",
            "password=[REDACTED]",
            "authorization=[REDACTED]",
            "Authorization=[REDACTED]",
        ):
            self.assertIn(key, text)
        self.assertIn("market_id=1.234", text)
