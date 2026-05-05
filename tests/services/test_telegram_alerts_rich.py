"""PR2A rich alert formatting and sanitizer regression tests."""

import unittest
from services.telegram_alerts_service import TelegramAlertsService


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
        svc = TelegramAlertsService(settings_service=_Settings(), telegram_sender=sender)
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
        svc = TelegramAlertsService(settings_service=_Settings(), telegram_sender=sender)
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
        svc = TelegramAlertsService(settings_service=_Settings(), telegram_sender=sender)
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
        svc = TelegramAlertsService(settings_service=_Settings(), telegram_sender=sender)
        details = {"evidence_summary": {"rule_hits_in_window": 3, "raw": {1, 2}}, "suggested_action": "Escalate"}
        svc.notify_alert(
            {
                "severity": "critical",
                "code": "CTO-2",
                "source": "cto_reviewer",
                "message": "operator evidence",
                "details": details,
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
        svc = TelegramAlertsService(settings_service=_SettingsCooldown(), telegram_sender=sender)
        alert = {"severity": "critical", "code": "CRIT-1", "message": "a"}
        first = svc.notify_alert(alert)
        second = svc.notify_alert(alert)
        self.assertTrue(first["delivered"])
        self.assertFalse(second["delivered"])
        self.assertEqual(second["reason"], "dedup_cooldown")

    @staticmethod
    def _details_payload():
        """Build details payload covering sensitive and safe keys."""
        return {
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
        }

    def _send_redaction_case(self):
        sender = _Sender()
        svc = TelegramAlertsService(settings_service=_Settings(), telegram_sender=sender)
        alert = {
            "severity": "critical",
            "code": "CTO-KEYS",
            "message": "sanitizer coverage",
            "details": self._details_payload(),
        }
        svc.notify_alert(alert)
        return sender.messages[0][1]

    def test_redacts_core_keys(self):
        """Core sensitive details are redacted."""
        text = self._send_redaction_case()
        self.assertIn("[REDACTED]", text)
        for key in ("token", "auth_token", "access_token", "bearer", "user_session", "session", "session_token"):
            self.assertIn(f"{key}=[REDACTED]", text)

    def test_redacts_compound_keys(self):
        """Compound details are redacted and raw values are absent."""
        text = self._send_redaction_case()
        for key in ("api_key", "secret", "password", "authorization", "Authorization"):
            self.assertIn(f"{key}=[REDACTED]", text)
        for raw in (
            "value-api-key",
            "value-secret",
            "value-password",
            "value-authorization",
            "value-authorization-upper",
        ):
            self.assertNotIn(raw, text)
        for raw in (
            f"token={make_value('token')}",
            f"auth_token={make_value('auth')}",
            f"access_token={make_value('access')}",
            f"bearer={make_value('bearer')}",
            f"user_session={make_value('user-session')}",
            f"session={make_value('session')}",
            f"session_token={make_value('session-token')}",
        ):
            self.assertNotIn(raw, text)

    def test_keeps_safe_fields(self):
        """Safe operational fields remain visible."""
        text = self._send_redaction_case()
        self.assertIn("market_id=1.234", text)
