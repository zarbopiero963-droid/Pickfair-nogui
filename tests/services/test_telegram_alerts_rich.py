import pytest

from services.telegram_alerts_service import TelegramAlertsService


class _Settings:
    def load_telegram_config_row(self):
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
    def __init__(self):
        self.messages = []

    def send_message(self, chat_id, text):
        self.messages.append((chat_id, text))


@pytest.mark.smoke
def test_telegram_alerts_rich_settings_and_sender_method_are_honest():
    sender = _Sender()
    svc = TelegramAlertsService(settings_service=_Settings(), telegram_sender=sender)

    status = svc.availability_status()
    assert status["alerts_enabled"] is True
    assert status["sender_available"] is True
    assert status["deliverable"] is True

    result = svc.notify_alert({"severity": "error", "code": "RICH-1", "message": "boom", "details": {"x": 1}})

    assert result["delivered"] is True
    assert len(sender.messages) == 1
    assert "Details: x=1" in sender.messages[0][1]


@pytest.mark.smoke
def test_telegram_alerts_rich_includes_governance_fields():
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
    assert result["delivered"] is True
    text = sender.messages[0][1]
    assert "Incident Class: execution_consistency_incident" in text
    assert "Normalized Severity: CRITICAL" in text


@pytest.mark.smoke
def test_telegram_alerts_rich_includes_timestamp_and_suggested_action():
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
    assert "Time: 2026-04-14 12:00:00 UTC" in text
    assert "Suggested action: Drain queue and inspect stalls" in text


@pytest.mark.smoke
def test_telegram_alerts_rich_includes_source_and_sanitized_evidence_summary():
    sender = _Sender()
    svc = TelegramAlertsService(settings_service=_Settings(), telegram_sender=sender)
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
    assert "Source: cto_reviewer" in text
    assert "rule_hits_in_window" in text
    assert "evidence_summary=" in text
    assert "<object object at" not in text
    assert "raw" in text


@pytest.mark.smoke
def test_telegram_alerts_rich_records_suppression_reason_for_critical_dedup_drop():
    class _SettingsCooldown(_Settings):
        def load_telegram_config_row(self):
            data = super().load_telegram_config_row()
            data["alert_dedup_enabled"] = True
            data["telegram_alert_cooldown_sec"] = 999
            return data

    sender = _Sender()
    svc = TelegramAlertsService(settings_service=_SettingsCooldown(), telegram_sender=sender)
    alert = {"severity": "critical", "code": "CRIT-1", "message": "a"}
    first = svc.notify_alert(alert)
    second = svc.notify_alert(alert)
    assert first["delivered"] is True
    assert second["delivered"] is False
    assert second["reason"] == "dedup_cooldown"


@pytest.mark.smoke
def test_telegram_alerts_rich_redacts_expanded_sensitive_keyset_case_insensitive():
    sender = _Sender()
    svc = TelegramAlertsService(settings_service=_Settings(), telegram_sender=sender)
    svc.notify_alert(
        {
            "severity": "critical",
            "code": "CTO-KEYS",
            "message": "sanitizer coverage",
            "details": {
                "token": "tkn",
                "auth_token": "auth",
                "access_token": "acc",
                "bearer": "bear",
                "user_session": "us",
                "session": "sess",
                "session_token": "st",
                "api_key": "api",
                "secret": "sec",
                "password": "pwd",
                "authorization": "authz",
                "Authorization": "AUTHZ_UPPER",
                "market_id": "1.234",
            },
        }
    )
    text = sender.messages[0][1]
    assert "[REDACTED]" in text
    for raw in (
        "token=tkn",
        "auth_token=auth",
        "access_token=acc",
        "bearer=bear",
        "user_session=us",
        "session=sess",
        "session_token=st",
        "api_key=api",
        "secret=sec",
        "password=pwd",
        "authorization=authz",
        "Authorization=AUTHZ_UPPER",
    ):
        assert raw not in text
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
        assert key in text
    assert "market_id=1.234" in text
