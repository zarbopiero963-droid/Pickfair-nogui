import pytest

from services.telegram_alerts_service import TelegramAlertsService


class _Settings:
    def __init__(self, min_alert_severity="INFO"):
        self.min_alert_severity = min_alert_severity

    def load_telegram_config_row(self):
        return {
            "alerts_enabled": True,
            "alerts_chat_id": "9988",
            "alerts_chat_name": "ops",
            "min_alert_severity": self.min_alert_severity,
            "alert_cooldown_sec": 0,
            "alert_dedup_enabled": False,
            "alert_format_rich": True,
        }


class _Sender:
    def __init__(self):
        self.messages = []

    def send_alert_message(self, chat_id, text):
        self.messages.append((chat_id, text))


@pytest.mark.smoke
def test_incident_lifecycle_is_rendered_when_present():
    sender = _Sender()
    svc = TelegramAlertsService(settings_service=_Settings(), telegram_sender=sender)

    result = svc.notify_alert({"severity": "high", "code": "INC-1", "message": "disk pressure", "lifecycle": "updated"})

    assert result["delivered"] is True
    assert len(sender.messages) == 1
    assert "Severity: HIGH" in sender.messages[0][1]
    assert "Lifecycle: UPDATED" in sender.messages[0][1]


@pytest.mark.smoke
def test_incident_lifecycle_falls_back_to_legacy_when_missing():
    sender = _Sender()
    svc = TelegramAlertsService(settings_service=_Settings(), telegram_sender=sender)

    result = svc.notify_alert({"severity": "warning", "code": "INC-2", "message": "degraded path"})

    assert result["delivered"] is True
    assert len(sender.messages) == 1
    assert "Lifecycle:" not in sender.messages[0][1]


@pytest.mark.smoke
def test_incident_lifecycle_ignores_unsupported_state_for_compatibility():
    sender = _Sender()
    svc = TelegramAlertsService(settings_service=_Settings(), telegram_sender=sender)

    result = svc.notify_alert({"severity": "high", "code": "INC-3", "message": "anomaly", "lifecycle": "foo"})

    assert result["delivered"] is True
    assert len(sender.messages) == 1
    assert "Lifecycle:" not in sender.messages[0][1]


@pytest.mark.smoke
def test_incident_severity_high_respects_thresholds_non_breaking():
    sender = _Sender()
    svc = TelegramAlertsService(settings_service=_Settings(min_alert_severity="CRITICAL"), telegram_sender=sender)

    high = svc.notify_alert({"severity": "high", "code": "INC-4", "message": "high incident"})
    critical = svc.notify_alert({"severity": "critical", "code": "INC-5", "message": "critical incident", "lifecycle": "resolved"})

    assert high["delivered"] is False
    assert high["reason"] == "below_min_severity"
    assert critical["delivered"] is True
    assert len(sender.messages) == 1
    assert "Lifecycle: RESOLVED" in sender.messages[0][1]
