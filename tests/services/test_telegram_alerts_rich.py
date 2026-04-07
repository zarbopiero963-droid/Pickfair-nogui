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
