import pytest

from services.telegram_alerts_service import TelegramAlertsService


class SettingsStub:
    def load_telegram_config_row(self):
        return {
            "alerts_enabled": True,
            "alerts_chat_id": "12345",
            "alerts_chat_name": "ops",
            "min_alert_severity": "WARNING",
        }


class SenderStub:
    def __init__(self):
        self.calls = []

    def send_alert_message(self, chat_id, text):
        self.calls.append((chat_id, text))


class DisabledSettingsStub:
    def load_telegram_config_row(self):
        return {
            "alerts_enabled": False,
            "alerts_chat_id": "12345",
            "alerts_chat_name": "ops",
            "min_alert_severity": "WARNING",
        }


@pytest.mark.smoke
def test_telegram_alert_pipeline_calls_sender_when_enabled():
    sender = SenderStub()
    svc = TelegramAlertsService(settings_service=SettingsStub(), telegram_sender=sender)

    svc.notify_alert({"severity": "critical", "code": "X1", "message": "boom"})

    assert len(sender.calls) == 1
    assert sender.calls[0][0] == "12345"


@pytest.mark.smoke
def test_telegram_alert_pipeline_disabled_path_is_noop():
    sender = SenderStub()
    svc = TelegramAlertsService(settings_service=DisabledSettingsStub(), telegram_sender=sender)

    svc.notify_alert({"severity": "critical", "code": "X1", "message": "boom"})

    assert sender.calls == []
