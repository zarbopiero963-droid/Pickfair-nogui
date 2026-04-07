import pytest

from services.telegram_alerts_service import TelegramAlertsService


class SettingsStub:
    def load_telegram_config_row(self):
        return {
            "alerts_enabled": True,
            "alerts_chat_id": "12345",
            "alerts_chat_name": "ops",
            "min_alert_severity": "WARNING",
            "alert_cooldown_sec": 0,
            "alert_dedup_enabled": True,
            "alert_format_rich": True,
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
            "alert_cooldown_sec": 0,
            "alert_dedup_enabled": True,
            "alert_format_rich": True,
        }


class StrictSeveritySettingsStub(SettingsStub):
    def load_telegram_config_row(self):
        data = super().load_telegram_config_row()
        data["min_alert_severity"] = "CRITICAL"
        return data


class CooldownSettingsStub(SettingsStub):
    def load_telegram_config_row(self):
        data = super().load_telegram_config_row()
        data["alert_cooldown_sec"] = 600
        data["alert_dedup_enabled"] = True
        return data


@pytest.mark.smoke
def test_telegram_alert_pipeline_calls_sender_when_enabled():
    sender = SenderStub()
    svc = TelegramAlertsService(settings_service=SettingsStub(), telegram_sender=sender)

    result = svc.notify_alert({"severity": "critical", "code": "X1", "message": "boom"})

    assert result["delivered"] is True
    assert len(sender.calls) == 1
    assert sender.calls[0][0] == "12345"


@pytest.mark.smoke
def test_telegram_alert_pipeline_disabled_path_is_noop():
    sender = SenderStub()
    svc = TelegramAlertsService(settings_service=DisabledSettingsStub(), telegram_sender=sender)

    result = svc.notify_alert({"severity": "critical", "code": "X1", "message": "boom"})

    assert result["delivered"] is False
    assert sender.calls == []


@pytest.mark.smoke
def test_telegram_alert_pipeline_applies_min_severity_threshold():
    sender = SenderStub()
    svc = TelegramAlertsService(settings_service=StrictSeveritySettingsStub(), telegram_sender=sender)

    warn = svc.notify_alert({"severity": "warning", "code": "X1", "message": "warn"})
    crash = svc.notify_alert({"severity": "critical", "code": "X2", "message": "crash"})

    assert warn["delivered"] is False
    assert crash["delivered"] is True
    assert len(sender.calls) == 1
    assert "Code: X2" in sender.calls[0][1]


@pytest.mark.smoke
def test_telegram_alert_pipeline_dedup_and_cooldown():
    sender = SenderStub()
    svc = TelegramAlertsService(settings_service=CooldownSettingsStub(), telegram_sender=sender)

    payload = {"severity": "critical", "code": "X1", "message": "boom", "details": {"b": 1, "a": 2}}
    first = svc.notify_alert(payload)
    second = svc.notify_alert(payload)

    assert first["delivered"] is True
    assert second["reason"] == "dedup_cooldown"
    assert len(sender.calls) == 1
    assert "Details: a=2, b=1" in sender.calls[0][1]


@pytest.mark.smoke
def test_telegram_alert_pipeline_enabled_but_missing_sender_is_truthful_degraded():
    svc = TelegramAlertsService(settings_service=SettingsStub(), telegram_sender=None)

    result = svc.notify_alert({"severity": "critical", "code": "X3", "message": "lost"})

    assert result["delivered"] is False
    assert result["reason"] == "sender_unavailable"
    assert result["sender_available"] is False
