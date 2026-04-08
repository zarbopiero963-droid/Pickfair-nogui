from services.telegram_alerts_service import TelegramAlertsService


class _Settings:
    def __init__(self, *, cooldown_sec: int = 300, dedup_enabled: bool = True):
        self.cooldown_sec = cooldown_sec
        self.dedup_enabled = dedup_enabled

    def load_telegram_config_row(self):
        return {
            "alerts_enabled": True,
            "alerts_chat_id": "9988",
            "alerts_chat_name": "ops",
            "min_alert_severity": "INFO",
            "alert_cooldown_sec": self.cooldown_sec,
            "alert_dedup_enabled": self.dedup_enabled,
            "alert_format_rich": True,
        }


class _Sender:
    def __init__(self):
        self.messages = []

    def send_message(self, chat_id, text):
        self.messages.append((chat_id, text))


class _FailingSender:
    def send_message(self, _chat_id, _text):
        raise RuntimeError("telegram down")


def test_anomaly_alert_payload_is_formatted_and_delivered():
    sender = _Sender()
    svc = TelegramAlertsService(settings_service=_Settings(cooldown_sec=0), telegram_sender=sender)

    result = svc.notify_anomaly_alert(
        {
            "code": "FINANCIAL_DRIFT",
            "severity": "critical",
            "source": "watchdog_service",
            "description": "balance drift detected",
            "details": {"expected": 100.0, "actual": 97.1},
        }
    )

    assert result["delivered"] is True
    assert len(sender.messages) == 1
    text = sender.messages[0][1]
    assert "Code: FINANCIAL_DRIFT" in text
    assert "Category: ANOMALY" in text
    assert "Details: actual=97.1, expected=100.0" in text


def test_duplicate_anomaly_within_cooldown_is_deduped():
    sender = _Sender()
    svc = TelegramAlertsService(settings_service=_Settings(cooldown_sec=600), telegram_sender=sender)
    payload = {
        "code": "GHOST_ORDER",
        "severity": "warning",
        "source": "watchdog_service",
        "description": "ghost order found",
        "details": {"order_id": "A1"},
    }

    first = svc.notify_anomaly_alert(payload)
    second = svc.notify_anomaly_alert(payload)

    assert first["delivered"] is True
    assert second["delivered"] is False
    assert second["reason"] == "dedup_cooldown"
    assert len(sender.messages) == 1


def test_anomaly_delivery_failure_returns_non_crashing_result():
    svc = TelegramAlertsService(settings_service=_Settings(cooldown_sec=0), telegram_sender=_FailingSender())

    result = svc.notify_anomaly_alert(
        {
            "code": "DB_CONTENTION",
            "severity": "error",
            "description": "db lock contention",
        }
    )

    assert result["delivered"] is False
    assert result["reason"] == "send_failed"
