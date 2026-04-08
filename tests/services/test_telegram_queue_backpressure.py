import pytest

from services.telegram_alerts_service import TelegramAlertsService
from telegram_sender import TelegramSender


class _Settings:
    def __init__(self):
        self.cfg = {
            "alerts_enabled": True,
            "alerts_chat_id": "100",
            "alerts_chat_name": "ops",
            "min_alert_severity": "INFO",
            "alert_cooldown_sec": 0,
            "alert_dedup_enabled": False,
            "alert_format_rich": True,
            "alert_aggregation_enabled": True,
            "alert_aggregation_threshold": 3,
            "alert_aggregation_window_sec": 60,
        }

    def load_telegram_config_row(self):
        return dict(self.cfg)


class _SenderRecorder:
    def __init__(self):
        self.calls = []

    def send_alert_message(self, chat_id, text):
        self.calls.append((chat_id, text))


@pytest.mark.smoke

def test_bounded_queue_does_not_grow_unbounded(monkeypatch):
    sender = TelegramSender(client=None, queue_maxsize=2)
    monkeypatch.setattr(sender, "start_worker", lambda: None)

    sender.queue_message("1", "m1")
    sender.queue_message("1", "m2")
    sender.queue_message("1", "m3")

    assert sender.get_queue_size() == 2
    stats = sender.get_stats()
    assert stats["queue_maxsize"] == 2


@pytest.mark.smoke

def test_overflow_increments_counters_and_sets_backpressure(monkeypatch):
    sender = TelegramSender(client=None, queue_maxsize=1)
    monkeypatch.setattr(sender, "start_worker", lambda: None)

    queued = sender.queue_message("1", "first")
    dropped = sender.queue_message("1", "second")

    stats = sender.get_stats()
    assert queued is True
    assert dropped is False
    assert stats["messages_dropped"] == 1
    assert stats["queue_backpressure"] is True


@pytest.mark.smoke

def test_aggregation_triggers_under_burst():
    settings = _Settings()
    sender = _SenderRecorder()
    svc = TelegramAlertsService(settings_service=settings, telegram_sender=sender)

    for idx in range(3):
        svc.notify_alert({"severity": "warning", "code": "QBURST", "message": f"m{idx}"})

    assert len(sender.calls) == 3
    assert "Alert Burst Detected" in sender.calls[-1][1]
    assert "threshold=3" in sender.calls[-1][1]


@pytest.mark.smoke

def test_sender_does_not_crash_on_burst_enqueue(monkeypatch):
    sender = TelegramSender(client=None, queue_maxsize=5)
    monkeypatch.setattr(sender, "start_worker", lambda: None)

    dropped = 0
    for i in range(100):
        if not sender.queue_message("1", f"msg-{i}"):
            dropped += 1

    stats = sender.get_stats()
    assert sender.get_queue_size() == 5
    assert dropped == stats["messages_dropped"]
    assert dropped > 0
