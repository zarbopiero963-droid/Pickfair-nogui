import time

import pytest

from telegram_sender import TelegramSender


class _Msg:
    def __init__(self, msg_id):
        self.id = msg_id


class _ClientOk:
    def __init__(self):
        self.entity_calls = []
        self.send_calls = []

    async def get_entity(self, chat_id):
        self.entity_calls.append(chat_id)
        return {"entity": chat_id}

    async def send_message(self, entity, text, **kwargs):
        self.send_calls.append((entity, text, kwargs))
        return _Msg(321)


class _ClientFail:
    async def get_entity(self, chat_id):
        return {"entity": chat_id}

    async def send_message(self, entity, text, **kwargs):
        raise RuntimeError("boom")


class _ClientFlakyThenOk:
    def __init__(self):
        self.send_attempts = 0

    async def get_entity(self, chat_id):
        return {"entity": chat_id}

    async def send_message(self, entity, text, **kwargs):
        self.send_attempts += 1
        if self.send_attempts == 1:
            raise RuntimeError("temporary send failure")
        return _Msg(777)


class _ClientGetEntityFail:
    def __init__(self):
        self.send_calls = 0

    async def get_entity(self, chat_id):
        raise RuntimeError("entity lookup failed")

    async def send_message(self, entity, text, **kwargs):
        self.send_calls += 1
        return _Msg(999)


def test_send_message_success_sends_once_and_returns_success():
    client = _ClientOk()
    sender = TelegramSender(client=client)

    result = sender.send_message_sync("123", "hello", max_retries=1, message_type="ALERT")

    assert result.success is True
    assert result.message_id == 321
    assert result.error is None
    assert client.entity_calls == [123]
    assert client.send_calls == [({"entity": 123}, "hello", {"parse_mode": "MarkdownV2"})]
    assert sender.get_stats()["messages_sent"] == 1


def test_send_message_failure_reports_failed_and_no_false_success():
    sender = TelegramSender(client=_ClientFail())

    result = sender.send_message_sync("123", "hello", max_retries=1)

    assert result.success is False
    assert "boom" in (result.error or "")
    assert sender.get_stats()["messages_failed"] == 1


def test_send_message_retries_then_succeeds(monkeypatch):
    client = _ClientFlakyThenOk()
    sender = TelegramSender(client=client)
    
    async def _no_sleep(*_args, **_kwargs):
        return None

    monkeypatch.setattr("telegram_sender.asyncio.sleep", _no_sleep)

    result = sender.send_message_sync("123", "retry-ok", max_retries=3, message_type="CUSTOM")

    assert result.success is True
    assert result.message_id == 777
    assert client.send_attempts == 2
    stats = sender.get_stats()
    assert stats["messages_sent"] == 1
    assert stats["messages_failed"] == 0


def test_send_message_get_entity_failure_is_reported_and_send_not_called(monkeypatch):
    client = _ClientGetEntityFail()
    sender = TelegramSender(client=client)
    
    async def _no_sleep(*_args, **_kwargs):
        return None

    monkeypatch.setattr("telegram_sender.asyncio.sleep", _no_sleep)

    result = sender.send_message_sync("123", "won't-send", max_retries=1)

    assert result.success is False
    assert "entity lookup failed" in (result.error or "")
    assert client.send_calls == 0
    assert sender.get_stats()["messages_failed"] == 1


def test_queue_start_worker_is_idempotent_and_not_duplicated(monkeypatch):
    sender = TelegramSender(client=None, queue_maxsize=3)
    calls = {"count": 0}

    real_start_worker = sender.start_worker

    def _counting_start_worker():
        calls["count"] += 1
        return real_start_worker()

    monkeypatch.setattr(sender, "start_worker", _counting_start_worker)

    try:
        assert sender.queue_message("1", "m1") is True
        assert sender.queue_message("1", "m2") is True
        assert calls["count"] == 1
    finally:
        sender.stop_worker()


def test_queue_backpressure_returns_false_and_tracks_drops(monkeypatch):
    sender = TelegramSender(client=None, queue_maxsize=1)
    monkeypatch.setattr(sender, "start_worker", lambda: None)

    assert sender.queue_message("1", "first") is True
    assert sender.queue_message("1", "second") is False

    stats = sender.get_stats()
    assert stats["messages_queued"] == 1
    assert stats["messages_dropped"] == 1
    assert stats["queue_backpressure"] is True


def test_stop_worker_repeated_calls_are_safe_and_leave_stopped():
    sender = TelegramSender(client=None)

    sender.start_worker()
    sender.stop_worker()
    sender.stop_worker()

    assert sender.get_stats()["worker_running"] is False


def test_worker_drains_queued_message_before_stop():
    client = _ClientOk()
    sender = TelegramSender(client=client, queue_maxsize=2, base_delay=0.0)

    try:
        assert sender.queue_message("99", "drain-me") is True

        deadline = time.monotonic() + 1.0
        while len(client.send_calls) < 1 and sender.get_queue_size() > 0:
            if time.monotonic() >= deadline:
                pytest.fail("Timed out waiting for sender worker to drain queued message")
            time.sleep(0.01)

        assert sender.get_queue_size() == 0
        assert len(client.send_calls) == 1
    finally:
        sender.stop_worker()


def test_sender_instances_have_isolated_state(monkeypatch):
    a = TelegramSender(client=None, queue_maxsize=1)
    b = TelegramSender(client=None, queue_maxsize=1)
    monkeypatch.setattr(a, "start_worker", lambda: None)
    monkeypatch.setattr(b, "start_worker", lambda: None)

    assert a.queue_message("1", "a1") is True
    assert a.queue_message("1", "a2") is False
    assert b.queue_message("2", "b1") is True

    assert a.get_stats()["messages_dropped"] == 1
    assert b.get_stats()["messages_dropped"] == 0
    assert a.get_queue_size() == 1
    assert b.get_queue_size() == 1


def test_send_message_escapes_markdown_v2_special_characters():
    client = _ClientOk()
    sender = TelegramSender(client=client)

    raw = r"_*[]()~`>#+-=|{}.!"
    result = sender.send_message_sync("123", raw, max_retries=1)

    assert result.success is True
    assert len(client.send_calls) == 1
    sent_entity, sent_text, sent_kwargs = client.send_calls[0]
    assert sent_entity == {"entity": 123}
    assert sent_text == r"\_\*\[\]\(\)\~\`\>\#\+\-\=\|\{\}\.\!"
    assert sent_kwargs == {"parse_mode": "MarkdownV2"}


def test_queue_full_behavior_is_deterministic_with_explicit_db_log(monkeypatch):
    sender = TelegramSender(client=None, queue_maxsize=1)
    monkeypatch.setattr(sender, "start_worker", lambda: None)

    events = []

    def _capture(**kwargs):
        events.append(kwargs)

    monkeypatch.setattr(sender, "_db_log", _capture)

    assert sender.queue_message("1", "first", message_type="ALERT") is True
    assert sender.queue_message("1", "second", message_type="ALERT") is False

    assert events[0]["status"] == "QUEUED"
    assert events[0]["error"] is None
    assert events[1]["status"] == "DROPPED_QUEUE_FULL"
    assert events[1]["error"] == "queue_full"
    stats = sender.get_stats()
    assert stats["messages_dropped"] == 1
    assert stats["queue_backpressure"] is True
