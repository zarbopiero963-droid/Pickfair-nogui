import asyncio

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

    async def send_message(self, entity, text):
        self.send_calls.append((entity, text))
        return _Msg(321)


class _ClientFail:
    async def get_entity(self, chat_id):
        return {"entity": chat_id}

    async def send_message(self, entity, text):
        raise RuntimeError("boom")


def test_send_message_success_sends_once_and_returns_success():
    client = _ClientOk()
    sender = TelegramSender(client=client)

    result = sender.send_message_sync("123", "hello", max_retries=1, message_type="ALERT")

    assert result.success is True
    assert result.message_id == 321
    assert result.error is None
    assert client.entity_calls == [123]
    assert client.send_calls == [({"entity": 123}, "hello")]
    assert sender.get_stats()["messages_sent"] == 1


def test_send_message_failure_reports_failed_and_no_false_success():
    sender = TelegramSender(client=_ClientFail())

    result = sender.send_message_sync("123", "hello", max_retries=1)

    assert result.success is False
    assert "boom" in (result.error or "")
    assert sender.get_stats()["messages_failed"] == 1


def test_queue_start_worker_is_idempotent_and_not_duplicated(monkeypatch):
    sender = TelegramSender(client=None, queue_maxsize=3)
    calls = {"count": 0}

    def _fake_start_worker():
        calls["count"] += 1
        sender._running = True

    monkeypatch.setattr(sender, "start_worker", _fake_start_worker)

    assert sender.queue_message("1", "m1") is True
    assert sender.queue_message("1", "m2") is True

    assert calls["count"] == 1


def test_queue_backpressure_returns_false_and_tracks_drops(monkeypatch):
    sender = TelegramSender(client=None, queue_maxsize=1)
    monkeypatch.setattr(sender, "start_worker", lambda: None)

    assert sender.queue_message("1", "first") is True
    assert sender.queue_message("1", "second") is False

    stats = sender.get_stats()
    assert stats["messages_queued"] == 1
    assert stats["messages_dropped"] == 1
    assert stats["queue_backpressure"] is True


def test_stop_worker_repeated_calls_are_safe_and_leave_stopped(monkeypatch):
    sender = TelegramSender(client=None)

    class _NoopThread:
        def join(self, timeout=None):
            return None

    sender._worker_thread = _NoopThread()
    sender._running = True

    sender.stop_worker()
    sender.stop_worker()

    assert sender.get_stats()["worker_running"] is False


def test_worker_drains_queued_message_before_stop():
    client = _ClientOk()
    sender = TelegramSender(client=client, queue_maxsize=2)

    async def _no_wait():
        return None

    sender.rate_limiter.wait_if_needed_async = _no_wait

    assert sender.queue_message("99", "drain-me") is True

    deadline = asyncio.get_event_loop().time() + 2.0
    while sender.get_queue_size() > 0 and asyncio.get_event_loop().time() < deadline:
        pass

    sender.stop_worker()

    assert sender.get_queue_size() == 0
    assert len(client.send_calls) == 1


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
