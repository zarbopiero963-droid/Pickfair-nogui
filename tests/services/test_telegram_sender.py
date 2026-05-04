import threading
import time

import pytest

from telegram_sender import QueuedMessage, TelegramSender


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




def test_start_worker_repeated_calls_are_idempotent(monkeypatch):
    sender = TelegramSender(client=None)

    starts = {"count": 0}
    real_thread = threading.Thread

    def _counting_thread(*args, **kwargs):
        if kwargs.get("name") == "TelegramSenderWorker":
            starts["count"] += 1
        return real_thread(*args, **kwargs)

    monkeypatch.setattr("telegram_sender.threading.Thread", _counting_thread)

    sender.start_worker()
    sender.start_worker()

    try:
        assert starts["count"] == 1
        assert sender.get_stats()["worker_running"] is True
    finally:
        sender.stop_worker()


def test_start_worker_is_race_safe_under_concurrent_calls(monkeypatch):
    sender = TelegramSender(client=None)

    starts = {"count": 0}
    real_thread = threading.Thread

    def _counting_thread(*args, **kwargs):
        if kwargs.get("name") == "TelegramSenderWorker":
            starts["count"] += 1
        return real_thread(*args, **kwargs)

    monkeypatch.setattr("telegram_sender.threading.Thread", _counting_thread)

    workers = [real_thread(target=sender.start_worker) for _ in range(10)]
    for w in workers:
        w.start()
    for w in workers:
        w.join()

    try:
        assert starts["count"] == 1
        assert sender.get_stats()["worker_running"] is True
    finally:
        sender.stop_worker()


def test_worker_loop_closes_event_loop_on_send_exception(monkeypatch):
    sender = TelegramSender(client=None)

    class _FakeLoop:
        def __init__(self):
            self.closed = False
            self.shutdown_called = False
            self.calls = 0

        def run_until_complete(self, _coro):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("boom")
            self.shutdown_called = True
            return None

        async def shutdown_asyncgens(self):
            self.shutdown_called = True

        def close(self):
            self.closed = True

    fake_loop = _FakeLoop()

    monkeypatch.setattr("telegram_sender.asyncio.new_event_loop", lambda: fake_loop)
    monkeypatch.setattr("telegram_sender.asyncio.set_event_loop", lambda _loop: None)

    sender._queue.put(QueuedMessage(chat_id="1", text="x", max_retries=1, callback=None, message_type="GENERIC"))
    sender._running = True

    original_get = sender._queue.get

    def _one_then_stop(*args, **kwargs):
        item = original_get(*args, **kwargs)
        sender._running = False
        return item

    monkeypatch.setattr(sender._queue, "get", _one_then_stop)

    sender._worker_loop()

    assert fake_loop.closed is True


def test_send_message_sync_closes_event_loop_on_exception(monkeypatch):
    sender = TelegramSender(client=None)

    class _FakeLoop:
        def __init__(self):
            self.closed = False

        def run_until_complete(self, _coro):
            raise RuntimeError("sync boom")

        def close(self):
            self.closed = True

    fake_loop = _FakeLoop()

    monkeypatch.setattr("telegram_sender.asyncio.new_event_loop", lambda: fake_loop)

    with pytest.raises(RuntimeError, match="sync boom"):
        sender.send_message_sync("1", "x", max_retries=1)

    assert fake_loop.closed is True



def test_stop_worker_keeps_reference_when_join_times_out_with_alive_worker(monkeypatch):
    sender = TelegramSender(client=None)

    class _FakeThread:
        def __init__(self):
            self.join_calls = []
            self._alive = True

        def join(self, timeout=None):
            self.join_calls.append(timeout)

        def is_alive(self):
            return self._alive

    fake_thread = _FakeThread()
    sender._worker_thread = fake_thread
    sender._running = True

    sender.stop_worker()

    assert sender._worker_thread is fake_thread
    assert fake_thread.join_calls == [5]
    assert sender.get_stats()["worker_running"] is False


def test_stop_worker_clears_reference_after_stopped_joined_worker(monkeypatch):
    sender = TelegramSender(client=None)

    class _FakeThread:
        def __init__(self):
            self._alive = True

        def join(self, timeout=None):
            self._alive = False

        def is_alive(self):
            return self._alive

    fake_thread = _FakeThread()
    sender._worker_thread = fake_thread
    sender._running = True

    sender.stop_worker()

    assert sender._worker_thread is None


def test_start_worker_does_not_create_second_worker_if_existing_alive(monkeypatch):
    sender = TelegramSender(client=None)

    class _AliveThread:
        def is_alive(self):
            return True

    sender._worker_thread = _AliveThread()
    sender._running = False

    start_calls = {"count": 0}

    def _unexpected_thread(*args, **kwargs):
        start_calls["count"] += 1
        raise AssertionError("must not create a second worker when one is alive")

    monkeypatch.setattr("telegram_sender.threading.Thread", _unexpected_thread)

    sender.start_worker()

    assert start_calls["count"] == 0


def test_start_worker_logs_only_on_real_new_start(monkeypatch):
    sender = TelegramSender(client=None)

    logs = []
    monkeypatch.setattr("telegram_sender.logger.info", lambda msg: logs.append(msg))

    class _FakeThread:
        def __init__(self, *args, **kwargs):
            self._alive = False

        def start(self):
            self._alive = True

        def join(self, timeout=None):
            self._alive = False

        def is_alive(self):
            return self._alive

    monkeypatch.setattr("telegram_sender.threading.Thread", _FakeThread)

    sender.start_worker()
    sender.start_worker()

    assert logs.count("[TG_SENDER] Worker started") == 1
    sender.stop_worker()


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


def test_rate_limiter_uses_monotonic_not_event_loop_time(monkeypatch):
    sender = TelegramSender(client=None, base_delay=0.01)

    class _BadLoop:
        def time(self):
            raise AssertionError("event loop time should not be used for rate limiting")

    async def _no_sleep(*_args, **_kwargs):
        return None

    monkeypatch.setattr("telegram_sender.asyncio.get_event_loop", lambda: _BadLoop())
    monkeypatch.setattr("telegram_sender.asyncio.sleep", _no_sleep)

    sender.rate_limiter.last_send_time = 0.0
    sender.send_message_sync("123", "hello", max_retries=1)

    assert sender.rate_limiter.last_send_time > 0


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
