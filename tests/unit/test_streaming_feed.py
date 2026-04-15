import time
from queue import Queue

import pytest

from services.streaming_feed import StreamingConfigError, StreamingFeed


class _Listener:
    def __init__(self, *, output_queue):
        self.output_queue = output_queue


class _Stream:
    def __init__(self, output_queue: Queue, scripted_messages):
        self.output_queue = output_queue
        self.scripted_messages = list(scripted_messages)
        self.subscriptions = []
        self.stopped = False

    def subscribe_to_markets(self, **kwargs):
        self.subscriptions.append(dict(kwargs))
        for msg in self.scripted_messages:
            self.output_queue.put(msg)

    def stop(self):
        self.stopped = True


class _StreamingAPI:
    def __init__(self, scripts, subscriptions):
        self._scripts = list(scripts)
        self._subscriptions = subscriptions

    def create_stream(self, listener):
        script = self._scripts.pop(0) if self._scripts else []
        stream = _Stream(listener.output_queue, script)
        self._subscriptions.append(stream.subscriptions)
        return stream


class _Client:
    def __init__(self, scripts, subscriptions):
        self.streaming = _StreamingAPI(scripts=scripts, subscriptions=subscriptions)
        self.keep_alive_calls = 0

    def keep_alive(self):
        self.keep_alive_calls += 1


def test_streaming_feed_rejects_unbounded_subscription():
    feed = StreamingFeed(
        client_getter=lambda: None,
        config={
            "enabled": True,
            "market_data_mode": "stream",
            "market_ids": [],
            "event_type_ids": [],
            "country_codes": [],
            "market_types": [],
        },
        on_market_book=lambda _book: None,
    )

    with pytest.raises(StreamingConfigError, match="UNBOUNDED"):
        feed.start()


@pytest.mark.integration
def test_streaming_feed_reconnect_reuses_clk_and_initialclk():
    subscriptions = []
    disconnects = []
    updates = []

    # 1st connection emits clk/initialClk and a market update, then heartbeat timeout triggers reconnect
    scripts = [
        [{"initialClk": "ic_1", "clk": "c_1", "mc": [{"id": "1.100", "rc": []}]}],
        [{"mc": [{"id": "1.100", "rc": []}]}],
    ]
    client = _Client(scripts=scripts, subscriptions=subscriptions)

    feed = StreamingFeed(
        client_getter=lambda: client,
        config={
            "enabled": True,
            "market_data_mode": "stream",
            "market_ids": ["1.100"],
            "heartbeat_timeout_sec": 1,
            "reconnect_backoff_sec": 1,
            "fields": ["EX_BEST_OFFERS", "EX_MARKET_DEF", "EX_LTP"],
        },
        on_market_book=lambda book: updates.append(book),
        on_disconnect=lambda payload: disconnects.append(payload),
        listener_factory=_Listener,
    )

    feed.start()
    deadline = time.time() + 4.0
    while time.time() < deadline:
        if len(subscriptions) >= 2:
            break
        time.sleep(0.05)
    feed.stop()

    assert len(updates) >= 1
    assert len(disconnects) >= 1
    assert len(subscriptions) >= 2
    second_sub = subscriptions[1][0]
    assert second_sub.get("initial_clk") == "ic_1"
    assert second_sub.get("clk") == "c_1"


@pytest.mark.integration
def test_streaming_feed_503_degraded_does_not_disconnect():
    subscriptions = []
    disconnects = []

    scripts = [[{"status": "503"}, {"mc": [{"id": "1.200", "rc": []}]}]]
    client = _Client(scripts=scripts, subscriptions=subscriptions)

    feed = StreamingFeed(
        client_getter=lambda: client,
        config={
            "enabled": True,
            "market_data_mode": "stream",
            "market_ids": ["1.200"],
            "heartbeat_timeout_sec": 10,
        },
        on_market_book=lambda _book: None,
        on_disconnect=lambda payload: disconnects.append(payload),
        listener_factory=_Listener,
    )

    feed.start()
    time.sleep(0.2)
    status = feed.status()
    feed.stop()

    assert status["degraded_503"] is True
    assert disconnects == []
