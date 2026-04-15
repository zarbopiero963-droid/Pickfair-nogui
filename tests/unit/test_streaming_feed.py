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


def test_streaming_feed_partial_runner_updates_merge_without_wiping_state():
    updates = []
    feed = StreamingFeed(
        client_getter=lambda: None,
        config={"enabled": False, "market_data_mode": "poll"},
        on_market_book=lambda book: updates.append(book),
    )

    feed._process_message(
        {
            "mc": [
                {
                    "id": "1.300",
                    "rc": [
                        {
                            "id": 10,
                            "ltp": 2.0,
                            "batb": [[0, 1.99, 10.0]],
                            "batl": [[0, 2.02, 11.0]],
                        }
                    ],
                }
            ]
        }
    )
    feed._process_message({"mc": [{"id": "1.300", "rc": [{"id": 10, "ltp": 2.1}]}]})
    feed._process_message({"mc": [{"id": "1.300", "rc": [{"id": 20, "batb": [[0, 3.1, 7.0]]}]}]})

    final = updates[-1]
    r10 = next(r for r in final["runners"] if r["selectionId"] == 10)
    r20 = next(r for r in final["runners"] if r["selectionId"] == 20)

    assert r10["ltp"] == 2.1
    assert r10["ex"]["availableToBack"] == [{"price": 1.99, "size": 10.0}]
    assert r10["ex"]["availableToLay"] == [{"price": 2.02, "size": 11.0}]
    assert r20["ex"]["availableToBack"] == [{"price": 3.1, "size": 7.0}]


def test_streaming_feed_market_definition_and_runner_metadata_merge_incrementally():
    updates = []
    feed = StreamingFeed(
        client_getter=lambda: None,
        config={"enabled": False, "market_data_mode": "poll"},
        on_market_book=lambda book: updates.append(book),
    )

    feed._process_message(
        {
            "mc": [
                {
                    "id": "1.400",
                    "marketDefinition": {
                        "status": "OPEN",
                        "inPlay": False,
                        "runners": [{"id": 11, "name": "Home", "status": "ACTIVE"}],
                    },
                }
            ]
        }
    )
    feed._process_message(
        {
            "mc": [
                {
                    "id": "1.400",
                    "rc": [{"id": 11, "ltp": 1.8, "batb": [[0, 1.79, 20.0]]}],
                }
            ]
        }
    )
    feed._process_message(
        {
            "mc": [
                {
                    "id": "1.400",
                    "marketDefinition": {
                        "status": "SUSPENDED",
                        "inPlay": True,
                        "runners": [{"id": 11, "name": "Home", "status": "REMOVED"}],
                    },
                }
            ]
        }
    )

    final = updates[-1]
    runner = final["runners"][0]
    assert final["status"] == "SUSPENDED"
    assert final["inplay"] is True
    assert runner["runnerName"] == "Home"
    assert runner["status"] == "REMOVED"
    assert runner["ex"]["availableToBack"] == [{"price": 1.79, "size": 20.0}]


def test_streaming_feed_high_churn_updates_keep_runner_state_coherent():
    updates = []
    feed = StreamingFeed(
        client_getter=lambda: None,
        config={"enabled": False, "market_data_mode": "poll"},
        on_market_book=lambda book: updates.append(book),
    )

    for idx in range(30):
        feed._process_message(
            {
                "mc": [
                    {
                        "id": "1.500",
                        "rc": [
                            {"id": 101, "ltp": 2.0 + (idx * 0.01)},
                            {"id": 102, "batl": [[0, 3.0 + (idx * 0.01), 5.0 + idx]]},
                        ],
                    }
                ]
            }
        )
        if idx % 5 == 0:
            feed._process_message(
                {
                    "mc": [
                        {
                            "id": "1.500",
                            "marketDefinition": {
                                "status": "OPEN" if idx < 20 else "SUSPENDED",
                                "inPlay": idx >= 10,
                                "runners": [
                                    {"id": 101, "name": "Runner 101", "status": "ACTIVE"},
                                    {"id": 102, "name": "Runner 102", "status": "ACTIVE"},
                                ],
                            },
                        }
                    ]
                }
            )

    final = updates[-1]
    runners = {r["selectionId"]: r for r in final["runners"]}
    assert set(runners.keys()) == {101, 102}
    assert runners[101]["ltp"] == pytest.approx(2.29)
    assert runners[102]["ex"]["availableToLay"][-1]["price"] == pytest.approx(3.29)
    assert runners[102]["runnerName"] == "Runner 102"
    assert final["status"] == "SUSPENDED"
    assert final["inplay"] is True
