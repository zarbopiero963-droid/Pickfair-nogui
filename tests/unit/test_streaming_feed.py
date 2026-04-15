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
        self.started = False

    def subscribe_to_markets(self, **kwargs):
        self.subscriptions.append(dict(kwargs))
        for msg in self.scripted_messages:
            self.output_queue.put(msg)

    def stop(self):
        self.stopped = True

    def start(self):
        self.started = True


class _StreamingAPI:
    def __init__(self, scripts, subscriptions):
        self._scripts = list(scripts)
        self._subscriptions = subscriptions
        self.streams = []

    def create_stream(self, listener):
        script = self._scripts.pop(0) if self._scripts else []
        stream = _Stream(listener.output_queue, script)
        self._subscriptions.append(stream.subscriptions)
        self.streams.append(stream)
        return stream


class _Client:
    def __init__(self, scripts, subscriptions):
        self.streaming = _StreamingAPI(scripts=scripts, subscriptions=subscriptions)
        self.keep_alive_calls = 0

    def keep_alive(self):
        self.keep_alive_calls += 1


EDGE_FIXTURE_SPARSE_LADDER_SEQUENCE = [
    {"mc": [{"id": "1.810", "rc": [{"id": 31, "batb": [[0, 2.0, 10.0], [1, 1.99, 8.0]], "batl": [[0, 2.04, 12.0], [1, 2.06, 7.0]]}]}]},
    {"mc": [{"id": "1.810", "rc": [{"id": 31, "batb": [[0, 2.02, 9.0]]}]}]},
]

EDGE_FIXTURE_MARKET_DEF_TRANSITIONS = [
    {"mc": [{"id": "1.820", "marketDefinition": {"status": "OPEN", "inPlay": False, "runners": [{"id": 41, "name": "Runner A", "status": "ACTIVE"}, {"id": 42, "name": "Runner B", "status": "ACTIVE"}]}}]},
    {"mc": [{"id": "1.820", "rc": [{"id": 41, "ltp": 1.9, "batb": [[0, 1.88, 20.0]]}, {"id": 42, "ltp": 2.1, "batl": [[0, 2.12, 18.0]]}]}]},
    {"mc": [{"id": "1.820", "marketDefinition": {"status": "SUSPENDED", "inPlay": True, "runners": [{"id": 41, "name": "Runner A", "status": "REMOVED"}, {"id": 42, "name": "Runner B", "status": "ACTIVE"}]}}]},
]


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
        session_gate=lambda: {"ok": True},
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
    assert any(stream.started for stream in client.streaming.streams)


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
        session_gate=lambda: {"ok": True},
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


def test_streaming_feed_parses_market_book_resource_objects_from_default_listener():
    class _PS:
        def __init__(self, price, size):
            self.price = price
            self.size = size

    class _RunnerDef:
        def __init__(self, selection_id, name, status):
            self.selection_id = selection_id
            self.name = name
            self.status = status
            self.handicap = 0
            self.sort_priority = 1

    class _MD:
        status = "OPEN"
        in_play = True
        runners = [_RunnerDef(7, "Runner 7", "ACTIVE")]

    class _Ex:
        available_to_back = [_PS(2.02, 9.0)]
        available_to_lay = [_PS(2.06, 8.5)]
        traded_volume = [_PS(2.04, 100.0)]

    class _Runner:
        selection_id = 7
        status = "ACTIVE"
        handicap = 0
        last_price_traded = 2.04
        ex = _Ex()

    class _MarketBook:
        market_id = "1.700"
        status = "OPEN"
        inplay = True
        market_definition = _MD()
        runners = [_Runner()]

    updates = []
    feed = StreamingFeed(
        client_getter=lambda: None,
        config={"enabled": False, "market_data_mode": "poll"},
        on_market_book=lambda book: updates.append(book),
    )

    feed._process_message(_MarketBook())

    assert updates, "resource payload should produce downstream market book update"
    out = updates[-1]
    assert out["marketId"] == "1.700"
    assert out["status"] == "OPEN"
    assert out["inplay"] is True
    assert out["runners"][0]["selectionId"] == 7
    assert out["runners"][0]["ex"]["availableToBack"] == [{"price": 2.02, "size": 9.0}]


def test_streaming_feed_sparse_ladder_update_preserves_untouched_levels():
    updates = []
    feed = StreamingFeed(
        client_getter=lambda: None,
        config={"enabled": False, "market_data_mode": "poll"},
        on_market_book=lambda book: updates.append(book),
    )

    for payload in EDGE_FIXTURE_SPARSE_LADDER_SEQUENCE:
        feed._process_message(payload)

    final = updates[-1]
    runner = final["runners"][0]
    assert runner["selectionId"] == 31
    # level 0 changed; level 1 must remain from prior update (no wipe)
    assert runner["ex"]["availableToBack"] == [
        {"price": 2.02, "size": 9.0},
        {"price": 1.99, "size": 8.0},
    ]
    assert runner["ex"]["availableToLay"] == [
        {"price": 2.04, "size": 12.0},
        {"price": 2.06, "size": 7.0},
    ]


def test_streaming_feed_market_definition_transition_and_runner_status_regression_guard():
    updates = []
    feed = StreamingFeed(
        client_getter=lambda: None,
        config={"enabled": False, "market_data_mode": "poll"},
        on_market_book=lambda book: updates.append(book),
    )

    for payload in EDGE_FIXTURE_MARKET_DEF_TRANSITIONS:
        feed._process_message(payload)

    final = updates[-1]
    runners = {r["selectionId"]: r for r in final["runners"]}
    assert final["status"] == "SUSPENDED"
    assert final["inplay"] is True
    assert runners[41]["status"] == "REMOVED"
    assert runners[41]["runnerName"] == "Runner A"
    # price state from prior updates should remain coherent after metadata/status transition
    assert runners[41]["ex"]["availableToBack"] == [{"price": 1.88, "size": 20.0}]
    assert runners[42]["ex"]["availableToLay"] == [{"price": 2.12, "size": 18.0}]


def test_streaming_feed_mixed_repeated_partial_updates_converge_without_data_loss():
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
                    "id": "1.830",
                    "rc": [
                        {"id": 51, "batb": [[0, 1.9, 11.0], [1, 1.88, 7.0]], "batl": [[0, 1.95, 10.0]]},
                        {"id": 52, "batb": [[0, 2.4, 6.0]], "batl": [[0, 2.5, 5.0]]},
                    ],
                }
            ]
        }
    )
    for i in range(12):
        feed._process_message({"mc": [{"id": "1.830", "rc": [{"id": 51, "ltp": 1.91 + (i * 0.01)}]}]})
    feed._process_message({"mc": [{"id": "1.830", "rc": [{"id": 51, "trd": [[1.9, 100.0]]}]}]})

    final = updates[-1]
    runners = {r["selectionId"]: r for r in final["runners"]}
    assert runners[51]["ltp"] == pytest.approx(2.02)
    assert runners[51]["ex"]["availableToBack"] == [{"price": 1.9, "size": 11.0}, {"price": 1.88, "size": 7.0}]
    assert runners[51]["ex"]["tradedVolume"] == [{"price": 1.9, "size": 100.0}]
    # untouched runner 52 must remain present and intact despite repeated updates to runner 51 only
    assert runners[52]["ex"]["availableToBack"] == [{"price": 2.4, "size": 6.0}]
    assert runners[52]["ex"]["availableToLay"] == [{"price": 2.5, "size": 5.0}]


def test_streaming_feed_tv_then_trd_is_deterministic():
    updates = []
    feed = StreamingFeed(
        client_getter=lambda: None,
        config={"enabled": False, "market_data_mode": "poll"},
        on_market_book=lambda book: updates.append(book),
    )

    feed._process_message({"mc": [{"id": "1.840", "rc": [{"id": 77, "tv": [[2.0, 10.0]]}]}]})
    feed._process_message({"mc": [{"id": "1.840", "rc": [{"id": 77, "trd": [[2.2, 22.0]]}]}]})
    final = updates[-1]
    runner = next(r for r in final["runners"] if r["selectionId"] == 77)
    assert runner["ex"]["tradedVolume"] == [{"price": 2.2, "size": 22.0}]


def test_streaming_feed_trd_then_tv_is_deterministic():
    updates = []
    feed = StreamingFeed(
        client_getter=lambda: None,
        config={"enabled": False, "market_data_mode": "poll"},
        on_market_book=lambda book: updates.append(book),
    )

    feed._process_message({"mc": [{"id": "1.841", "rc": [{"id": 78, "trd": [[1.9, 19.0]]}]}]})
    feed._process_message({"mc": [{"id": "1.841", "rc": [{"id": 78, "tv": [[1.8, 18.0]]}]}]})
    final = updates[-1]
    runner = next(r for r in final["runners"] if r["selectionId"] == 78)
    assert runner["ex"]["tradedVolume"] == [{"price": 1.8, "size": 18.0}]


def test_streaming_feed_trd_precedence_when_tv_and_trd_arrive_together():
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
                    "id": "1.842",
                    "rc": [
                        {
                            "id": 79,
                            "tv": [[2.4, 24.0]],
                            "trd": [[2.6, 26.0]],
                        }
                    ],
                }
            ]
        }
    )
    final = updates[-1]
    runner = next(r for r in final["runners"] if r["selectionId"] == 79)
    assert runner["ex"]["tradedVolume"] == [{"price": 2.6, "size": 26.0}]


def test_streaming_feed_metadata_only_rc_update_preserves_price_state_and_ladders():
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
                    "id": "1.850",
                    "rc": [
                        {
                            "id": 88,
                            "ltp": 3.1,
                            "batb": [[0, 3.05, 12.0]],
                            "batl": [[0, 3.2, 9.0]],
                            "trd": [[3.1, 42.0]],
                        }
                    ],
                }
            ]
        }
    )
    feed._process_message(
        {
            "mc": [
                {
                    "id": "1.850",
                    "rc": [
                        {
                            "id": 88,
                            "status": "REMOVED",
                            "hc": 0.0,
                            "adjustmentFactor": 12.5,
                            "removalDate": "2026-03-01T10:00:00Z",
                        }
                    ],
                }
            ]
        }
    )

    final = updates[-1]
    runner = next(r for r in final["runners"] if r["selectionId"] == 88)
    assert runner["status"] == "REMOVED"
    assert runner["handicap"] == 0.0
    assert runner["adjustmentFactor"] == 12.5
    assert runner["removalDate"] == "2026-03-01T10:00:00Z"
    assert runner["ltp"] == pytest.approx(3.1)
    assert runner["ex"]["availableToBack"] == [{"price": 3.05, "size": 12.0}]
    assert runner["ex"]["availableToLay"] == [{"price": 3.2, "size": 9.0}]
    assert runner["ex"]["tradedVolume"] == [{"price": 3.1, "size": 42.0}]


def test_streaming_feed_partial_market_definition_nested_metadata_is_preserved():
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
                    "id": "1.860",
                    "marketDefinition": {
                        "status": "OPEN",
                        "inPlay": False,
                        "keyLineDefinition": {"kl": [{"id": 88, "hc": 0.0}]},
                        "runners": [
                            {
                                "id": 88,
                                "name": "Runner 88",
                                "status": "ACTIVE",
                                "adjustmentFactor": 11.0,
                                "bsp": 2.9,
                            }
                        ],
                    },
                }
            ]
        }
    )
    feed._process_message(
        {
            "mc": [
                {
                    "id": "1.860",
                    "marketDefinition": {
                        "inPlay": True,
                        "runners": [
                            {
                                "id": 88,
                                "status": "REMOVED",
                                "removalDate": "2026-03-01T11:00:00Z",
                            }
                        ],
                    },
                }
            ]
        }
    )

    final = updates[-1]
    assert final["status"] == "OPEN"
    assert final["inplay"] is True
    assert final["marketDefinition"]["keyLineDefinition"] == {"kl": [{"id": 88, "hc": 0.0}]}
    runner_def = final["marketDefinition"]["runners"][0]
    assert runner_def["name"] == "Runner 88"
    assert runner_def["adjustmentFactor"] == 11.0
    assert runner_def["bsp"] == 2.9
    assert runner_def["status"] == "REMOVED"
    assert runner_def["removalDate"] == "2026-03-01T11:00:00Z"
    runner = final["runners"][0]
    assert runner["runnerName"] == "Runner 88"
    assert runner["status"] == "REMOVED"
    assert runner["adjustmentFactor"] == 11.0
    assert runner["bsp"] == 2.9
    assert runner["removalDate"] == "2026-03-01T11:00:00Z"


def test_streaming_feed_session_gate_blocks_subscribe_when_invalid():
    subscriptions = []
    disconnects = []
    client = _Client(scripts=[[{"mc": [{"id": "1.999", "rc": []}]}]], subscriptions=subscriptions)

    feed = StreamingFeed(
        client_getter=lambda: client,
        config={
            "enabled": True,
            "market_data_mode": "stream",
            "market_ids": ["1.999"],
            "max_auth_failures": 1,
        },
        on_market_book=lambda _book: None,
        on_disconnect=lambda payload: disconnects.append(payload),
        listener_factory=_Listener,
        session_gate=lambda: {"ok": False, "reason": "SESSION_INVALID"},
    )
    feed.start()
    time.sleep(0.2)
    feed.stop()

    assert subscriptions == []
    status = feed.status()
    assert status["auth_degraded"] is True
    assert status["auth_failure_count"] >= 1
    assert disconnects and disconnects[0]["kind"] == "auth"


def test_streaming_feed_repeated_auth_failure_is_bounded():
    subscriptions = []
    guard_calls = {"n": 0}
    client = _Client(scripts=[[{"mc": [{"id": "1.998", "rc": []}]}]], subscriptions=subscriptions)

    def _guard():
        guard_calls["n"] += 1
        return {"ok": False, "reason": "SESSION_EXPIRED"}

    feed = StreamingFeed(
        client_getter=lambda: client,
        config={
            "enabled": True,
            "market_data_mode": "stream",
            "market_ids": ["1.998"],
            "reconnect_backoff_sec": 1,
            "max_auth_failures": 2,
        },
        on_market_book=lambda _book: None,
        on_disconnect=lambda _payload: None,
        listener_factory=_Listener,
        session_gate=_guard,
    )
    feed.start()
    time.sleep(2.3)
    feed.stop()

    status = feed.status()
    assert status["auth_degraded"] is True
    assert status["auth_failure_count"] == 2
    assert guard_calls["n"] == 2
    assert subscriptions == []


def test_streaming_feed_keepalive_failure_is_visible_in_status():
    feed = StreamingFeed(
        client_getter=lambda: None,
        config={"enabled": False, "market_data_mode": "poll"},
        on_market_book=lambda _book: None,
    )
    feed._mark_keepalive_failure("SESSION_EXPIRED")
    snap = feed.status()

    assert snap["keepalive_failure_count"] == 1
    assert "SESSION_EXPIRED" in snap["last_keepalive_error"]
    assert "SESSION_EXPIRED" in snap["last_auth_error"]
