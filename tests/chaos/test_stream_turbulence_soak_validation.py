from __future__ import annotations

import threading
import time
from queue import Queue

import pytest

from core.event_bus import EventBus
from observability.runtime_probe import RuntimeProbe
from services.streaming_feed import StreamingFeed


class _Listener:
    def __init__(self, *, output_queue):
        self.output_queue = output_queue


class _Stream:
    def __init__(self, output_queue: Queue, scripted_messages):
        self.output_queue = output_queue
        self.scripted_messages = list(scripted_messages)
        self.subscriptions = []
        self.started = False
        self.stopped = False

    def subscribe_to_markets(self, **kwargs):
        self.subscriptions.append(dict(kwargs))
        for msg in self.scripted_messages:
            self.output_queue.put(msg)

    def start(self):
        self.started = True

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


@pytest.mark.chaos
@pytest.mark.integration
def test_stream_turbulence_soak_has_bounded_recovery_and_visibility_thresholds():
    """
    Deterministic staged turbulence harness with explicit pass/fail thresholds.

    Stages exercised in one run:
    1) Burst updates + heartbeat silence to force reconnect
    2) Session invalidation during reconnect (single auth failure)
    3) Bounded recovery and coherent normalized state restoration
    4) Queue/backpressure visibility during churn via EventBus pressure snapshot
    """
    # --- explicit pass/fail thresholds ---
    HEARTBEAT_TIMEOUT_SEC = 1
    RECONNECT_BACKOFF_SEC = 1
    MAX_AUTH_FAILURES = 2
    MAX_SESSION_GATE_CALLS = 3
    MIN_RECONNECT_SUBSCRIPTIONS = 2
    MIN_BURST_UPDATES = 60
    MIN_QUEUE_HIGH_WATERMARK_DURING_CHURN = 20
    MAX_AUTH_DISCONNECTS = 1

    subscriptions = []
    disconnects = []
    updates = []
    auth_disconnect_seen = threading.Event()

    burst_script = [
        {
            "initialClk": "ic_soak_1" if i == 0 else "",
            "clk": f"c_soak_{i}",
            "mc": [
                {
                    "id": "1.950",
                    "rc": [
                        {
                            "id": 701,
                            "ltp": 2.0 + (i * 0.01),
                            "batb": [[0, 1.99 + (i * 0.005), 20.0 + i]],
                            "batl": [[0, 2.04 + (i * 0.005), 22.0 + i]],
                        }
                    ],
                }
            ],
        }
        for i in range(80)
    ]

    recovered_script = [
        {
            "mc": [
                {
                    "id": "1.950",
                    "marketDefinition": {
                        "status": "OPEN",
                        "inPlay": True,
                        "runners": [
                            {
                                "id": 701,
                                "name": "Runner 701",
                                "status": "ACTIVE",
                                "adjustmentFactor": 9.8,
                            }
                        ],
                    },
                    "rc": [
                        {
                            "id": 701,
                            "trd": [[2.3, 180.0]],
                        }
                    ],
                }
            ]
        }
    ]

    client = _Client(scripts=[burst_script, recovered_script], subscriptions=subscriptions)
    gate_calls = {"n": 0}

    def _session_gate():
        gate_calls["n"] += 1
        # 1st connect OK, 2nd attempt fails (session churn), 3rd recovers.
        if gate_calls["n"] == 2:
            return {"ok": False, "reason": "SESSION_EXPIRED"}
        return {"ok": True}

    event_bus = EventBus(workers=1)

    def _slow_subscriber(_payload):
        time.sleep(0.01)

    event_bus.subscribe("MARKET_BOOK_UPDATE", _slow_subscriber)

    def _on_book(book):
        updates.append(dict(book))
        event_bus.publish("MARKET_BOOK_UPDATE", dict(book))

    def _on_disconnect(payload):
        payload = dict(payload or {})
        disconnects.append(payload)
        if payload.get("kind") == "auth":
            auth_disconnect_seen.set()

    feed = StreamingFeed(
        client_getter=lambda: client,
        config={
            "enabled": True,
            "market_data_mode": "stream",
            "market_ids": ["1.950"],
            "heartbeat_timeout_sec": HEARTBEAT_TIMEOUT_SEC,
            "reconnect_backoff_sec": RECONNECT_BACKOFF_SEC,
            "max_auth_failures": MAX_AUTH_FAILURES,
        },
        on_market_book=_on_book,
        on_disconnect=_on_disconnect,
        listener_factory=_Listener,
        session_gate=_session_gate,
    )

    runtime_stub = type("RuntimeStub", (), {"streaming_feed": feed})()
    probe = RuntimeProbe(event_bus=event_bus, runtime_controller=runtime_stub)

    try:
        feed.start()

        burst_deadline = time.time() + 3.0
        while time.time() < burst_deadline and len(updates) < MIN_BURST_UPDATES:
            time.sleep(0.02)

        assert len(updates) >= MIN_BURST_UPDATES, "burst stage must emit enough updates"

        ctx_during_churn = probe.collect_correlation_context()
        eb_ctx = ctx_during_churn.get("event_bus") or {}
        assert int(eb_ctx.get("queue_high_watermark", 0) or 0) >= MIN_QUEUE_HIGH_WATERMARK_DURING_CHURN

        assert auth_disconnect_seen.wait(timeout=4.0), "session invalidation during reconnect must be exercised"

        status_during_auth_failure = feed.status()
        assert status_during_auth_failure["auth_failure_count"] >= 1
        assert status_during_auth_failure["auth_degraded"] is False

        recover_deadline = time.time() + 5.0
        while time.time() < recover_deadline:
            if len(subscriptions) >= MIN_RECONNECT_SUBSCRIPTIONS and updates:
                last = updates[-1]
                runners = last.get("runners") or []
                if (
                    runners
                    and last.get("marketId") == "1.950"
                    and last.get("inplay") is True
                    and runners[0].get("runnerName") == "Runner 701"
                    and runners[0].get("ex", {}).get("tradedVolume")
                ):
                    break
            time.sleep(0.02)

        assert len(subscriptions) >= MIN_RECONNECT_SUBSCRIPTIONS
        assert gate_calls["n"] <= MAX_SESSION_GATE_CALLS
        assert len([d for d in disconnects if d.get("kind") == "auth"]) <= MAX_AUTH_DISCONNECTS

        final = updates[-1]
        runner = (final.get("runners") or [])[0]
        assert final["marketId"] == "1.950"
        assert final["inplay"] is True
        assert final["marketDefinition"]["status"] == "OPEN"
        assert runner["runnerName"] == "Runner 701"
        assert runner["status"] == "ACTIVE"
        assert runner["adjustmentFactor"] == pytest.approx(9.8)
        assert runner["ex"]["tradedVolume"] == [{"price": 2.3, "size": 180.0}]

        final_status = feed.status()
        assert final_status["auth_degraded"] is False

    finally:
        feed.stop()
        event_bus.stop_lossy(timeout=1.0)
