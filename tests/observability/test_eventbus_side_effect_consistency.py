import time

import pytest

from core.event_bus import EventBus


def wait_until(condition, timeout=2.0, interval=0.01):
    start = time.time()
    while time.time() - start < timeout:
        if condition():
            return True
        time.sleep(interval)
    return False


@pytest.mark.observability
@pytest.mark.core
@pytest.mark.failure
def test_subscriber_failure_preserves_successful_side_effect_evidence():
    bus = EventBus(workers=2)
    side_effects = []

    def broken(_payload):
        raise RuntimeError("boom")

    def healthy(payload):
        side_effects.append(("healthy", payload["id"]))

    bus.subscribe("ORDER_PLACED", broken)
    bus.subscribe("ORDER_PLACED", healthy)

    for i in range(4):
        bus.publish("ORDER_PLACED", {"id": i})

    result = bus.stop()

    assert result == {"drain": True, "dropped_events": 0}
    assert side_effects == [("healthy", 0), ("healthy", 1), ("healthy", 2), ("healthy", 3)]


@pytest.mark.observability
@pytest.mark.core
def test_publish_after_stop_is_noop_and_does_not_fake_delivery():
    bus = EventBus(workers=1)
    delivered = []

    def record(payload):
        delivered.append(payload)

    bus.subscribe("PING", record)
    bus.publish("PING", "before-stop")
    assert wait_until(lambda: delivered == ["before-stop"])

    result = bus.stop()
    bus.publish("PING", "after-stop")

    time.sleep(0.1)

    assert result == {"drain": True, "dropped_events": 0}
    assert delivered == ["before-stop"], "dopo stop il bus non deve accettare nuovi eventi"


@pytest.mark.observability
@pytest.mark.core
def test_subscriber_errors_tracked_per_subscriber_name():
    """EventBus must track per-subscriber error counts for poison-pill detection."""
    bus = EventBus(workers=1)

    def bad_handler(_payload):
        raise RuntimeError("poison")

    bus.subscribe("SIGNAL", bad_handler)

    for _ in range(4):
        bus.publish("SIGNAL", {})

    bus.stop()

    errors = bus.subscriber_error_counts()
    assert errors.get("bad_handler", 0) == 4, \
        "each exception from bad_handler must increment its error count"


@pytest.mark.observability
@pytest.mark.core
def test_subscriber_errors_isolated_per_subscriber():
    """Error count for a broken subscriber must not affect count for a healthy one."""
    bus = EventBus(workers=1)
    healthy_calls = []

    def broken_sub(_payload):
        raise ValueError("boom")

    def healthy_sub(payload):
        healthy_calls.append(payload)

    bus.subscribe("EVT", broken_sub)
    bus.subscribe("EVT", healthy_sub)

    for i in range(3):
        bus.publish("EVT", i)

    bus.stop()

    errors = bus.subscriber_error_counts()
    assert errors.get("broken_sub", 0) == 3
    assert errors.get("healthy_sub", 0) == 0
    assert len(healthy_calls) == 3


@pytest.mark.observability
@pytest.mark.core
def test_published_total_increments_once_per_dispatched_event():
    """published_total_count() must increment exactly once per publish() call
    that has at least one subscriber, regardless of subscriber count."""
    bus = EventBus(workers=1)
    received = []

    bus.subscribe("TICK", lambda d: received.append(d))

    for i in range(3):
        bus.publish("TICK", i)

    bus.stop()

    assert bus.published_total_count() == 3
    assert len(received) == 3


@pytest.mark.observability
@pytest.mark.core
def test_published_total_not_incremented_for_no_subscriber_events():
    """Events published with no subscribers must not increment published_total."""
    bus = EventBus(workers=1)
    bus.publish("ORPHAN", "data")
    bus.stop()
    assert bus.published_total_count() == 0


@pytest.mark.observability
@pytest.mark.core
def test_queue_depth_returns_zero_on_idle_bus():
    """queue_depth() must return 0 when no work is pending."""
    bus = EventBus(workers=2)
    assert bus.queue_depth() == 0
    bus.stop()


@pytest.mark.observability
@pytest.mark.core
def test_published_total_multiple_subscribers_counts_event_once():
    """With N subscribers, published_total must still increment by 1 per publish call."""
    bus = EventBus(workers=2)
    calls = []

    bus.subscribe("SIG", lambda d: calls.append(("a", d)))
    bus.subscribe("SIG", lambda d: calls.append(("b", d)))
    bus.subscribe("SIG", lambda d: calls.append(("c", d)))

    bus.publish("SIG", 42)
    bus.stop()

    # 3 subscribers each received the event, but published_total counts the event once
    assert bus.published_total_count() == 1
    assert len(calls) == 3


@pytest.mark.observability
@pytest.mark.core
def test_delivered_total_counts_only_successful_callbacks():
    bus = EventBus(workers=1)

    def ok_handler(_payload):
        return None

    def bad_handler(_payload):
        raise RuntimeError("boom")

    bus.subscribe("SIG", ok_handler)
    bus.subscribe("SIG", bad_handler)

    for _ in range(4):
        bus.publish("SIG", {})

    bus.stop()

    # 4 successful callbacks from ok_handler only; bad_handler failures excluded.
    assert bus.delivered_total_count() == 4


@pytest.mark.observability
@pytest.mark.core
def test_pressure_snapshot_exposes_queue_counters_and_high_watermark():
    bus = EventBus(workers=1)
    sink = []
    bus.subscribe("SIG", lambda payload: sink.append(payload))
    bus.publish("SIG", {"id": 1})
    bus.stop()
    snap = bus.pressure_snapshot()
    assert snap["enqueued_total"] >= 1
    assert snap["dequeued_total"] >= 1
    assert snap["queue_high_watermark"] >= 1
