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
