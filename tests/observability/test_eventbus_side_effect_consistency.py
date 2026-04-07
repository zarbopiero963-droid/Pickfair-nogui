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
