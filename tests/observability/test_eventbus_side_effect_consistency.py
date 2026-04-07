import time

import pytest

from core.event_bus import EventBus


@pytest.mark.observability
def test_eventbus_partial_fanout_failure_does_not_prevent_other_side_effects():
    bus = EventBus(workers=1)
    sink = []

    def bad_handler(payload):
        raise RuntimeError(f"boom:{payload}")

    def good_handler(payload):
        sink.append(payload)

    bus.subscribe("order.finalized", bad_handler)
    bus.subscribe("order.finalized", good_handler)

    bus.publish("order.finalized", {"order_id": "A1"})
    bus.stop()

    assert sink == [{"order_id": "A1"}]
    assert bus.stats()["queue_size"] == 0


@pytest.mark.observability
def test_eventbus_stats_reflect_subscriber_fanout():
    bus = EventBus(workers=1)

    bus.subscribe("evt", lambda _p: None)
    bus.subscribe("evt", lambda _p: None)

    stats = bus.stats()
    assert stats["subscribers"]["evt"] == 2

    bus.stop()
