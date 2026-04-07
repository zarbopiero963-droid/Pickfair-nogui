import threading
import time

import pytest

from core.event_bus import EventBus


@pytest.mark.chaos
def test_stop_default_drains_queued_events_instead_of_dropping_them():
    bus = EventBus(workers=1)
    gate = threading.Event()
    processed = []

    def slow_handler(payload):
        gate.wait(timeout=0.5)
        processed.append(payload)

    bus.subscribe("evt", slow_handler)

    for i in range(5):
        bus.publish("evt", i)

    # ensure first event has been dequeued and processing is blocked
    time.sleep(0.05)
    gate.set()
    bus.stop()

    assert sorted(processed) == [0, 1, 2, 3, 4]
    assert bus.stats()["queue_size"] == 0


@pytest.mark.chaos
def test_stop_lossy_makes_event_loss_semantics_explicit():
    bus = EventBus(workers=1)
    gate = threading.Event()
    processed = []

    def blocked_handler(payload):
        gate.wait(timeout=0.5)
        processed.append(payload)

    bus.subscribe("evt", blocked_handler)

    for i in range(20):
        bus.publish("evt", i)

    time.sleep(0.02)
    bus.stop_lossy()
    gate.set()

    # lossy shutdown is now explicit and observable when drain=False
    assert len(processed) < 20
    assert bus.stats()["queue_size"] > 0
