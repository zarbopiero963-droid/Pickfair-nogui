import threading
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


@pytest.mark.chaos
@pytest.mark.core
def test_stop_drain_true_processes_queued_events_before_shutdown():
    bus = EventBus(workers=1)
    processed = []

    def slow_handler(payload):
        time.sleep(0.02)
        processed.append(payload)

    bus.subscribe("PING", slow_handler)

    for i in range(6):
        bus.publish("PING", i)

    result = bus.stop()

    assert result == {"drain": True, "dropped_events": 0}
    assert processed == list(range(6)), "draining stop non deve perdere eventi in coda"


@pytest.mark.chaos
@pytest.mark.core
@pytest.mark.failure
def test_stop_drain_false_is_explicitly_lossy_and_reports_dropped_events():
    bus = EventBus(workers=1)
    started = threading.Event()
    continue_work = threading.Event()
    processed = []

    def blocking_handler(payload):
        started.set()
        continue_work.wait(timeout=1.0)
        processed.append(payload)

    bus.subscribe("PING", blocking_handler)

    for i in range(5):
        bus.publish("PING", i)

    assert started.wait(timeout=1.0), "il primo evento deve essere in esecuzione"

    result = bus.stop_lossy(timeout=2.0)
    continue_work.set()

    assert result["drain"] is False
    assert result["dropped_events"] >= 1, "stop lossy deve rendere esplicita la perdita"
    assert wait_until(lambda: len(processed) == 1)
    assert processed == [0], "solo l'evento già in esecuzione può completare"
