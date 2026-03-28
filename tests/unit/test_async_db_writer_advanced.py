import threading
import time

import pytest

from core.event_bus import EventBus


def wait_until(condition, timeout=3.0, interval=0.01):
    start = time.time()
    while time.time() - start < timeout:
        if condition():
            return True
        time.sleep(interval)
    return False


@pytest.mark.core
@pytest.mark.concurrency
@pytest.mark.invariant
def test_event_bus_does_not_lose_events_under_parallel_publish():
    bus = EventBus()
    received = []
    lock = threading.Lock()

    def handler(payload):
        with lock:
            received.append(payload)

    bus.subscribe("PING", handler)

    threads = []
    total = 50

    for i in range(total):
        t = threading.Thread(target=bus.publish, args=("PING", i))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    ok = wait_until(lambda: len(received) == total)
    assert ok, "EventBus non deve perdere eventi sotto publish concorrente"
    assert sorted(received) == list(range(total)), "EventBus deve consegnare tutti i payload pubblicati"


@pytest.mark.core
@pytest.mark.failure
def test_event_bus_survives_multiple_failing_subscribers():
    bus = EventBus()
    healthy_calls = []

    def broken_one(_payload):
        raise RuntimeError("boom-1")

    def broken_two(_payload):
        raise ValueError("boom-2")

    def healthy(payload):
        healthy_calls.append(payload)

    bus.subscribe("PING", broken_one)
    bus.subscribe("PING", broken_two)
    bus.subscribe("PING", healthy)

    bus.publish("PING", {"ok": True})

    ok = wait_until(lambda: healthy_calls == [{"ok": True}])
    assert ok, "Subscriber sani devono continuare a ricevere eventi anche se altri subscriber falliscono"


@pytest.mark.core
@pytest.mark.concurrency
@pytest.mark.failure
def test_event_bus_unsubscribe_during_publish_does_not_break_delivery():
    bus = EventBus()
    received = []

    def second(payload):
        received.append(("second", payload))

    def first(payload):
        received.append(("first", payload))
        bus.unsubscribe("PING", second)

    bus.subscribe("PING", first)
    bus.subscribe("PING", second)

    bus.publish("PING", "x")

    ok = wait_until(lambda: len(received) == 2)
    assert ok, "Publish deve lavorare su snapshot/copia dei subscriber"

    bus.publish("PING", "y")
    ok2 = wait_until(lambda: ("first", "y") in received)
    assert ok2, "Il primo subscriber deve continuare a ricevere eventi"
    assert ("second", "y") not in received, "Il secondo subscriber deve risultare disiscritto dal publish successivo"