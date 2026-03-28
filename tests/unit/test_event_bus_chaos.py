import threading
import time

import pytest

from core.event_bus import EventBus


def wait_until(condition, timeout=5.0, interval=0.01):
    start = time.time()
    while time.time() - start < timeout:
        if condition():
            return True
        time.sleep(interval)
    return False


@pytest.mark.core
@pytest.mark.chaos
@pytest.mark.concurrency
def test_event_bus_handles_heavy_parallel_publish_without_losing_all_events():
    bus = EventBus()
    received = []
    lock = threading.Lock()

    def handler(payload):
        with lock:
            received.append(payload)

    bus.subscribe("PING", handler)

    threads = []
    total = 200

    for i in range(total):
        t = threading.Thread(target=bus.publish, args=("PING", i))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    ok = wait_until(lambda: len(received) == total, timeout=5.0)
    assert ok, "Sotto publish parallelo pesante, EventBus non deve perdere eventi"


@pytest.mark.core
@pytest.mark.chaos
@pytest.mark.failure
def test_event_bus_survives_slow_and_failing_subscribers_together():
    bus = EventBus()
    healthy_calls = []

    def slow(_payload):
        time.sleep(0.05)

    def failing(_payload):
        raise RuntimeError("subscriber exploded")

    def healthy(payload):
        healthy_calls.append(payload)

    bus.subscribe("PING", slow)
    bus.subscribe("PING", failing)
    bus.subscribe("PING", healthy)

    bus.publish("PING", {"id": 1})

    ok = wait_until(lambda: healthy_calls == [{"id": 1}], timeout=5.0)
    assert ok, "Subscriber lenti o in errore non devono impedire la consegna ai subscriber sani"


@pytest.mark.core
@pytest.mark.chaos
@pytest.mark.concurrency
def test_event_bus_subscribe_and_unsubscribe_under_load_do_not_crash():
    bus = EventBus()
    received = []

    def handler(payload):
        received.append(payload)

    def subscribe_unsubscribe_loop():
        for _ in range(50):
            bus.subscribe("PING", handler)
            bus.unsubscribe("PING", handler)

    def publish_loop():
        for i in range(100):
            bus.publish("PING", i)

    threads = []
    for _ in range(5):
        threads.append(threading.Thread(target=subscribe_unsubscribe_loop))
        threads.append(threading.Thread(target=publish_loop))

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert True, "Subscribe/unsubscribe concorrenti sotto carico non devono causare crash"