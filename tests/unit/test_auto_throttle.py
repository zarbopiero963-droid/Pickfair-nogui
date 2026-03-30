import time
import pytest

from auto_throttle import AutoThrottle


def test_basic_limit():
    t = AutoThrottle(max_calls=2, period=1)

    assert t.allow_call() is True
    t.record_call()

    assert t.allow_call() is True
    t.record_call()

    assert t.allow_call() is False


def test_delay_never_negative():
    t = AutoThrottle(max_calls=1, period=1)

    t.record_call()
    delay = t.acquire_delay()

    assert delay >= 0


def test_backoff_monotonic():
    t = AutoThrottle(max_calls=1, period=1, base_backoff=0.1)

    t.record_call()

    d1 = t.acquire_delay()
    d2 = t.acquire_delay()
    d3 = t.acquire_delay()

    assert d2 >= d1
    assert d3 >= d2


def test_backoff_caps():
    t = AutoThrottle(max_calls=1, period=1, base_backoff=0.1, max_backoff=0.2)

    t.record_call()

    for _ in range(10):
        d = t.acquire_delay()

    assert d <= 0.2


def test_reset_after_idle():
    t = AutoThrottle(max_calls=1, period=0.2)

    t.record_call()
    assert t.allow_call() is False

    time.sleep(0.5)

    assert t.allow_call() is True


def test_burst_requests():
    t = AutoThrottle(max_calls=5, period=1)

    results = []
    for _ in range(20):
        results.append(t.allow_call())
        t.record_call()

    assert results.count(True) == 5


def test_saturation_high_frequency():
    t = AutoThrottle(max_calls=3, period=1)

    for _ in range(100):
        t.record_call()

    delay = t.acquire_delay()
    assert delay > 0


def test_timestamp_density():
    t = AutoThrottle(max_calls=100, period=1)

    for _ in range(1000):
        t.record_call()

    assert len(t._timestamps) <= 1000


def test_reset():
    t = AutoThrottle(max_calls=1, period=1)

    t.record_call()
    assert t.allow_call() is False

    t.reset()
    assert t.allow_call() is True