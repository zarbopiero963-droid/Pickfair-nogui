import time
import pytest

from auto_throttle import AutoThrottle


@pytest.mark.unit
def test_basic_limit():
    t = AutoThrottle(max_calls=2, period=1)

    assert t.allow_call() is True
    assert t.allow_call() is True
    assert t.allow_call() is False


@pytest.mark.unit
def test_delay_never_negative():
    t = AutoThrottle(max_calls=1, period=1)

    t.record_call()
    delay = t.acquire_delay()

    assert delay >= 0.0


@pytest.mark.unit
def test_backoff_monotonic():
    t = AutoThrottle(max_calls=1, period=60, base_backoff=0.1, max_backoff=1.0)

    t.record_call()

    d1 = t.acquire_delay()
    d2 = t.acquire_delay()
    d3 = t.acquire_delay()

    assert d2 >= d1
    assert d3 >= d2


@pytest.mark.unit
def test_backoff_caps():
    t = AutoThrottle(max_calls=1, period=60, base_backoff=0.1, max_backoff=0.2)

    t.record_call()

    last = 0.0
    for _ in range(10):
        last = t.acquire_delay()

    assert last <= 0.2


@pytest.mark.unit
def test_reset_after_idle():
    t = AutoThrottle(max_calls=1, period=0.2)

    assert t.allow_call() is True
    assert t.allow_call() is False

    time.sleep(0.5)

    assert t.allow_call() is True


@pytest.mark.unit
def test_burst_requests():
    t = AutoThrottle(max_calls=5, period=1)

    results = [t.allow_call() for _ in range(20)]

    assert results.count(True) == 5
    assert results.count(False) == 15


@pytest.mark.unit
def test_saturation_high_frequency():
    t = AutoThrottle(max_calls=3, period=1)

    for _ in range(100):
        t.record_call()

    delay = t.acquire_delay()
    assert delay > 0


@pytest.mark.unit
def test_timestamp_density():
    t = AutoThrottle(max_calls=100, period=1)

    for _ in range(1000):
        t.record_call()

    assert len(t._timestamps) <= 1000
    assert t.get_current_rate() >= 0


@pytest.mark.unit
def test_reset():
    t = AutoThrottle(max_calls=1, period=1)

    assert t.allow_call() is True
    assert t.allow_call() is False

    t.reset()

    assert t.allow_call() is True


@pytest.mark.unit
def test_invalid_inputs_are_sanitized():
    t = AutoThrottle(max_calls="bad", period="bad", base_backoff="bad", max_backoff="bad")

    assert t.max_calls >= 1
    assert t.period > 0
    assert t.base_backoff >= 0
    assert t.max_backoff >= t.base_backoff