import threading

import pytest

from auto_throttle import AutoThrottle


@pytest.mark.chaos
@pytest.mark.concurrency
def test_concurrent_calls_do_not_crash():
    t = AutoThrottle(max_calls=10, period=1, base_backoff=0.001, max_backoff=0.01)

    def worker():
        for _ in range(100):
            t.allow_call()

    threads = [threading.Thread(target=worker) for _ in range(10)]

    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert t.get_current_rate() >= 0


@pytest.mark.chaos
def test_many_very_close_calls_never_produce_negative_delay():
    t = AutoThrottle(max_calls=1, period=60, base_backoff=0.001, max_backoff=0.1)

    t.record_call()

    for _ in range(1000):
        assert t.acquire_delay() >= 0.0