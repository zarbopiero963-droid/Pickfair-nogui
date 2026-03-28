import threading

import pytest

from auto_throttle import AutoThrottle


class FakeClock:
    def __init__(self, start=1000.0):
        self.now = float(start)
        self._lock = threading.Lock()

    def time(self):
        with self._lock:
            return self.now

    def advance(self, seconds):
        with self._lock:
            self.now += float(seconds)


def test_allow_call_respects_limit_and_window(monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr("auto_throttle.time.time", clock.time)

    throttle = AutoThrottle(max_calls=2, period=10.0)

    assert throttle.allow_call() is True
    assert throttle.allow_call() is True
    assert throttle.allow_call() is False

    clock.advance(10.01)
    assert throttle.allow_call() is True


@pytest.mark.guardrail
@pytest.mark.invariant
def test_negative_inputs_are_normalized_to_safe_values():
    throttle = AutoThrottle(max_calls=-5, period=-3)
    assert throttle.max_calls == 1
    assert throttle.period == 0.0
    assert throttle.get_current_rate() >= 0.0


@pytest.mark.guardrail
@pytest.mark.invariant
def test_update_never_makes_rate_negative():
    throttle = AutoThrottle()
    throttle.update(api_calls_min=-99)
    assert throttle.get_current_rate() == 0.0

    throttle.update(api_calls_min="abc")
    assert throttle.get_current_rate() == 0.0


@pytest.mark.failure
def test_wait_is_noop_and_does_not_raise():
    throttle = AutoThrottle()
    assert throttle.wait() is None


@pytest.mark.guardrail
def test_reset_clears_state():
    throttle = AutoThrottle(max_calls=2, period=60.0)
    throttle.record_call()
    throttle.record_call()
    assert throttle.get_current_rate() > 0.0

    throttle.reset()
    assert throttle.get_current_rate() == 0.0
    assert throttle.is_blocked() is False
    assert throttle.allow_call() is True


@pytest.mark.chaos
@pytest.mark.concurrency
def test_parallel_allow_call_does_not_over_admit(monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr("auto_throttle.time.time", clock.time)

    throttle = AutoThrottle(max_calls=5, period=60.0)
    results = []
    lock = threading.Lock()

    def worker():
        allowed = throttle.allow_call()
        with lock:
            results.append(allowed)

    threads = [threading.Thread(target=worker) for _ in range(30)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results.count(True) == 5
    assert results.count(False) == 25


@pytest.mark.failure
def test_record_call_can_overflow_but_rate_stays_non_negative(monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr("auto_throttle.time.time", clock.time)

    throttle = AutoThrottle(max_calls=1, period=5.0)
    for _ in range(20):
        throttle.record_call()

    assert throttle.get_current_rate() >= 0.0