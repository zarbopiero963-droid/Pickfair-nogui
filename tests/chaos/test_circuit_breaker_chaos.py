import threading
import time

import pytest

from circuit_breaker import CircuitBreaker, State, TransientError


@pytest.mark.chaos
@pytest.mark.core
@pytest.mark.concurrency
def test_concurrent_failures_open_breaker_without_corrupting_state():
    cb = CircuitBreaker(max_failures=5, reset_timeout=1.0)

    def worker():
        try:
            cb.call(lambda: (_ for _ in ()).throw(RuntimeError("temporary chaos")))
        except Exception:
            pass

    threads = [threading.Thread(target=worker) for _ in range(20)]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert cb.state == State.OPEN
    assert cb.failures >= 5


@pytest.mark.chaos
@pytest.mark.core
@pytest.mark.invariant
def test_open_does_not_jump_directly_to_closed_without_half_open_success():
    cb = CircuitBreaker(max_failures=1, reset_timeout=0.05)
    cb.record_failure(RuntimeError("boom"))

    # forza _on_success mentre è OPEN: non deve chiudere
    cb._on_success()
    assert cb.state == State.OPEN

    time.sleep(0.06)
    assert cb.is_half_open() is True

    result = cb.call(lambda: "ok")
    assert result == "ok"
    assert cb.state == State.CLOSED


@pytest.mark.chaos
@pytest.mark.core
@pytest.mark.failure
def test_repeated_transient_failures_keep_breaker_open_until_timeout():
    cb = CircuitBreaker(max_failures=1, reset_timeout=0.1)

    with pytest.raises(TransientError):
        cb.call(lambda: (_ for _ in ()).throw(RuntimeError("x")))

    assert cb.state == State.OPEN

    with pytest.raises(RuntimeError, match="OPEN"):
        cb.call(lambda: "blocked")

    time.sleep(0.11)
    assert cb.is_open() is False
    assert cb.state == State.HALF_OPEN