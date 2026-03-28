import time

import pytest

from circuit_breaker import CircuitBreaker, State, TransientError


@pytest.mark.unit
@pytest.mark.core
@pytest.mark.invariant
def test_half_open_after_reset_timeout():
    cb = CircuitBreaker(max_failures=1, reset_timeout=0.05)

    cb.record_failure(RuntimeError("boom"))
    assert cb.state == State.OPEN
    assert cb.is_open() is True

    time.sleep(0.06)

    assert cb.is_open() is False
    assert cb.state == State.HALF_OPEN
    assert cb.is_half_open() is True


@pytest.mark.unit
@pytest.mark.core
@pytest.mark.invariant
def test_success_in_half_open_transitions_to_closed():
    cb = CircuitBreaker(max_failures=1, reset_timeout=0.05)
    cb.record_failure(RuntimeError("boom"))
    time.sleep(0.06)

    assert cb.state in {State.OPEN, State.HALF_OPEN}
    result = cb.call(lambda: "ok")

    assert result == "ok"
    assert cb.state == State.CLOSED
    assert cb.failures == 0


@pytest.mark.unit
@pytest.mark.core
@pytest.mark.invariant
def test_failure_in_half_open_reopens_breaker():
    cb = CircuitBreaker(max_failures=1, reset_timeout=0.05)
    cb.record_failure(RuntimeError("boom"))
    time.sleep(0.06)

    with pytest.raises(TransientError):
        cb.call(lambda: (_ for _ in ()).throw(RuntimeError("temp fail")))

    assert cb.state == State.OPEN
    assert cb.is_open() is True


@pytest.mark.unit
@pytest.mark.core
@pytest.mark.invariant
def test_open_state_blocks_calls():
    cb = CircuitBreaker(max_failures=1, reset_timeout=10.0)
    cb.record_failure(RuntimeError("boom"))

    with pytest.raises(RuntimeError, match="OPEN"):
        cb.call(lambda: "should not run")


@pytest.mark.unit
@pytest.mark.core
@pytest.mark.invariant
def test_snapshot_is_consistent():
    cb = CircuitBreaker(max_failures=2, reset_timeout=1.0)
    cb.record_failure(RuntimeError("x"))
    snap = cb.snapshot()

    assert snap["state"] == "CLOSED"
    assert snap["failures"] == 1
    assert snap["max_failures"] == 2
    assert snap["reset_timeout"] == 1.0