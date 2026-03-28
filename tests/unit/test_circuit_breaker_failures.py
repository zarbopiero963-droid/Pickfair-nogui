import time

import pytest

from circuit_breaker import CircuitBreaker, State, PermanentError, TransientError


@pytest.mark.unit
@pytest.mark.failure
def test_threshold_failure_opens_breaker():
    cb = CircuitBreaker(max_failures=2, reset_timeout=1.0)

    with pytest.raises(TransientError):
        cb.call(lambda: (_ for _ in ()).throw(RuntimeError("temporary")))

    with pytest.raises(TransientError):
        cb.call(lambda: (_ for _ in ()).throw(RuntimeError("temporary")))

    assert cb.state == State.OPEN


@pytest.mark.unit
@pytest.mark.failure
def test_market_closed_is_permanent():
    cb = CircuitBreaker(max_failures=2, reset_timeout=1.0)

    with pytest.raises(PermanentError):
        cb.call(lambda: (_ for _ in ()).throw(RuntimeError("market closed")))

    assert cb.state == State.CLOSED
    assert cb.failures == 0


@pytest.mark.unit
@pytest.mark.failure
def test_half_open_probe_already_in_progress_blocks_second_call():
    cb = CircuitBreaker(max_failures=1, reset_timeout=0.01)
    cb.record_failure(RuntimeError("boom"))
    time.sleep(0.02)

    assert cb.is_half_open() is True

    # simula probe già in corso
    cb._half_open_in_flight = True

    with pytest.raises(RuntimeError, match="HALF_OPEN"):
        cb.call(lambda: "x")