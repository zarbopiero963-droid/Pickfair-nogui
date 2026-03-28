import pytest

from circuit_breaker import CircuitBreaker, State, PermanentError, TransientError


@pytest.mark.unit
@pytest.mark.guardrail
def test_starts_closed():
    cb = CircuitBreaker(max_failures=3, reset_timeout=1.0)
    assert cb.state == State.CLOSED
    assert cb.failures == 0
    assert cb.is_open() is False
    assert cb.is_half_open() is False


@pytest.mark.unit
@pytest.mark.guardrail
def test_opens_after_threshold():
    cb = CircuitBreaker(max_failures=2, reset_timeout=10.0)

    cb.record_failure(RuntimeError("x"))
    assert cb.state == State.CLOSED
    assert cb.failures == 1

    cb.record_failure(RuntimeError("y"))
    assert cb.state == State.OPEN
    assert cb.is_open() is True


@pytest.mark.unit
@pytest.mark.guardrail
def test_reset_closes_breaker():
    cb = CircuitBreaker(max_failures=1, reset_timeout=10.0)
    cb.record_failure(RuntimeError("boom"))
    assert cb.state == State.OPEN

    cb.reset()
    assert cb.state == State.CLOSED
    assert cb.failures == 0
    assert cb.opened_at is None


@pytest.mark.unit
@pytest.mark.guardrail
def test_success_in_closed_resets_failures():
    cb = CircuitBreaker(max_failures=3, reset_timeout=10.0)
    cb.record_failure(RuntimeError("boom"))
    assert cb.failures == 1

    result = cb.call(lambda: "ok")

    assert result == "ok"
    assert cb.state == State.CLOSED
    assert cb.failures == 0


@pytest.mark.unit
@pytest.mark.guardrail
def test_permanent_error_does_not_increment_failures():
    cb = CircuitBreaker(max_failures=2, reset_timeout=10.0)

    def fn():
        raise PermanentError("market_closed")

    with pytest.raises(PermanentError):
        cb.call(fn)

    assert cb.state == State.CLOSED
    assert cb.failures == 0


@pytest.mark.unit
@pytest.mark.guardrail
def test_generic_exception_becomes_transient_error():
    cb = CircuitBreaker(max_failures=2, reset_timeout=10.0)

    def fn():
        raise RuntimeError("temporary network issue")

    with pytest.raises(TransientError):
        cb.call(fn)

    assert cb.failures == 1
    assert cb.state == State.CLOSED


@pytest.mark.unit
@pytest.mark.guardrail
def test_invalid_session_becomes_permanent_error():
    cb = CircuitBreaker(max_failures=2, reset_timeout=10.0)

    def fn():
        raise RuntimeError("invalid_session")

    with pytest.raises(PermanentError):
        cb.call(fn)

    assert cb.failures == 0
    assert cb.state == State.CLOSED