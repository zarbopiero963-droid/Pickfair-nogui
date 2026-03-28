import pytest

from circuit_breaker import CircuitBreaker, State, TransientError


class FakeApi:
    def __init__(self):
        self.calls = 0

    def flaky(self):
        self.calls += 1
        if self.calls <= 2:
            raise RuntimeError("temporary upstream failure")
        return {"ok": True}


@pytest.mark.integration
def test_circuit_breaker_wraps_flaky_dependency():
    cb = CircuitBreaker(max_failures=3, reset_timeout=0.1)
    api = FakeApi()

    with pytest.raises(TransientError):
        cb.call(api.flaky)

    with pytest.raises(TransientError):
        cb.call(api.flaky)

    result = cb.call(api.flaky)

    assert result == {"ok": True}
    assert cb.state == State.CLOSED
    assert cb.failures == 0