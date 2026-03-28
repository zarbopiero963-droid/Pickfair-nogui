import pytest


@pytest.mark.smoke
def test_import_circuit_breaker():
    import circuit_breaker  # noqa: F401


@pytest.mark.smoke
def test_construct_circuit_breaker():
    from circuit_breaker import CircuitBreaker

    cb = CircuitBreaker()
    assert cb is not None
    assert cb.state.value == "CLOSED"