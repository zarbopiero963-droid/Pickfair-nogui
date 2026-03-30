import pytest


class DummySession:
    pass


@pytest.fixture
def client():
    from betfair_client import BetfairClient

    return BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=DummySession(),
        max_retries=2,
    )


@pytest.mark.unit
@pytest.mark.guardrail
def test_classify_error_transient_timeout(client):
    assert client._classify_error("TIMEOUT") == "TRANSIENT"


@pytest.mark.unit
@pytest.mark.guardrail
def test_classify_error_transient_network(client):
    assert client._classify_error("NETWORK_ERROR: conn reset") == "TRANSIENT"


@pytest.mark.unit
@pytest.mark.guardrail
def test_classify_error_transient_http5xx(client):
    assert client._classify_error("HTTP_500") == "TRANSIENT"
    assert client._classify_error("HTTP_503") == "TRANSIENT"


@pytest.mark.unit
@pytest.mark.guardrail
def test_classify_error_permanent_session(client):
    assert client._classify_error("SESSION_EXPIRED") == "PERMANENT"


@pytest.mark.unit
@pytest.mark.guardrail
def test_classify_error_permanent_invalid_json(client):
    assert client._classify_error("INVALID_JSON") == "PERMANENT"


@pytest.mark.unit
@pytest.mark.guardrail
def test_classify_error_permanent_api(client):
    assert client._classify_error("API_ERROR: INVALID_MARKET_ID") == "PERMANENT"


@pytest.mark.unit
@pytest.mark.guardrail
def test_classify_error_unknown_default(client):
    assert client._classify_error("WHATEVER_RANDOM") == "UNKNOWN"