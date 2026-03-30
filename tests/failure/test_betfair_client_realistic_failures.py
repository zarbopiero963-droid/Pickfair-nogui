import pytest

from tests.helpers.betfair_fixtures import load_betfair_fixture


class FakeResponse:
    def __init__(self, *, json_data=None, status_code=200, raise_http=False, text=""):
        self._json_data = json_data
        self.status_code = status_code
        self._raise_http = raise_http
        self.text = text

    def raise_for_status(self):
        if self._raise_http:
            import requests
            raise requests.exceptions.HTTPError(response=self)

    def json(self):
        if isinstance(self._json_data, Exception):
            raise self._json_data
        return self._json_data


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)

    def post(self, url, **kwargs):
        _ = url, kwargs
        if not self.responses:
            raise RuntimeError("no more fake responses")
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


@pytest.mark.failure
def test_session_expired_realistic_fixture_disconnects():
    from betfair_client import BetfairClient

    session = FakeSession([
        FakeResponse(json_data=load_betfair_fixture("session_expired.json")),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
    )
    client.session_token = "TOK"
    client.connected = True

    with pytest.raises(RuntimeError, match="SESSION_EXPIRED"):
        client.get_market_book("1.23456789")

    assert client.session_token == ""
    assert client.connected is False


@pytest.mark.failure
def test_invalid_json_rpc_shape_realistic_fixture():
    from betfair_client import BetfairClient

    session = FakeSession([
        FakeResponse(json_data=load_betfair_fixture("invalid_json_rpc_shape.json")),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
    )
    client.session_token = "TOK"

    with pytest.raises(RuntimeError, match="INVALID_JSON_RPC"):
        client.get_market_book("1.23456789")