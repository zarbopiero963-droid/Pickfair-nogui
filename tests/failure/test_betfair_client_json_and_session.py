import pytest
import requests


class FakeResponse:
    def __init__(self, *, status_code=200, json_data=None, text="", raise_http=False):
        self.status_code = status_code
        self._json_data = json_data
        self.text = text
        self._raise_http = raise_http

    def raise_for_status(self):
        if self._raise_http:
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
def test_market_book_invalid_json_raises():
    from betfair_client import BetfairClient

    session = FakeSession([
        FakeResponse(json_data=ValueError("bad json"))
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
    )
    client.session_token = "TOK"

    with pytest.raises(RuntimeError, match="INVALID_JSON"):
        client.get_market_book("1.300")


@pytest.mark.failure
def test_market_book_invalid_json_rpc_raises():
    from betfair_client import BetfairClient

    session = FakeSession([
        FakeResponse(json_data={"not": "a list"})
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
        client.get_market_book("1.301")


@pytest.mark.failure
def test_session_expired_disconnects_and_clears_token():
    from betfair_client import BetfairClient

    session = FakeSession([
        FakeResponse(
            json_data=[{
                "error": {"code": -32099, "message": "INVALID_SESSION"}
            }]
        )
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
        client.get_market_book("1.302")

    assert client.session_token == ""
    assert client.connected is False