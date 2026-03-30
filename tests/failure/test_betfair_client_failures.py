import pytest
import requests
from requests.exceptions import Timeout


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
def test_login_timeout(monkeypatch, tmp_path):
    from betfair_client import BetfairClient

    cert = tmp_path / "client.crt"
    key = tmp_path / "client.key"
    cert.write_text("CERT")
    key.write_text("KEY")

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem=str(cert),
        key_pem=str(key),
        session=FakeSession([Timeout()]),
    )

    with pytest.raises(RuntimeError, match="LOGIN_TIMEOUT"):
        client.login("pw")


@pytest.mark.failure
def test_login_http_error(monkeypatch, tmp_path):
    from betfair_client import BetfairClient

    cert = tmp_path / "client.crt"
    key = tmp_path / "client.key"
    cert.write_text("CERT")
    key.write_text("KEY")

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem=str(cert),
        key_pem=str(key),
        session=FakeSession([
            FakeResponse(status_code=500, raise_http=True, text="boom")
        ]),
    )

    with pytest.raises(RuntimeError, match="LOGIN_HTTP_ERROR"):
        client.login("pw")


@pytest.mark.failure
def test_invalid_login_json(monkeypatch, tmp_path):
    from betfair_client import BetfairClient

    cert = tmp_path / "client.crt"
    key = tmp_path / "client.key"
    cert.write_text("CERT")
    key.write_text("KEY")

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem=str(cert),
        key_pem=str(key),
        session=FakeSession([
            FakeResponse(json_data=ValueError("bad json"))
        ]),
    )

    with pytest.raises(RuntimeError, match="INVALID_LOGIN_JSON"):
        client.login("pw")


@pytest.mark.failure
def test_session_expired_clears_state():
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
        client.get_market_book("1.100")

    assert client.session_token == ""
    assert client.connected is False


@pytest.mark.failure
def test_invalid_json_in_jsonrpc():
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
        client.get_market_book("1.100")


@pytest.mark.failure
def test_invalid_json_rpc_shape():
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
        client.get_market_book("1.100")


@pytest.mark.failure
def test_http_error_on_market_book_retries_and_fails():
    from betfair_client import BetfairClient

    session = FakeSession([
        FakeResponse(status_code=502, raise_http=True, text="bad gateway"),
        FakeResponse(status_code=502, raise_http=True, text="bad gateway"),
        FakeResponse(status_code=502, raise_http=True, text="bad gateway"),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
        max_retries=2,
    )
    client.session_token = "TOK"

    with pytest.raises(RuntimeError, match=r"REQUEST_FAILED: HTTP_502"):
        client.get_market_book("1.100")


@pytest.mark.failure
def test_place_bet_timeout_ambiguous_returns_order_unknown_true():
    from betfair_client import BetfairClient

    session = FakeSession([
        Timeout(),
        Timeout(),
        Timeout(),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
        max_retries=2,
    )
    client.session_token = "TOK"

    out = client.place_bet(
        market_id="1.100",
        selection_id=11,
        side="BACK",
        price=2.0,
        size=5.0,
    )

    assert out["ok"] is False
    assert "REQUEST_FAILED: TIMEOUT" in out["error"] or "TIMEOUT" in out["error"]
    assert out["classification"] == "TRANSIENT"
    assert out["order_unknown"] is True