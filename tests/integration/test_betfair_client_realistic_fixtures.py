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
        return self._json_data


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if not self.responses:
            raise RuntimeError("no more fake responses")
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


@pytest.mark.integration
def test_login_success_with_realistic_fixture(tmp_path):
    from betfair_client import BetfairClient

    cert = tmp_path / "client.crt"
    key = tmp_path / "client.key"
    cert.write_text("CERT")
    key.write_text("KEY")

    session = FakeSession([
        FakeResponse(json_data=load_betfair_fixture("login_success.json")),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem=str(cert),
        key_pem=str(key),
        session=session,
    )

    out = client.login("pw")

    assert out["connected"] is True
    assert client.session_token == "TOK_REALISTIC_123"
    assert client.connected is True


@pytest.mark.integration
def test_get_account_funds_with_realistic_fixture():
    from betfair_client import BetfairClient

    session = FakeSession([
        FakeResponse(json_data=load_betfair_fixture("account_funds_success.json")),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
    )
    client.session_token = "TOK"

    out = client.get_account_funds()

    assert out["available"] == 152.73
    assert out["exposure"] == 18.5
    assert "discount_rate" in out


@pytest.mark.integration
def test_get_market_book_with_realistic_fixture():
    from betfair_client import BetfairClient

    session = FakeSession([
        FakeResponse(json_data=load_betfair_fixture("market_book_success.json")),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
    )
    client.session_token = "TOK"

    out = client.get_market_book("1.23456789")

    assert out["marketId"] == "1.23456789"
    assert len(out["runners"]) == 2
    assert out["runners"][0]["availableToBack"][0]["price"] == 2.0
    assert out["runners"][0]["availableToLay"][0]["price"] == 2.02


@pytest.mark.integration
def test_get_market_book_missing_quotes_with_realistic_fixture():
    from betfair_client import BetfairClient

    session = FakeSession([
        FakeResponse(json_data=load_betfair_fixture("market_book_missing_quotes.json")),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
    )
    client.session_token = "TOK"

    out = client.get_market_book("1.23456789")

    assert out["runners"][0]["availableToBack"] == []
    assert out["runners"][0]["availableToLay"] == []
    assert out["runners"][1]["availableToBack"] == []
    assert out["runners"][1]["availableToLay"] == []


@pytest.mark.integration
def test_place_bet_success_with_realistic_fixture():
    from betfair_client import BetfairClient

    session = FakeSession([
        FakeResponse(json_data=load_betfair_fixture("place_orders_success.json")),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
    )
    client.session_token = "TOK"

    out = client.place_bet(
        market_id="1.23456789",
        selection_id=101,
        side="BACK",
        price=2.0,
        size=5.0,
    )

    assert out["ok"] is True
    assert out["result"]["status"] == "SUCCESS"


@pytest.mark.integration
def test_place_bet_rejected_with_realistic_fixture():
    from betfair_client import BetfairClient

    session = FakeSession([
        FakeResponse(json_data=load_betfair_fixture("place_orders_rejected_insufficient_funds.json")),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
    )
    client.session_token = "TOK"

    out = client.place_bet(
        market_id="1.23456789",
        selection_id=101,
        side="BACK",
        price=2.0,
        size=5000.0,
    )

    assert out["ok"] is False
    assert "BET_REJECTED" in out["error"]