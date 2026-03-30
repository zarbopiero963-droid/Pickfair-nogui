import pytest

from tests.helpers.betfair_fixtures import load_betfair_fixture


class FakeResponse:
    def __init__(self, *, json_data=None):
        self._json_data = json_data
        self.status_code = 200
        self.text = ""

    def raise_for_status(self):
        return None

    def json(self):
        return self._json_data


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)

    def post(self, url, **kwargs):
        _ = url, kwargs
        if not self.responses:
            raise RuntimeError("no more fake responses")
        return self.responses.pop(0)


@pytest.mark.e2e
def test_realistic_full_flow(tmp_path):
    from betfair_client import BetfairClient

    cert = tmp_path / "client.crt"
    key = tmp_path / "client.key"
    cert.write_text("CERT")
    key.write_text("KEY")

    session = FakeSession([
        FakeResponse(json_data=load_betfair_fixture("login_success.json")),
        FakeResponse(json_data=load_betfair_fixture("account_funds_success.json")),
        FakeResponse(json_data=load_betfair_fixture("market_book_success.json")),
        FakeResponse(json_data=load_betfair_fixture("place_orders_success.json")),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem=str(cert),
        key_pem=str(key),
        session=session,
    )

    login_out = client.login("pw")
    assert login_out["connected"] is True

    funds = client.get_account_funds()
    assert funds["available"] == 152.73

    market = client.get_market_book("1.23456789")
    assert market["marketId"] == "1.23456789"

    bet = client.place_bet(
        market_id="1.23456789",
        selection_id=101,
        side="BACK",
        price=2.0,
        size=5.0,
    )
    assert bet["ok"] is True

    logout = client.logout()
    assert logout["ok"] is True