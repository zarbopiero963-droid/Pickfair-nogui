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


@pytest.mark.integration
def test_network_error_then_success_recovers_for_market_book():
    from betfair_client import BetfairClient

    session = FakeSession([
        requests.exceptions.ConnectionError("net down"),
        FakeResponse(
            json_data=[{
                "result": [
                    {"marketId": "1.100", "runners": []}
                ]
            }]
        ),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
        max_retries=1,
    )
    client.session_token = "TOK"

    out = client.get_market_book("1.100")
    assert out["marketId"] == "1.100"


@pytest.mark.integration
def test_network_error_then_success_recovers_for_place_bet():
    from betfair_client import BetfairClient

    session = FakeSession([
        requests.exceptions.ConnectionError("net down"),
        FakeResponse(
            json_data=[{
                "result": {
                    "status": "SUCCESS",
                    "instructionReports": [
                        {"status": "SUCCESS", "betId": "BET1"}
                    ],
                }
            }]
        ),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
        max_retries=1,
    )
    client.session_token = "TOK"

    out = client.place_bet(
        market_id="1.100",
        selection_id=11,
        side="BACK",
        price=2.0,
        size=5.0,
    )
    assert out["ok"] is True