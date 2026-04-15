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
def test_market_book_timeout_then_success_recovers():
    from betfair_client import BetfairClient

    session = FakeSession([
        Timeout(),
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
    assert len(session.calls) == 2


@pytest.mark.integration
def test_market_book_http_500_then_success_recovers():
    from betfair_client import BetfairClient

    session = FakeSession([
        FakeResponse(status_code=500, raise_http=True, text="boom"),
        FakeResponse(
            json_data=[{
                "result": [
                    {"marketId": "1.101", "runners": []}
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

    out = client.get_market_book("1.101")

    assert out["marketId"] == "1.101"
    assert len(session.calls) == 2


@pytest.mark.integration
def test_place_bet_network_then_success_recovers():
    from betfair_client import BetfairClient

    session = FakeSession([
        requests.exceptions.ConnectionError("net down"),
        FakeResponse(
            json_data=[{
                "result": {
                    "status": "SUCCESS",
                    "instructionReports": [
                        {"status": "SUCCESS", "betId": "B1"}
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
        market_id="1.102",
        selection_id=12,
        side="BACK",
        price=2.0,
        size=5.0,
    )

    assert out["ok"] is True
    assert len(session.calls) == 2


@pytest.mark.integration
def test_io_snapshot_tracks_degraded_then_recovered_request_state():
    from betfair_client import BetfairClient

    session = FakeSession([
        Timeout(),
        FakeResponse(
            json_data=[{
                "result": [
                    {"marketId": "1.103", "runners": []}
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
    out = client.get_market_book("1.103")

    snap = client.io_snapshot()
    assert out["marketId"] == "1.103"
    assert snap["last_operation"] == "SportsAPING/v1.0/listMarketBook"
    assert snap["last_status"] in {"SUCCESS", "SLOW"}
    assert snap["total_calls"] >= 1
