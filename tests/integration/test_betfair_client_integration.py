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
def test_login_success(monkeypatch, tmp_path):
    from betfair_client import BetfairClient

    cert = tmp_path / "client.crt"
    key = tmp_path / "client.key"
    cert.write_text("CERT")
    key.write_text("KEY")

    session = FakeSession([
        FakeResponse(
            json_data={
                "loginStatus": "SUCCESS",
                "sessionToken": "TOK123",
                "sessionExpiryTime": "2026-12-01T10:00:00Z",
            }
        )
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
    assert out["session_token"] is True
    assert client.session_token == "TOK123"
    assert client.connected is True


@pytest.mark.integration
def test_get_market_book_empty_returns_none():
    from betfair_client import BetfairClient

    session = FakeSession([
        FakeResponse(json_data=[{"result": []}]),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
    )
    client.session_token = "TOK"

    assert client.get_market_book("1.100") is None


@pytest.mark.integration
def test_get_market_book_missing_quotes_is_hardened():
    from betfair_client import BetfairClient

    session = FakeSession([
        FakeResponse(
            json_data=[{
                "result": [
                    {
                        "marketId": "1.100",
                        "runners": [
                            {"selectionId": 11, "ex": {}},
                            {"selectionId": 22},
                        ],
                    }
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
    )
    client.session_token = "TOK"

    out = client.get_market_book("1.100")

    assert out["marketId"] == "1.100"
    assert out["runners"][0]["availableToBack"] == []
    assert out["runners"][0]["availableToLay"] == []
    assert out["runners"][1]["availableToBack"] == []
    assert out["runners"][1]["availableToLay"] == []


@pytest.mark.integration
def test_place_bet_success_returns_ok_true():
    from betfair_client import BetfairClient

    session = FakeSession([
        FakeResponse(
            json_data=[{
                "result": {
                    "status": "SUCCESS",
                    "instructionReports": [
                        {"status": "SUCCESS", "betId": "BET1"}
                    ],
                }
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

    out = client.place_bet(
        market_id="1.100",
        selection_id=11,
        side="BACK",
        price=2.0,
        size=5.0,
    )

    assert out["ok"] is True
    assert out["result"]["status"] == "SUCCESS"


@pytest.mark.integration
def test_place_bet_failed_status_returns_structured_error():
    from betfair_client import BetfairClient

    session = FakeSession([
        FakeResponse(
            json_data=[{
                "result": {
                    "status": "FAILURE",
                    "instructionReports": [],
                }
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

    out = client.place_bet(
        market_id="1.100",
        selection_id=11,
        side="BACK",
        price=2.0,
        size=5.0,
    )

    assert out["ok"] is False
    assert "BET_FAILED" in out["error"]
    assert out["classification"] in {"PERMANENT", "UNKNOWN"}


@pytest.mark.integration
def test_place_bet_rejected_report_returns_structured_error():
    from betfair_client import BetfairClient

    session = FakeSession([
        FakeResponse(
            json_data=[{
                "result": {
                    "status": "SUCCESS",
                    "instructionReports": [
                        {
                            "status": "FAILURE",
                            "errorCode": "INSUFFICIENT_FUNDS",
                        }
                    ],
                }
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

    out = client.place_bet(
        market_id="1.100",
        selection_id=11,
        side="BACK",
        price=2.0,
        size=5.0,
    )

    assert out["ok"] is False
    assert "BET_REJECTED" in out["error"]
    assert out["classification"] == "UNKNOWN"