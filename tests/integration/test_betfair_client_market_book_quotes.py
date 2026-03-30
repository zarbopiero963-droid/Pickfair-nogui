import pytest


class FakeResponse:
    def __init__(self, *, json_data=None):
        self._json_data = json_data

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


@pytest.mark.integration
def test_market_book_empty_returns_none():
    from betfair_client import BetfairClient

    session = FakeSession([
        FakeResponse(json_data=[{"result": []}])
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
    )
    client.session_token = "TOK"

    assert client.get_market_book("1.400") is None


@pytest.mark.integration
def test_market_book_missing_quotes_is_safe():
    from betfair_client import BetfairClient

    session = FakeSession([
        FakeResponse(
            json_data=[{
                "result": [{
                    "marketId": "1.401",
                    "runners": [
                        {"selectionId": 1, "ex": {}},
                        {"selectionId": 2},
                    ],
                }]
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

    out = client.get_market_book("1.401")

    assert out["marketId"] == "1.401"
    assert out["runners"][0]["availableToBack"] == []
    assert out["runners"][0]["availableToLay"] == []
    assert out["runners"][1]["availableToBack"] == []
    assert out["runners"][1]["availableToLay"] == []