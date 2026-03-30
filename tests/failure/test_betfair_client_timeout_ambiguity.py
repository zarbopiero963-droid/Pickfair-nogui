import pytest
from requests.exceptions import Timeout


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
def test_place_bet_timeout_ambiguous_sets_order_unknown_true():
    from betfair_client import BetfairClient

    session = FakeSession([Timeout(), Timeout(), Timeout()])

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
        market_id="1.200",
        selection_id=1,
        side="BACK",
        price=2.0,
        size=2.0,
    )

    assert out["ok"] is False
    assert out["order_unknown"] is True
    assert out["classification"] == "TRANSIENT"
    assert "TIMEOUT" in out["error"]


@pytest.mark.failure
def test_place_bet_timeout_ambiguous_never_crashes():
    from betfair_client import BetfairClient

    session = FakeSession([Timeout()])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
        max_retries=0,
    )
    client.session_token = "TOK"

    out = client.place_bet(
        market_id="1.201",
        selection_id=2,
        side="LAY",
        price=3.0,
        size=5.0,
    )

    assert isinstance(out, dict)
    assert out["ok"] is False
    assert out["order_unknown"] is True