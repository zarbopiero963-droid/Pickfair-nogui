import pytest
from betfair_client import BetfairClient


class ChaosSession:
    def post(self, *a, **k):
        class R:
            def raise_for_status(self): pass
            def json(self):
                return [{"error": "INVALID_SESSION"}]
        return R()


def test_session_expired():
    c = BetfairClient(
        username="u",
        app_key="k",
        cert_pem="c",
        key_pem="k",
        session=ChaosSession()
    )
    c.session_token = "X"

    with pytest.raises(RuntimeError):
        c._post_jsonrpc("url", "m", {})

    assert c.session_token == ""