import pytest
from betfair_client import BetfairClient


class TimeoutSession:
    def post(self, *a, **k):
        raise TimeoutError()


def test_timeout():
    c = BetfairClient(
        username="u",
        app_key="k",
        cert_pem="c",
        key_pem="k",
        session=TimeoutSession()
    )
    c.session_token = "X"

    with pytest.raises(RuntimeError):
        c._post_jsonrpc("url", "m", {})


class HttpErrorSession:
    def post(self, *a, **k):
        class R:
            def raise_for_status(self):
                raise Exception("HTTP ERROR")
        return R()


def test_http_error():
    c = BetfairClient(
        username="u",
        app_key="k",
        cert_pem="c",
        key_pem="k",
        session=HttpErrorSession()
    )
    c.session_token = "X"

    with pytest.raises(RuntimeError):
        c._post_jsonrpc("url", "m", {})