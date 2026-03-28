import pytest
from betfair_client import BetfairClient


def make_client():
    c = BetfairClient(
        username="u",
        app_key="k",
        cert_pem="c",
        key_pem="k"
    )
    c.session_token = "X"
    return c


def test_cashout_equal_profit():
    c = make_client()

    r = c.calculate_cashout(100, 2.0, 1.5)

    assert abs(r["profit_if_win"] - r["profit_if_lose"]) < 0.5


def test_market_book_empty():
    c = make_client()

    c._post_jsonrpc = lambda *a, **k: []

    assert c.get_market_book("1") is None


def test_not_authenticated():
    c = BetfairClient(username="u", app_key="k", cert_pem="c", key_pem="k")

    with pytest.raises(RuntimeError):
        c._post_jsonrpc("url", "m", {})