"""T3 regressions: odds type/finiteness validation (offline, no network).

Rows: PF-I04T-01, PF-I04T-02.
"""
from __future__ import annotations

import pytest

from betfair_client import BetfairClient
from core import validators

_NAN = float("nan")
_INF = float("inf")

# I04T-01/02: NaN, +Infinity, -Infinity, None, non-numeric strings, bool.
_INVALID_ODDS = [_NAN, _INF, -_INF, "NaN", "Infinity", "-Infinity", None, "abc", "", True, False]


@pytest.mark.parametrize("value,expected", [(1.01, 1.01), (2.0, 2.0), ("2.0", 2.0)])
def test_pf_i04t_02_finite_odds_pass_type_and_finiteness(value, expected):
    """ID: PF-I04T-02

    Oracle: `1.01` is a finite number (type/finiteness OK, market rules are a
    separate check); `2.0` is valid as type/finiteness.
    """
    assert validators.order_price(value) == expected


@pytest.mark.parametrize("value", _INVALID_ODDS, ids=repr)
def test_pf_i04t_02_non_finite_bool_none_text_odds_rejected(value):
    """ID: PF-I04T-02

    Oracle: NaN, Infinity, -Infinity, True/False, None and non-numeric text
    are rejected with the explicit reason INVALID_PRICE.
    """
    with pytest.raises(ValueError, match="^INVALID_PRICE$"):
        validators.order_price(value)


@pytest.mark.parametrize("value", _INVALID_ODDS, ids=repr)
def test_pf_i04t_01_lay_liability_never_computed_on_invalid_odds(value):
    """ID: PF-I04T-01

    Invalid odds are rejected BEFORE the liability calculation: the single
    risk formula (LAY = stake * (price - 1)) raises INVALID_PRICE instead of
    returning a NaN/inf/negative liability.
    """
    with pytest.raises(ValueError, match="^INVALID_PRICE$"):
        validators.order_exposure("LAY", 10.0, value)


class _CountingSession:
    def __init__(self):
        self.posts = 0

    def post(self, *args, **kwargs):  # pragma: no cover - must never run
        self.posts += 1
        raise AssertionError("transport reached with invalid odds")


@pytest.mark.parametrize("value", _INVALID_ODDS, ids=repr)
def test_pf_i04t_01_client_rejects_invalid_odds_with_zero_transport(value):
    """ID: PF-I04T-01

    Order boundary: BetfairClient.place_bet rejects every invalid odds value
    with INVALID_PRICE and zero POSTs on the transport.
    """
    session = _CountingSession()
    client = BetfairClient(username="u", app_key="a", cert_pem="c.pem", key_pem="k.pem", session=session)
    client.session_token = "TOK"
    with pytest.raises(RuntimeError, match="^INVALID_PRICE$"):
        client.place_bet(market_id="1.234", selection_id=5678, side="BACK", price=value, size=5.0)
    assert session.posts == 0
