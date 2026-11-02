"""CB-586-FI-08B (#586 c.6588978024, P1): i fondi SIM sono esatti al centesimo, senza deriva binaria.

Scheda Grok: dopo 3 BACK 0,10@2.0 exposure 0.30000000000000004, available 9.700000000000001, total
10.000000000000002; con bankroll 0,30/0,60/0,70 impegnato da N x 0,10, available -5.55e-17/-1.11e-16 e
RuntimeController._account_from_funds rendeva BALANCE_NOT_FINITE_OR_NEGATIVE (ingressi negati), con 1,00/10 ok:
stesso stato economico, verdetto di rischio diverso.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from core.runtime_controller import RuntimeController
from simulation_broker import SimulationBroker

MKT, SEL = "1.800", 7
BOOK = {"marketId": MKT, "status": "OPEN", "runners": [{"selectionId": SEL, "status": "ACTIVE", "ex": {
    "availableToBack": [{"price": 2.0, "size": 500.0}], "availableToLay": [{"price": 2.02, "size": 500.0}]}}]}


def _broker(balance):
    b = SimulationBroker(starting_balance=balance, commission_pct=4.5)
    b.login()
    b.update_market_book(dict(BOOK))
    return b


def _back(b, size=0.10, price=2.0):
    return b.place_bet(market_id=MKT, selection_id=SEL, side="BACK", price=price, size=size)


def _cent_exact(v):
    d = Decimal(repr(v))
    return d == d.quantize(Decimal("0.01"))


def test_funds_stay_cent_exact_after_each_of_seven_microstakes():
    b = _broker(10.0)
    for _ in range(7):
        _back(b)
        f = b.get_account_funds()
        for key in ("available", "exposure", "total", "reserved_unmatched"):
            assert _cent_exact(f[key]), (key, f[key])
    assert (f["exposure"], f["available"], f["total"]) == (0.70, 9.30, 10.00)


def test_three_microstakes_give_the_exact_values_of_the_card():
    b = _broker(10.0)
    for _ in range(3):
        _back(b)
    f = b.get_account_funds()
    assert (f["exposure"], f["available"], f["total"]) == (0.30, 9.70, 10.00)


@pytest.mark.parametrize("n", [3, 6, 7, 10])
def test_fully_committed_bankroll_has_zero_available_and_the_same_risk_verdict(n):
    b = _broker(round(n * 0.10, 2))
    for _ in range(n):
        _back(b)
    f = b.get_account_funds()
    assert f["available"] == 0.0
    verdict = RuntimeController._account_from_funds(f)
    assert verdict["ok"] is True and verdict["available"] == 0.0, verdict


def test_sub_cent_values_are_kept_not_rounded_away():
    """Una liability non al centesimo (LAY 0,10@2.37 = 0,137) resta 0,137: si toglie il rumore, non il valore."""
    b = _broker(10.0)
    b.update_market_book({"marketId": MKT, "status": "OPEN", "runners": [{"selectionId": SEL, "status": "ACTIVE",
                          "ex": {"availableToBack": [], "availableToLay": [{"price": 2.37, "size": 500.0}]}}]})
    b.place_bet(market_id=MKT, selection_id=SEL, side="LAY", price=2.37, size=0.10)
    f = b.get_account_funds()
    assert f["exposure"] == 0.137 and f["available"] == 9.863 and f["total"] == 10.0
