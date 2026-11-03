"""CB-586-FI-03Z (#586 c.6588978024, P1): un livello del book con size 0 non offre liquidita'.

Su Betfair un delta con size 0 rimuove il livello: il SIM non deve abbinarci contro (nessun matched finto, nessuna
esposizione abbinata inventata). Scheda Grok: LAY 0,10@3.0 su book back 2.0/0 e lay 2.02/0 -> EXECUTION_COMPLETE
0,10 @2.02 (FAIL); atteso ordine interamente a riposo, riserva unmatched 0,20 (liability LAY 0,10 x (3.0-1)).
"""
from __future__ import annotations

import math

import pytest

from simulation_broker import SimulationBroker

MKT, SEL = "1.900", 11


def _book(back, lay):
    return {"marketId": MKT, "status": "OPEN", "runners": [{"selectionId": SEL, "status": "ACTIVE", "ex": {
        "availableToBack": [{"price": p, "size": s} for p, s in back],
        "availableToLay": [{"price": p, "size": s} for p, s in lay]}}]}


def _broker():
    b = SimulationBroker(starting_balance=10.0, commission_pct=4.5)
    b.login()
    return b


def _place(b, side, price, size):
    r = b.place_bet(market_id=MKT, selection_id=SEL, side=side, price=price, size=size)
    return r["instructionReports"][0]


def test_lay_against_zero_size_levels_rests_fully_unmatched():
    b = _broker()
    b.update_market_book(_book([(2.0, 0.0)], [(2.02, 0.0)]))
    rep = _place(b, "LAY", 3.0, 0.10)
    assert rep["sizeMatched"] == 0
    assert b.get_account_funds()["reserved_unmatched"] == pytest.approx(0.20)


def test_back_against_a_zero_size_level_rests_fully_unmatched():
    b = _broker()
    b.update_market_book(_book([(2.0, 0.0)], [(2.02, 5.0)]))
    rep = _place(b, "BACK", 1.5, 0.10)
    assert rep["sizeMatched"] == 0


def test_zero_size_level_is_absent_and_the_next_level_is_the_best():
    b = _broker()
    b.update_market_book(_book([(2.0, 0.0), (1.9, 5.0)], [(2.02, 0.0), (2.1, 5.0)]))
    rep = _place(b, "BACK", 1.5, 0.10)
    assert rep["sizeMatched"] == pytest.approx(0.10) and rep["averagePriceMatched"] == pytest.approx(1.9)
    rep = _place(b, "LAY", 3.0, 0.10)
    assert rep["sizeMatched"] == pytest.approx(0.10) and rep["averagePriceMatched"] == pytest.approx(2.1)


def test_positive_liquidity_still_matches_and_is_capped():
    b = _broker()
    b.update_market_book(_book([(2.0, 0.05)], [(2.02, 5.0)]))
    rep = _place(b, "BACK", 1.5, 0.10)
    assert rep["sizeMatched"] == pytest.approx(0.05) and math.isclose(rep["averagePriceMatched"], 2.0)


def test_best_counterparty_price_and_matching_agree_on_zero_size():
    b = _broker()
    b.update_market_book(_book([(2.0, 0.0)], [(2.02, 0.0)]))
    assert b.best_counterparty_price(MKT, SEL, "BACK") is None
    assert _place(b, "BACK", 1.5, 0.10)["sizeMatched"] == 0


def test_matching_never_mutates_the_stored_book():
    """Il book memorizzato (stato persistito, letto anche da best_counterparty_price) resta quello ricevuto: saltare
    un livello vuoto non deve rimuoverlo dallo stato condiviso."""
    b = _broker()
    b.update_market_book(_book([(2.0, 0.0), (1.9, 5.0)], [(2.02, 0.0), (2.1, 5.0)]))
    before = [dict(lv) for lv in b.state.market_books[MKT]["runners"][0]["ex"]["availableToBack"]]
    before_lay = [dict(lv) for lv in b.state.market_books[MKT]["runners"][0]["ex"]["availableToLay"]]
    _place(b, "BACK", 1.5, 0.10)
    _place(b, "LAY", 3.0, 0.10)
    ex = b.state.market_books[MKT]["runners"][0]["ex"]
    assert [lv["price"] for lv in ex["availableToBack"]] == [lv["price"] for lv in before]
    assert [lv["price"] for lv in ex["availableToLay"]] == [lv["price"] for lv in before_lay]
