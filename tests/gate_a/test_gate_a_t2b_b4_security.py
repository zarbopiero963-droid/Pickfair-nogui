"""Gate A T2b batch b4 — security: core.safety_layer (gate LIVE, cashout request, market book)."""
from __future__ import annotations

import pytest

from core.safety_layer import (
    MarketSanityError,
    RiskInvariantError,
    SafetyLayer,
    assert_live_gate_or_refuse,
)


def _valid_cashout_payload(_sl):
    """Payload completo secondo CASHOUT_REQUEST_SCHEMA (market_id, selection_id, side, stake, price, green_up)."""
    return {"market_id": "1.1", "selection_id": 1, "side": "LAY", "stake": 5.0, "price": 2.0, "green_up": 0.0}


def test_trg_628_b_0858():
    """ID: TRG-628-B-0858 / PF-B1-SAFETY-KILL

    Con kill_switch truthy (True, 1, stringa non vuota) LIVE e' rifiutato anche con tutti gli altri gate OK;
    con kill switch spento e gate OK LIVE e' ammesso."""
    for ks in (True, 1, "yes", "false"):
        d = assert_live_gate_or_refuse(execution_mode="LIVE", live_enabled=True, live_readiness_ok=True,
                                       kill_switch=ks)
        assert d.allowed is False and d.reason_code == "kill_switch_active", ks
        assert d.effective_execution_mode == "SIMULATION"
    d = assert_live_gate_or_refuse(execution_mode="LIVE", live_enabled=True, live_readiness_ok=True,
                                   kill_switch=False)
    assert d.allowed is True and d.effective_execution_mode == "LIVE"


def test_trg_628_b_0860():
    """ID: TRG-628-B-0860 / PF-B1-SAFETY-CASH-PRICE

    validate_cashout_request rifiuta prezzo non finito (NaN, +inf) o <= 1."""
    sl = SafetyLayer()
    base = _valid_cashout_payload(sl)
    assert sl.validate_cashout_request(dict(base)) is True
    for bad in (float("nan"), float("inf"), 1.0, 0.5):
        with pytest.raises(MarketSanityError):
            sl.validate_cashout_request({**base, "price": bad})


def test_trg_628_b_0861():
    """ID: TRG-628-B-0861 / PF-B1-SAFETY-CASH-STAKE

    validate_cashout_request rifiuta stake non finito (NaN, +inf) o <= 0."""
    sl = SafetyLayer()
    base = _valid_cashout_payload(sl)
    for bad in (float("nan"), float("inf"), 0.0, -2.0):
        with pytest.raises(RiskInvariantError):
            sl.validate_cashout_request({**base, "stake": bad})


def _book(back=None, lay=None):
    ex = {}
    if back is not None:
        ex["availableToBack"] = back
    if lay is not None:
        ex["availableToLay"] = lay
    return {"runners": [{"selectionId": 1, "ex": ex}]}


def test_trg_628_b_0863():
    """ID: TRG-628-B-0863 / PF-B1-SAFETY-BOOK-FIRST

    validate_market_book controlla soltanto la PRIMA offerta di ogni ladder presente: ladder assente/vuoto
    ammesso (mercato sottile), prima offerta non dict o price<=1 => MarketSanityError, offerte successive
    non ispezionate."""
    sl = SafetyLayer()
    assert sl.validate_market_book(_book()) is True
    assert sl.validate_market_book(_book(back=[], lay=[])) is True
    assert sl.validate_market_book(_book(back=[{"price": 2.0}, {"price": 0.5}], lay=[{"price": 2.1}, "x"])) is True
    for bad in ([{"price": 1.0}], ["x"], [{"price": 0}]):
        with pytest.raises(MarketSanityError):
            sl.validate_market_book(_book(back=bad))
        with pytest.raises(MarketSanityError):
            sl.validate_market_book(_book(lay=bad))


@pytest.mark.xfail(strict=True, reason="TRG-628-B-0863: prima offerta con price NaN/inf passa validate_market_book "
                                       "(manca isfinite, a differenza di cashout/common order rules F15)")
def test_trg_628_b_0863_non_finite_best_offer_fail_closed():
    """ID: TRG-628-B-0863 / PF-B1-SAFETY-BOOK-FIRST

    Comportamento fail-closed atteso (P35, coerente con F15 #453/#461 PR26): una prima offerta con prezzo non
    finito e' dato di mercato corrotto e deve essere rifiutata."""
    sl = SafetyLayer()
    for bad in (float("nan"), float("inf"), "nan"):
        with pytest.raises(MarketSanityError):
            sl.validate_market_book(_book(back=[{"price": bad, "size": 10}]))
