"""Gate A — T2b batch b2, componente simulation (simulation_broker.SimulationBroker).

SimulationBroker reale in memoria (nessun DB di produzione, nessuna rete). PF-OD12-12: simulatore realistico,
nessun abbinamento garantito; LF-01: netto di mercato, mai doppia commissione; D4: availableToBack/availableToLay.
"""
from __future__ import annotations

from typing import Any, Dict, List

import pytest

from simulation_broker import SimulationBroker


class _Db:
    def __init__(self) -> None:
        self.saved: List[Dict[str, Any]] = []

    def save_simulation_bet(self, data):
        self.saved.append(data)


def _book(market_id="1.50", status="OPEN", runners=None) -> Dict[str, Any]:
    return {"marketId": market_id, "status": status, "runners": runners if runners is not None else [
        {"selectionId": 1, "status": "ACTIVE",
         "ex": {"availableToBack": [{"price": 2.0, "size": 100.0}],
                "availableToLay": [{"price": 2.2, "size": 100.0}]}},
        {"selectionId": 2, "status": "ACTIVE",
         "ex": {"availableToBack": [{"price": 3.0, "size": 100.0}],
                "availableToLay": [{"price": 3.2, "size": 100.0}]}},
    ]}


def _broker(**kw) -> SimulationBroker:
    return SimulationBroker(starting_balance=1000.0, **kw)


def _report(resp) -> Dict[str, Any]:
    return resp["instructionReports"][0]


# ---------------------------------------------------------------------------
@pytest.mark.parametrize("campo,valore,codice", [
    ("market_id", "", "INVALID_MARKET_ID"),
    ("selection_id", "x", "INVALID_SELECTION_ID"),
    ("selection_id", 0, "INVALID_SELECTION_ID"),
    ("price", float("nan"), "INVALID_PRICE"),
    ("price", 1.0, "INVALID_PRICE"),
    ("size", float("inf"), "INVALID_SIZE"),
    ("size", 0, "INVALID_SIZE"),
    ("side", "BAK", "INVALID_SIDE"),
    ("side", None, "INVALID_SIDE"),
])
def test_trg_628_b_0814(campo, valore, codice):
    """ID: TRG-628-B-0814 / PF-B1-SIM-INVALID

    Argomenti invalidi rifiutati con lo stesso codice del client LIVE PRIMA di creare l'ordine, abbinarlo o
    scriverlo: nessun ordine, nessun fill, saldo invariato, nessuna scrittura DB."""
    db = _Db()
    b = _broker(db=db)
    b.update_market_book(_book())
    args = dict(market_id="1.50", selection_id=1, side="BACK", price=2.0, size=10.0)
    args[campo] = valore
    with pytest.raises(RuntimeError) as exc:
        b.place_bet(**args)
    assert str(exc.value) == codice
    assert b.state.orders == {}
    assert b.state.fills == []
    assert b.state.balance == 1000.0
    assert db.saved == []


# ---------------------------------------------------------------------------
def test_trg_628_b_0815():
    """ID: TRG-628-B-0815 / PF-B1-SIM-RESTING-ACCEPTED

    Ordine valido che non incrocia il book (BACK a 2.5 con miglior back 2.0) resta a riposo: risposta SUCCESS,
    report SUCCESS con orderStatus EXECUTABLE e sizeMatched 0 (come Betfair), ordine memorizzato. Idem senza book."""
    b = _broker()
    b.update_market_book(_book())
    resp = b.place_bet(market_id="1.50", selection_id=1, side="BACK", price=2.5, size=10.0)
    assert resp["status"] == "SUCCESS"
    rep = _report(resp)
    assert rep["status"] == "SUCCESS" and rep["orderStatus"] == "EXECUTABLE" and rep["sizeMatched"] == 0.0
    assert b.state.orders[rep["betId"]].status == "EXECUTABLE"

    senza_book = _broker().place_bet(market_id="1.99", selection_id=1, side="LAY", price=2.0, size=5.0)
    assert senza_book["status"] == "SUCCESS"
    assert _report(senza_book)["status"] == "SUCCESS" and _report(senza_book)["orderStatus"] == "EXECUTABLE"


# ---------------------------------------------------------------------------
def test_trg_628_b_0816():
    """ID: TRG-628-B-0816 / PF-B1-SIM-BOOK-LADDER

    BACK si abbina sul miglior availableToBack (2.0), LAY sul miglior availableToLay (2.2), al prezzo del book
    (D4). Un BACK a 2.2 non incrocia (2.2 > miglior back 2.0) e resta a riposo; un LAY a 2.0 idem."""
    b = _broker()
    b.update_market_book(_book())
    back = _report(b.place_bet(market_id="1.50", selection_id=1, side="BACK", price=2.0, size=10.0))
    assert back["sizeMatched"] == 10.0 and back["averagePriceMatched"] == 2.0
    lay = _report(b.place_bet(market_id="1.50", selection_id=1, side="LAY", price=2.2, size=10.0))
    assert lay["sizeMatched"] == 10.0 and lay["averagePriceMatched"] == 2.2

    b2 = _broker()
    b2.update_market_book(_book())
    assert _report(b2.place_bet(market_id="1.50", selection_id=1, side="BACK", price=2.2, size=5.0))[
        "sizeMatched"] == 0.0
    assert _report(b2.place_bet(market_id="1.50", selection_id=1, side="LAY", price=2.0, size=5.0))[
        "sizeMatched"] == 0.0


# ---------------------------------------------------------------------------
def _book_un_livello(livello: Dict[str, Any]) -> Dict[str, Any]:
    return _book(runners=[{"selectionId": 1, "status": "ACTIVE",
                           "ex": {"availableToBack": [livello], "availableToLay": []}}])


def test_trg_628_b_0817():
    """ID: TRG-628-B-0817 / PF-B1-SIM-ZERO-DEPTH (parte ratificata, CB-586-FI-03Z)

    Un livello con size esplicita <= 0 non offre liquidita': con prezzo compatibile l'ordine NON viene abbinato
    (matched 0, EXECUTABLE), mai matched=requested."""
    for size in (0.0, -3.0):
        b = _broker()
        b.update_market_book(_book_un_livello({"price": 2.0, "size": size}))
        rep = _report(b.place_bet(market_id="1.50", selection_id=1, side="BACK", price=2.0, size=10.0))
        assert rep["sizeMatched"] == 0.0 and rep["orderStatus"] == "EXECUTABLE"


@pytest.mark.xfail(strict=True, reason="TRG-628-B-0817: livello con prezzo compatibile e size ASSENTE = liquidita' "
                                       "illimitata, ordine abbinato per intero (matched=requested): contraddice "
                                       "PF-OD12-12 (nessun abbinamento garantito)")
def test_trg_628_b_0817_absent_size_no_guaranteed_fill():
    """ID: TRG-628-B-0817 / PF-B1-SIM-ZERO-DEPTH (residuo)

    Profondita' non dichiarata non e' liquidita' illimitata: l'ordine non deve risultare abbinato per intero."""
    b = _broker()
    b.update_market_book(_book_un_livello({"price": 2.0}))
    rep = _report(b.place_bet(market_id="1.50", selection_id=1, side="BACK", price=2.0, size=10.0))
    assert rep["sizeMatched"] < 10.0


# ---------------------------------------------------------------------------
def test_trg_628_b_0818():
    """ID: TRG-628-B-0818 / PF-B1-SIM-PARTIAL

    Con partial_fill abilitato il fill e' limitato alla size positiva disponibile (4 su 10 richiesti, resto a
    riposo); con partial_fill disabilitato e size insufficiente nessun fill parziale (ordine a riposo)."""
    b = _broker(partial_fill_enabled=True)
    b.update_market_book(_book_un_livello({"price": 2.0, "size": 4.0}))
    rep = _report(b.place_bet(market_id="1.50", selection_id=1, side="BACK", price=2.0, size=10.0))
    assert rep["sizeMatched"] == 4.0 and rep["orderStatus"] == "EXECUTABLE"

    full = _broker(partial_fill_enabled=True)
    full.update_market_book(_book_un_livello({"price": 2.0, "size": 40.0}))
    rep_full = _report(full.place_bet(market_id="1.50", selection_id=1, side="BACK", price=2.0, size=10.0))
    assert rep_full["sizeMatched"] == 10.0 and rep_full["orderStatus"] == "EXECUTION_COMPLETE"

    nop = _broker(partial_fill_enabled=False)
    nop.update_market_book(_book_un_livello({"price": 2.0, "size": 4.0}))
    rep_nop = _report(nop.place_bet(market_id="1.50", selection_id=1, side="BACK", price=2.0, size=10.0))
    assert rep_nop["sizeMatched"] == 0.0 and rep_nop["orderStatus"] == "EXECUTABLE"


# ---------------------------------------------------------------------------
def _chiuso(vincitore: int) -> Dict[str, Any]:
    return _book(status="CLOSED", runners=[
        {"selectionId": 1, "status": "WINNER" if vincitore == 1 else "LOSER"},
        {"selectionId": 2, "status": "WINNER" if vincitore == 2 else "LOSER"},
    ])


def test_trg_628_b_0820():
    """ID: TRG-628-B-0820 / PF-B1-SIM-SETTLEMENT-DEDUPE

    Il primo book CLOSED regola il mercato (SETTLED); un secondo book CLOSED restituisce DUPLICATE con lo stesso
    record e non tocca saldo, realizzato ne' registro dei settlement."""
    b = _broker()
    b.update_market_book(_book())
    b.place_bet(market_id="1.50", selection_id=1, side="BACK", price=2.0, size=10.0)
    primo = b.update_market_book(_chiuso(1))
    assert primo["settlement_status"] == "SETTLED"
    saldo, realizzato = b.state.balance, b.state.realized_pnl
    secondo = b.update_market_book(_chiuso(2))
    assert secondo["settlement_status"] == "DUPLICATE"
    assert secondo["settlement"]["settlement_id"] == primo["settlement"]["settlement_id"]
    assert b.state.balance == saldo and b.state.realized_pnl == realizzato
    assert len(b.list_settlements()) == 1


def test_trg_628_b_0822():
    """ID: TRG-628-B-0822 / PF-B1-SIM-MARKET-NET

    LF-01: il lordo si aggrega per mercato PRIMA della commissione (una sola volta sul netto di mercato) e
    l'accredito e' netto meno la cassa gia' mossa dai fill. Esempio: BACK 10@2.0 vince (+10), BACK 10@3.0 perde
    (-10) => lordo 0, commissione 0, saldo finale 1000 (per-bet sarebbe -0.45). Solo il vincente: lordo 10,
    commissione 4.5% = 0.45, netto 9.55, saldo 1009.55."""
    b = _broker()
    b.update_market_book(_book())
    b.place_bet(market_id="1.50", selection_id=1, side="BACK", price=2.0, size=10.0)
    b.place_bet(market_id="1.50", selection_id=2, side="BACK", price=3.0, size=10.0)
    assert b.state.balance == pytest.approx(980.0)          # cassa dei fill: due stake BACK
    rec = b.update_market_book(_chiuso(1))["settlement"]
    assert rec["gross_pnl"] == pytest.approx(0.0)
    assert rec["commission_amount"] == pytest.approx(0.0)
    assert rec["net_pnl"] == pytest.approx(0.0)
    assert rec["fill_cash"] == pytest.approx(-20.0)
    assert rec["balance_credit"] == pytest.approx(20.0)
    assert b.state.balance == pytest.approx(1000.0)

    w = _broker()
    w.update_market_book(_book())
    w.place_bet(market_id="1.50", selection_id=1, side="BACK", price=2.0, size=10.0)
    rec_w = w.update_market_book(_chiuso(1))["settlement"]
    assert rec_w["gross_pnl"] == pytest.approx(10.0)
    assert rec_w["commission_amount"] == pytest.approx(0.45)
    assert rec_w["net_pnl"] == pytest.approx(9.55)
    assert w.state.balance == pytest.approx(1009.55)
