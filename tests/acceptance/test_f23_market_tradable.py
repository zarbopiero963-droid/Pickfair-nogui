"""F-23 (#586, NF-11/STR-10/SUP-04) — nessun nuovo ingresso su un mercato non negoziabile (PF-EXEC-05 all'ammissione).

Decisione owner PF-EXEC-05 (#426 c.6235676826): mercato sospeso / feed / disconnessione = nessun ingresso, nessun
riprezzamento, nessun retry, riconciliazione.

Su `main` `ada4076`:
- segnale e auto-next controllavano lo stato del mercato solo dentro il Liquidity Guard: con il guard disattivato
  (`liquidity_guard_enabled=False`) un mercato SUSPENDED, CLOSED o un book assente passavano;
- bet manuale e lotto Dutching usavano il book in CACHE (`market_tracker`) senza guardarne lo stato: un mercato
  SUSPENDED in cache passava anche con il guard attivo (e senza book in cache, in solo-avviso, nessun blocco).

Ora una lettura FRESCA del book all'ammissione (fuori dal lock, come i fondi) decide la negoziabilita':
mercato OPEN, selezioni presenti e ACTIVE, book del mercato richiesto; lettura fallita/assente => non verificabile =>
nessun ingresso. Nessuna soglia d'eta' nuova: la lettura e' contemporanea all'ammissione. Uscite protettive (hedge
del cashout) non sono toccate (PF-EXEC-03). La corsa fra verifica e invio resta all'exchange: un mercato sospeso
nel frattempo e' un rifiuto esplicito di Betfair (FAILURE), gia' gestito come esito certo.
"""
from __future__ import annotations

import pytest

from core import liquidity
from core.risk_gate import RiskGate

MID = "1.555"


def _book(status="OPEN", runners=((11, "ACTIVE"), (12, "ACTIVE")), market_id=MID):
    out = {"status": status, "runners": [{"selectionId": s, "status": st} for s, st in runners]}
    if market_id is not None:
        out["marketId"] = market_id
    return out


# ------------------------------------------------------------------ regola pura
@pytest.mark.parametrize("book,selezioni,atteso", [
    (_book(), [11], ""),
    (_book(), [11, 12], ""),
    (_book(market_id=None), [11], ""),                                  # marketId assente: si fida della richiesta
    (_book("SUSPENDED"), [11], "mercato_non_negoziabile:SUSPENDED"),
    (_book("CLOSED"), [11], "mercato_non_negoziabile:CLOSED"),
    (_book("INACTIVE"), [11], "mercato_non_negoziabile:INACTIVE"),
    ({"runners": [{"selectionId": 11, "status": "ACTIVE"}]}, [11], "mercato_non_negoziabile:STATUS_MISSING"),
    (_book(runners=((11, "REMOVED"), (12, "ACTIVE"))), [11], "selezione_non_negoziabile:11:REMOVED"),
    (_book(runners=((11, "ACTIVE"), (12, "WINNER"))), [11, 12], "selezione_non_negoziabile:12:WINNER"),
    (_book(runners=((12, "ACTIVE"),)), [11], "selezione_non_negoziabile:11:ABSENT"),
    (_book(runners=((11, None),)), [11], "selezione_non_negoziabile:11:STATUS_MISSING"),
    (None, [11], "mercato_non_verificabile:BOOK_UNAVAILABLE"),
    ({}, [11], "mercato_non_verificabile:BOOK_UNAVAILABLE"),
    ({"status": "OPEN", "runners": "x"}, [11], "mercato_non_verificabile:BOOK_UNAVAILABLE"),
    (_book(market_id="1.999"), [11], "mercato_non_verificabile:BOOK_MISMATCH"),
    (_book(), [], "mercato_non_verificabile:SELECTION_MISSING"),
    (_book(), ["abc"], "mercato_non_verificabile:SELECTION_MISSING"),
    (_book(), [True], "mercato_non_verificabile:SELECTION_MISSING"),
])
def test_regola_di_negoziabilita(book, selezioni, atteso: str) -> None:
    assert liquidity.market_tradable_reason(book, MID, selezioni) == atteso


# ------------------------------------------------------------------ RiskGate: ingresso negato, uscita protettiva no
def _gate():
    return RiskGate()                      # config di produzione (trading_config), come in headless_main


def _entry(**extra):
    return {"market_id": MID, "selection_id": 11, "bet_type": "BACK", "price": 2.0, "stake": 5.0, **extra}


def test_block_risk_gate_nega_un_ingresso_su_mercato_non_negoziabile() -> None:
    gate = _gate()
    out = gate._check(_entry(market_tradability_required=True,
                             market_not_tradable_reason="mercato_non_negoziabile:SUSPENDED"))
    assert out["allowed"] is False and out["reason"] == "RISK_MARKET_NOT_TRADABLE", out


@pytest.mark.parametrize("motivo", [None, 0, ["x"]])
def test_block_risk_gate_nega_se_l_esito_della_verifica_manca(motivo) -> None:
    gate = _gate()
    out = gate._check(_entry(market_tradability_required=True, market_not_tradable_reason=motivo))
    assert out["allowed"] is False and out["reason"] == "RISK_MARKET_NOT_TRADABLE", out


def test_risk_gate_ammette_un_ingresso_su_mercato_negoziabile() -> None:
    gate = _gate()
    out = gate._check(_entry(market_tradability_required=True, market_not_tradable_reason=""))
    assert out["allowed"] is True, out


def test_risk_gate_non_tocca_le_uscite_protettive() -> None:
    """Hedge del cashout e uscite protettive non portano la richiesta di verifica: mai ritardate (PF-EXEC-03)."""
    gate = _gate()
    out = gate._check(_entry())
    assert out["allowed"] is True, out


# ------------------------------------------------------------------ runtime vero (HeadlessApp, RiskGate cablato)
GUARD_OFF = {"roserpina.liquidity_guard_enabled": False}


def _riga(snap, m):
    rows = [x for x in snap["orders"] if x["market_id"] == m]
    return rows[-1] if rows else None


@pytest.mark.safety
def test_block_runtime_vero_mercato_sospeso_con_guard_spento_nessun_ingresso() -> None:
    import os, sys
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "f026"))
    from test_f026_financial import run

    r = run([{"op": "mode", "m": "1.931", "mode": "suspended"}, {"op": "signal", "m": "1.931", "price": 2.0}],
            settings=GUARD_OFF)
    riga = _riga(r["snaps"][-1], "1.931")
    assert riga is not None and riga["status"] == "DENIED" and riga["reason"] == "RISK_MARKET_NOT_TRADABLE", riga


@pytest.mark.safety
def test_block_runtime_vero_book_assente_con_guard_spento_nessun_ingresso() -> None:
    import os, sys
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "f026"))
    from test_f026_financial import run

    r = run([{"op": "book_down"}, {"op": "signal", "m": "1.932", "price": 2.0}], settings=GUARD_OFF)
    riga = _riga(r["snaps"][-1], "1.932")
    assert riga is not None and riga["status"] == "DENIED" and riga["reason"] == "RISK_MARKET_NOT_TRADABLE", riga


@pytest.mark.safety
def test_runtime_vero_mercato_aperto_con_guard_spento_parte() -> None:
    import os, sys
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "f026"))
    from test_f026_financial import run

    r = run([{"op": "signal", "m": "1.933", "price": 2.0}], settings=GUARD_OFF)
    assert _riga(r["snaps"][-1], "1.933")["status"] == "SUBMITTED"


# ------------------------------------------------------------------ bet manuale e lotto Dutching (controller reale)
def _catena_con_book(tmp_path, book_per_mercato):
    from tests.acceptance.test_issue437_pr27 import _catena

    c = _catena(tmp_path)
    c.rc.betfair_service.get_account_funds = lambda: {"available": 1000.0, "exposure": 0.0, "total": 1000.0,
                                                      "simulated": True}
    letture = []

    def _snapshot(market_id, include_prices=False):
        letture.append(market_id)
        b = book_per_mercato(market_id)
        if isinstance(b, Exception):
            raise b
        return b

    c.rc.betfair_service.get_market_book_snapshot = _snapshot
    c.letture_book = letture
    return c


def _book_manuale(status="OPEN", sel_status="ACTIVE"):
    return lambda mid: {"marketId": mid, "status": status,
                        "runners": [{"selectionId": 11, "status": sel_status}, {"selectionId": 12, "status": "ACTIVE"}]}


def _book_lotto(status="OPEN"):
    return lambda mid: {"marketId": mid, "status": status,
                        "runners": [{"selectionId": s, "status": "ACTIVE"} for s in (10, 20, 30)]}


@pytest.mark.parametrize("book,atteso", [
    (_book_manuale("SUSPENDED"), "mercato_non_negoziabile:SUSPENDED"),
    (_book_manuale("CLOSED"), "mercato_non_negoziabile:CLOSED"),
    (_book_manuale(sel_status="REMOVED"), "selezione_non_negoziabile:11:REMOVED"),
    (lambda mid: None, "mercato_non_verificabile:BOOK_UNAVAILABLE"),
    (lambda mid: TimeoutError("read timed out"), "mercato_non_verificabile:BOOK_UNAVAILABLE"),
])
def test_block_bet_manuale_su_mercato_non_negoziabile_senza_effetti(tmp_path, book, atteso: str) -> None:
    from tests.acceptance.test_issue523_dutching_manual_bet import _dutching, _manual, _no_side_effects

    c = _catena_con_book(tmp_path, book)
    res = _dutching(c).manual_bet(_manual())

    assert res["ok"] is False and res["reason"] == atteso, res
    _no_side_effects(c)


def test_bet_manuale_su_mercato_negoziabile_parte_e_legge_il_book_fresco(tmp_path) -> None:
    from tests.acceptance.test_issue523_dutching_manual_bet import _dutching, _manual

    c = _catena_con_book(tmp_path, _book_manuale())
    res = _dutching(c).manual_bet(_manual(stake=4.0))

    assert res["ok"] is True, res
    assert c.letture_book == ["1.234"], "il book va letto fresco all'ingresso, una volta"


@pytest.mark.parametrize("book,atteso", [
    (_book_lotto("SUSPENDED"), "mercato_non_negoziabile:SUSPENDED"),
    (lambda mid: {"marketId": mid, "status": "OPEN",
                  "runners": [{"selectionId": 10, "status": "ACTIVE"}, {"selectionId": 20, "status": "REMOVED"}]},
     "selezione_non_negoziabile:20:REMOVED"),
    (lambda mid: None, "mercato_non_verificabile:BOOK_UNAVAILABLE"),
])
def test_block_lotto_dutching_su_mercato_non_negoziabile_rifiutato_intero(tmp_path, book, atteso: str) -> None:
    from tests.acceptance.test_issue523_dutching_manual_bet import _batch, _dutching, _no_side_effects

    c = _catena_con_book(tmp_path, book)
    res = _dutching(c).submit_dutching(_batch())

    assert res["ok"] is False and res.get("reason") == atteso, res
    _no_side_effects(c)


def test_lotto_dutching_su_mercato_negoziabile_parte(tmp_path) -> None:
    from tests.acceptance.test_issue523_dutching_manual_bet import _batch, _dutching

    c = _catena_con_book(tmp_path, _book_lotto())
    assert _dutching(c).submit_dutching(_batch(total_stake=10.0))["ok"] is True


def test_block_runtime_senza_la_verifica_di_negoziabilita_nega(tmp_path) -> None:
    """Fail-closed: un runtime che legge il book ma non espone la verifica non ammette ingressi del controller."""
    from tests.acceptance.test_issue523_dutching_manual_bet import _dutching, _manual, _no_side_effects

    c = _catena_con_book(tmp_path, _book_manuale())
    c.rc.controller_market_tradable_reason = None
    res = _dutching(c).manual_bet(_manual())

    assert res["ok"] is False and res["reason"] == "mercato_non_verificabile:BOOK_UNAVAILABLE", res
    _no_side_effects(c)


def test_auto_next_porta_l_esito_della_verifica_al_risk_gate() -> None:
    """Auto-next: la richiesta inviata porta la verifica di negoziabilita' fatta sul book fresco (il RiskGate la nega)."""
    from tests.acceptance.test_issue461_pr28e_funds_gate import _auto_next

    _rc, bus, esito = _auto_next({"market_id": "1.2", "selection_id": 2, "price": 6.0, "stake": 2.0,
                                  "bet_type": "LAY"}, funds={"available": 150.0, "exposure": 0.0})
    inviati = [p for e, p in bus.events if e == "CMD_QUICK_BET"]
    assert len(inviati) == 1, esito
    assert inviati[0]["market_tradability_required"] is True
    assert isinstance(inviati[0]["market_not_tradable_reason"], str)


# ------------------------------------------------------------------ sospensione FRA verifica e invio (parita' SIM/LIVE)
def _broker_con_book(status="OPEN", runner_status="ACTIVE"):
    from simulation_broker import SimulationBroker

    b = SimulationBroker(starting_balance=1000.0)
    b.update_market_book({"marketId": MID, "status": status, "inplay": False, "runners": [
        {"selectionId": 11, "status": runner_status,
         "ex": {"availableToBack": [{"price": 2.0, "size": 100.0}], "availableToLay": [{"price": 2.02, "size": 100.0}]}},
        {"selectionId": 12, "status": "ACTIVE",
         "ex": {"availableToBack": [{"price": 3.0, "size": 100.0}], "availableToLay": [{"price": 3.05, "size": 100.0}]}}]})
    return b


@pytest.mark.parametrize("status,codice", [("SUSPENDED", "MARKET_SUSPENDED"), ("CLOSED", "MARKET_NOT_OPEN_FOR_BETTING"),
                                            ("INACTIVE", "MARKET_NOT_OPEN_FOR_BETTING")])
def test_block_broker_sim_rifiuta_come_betfair_su_mercato_non_aperto(status: str, codice: str) -> None:
    """Betfair rifiuta placeOrders su un mercato non OPEN (FAILURE, nessun ordine): il SIM deve fare lo stesso,
    altrimenti in SIM una sospensione fra il controllo F-23 e l'invio produrrebbe un ordine inesistente in LIVE."""
    b = _broker_con_book(status)
    fondi_prima = b.get_account_funds()["available"]

    out = b.place_bet(market_id=MID, selection_id=11, side="BACK", price=2.0, size=5.0)

    assert out["status"] == "FAILURE" and out["errorCode"] == codice, out
    assert out["instructionReports"][0]["status"] == "FAILURE"
    assert b.state.orders == {}, "ordine registrato su mercato non aperto"
    assert b.get_account_funds()["available"] == fondi_prima


def test_block_broker_sim_rifiuta_selezione_non_attiva() -> None:
    b = _broker_con_book(runner_status="REMOVED")

    out = b.place_bet(market_id=MID, selection_id=11, side="BACK", price=2.0, size=5.0)

    assert out["status"] == "FAILURE" and out["instructionReports"][0]["errorCode"] == "RUNNER_REMOVED", out
    assert b.state.orders == {}


def test_block_batch_sim_su_mercato_sospeso_rifiutato_intero() -> None:
    b = _broker_con_book("SUSPENDED")

    out = b.place_orders(market_id=MID, instructions=[
        {"selectionId": 11, "side": "BACK", "price": 2.0, "size": 5.0},
        {"selectionId": 12, "side": "BACK", "price": 3.0, "size": 5.0}])

    assert out["status"] == "FAILURE" and out["errorCode"] == "MARKET_SUSPENDED", out
    assert len(out["instructionReports"]) == 2 and all(r["status"] == "FAILURE" for r in out["instructionReports"])
    assert b.state.orders == {}


def test_broker_sim_mercato_aperto_accetta_come_prima() -> None:
    b = _broker_con_book()

    out = b.place_bet(market_id=MID, selection_id=11, side="BACK", price=2.0, size=5.0)

    assert out["status"] == "SUCCESS", out


@pytest.mark.safety
def test_block_runtime_vero_sospensione_fra_verifica_e_invio_nessun_ordine_vivo() -> None:
    """Runtime vero: il book e' OPEN alla verifica F-23, il mercato viene sospeso subito prima del placeOrders.
    Esito atteso come su Betfair: rifiuto esplicito dell'exchange (FAILED, esito certo), nessun ordine vivo."""
    import os, sys
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "f026"))
    from test_f026_financial import run

    r = run([{"op": "mode", "m": "1.934", "mode": "suspend_on_place"}, {"op": "signal", "m": "1.934", "price": 2.0}],
            settings=GUARD_OFF)
    riga = _riga(r["snaps"][-1], "1.934")
    assert riga is not None and riga["status"] == "FAILED", riga                 # rifiuto certo dell'exchange
    assert not any(o["market_id"] == "1.934" and o["status"] in ("SUBMITTED", "AMBIGUOUS")
                   for o in r["snaps"][-1]["orders"]), "ordine vivo su un mercato sospeso"
    assert all(t["market_id"] != "1.934" for t in r["snaps"][-1]["tables"] if t.get("event_key")), \
        "tavolo trattenuto da un rifiuto certo dell'exchange"


def test_broker_sim_con_stato_assente_non_inventa_un_rifiuto() -> None:
    """Il broker fa la parte dell'exchange: uno stato assente nel book caricato e' «non noto», non «chiuso».
    Il rifiuto di un book senza stato spetta all'ammissione F-23 (STATUS_MISSING)."""
    from simulation_broker import SimulationBroker

    b = SimulationBroker(starting_balance=1000.0)
    b.update_market_book({"marketId": MID, "runners": [{"selectionId": 11, "ex": {
        "availableToBack": [{"price": 2.0, "size": 100.0}], "availableToLay": [{"price": 2.02, "size": 100.0}]}}]})

    assert b.place_bet(market_id=MID, selection_id=11, side="BACK", price=2.0, size=5.0)["status"] == "SUCCESS"
