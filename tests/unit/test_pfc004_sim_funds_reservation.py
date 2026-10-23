"""PFC-004 (G7-a, roadmap #461; decisione owner #531): il SimulationBroker riserva i fondi come Betfair.

Betfair riserva al piazzamento lo stake di un BACK e la liability di un LAY anche per la parte NON
abbinata, e rifiuta un ordine oltre il disponibile (INSUFFICIENT_FUNDS). Conta pero' il caso peggiore
della posizione: un ordine non abbinato pesa solo dove peggiora la perdita, quindi una copertura
(hedge/cashout) che riduce l'esposizione non chiede fondi in piu'. La riserva si ricava dagli ordini
EXECUTABLE (residuo = size - matched_size): si libera da sola a cancellazione, sostituzione, decadenza
e settlement, e sopravvive al riavvio senza doppi conteggi.
"""
from __future__ import annotations

import pytest

from simulation_broker import SimulationBroker


def _broker(balance=100.0, **kw):
    return SimulationBroker(starting_balance=balance, **kw)


def _book(b, market_id="1.1", sel=1, back=2.0, lay=2.02, size=1000.0):
    b.update_market_book(market_id, {"status": "OPEN", "runners": [{
        "selectionId": sel, "status": "ACTIVE",
        "ex": {"availableToBack": [{"price": back, "size": size}],
               "availableToLay": [{"price": lay, "size": size}]}}]})


def _place(b, side, price, size, market_id="1.1", sel=1, ref=None):
    return b.place_bet(market_id=market_id, selection_id=sel, side=side, price=price, size=size,
                       customer_ref=ref or f"r-{side}-{price}-{size}-{market_id}")


def _report(res):
    return res["instructionReports"][0]


def _funds(b):
    f = b.get_account_funds()
    return round(f["available"], 6), round(f["exposure"], 6), round(f["total"], 6)


# --- riserva e rilascio ---------------------------------------------------------------------------

def test_back_e_lay_non_abbinati_riservano_stake_e_liability():
    b = _broker()
    _place(b, "BACK", 2.0, 60.0)
    assert _funds(b) == (40.0, 60.0, 100.0)
    _place(b, "LAY", 3.0, 10.0, market_id="1.2")  # liability 10 x (3 - 1) = 20
    assert _funds(b) == (20.0, 80.0, 100.0)


def test_parziale_parte_abbinata_nel_ledger_residuo_riservato():
    b = _broker()
    _book(b, back=2.0, size=25.0)
    res = _place(b, "BACK", 2.0, 60.0)
    assert _report(res)["sizeMatched"] == pytest.approx(25.0)
    # 25 abbinati (esposizione del ledger) + 35 riservati: disponibile 40, nessun doppio conteggio
    assert _funds(b) == (40.0, 60.0, 100.0)


def test_lay_parziale_abbinato_a_quota_migliore_riserva_il_residuo_alla_quota_chiesta():
    b = _broker()
    _book(b, lay=3.0, size=5.0)
    _place(b, "LAY", 4.0, 10.0)  # 5 abbinati a 3.0 (liability 10) + 5 non abbinati a 4.0 (riserva 15)
    assert _funds(b) == (75.0, 25.0, 100.0)


def test_cancellazione_totale_e_parziale_rilasciano_la_riserva():
    b = _broker()
    bet = _report(_place(b, "BACK", 2.0, 60.0))["betId"]
    b.cancel_orders(market_id="1.1", instructions=[{"betId": bet, "sizeReduction": 20.0}])
    assert _funds(b) == (60.0, 40.0, 100.0)
    b.cancel_order(bet_id=bet, market_id="1.1")
    assert _funds(b) == (100.0, 0.0, 100.0)


def test_cancellazione_di_tutto_il_mercato_rilascia_la_riserva():
    b = _broker()
    _place(b, "BACK", 2.0, 30.0)
    _place(b, "LAY", 3.0, 10.0)
    b.cancel_orders(market_id="1.1")
    assert _funds(b) == (100.0, 0.0, 100.0)


def test_settlement_fa_decadere_il_non_abbinato_e_libera_la_riserva():
    b = _broker()
    _place(b, "BACK", 2.0, 60.0)
    b.update_market_book("1.1", {"status": "CLOSED", "runners": [
        {"selectionId": 1, "status": "LOSER"}, {"selectionId": 2, "status": "WINNER"}]})
    assert _funds(b) == (100.0, 0.0, 100.0)


def test_la_riserva_sopravvive_al_riavvio_senza_doppio_conteggio():
    b = _broker()
    _book(b, back=2.0, size=25.0)
    _place(b, "BACK", 2.0, 60.0)
    stato = b.state.to_dict()
    nuovo = _broker()
    nuovo.state.load_from_dict(stato)
    assert _funds(nuovo) == _funds(b) == (40.0, 60.0, 100.0)


# --- copertura: nessun fondo in piu' --------------------------------------------------------------

def test_hedge_non_abbinato_che_riduce_l_esposizione_non_chiede_fondi():
    # BACK 40 abbinato a 2.0 (esposizione 40), poi LAY 40 a 3.0 non abbinato: se si abbina la
    # posizione si chiude; il caso peggiore resta 40. Con disponibile 0 la copertura passa lo stesso:
    # un'uscita protettiva non deve essere bloccata dalla riserva (PF-EXEC-03).
    b = _broker(balance=40.0)
    _book(b, back=2.0, lay=3.5, size=40.0)  # LAY a 3.0 < miglior lay 3.5: resta non abbinato
    assert _report(_place(b, "BACK", 2.0, 40.0))["sizeMatched"] == pytest.approx(40.0)
    assert _funds(b)[0] == pytest.approx(0.0)
    res = _place(b, "LAY", 3.0, 40.0, ref="hedge")
    assert res["status"] == "SUCCESS" and _report(res)["status"] == "SUCCESS"
    assert _funds(b) == (0.0, 40.0, 40.0)


def test_hedge_oltre_la_posizione_riserva_solo_l_eccedenza():
    # BACK 10 abbinato (perdita se perde: 10; se vince: -10). LAY 30 a 3.0 non abbinato: se vince
    # la perdita peggiore e' 30 x 2 - 10 = 50, se perde -10 + ... : caso peggiore 50 -> riserva 40.
    b = _broker()
    _book(b, back=2.0, lay=3.5, size=10.0)
    _place(b, "BACK", 2.0, 10.0)
    _place(b, "LAY", 3.0, 30.0, ref="hedge-oltre")
    assert _funds(b) == (50.0, 50.0, 100.0)


# --- INSUFFICIENT_FUNDS ---------------------------------------------------------------------------

def test_oltre_il_disponibile_rifiutato_senza_toccare_niente():
    b = _broker()
    _place(b, "BACK", 2.0, 60.0)
    prima = (_funds(b), len(b.state.orders))
    res = _place(b, "BACK", 2.0, 60.0, market_id="1.2")
    assert res["status"] == "FAILURE" and res["errorCode"] == "INSUFFICIENT_FUNDS"
    assert _report(res)["status"] == "FAILURE" and _report(res)["errorCode"] == "INSUFFICIENT_FUNDS"
    assert _report(res)["betId"] == ""
    assert (_funds(b), len(b.state.orders)) == prima


def test_lay_con_liability_oltre_il_disponibile_rifiutato():
    b = _broker()
    res = _place(b, "LAY", 11.0, 10.5)  # liability 105 > 100
    assert res["errorCode"] == "INSUFFICIENT_FUNDS"
    assert _report(_place(b, "LAY", 11.0, 10.0))["status"] == "SUCCESS"  # liability 100 = disponibile
    assert _funds(b) == (0.0, 100.0, 100.0)


def test_rifiuto_anche_se_l_ordine_si_abbinerebbe_subito():
    b = _broker()
    _book(b, back=2.0, size=1000.0)
    res = _place(b, "BACK", 2.0, 150.0)
    assert res["errorCode"] == "INSUFFICIENT_FUNDS"
    assert _funds(b) == (100.0, 0.0, 100.0) and b.state.position_ledgers == {}


def test_order_manager_tratta_il_rifiuto_come_definitivo():
    from order_manager import map_betfair_status, OrderStatus
    b = _broker()
    res = _place(b, "BACK", 2.0, 150.0)
    stato, _rc = map_betfair_status(_report(res)["status"], res["status"], 0.0, 150.0)
    assert stato == OrderStatus.FAILED  # mai AMBIGUOUS: niente e' stato piazzato


# --- batch: tutto o niente ------------------------------------------------------------------------

def test_batch_oltre_il_disponibile_non_piazza_nessuna_gamba():
    b = _broker()
    res = b.place_orders(market_id="1.1", customer_ref="dutch-1", instructions=[
        {"selection_id": 1, "side": "BACK", "price": 3.0, "size": 60.0},
        {"selection_id": 2, "side": "BACK", "price": 3.0, "size": 60.0},
    ])
    assert res["status"] == "FAILURE" and res["errorCode"] == "INSUFFICIENT_FUNDS"
    assert [r["errorCode"] for r in res["instructionReports"]] == ["INSUFFICIENT_FUNDS"] * 2
    assert b.state.orders == {} and _funds(b) == (100.0, 0.0, 100.0)


def test_batch_nei_fondi_piazza_tutte_le_gambe():
    b = _broker()
    res = b.place_orders(market_id="1.1", customer_ref="dutch-2", instructions=[
        {"selection_id": 1, "side": "BACK", "price": 3.0, "size": 50.0},
        {"selection_id": 2, "side": "BACK", "price": 3.0, "size": 50.0},
    ])
    assert [r["status"] for r in res["instructionReports"]] == ["SUCCESS", "SUCCESS"]
    assert _funds(b) == (0.0, 100.0, 100.0)


# --- sostituzione -----------------------------------------------------------------------------------

def test_replace_lay_a_quota_piu_alta_oltre_i_fondi_rifiutato_e_l_originale_resta():
    b = _broker()
    bet = _report(_place(b, "LAY", 3.0, 40.0))["betId"]  # liability 80
    res = b.replace_orders(market_id="1.1", bet_id=bet, new_price=4.0)  # liability 120 > 100
    assert _report(res)["status"] == "FAILURE" and _report(res)["errorCode"] == "INSUFFICIENT_FUNDS"
    assert b.state.orders[bet].status == "EXECUTABLE" and _funds(b) == (20.0, 80.0, 100.0)


def test_replace_nei_fondi_sposta_la_riserva_senza_doppio_conteggio():
    b = _broker()
    bet = _report(_place(b, "LAY", 3.0, 40.0))["betId"]
    res = b.replace_orders(market_id="1.1", bet_id=bet, new_price=3.5)  # liability 100
    assert _report(res)["status"] == "SUCCESS"
    assert b.state.orders[bet].status == "CANCELLED" and _funds(b) == (0.0, 100.0, 100.0)
