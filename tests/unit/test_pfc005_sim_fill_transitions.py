"""PFC-005 (G7-b, roadmap #461; decisione owner #531): transizioni dei fondi SIM senza doppio conteggio.

- Un ordine a riposo si abbina quando un book APERTO successivo lo raggiunge (anche in piu' volte):
  la riserva diventa esposizione abbinata, il disponibile non cambia, ogni fill entra nel registro
  cronologico persistito e il settlement lo ripercorre.
- Il disponibile e' cassa meno caso peggiore del MERCATO: niente disponibile negativo dalla
  compensazione del PositionLedger a quote diverse, e su un mercato a vincitore unico un Dutching LAY
  piazzato gamba per gamba riserva la liability peggiore, non la somma.
"""
from __future__ import annotations

import json

import pytest

from simulation_broker import SimulationBroker


def _broker(balance=100.0, **kw):
    kw.setdefault("commission_pct", 4.5)
    return SimulationBroker(starting_balance=balance, **kw)


def _book(b, market_id="1.1", runners=None, status="OPEN", winners=None):
    runners = runners or {1: (2.0, 2.02, 1000.0)}
    book = {"status": status, "runners": [
        {"selectionId": sel, "status": "ACTIVE",
         "ex": {"availableToBack": [{"price": back, "size": size}],
                "availableToLay": [{"price": lay, "size": size}]}}
        for sel, (back, lay, size) in runners.items()]}
    if winners is not None:
        book["numberOfWinners"] = winners
    return b.update_market_book(market_id, book)


def _chiuso(b, market_id, esiti):
    return b.update_market_book(market_id, {"status": "CLOSED", "runners": [
        {"selectionId": sel, "status": st} for sel, st in esiti.items()]})


def _place(b, side, price, size, market_id="1.1", sel=1):
    res = b.place_bet(market_id=market_id, selection_id=sel, side=side, price=price, size=size)
    return res, res["instructionReports"][0]


def _funds(b):
    f = b.get_account_funds()
    return round(f["available"], 6), round(f["exposure"], 6), round(f["total"], 6)


# --- abbinamento differito ------------------------------------------------------------------------

def test_ordine_a_riposo_si_abbina_quando_il_book_lo_raggiunge():
    b = _broker()
    _book(b, runners={1: (1.8, 1.82, 1000.0)})
    _, rep = _place(b, "BACK", 2.0, 60.0)
    assert rep["sizeMatched"] == 0.0 and _funds(b) == (40.0, 60.0, 100.0)
    _book(b, runners={1: (2.1, 2.12, 1000.0)})
    order = b.state.orders[rep["betId"]]
    assert order.status == "EXECUTION_COMPLETE" and order.matched_size == pytest.approx(60.0)
    assert order.avg_price_matched == pytest.approx(2.1)  # prezzo del book, come al piazzamento
    # la riserva diventa esposizione abbinata: nessun doppio conteggio
    assert _funds(b) == (40.0, 60.0, 100.0)
    assert b.get_account_funds()["reserved_unmatched"] == pytest.approx(0.0)
    assert [f["fill_id"] for f in b.state.fills] == [rep["betId"]]


def test_parziale_poi_completato_con_due_fill_e_quota_media():
    b = _broker()
    _book(b, runners={1: (2.0, 2.02, 25.0)})
    _, rep = _place(b, "BACK", 2.0, 60.0)
    assert rep["sizeMatched"] == pytest.approx(25.0)
    _book(b, runners={1: (2.2, 2.22, 100.0)})
    order = b.state.orders[rep["betId"]]
    assert order.matched_size == pytest.approx(60.0) and order.status == "EXECUTION_COMPLETE"
    assert order.avg_price_matched == pytest.approx((25 * 2.0 + 35 * 2.2) / 60)
    assert [f["fill_id"] for f in b.state.fills] == [rep["betId"], rep["betId"] + "#2"]
    assert _funds(b) == (40.0, 60.0, 100.0)


def test_liquidita_parziale_lascia_il_residuo_a_riposo():
    b = _broker()
    _book(b, runners={1: (3.4, 3.5, 1000.0)})
    _, rep = _place(b, "LAY", 3.0, 10.0)  # miglior lay 3.5 > 3.0: a riposo, liability 20
    _book(b, runners={1: (2.9, 2.5, 4.0)})  # miglior lay 2.5 <= 3.0: 4 abbinati a 2.5
    order = b.state.orders[rep["betId"]]
    assert order.matched_size == pytest.approx(4.0) and order.status == "EXECUTABLE"
    # 4 x 1.5 = 6 abbinati + 6 x 2 = 12 riservati: la quota migliore libera fondi, mai il contrario
    assert _funds(b) == (82.0, 18.0, 100.0)


@pytest.mark.parametrize("variante", ["SUSPENDED", "senza_stato", "runner_sospeso"])
def test_nessun_abbinamento_differito_su_mercato_sospeso_o_runner_non_attivo(variante):
    b = _broker()
    _book(b, runners={1: (1.8, 1.82, 1000.0)})
    _, rep = _place(b, "BACK", 2.0, 60.0)
    book = {"status": "OPEN", "runners": [{"selectionId": 1, "status": "ACTIVE", "ex": {
        "availableToBack": [{"price": 2.5, "size": 1000.0}], "availableToLay": [{"price": 2.52, "size": 1000.0}]}}]}
    if variante == "SUSPENDED":
        book["status"] = "SUSPENDED"
    elif variante == "senza_stato":
        del book["status"]
    else:
        book["runners"][0]["status"] = "REMOVED"
    b.update_market_book("1.1", book)
    assert b.state.orders[rep["betId"]].matched_size == 0.0 and b.state.fills == []


# --- settlement e riavvio dopo fill multipli ------------------------------------------------------

def test_settlement_dopo_due_fill_regola_sulla_quota_media():
    b = _broker(balance=1000.0)
    _book(b, runners={1: (2.0, 2.02, 25.0)})
    _place(b, "BACK", 2.0, 60.0)
    _book(b, runners={1: (2.2, 2.22, 100.0)})
    esito = _chiuso(b, "1.1", {1: "WINNER"})
    assert esito["settlement_status"] == "SETTLED"
    lordo = 25 * 1.0 + 35 * 1.2
    assert esito["settlement"]["gross_pnl"] == pytest.approx(lordo)
    assert _funds(b) == (round(1000.0 + lordo * 0.955, 6), 0.0, round(1000.0 + lordo * 0.955, 6))


def test_fill_differiti_sopravvivono_al_riavvio_e_il_settlement_torna():
    b = _broker(balance=1000.0)
    _book(b, runners={1: (2.0, 2.02, 25.0)})
    _place(b, "BACK", 2.0, 60.0)
    _book(b, runners={1: (2.2, 2.22, 100.0)})
    stato = json.loads(json.dumps(b.state.to_dict()))
    nuovo = _broker(balance=1000.0)
    nuovo.state.load_from_dict(stato)
    assert nuovo.state.fills == b.state.fills and _funds(nuovo) == _funds(b) == (940.0, 60.0, 1000.0)
    _chiuso(nuovo, "1.1", {1: "LOSER"})
    assert _funds(nuovo) == (940.0, 0.0, 940.0)


def test_stato_precedente_senza_registro_dei_fill_si_ricostruisce_dagli_ordini():
    b = _broker(balance=1000.0)
    _book(b, runners={1: (3.0, 3.1, 100.0)})
    _place(b, "BACK", 3.0, 10.0)
    stato = json.loads(json.dumps(b.state.to_dict()))
    del stato["fills"]  # formato salvato prima di PFC-005
    nuovo = _broker(balance=1000.0)
    nuovo.state.load_from_dict(stato)
    assert len(nuovo.state.fills) == 1 and _funds(nuovo) == (990.0, 10.0, 1000.0)
    _chiuso(nuovo, "1.1", {1: "WINNER"})
    assert _funds(nuovo)[0] == pytest.approx(1000.0 + 20.0 * 0.955)


def test_registro_dei_fill_incoerente_blocca_ordini_e_settlement():
    b = _broker(balance=1000.0)
    _book(b, runners={1: (3.0, 3.1, 100.0)})
    _place(b, "BACK", 3.0, 10.0)
    b.state.fills.clear()  # registro perso: la cassa non e' ricostruibile
    res, _ = _place(b, "BACK", 3.0, 1.0, market_id="1.2")
    assert res["errorCode"] == "INSUFFICIENT_FUNDS"
    fondi = b.get_account_funds()
    assert fondi["available"] == 0.0 and fondi["error"] == "FILL_LEDGER_INCONSISTENT"  # non "zero valido"
    assert _chiuso(b, "1.1", {1: "WINNER"})["settlement_status"] == "LEDGER_INCONSISTENT"


@pytest.mark.parametrize("guasto", [{"size": None}, {"price": "x"}, {"selection_id": None}, {"side": "PIPPO"}])
def test_fill_persistito_malformato_e_un_rifiuto_pulito_non_un_eccezione(guasto):
    b = _broker(balance=1000.0)
    _book(b, runners={1: (3.0, 3.1, 100.0)})
    _place(b, "BACK", 3.0, 10.0)
    stato = json.loads(json.dumps(b.state.to_dict()))
    for chiave, valore in guasto.items():
        if valore is None:
            del stato["fills"][0][chiave]
        else:
            stato["fills"][0][chiave] = valore
    nuovo = _broker(balance=1000.0)
    nuovo.state.load_from_dict(stato)
    assert nuovo.get_account_funds()["error"] == "FILL_LEDGER_INCONSISTENT"
    assert _place(nuovo, "BACK", 3.0, 1.0, market_id="1.2")[0]["errorCode"] == "INSUFFICIENT_FUNDS"
    assert _chiuso(nuovo, "1.1", {1: "WINNER"})["settlement_status"] == "LEDGER_INCONSISTENT"


@pytest.mark.parametrize("registro", [None, {}, "x", 7])
def test_registro_dei_fill_presente_ma_non_lista_non_si_ricostruisce_dagli_ordini(registro):
    # GPT-6.1 Sol su #542: "fills" presente ma non lista non e' uno stato precedente a PFC-005.
    b = _broker(balance=1000.0)
    _book(b, runners={1: (3.0, 3.1, 100.0)})
    _place(b, "BACK", 3.0, 10.0)
    stato = json.loads(json.dumps(b.state.to_dict()))
    stato["fills"] = registro
    nuovo = _broker(balance=1000.0)
    nuovo.state.load_from_dict(stato)
    assert nuovo.state.fills == []
    assert nuovo.get_account_funds()["error"] == "FILL_LEDGER_INCONSISTENT"
    assert _chiuso(nuovo, "1.1", {1: "WINNER"})["settlement_status"] == "LEDGER_INCONSISTENT"


# --- liquidita' gia' consumata ----------------------------------------------------------------------

def test_book_riletto_non_ripresenta_la_liquidita_gia_presa():
    # Review indipendente di PFC-005: BACK 100 su un book da 10 riletto 9 volte non deve abbinarsi tutto.
    b = _broker(balance=1000.0)
    _book(b, runners={1: (2.0, 2.02, 10.0)})
    _, rep = _place(b, "BACK", 2.0, 100.0)
    for _ in range(9):
        _book(b, runners={1: (2.0, 2.02, 10.0)})
    order = b.state.orders[rep["betId"]]
    assert order.matched_size == pytest.approx(10.0) and order.status == "EXECUTABLE"


def test_liquidita_in_crescita_abbina_solo_l_incremento_anche_dopo_il_riavvio():
    b = _broker(balance=1000.0)
    _book(b, runners={1: (2.0, 2.02, 10.0)})
    _, rep = _place(b, "BACK", 2.0, 100.0)
    stato = json.loads(json.dumps(b.state.to_dict()))
    nuovo = _broker(balance=1000.0)
    nuovo.state.load_from_dict(stato)
    _book(nuovo, runners={1: (2.0, 2.02, 30.0)})
    assert nuovo.state.orders[rep["betId"]].matched_size == pytest.approx(30.0)
    _book(nuovo, runners={1: (2.0, 2.02, 30.0)})
    assert nuovo.state.orders[rep["betId"]].matched_size == pytest.approx(30.0)


def test_nuova_quota_migliore_e_liquidita_nuova():
    b = _broker(balance=1000.0)
    _book(b, runners={1: (2.0, 2.02, 10.0)})
    _, rep = _place(b, "BACK", 2.0, 100.0)
    _book(b, runners={1: (2.1, 2.12, 15.0)})  # livello diverso: liquidita' non ancora toccata
    assert b.state.orders[rep["betId"]].matched_size == pytest.approx(25.0)


# --- caso peggiore del mercato --------------------------------------------------------------------

def test_compensazione_a_quote_diverse_non_rende_negativo_il_disponibile():
    # Riproduzione della review indipendente di PFC-004: saldo 10, LAY 1@3.5 e BACK 11@1.5 abbinati.
    # Vince: -2.5 + 5.5 = +3; perde: +1 - 11 = -10. Caso peggiore 10 = cassa: disponibile 0, non -2.
    b = _broker(balance=10.0)
    _book(b, runners={1: (1.5, 3.5, 100.0)})
    _place(b, "LAY", 3.5, 1.0)
    _place(b, "BACK", 1.5, 11.0)
    assert _funds(b) == (0.0, 10.0, 10.0)


def test_dutching_lay_su_mercato_a_vincitore_unico_riserva_la_liability_peggiore():
    # LAY A 10@3 (liability 20) e LAY B 10@4 (liability 30) a riposo: vince A -> -20, vince B -> -30,
    # non vince nessuno dei due -> 0. Servono 30, non 50: la seconda gamba passa.
    b = _broker(balance=30.0)
    _book(b, runners={1: (5.0, 5.2, 100.0), 2: (5.0, 5.2, 100.0)}, winners=1)
    assert _place(b, "LAY", 3.0, 10.0, sel=1)[1]["status"] == "SUCCESS"
    assert _place(b, "LAY", 4.0, 10.0, sel=2)[1]["status"] == "SUCCESS"
    assert _funds(b) == (0.0, 30.0, 30.0)


def test_senza_numero_di_vincitori_le_esposizioni_dei_runner_si_sommano():
    b = _broker(balance=30.0)
    _book(b, runners={1: (5.0, 5.2, 100.0), 2: (5.0, 5.2, 100.0)})  # numberOfWinners assente
    assert _place(b, "LAY", 3.0, 10.0, sel=1)[1]["status"] == "SUCCESS"
    res, _ = _place(b, "LAY", 4.0, 10.0, sel=2)
    assert res["errorCode"] == "INSUFFICIENT_FUNDS"
    assert _funds(b) == (10.0, 20.0, 30.0)


def test_back_su_tutti_i_runner_resta_prudente():
    # Il book puo' non elencare tutti i runner: lo scenario "non vince nessuno dei noti" resta.
    b = _broker()
    _book(b, runners={1: (5.0, 5.2, 100.0), 2: (5.0, 5.2, 100.0)}, winners=1)
    _place(b, "BACK", 6.0, 40.0, sel=1)
    _place(b, "BACK", 6.0, 40.0, sel=2)
    assert _funds(b) == (20.0, 80.0, 100.0)


def test_copertura_passa_anche_con_disponibile_gia_negativo_un_nuovo_rischio_no():
    b = _broker(balance=40.0)
    _book(b, runners={1: (2.0, 3.5, 40.0)})
    _place(b, "BACK", 2.0, 40.0)  # abbinato: esposizione 40, disponibile 0
    b.record_realized_settlement(-10.0, "9.9")  # perdita regolata altrove: disponibile -10
    assert _funds(b)[0] == pytest.approx(-10.0)
    res, rep = _place(b, "LAY", 3.0, 20.0)  # vince: +40 - 40 = 0, perde: -40 -> esposizione invariata
    assert res["status"] == "SUCCESS" and rep["status"] == "SUCCESS"
    res, _ = _place(b, "BACK", 2.0, 1.0, market_id="1.2")
    assert res["errorCode"] == "INSUFFICIENT_FUNDS"


def test_gamba_con_lato_non_stringa_non_pesa_sui_fondi_del_batch():
    b = _broker()
    res = b.place_orders(market_id="1.1", instructions=[
        {"selection_id": 1, "side": "BACK", "price": 3.0, "size": 60.0},
        {"selection_id": 2, "side": "BACK", "bet_type": 123, "price": 3.0, "size": 50.0},
    ])
    assert [r["status"] for r in res["instructionReports"]] == ["SUCCESS", "FAILURE"]
    assert _funds(b) == (40.0, 60.0, 100.0)


def test_livello_esaurito_non_diventa_liquidita_illimitata_per_l_ordine_dopo():
    b = _broker(balance=1000.0)
    _book(b, runners={1: (2.0, 2.02, 10.0)})
    assert _place(b, "BACK", 2.0, 10.0)[1]["sizeMatched"] == pytest.approx(10.0)
    assert _place(b, "BACK", 2.0, 50.0)[1]["sizeMatched"] == 0.0  # livello finito, non "senza limite"


# --- rilievi Codex su #542 ------------------------------------------------------------------------

def test_stato_precedente_con_parziale_a_riposo_non_riconsuma_la_liquidita():
    b = _broker(balance=1000.0)
    _book(b, runners={1: (2.0, 2.02, 10.0)})
    _, rep = _place(b, "BACK", 2.0, 100.0)  # 10 abbinati, 90 a riposo
    stato = json.loads(json.dumps(b.state.to_dict()))
    del stato["fills"], stato["liquidity_consumed"]  # formato precedente a PFC-005
    nuovo = _broker(balance=1000.0)
    nuovo.state.load_from_dict(stato)
    _book(nuovo, runners={1: (2.0, 2.02, 10.0)})  # lo stesso book del feed
    assert nuovo.state.orders[rep["betId"]].matched_size == pytest.approx(10.0)


def test_sospensione_con_scale_vuote_non_azzera_la_liquidita_consumata():
    b = _broker(balance=1000.0)
    _book(b, runners={1: (2.0, 2.02, 10.0)})
    _, rep = _place(b, "BACK", 2.0, 100.0)
    b.update_market_book("1.1", {"status": "SUSPENDED", "runners": [
        {"selectionId": 1, "status": "ACTIVE", "ex": {"availableToBack": [], "availableToLay": []}}]})
    b.update_market_book("1.1", {"status": "OPEN", "runners": [
        {"selectionId": 1, "status": "ACTIVE", "ex": {"availableToBack": [], "availableToLay": []}}]})
    _book(b, runners={1: (2.0, 2.02, 10.0)})  # riapertura: il feed ripresenta lo stesso livello
    assert b.state.orders[rep["betId"]].matched_size == pytest.approx(10.0)


def test_book_con_valori_non_numerici_non_abbina_niente_e_non_solleva():
    b = _broker(balance=1000.0)
    _book(b, runners={1: (1.8, 1.82, 100.0), 2: (1.8, 1.82, 100.0)})
    _, r1 = _place(b, "BACK", 2.0, 10.0, sel=1)
    _, r2 = _place(b, "BACK", 2.0, 10.0, sel=2)
    esito = b.update_market_book("1.1", {"status": "OPEN", "runners": [
        {"selectionId": 1, "status": "ACTIVE", "ex": {"availableToBack": [{"price": 2.5, "size": 100.0}]}},
        {"selectionId": 2, "status": "ACTIVE", "ex": {"availableToBack": [{"price": "abc", "size": 100.0}]}}]})
    assert esito["ok"] is True
    assert b.state.orders[r1["betId"]].matched_size == 0.0 and b.state.orders[r2["betId"]].matched_size == 0.0
    assert b.state.fills == []


def test_fill_id_ripetuto_nel_registro_persistito_e_incoerente():
    # Codex su #542: BACK 10@3 e LAY 10@2 con lo stesso fill_id facevano dichiarare 1020 su 1000.
    b = _broker(balance=1000.0)
    _book(b, runners={1: (3.0, 3.1, 100.0)})
    _place(b, "BACK", 3.0, 10.0)
    _book(b, runners={1: (1.98, 2.0, 100.0)})
    _place(b, "LAY", 2.0, 10.0)
    stato = json.loads(json.dumps(b.state.to_dict()))
    stato["fills"][1]["fill_id"] = stato["fills"][0]["fill_id"]
    nuovo = _broker(balance=1000.0)
    nuovo.state.load_from_dict(stato)
    fondi = nuovo.get_account_funds()
    assert fondi["error"] == "FILL_LEDGER_INCONSISTENT" and fondi["available"] == 0.0


@pytest.mark.parametrize("campo,valore", [("price", 100.0), ("side", "LAY"), ("selection_id", 9)])
def test_fill_persistito_con_metadati_diversi_dall_ordine_e_incoerente(campo, valore):
    # Codex su #542: quota del primo fill portata a 100 dopo BACK 10@3 + LAY 10@2 => disponibile 30.
    b = _broker(balance=1000.0)
    _book(b, runners={1: (3.0, 3.1, 100.0)})
    _place(b, "BACK", 3.0, 10.0)
    _book(b, runners={1: (1.98, 2.0, 100.0)})
    _place(b, "LAY", 2.0, 10.0)
    stato = json.loads(json.dumps(b.state.to_dict()))
    stato["fills"][0][campo] = valore
    nuovo = _broker(balance=1000.0)
    nuovo.state.load_from_dict(stato)
    assert nuovo.get_account_funds()["error"] == "FILL_LEDGER_INCONSISTENT"
    assert _chiuso(nuovo, "1.1", {1: "WINNER"})["settlement_status"] == "LEDGER_INCONSISTENT"


@pytest.mark.parametrize("prezzo,dimensione", [(2.5, float("nan")), (float("inf"), 100.0), (float("nan"), 100.0)])
def test_valori_non_finiti_nel_book_non_abbinano(prezzo, dimensione):
    b = _broker(balance=1000.0)
    _book(b, runners={1: (1.8, 1.82, 100.0)})
    _, rep = _place(b, "BACK", 2.0, 60.0)
    b.update_market_book("1.1", {"status": "OPEN", "runners": [{"selectionId": 1, "status": "ACTIVE", "ex": {
        "availableToBack": [{"price": prezzo, "size": dimensione}]}}]})
    assert b.state.orders[rep["betId"]].matched_size == 0.0 and b.state.fills == []


def test_dimensione_nan_al_piazzamento_non_vale_liquidita_illimitata():
    b = _broker(balance=1000.0)
    b.update_market_book("1.1", {"status": "OPEN", "runners": [{"selectionId": 1, "status": "ACTIVE", "ex": {
        "availableToBack": [{"price": 2.5, "size": float("nan")}]}}]})
    assert _place(b, "BACK", 2.0, 60.0)[1]["sizeMatched"] == 0.0


# --- quarto giro Codex su #542 ----------------------------------------------------------------------

def test_registro_incoerente_blocca_anche_l_abbinamento_differito():
    b = _broker(balance=1000.0)
    _book(b, runners={1: (2.0, 2.02, 10.0)})
    _, rep = _place(b, "BACK", 2.0, 100.0)  # 10 abbinati, 90 a riposo
    stato = json.loads(json.dumps(b.state.to_dict()))
    stato["fills"] = []  # registro perso
    nuovo = _broker(balance=1000.0)
    nuovo.state.load_from_dict(stato)
    _book(nuovo, runners={1: (2.0, 2.02, 1000.0)})
    assert nuovo.state.orders[rep["betId"]].matched_size == pytest.approx(10.0)  # nessun nuovo fill
    assert nuovo.state.fills == []


def test_voce_malformata_in_piu_rende_il_registro_inaffidabile_anche_dopo_un_salvataggio():
    b = _broker(balance=1000.0)
    _book(b, runners={1: (3.0, 3.1, 100.0)})
    _place(b, "BACK", 3.0, 10.0)
    stato = json.loads(json.dumps(b.state.to_dict()))
    extra = dict(stato["fills"][0], fill_id="altro")
    del extra["price"]
    stato["fills"].append(extra)  # i fill validi tornano ancora con l'ordine
    nuovo = _broker(balance=1000.0)
    nuovo.state.load_from_dict(stato)
    assert nuovo.get_account_funds()["error"] == "FILL_LEDGER_INCONSISTENT"
    di_nuovo = _broker(balance=1000.0)
    di_nuovo.state.load_from_dict(json.loads(json.dumps(nuovo.state.to_dict())))  # salvato e ricaricato
    assert di_nuovo.get_account_funds()["error"] == "FILL_LEDGER_INCONSISTENT"
    assert _chiuso(di_nuovo, "1.1", {1: "WINNER"})["settlement_status"] == "LEDGER_INCONSISTENT"
    di_nuovo.reset()
    assert "error" not in di_nuovo.get_account_funds()


def test_quota_illeggibile_non_cancella_la_liquidita_consumata():
    b = _broker(balance=1000.0)
    _book(b, runners={1: (2.0, 2.02, 10.0)})
    _, rep = _place(b, "BACK", 2.0, 100.0)
    b.update_market_book("1.1", {"status": "OPEN", "runners": [{"selectionId": 1, "status": "ACTIVE", "ex": {
        "availableToBack": [{"price": "abc", "size": 10.0}]}}]})
    _book(b, runners={1: (2.0, 2.02, 10.0)})
    assert b.state.orders[rep["betId"]].matched_size == pytest.approx(10.0)


@pytest.mark.parametrize("fase", ["piazzamento", "differito"])
def test_dimensione_negativa_non_vale_liquidita_illimitata(fase):
    b = _broker(balance=1000.0)
    negativo = {"status": "OPEN", "runners": [{"selectionId": 1, "status": "ACTIVE", "ex": {
        "availableToBack": [{"price": 2.5, "size": -5.0}]}}]}
    if fase == "piazzamento":
        b.update_market_book("1.1", negativo)
        assert _place(b, "BACK", 2.0, 60.0)[1]["sizeMatched"] == 0.0
    else:
        _book(b, runners={1: (1.8, 1.82, 100.0)})
        _, rep = _place(b, "BACK", 2.0, 60.0)
        b.update_market_book("1.1", negativo)
        assert b.state.orders[rep["betId"]].matched_size == 0.0


def test_book_in_parte_malformato_non_ripresenta_la_liquidita_degli_altri_runner():
    # GPT-6.1 Sol su #542: R1 consumato, quota illeggibile su R2: R1 non deve tornare pieno.
    b = _broker(balance=1000.0)
    _book(b, runners={1: (2.0, 2.02, 10.0), 2: (3.0, 3.1, 10.0)})
    assert _place(b, "BACK", 2.0, 10.0, sel=1)[1]["sizeMatched"] == pytest.approx(10.0)
    b.update_market_book("1.1", {"status": "OPEN", "runners": [
        {"selectionId": 1, "status": "ACTIVE", "ex": {"availableToBack": [{"price": 2.0, "size": 10.0}]}},
        {"selectionId": 2, "status": "ACTIVE", "ex": {"availableToBack": [{"price": "abc", "size": 10.0}]}}]})
    assert _place(b, "BACK", 2.0, 50.0, sel=1)[1]["sizeMatched"] == 0.0  # nessun doppio consumo
    _book(b, runners={1: (2.0, 2.02, 10.0), 2: (3.0, 3.1, 10.0)})
    assert sum(o.matched_size for o in b.state.orders.values()) == pytest.approx(10.0)


# --- quinto giro Codex su #542 ----------------------------------------------------------------------

def test_id_del_fill_libero_anche_con_registro_ripristinato_a_numerazione_diversa():
    b = _broker(balance=1000.0)
    _book(b, runners={1: (2.0, 2.02, 10.0)})
    _, rep = _place(b, "BACK", 2.0, 100.0)
    bet = rep["betId"]
    stato = json.loads(json.dumps(b.state.to_dict()))
    stato["fills"][0]["fill_id"] = bet + "#2"  # id opaco gia' "occupato"
    nuovo = _broker(balance=1000.0)
    nuovo.state.load_from_dict(stato)
    _book(nuovo, runners={1: (2.0, 2.02, 30.0)})
    ids = [f["fill_id"] for f in nuovo.state.fills]
    assert len(ids) == len(set(ids)) == 2 and "error" not in nuovo.get_account_funds()


@pytest.mark.parametrize("runners", [
    ["non un dizionario"],
    [{"selectionId": 1, "status": "ACTIVE", "ex": "non un dizionario"}],
    [{"selectionId": 1, "status": "ACTIVE", "ex": {"availableToBack": "x"}}],
    [{"selectionId": 1, "status": "ACTIVE", "ex": {"availableToBack": ["x"]}}],
])
def test_book_strutturalmente_malformato_non_abbina_e_non_solleva(runners):
    b = _broker(balance=1000.0)
    _book(b, runners={1: (1.8, 1.82, 100.0)})
    _, rep = _place(b, "BACK", 2.0, 60.0)
    validi = [{"selectionId": 1, "status": "ACTIVE", "ex": {"availableToBack": [{"price": 2.5, "size": 100.0}]}}]
    assert b.update_market_book("1.1", {"status": "OPEN", "runners": validi + runners})["ok"] is True
    assert b.state.orders[rep["betId"]].matched_size == 0.0 and b.state.fills == []


def test_consumo_dei_livelli_inferiori_resta_se_il_livello_compare_ancora():
    # due livelli consumati; il primo si ricarica: il secondo non deve ripresentarsi intero
    b = _broker(balance=1000.0)
    b.update_market_book("1.1", {"status": "OPEN", "runners": [{"selectionId": 1, "status": "ACTIVE", "ex": {
        "availableToBack": [{"price": 2.2, "size": 10.0}, {"price": 2.1, "size": 10.0}]}}]})
    _place(b, "BACK", 2.2, 10.0)          # consuma 2.2
    b.update_market_book("1.1", {"status": "OPEN", "runners": [{"selectionId": 1, "status": "ACTIVE", "ex": {
        "availableToBack": [{"price": 2.2, "size": 10.0}, {"price": 2.1, "size": 10.0}]}}]})
    _place(b, "BACK", 2.1, 10.0)          # consuma 2.1
    b.update_market_book("1.1", {"status": "OPEN", "runners": [{"selectionId": 1, "status": "ACTIVE", "ex": {
        "availableToBack": [{"price": 2.2, "size": 20.0}, {"price": 2.1, "size": 10.0}]}}]})  # 2.2 ricaricato
    assert _place(b, "BACK", 2.1, 50.0)[1]["sizeMatched"] == pytest.approx(10.0)  # solo i 10 nuovi a 2.2
    b.update_market_book("1.1", {"status": "OPEN", "runners": [{"selectionId": 1, "status": "ACTIVE", "ex": {
        "availableToBack": [{"price": 2.2, "size": 20.0}, {"price": 2.1, "size": 10.0}]}}]})
    assert sum(o.matched_size for o in b.state.orders.values()) == pytest.approx(30.0)  # 2.1 gia' preso


@pytest.mark.parametrize("quota", [1.0, 0.0, None])
def test_quota_non_valida_per_l_exchange_non_cancella_il_consumo(quota):
    b = _broker(balance=1000.0)
    _book(b, runners={1: (2.0, 2.02, 10.0)})
    _, rep = _place(b, "BACK", 2.0, 100.0)
    livello = {"size": 10.0} if quota is None else {"price": quota, "size": 10.0}
    b.update_market_book("1.1", {"status": "OPEN", "runners": [{"selectionId": 1, "status": "ACTIVE", "ex": {
        "availableToBack": [livello]}}]})
    _book(b, runners={1: (2.0, 2.02, 10.0)})
    assert b.state.orders[rep["betId"]].matched_size == pytest.approx(10.0)


# --- sesto giro Codex su #542 -----------------------------------------------------------------------

def test_l_ordine_piu_vecchio_esaurisce_i_livelli_prima_del_successivo():
    b = _broker(balance=1000.0)
    _book(b, runners={1: (2.0, 2.02, 100.0)})
    _, vecchio = _place(b, "BACK", 3.5, 20.0)
    _, nuovo = _place(b, "BACK", 3.5, 10.0)
    b.update_market_book("1.1", {"status": "OPEN", "runners": [{"selectionId": 1, "status": "ACTIVE", "ex": {
        "availableToBack": [{"price": 3.6, "size": 10.0}, {"price": 3.5, "size": 10.0}]}}]})
    assert b.state.orders[vecchio["betId"]].matched_size == pytest.approx(20.0)
    assert b.state.orders[nuovo["betId"]].matched_size == 0.0


@pytest.mark.parametrize("guasto", ["assente", "vecchio"])
def test_ledger_vivo_incoerente_blocca_l_abbinamento_differito(guasto):
    b = _broker(balance=1000.0)
    _book(b, runners={1: (2.0, 2.02, 10.0)})
    _, rep = _place(b, "BACK", 2.0, 100.0)  # 10 abbinati
    stato = json.loads(json.dumps(b.state.to_dict()))
    if guasto == "assente":
        stato["position_ledgers"] = {}
    else:
        next(iter(stato["position_ledgers"].values()))["snapshot"]["open_size"] = 3.0
    nuovo = _broker(balance=1000.0)
    nuovo.state.load_from_dict(stato)
    _book(nuovo, runners={1: (2.0, 2.02, 30.0)})
    assert nuovo.state.orders[rep["betId"]].matched_size == pytest.approx(10.0)
