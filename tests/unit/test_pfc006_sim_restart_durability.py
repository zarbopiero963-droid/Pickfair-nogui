"""PFC-006 (G7-c, roadmap #461; decisione owner #531): riavvio con ordini EXECUTABLE non abbinati.

La fonte di reload del SimulationBroker e' lo snapshot `simulation_state`. Ogni operazione che cambia
ordini o fondi lo scrive prima di rispondere: un crash subito dopo un piazzamento non fa sparire
l'ordine (e la sua riserva), uno subito dopo una cancellazione non lo resuscita EXECUTABLE (che con
l'abbinamento differito potrebbe perfino abbinarsi). Dopo il riavvio l'ordine a riposo riserva gli
stessi fondi, si abbina, si cancella e si regola come se il processo non fosse mai caduto.
"""
from __future__ import annotations

import json

import pytest

from simulation_broker import SimulationBroker


class _Db:
    """Il DB del runtime per quel che conta qui: snapshot di reload e cronologia per-bet."""

    def __init__(self):
        self.snapshot = None
        self.bets = {}

    def save_simulation_state(self, state_key, state):
        assert state_key == "default"
        self.snapshot = json.loads(json.dumps(state))  # serializzato, come nel DB

    def save_simulation_bet(self, payload):
        self.bets[payload["bet_id"]] = dict(payload)


def _broker(db, balance=100.0):
    return SimulationBroker(starting_balance=balance, commission_pct=4.5, db=db)


def _crash_e_riavvio(db, balance=100.0):
    """Il processo muore senza disconnect: il nuovo broker legge solo cio' che e' su disco."""
    nuovo = _broker(db, balance)
    if db.snapshot is not None:
        nuovo.state.load_from_dict(db.snapshot)
    return nuovo


def _book(b, market_id="1.1", back=1.8, lay=3.5, size=1000.0, status="OPEN"):
    b.update_market_book(market_id, {"status": status, "runners": [{
        "selectionId": 1, "status": "ACTIVE",
        "ex": {"availableToBack": [{"price": back, "size": size}],
               "availableToLay": [{"price": lay, "size": size}]}}]})


def _place(b, side="BACK", price=2.0, size=60.0, market_id="1.1"):
    return b.place_bet(market_id=market_id, selection_id=1, side=side, price=price,
                       size=size)["instructionReports"][0]["betId"]


def _funds(b):
    f = b.get_account_funds()
    return round(f["available"], 6), round(f["exposure"], 6), round(f["total"], 6)


def test_crash_dopo_il_piazzamento_l_ordine_e_la_riserva_sopravvivono():
    db = _Db()
    b = _broker(db)
    _book(b)
    bet = _place(b)  # a riposo: miglior back 1.8 < 2.0
    riavviato = _crash_e_riavvio(db)
    assert riavviato.state.orders[bet].status == "EXECUTABLE"
    assert _funds(riavviato) == _funds(b) == (40.0, 60.0, 100.0)
    # la riserva c'e' davvero: un secondo ordine oltre il disponibile e' rifiutato
    res = riavviato.place_bet(market_id="1.2", selection_id=1, side="BACK", price=2.0, size=50.0)
    assert res["errorCode"] == "INSUFFICIENT_FUNDS"


def test_crash_dopo_il_batch_tutte_le_gambe_sopravvivono():
    db = _Db()
    b = _broker(db)
    b.place_orders(market_id="1.1", instructions=[
        {"selection_id": 1, "side": "BACK", "price": 3.0, "size": 30.0},
        {"selection_id": 2, "side": "BACK", "price": 3.0, "size": 30.0}])
    riavviato = _crash_e_riavvio(db)
    assert len(riavviato.state.orders) == 2 and _funds(riavviato) == (40.0, 60.0, 100.0)


@pytest.mark.parametrize("modo", ["singola", "parziale", "mercato"])
def test_crash_dopo_la_cancellazione_l_ordine_non_risorge(modo):
    db = _Db()
    b = _broker(db)
    _book(b)
    bet = _place(b)
    if modo == "singola":
        b.cancel_order(bet_id=bet, market_id="1.1")
        atteso = (100.0, 0.0, 100.0)
    elif modo == "parziale":
        b.cancel_orders(market_id="1.1", instructions=[{"betId": bet, "sizeReduction": 20.0}])
        atteso = (60.0, 40.0, 100.0)
    else:
        b.cancel_orders(market_id="1.1")  # tutto il mercato, senza istruzioni
        atteso = (100.0, 0.0, 100.0)
    riavviato = _crash_e_riavvio(db)
    assert _funds(riavviato) == atteso
    # il book raggiunge la quota: un ordine cancellato non deve abbinarsi dopo il riavvio
    _book(riavviato, back=2.5)
    ordine = riavviato.state.orders[bet]
    if modo == "parziale":
        assert ordine.matched_size == pytest.approx(40.0)
    else:
        assert ordine.status == "CANCELLED" and ordine.matched_size == 0.0
    assert db.bets[bet]["status"] == ordine.status  # anche la cronologia per-bet


def test_crash_dopo_la_sostituzione_resta_solo_il_nuovo_ordine():
    db = _Db()
    b = _broker(db)
    _book(b)
    bet = _place(b, side="LAY", price=3.0, size=40.0)  # miglior lay 3.5 > 3.0: a riposo
    nuovo_bet = b.replace_orders(market_id="1.1", bet_id=bet, new_price=3.2)["instructionReports"][0]["betId"]
    riavviato = _crash_e_riavvio(db)
    stati = {k: o.status for k, o in riavviato.state.orders.items()}
    assert stati == {bet: "CANCELLED", nuovo_bet: "EXECUTABLE"}
    assert _funds(riavviato) == (12.0, 88.0, 100.0)  # liability 40 x 2.2, una volta sola


def test_ordine_a_riposo_dopo_il_riavvio_si_abbina_e_si_regola_senza_doppi_conteggi():
    db = _Db()
    b = _broker(db, balance=1000.0)
    _book(b)
    bet = _place(b)
    riavviato = _crash_e_riavvio(db, balance=1000.0)
    _book(riavviato, back=2.2)  # abbinamento differito sul broker riavviato
    assert riavviato.state.orders[bet].matched_size == pytest.approx(60.0)
    di_nuovo = _crash_e_riavvio(db, balance=1000.0)  # crash anche dopo il fill differito
    assert di_nuovo.state.orders[bet].matched_size == pytest.approx(60.0)
    assert _funds(di_nuovo) == (940.0, 60.0, 1000.0)
    esito = di_nuovo.update_market_book("1.1", {"status": "CLOSED", "runners": [
        {"selectionId": 1, "status": "WINNER"}]})
    assert esito["settlement_status"] == "SETTLED"
    atteso = round(1000.0 + 60.0 * 1.2 * 0.955, 6)
    assert _funds(di_nuovo) == (atteso, 0.0, atteso)
    # e il settlement stesso e' durevole: un altro crash non lo rifa' ne' lo perde
    ultimo = _crash_e_riavvio(db, balance=1000.0)
    assert _funds(ultimo) == (atteso, 0.0, atteso)
    assert ultimo.update_market_book("1.1", {"status": "CLOSED", "runners": [
        {"selectionId": 1, "status": "WINNER"}]})["settlement_status"] == "DUPLICATE"


def test_ordine_rifiutato_non_scrive_nulla():
    db = _Db()
    b = _broker(db)
    res = b.place_bet(market_id="1.1", selection_id=1, side="BACK", price=2.0, size=150.0)
    assert res["errorCode"] == "INSUFFICIENT_FUNDS"
    assert db.snapshot is None and db.bets == {}


class _DbGuasto(_Db):
    def save_simulation_state(self, state_key, state):
        raise OSError("disco pieno")


def test_snapshot_non_scritto_e_dichiarato_in_ogni_esito():
    # Codex su #542: un fallimento della scrittura non deve restare un successo silenzioso.
    db = _DbGuasto()
    b = _broker(db)
    _book(b)
    res = b.place_bet(market_id="1.1", selection_id=1, side="BACK", price=2.0, size=10.0)
    assert res["status"] == "SUCCESS" and res["snapshot_persisted"] is False
    bet = res["instructionReports"][0]["betId"]
    batch = b.place_orders(market_id="1.2", instructions=[
        {"selection_id": 1, "side": "BACK", "price": 3.0, "size": 5.0}])
    assert batch["snapshot_persisted"] is False
    _book(b, back=2.5)  # abbinamento differito
    assert b.state.orders[bet].matched_size == pytest.approx(10.0)
    esito = b.update_market_book("1.1", {"status": "CLOSED", "runners": [{"selectionId": 1, "status": "WINNER"}]})
    assert esito["settlement_status"] == "SETTLED" and esito["snapshot_persisted"] is False
    lay = b.place_bet(market_id="1.3", selection_id=1, side="LAY", price=3.0, size=5.0)["instructionReports"][0]["betId"]
    assert b.replace_orders(market_id="1.3", bet_id=lay, new_price=3.2)["snapshot_persisted"] is False
    assert b.cancel_orders(market_id="1.3")["snapshot_persisted"] is False
    assert b.persist_snapshot() is False


def test_abbinamento_differito_con_snapshot_non_scritto_e_dichiarato():
    db = _DbGuasto()
    b = _broker(db)
    _book(b)
    b.place_bet(market_id="1.1", selection_id=1, side="BACK", price=2.0, size=10.0)
    esito = b.update_market_book("1.1", {"status": "OPEN", "runners": [{"selectionId": 1, "status": "ACTIVE", "ex": {
        "availableToBack": [{"price": 2.5, "size": 100.0}]}}]})
    assert esito["ok"] is True and esito["snapshot_persisted"] is False


def test_senza_db_nessun_campo_di_durabilita():
    b = SimulationBroker(starting_balance=100.0)
    res = b.place_bet(market_id="1.1", selection_id=1, side="BACK", price=2.0, size=10.0)
    assert "snapshot_persisted" not in res
    assert b.persist_snapshot() is False  # invariato: senza db non c'e' nulla da scrivere
