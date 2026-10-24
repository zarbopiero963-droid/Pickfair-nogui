"""#543: il ricalcolo dei fondi del SimulationBroker non e' piu' quadratico, con contabilita' IDENTICA.

L'ottimizzazione (ordini e fill raggruppati per mercato in una passata; memo per mercato indirizzato dal
contenuto, cioe' con chiave = tutti i campi letti dal calcolo) si prova per differenza: un broker di
riferimento con il codice di prima (`_Riferimento`, copia verbatim di `_esposizione_totale_locked` e
`_cassa_locked` di main `e9d0705`) e il broker ottimizzato ricevono la stessa sequenza deterministica di
operazioni, e dopo OGNI operazione esiti e fondi devono coincidere.

Sequenza: BACK e LAY a riposo, abbinati e parziali, batch Dutching, cancellazioni, sostituzioni, book che
abbinano ordini a riposo, settlement (mercato chiuso), riavvio da snapshot, registro dei fill corrotto.
"""
from __future__ import annotations

import copy
import random
import threading

import pytest

from simulation_broker import SimulationBroker

pytestmark = [pytest.mark.safety]


class _Riferimento(SimulationBroker):
    """Il calcolo dei fondi di prima di #543, senza raggruppamento ne' memo."""

    def _esposizione_totale_locked(self, candidati=None, escludi=None, solo_abbinato=False):
        mercati = {str(o.market_id) for o in self.state.orders.values()} | {
            str(o.market_id) for o in candidati or []}
        mercati -= set(self.state.settlements)
        return float(sum(
            self._esposizione_mercato_locked(
                m, [o for o in candidati or [] if str(o.market_id) == m], escludi or set(), solo_abbinato)
            for m in mercati))

    def _cassa_locked(self):
        mercati = {str(f.get("market_id")) for f in self.state.fills} | {
            str(o.market_id) for o in self.state.orders.values() if o.matched_size > 0.0}
        mercati -= set(self.state.settlements)
        return float(self.state.balance) - float(
            sum(self._ricostruisci_fill_mercato_locked(m)[0] for m in mercati))


_MERCATI = [f"1.54{i}" for i in range(6)]


def _book(rng, market_id, *, chiuso=False, vincitore=None):
    runners = []
    for sel in (1, 2, 3):
        if chiuso:
            runners.append({"selectionId": sel, "status": "WINNER" if sel == vincitore else "LOSER"})
            continue
        back = rng.choice([1.8, 2.0, 2.2, 3.0])
        runners.append({"selectionId": sel, "status": "ACTIVE", "ex": {
            "availableToBack": [{"price": back, "size": rng.choice([0.0, 2.0, 5.0, 12.0])}],
            "availableToLay": [{"price": round(back + 0.1, 2), "size": rng.choice([0.0, 3.0, 8.0])}]}})
    book = {"marketId": market_id, "status": "CLOSED" if chiuso else "OPEN", "runners": runners}
    if market_id[-1] in "024":
        book["numberOfWinners"] = 1          # meta' dei mercati a vincitore unico, meta' runner per runner
    return book


def _fondi(b):
    f = b.get_account_funds()
    return {k: (round(v, 9) if isinstance(v, float) else v) for k, v in f.items()}


def _esito(r):
    reports = r.get("instructionReports") or []
    return (r.get("status"), r.get("errorCode"),
            tuple((x.get("status"), x.get("errorCode"), round(float(x.get("sizeMatched") or 0.0), 9),
                   x.get("orderStatus")) for x in reports))


class _Coppia:
    """Stesse operazioni su ottimizzato e riferimento; gli ordini si scelgono per posizione (bet_id casuali)."""

    def __init__(self, saldo):
        self.opt = SimulationBroker(starting_balance=saldo)
        self.ref = _Riferimento(starting_balance=saldo)
        self.ids = {id(self.opt): [], id(self.ref): []}

    def entrambi(self, operazione):
        esiti = []
        for b in (self.opt, self.ref):
            esiti.append(operazione(b, self.ids[id(b)]))
        return esiti

    def verifica(self, passo):
        assert _fondi(self.opt) == _fondi(self.ref), passo
        stati = [[(o.status, round(o.matched_size, 9)) for o in b.state.orders.values()] for b in (self.opt, self.ref)]
        assert stati[0] == stati[1], passo


def _registra(r, ids):
    for rep in r.get("instructionReports") or []:
        if rep.get("betId"):
            ids.append(rep["betId"])
    return _esito(r)


def _sequenza(seed, passi=260, saldo=400.0):
    rng = random.Random(seed)
    c = _Coppia(saldo)
    regolati = set()
    for m in _MERCATI:
        book = _book(rng, m)
        c.entrambi(lambda b, ids, book=book, m=m: b.update_market_book(m, copy.deepcopy(book)))
    for passo in range(passi):
        scelta = rng.random()
        m = rng.choice(_MERCATI)
        if scelta < 0.40:
            args = dict(market_id=m, selection_id=rng.choice([1, 2, 3]), side=rng.choice(["BACK", "LAY"]),
                        price=rng.choice([1.9, 2.0, 2.2, 2.5, 3.0]), size=rng.choice([1.0, 2.5, 6.0, 15.0]))
            e = c.entrambi(lambda b, ids, a=args: _registra(b.place_bet(**a), ids))
        elif scelta < 0.52:
            legs = [{"selection_id": s, "side": "BACK", "price": rng.choice([2.0, 3.0, 4.0]),
                     "size": rng.choice([1.0, 3.0, 9.0])} for s in rng.sample([1, 2, 3], rng.choice([2, 3]))]
            e = c.entrambi(lambda b, ids, legs=legs: _registra(
                b.place_orders(market_id=m, instructions=copy.deepcopy(legs)), ids))
        elif scelta < 0.62:
            k = rng.random()
            e = c.entrambi(lambda b, ids, k=k: _esito(b.cancel_order(bet_id=ids[int(k * len(ids))]))
                           if ids else None)
        elif scelta < 0.70:
            k, prezzo = rng.random(), rng.choice([1.8, 2.4, 3.5])
            e = c.entrambi(lambda b, ids, k=k, p=prezzo: _registra(
                b.replace_orders(bet_id=ids[int(k * len(ids))], new_price=p), ids) if ids else None)
        elif scelta < 0.90:
            book = _book(rng, m)            # nuovo book: abbina ordini a riposo (fill differiti)
            e = c.entrambi(lambda b, ids, book=book: b.update_market_book(m, copy.deepcopy(book)).get("ok"))
        elif scelta < 0.95 and m not in regolati:
            regolati.add(m)
            book = _book(rng, m, chiuso=True, vincitore=rng.choice([1, 2, 3]))
            e = c.entrambi(lambda b, ids, book=book: b.update_market_book(m, copy.deepcopy(book)).get(
                "settlement_status"))
        else:
            # riavvio: stato ricaricato da snapshot (memo vuoto sul nuovo broker ottimizzato)
            for attr, cls in (("opt", SimulationBroker), ("ref", _Riferimento)):
                vecchio = getattr(c, attr)
                nuovo = cls(starting_balance=saldo)
                nuovo.state.load_from_dict(vecchio.state.to_dict())
                c.ids[id(nuovo)] = c.ids.pop(id(vecchio))
                setattr(c, attr, nuovo)
            e = [None, None]
        assert e[0] == e[1], (seed, passo)
        c.verifica((seed, passo))
    return c


@pytest.mark.parametrize("seed", range(12))
def test_contabilita_identica_al_calcolo_di_prima(seed):
    c = _sequenza(seed)
    # la sequenza deve aver prodotto fill e ordini (la copertura dei casi e' nel test che segue)
    assert c.opt.state.fills and c.opt.state.orders


def test_la_sequenza_copre_tutti_i_casi():
    """Il test differenziale vale solo se i casi compaiono davvero nelle sequenze."""
    visti = set()
    for seed in range(12):
        c = _sequenza(seed)
        for o in c.opt.state.orders.values():
            visti.add(o.status)
            if 0.0 < o.matched_size < o.size:
                visti.add("PARZIALE")
            if o.batch_id or o.side == "LAY":
                visti.add(o.side)
        if c.opt.state.settlements:
            visti.add("SETTLEMENT")
    assert {"EXECUTABLE", "EXECUTION_COMPLETE", "CANCELLED", "PARZIALE", "SETTLEMENT", "LAY"} <= visti, visti


def test_rifiuto_per_fondi_identico_al_limite():
    """Al limite del disponibile ottimizzato e riferimento accettano e rifiutano gli stessi ordini."""
    c = _Coppia(10.0)
    rng = random.Random(7)
    for m in _MERCATI:
        book = _book(rng, m)
        c.entrambi(lambda b, ids, book=book, m=m: b.update_market_book(m, copy.deepcopy(book)))
    rifiuti = 0
    for i in range(40):
        args = dict(market_id=_MERCATI[i % 6], selection_id=1 + i % 3, side="BACK" if i % 2 else "LAY",
                    price=3.0, size=1.0 + (i % 4))
        e = c.entrambi(lambda b, ids, a=args: _registra(b.place_bet(**a), ids))
        assert e[0] == e[1], i
        rifiuti += e[0][1] == "INSUFFICIENT_FUNDS"
        c.verifica(i)
    assert rifiuti > 0


def test_registro_dei_fill_corrotto_resta_non_verificabile_anche_con_il_memo():
    c = _sequenza(3, passi=60)
    _fondi(c.opt)                         # memo popolato
    for b in (c.opt, c.ref):
        b.state.fills_corrupted = True
    assert _fondi(c.opt) == _fondi(c.ref)
    assert c.opt.get_account_funds()["error"] == "FILL_LEDGER_INCONSISTENT"


def test_un_campo_cambiato_fuori_dalle_api_non_lascia_un_risultato_vecchio():
    """Memo per contenuto: anche una modifica diretta di un ordine o di un fill cambia la chiave."""
    c = _sequenza(5, passi=80)
    _fondi(c.opt)
    ordine = next(o for o in c.opt.state.orders.values() if o.status == "EXECUTABLE")
    gemello = c.ref.state.orders[list(c.ref.state.orders)[list(c.opt.state.orders).index(ordine.bet_id)]]
    for o in (ordine, gemello):
        o.size += 50.0                     # piu' riserva sul non abbinato
    assert _fondi(c.opt) == _fondi(c.ref)
    for b in (c.opt, c.ref):
        b.state.fills[0]["size"] = float(b.state.fills[0]["size"]) + 1.0   # registro ora incoerente
    assert _fondi(c.opt) == _fondi(c.ref)


def test_memo_senza_crescita_sui_mercati_regolati():
    c = _sequenza(9, passi=40)
    rng = random.Random(1)
    for m in _MERCATI:
        book = _book(rng, m, chiuso=True, vincitore=1)
        c.opt.update_market_book(m, copy.deepcopy(book))
    _fondi(c.opt)
    aperti = {str(o.market_id) for o in c.opt.state.orders.values()} - set(c.opt.state.settlements)
    assert set(c.opt._memo_fill_mercato) <= aperti
    assert {k[0] for k in c.opt._memo_esposizione_mercato} <= aperti


def test_concorrenza_book_e_piazzamenti_resta_coerente():
    """Thread di piazzamento e thread dei book insieme: nessuno stallo, e alla fine i fondi del broker
    ottimizzato coincidono con il calcolo di riferimento sullo stesso stato."""
    b = SimulationBroker(starting_balance=500.0)
    rng = random.Random(11)
    for m in _MERCATI:
        b.update_market_book(m, _book(rng, m))
    errori = []

    def piazza(seed):
        r = random.Random(seed)
        try:
            for _ in range(80):
                b.place_bet(market_id=r.choice(_MERCATI), selection_id=r.choice([1, 2, 3]),
                            side=r.choice(["BACK", "LAY"]), price=r.choice([2.0, 2.5]), size=r.choice([1.0, 4.0]))
                b.get_account_funds()
        except Exception as exc:  # noqa: BLE001 - qualunque errore fa fallire il test
            errori.append(exc)

    def book(seed):
        r = random.Random(seed)
        try:
            for _ in range(80):
                m = r.choice(_MERCATI)
                b.update_market_book(m, _book(r, m))
        except Exception as exc:  # noqa: BLE001
            errori.append(exc)

    threads = [threading.Thread(target=piazza, args=(s,)) for s in (1, 2, 3)] + [
        threading.Thread(target=book, args=(s,)) for s in (4, 5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    assert not any(t.is_alive() for t in threads) and not errori, errori
    ref = _Riferimento(starting_balance=500.0)
    ref.state.load_from_dict(b.state.to_dict())
    assert _fondi(b) == _fondi(ref)


def test_abbinato_cambiato_senza_fill_rende_i_fondi_non_verificabili_anche_con_il_memo():
    """Un ordine il cui abbinato non torna con i fill rende il registro incoerente: il memo non deve
    restituire la cassa di prima (sarebbe fail-open)."""
    c = _sequenza(2, passi=120)
    _fondi(c.opt)
    pos = next(i for i, o in enumerate(c.opt.state.orders.values())
               if o.matched_size > 0.0 and str(o.market_id) not in c.opt.state.settlements)
    for b in (c.opt, c.ref):
        ordine = list(b.state.orders.values())[pos]
        ordine.matched_size += 1.0
    assert _fondi(c.opt) == _fondi(c.ref)
    assert c.opt.get_account_funds().get("error") == "FILL_LEDGER_INCONSISTENT"


def test_numero_di_vincitori_cambiato_sul_book_ricalcola_l_esposizione():
    """Lo stesso mercato passa da runner per runner a vincitore unico: l'esposizione cambia e si ricalcola."""
    c = _Coppia(1000.0)
    book = {"marketId": "1.5431", "status": "OPEN", "runners": [
        {"selectionId": s, "status": "ACTIVE", "ex": {"availableToBack": [{"price": 1.9, "size": 100.0}],
                                                     "availableToLay": [{"price": 2.0, "size": 100.0}]}}
        for s in (1, 2, 3)]}
    c.entrambi(lambda b, ids: b.update_market_book("1.5431", copy.deepcopy(book)))
    for sel in (1, 2):                   # due LAY abbinati: runner per runner 10 + 10, vincitore unico 0
        c.entrambi(lambda b, ids, s=sel: _registra(
            b.place_bet(market_id="1.5431", selection_id=s, side="LAY", price=2.0, size=10.0), ids))
    prima = _fondi(c.opt)
    c.verifica("runner per runner")
    book["numberOfWinners"] = 1          # stessi ordini: cambia solo il modello degli scenari
    c.entrambi(lambda b, ids: b.update_market_book("1.5431", copy.deepcopy(book)))
    c.verifica("vincitore unico")
    assert prima["exposure"] == pytest.approx(20.0) and _fondi(c.opt)["exposure"] == pytest.approx(0.0)
