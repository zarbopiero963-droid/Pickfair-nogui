"""F6 (#453, DECISIONE-426 P25) — i lati del book come su Betfair.

Su Betfair (Exchange API, ``ExchangePrices``):

- ``availableToBack`` sono i prezzi a cui si PUNTA adesso: le offerte di chi
  banca. Il migliore e' il piu' alto;
- ``availableToLay`` sono i prezzi a cui si BANCA adesso: le offerte di chi
  punta. Il migliore e' il piu' basso.

Quindi un BACK a quota P si abbina subito se il miglior ``availableToBack`` e'
>= P, e prende quel prezzo; un LAY a quota P se il miglior ``availableToLay`` e'
<= P. Per chiudere un BACK si banca al miglior ``availableToLay``; per chiudere
un LAY si punta al miglior ``availableToBack``. E' la convenzione che l'owner ha
gia' deciso per il dutching nella #383: si invertono insieme il lato del book e
la disuguaglianza.

FALSO SUCCESSO RIPRODOTTO sul main ``774bb3b`` (book 2.00 / 2.02)::

    cashout_router._closing_price BACK (hedge LAY): (2.0, 100.0)   Betfair: 2.02
    SIM BACK @ 2.00: abbinato 0          Betfair: 10 @ 2.00
    SIM BACK @ 2.02: abbinato 10 @ 2.02  Betfair: resta a riposo

I test del router e del broker erano verdi, perche' descrivevano la stessa
convenzione rovesciata. In LIVE, inoltre, il router chiedeva il book senza
prezzi: Betfair non popola le ladder senza ``priceProjection``, e ogni posizione
veniva saltata in silenzio. Qui l'esito atteso viene sempre da un oracolo
indipendente (``_esito_betfair``), non dal codice sotto test.
"""
from __future__ import annotations

import itertools
import threading
from typing import Any, Dict, List, Optional, Tuple

import pytest

from cashout_router import CashoutRouter, _closing_price
from simulation_broker import SimulationBroker

MERCATO = "1.500"
EVENTO = "Alfa v Beta"
SEL = 11


# --------------------------------------------------------------------------
# Oracolo Betfair, indipendente dal codice sotto test
# --------------------------------------------------------------------------
def _esito_betfair(side: str, prezzo: float, best_back: Optional[float],
                   best_lay: Optional[float]) -> Tuple[bool, Optional[float]]:
    """(si abbina subito?, prezzo di abbinamento) di un ordine LIMIT su Betfair."""
    if side == "BACK":
        if best_back is not None and best_back >= prezzo:
            return True, best_back
        return False, None
    if best_lay is not None and best_lay <= prezzo:
        return True, best_lay
    return False, None


def _book(best_back: float, best_lay: float, *, size_back: float = 100.0,
          size_lay: float = 100.0, market_id: str = MERCATO) -> Dict[str, Any]:
    return {
        "marketId": market_id,
        "status": "OPEN",
        "runners": [{
            "selectionId": SEL,
            "status": "ACTIVE",
            "ex": {
                "availableToBack": [{"price": best_back, "size": size_back}],
                "availableToLay": [{"price": best_lay, "size": size_lay}],
            },
        }],
    }


# --------------------------------------------------------------------------
# Motore SIM contro l'oracolo
# --------------------------------------------------------------------------
_BOOK_TIPICI = [(2.0, 2.02), (1.5, 1.52), (3.5, 3.6)]


def _prezzi_attorno(back: float, lay: float) -> List[float]:
    """Sotto il back, sul back, dentro lo spread, sul lay, sopra il lay."""
    return [round(back - 0.1, 2), back, round((back + lay) / 2, 3), lay, round(lay + 0.1, 2)]


_CASI_SIM = [
    (side, back, lay, prezzo)
    for (back, lay), side in itertools.product(_BOOK_TIPICI, ("BACK", "LAY"))
    for prezzo in _prezzi_attorno(back, lay)
]


@pytest.mark.parametrize("side,back,lay,prezzo", _CASI_SIM)
def test_sim_abbina_come_betfair(side, back, lay, prezzo):
    """Il broker simulato abbina come l'exchange: stesso esito e stesso prezzo
    dell'oracolo, su tre book e cinque prezzi per lato."""
    broker = SimulationBroker(starting_balance=1000.0)
    broker.update_market_book(_book(back, lay))

    esito = broker.place_bet(
        market_id=MERCATO, selection_id=SEL, side=side, price=prezzo, size=10.0,
    )
    report = (esito.get("instructionReports") or [{}])[0]
    abbinato = float(report.get("sizeMatched") or 0.0)

    atteso, prezzo_atteso = _esito_betfair(side, prezzo, back, lay)
    assert (abbinato > 0.0) is atteso, (side, back, lay, prezzo, report)
    if atteso:
        assert abbinato == pytest.approx(10.0)
        assert float(report.get("averagePriceMatched")) == pytest.approx(prezzo_atteso)


_QUOTE_NON_VALIDE = ["bad", 0.0, -2.0, 1.0, float("nan"), float("inf")]


@pytest.mark.parametrize("side", ["BACK", "LAY"])
@pytest.mark.parametrize("quota", _QUOTE_NON_VALIDE, ids=lambda q: repr(q))
def test_sim_quota_non_valida_non_si_abbina(side, quota):
    """Fail-closed sulla quota. Con la semantica Betfair un BACK si abbina se la
    quota chiesta e' <= del miglior back: una quota 0 (anche da un valore
    malformato) accetterebbe qualsiasi prezzo, e un LAY a quota infinita
    qualsiasi lay. Il controllo e' esplicito, come nel client LIVE
    (``INVALID_PRICE``), e non affidato alla disuguaglianza: prima di F6 questi
    casi erano bloccati solo per caso dal verso opposto."""
    broker = SimulationBroker(starting_balance=1000.0)
    broker.update_market_book(_book(2.0, 2.02))
    fondi_prima = broker.get_account_funds()

    esito = broker.place_orders(market_id=MERCATO, instructions=[
        {"selectionId": SEL, "side": side, "price": quota, "size": 10.0},
    ])
    report = esito["instructionReports"][0]

    assert float(report.get("sizeMatched") or 0.0) == 0.0, (side, quota, report)
    assert report["status"] == "FAILURE"
    ordine = broker.state.orders.get(report.get("betId"))
    assert ordine is None or ordine.matched_size == 0.0
    fondi = broker.get_account_funds()
    assert fondi["available"] == pytest.approx(fondi_prima["available"])
    assert fondi["exposure"] == pytest.approx(fondi_prima["exposure"])


@pytest.mark.parametrize("side", ["BACK", "LAY"])
@pytest.mark.parametrize("quota", [0.0, 1.0, float("nan"), float("inf")], ids=lambda q: repr(q))
def test_motore_di_matching_quota_non_valida_non_si_abbina(side, quota):
    from core.simulation_matching_engine import SimulationMatchingEngine
    from core.simulation_order_book import SimulationOrderBook
    from core.simulation_state import SimulationState

    order_book = SimulationOrderBook()
    order_book.update_market_book(MERCATO, _book(2.0, 2.02))
    motore = SimulationMatchingEngine(order_book=order_book, state=SimulationState(starting_balance=1000.0))
    risultato = motore.submit_order(
        bet_id="q1", market_id=MERCATO, selection_id=SEL, side=side, price=quota, size=10.0,
    )
    assert risultato.matched_size == 0.0, (side, quota, risultato)
    assert risultato.status == "FAILURE"


def test_sim_consuma_la_liquidita_del_lato_abbinato():
    """Un BACK consuma ``availableToBack`` e un LAY ``availableToLay``: l'altro
    lato resta intatto."""
    broker = SimulationBroker(starting_balance=1000.0)
    broker.update_market_book(_book(2.0, 2.02, size_back=100.0, size_lay=50.0))

    broker.place_bet(market_id=MERCATO, selection_id=SEL, side="BACK", price=2.0, size=10.0)
    ex = broker.get_market_book(MERCATO)["runners"][0]["ex"]
    assert ex["availableToBack"][0]["size"] == pytest.approx(90.0)
    assert ex["availableToLay"][0]["size"] == pytest.approx(50.0)

    broker.place_bet(market_id=MERCATO, selection_id=SEL, side="LAY", price=2.02, size=5.0)
    ex = broker.get_market_book(MERCATO)["runners"][0]["ex"]
    assert ex["availableToLay"][0]["size"] == pytest.approx(45.0)
    assert ex["availableToBack"][0]["size"] == pytest.approx(90.0)


def test_sim_abbinamento_parziale_sul_lato_giusto():
    """Con meno liquidita' della richiesta si abbina la parte disponibile sul
    lato giusto; il resto resta a riposo."""
    broker = SimulationBroker(starting_balance=1000.0)
    broker.update_market_book(_book(2.0, 2.02, size_back=4.0, size_lay=100.0))

    esito = broker.place_bet(market_id=MERCATO, selection_id=SEL, side="BACK", price=2.0, size=10.0)
    report = esito["instructionReports"][0]
    assert float(report["sizeMatched"]) == pytest.approx(4.0)
    assert float(report["averagePriceMatched"]) == pytest.approx(2.0)


# --------------------------------------------------------------------------
# Prezzo di chiusura del cashout
# --------------------------------------------------------------------------
def test_router_chiude_al_prezzo_eseguibile():
    """Chiudere un BACK = bancare al miglior ``availableToLay`` (con la sua
    profondita'); chiudere un LAY = puntare al miglior ``availableToBack``."""
    book = _book(2.0, 2.02, size_back=100.0, size_lay=40.0)
    assert _closing_price(book, SEL, "BACK") == (2.02, 40.0)
    assert _closing_price(book, SEL, "LAY") == (2.0, 100.0)

    for lato, (back, lay) in itertools.product(("BACK", "LAY"), _BOOK_TIPICI):
        prezzo, _size = _closing_price(_book(back, lay), SEL, lato)
        hedge = "LAY" if lato == "BACK" else "BACK"
        # Il prezzo di chiusura si abbina SUBITO per l'oracolo.
        assert _esito_betfair(hedge, prezzo, back, lay) == (True, prezzo), (lato, back, lay)


class _Pubblicati:
    def __init__(self):
        self.eventi: List[Tuple[str, Dict[str, Any]]] = []

    def __call__(self, topic, payload=None):
        self.eventi.append((topic, dict(payload or {})))

    def di(self, topic):
        return [p for t, p in self.eventi if t == topic]


def _router(book: Dict[str, Any], ordini: List[Dict[str, Any]], pubblica) -> CashoutRouter:
    return CashoutRouter(
        fetch_current_orders=lambda: list(ordini),
        fetch_bot_orders=lambda: [{"bet_id": o["betId"], "market_id": MERCATO, "event_name": EVENTO}
                                  for o in ordini],
        fetch_market_book=lambda _mid: book,
        cancel_orders=lambda *a, **k: {"status": "SUCCESS"},
        publish=pubblica,
        commission_pct=4.5,
        source="TELEGRAM",
    )


def _back_abbinato(bet_id: str = "B1", prezzo: float = 2.02, size: float = 10.0) -> Dict[str, Any]:
    return {
        "betId": bet_id, "marketId": MERCATO, "selectionId": SEL, "side": "BACK",
        "priceSize": {"price": prezzo, "size": size}, "averagePriceMatched": prezzo,
        "sizeMatched": size, "sizeRemaining": 0.0, "status": "EXECUTION_COMPLETE",
    }


def test_router_hedge_al_best_lay_con_green_up():
    """BACK 10 @ 2.02, mercato sceso a 1.98 / 2.00: l'hedge e' un LAY 10,10 @ 2,00.
    Green-up: vince 10 x 1.02 - 10.10 x 1.00 = +0.10; perde -10 + 10.10 = +0.10."""
    pubblica = _Pubblicati()
    esito = _router(_book(1.98, 2.0), [_back_abbinato()], pubblica).route({"signal_type": "CASHOUT_ALL"})

    richieste = pubblica.di("REQ_EXECUTE_CASHOUT")
    assert esito["published"] == 1 and len(richieste) == 1, (esito, pubblica.eventi)
    req = richieste[0]
    assert str(req["side"]).upper() == "LAY"
    assert float(req["price"]) == pytest.approx(2.0)
    assert float(req["stake"]) == pytest.approx(10.10, abs=0.01)


def test_router_gate_profondita_sul_lato_eseguibile():
    """Il gate di profondita' (L95) guarda la liquidita' su cui l'hedge si
    abbina davvero: con 5 disponibili sul lato lay e uno stake di 10,10 il
    cashout non parte (fail-closed), anche se sul lato back ci sono 100."""
    pubblica = _Pubblicati()
    book = _book(1.98, 2.0, size_back=100.0, size_lay=5.0)
    esito = _router(book, [_back_abbinato()], pubblica).route({"signal_type": "CASHOUT_ALL"})

    assert pubblica.di("REQ_EXECUTE_CASHOUT") == [], pubblica.eventi
    assert esito["published"] == 0 and esito["skipped"] == 1


# --------------------------------------------------------------------------
# Resolver Telegram (percorso SIM della vecchia tab Telegram)
# --------------------------------------------------------------------------
def _book_over(best_back: float, best_lay: float) -> Dict[str, Any]:
    book = _book(best_back, best_lay)
    book["runners"][0]["runnerName"] = "Over 2.5"
    return book


def test_resolver_aggressivo_prende_il_prezzo_eseguibile():
    """``aggressive_best_price=True`` vuole un BACK che si abbini subito: il
    miglior ``availableToBack``. Il passivo si offre al miglior
    ``availableToLay`` e resta a riposo, come su Betfair."""
    from services.telegram_bet_resolver import TelegramBetResolver

    resolver = TelegramBetResolver(client_getter=lambda: None)
    aggressivo = resolver._resolve_runner(
        market_id=MERCATO, market_book=_book_over(2.0, 2.02), target_line=2.5,
        aggressive_best_price=True,
    )
    passivo = resolver._resolve_runner(
        market_id=MERCATO, market_book=_book_over(2.0, 2.02), target_line=2.5,
        aggressive_best_price=False,
    )
    assert aggressivo["price"] == pytest.approx(2.0)
    assert _esito_betfair("BACK", aggressivo["price"], 2.0, 2.02) == (True, 2.0)
    assert passivo["price"] == pytest.approx(2.02)
    assert _esito_betfair("BACK", passivo["price"], 2.0, 2.02) == (False, None)


# --------------------------------------------------------------------------
# Motore di matching di test (core/), stessa convenzione
# --------------------------------------------------------------------------
@pytest.mark.parametrize("side,back,lay,prezzo", _CASI_SIM)
def test_motore_di_matching_come_betfair(side, back, lay, prezzo):
    from core.simulation_matching_engine import SimulationMatchingEngine
    from core.simulation_order_book import SimulationOrderBook
    from core.simulation_state import SimulationState

    order_book = SimulationOrderBook()
    order_book.update_market_book(MERCATO, _book(back, lay))
    motore = SimulationMatchingEngine(
        order_book=order_book, state=SimulationState(starting_balance=1000.0),
        queue_ahead_ratio=0.0, slippage_ticks=0,
    )
    risultato = motore._simulate_match(
        market_id=MERCATO, selection_id=SEL, side=side, price=prezzo, size=10.0,
    )
    atteso, prezzo_atteso = _esito_betfair(side, prezzo, back, lay)
    assert (risultato["matched_size"] > 0.0) is atteso, (side, back, lay, prezzo, risultato)
    if atteso:
        assert risultato["average_matched_price"] == pytest.approx(prezzo_atteso)


def test_motore_di_matching_consuma_il_lato_abbinato():
    from core.simulation_order_book import SimulationOrderBook

    order_book = SimulationOrderBook()
    order_book.update_market_book(MERCATO, _book(2.0, 2.02, size_back=100.0, size_lay=50.0))
    order_book.consume_liquidity(MERCATO, SEL, "BACK", 2.0, 10.0)
    assert order_book.get_best_back(MERCATO, SEL)["size"] == pytest.approx(90.0)
    assert order_book.get_best_lay(MERCATO, SEL)["size"] == pytest.approx(50.0)
    order_book.consume_liquidity(MERCATO, SEL, "LAY", 2.02, 5.0)
    assert order_book.get_best_lay(MERCATO, SEL)["size"] == pytest.approx(45.0)


# --------------------------------------------------------------------------
# LIVE, offline: BetfairService vero, BetfairClient vero, solo il trasporto
# JSON-RPC finto (risponde come Betfair)
# --------------------------------------------------------------------------
class _Exchange:
    """Trasporto JSON-RPC che risponde come l'exchange.

    - ``listMarketBook``: le ladder ci sono solo se la richiesta chiede
      ``priceProjection`` con ``EX_BEST_OFFERS`` (contratto Betfair);
    - ``placeOrders``: abbina con l'oracolo (``_esito_betfair``);
    - ``listCurrentOrders`` e ``cancelOrders`` come l'API.
    """

    def __init__(self, best_back: float, best_lay: float, ordini: List[Dict[str, Any]]):
        self.best_back = best_back
        self.best_lay = best_lay
        self.ordini = list(ordini)
        self.richieste: List[Tuple[str, Dict[str, Any]]] = []
        self.piazzati: List[Dict[str, Any]] = []
        self._ids = itertools.count(1)
        self._lock = threading.Lock()

    def __call__(self, url, method, params, **_kw):
        with self._lock:
            self.richieste.append((method, dict(params or {})))
            nome = method.rsplit("/", 1)[-1]
            if nome == "listMarketBook":
                return self._market_book(params)
            if nome == "listCurrentOrders":
                return {"currentOrders": [dict(o) for o in self.ordini], "moreAvailable": False}
            if nome == "placeOrders":
                return self._place(params)
            if nome == "cancelOrders":
                return {"status": "SUCCESS", "instructionReports": []}
            raise AssertionError(f"metodo inatteso nel test: {method}")

    def _market_book(self, params):
        runner: Dict[str, Any] = {"selectionId": SEL, "status": "ACTIVE"}
        proiezione = (params.get("priceProjection") or {}).get("priceData") or []
        if "EX_BEST_OFFERS" in proiezione:
            runner["ex"] = {
                "availableToBack": [{"price": self.best_back, "size": 100.0}],
                "availableToLay": [{"price": self.best_lay, "size": 100.0}],
            }
        return [{"marketId": MERCATO, "status": "OPEN", "runners": [runner]}]

    def _place(self, params):
        reports = []
        for istr in params.get("instructions") or []:
            lato = istr["side"]
            prezzo = float(istr["limitOrder"]["price"])
            size = float(istr["limitOrder"]["size"])
            abbinato, prezzo_fill = _esito_betfair(lato, prezzo, self.best_back, self.best_lay)
            bet_id = f"H{next(self._ids)}"
            ordine = {
                "betId": bet_id, "marketId": params["marketId"], "selectionId": istr["selectionId"],
                "side": lato, "priceSize": {"price": prezzo, "size": size},
                "averagePriceMatched": prezzo_fill or 0.0,
                "sizeMatched": size if abbinato else 0.0,
                "sizeRemaining": 0.0 if abbinato else size,
                "status": "EXECUTION_COMPLETE" if abbinato else "EXECUTABLE",
            }
            self.piazzati.append(ordine)
            reports.append({
                "status": "SUCCESS", "betId": bet_id, "sizeMatched": ordine["sizeMatched"],
                "averagePriceMatched": ordine["averagePriceMatched"],
                "orderStatus": ordine["status"], "instruction": istr,
            })
        return {"status": "SUCCESS", "marketId": params["marketId"], "instructionReports": reports}


@pytest.fixture
def headless_live(tmp_path, monkeypatch):
    """L'app headless vera, poi il servizio Betfair passato in LIVE con un
    ``BetfairClient`` vero il cui solo trasporto e' l'exchange finto."""
    import headless_main
    from betfair_client import BetfairClient
    from database import Database

    monkeypatch.chdir(tmp_path)
    percorso = str(tmp_path / "live.db")
    monkeypatch.setattr(headless_main, "Database", lambda: Database(percorso))
    app = headless_main.HeadlessApp()
    app.build(start_services=False)

    eventi: List[Tuple[str, Any]] = []
    pubblica = app.bus.publish

    def _registra(topic, payload=None, *a, **k):
        eventi.append((topic, payload))
        return pubblica(topic, payload, *a, **k)

    app.bus.publish = _registra

    exchange = _Exchange(best_back=1.98, best_lay=2.0, ordini=[_back_abbinato("L1")])
    client = BetfairClient(username="u", app_key="k", cert_pem="c", key_pem="p")
    client.session_token = "t"
    monkeypatch.setattr(client, "_post_jsonrpc", exchange)
    svc = app.betfair_service
    svc.simulation_mode = False
    svc.simulation_broker = None
    svc.client = client
    svc.connected = True
    # L'ordine del bot, nel ledger LIVE (tabella orders), come lo scrive il
    # percorso d'ordine: identita' I1 del router.
    app.db.insert_order({
        "customer_ref": "cr-l1", "correlation_id": "co-l1", "status": "MATCHED",
        "payload": {"market_id": MERCATO, "selection_id": SEL, "event_name": EVENTO},
        "response": {"bet_id": "L1"},
    })

    def _svuota(timeout=10.0):
        fatto = threading.Event()
        threading.Thread(target=lambda: (app.bus._queue.join(), fatto.set()), daemon=True).start()
        assert fatto.wait(timeout), "EventBus non drenato"

    yield app, exchange, eventi, _svuota
    try:
        svc.client = None
        app.stop()
    except Exception:
        pass


def test_live_cashout_chiede_i_prezzi_e_chiude_al_best_lay(headless_live):
    """In LIVE il router chiede il book CON i prezzi e banca al miglior
    ``availableToLay``: l'hedge si abbina sull'exchange e la posizione si chiude.

    Prima: book senza ladder (nessuna ``priceProjection``), ogni posizione
    saltata in silenzio; e anche coi prezzi, l'hedge LAY al miglior back (1,98)
    sarebbe rimasto a riposo."""
    app, exchange, eventi, svuota = headless_live
    app.runtime._route_cashout_signal({"signal_type": "CASHOUT_ALL", "source": "TELEGRAM"})
    svuota()

    libri = [p for m, p in exchange.richieste if m.endswith("listMarketBook")]
    assert libri, exchange.richieste
    assert all("EX_BEST_OFFERS" in ((p.get("priceProjection") or {}).get("priceData") or []) for p in libri)

    assert len(exchange.piazzati) == 1, exchange.piazzati
    hedge = exchange.piazzati[0]
    assert hedge["side"] == "LAY"
    assert hedge["priceSize"]["price"] == pytest.approx(2.0)
    assert hedge["priceSize"]["size"] == pytest.approx(10.10, abs=0.01)
    assert hedge["sizeMatched"] == pytest.approx(hedge["priceSize"]["size"])

    successi = [p for t, p in eventi if t == "CASHOUT_SUCCESS"]
    falliti = [p for t, p in eventi if t == "CASHOUT_FAILED"]
    assert len(successi) == 1 and falliti == [], eventi
