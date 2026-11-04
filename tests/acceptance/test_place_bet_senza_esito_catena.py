"""P0 placeOrders senza esito — la CATENA dopo il client (mandato owner 04/11/2026 a Claude D).

`test_place_bet_risposta_senza_esito.py` prova il client e l'engine con stub. Qui la catena reale:

BetfairClient (trasporto finto, POST contati) -> TradingEngine -> Database SQLite su file -> evento terminale sul bus
-> RuntimeController._release_if_terminal (TableManager + DuplicationGuard reali) -> riavvio (nuovo Database e nuovo
engine sullo stesso file).

Per ogni corpo 2xx senza esito si verifica che l'ordine:
- sia registrato AMBIGUOUS su DB (mai FAILED), con il suo customer_ref;
- vada in riconciliazione con il customer_ref;
- pubblichi QUICK_BET_AMBIGUOUS e non QUICK_BET_FAILED, cosi' il runtime NON libera tavolo, esposizione e chiave;
- porti sul filo il customerOrderRef (identita' con cui la riconciliazione lo ritrova in listCurrentOrders);
- dopo un riavvio blocchi lo stesso intento senza un secondo POST.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Tuple

import pytest

import betfair_client
from core.duplication_guard import DuplicationGuard
from core.order_ref import customer_order_ref
from core.runtime_controller import RuntimeController
from core.table_manager import TableManager
from core.trading_engine import TradingEngine
from database import Database

from tests.acceptance.test_place_bet_risposta_senza_esito import RISPOSTE_SENZA_ESITO

CUSTOMER_REF = "PFCHAINP0X1"
EVENT_KEY = "EV-P0-CHAIN"


class _RispostaHttp:
    status_code = 200

    def __init__(self, corpo: Any) -> None:
        self._corpo = corpo

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Any:
        return self._corpo


class _Trasporto:
    """Sessione HTTP finta: conta e conserva ogni POST (un POST = un invio a Betfair)."""

    def __init__(self, corpo: Any) -> None:
        self.corpo = corpo
        self.inviati: List[Dict[str, Any]] = []

    def post(self, _url: str, **kw: Any) -> Any:
        self.inviati.append(json.loads(kw["data"]))
        return _RispostaHttp(self.corpo)


class _Bus:
    def __init__(self) -> None:
        self.eventi: List[Tuple[str, Any]] = []

    def subscribe(self, *_a: Any, **_k: Any) -> None:
        return None

    def publish(self, nome: str, payload: Any = None) -> None:
        self.eventi.append((nome, payload))


class _Riconciliazione:
    def __init__(self) -> None:
        self.accodati: List[Dict[str, Any]] = []

    def enqueue(self, **meta: Any) -> None:
        self.accodati.append(meta)


class _RuntimeLive:
    def get_effective_execution_mode(self) -> str:
        return "LIVE"

    def is_live_allowed(self) -> bool:
        return True

    is_emergency_stopped = False
    betfair_service = None


def _engine(db: Database, trasporto: _Trasporto, bus: _Bus, ric: _Riconciliazione) -> TradingEngine:
    client = betfair_client.BetfairClient(
        username="u", app_key="a", cert_pem="c.pem", key_pem="k.pem", session=trasporto, max_retries=2,
    )
    client.session_token = "TOK"
    eng = TradingEngine(bus=bus, db=db, client_getter=lambda: client, executor=None,
                        reconciliation_engine=ric)
    eng._runtime_state = "READY"
    eng.runtime_controller = _RuntimeLive()
    return eng


def _richiesta() -> Dict[str, Any]:
    return {"customer_ref": CUSTOMER_REF, "event_key": EVENT_KEY, "table_id": 1,
            "market_id": "1.23456789", "selection_id": 47999, "bet_type": "BACK", "price": 2.5, "stake": 10.0}


def _runtime_con_riserva() -> RuntimeController:
    """RuntimeController reale ridotto al rilascio: tavolo 1 attivo e chiave anti-duplicato acquisita."""
    rc = RuntimeController.__new__(RuntimeController)
    rc.table_manager = TableManager(table_count=2)
    rc.duplication_guard = DuplicationGuard()
    assert rc.duplication_guard.acquire(EVENT_KEY)
    rc.table_manager.activate(table_id=1, event_key=EVENT_KEY, exposure=10.0, market_id="1.23456789",
                              selection_id=47999, meta={"customer_ref": CUSTOMER_REF})
    return rc


def _terminali(bus: _Bus) -> List[Tuple[str, Any]]:
    return [(n, p) for n, p in bus.eventi if n in ("QUICK_BET_AMBIGUOUS", "QUICK_BET_FAILED")]


@pytest.mark.parametrize("corpo", list(RISPOSTE_SENZA_ESITO.values()), ids=list(RISPOSTE_SENZA_ESITO))
def test_block_catena_ambigua_trattiene_tutto_e_sopravvive_al_riavvio(tmp_path, corpo: Any) -> None:
    percorso_db = str(tmp_path / "pickfair.sqlite3")
    trasporto, bus, ric = _Trasporto(corpo), _Bus(), _Riconciliazione()
    db = Database(percorso_db)

    esito = _engine(db, trasporto, bus, ric).submit_quick_bet(_richiesta())

    # 1. Un solo invio, con l'identita' che la riconciliazione usa per ritrovarlo.
    assert len(trasporto.inviati) == 1
    params = trasporto.inviati[0][0]["params"]
    assert params["instructions"][0].get("customerOrderRef") == customer_order_ref(CUSTOMER_REF)

    # 2. Ledger: AMBIGUOUS, mai FAILED.
    assert esito["status"] == "AMBIGUOUS", esito
    riga = db.get_order(esito["order_id"])
    assert riga["status"] == "AMBIGUOUS", f"DB registra {riga['status']!r} per un esito ignoto"
    assert riga["customer_ref"] == CUSTOMER_REF

    # 3. Riconciliazione richiesta con il customer_ref.
    assert len(ric.accodati) == 1
    assert ric.accodati[0].get("customer_ref") == CUSTOMER_REF

    # 4. Evento terminale: solo AMBIGUOUS; il runtime reale non libera nulla.
    terminali = _terminali(bus)
    assert [n for n, _ in terminali] == ["QUICK_BET_AMBIGUOUS"], terminali
    rc = _runtime_con_riserva()
    nome, payload = terminali[0]
    rc._release_if_terminal(payload, event_name=nome)
    assert rc.table_manager.total_exposure() == 10.0, "riserva/tavolo liberati su esito ignoto"
    assert not rc.duplication_guard.acquire(EVENT_KEY), "chiave anti-duplicato liberata su esito ignoto"

    # 5. Riavvio: nuovo DB e nuovo engine sullo stesso file. Lo stesso intento non riparte.
    trasporto2, bus2, ric2 = _Trasporto(corpo), _Bus(), _Riconciliazione()
    db2 = Database(percorso_db)
    assert db2.get_order(esito["order_id"])["status"] == "AMBIGUOUS"
    secondo = _engine(db2, trasporto2, bus2, ric2).submit_quick_bet(_richiesta())
    assert secondo["status"] == "DUPLICATE_BLOCKED", secondo
    assert trasporto2.inviati == [], "secondo POST dopo il riavvio su un ordine forse vivo"


def test_rifiuto_esplicito_nella_catena_libera_come_prima(tmp_path) -> None:
    """Controllo opposto: un FAILURE dichiarato da Betfair resta FAILED e il runtime libera tavolo e chiave."""
    corpo = [{"result": {"status": "FAILURE", "errorCode": "MARKET_SUSPENDED",
                         "instructionReports": [{"status": "FAILURE", "errorCode": "MARKET_SUSPENDED"}]}}]
    trasporto, bus, ric = _Trasporto(corpo), _Bus(), _Riconciliazione()
    db = Database(str(tmp_path / "pickfair.sqlite3"))

    esito = _engine(db, trasporto, bus, ric).submit_quick_bet(_richiesta())

    assert esito["status"] == "FAILED", esito
    assert db.get_order(esito["order_id"])["status"] == "FAILED"
    assert ric.accodati == []
    terminali = _terminali(bus)
    assert [n for n, _ in terminali] == ["QUICK_BET_FAILED"], terminali
    rc = _runtime_con_riserva()
    nome, payload = terminali[0]
    rc._release_if_terminal(payload, event_name=nome)
    assert rc.table_manager.total_exposure() == 0.0
    assert rc.duplication_guard.acquire(EVENT_KEY)


@pytest.mark.parametrize("corpo", list(RISPOSTE_SENZA_ESITO.values()), ids=list(RISPOSTE_SENZA_ESITO))
def test_block_order_manager_risposta_senza_esito_ambigua(corpo: Any) -> None:
    """Percorso OrderManager (saga) col BetfairClient reale: AMBIGUOUS, un solo POST, mai FAILED.

    Oggi OrderManager non e' istanziato dal wiring di produzione (TradingEngine.order_manager resta None), ma e'
    un percorso d'ordine pubblico: deve leggere lo stesso esito ignoto."""
    from order_manager import OrderManager

    trasporto = _Trasporto(corpo)
    client = betfair_client.BetfairClient(
        username="u", app_key="a", cert_pem="c.pem", key_pem="k.pem", session=trasporto, max_retries=2,
    )
    client.session_token = "TOK"
    om = OrderManager(client_getter=lambda: client, sleep_fn=lambda *_a: None)

    esito = om.place_order({**_richiesta(), "customer_ref": "PFOMP0X1"})

    assert esito["status"] == "AMBIGUOUS", esito
    assert len(trasporto.inviati) == 1, "secondo POST su un esito ignoto"
