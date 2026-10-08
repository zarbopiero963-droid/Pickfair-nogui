"""PR26 (#461/#437, PKG-P23) — unica autorita' ordini e contratti adapter.

Gate owner su #461 (issuecomment-6053160870): LIVE_BLOCKED_UNTIL_PR26_DURABLE_DEDUPE.

FALSO SUCCESSO RIPRODOTTO in Phase 0 sul main `456fc94`. Il de-dup dell'engine
guardava solo `_inflight_keys` (rilasciate su SUCCESS/FAILURE) e
`Database.order_exists_inflight` (solo INFLIGHT/SUBMITTED). Con il client
Betfair VERO e una sessione HTTP che conta le POST:

    stesso customer_ref dopo un rifiuto del broker (FAILED post-invio)  -> POST=2
    stesso customer_ref dopo timeout (AMBIGUOUS) e restart              -> POST=2
    stesso customer_ref dopo COMPLETED e restart                        -> POST=2

La finestra di 60 s di Betfair sul customerRef non copre un restart o una
riconsegna Telegram tardiva. Ora l'intento consegnato al trasporto e' marcato
in modo durevole (`consumed_order_intents`) PRIMA dell'invio e non si libera
con lo stato terminale: si toglie la marcatura solo con la prova che nulla e'
partito.

Inclusi i follow-up broker/adapter della scheda: F8, F10, F14, F15, e le
firme al confine adapter di OrderManager (busta BetfairClient, cancel singolo).
"""
from __future__ import annotations

import json
import math
import threading
import time
from typing import Any, Dict, List, Optional

import pytest
import requests

from betfair_client import BetfairClient
from core.safety_layer import MarketSanityError, RiskInvariantError, SafetyLayer
from core.trading_engine import (
    STATUS_AMBIGUOUS,
    STATUS_DUPLICATE_BLOCKED,
    STATUS_FAILED,
    STATUS_ACCEPTED_FOR_PROCESSING,
    TradingEngine,
)
from database import Database
from order_manager import OrderManager, OrderStatus, ValidationError
from simulation_broker import SimulationBroker

pytestmark = [pytest.mark.integration, pytest.mark.safety]

# ACK di un invio riuscito: l'ordine e' SUBMITTED su DB, non ancora terminale.
_ACK = STATUS_ACCEPTED_FOR_PROCESSING


# --------------------------------------------------------------------------
# Confine del trasporto: il BetfairClient VERO con una sessione HTTP finta
# --------------------------------------------------------------------------
class _Risposta:
    status_code = 200

    def __init__(self, corpo: Any) -> None:
        self._corpo = corpo

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Any:
        return self._corpo


def _rpc(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [{"jsonrpc": "2.0", "id": 1, "result": result}]


_PIAZZATO = {"status": "SUCCESS", "instructionReports": [
    {"status": "SUCCESS", "betId": "B-1", "sizeMatched": 0.0, "orderStatus": "EXECUTABLE"}]}
_RIFIUTATO = {"status": "FAILURE", "errorCode": "MARKET_SUSPENDED", "instructionReports": []}


class _SessioneHttp:
    """Conta le POST di placeOrders. `esiti`: corpi o eccezioni, in ordine."""

    def __init__(self, *esiti: Any, attesa: float = 0.0) -> None:
        self.esiti = list(esiti) or [_PIAZZATO]
        self.post_inviate: List[Dict[str, Any]] = []
        self.attesa = attesa
        self._lock = threading.Lock()

    def post(self, url, **kwargs):
        corpo = json.loads(kwargs["data"])
        with self._lock:
            self.post_inviate.append(corpo[0]["params"])
            esito = self.esiti.pop(0) if len(self.esiti) > 1 else self.esiti[0]
        if self.attesa:
            time.sleep(self.attesa)
        if isinstance(esito, BaseException):
            raise esito
        return _Risposta(_rpc(esito))


def _client_reale(sessione: _SessioneHttp) -> BetfairClient:
    client = BetfairClient(username="u", app_key="a", cert_pem="c.pem",
                           key_pem="k.pem", session=sessione)
    client.session_token = "TOK"
    return client


# --------------------------------------------------------------------------
# Engine reale con DB SQLite reale
# --------------------------------------------------------------------------
class _Bus:
    def __init__(self) -> None:
        self.eventi: List[tuple] = []
        self._sub: Dict[str, List[Any]] = {}

    def subscribe(self, topic, handler):
        self._sub.setdefault(topic, []).append(handler)

    def publish(self, topic, payload=None):
        self.eventi.append((topic, payload))
        for handler in list(self._sub.get(topic, [])):
            handler(payload)

    def nomi(self) -> List[str]:
        return [t for t, _ in self.eventi]


class _Executor:
    def submit(self, _nome, fn):
        return fn()


class _Riconciliazione:
    def __init__(self) -> None:
        self.accodati: List[Dict[str, Any]] = []

    def enqueue(self, **meta):
        self.accodati.append(meta)


class _Servizio:
    def __init__(self, simulation_broker=None) -> None:
        self.simulation_broker = simulation_broker
        self._session_invalid = False


class _Runtime:
    def __init__(self, modo: str = "LIVE", simulation_broker=None) -> None:
        self.modo = modo
        self.is_emergency_stopped = False
        self.betfair_service = _Servizio(simulation_broker)

    def get_effective_execution_mode(self) -> str:
        return self.modo

    def is_live_allowed(self) -> bool:
        return self.modo == "LIVE"


def _engine(db: Any, client: Any, *, modo: str = "LIVE", simulation_broker=None,
            getter_restituisce_client: bool = True):
    bus, rec = _Bus(), _Riconciliazione()
    eng = TradingEngine(
        bus=bus, db=db,
        client_getter=(lambda: client) if getter_restituisce_client else (lambda: None),
        executor=_Executor(), reconciliation_engine=rec,
    )
    eng.runtime_controller = _Runtime(modo, simulation_broker)
    return eng, bus, rec


def _richiesta(ref: str = "PF26REF0001", **extra: Any) -> Dict[str, Any]:
    base = {"market_id": "1.234", "selection_id": 5678, "bet_type": "BACK",
            "price": 2.0, "stake": 5.0, "customer_ref": ref,
            "event_key": "1.234:5678:BACK"}
    base.update(extra)
    return base


def _riavvio(db_path: str, client: Any, **kw):
    """Nuovo processo: nuovo Database sullo stesso file, nuovo engine."""
    db = Database(db_path)
    eng, bus, rec = _engine(db, client, **kw)
    eng._repopulate_inflight_from_db()
    return eng, db, bus, rec


@pytest.fixture()
def db_path(tmp_path) -> str:
    return str(tmp_path / "pickfair_pr26.db")


# ==========================================================================
# 1. De-dup durevole: un intento inviato non riparte mai
# ==========================================================================
def test_block_rifiuto_broker_post_invio_stesso_ref_non_reinvia(db_path):
    """FAILED dopo l'invio (rifiuto esplicito di Betfair): la chiave in memoria
    si libera, ma l'intento e' consumato. Sul main: POST=2."""
    sessione = _SessioneHttp(_RIFIUTATO, _PIAZZATO)
    db = Database(db_path)
    eng, _bus, _rec = _engine(db, _client_reale(sessione))

    primo = eng.submit_quick_bet(_richiesta())
    assert primo["status"] == STATUS_FAILED
    assert len(sessione.post_inviate) == 1

    secondo = eng.submit_quick_bet(_richiesta())
    assert secondo["status"] == STATUS_DUPLICATE_BLOCKED
    assert len(sessione.post_inviate) == 1


def test_block_timeout_ambiguo_poi_restart_nessun_secondo_ordine(db_path):
    """Timeout: AMBIGUOUS, una sola riconciliazione, nessun replay. Dopo un
    restart lo stesso intento resta bloccato. Sul main: POST=2."""
    sessione = _SessioneHttp(requests.exceptions.Timeout("read timeout"), _PIAZZATO)
    db = Database(db_path)
    eng, _bus, rec = _engine(db, _client_reale(sessione))

    primo = eng.submit_quick_bet(_richiesta())
    assert primo["status"] == STATUS_AMBIGUOUS
    assert len(rec.accodati) == 1
    assert len(sessione.post_inviate) == 1

    eng2, db2, _bus2, rec2 = _riavvio(db_path, _client_reale(sessione))
    secondo = eng2.submit_quick_bet(_richiesta())
    assert secondo["status"] == STATUS_DUPLICATE_BLOCKED
    assert len(sessione.post_inviate) == 1
    assert rec2.accodati == []


def test_block_completed_poi_restart_nessun_secondo_ordine(db_path):
    """Ordine piazzato e poi COMPLETED (stato terminale scritto dalla
    riconciliazione): dopo un restart, o oltre i 60 s di Betfair, la stessa
    riconsegna non piazza. Sul main: POST=2."""
    sessione = _SessioneHttp(_PIAZZATO)
    db = Database(db_path)
    eng, _bus, _rec = _engine(db, _client_reale(sessione))

    primo = eng.submit_quick_bet(_richiesta())
    assert primo["status"] == _ACK
    db.update_order(primo["order_id"], {"status": "COMPLETED", "finalized": True})

    eng2, _db2, _bus2, _rec2 = _riavvio(db_path, _client_reale(sessione))
    secondo = eng2.submit_quick_bet(_richiesta())
    assert secondo["status"] == STATUS_DUPLICATE_BLOCKED
    assert len(sessione.post_inviate) == 1


def test_block_risposta_persa_dopo_invio_resta_ambiguo_e_consumato(db_path):
    """Il trasporto ha spedito, poi il post-processing esplode (TypeError):
    AMBIGUOUS (fix #452/#478 conservato), e l'intento resta consumato."""

    class _ClientCheEsplodeDopo:
        def __init__(self):
            self.invii = 0

        def place_bet(self, *, market_id, selection_id, side, price, size, customer_ref=""):
            self.invii += 1
            raise TypeError("'NoneType' object is not subscriptable")

    client = _ClientCheEsplodeDopo()
    db = Database(db_path)
    eng, _bus, rec = _engine(db, client)

    primo = eng.submit_quick_bet(_richiesta())
    assert primo["status"] == STATUS_AMBIGUOUS
    assert client.invii == 1
    assert len(rec.accodati) == 1
    assert db.is_order_intent_consumed("PF26REF0001") is True

    secondo = eng.submit_quick_bet(_richiesta())
    assert secondo["status"] == STATUS_DUPLICATE_BLOCKED
    assert client.invii == 1


def test_block_crash_dopo_marcatura_prima_dell_invio(db_path):
    """Crash fra il commit della marcatura e l'invio: al restart l'intento e'
    consumato e non riparte da solo (decide la riconciliazione)."""
    db = Database(db_path)
    assert db.consume_order_intent("PF26REF0001", attempt_id="A-CRASH",
                                   correlation_id="C-1") is True
    sessione = _SessioneHttp(_PIAZZATO)
    eng2, _db2, _bus2, _rec2 = _riavvio(db_path, _client_reale(sessione))

    esito = eng2.submit_quick_bet(_richiesta())
    assert esito["status"] == STATUS_DUPLICATE_BLOCKED
    assert sessione.post_inviate == []


# ==========================================================================
# 2. Retry ammesso solo con la prova che nulla e' partito
# ==========================================================================
@pytest.mark.parametrize("campo,valore,codice", [
    ("price", float("nan"), "INVALID_PRICE"),
    ("stake", float("inf"), "INVALID_SIZE"),
    ("selection_id", 0, "INVALID_SELECTION_ID"),
])
def test_pass_rifiuto_pre_invio_del_client_reale_libera_l_intento(db_path, campo, valore, codice):
    sessione = _SessioneHttp(_PIAZZATO)
    db = Database(db_path)
    eng, _bus, _rec = _engine(db, _client_reale(sessione))

    primo = eng.submit_quick_bet(_richiesta(**{campo: valore}))
    assert primo["status"] == STATUS_FAILED
    assert codice in str(primo.get("error"))
    assert sessione.post_inviate == []
    assert db.is_order_intent_consumed("PF26REF0001") is False

    secondo = eng.submit_quick_bet(_richiesta())
    assert secondo["status"] == _ACK
    assert len(sessione.post_inviate) == 1


def test_pass_lato_invalido_rifiutato_dall_engine_libera_l_intento(db_path):
    sessione = _SessioneHttp(_PIAZZATO)
    db = Database(db_path)
    eng, _bus, _rec = _engine(db, _client_reale(sessione))

    primo = eng.submit_quick_bet(_richiesta(bet_type="BAKC"))
    assert primo["status"] == STATUS_FAILED
    assert sessione.post_inviate == []
    assert db.is_order_intent_consumed("PF26REF0001") is False


def test_pass_stop_rispettato_nessun_invio_nessuna_marcatura(db_path):
    """STOP d'emergenza: blocco prima del trasporto, intento NON consumato.
    Tolto lo STOP, lo stesso intento parte una volta sola."""
    sessione = _SessioneHttp(_PIAZZATO)
    db = Database(db_path)
    eng, _bus, _rec = _engine(db, _client_reale(sessione))
    eng.runtime_controller.is_emergency_stopped = True

    bloccato = eng.submit_quick_bet(_richiesta())
    assert bloccato["status"] == STATUS_FAILED
    assert "EMERGENCY_STOP_ACTIVE" in str(bloccato.get("error"))
    assert sessione.post_inviate == []
    assert db.is_order_intent_consumed("PF26REF0001") is False

    eng.runtime_controller.is_emergency_stopped = False
    assert eng.submit_quick_bet(_richiesta())["status"] == _ACK
    assert eng.submit_quick_bet(_richiesta())["status"] == STATUS_DUPLICATE_BLOCKED
    assert len(sessione.post_inviate) == 1


def test_block_live_senza_capacita_durevole_non_invia():
    """LIVE dichiarato e DB senza de-dup durevole: nessun invio (fail-closed)."""

    class _DbSenzaDedupDurevole:
        def __init__(self):
            self.rows: Dict[str, Dict[str, Any]] = {}

        def insert_order(self, payload):
            oid = str(len(self.rows) + 1)
            self.rows[oid] = dict(payload)
            return oid

        def update_order(self, oid, update):
            self.rows[oid].update(update)

        def get_order(self, oid):
            return dict(self.rows[oid])

        def insert_audit_event(self, _event):
            return None

    sessione = _SessioneHttp(_PIAZZATO)
    eng, _bus, _rec = _engine(_DbSenzaDedupDurevole(), _client_reale(sessione))
    esito = eng.submit_quick_bet(_richiesta())
    assert esito["status"] == STATUS_FAILED
    assert "DURABLE_DEDUPE_UNAVAILABLE" in str(esito.get("error"))
    assert sessione.post_inviate == []


# ==========================================================================
# 3. Concorrenza: due processi sullo stesso file SQLite
# ==========================================================================
def test_block_corsa_persa_non_invia_e_non_cancella_la_marcatura_altrui(db_path):
    """Finestra di corsa riprodotta in modo deterministico: il secondo engine
    ha gia' superato i controlli di lettura quando il primo ha consumato
    l'intento. La scrittura atomica lo ferma. Sul main: POST=2."""
    sessione = _SessioneHttp(_PIAZZATO)
    db_a, db_b = Database(db_path), Database(db_path)
    eng_a, _ba, _ra = _engine(db_a, _client_reale(sessione))
    eng_b, _bb, _rb = _engine(db_b, _client_reale(sessione))
    eng_b._is_duplicate_in_db = lambda _ctx: False
    eng_b._intento_gia_consumato = lambda _ctx: False

    assert eng_a.submit_quick_bet(_richiesta())["status"] == _ACK
    perdente = eng_b.submit_quick_bet(_richiesta())

    assert perdente["status"] == STATUS_FAILED
    assert "INTENT_ALREADY_CONSUMED" in str(perdente.get("error"))
    assert len(sessione.post_inviate) == 1
    assert db_a.is_order_intent_consumed("PF26REF0001") is True


def test_pass_marcatura_atomica_fra_thread_e_connessioni(db_path):
    dbs = [Database(db_path) for _ in range(8)]
    barriera = threading.Barrier(len(dbs))
    vinti: List[bool] = []

    def _prova(i):
        barriera.wait()
        vinti.append(dbs[i].consume_order_intent("PF26RACE", attempt_id=f"A{i}"))

    threads = [threading.Thread(target=_prova, args=(i,)) for i in range(len(dbs))]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    assert sorted(vinti) == [False] * 7 + [True]
    assert dbs[0].release_order_intent("PF26RACE", attempt_id="NON-MIO") is False
    assert dbs[0].is_order_intent_consumed("PF26RACE") is True


def test_pass_due_engine_in_parallelo_un_solo_invio_per_intento(db_path):
    sessione = _SessioneHttp(_PIAZZATO, attesa=0.01)
    motori = [_engine(Database(db_path), _client_reale(sessione))[0] for _ in range(2)]
    refs = [f"PF26PAR{i:03d}" for i in range(10)]
    barriera = threading.Barrier(2)

    def _lavora(eng):
        barriera.wait()
        for ref in refs:
            eng.submit_quick_bet(_richiesta(ref))

    threads = [threading.Thread(target=_lavora, args=(m,)) for m in motori]
    for t in threads:
        t.start()
    for t in threads:
        t.join(30)
    inviati = [p.get("customerRef") for p in sessione.post_inviate]
    assert sorted(inviati) == sorted(refs)


# ==========================================================================
# 4. DUPLICATE non rilascia la chiave dell'originale
# ==========================================================================
def test_block_duplicato_non_rilascia_la_chiave_dell_originale(db_path):
    sessione = _SessioneHttp(_PIAZZATO)
    db = Database(db_path)
    eng, _bus, _rec = _engine(db, _client_reale(sessione))

    assert eng.submit_quick_bet(_richiesta())["status"] == _ACK
    assert eng.submit_quick_bet(_richiesta())["status"] == STATUS_DUPLICATE_BLOCKED
    assert "PF26REF0001" in eng._inflight_keys


# ==========================================================================
# 5. SIM: il broker dichiarato e' l'autorita', con lo stesso adapter
# ==========================================================================
def _broker_con_liquidita() -> SimulationBroker:
    sb = SimulationBroker(starting_balance=1000.0)
    sb.update_market_book({"marketId": "1.234", "runners": [{"selectionId": 5678, "ex": {
        "availableToBack": [{"price": 2.0, "size": 100.0}],
        "availableToLay": [{"price": 2.02, "size": 100.0}],
    }}]})
    return sb


def test_pass_sim_broker_dichiarato_senza_execute_piazza_una_volta(db_path):
    """`engine.simulation_broker` impostato e getter che non lo restituisce:
    sul main NO_VALID_EXECUTION_PATH (SimulationBroker non ha execute())."""
    sb = _broker_con_liquidita()
    db = Database(db_path)
    eng, _bus, _rec = _engine(db, None, modo="SIMULATION", simulation_broker=sb,
                              getter_restituisce_client=False)
    eng.simulation_broker = sb

    primo = eng.submit_quick_bet(_richiesta())
    assert primo["status"] == _ACK
    assert len(sb.state.orders) == 1
    ordine = next(iter(sb.state.orders.values()))
    assert (ordine.side, ordine.size, ordine.customer_ref) == ("BACK", 5.0, "PF26REF0001")

    assert eng.submit_quick_bet(_richiesta())["status"] == STATUS_DUPLICATE_BLOCKED
    assert len(sb.state.orders) == 1


def test_pass_sim_quota_invalida_nessuna_registrazione_e_retry_ammesso(db_path):
    sb = _broker_con_liquidita()
    db = Database(db_path)
    eng, _bus, _rec = _engine(db, sb, modo="SIMULATION", simulation_broker=sb)
    fondi = sb.get_account_funds()

    primo = eng.submit_quick_bet(_richiesta(price=float("nan")))
    assert primo["status"] == STATUS_FAILED
    assert sb.state.orders == {}
    assert sb.get_account_funds() == fondi
    assert db.is_order_intent_consumed("PF26REF0001") is False

    assert eng.submit_quick_bet(_richiesta())["status"] == _ACK
    assert len(sb.state.orders) == 1


def test_pass_riconsegna_cmd_quick_bet_dopo_restart_un_solo_ordine_sim(db_path):
    """Percorso dei produttori (segnale Telegram/GUI/headless, auto-next,
    gambe dutching, manual_bet): tutti pubblicano CMD_QUICK_BET verso l'engine.
    Riconsegna dello stesso intento, anche dopo un restart: un ordine."""
    sb = _broker_con_liquidita()
    db = Database(db_path)
    eng, bus, _rec = _engine(db, sb, modo="SIMULATION", simulation_broker=sb)
    bus.publish("CMD_QUICK_BET", _richiesta())
    bus.publish("CMD_QUICK_BET", _richiesta())
    db.update_order("1", {"status": "COMPLETED", "finalized": True})

    eng2, _db2, bus2, _rec2 = _riavvio(db_path, sb, modo="SIMULATION", simulation_broker=sb)
    bus2.publish("CMD_QUICK_BET", _richiesta())
    assert len(sb.state.orders) == 1


# ==========================================================================
# 6. F8 / F10: SimulationBroker
# ==========================================================================
def test_f8_ordine_sim_accettato_non_abbinato_e_success_executable():
    sb = SimulationBroker(starting_balance=1000.0)
    esito = sb.place_bet(market_id="1.999", selection_id=1, side="BACK", price=3.0, size=2.0)
    report = esito["instructionReports"][0]
    assert report["status"] == "SUCCESS"
    assert report["orderStatus"] == "EXECUTABLE"
    assert report["sizeMatched"] == 0.0
    assert report["betId"] in sb.state.orders


@pytest.mark.parametrize("campo,valore,codice", [
    ("market_id", "", "INVALID_MARKET_ID"),
    ("selection_id", 0, "INVALID_SELECTION_ID"),
    ("selection_id", "bad", "INVALID_SELECTION_ID"),
    ("price", 1.0, "INVALID_PRICE"),
    ("price", float("nan"), "INVALID_PRICE"),
    ("price", float("inf"), "INVALID_PRICE"),
    ("size", 0.0, "INVALID_SIZE"),
    ("size", float("nan"), "INVALID_SIZE"),
])
def test_f10_argomenti_invalidi_rifiutati_prima_di_registrare(campo, valore, codice):
    sb = _broker_con_liquidita()
    fondi = sb.get_account_funds()
    args = {"market_id": "1.234", "selection_id": 5678, "side": "BACK",
            "price": 2.0, "size": 5.0}
    args[campo] = valore
    with pytest.raises(RuntimeError, match=f"^{codice}$"):
        sb.place_bet(**args)
    assert sb.state.orders == {}
    assert sb.get_account_funds() == fondi


# ==========================================================================
# 7. F14 e firme al confine adapter: OrderManager
# ==========================================================================
class _DbSaga:
    def __init__(self):
        self.sagas: Dict[str, Dict[str, Any]] = {}

    def create_order_saga(self, **kw):
        self.sagas[kw["customer_ref"]] = dict(kw)

    def get_order_saga(self, ref):
        return self.sagas.get(ref)

    def get_order_saga_by_logical_key(self, _key):
        return None

    def update_order_saga(self, **kw):
        self.sagas[kw["customer_ref"]].update(kw)


def _om(client) -> OrderManager:
    return OrderManager(db=_DbSaga(), client_getter=lambda: client, sleep_fn=lambda *_: None)


_OM_PAYLOAD = {"market_id": "1.234", "selection_id": 5678, "bet_type": "BACK",
               "price": 2.0, "stake": 5.0, "customer_ref": "PF26OM01"}


@pytest.mark.parametrize("codice", ["INVALID_PRICE", "INVALID_SIZE",
                                    "INVALID_SELECTION_ID", "INVALID_SIDE"])
def test_f14_rifiuto_certo_pre_invio_e_failed_non_ambiguous(codice):
    class _Client:
        def place_bet(self, *, market_id, selection_id, side, price, size, customer_ref=""):
            raise RuntimeError(codice)

    esito = _om(_Client()).place_order(dict(_OM_PAYLOAD))
    assert esito["status"] == OrderStatus.FAILED.value
    assert esito["error_class"] == "PERMANENT"


@pytest.mark.parametrize("campo,valore", [
    ("price", float("nan")), ("price", float("inf")),
    ("stake", float("nan")), ("stake", float("inf")),
    ("bet_type", "BAKC"), ("bet_type", 1),
])
def test_f14_payload_invalido_rifiutato_prima_del_client(campo, valore):
    sessione = _SessioneHttp(_PIAZZATO)
    om = _om(_client_reale(sessione))
    with pytest.raises(ValidationError):
        om.place_order({**_OM_PAYLOAD, campo: valore})
    assert sessione.post_inviate == []
    assert om.db.sagas == {}


def test_om_busta_betfair_client_ok_letta_come_piazzato():
    sessione = _SessioneHttp(_PIAZZATO)
    esito = _om(_client_reale(sessione)).place_order(dict(_OM_PAYLOAD))
    assert esito["status"] == OrderStatus.PLACED.value
    assert esito["bet_id"] == "B-1"
    assert len(sessione.post_inviate) == 1


def test_om_busta_betfair_client_timeout_e_ambiguous_non_failed():
    sessione = _SessioneHttp(requests.exceptions.Timeout("read timeout"))
    esito = _om(_client_reale(sessione)).place_order(dict(_OM_PAYLOAD))
    assert esito["status"] == OrderStatus.AMBIGUOUS.value
    assert len(sessione.post_inviate) == 1


def test_om_busta_betfair_client_rifiuto_e_failed():
    sessione = _SessioneHttp(_RIFIUTATO)
    esito = _om(_client_reale(sessione)).place_order(dict(_OM_PAYLOAD))
    assert esito["status"] == OrderStatus.FAILED.value
    assert esito["reason_code"] == "BROKER_REJECTED"


def _om_piazzato(client) -> OrderManager:
    om = _om(client)
    om.db.sagas["PF26OM01"] = {"customer_ref": "PF26OM01", "status": OrderStatus.PLACED.value}
    return om


def test_om_cancel_client_reale_un_solo_bet_id():
    class _ClientCancel:
        def __init__(self):
            self.chiamate = []

        def cancel_orders(self, *, market_id, bet_ids=None):  # firma di BetfairClient
            self.chiamate.append((market_id, bet_ids))
            return {"ok": True, "status": "SUCCESS", "result": {
                "status": "SUCCESS",
                "instructionReports": [{"status": "SUCCESS", "sizeCancelled": 5.0}]}}

    client = _ClientCancel()
    esito = _om_piazzato(client).cancel_order("PF26OM01", bet_id="B-1", market_id="1.234")
    assert client.chiamate == [("1.234", ["B-1"])]
    assert esito["status"] == OrderStatus.CANCELLED.value


def test_om_cancel_simulation_broker_reale():
    sb = SimulationBroker(starting_balance=1000.0)
    bet_id = sb.place_bet(market_id="1.234", selection_id=5678, side="BACK",
                          price=3.0, size=5.0)["instructionReports"][0]["betId"]
    altro = sb.place_bet(market_id="1.234", selection_id=5678, side="BACK",
                         price=3.0, size=5.0)["instructionReports"][0]["betId"]
    esito = _om_piazzato(sb).cancel_order("PF26OM01", bet_id=bet_id, market_id="1.234")
    assert esito["status"] == OrderStatus.CANCELLED.value
    assert sb.state.orders[bet_id].status == "CANCELLED"
    assert sb.state.orders[altro].status == "EXECUTABLE"


@pytest.mark.parametrize("kwargs", [{"bet_id": ""}, {"bet_id": "B-1", "size_reduction": 1.0}])
def test_om_cancel_mai_cancella_tutto_il_mercato(kwargs):
    class _ClientCancel:
        def __init__(self):
            self.chiamate = []

        def cancel_orders(self, *, market_id, bet_ids=None):
            self.chiamate.append((market_id, bet_ids))
            return {"ok": True}

    client = _ClientCancel()
    om = _om_piazzato(client)
    with pytest.raises(ValidationError):
        om.cancel_order("PF26OM01", market_id="1.234", **kwargs)
    assert client.chiamate == []
    assert om.db.sagas["PF26OM01"]["status"] == OrderStatus.PLACED.value


# ==========================================================================
# 8. F15: SafetyLayer cashout
# ==========================================================================
_CASHOUT = {"market_id": "1.234", "selection_id": 5678, "side": "LAY",
            "price": 2.0, "stake": 5.0, "green_up": 1.0}


@pytest.mark.parametrize("campo,valore,errore", [
    ("price", float("nan"), MarketSanityError),
    ("price", float("inf"), MarketSanityError),
    ("stake", float("nan"), RiskInvariantError),
    ("stake", float("inf"), RiskInvariantError),
])
def test_f15_cashout_non_finito_rifiutato(campo, valore, errore):
    layer = SafetyLayer()
    payload = dict(_CASHOUT)
    payload[campo] = valore
    with pytest.raises(errore):
        layer.validate_cashout_request(payload)


def test_f15_cashout_valido_resta_valido():
    assert SafetyLayer().validate_cashout_request(dict(_CASHOUT)) is True
