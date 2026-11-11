"""Gate A — batch A, componente trading (TradingEngine).

Test indipendenti per comportamenti dichiarati PRESENTI su main e privi di un
test dedicato. Ogni test chiama il codice reale (TradingEngine + Database SQLite
su tmp_path + BetfairClient vero con sessione HTTP finta che conta le POST).
Nessuna rete, nessuna credenziale.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List

import pytest

from betfair_client import BetfairClient
from core.trading_engine import (
    STATUS_ACCEPTED_FOR_PROCESSING,
    STATUS_DUPLICATE_BLOCKED,
    STATUS_FAILED,
    TradingEngine,
)
from database import Database

_PIAZZATO = {"status": "SUCCESS", "instructionReports": [
    {"status": "SUCCESS", "betId": "B-1", "sizeMatched": 0.0, "orderStatus": "EXECUTABLE"}]}


class _Risposta:
    status_code = 200

    def __init__(self, corpo: Any) -> None:
        self._corpo = corpo

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Any:
        return self._corpo


class _SessioneHttp:
    """Sessione HTTP finta: conta le POST e registra lo stato del DB al momento dell'invio."""

    def __init__(self, db: Any = None, ref: str = "") -> None:
        self.post_inviate: List[Dict[str, Any]] = []
        self.consumato_al_momento_dell_invio: List[bool] = []
        self._db = db
        self._ref = ref

    def post(self, url, **kwargs):
        corpo = json.loads(kwargs["data"])
        self.post_inviate.append(corpo[0]["params"])
        if self._db is not None:
            self.consumato_al_momento_dell_invio.append(
                self._db.is_order_intent_consumed(self._ref))
        return _Risposta([{"jsonrpc": "2.0", "id": 1, "result": _PIAZZATO}])


def _client_reale(sessione: _SessioneHttp) -> BetfairClient:
    client = BetfairClient(username="u", app_key="a", cert_pem="c.pem",
                           key_pem="k.pem", session=sessione)
    client.session_token = "TOK"
    return client


class _Bus:
    def subscribe(self, topic, handler):
        return None

    def publish(self, topic, payload=None):
        return None


class _Executor:
    def submit(self, _nome, fn):
        return fn()


class _Riconciliazione:
    def __init__(self) -> None:
        self.accodati: List[Dict[str, Any]] = []

    def enqueue(self, **meta):
        self.accodati.append(meta)


class _Servizio:
    simulation_broker = None
    _session_invalid = False


class _RuntimeLive:
    is_emergency_stopped = False

    def __init__(self) -> None:
        self.betfair_service = _Servizio()

    def get_effective_execution_mode(self) -> str:
        return "LIVE"

    def is_live_allowed(self) -> bool:
        return True


def _engine(db: Any, client: Any) -> TradingEngine:
    eng = TradingEngine(bus=_Bus(), db=db, client_getter=lambda: client,
                        executor=_Executor(), reconciliation_engine=_Riconciliazione())
    eng.runtime_controller = _RuntimeLive()
    return eng


def _richiesta(ref: str, **extra: Any) -> Dict[str, Any]:
    base = {"market_id": "1.234", "selection_id": 5678, "bet_type": "BACK",
            "price": 2.0, "stake": 5.0, "customer_ref": ref,
            "event_key": "1.234:5678:BACK"}
    base.update(extra)
    return base


# ---------------------------------------------------------------------------
# TRG-628-B-0750 — consumo durevole del customer_ref PRIMA del trasporto
# ---------------------------------------------------------------------------
def test_trg_628_b_0750(tmp_path):
    """ID: TRG-628-B-0750 / PF-B1-ORDER-DEDUPE-DURABLE

    Nel momento in cui il trasporto (POST placeOrders) parte, l'intento e' gia'
    marcato in modo durevole nel DB SQLite: la marcatura precede l'invio.
    """
    ref = "GATEA0750REF"
    db = Database(str(tmp_path / "g0750.db"))
    sessione = _SessioneHttp(db=db, ref=ref)
    eng = _engine(db, _client_reale(sessione))

    esito = eng.submit_quick_bet(_richiesta(ref))

    assert esito["status"] == STATUS_ACCEPTED_FOR_PROCESSING
    assert len(sessione.post_inviate) == 1
    assert sessione.consumato_al_momento_dell_invio == [True], (
        "il trasporto e' partito prima della marcatura durevole dell'intento")
    # durevole: un nuovo Database sullo stesso file (riavvio) la vede ancora
    assert Database(str(tmp_path / "g0750.db")).is_order_intent_consumed(ref) is True


def test_trg_628_b_0750_write_failure(tmp_path):
    """ID: TRG-628-B-0750 / PF-B1-ORDER-DEDUPE-DURABLE

    Se la scrittura durevole della marcatura fallisce, il trasporto NON parte
    (DURABLE_DEDUPE_WRITE_FAILED, esito FAILED certo).
    """
    ref = "GATEA0750WF"
    db = Database(str(tmp_path / "g0750wf.db"))

    def _consume_rotto(*_a, **_kw):
        raise RuntimeError("disk I/O error")

    db.consume_order_intent = _consume_rotto  # guasto della sola scrittura di marcatura
    sessione = _SessioneHttp()
    eng = _engine(db, _client_reale(sessione))

    esito = eng.submit_quick_bet(_richiesta(ref))

    assert sessione.post_inviate == [], "ordine inviato senza marcatura durevole"
    assert esito["status"] == STATUS_FAILED
    assert "DURABLE_DEDUPE_WRITE_FAILED" in str(esito.get("error"))


# ---------------------------------------------------------------------------
# TRG-628-B-0752 — DUPLICATE_BLOCKED / INVALID_REQUEST non rilasciano la chiave
# ---------------------------------------------------------------------------
def test_trg_628_b_0752(tmp_path):
    """ID: TRG-628-B-0752 / PF-B1-ORDER-DUPLICATE-KEY

    Dopo un riavvio la chiave dell'ordine originale (SUBMITTED) e' ripopolata
    dal DB senza proprietario locale. Una richiesta INVALID_REQUEST e una
    DUPLICATE_BLOCKED con lo stesso customer_ref non devono rilasciarla.
    """
    ref = "GATEA0752REF"
    path = str(tmp_path / "g0752.db")
    sessione = _SessioneHttp()
    eng1 = _engine(Database(path), _client_reale(sessione))
    primo = eng1.submit_quick_bet(_richiesta(ref))
    assert primo["status"] == STATUS_ACCEPTED_FOR_PROCESSING

    # riavvio: nuovo Database e nuovo engine, chiave ripopolata dal DB
    eng2 = _engine(Database(path), _client_reale(sessione))
    eng2._repopulate_inflight_from_db()
    assert ref in eng2._inflight_keys
    assert ref not in eng2._local_inflight_keys

    invalida = eng2.submit_quick_bet(_richiesta(ref, price=float("nan")))  # normalizzazione fallisce
    assert invalida["reason"] == "INVALID_REQUEST"
    assert ref in eng2._inflight_keys, "INVALID_REQUEST ha rilasciato la chiave dell'originale"

    duplicato = eng2.submit_quick_bet(_richiesta(ref))
    assert duplicato["status"] == STATUS_DUPLICATE_BLOCKED
    assert ref in eng2._inflight_keys, "DUPLICATE_BLOCKED ha rilasciato la chiave dell'originale"
    assert len(sessione.post_inviate) == 1
