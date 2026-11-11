"""Gate A — T2b batch b2, componente trading (core/trading_engine.TradingEngine).

TradingEngine reale + Database SQLite su tmp_path. LIVE solo con client finto o BetfairClient con sessione HTTP
finta (nessuna scrittura reale, nessuna rete); SIM con SimulationBroker reale.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List

import pytest

from betfair_client import BetfairClient
from core.trading_constants import ERROR_AMBIGUOUS
from core.trading_engine import (
    STATUS_AMBIGUOUS,
    STATUS_FAILED,
    ExecutionError,
    TradingEngine,
)
from database import Database
from simulation_broker import SimulationBroker

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
    def __init__(self) -> None:
        self.post_inviate: List[Dict[str, Any]] = []

    def post(self, url, **kwargs):
        self.post_inviate.append(json.loads(kwargs["data"])[0]["params"])
        return _Risposta([{"jsonrpc": "2.0", "id": 1, "result": _PIAZZATO}])


def _client_reale(sessione: _SessioneHttp) -> BetfairClient:
    client = BetfairClient(username="u", app_key="a", cert_pem="c.pem", key_pem="k.pem", session=sessione)
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
    def __init__(self, sim=None, session_invalid=False) -> None:
        self.simulation_broker = sim
        self._session_invalid = session_invalid


class _Runtime:
    is_emergency_stopped = False

    def __init__(self, mode="LIVE", live_allowed=True, session_invalid=False, sim=None) -> None:
        self._mode = mode
        self._live_allowed = live_allowed
        self.betfair_service = _Servizio(sim=sim, session_invalid=session_invalid)

    def get_effective_execution_mode(self) -> str:
        return self._mode

    def is_live_allowed(self) -> bool:
        return self._live_allowed


def _engine(db: Any, client: Any, runtime: Any = None, rec: Any = None) -> TradingEngine:
    eng = TradingEngine(bus=_Bus(), db=db, client_getter=lambda: client, executor=_Executor(),
                        reconciliation_engine=rec or _Riconciliazione())
    eng.runtime_controller = runtime if runtime is not None else _Runtime()
    return eng


def _richiesta(ref: str, **extra: Any) -> Dict[str, Any]:
    base = {"market_id": "1.234", "selection_id": 5678, "bet_type": "BACK", "price": 2.0, "stake": 5.0,
            "customer_ref": ref, "event_key": "1.234:5678:BACK"}
    base.update(extra)
    return base


# ---------------------------------------------------------------------------
@pytest.mark.parametrize("difetto", [{"price": float("nan")}, {"stake": "abc"}, {"market_id": ""},
                                     {"selection_id": -1}])
def test_trg_628_b_0747(tmp_path, difetto):
    """ID: TRG-628-B-0747 / PF-B1-ORDER-INVALID

    Richiesta non normalizzabile => FAILED con reason INVALID_REQUEST prima di qualsiasi submit: nessuna POST,
    nessuna riga ordine INFLIGHT."""
    sessione = _SessioneHttp()
    db = Database(str(tmp_path / "g0747.db"))
    eng = _engine(db, _client_reale(sessione))
    esito = eng.submit_quick_bet(_richiesta("GATEA0747", **difetto))
    assert esito["status"] == STATUS_FAILED
    assert esito["reason"] == "INVALID_REQUEST"
    assert sessione.post_inviate == []
    assert esito.get("order_id") in (None, "")


# ---------------------------------------------------------------------------
def test_trg_628_b_0754(tmp_path):
    """ID: TRG-628-B-0754 / PF-B1-ORDER-DOWNSTREAM-FAIL

    _raise_if_failed_semantic_response: order_unknown=True (anche con ok=False) => ExecutionError AMBIGUOUS (mai
    FAILED definitivo); ok=False / status FAILURE senza order_unknown => RuntimeError DOWNSTREAM_SEMANTIC_FAILURE;
    risposta non dict => AMBIGUOUS; SUCCESS con betId => nessuna eccezione."""
    eng = _engine(Database(str(tmp_path / "g0754.db")), None)

    with pytest.raises(ExecutionError) as amb:
        eng._raise_if_failed_semantic_response({"ok": False, "error": "REQUEST_FAILED: TIMEOUT",
                                                "order_unknown": True})
    assert amb.value.error_type == ERROR_AMBIGUOUS

    with pytest.raises(ExecutionError) as malformed:
        eng._raise_if_failed_semantic_response("boh")
    assert malformed.value.error_type == ERROR_AMBIGUOUS

    for fallita in ({"ok": False, "error": "BET_REJECTED: INSUFFICIENT_FUNDS", "order_unknown": False},
                    {"status": "FAILURE", "errorCode": "INVALID_ODDS",
                     "instructionReports": [{"status": "FAILURE"}]}):
        with pytest.raises(RuntimeError) as exc:
            eng._raise_if_failed_semantic_response(fallita)
        assert not isinstance(exc.value, ExecutionError)
        assert str(exc.value).startswith("DOWNSTREAM_SEMANTIC_FAILURE")

    eng._raise_if_failed_semantic_response({"ok": True, "result": _PIAZZATO, **_PIAZZATO})


def test_trg_628_b_0754_end_to_end_order_unknown(tmp_path):
    """ID: TRG-628-B-0754 / PF-B1-ORDER-DOWNSTREAM-FAIL (percorso reale)

    Client LIVE che restituisce order_unknown=True => esito AMBIGUOUS e ordine accodato alla riconciliazione."""

    class _ClientIgnoto:
        def place_bet(self, **_kw):
            return {"ok": False, "error": "REQUEST_FAILED: NETWORK_ERROR", "order_unknown": True}

    rec = _Riconciliazione()
    eng = _engine(Database(str(tmp_path / "g0754e.db")), _ClientIgnoto(), rec=rec)
    esito = eng.submit_quick_bet(_richiesta("GATEA0754E"))
    assert esito["status"] == STATUS_AMBIGUOUS
    assert len(rec.accodati) == 1


# ---------------------------------------------------------------------------
def test_trg_628_b_0757(tmp_path):
    """ID: TRG-628-B-0757 / PF-B1-ORDER-SIM-ROUTE

    In SIMULATION l'ordine va al broker simulato: a `execute()` se lo espone, altrimenti a `place_bet()` del
    SimulationBroker reale; il client LIVE (BetfairClient) non riceve nulla."""
    sessione = _SessioneHttp()
    broker = SimulationBroker(starting_balance=1000.0)
    eng = _engine(Database(str(tmp_path / "g0757.db")), _client_reale(sessione),
                  runtime=_Runtime(mode="SIMULATION", sim=broker))
    eng.simulation_broker = broker
    esito = eng.submit_quick_bet(_richiesta("GATEA0757"))
    assert esito["status"] not in (STATUS_FAILED,), esito
    assert sessione.post_inviate == []
    assert len(broker.state.orders) == 1
    ordine = next(iter(broker.state.orders.values()))
    assert (ordine.market_id, ordine.selection_id, ordine.side, ordine.price, ordine.size) == \
        ("1.234", 5678, "BACK", 2.0, 5.0)

    class _BrokerExecute:
        def __init__(self) -> None:
            self.chiamate: List[Dict[str, Any]] = []

        def execute(self, payload):
            self.chiamate.append(dict(payload))
            return {"ok": True, "status": "SUCCESS", "instructionReports": [
                {"status": "SUCCESS", "betId": "SIM-X", "sizeMatched": 0.0}]}

    bx = _BrokerExecute()
    sessione2 = _SessioneHttp()
    eng2 = _engine(Database(str(tmp_path / "g0757b.db")), _client_reale(sessione2),
                   runtime=_Runtime(mode="SIMULATION"))
    eng2.simulation_broker = bx
    eng2.submit_quick_bet(_richiesta("GATEA0757B"))
    assert len(bx.chiamate) == 1 and bx.chiamate[0]["customer_ref"] == "GATEA0757B"
    assert sessione2.post_inviate == []


# ---------------------------------------------------------------------------
def test_trg_628_b_0758(tmp_path):
    """ID: TRG-628-B-0758 / PF-B1-ORDER-LIVE-GATES

    LIVE non consentito (is_live_allowed False), sessione BetfairService invalida, circuit breaker ordini aperto:
    nessuna POST al client e nessun esito di successo. Controprova: con i gate aperti una sola POST."""
    casi = {
        "live_non_consentito": dict(runtime=_Runtime(live_allowed=False)),
        "sessione_invalida": dict(runtime=_Runtime(session_invalid=True)),
        "breaker_aperto": dict(runtime=_Runtime(), breaker=True),
    }
    for nome, caso in casi.items():
        sessione = _SessioneHttp()
        eng = _engine(Database(str(tmp_path / f"g0758_{nome}.db")), _client_reale(sessione),
                      runtime=caso["runtime"])
        if caso.get("breaker"):
            for _ in range(3):
                eng._order_submission_breaker.record_failure(RuntimeError("x"))
            assert eng._order_submission_breaker.is_open()
        esito = eng.submit_quick_bet(_richiesta(f"GATEA0758{nome[:6]}"))
        assert sessione.post_inviate == [], f"{nome}: ordine LIVE inviato"
        assert esito["status"] in (STATUS_FAILED, "DENIED"), (nome, esito)

    sessione_ok = _SessioneHttp()
    eng_ok = _engine(Database(str(tmp_path / "g0758_ok.db")), _client_reale(sessione_ok))
    eng_ok.submit_quick_bet(_richiesta("GATEA0758OK"))
    assert len(sessione_ok.post_inviate) == 1


# ---------------------------------------------------------------------------
@pytest.mark.parametrize("eccezione", [ValueError("report malformato"), KeyError("betId"),
                                       RuntimeError("INVALID_PRICE"), ConnectionResetError("reset")])
def test_trg_628_b_0759(tmp_path, eccezione):
    """ID: TRG-628-B-0759 / PF-B1-ORDER-LIVE-UNKNOWN

    Un'eccezione dal client LIVE che non prova il mancato invio (anche un testo INVALID_PRICE se il client NON e'
    il BetfairClient reale) => esito AMBIGUOUS, ordine in riconciliazione, mai FAILED."""

    class _ClientCheSolleva:
        def place_bet(self, **_kw):
            raise eccezione

    rec = _Riconciliazione()
    eng = _engine(Database(str(tmp_path / "g0759.db")), _ClientCheSolleva(), rec=rec)
    esito = eng.submit_quick_bet(_richiesta("GATEA0759"))
    assert esito["status"] == STATUS_AMBIGUOUS, esito
    assert len(rec.accodati) == 1
