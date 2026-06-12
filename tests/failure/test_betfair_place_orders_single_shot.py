"""PR-F programma test hedge-fund grade: ambiguita' di rete su placeOrders.

Due bug reali di produzione provati da questa suite (fix autorizzato
dall'owner su betfair_client.py + 1 riga di wiring in core/trading_engine.py):

1. RETRY CIECO: _post_jsonrpc ritentava QUALSIASI metodo su timeout/errore
   di rete, incluso placeOrders (non idempotente, senza customerRef):
   1 segnale -> fino a 3 bet reali. Il DuplicationGuard non protegge:
   deduplica i segnali, non i retry HTTP interni al client.
   Contratto nuovo: placeOrders e' SINGLE-SHOT, mai re-inviato.

2. ORDINE FANTASMA: order_unknown era True solo per "TIMEOUT" — un
   connection reset / HTTP 5xx DOPO l'invio finiva FAILED locale senza
   reconciliation, con l'ordine potenzialmente vivo sull'exchange.
   Contratto nuovo: TIMEOUT/NETWORK_ERROR/HTTP_5xx => order_unknown=True
   e il motore lo instrada su AMBIGUOUS + reconciliation (mai SUCCESS,
   mai FAILED definitivo).

Dedup verificato: timeout->AMBIGUOUS via testo errore, breaker wiring,
sessione scaduta e retry dei READ idempotenti (get_market_book) sono GIA'
coperti (test_betfair_client_timeout_ambiguity, test_circuit_breaker_wiring,
test_session_expiry_recovery, test_betfair_client_net); qui solo il
contratto single-shot, order_unknown esteso e il wiring engine mancante.
"""
from __future__ import annotations

import pytest
import requests
from requests.exceptions import Timeout

from betfair_client import BetfairClient
from core.trading_constants import AMBIGUITY_SUBMIT_UNKNOWN


class FakeResponse:
    def __init__(self, *, status_code=200, json_data=None, raise_http=False):
        self.status_code = status_code
        self._json_data = json_data
        self._raise_http = raise_http

    def raise_for_status(self):
        if self._raise_http:
            raise requests.exceptions.HTTPError(response=self)

    def json(self):
        return self._json_data


class RecordingSession:
    """Conta i POST: e' l'assert chiave contro il re-invio di placeOrders."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if not self.responses:
            raise AssertionError(
                f"sessione esaurita dopo {len(self.calls)} POST: "
                "il client ha ri-inviato piu' del previsto"
            )
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def _client(session, *, max_retries=2):
    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
        max_retries=max_retries,
    )
    client.session_token = "TOK"
    return client


def _place(client, **overrides):
    payload = {
        "market_id": "1.234",
        "selection_id": 5678,
        "side": "BACK",
        "price": 2.0,
        "size": 5.0,
    }
    payload.update(overrides)
    return client.place_bet(**payload)


_SUCCESS_RESPONSE = FakeResponse(
    json_data=[{
        "result": {
            "status": "SUCCESS",
            "instructionReports": [{"status": "SUCCESS", "betId": "B-RETRY"}],
        }
    }]
)


# ---------------------------------------------------------------------------
# BUG 1: placeOrders deve essere SINGLE-SHOT (mai re-inviato)
# ---------------------------------------------------------------------------

@pytest.mark.failure
def test_place_orders_timeout_is_never_resent():
    """Timeout su placeOrders: la bet puo' essere GIA' passata su Betfair.
    Il client NON deve ri-inviare (era: fino a 3 POST con max_retries=2 ->
    rischio doppia/tripla bet reale). Esito: 1 solo POST, order_unknown."""
    session = RecordingSession([Timeout(), _SUCCESS_RESPONSE])
    client = _client(session, max_retries=2)

    out = _place(client)

    assert len(session.calls) == 1, "placeOrders RI-INVIATO dopo timeout"
    assert out["ok"] is False
    assert out["order_unknown"] is True
    assert out["classification"] == "TRANSIENT"


@pytest.mark.failure
def test_place_orders_network_error_is_never_resent():
    """Connection reset: stesso contratto single-shot del timeout."""
    session = RecordingSession([
        requests.exceptions.ConnectionError("connection reset by peer"),
        _SUCCESS_RESPONSE,
    ])
    client = _client(session, max_retries=2)

    out = _place(client)

    assert len(session.calls) == 1, "placeOrders RI-INVIATO dopo network error"
    assert out["ok"] is False


@pytest.mark.failure
def test_idempotent_reads_keep_retrying():
    """Controllo PASS: il single-shot vale SOLO per placeOrders — i read
    idempotenti (get_market_book) continuano a ritentare e recuperare."""
    session = RecordingSession([
        requests.exceptions.ConnectionError("net down"),
        FakeResponse(json_data=[{"result": [{"marketId": "1.234", "runners": []}]}]),
    ])
    client = _client(session, max_retries=1)

    out = client.get_market_book("1.234")

    assert out["marketId"] == "1.234"
    assert len(session.calls) == 2


# ---------------------------------------------------------------------------
# BUG 2 (lato client): risposta persa DOPO l'invio => order_unknown=True
# ---------------------------------------------------------------------------

@pytest.mark.failure
@pytest.mark.parametrize("effect", [
    requests.exceptions.ConnectionError("connection reset by peer"),
    FakeResponse(status_code=503, raise_http=True),
    FakeResponse(status_code=500, raise_http=True),
], ids=["connection_reset", "http_503", "http_500"])
def test_response_lost_after_send_marks_order_unknown(effect):
    """La richiesta puo' aver raggiunto Betfair anche se la risposta e'
    andata persa (reset/5xx): l'esito e' SCONOSCIUTO, mai FAILED definitivo."""
    client = _client(RecordingSession([effect]))

    out = _place(client)

    assert out["ok"] is False
    assert out["order_unknown"] is True, (
        "risposta persa dopo l'invio marcata come fallimento definitivo: "
        "ordine fantasma possibile sull'exchange senza reconciliation"
    )


@pytest.mark.failure
def test_definitive_rejections_are_not_order_unknown():
    """Controllo PASS: i rifiuti DEFINITIVI (Betfair ha risposto NO, o la
    richiesta non e' mai partita) restano order_unknown=False — niente
    false ambiguita' che intaserebbero la reconciliation."""
    rejected = FakeResponse(
        json_data=[{
            "result": {
                "status": "FAILURE",
                "instructionReports": [{"status": "FAILURE"}],
            }
        }]
    )
    out = _place(_client(RecordingSession([rejected])))
    assert out["ok"] is False
    assert out["order_unknown"] is False  # BET_FAILED: risposta arrivata

    http_400 = FakeResponse(status_code=400, raise_http=True)
    out = _place(_client(RecordingSession([http_400])))
    assert out["ok"] is False
    assert out["order_unknown"] is False  # 4xx: richiesta respinta, non persa

    no_auth = _client(RecordingSession([]))
    no_auth.session_token = ""
    out = _place(no_auth)
    assert out["ok"] is False
    assert out["order_unknown"] is False  # mai partita: nessun POST


# ---------------------------------------------------------------------------
# BUG 2 (wiring engine): order_unknown=True => AMBIGUOUS + reconciliation
# ---------------------------------------------------------------------------

class _Bus:
    def subscribe(self, *_):
        pass

    def publish(self, *_):
        pass


class _DB:
    def insert_order(self, payload):
        return "OID-NET-1"

    def update_order(self, *_args, **_kwargs):
        pass


class _Runtime:
    @staticmethod
    def get_effective_execution_mode():
        return "LIVE"

    @staticmethod
    def is_live_allowed():
        return True


class _LiveClientOrderUnknown:
    """Simula il contratto post-fix del client: risposta persa in rete."""

    @staticmethod
    def place_order(_payload):
        return {
            "ok": False,
            "error": "REQUEST_FAILED: NETWORK_ERROR: connection reset by peer",
            "classification": "TRANSIENT",
            "order_unknown": True,
        }


class _LiveClientRejected:
    @staticmethod
    def place_order(_payload):
        return {
            "ok": False,
            "error": "BET_FAILED: FAILURE",
            "classification": "PERMANENT",
            "order_unknown": False,
        }


class _RecordingReconcile:
    def __init__(self):
        self.enqueued = []

    def enqueue(self, **meta):
        self.enqueued.append(meta)


def _engine(live_client, reconcile):
    from core.trading_engine import TradingEngine

    engine = TradingEngine(
        bus=_Bus(),
        db=_DB(),
        client_getter=lambda: None,
        executor=None,
        reconciliation_engine=reconcile,
    )
    engine._runtime_state = "READY"
    engine.runtime_controller = _Runtime()
    engine.betfair_client = live_client
    return engine


@pytest.mark.failure
def test_engine_routes_order_unknown_to_ambiguous_and_reconcile():
    """ok=False + order_unknown=True dal client live: l'ordine puo' esistere
    sull'exchange -> AMBIGUOUS (mai FAILED, mai SUCCESS) e UN enqueue alla
    reconciliation che andra' a verificare su Betfair."""
    reconcile = _RecordingReconcile()
    engine = _engine(_LiveClientOrderUnknown(), reconcile)

    result = engine.submit_quick_bet({
        "customer_ref": "C-NET-1",
        "market_id": "1.234",
        "selection_id": 5678,
        "side": "BACK",
        "price": 2.0,
        "size": 5.0,
    })

    assert result["status"] == "AMBIGUOUS", (
        f"risposta persa finita in {result['status']!r}: ordine fantasma "
        "possibile senza reconciliation"
    )
    assert result["ambiguity_reason"] == AMBIGUITY_SUBMIT_UNKNOWN
    assert len(reconcile.enqueued) == 1
    assert reconcile.enqueued[0]["ambiguity_reason"] == AMBIGUITY_SUBMIT_UNKNOWN


@pytest.mark.failure
def test_engine_keeps_definitive_rejection_failed_without_reconcile():
    """Controllo PASS: rifiuto definitivo (order_unknown=False) resta FAILED
    e NON sporca la coda di reconciliation."""
    reconcile = _RecordingReconcile()
    engine = _engine(_LiveClientRejected(), reconcile)

    result = engine.submit_quick_bet({
        "customer_ref": "C-NET-2",
        "market_id": "1.234",
        "selection_id": 5678,
        "side": "BACK",
        "price": 2.0,
        "size": 5.0,
    })

    assert result["status"] == "FAILED"
    assert reconcile.enqueued == []
