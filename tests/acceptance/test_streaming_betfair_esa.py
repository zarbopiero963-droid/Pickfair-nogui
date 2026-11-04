"""Streaming mercati Betfair (ESA) end-to-end OFFLINE: BetfairClient -> betfairlightweight -> StreamingFeed.

Pila REALE: `BetfairClient.streaming` (core.betfair_streaming) -> `BetfairStream` e `StreamListener` di betfairlightweight
-> `StreamingFeed` -> book di mercato. Unico finto: il socket, che riproduce messaggi ESA SINTETICI (MOCK) scritti sul
contratto pubblico (op connection/status/mcm, img, marketDefinition, rc con batb/batl, clk, heartbeat). Non sono dati
registrati da Betfair: la prova sulle API reali e' il livello B (PF-BETFAIR-E2E-01, DEFERRED_ACCESS).

Su `main` `ada4076`: il BetfairClient non esponeva `streaming` (STREAMING_INTERFACE_UNAVAILABLE, il feed ricadeva sempre
sul REST) e la sottoscrizione inviava filtri snake_case (`market_ids`, `ladder_levels`) che Betfair non riconosce.
"""
from __future__ import annotations

import json
import threading
import time
from typing import Any, Dict, List

import pytest

import betfair_client
from core import betfair_streaming
from services.streaming_feed import StreamingFeed

APP_KEY = "APPKEY-TEST"
TOKEN = "SESSION-TEST-TOKEN"
M1, M2 = "1.111", "1.222"


def _definition(status="OPEN", runners=((11, "ACTIVE"), (12, "ACTIVE")), inplay=False) -> Dict[str, Any]:
    return {"bspMarket": False, "turnInPlayEnabled": True, "persistenceEnabled": True, "marketBaseRate": 5.0,
            "eventId": "31", "eventTypeId": "1", "numberOfWinners": 1, "bettingType": "ODDS",
            "marketType": "MATCH_ODDS", "marketTime": "2026-11-04T19:00:00.000Z",
            "suspendTime": "2026-11-04T19:00:00.000Z", "bspReconciled": False, "complete": True, "inPlay": inplay,
            "crossMatching": True, "runnersVoidable": False, "numberOfActiveRunners": 2, "betDelay": 0,
            "status": status, "runners": [{"status": st, "sortPriority": i + 1, "id": sid}
                                          for i, (sid, st) in enumerate(runners)],
            "regulators": ["MR_INT"], "countryCode": "IT", "discountAllowed": True, "timezone": "Europe/Rome",
            "openDate": "2026-11-04T19:00:00.000Z", "version": 1, "priceLadderDefinition": {"type": "CLASSIC"}}


def _img(mid, *, back=2.0, lay=2.02, status="OPEN") -> Dict[str, Any]:
    return {"id": mid, "img": True, "marketDefinition": _definition(status),
            "rc": [{"id": 11, "batb": [[0, back, 100.0]], "batl": [[0, lay, 50.0]]},
                   {"id": 12, "batb": [[0, 3.0, 40.0]], "batl": [[0, 3.1, 20.0]]}]}


def _mcm(clk, mc=None, ct=None, initial=None) -> Dict[str, Any]:
    msg = {"op": "mcm", "id": 1, "clk": clk, "pt": 1762282800000}      # id della sottoscrizione
    if initial:
        msg["initialClk"] = initial
    if ct:
        msg["ct"] = ct
    if mc is not None:
        msg["mc"] = mc
    return msg


CONNECTION = {"op": "connection", "connectionId": "002-000000000001-1"}
# betfairlightweight assegna l'id 1 alla sottoscrizione e il 2 all'autenticazione (aperta dal primo invio): Betfair
# risponde con gli id delle richieste.
STATUS_AUTH = {"op": "status", "id": 2, "statusCode": "SUCCESS", "connectionClosed": False}
STATUS_SUB = {"op": "status", "id": 1, "statusCode": "SUCCESS", "connectionClosed": False}


class _FakeSocket:
    """Socket TLS finto: registra cio' che il client invia e restituisce i frame ESA in ordine."""

    def __init__(self, frames: List[Dict[str, Any]], *, chiudi_subito: bool = False) -> None:
        self.inviati: List[Dict[str, Any]] = []
        self.chiudi_subito = chiudi_subito
        self._frames = [(json.dumps(f) + "\r\n").encode() for f in frames]
        self._fine = threading.Event()

    def sendall(self, data: bytes) -> None:
        for line in data.decode().split("\r\n"):
            if line:
                self.inviati.append(json.loads(line))

    def recv(self, _n: int) -> bytes:
        if self._frames:
            return self._frames.pop(0)
        if not self.chiudi_subito:
            self._fine.wait(2.0)                     # fine dei dati: chiusura dal server
        return b""

    def settimeout(self, _t) -> None:
        return None

    def shutdown(self, _how) -> None:
        self._fine.set()

    def close(self) -> None:
        self._fine.set()


def _client() -> betfair_client.BetfairClient:
    c = betfair_client.BetfairClient(username="u", app_key=APP_KEY, cert_pem="c.pem", key_pem="k.pem")
    c.session_token = TOKEN
    return c


def _avvia(monkeypatch, frames, *, market_ids=(M1,), attesi=1, attesa=5.0, attendi_disconnessione=False):
    sock = _FakeSocket(frames, chiudi_subito=attendi_disconnessione)
    monkeypatch.setattr(betfair_streaming.VerifiedBetfairStream, "_create_socket", lambda self: sock)
    books: List[Dict[str, Any]] = []
    disconnessioni: List[Dict[str, Any]] = []
    feed = StreamingFeed(client_getter=_client,
                         config={"enabled": True, "market_data_mode": "stream", "market_ids": list(market_ids),
                                 "heartbeat_timeout_sec": 30, "reconnect_backoff_sec": 60},
                         on_market_book=lambda b: books.append(json.loads(json.dumps(b, default=str))),
                         on_disconnect=disconnessioni.append)
    feed.start()
    fine = time.monotonic() + attesa
    while (len(books) < attesi or (attendi_disconnessione and not disconnessioni)) and time.monotonic() < fine:
        time.sleep(0.02)
    time.sleep(0.1)
    feed.stop()
    return sock, books, disconnessioni


def _stato(book) -> str:
    return str(book.get("status") or "").upper()


def _runner(book, sid) -> Dict[str, Any]:
    return next(r for r in book["runners"] if int(r["selectionId"]) == sid)


# ------------------------------------------------------------------ interfaccia e sicurezza del trasporto
def test_block_il_client_espone_lo_streaming() -> None:
    assert callable(getattr(_client().streaming, "create_stream", None))


def test_block_nessuna_autenticazione_senza_sessione() -> None:
    c = _client()
    c.session_token = ""
    with pytest.raises(RuntimeError, match="STREAM_SESSION_MISSING"):
        c.streaming.create_stream()


def test_block_tls_con_verifica_del_certificato(monkeypatch) -> None:
    contesti = []

    class _Ctx:
        def wrap_socket(self, s, server_hostname=None):
            contesti.append(server_hostname)
            s.close()
            raise ConnectionRefusedError("fine test")

    monkeypatch.setattr(betfair_streaming.ssl, "create_default_context", lambda: _Ctx())
    stream = _client().streaming.create_stream()
    with pytest.raises(ConnectionRefusedError):
        stream._create_socket()
    assert contesti == ["stream-api.betfair.com"], "socket dello stream senza contesto TLS verificato"


# ------------------------------------------------------------------ protocollo: autenticazione e sottoscrizione
def test_block_autenticazione_e_sottoscrizione_nel_formato_esa(monkeypatch) -> None:
    sock, books, _d = _avvia(monkeypatch, [CONNECTION, STATUS_AUTH, STATUS_SUB,
                                           _mcm("AAA", [_img(M1)], ct="SUB_IMAGE", initial="III")])

    auth = next(m for m in sock.inviati if m["op"] == "authentication")
    assert auth["appKey"] == APP_KEY and auth["session"] == TOKEN
    sub = next(m for m in sock.inviati if m["op"] == "marketSubscription")
    assert sub["marketFilter"] == {"marketIds": [M1]}, sub["marketFilter"]
    assert sub["marketDataFilter"]["fields"] == ["EX_BEST_OFFERS", "EX_MARKET_DEF", "EX_LTP"]
    assert sub["marketDataFilter"]["ladderLevels"] == 3
    assert not any("_" in k for k in sub["marketFilter"]) and not any("_" in k for k in sub["marketDataFilter"])
    assert books, "nessun book dall'immagine iniziale"


# ------------------------------------------------------------------ snapshot, delta, stati, heartbeat
def test_snapshot_e_aggiornamento_incrementale(monkeypatch) -> None:
    _s, books, _d = _avvia(monkeypatch, [
        CONNECTION, STATUS_AUTH, STATUS_SUB,
        _mcm("AAA", [_img(M1)], ct="SUB_IMAGE", initial="III"),
        _mcm("AAB", [{"id": M1, "rc": [{"id": 11, "batb": [[0, 2.04, 80.0]]}]}]),
    ], attesi=2)

    assert len(books) >= 2, books
    primo, ultimo = books[0], books[-1]
    assert primo["marketId"] == M1 and _stato(primo) == "OPEN"
    assert _runner(primo, 11)["ex"]["availableToBack"][0]["price"] == 2.0
    assert _runner(ultimo, 11)["ex"]["availableToBack"][0] == {"price": 2.04, "size": 80.0}
    assert _runner(ultimo, 11)["ex"]["availableToLay"][0]["price"] == 2.02, "delta ha perso il lato non aggiornato"


def test_mercato_open_suspended_open_closed_e_selezione_ritirata(monkeypatch) -> None:
    _s, books, _d = _avvia(monkeypatch, [
        CONNECTION, STATUS_AUTH, STATUS_SUB,
        _mcm("AAA", [_img(M1)], ct="SUB_IMAGE", initial="III"),
        _mcm("AAB", [{"id": M1, "marketDefinition": _definition("SUSPENDED")}]),
        _mcm("AAC", [{"id": M1, "marketDefinition": _definition("OPEN", runners=((11, "ACTIVE"), (12, "REMOVED")))}]),
        _mcm("AAD", [{"id": M1, "marketDefinition": _definition("CLOSED", runners=((11, "WINNER"), (12, "REMOVED")))}]),
    ], attesi=4)

    stati = [_stato(b) for b in books]
    assert stati[:4] == ["OPEN", "SUSPENDED", "OPEN", "CLOSED"], stati
    assert str(_runner(books[2], 12)["status"]).upper() == "REMOVED"
    assert str(_runner(books[3], 11)["status"]).upper() == "WINNER"


def test_heartbeat_non_produce_book_ne_altera_lo_stato(monkeypatch) -> None:
    _s, books, _d = _avvia(monkeypatch, [
        CONNECTION, STATUS_AUTH, STATUS_SUB,
        _mcm("AAA", [_img(M1)], ct="SUB_IMAGE", initial="III"),
        _mcm("AAB", ct="HEARTBEAT"),
    ], attesi=1)

    assert len(books) == 1 and _stato(books[0]) == "OPEN"


def test_due_mercati_contemporanei_non_si_mescolano(monkeypatch) -> None:
    _s, books, _d = _avvia(monkeypatch, [
        CONNECTION, STATUS_AUTH, STATUS_SUB,
        _mcm("AAA", [_img(M1, back=2.0), _img(M2, back=5.0)], ct="SUB_IMAGE", initial="III"),
        _mcm("AAB", [{"id": M2, "marketDefinition": _definition("SUSPENDED")}]),
    ], market_ids=(M1, M2), attesi=3)

    ultimo = {}
    for b in books:
        ultimo[b["marketId"]] = b
    assert _stato(ultimo[M1]) == "OPEN" and _runner(ultimo[M1], 11)["ex"]["availableToBack"][0]["price"] == 2.0
    assert _stato(ultimo[M2]) == "SUSPENDED" and _runner(ultimo[M2], 11)["ex"]["availableToBack"][0]["price"] == 5.0


def test_chiusura_dal_server_e_una_disconnessione_dichiarata(monkeypatch) -> None:
    """Fine dei dati dal server: il feed lo dichiara (on_disconnect), non resta «sano» in silenzio."""
    inizio = time.monotonic()
    _s, books, disconnessioni = _avvia(monkeypatch, [CONNECTION, STATUS_AUTH, STATUS_SUB,
                                                     _mcm("AAA", [_img(M1)], ct="SUB_IMAGE", initial="III")],
                                       attesi=1, attesa=6.0, attendi_disconnessione=True)
    assert books and disconnessioni, "chiusura del server non dichiarata"
    assert disconnessioni[0]["reason"].startswith("STREAM_READER_FAILED"), disconnessioni[0]
    assert time.monotonic() - inizio < 5.0, "disconnessione rilevata solo dopo il timeout dell'heartbeat (30 s)"
