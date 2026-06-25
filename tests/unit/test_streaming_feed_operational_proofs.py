"""Proof operazionali StreamingFeed (test_suite_proof_streaming_feed).

Phase 0 dedup (verificato): lo StreamingFeed e' GIA' coperto in modo forte
(tests/unit/test_streaming_feed.py + chaos soak): bounds di subscription,
reinjection clk/initialClk su reconnect, 503 degraded senza disconnect, merge
incrementale ladder/runner/definition, precedenza trd>tv, session-gate che
blocca, auth-failure bounded, keepalive failure visibile, parsing resource
object, coerenza high-churn, recovery soak (heartbeat timeout -> reconnect ->
auth recovery, queue watermark). Scrivere quelli sarebbe duplicazione.

Qui SOLO garanzie deterministiche a funzione pura ancora non verificate al
livello unit, caratterizzate sul comportamento REALE (probe):

1. heartbeat-dead: prima del primo messaggio NON e' morto (niente reconnect
   spurio); muore SOLO oltre il timeout (== timeout non e' morto); il timeout
   ha un floor di 1.0s anche con heartbeat_timeout_sec=0.
2. degradazione 503: dopo un 503 `healthy` e' False e il flag `degraded_503`
   PERSISTE anche se riprende il traffico normale — si azzera solo alla
   riconnessione (caratterizzazione del confine di osservabilita').
3. subscribe kwargs: clk/initialClk vuoti vengono OMESSI (niente replay di un
   clk vuoto); compaiono solo dopo essere stati catturati.
4. callback on_disconnect che solleva: l'eccezione e' isolata (non propaga) e
   lo stato resta coerente (connected=False).

Tutto pure-unit e deterministico (clock monotonico finto per l'heartbeat);
nessuna modifica al codice di produzione (services/streaming_feed.py).
"""
from __future__ import annotations

import pytest

from services.streaming_feed import StreamingFeed


def _opaque(*_args, **_kwargs):
    """Stub inerte: ritorna un oggetto opaco (client/listener finto)."""
    return object()


def _ignore(*_args, **_kwargs):
    """Callback inerte: ignora gli argomenti e non fa nulla."""
    return None


def _allow(*_args, **_kwargs):
    """Session gate finto: concede sempre l'accesso."""
    return True


def _make_feed(on_disconnect=None, **cfg):
    """Costruisce uno StreamingFeed con dipendenze finte inerti."""
    return StreamingFeed(
        client_getter=_opaque,
        config=dict(cfg),
        on_market_book=_ignore,
        on_disconnect=on_disconnect,
        listener_factory=_opaque,
        session_gate=_allow,
    )


# ---------------------------------------------------------------------------
# 1. HEARTBEAT-DEAD: no-message, confine del timeout, floor 1.0s
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_heartbeat_not_dead_before_first_message():
    """Senza alcun messaggio ricevuto (_last_message_at<=0) il feed NON e'
    considerato morto: niente reconnect spurio prima del primo update."""
    feed = _make_feed(heartbeat_timeout_sec=2)
    assert feed._is_heartbeat_dead() is False


@pytest.mark.unit
def test_heartbeat_dead_only_strictly_past_timeout(monkeypatch):
    """La morte scatta SOLO oltre il timeout: a elapsed == timeout non e'
    ancora morto, appena oltre lo e'."""
    clock = {"t": 1000.0}
    monkeypatch.setattr("services.streaming_feed.time.monotonic", lambda: clock["t"])

    feed = _make_feed(heartbeat_timeout_sec=2)
    feed._last_message_at = 1000.0

    clock["t"] = 1002.0          # elapsed == timeout
    assert feed._is_heartbeat_dead() is False
    clock["t"] = 1002.01         # appena oltre
    assert feed._is_heartbeat_dead() is True


@pytest.mark.unit
def test_heartbeat_timeout_has_one_second_floor(monkeypatch):
    """Il timeout ha un floor di 1.0s: anche con heartbeat_timeout_sec=0 non
    si dichiara morto prima di 1 secondo reale (no flapping aggressivo)."""
    clock = {"t": 5000.0}
    monkeypatch.setattr("services.streaming_feed.time.monotonic", lambda: clock["t"])

    feed = _make_feed(heartbeat_timeout_sec=0)
    feed._last_message_at = 5000.0

    clock["t"] = 5000.5          # 0.5s: sotto il floor
    assert feed._is_heartbeat_dead() is False
    clock["t"] = 5001.5          # 1.5s: oltre il floor di 1.0
    assert feed._is_heartbeat_dead() is True


# ---------------------------------------------------------------------------
# 2. DEGRADAZIONE 503: healthy=False e flag persistente fino al reconnect
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_503_degradation_persists_until_reconnect():
    """Un 503 mette il feed in degraded (healthy=False) SENZA disconnettere, e
    il flag PERSISTE anche se riprende il traffico normale: si azzera solo alla
    riconnessione. Caratterizza il confine di osservabilita' (ops vede
    'degraded' finche' non riconnette)."""
    feed = _make_feed()
    feed._connected = True
    assert feed.healthy is True

    # 503: degradato, ma niente disconnect (resta connected).
    feed._process_message({"status": "503"})
    assert feed.status()["degraded_503"] is True
    assert feed.status()["connected"] is True
    assert feed.healthy is False

    # Traffico normale successivo: NON ripristina il flag (solo il reconnect lo fa).
    feed._process_message({"clk": "abc", "mc": []})
    assert feed.status()["degraded_503"] is True
    assert feed.healthy is False


# ---------------------------------------------------------------------------
# 3. SUBSCRIBE KWARGS: clk vuoto OMESSO, presente solo dopo cattura
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_subscribe_kwargs_omit_clk_when_empty_and_include_when_set():
    """A feed fresco (nessun clk catturato) le chiavi clk/initial_clk sono
    OMESSE: non si rischia di rigiocare un clk vuoto. Dopo la cattura (es. dal
    primo messaggio) compaiono nei kwargs della successiva subscribe."""
    feed = _make_feed(market_ids=["1.1"])

    fresh = feed._build_subscribe_kwargs()
    assert "clk" not in fresh
    assert "initial_clk" not in fresh

    feed._clk = "CLK1"
    feed._initial_clk = "INIT1"
    after = feed._build_subscribe_kwargs()
    assert after["clk"] == "CLK1"
    assert after["initial_clk"] == "INIT1"


# ---------------------------------------------------------------------------
# 4. on_disconnect che solleva: eccezione isolata, stato coerente
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_disconnect_callback_exception_is_isolated():
    """Se la callback on_disconnect solleva, l'eccezione e' isolata (non
    propaga da _notify_disconnect) e lo stato resta coerente: connected=False.
    Un subscriber difettoso non deve impedire la marcatura del disconnect."""
    received = []

    def bad_on_disconnect(payload):
        received.append(payload)
        raise RuntimeError("subscriber boom")

    feed = _make_feed(on_disconnect=bad_on_disconnect)
    feed._connected = True

    # Non deve sollevare nonostante la callback difettosa.
    feed._notify_disconnect(reason="transport_error", kind="transport")

    assert received, "la callback deve comunque essere invocata"
    assert feed.status()["connected"] is False
