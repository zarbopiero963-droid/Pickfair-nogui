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
def test_503_degradation_persists_across_normal_traffic():
    """Un 503 mette il feed in degraded (healthy=False) SENZA disconnettere, e
    il flag PERSISTE anche se riprende il traffico normale: un messaggio valido
    NON lo ripulisce (solo la riconnessione lo fa, vedi test sotto). Caratterizza
    il confine di osservabilita' (ops vede 'degraded' finche' non riconnette)."""
    feed = _make_feed()
    feed._connected = True
    assert feed.healthy is True

    # 503: degradato, ma niente disconnect (resta connected).
    feed._process_message({"status": "503"})
    assert feed.status()["degraded_503"] is True
    assert feed.status()["connected"] is True
    assert feed.healthy is False

    # Traffico normale successivo: NON ripristina il flag.
    feed._process_message({"clk": "abc", "mc": []})
    assert feed.status()["degraded_503"] is True
    assert feed.healthy is False


# Stub minimi per pilotare UN ciclo di _connect_and_consume in modo
# deterministico (no thread, no sleep). I metodi senza self sono @staticmethod
# per non introdurre antipattern.
class _StubListener:
    """Listener finto: espone solo l'output_queue richiesta dal feed."""

    def __init__(self, *, output_queue):
        self.output_queue = output_queue


class _StubStream:
    """Stream finto: la subscribe e' un no-op."""

    def __init__(self, output_queue):
        self.output_queue = output_queue

    @staticmethod
    def subscribe_to_markets(**_kwargs):
        return None


class _StubStreaming:
    """API streaming finta: crea uno _StubStream sulla coda del listener."""

    @staticmethod
    def create_stream(listener):
        return _StubStream(listener.output_queue)


class _StubClient:
    """Client finto con l'interfaccia .streaming attesa da _connect_and_consume."""

    streaming = _StubStreaming()


@pytest.mark.unit
def test_503_flag_is_reset_on_reconnect(monkeypatch):
    """Controprova del confine: una riconnessione AZZERA `degraded_503` e
    ripristina healthy=True. Pilotiamo un singolo ciclo di _connect_and_consume
    in modo deterministico (reader/keepalive resi no-op, stop_event impostato
    cosi' il loop esce subito dopo il reset sincrono)."""
    feed = _make_feed(market_ids=["1.1"])
    feed.client_getter = _StubClient
    feed.listener_factory = _StubListener

    # Niente thread: rende il test deterministico.
    monkeypatch.setattr(feed, "_start_stream_reader", _ignore)
    monkeypatch.setattr(feed, "_start_keepalive_loop", _ignore)

    # Stato pre-riconnessione: degradato e disconnesso.
    feed._degraded_503 = True
    feed._connected = False
    feed._stop_event.set()

    feed._connect_and_consume()

    assert feed.status()["degraded_503"] is False
    assert feed.status()["connected"] is True
    assert feed.healthy is True


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


# ---------------------------------------------------------------------------
# 5. IDEMPOTENZA start()/stop(): niente doppi thread, stop safe, restart pulito
# ---------------------------------------------------------------------------

def _quiet_loop(feed, monkeypatch):
    """Rende `_run_loop` un no-op che attende solo lo stop: il thread parte ed
    esce subito quando stop() imposta l'evento (niente loop/rete/sleep reali)."""
    monkeypatch.setattr(feed, "_run_loop", lambda: feed._stop_event.wait())


@pytest.mark.unit
def test_double_start_does_not_spawn_second_thread(monkeypatch):
    """Due `start()` rapidi NON creano un secondo thread/handler: il secondo
    ritorna `already_running` e il thread resta lo stesso."""
    feed = _make_feed(market_ids=["1.1"])
    _quiet_loop(feed, monkeypatch)
    try:
        first = feed.start()
        thread = feed._run_thread
        second = feed.start()
        assert first == {"started": True}
        assert second.get("reason") == "already_running"
        assert feed._run_thread is thread          # stesso thread, non un secondo
        assert feed.status()["running"] is True
    finally:
        feed.stop()
    assert feed.status()["running"] is False


@pytest.mark.unit
def test_double_stop_is_idempotent_and_safe():
    """`stop()` e' idempotente e sicuro: anche senza start, e ripetuto, non
    solleva e lascia il feed fermo/disconnesso."""
    feed = _make_feed(market_ids=["1.1"])
    assert feed.stop() == {"stopped": True}        # stop senza start: no-op safe
    assert feed.stop() == {"stopped": True}        # ripetuto: idempotente
    assert feed.status()["running"] is False
    assert feed.status()["connected"] is False


@pytest.mark.unit
def test_rapid_start_stop_cleans_up_and_allows_restart(monkeypatch):
    """Un ciclo start->stop rapido pulisce il thread (running=False), e un
    successivo start riparte correttamente (stop_event ripulito da start)."""
    feed = _make_feed(market_ids=["1.1"])
    _quiet_loop(feed, monkeypatch)

    feed.start()
    feed.stop()
    assert feed.status()["running"] is False
    assert feed.status()["connected"] is False

    feed.start()                                   # restart dopo stop
    try:
        assert feed.status()["running"] is True
    finally:
        feed.stop()
    assert feed.status()["running"] is False


# ---------------------------------------------------------------------------
# 6. STALENESS con jump-tempo-lungo simulato (no sleep reali)
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_heartbeat_dead_after_long_simulated_time_jump(monkeypatch):
    """Un lungo salto temporale simulato (clock finto, niente sleep) oltre il
    timeout rende il feed stale; un nuovo messaggio ripristina la freschezza."""
    clock = {"t": 1000.0}
    monkeypatch.setattr("services.streaming_feed.time.monotonic", lambda: clock["t"])

    feed = _make_feed(heartbeat_timeout_sec=2)
    feed._last_message_at = 1000.0
    assert feed._is_heartbeat_dead() is False

    clock["t"] = 1000.0 + 100.0                    # +100s simulati
    assert feed._is_heartbeat_dead() is True

    # Un messaggio fresco aggiorna last_message_at => non piu' stale.
    feed._process_message({"clk": "x", "mc": []})
    assert feed._is_heartbeat_dead() is False
