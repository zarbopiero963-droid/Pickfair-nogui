"""Proof operazionali EventBus (test_suite_proof_eventbus): gap netto-nuovi.

Phase 0 dedup (verificato): l'EventBus e' GIA' coperto in modo forte da 35
test su 6 file (no-loss sotto publish parallelo 50/200, isolamento errori,
snapshot-fanout, stop drain/lossy, per-subscriber error counts, poison-pill
end-to-end, metriche published/delivered, pressure snapshot, subscribe/
unsubscribe concorrente sotto carico). Scrivere quelli sarebbe duplicazione.

Qui SOLO le garanzie operative deterministiche ancora non verificate, tutte
caratterizzate sul comportamento REALE (probe):

1. ORDERING FIFO per event_type con un singolo worker: gli eventi pubblicati
   in sequenza arrivano al subscriber nello stesso ordine (flusso d'ordine:
   placed -> filled -> cancelled non deve riordinarsi).
2. RE-ENTRANCY: un handler che pubblica un altro evento durante la propria
   esecuzione non va in deadlock; l'evento figlio viene consegnato DOPO il
   padre (cascade placed -> hedge -> unwind).
3. CONSISTENZA CONTATORI: dopo stop(drain=True) ogni item accodato e' o
   delivered o conteggiato come errore -> delivered_total + somma errori ==
   enqueued_total == dequeued_total. Invariante anti-perdita-silenziosa su cui
   si appoggiano gli anomaly detector.
4. UNSUBSCRIBE IDEMPOTENTE: rimuovere un event_type sconosciuto o una callback
   non registrata (o ri-rimuovere) e' un no-op sicuro, mai un'eccezione.
5. IDENTITA' DEL PAYLOAD: il subscriber riceve esattamente l'oggetto passato a
   publish() (nessuna copia/corruzione in transito).

Tutto pure-unit e deterministico: worker singolo per l'ordine, Event di
sincronizzazione per la cascata, stop(drain=True) come barriera prima delle
asserzioni. Nessuna modifica al codice di produzione (core/event_bus.py).
"""
from __future__ import annotations

import threading

import pytest

from core.event_bus import EventBus

# ---------------------------------------------------------------------------
# 1. ORDERING FIFO (single worker)
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.concurrency
def test_single_worker_preserves_publish_order_per_event_type():
    """Con un solo worker la coda e' FIFO: il subscriber vede gli eventi
    nell'ordine esatto di pubblicazione, senza riordino."""
    bus = EventBus(workers=1)
    seen: list[int] = []
    bus.subscribe("ORDER_FLOW", seen.append)
    try:
        for i in range(25):
            bus.publish("ORDER_FLOW", i)
    finally:
        res = bus.stop()  # drain: barriera, processa tutto prima di asserire

    assert res == {"drain": True, "dropped_events": 0}
    assert seen == list(range(25))


# ---------------------------------------------------------------------------
# 2. RE-ENTRANCY (handler che pubblica durante la propria callback)
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.concurrency
def test_reentrant_publish_from_handler_delivers_child_without_deadlock():
    """Un handler puo' pubblicare un altro evento dentro la propria esecuzione
    (cascade): nessun deadlock, e il figlio e' consegnato DOPO il padre.

    Sincronizziamo sulla consegna del figlio PRIMA di stop(): la publish
    re-entrante deve avvenire mentre il bus accetta ancora (stop() mette
    accepting=False)."""
    bus = EventBus(workers=1)
    order: list[tuple[str, int]] = []
    child_done = threading.Event()

    def parent(d):
        order.append(("parent", d))
        bus.publish("CHILD", d + 100)  # re-entrant, accepting ancora True

    def child(d):
        order.append(("child", d))
        child_done.set()

    bus.subscribe("PARENT", parent)
    bus.subscribe("CHILD", child)

    bus.publish("PARENT", 1)
    delivered = child_done.wait(timeout=5.0)  # niente deadlock
    res = bus.stop()

    assert delivered is True, "cascade child non consegnato (possibile deadlock)"
    assert order == [("parent", 1), ("child", 101)]
    assert res["drain"] is True


# ---------------------------------------------------------------------------
# 3. CONSISTENZA CONTATORI (no perdita silenziosa dopo drain)
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_counters_balance_after_drain_no_silent_loss():
    """Dopo stop(drain=True), ogni item accodato e' stato consumato esattamente
    una volta: delivered_total + somma(errori) == enqueued_total ==
    dequeued_total. Un handler sano e uno che lancia sempre -> meta' delivered,
    meta' errori, ma il totale quadra (niente eventi persi nel silenzio)."""
    bus = EventBus(workers=2)

    def ok(_d):
        return None

    def boom(_d):
        raise RuntimeError("subscriber esplode")

    bus.subscribe("T", ok)
    bus.subscribe("T", boom)

    n = 12
    for i in range(n):
        bus.publish("T", i)
    bus.stop()

    snap = bus.pressure_snapshot()
    delivered = bus.delivered_total_count()
    errors = sum(bus.subscriber_error_counts().values())

    # 12 publish x 2 subscriber = 24 item accodati
    assert snap["enqueued_total"] == n * 2
    assert snap["dequeued_total"] == n * 2
    # invariante anti-perdita: ogni item o delivered o errore, esattamente una volta
    assert delivered + errors == snap["enqueued_total"]
    assert delivered == n   # solo l'handler sano
    assert errors == n      # solo l'handler che lancia
    # published conta una volta per publish con >=1 subscriber (non per fanout)
    assert bus.published_total_count() == n


# ---------------------------------------------------------------------------
# 4. UNSUBSCRIBE IDEMPOTENTE (cleanup resiliente)
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_unsubscribe_unknown_or_unregistered_is_safe_noop():
    """Rimuovere un event_type sconosciuto, una callback non registrata, o
    ri-rimuovere una callback gia' tolta non solleva eccezioni ed e' un no-op:
    il cleanup di grafi di subscription complessi resta robusto."""
    bus = EventBus(workers=1)

    def handler(_d):
        return None

    try:
        bus.unsubscribe("MAI_VISTO", handler)          # event_type sconosciuto
        bus.subscribe("X", handler)
        bus.unsubscribe("X", lambda z: z)               # callback non registrata
        bus.unsubscribe("X", handler)                   # rimozione reale
        bus.unsubscribe("X", handler)                   # ri-rimozione: ora sconosciuta
    finally:
        bus.stop()

    # tutte le subscription rimosse: stats pulito, nessun residuo
    assert bus.stats()["subscribers"] == {}


# ---------------------------------------------------------------------------
# 5. IDENTITA' DEL PAYLOAD (nessuna copia/corruzione in transito)
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_payload_object_identity_is_preserved_through_dispatch():
    """Il subscriber riceve ESATTAMENTE l'oggetto passato a publish(): il bus
    non copia ne' avvolge il payload. Garanzia necessaria perche' i consumer
    si affidano all'identita' (es. correlazione per riferimento)."""
    bus = EventBus(workers=1)
    received: list[object] = []
    payload = {"id": 1, "legs": []}

    bus.subscribe("P", received.append)
    bus.publish("P", payload)
    bus.stop()

    assert len(received) == 1
    assert received[0] is payload
