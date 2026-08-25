"""Intake delle ambiguita' di submission nel ReconciliationEngine.

Piano #437 punto 4 (PR «iniezione ReconciliationEngine»): il TradingEngine
chiama ``enqueue(**meta)`` per ogni submission ambigua (timeout, risposta
persa). Il motore reale deve esporre quell'intake per davvero: registro
pending deduplicato, con cap, thread-safe, evento bus di visibilita',
MAI un'eccezione verso il percorso ordine, e drain quando il batch
dell'ambiguita' viene riconciliato.
"""

from __future__ import annotations

import threading

import pytest

from core.reconciliation_engine import ReconciliationEngine
from tests.fixtures.fake_batch_manager import FakeBatchManager


class _FakeDB:
    def __init__(self, batch_sagas=None):
        self._batch_sagas = batch_sagas or {}

    @staticmethod
    def persist_decision_log(batch_id, entries):
        return None

    @staticmethod
    def get_pending_sagas():
        return []

    @staticmethod
    def get_reconcile_marker(batch_id):
        return None

    @staticmethod
    def set_reconcile_marker(batch_id, value):
        return None

    def get_batch_sagas(self, batch_id):
        return list(self._batch_sagas.get(batch_id, []))


class _FakeBus:
    def __init__(self):
        self.events = []

    def publish(self, name, payload):
        self.events.append((name, payload))


class _BatchManagerConBatchAperti(FakeBatchManager):
    def __init__(self, aperti=None):
        super().__init__()
        self._aperti = aperti or []

    def get_open_batches(self):
        return list(self._aperti)


def _make_engine(db=None, batch_manager=None):
    return ReconciliationEngine(
        db=db or _FakeDB(),
        bus=_FakeBus(),
        batch_manager=batch_manager or FakeBatchManager(),
        client_getter=lambda: None,
    )


@pytest.mark.unit
def test_enqueue_registra_ambiguita_e_pubblica_evento():
    eng = _make_engine()
    eng.enqueue(
        order_id="OID1",
        ambiguity_reason="SUBMIT_TIMEOUT",
        customer_ref="REF1",
        correlation_id="CID1",
    )

    snapshot = eng.ambiguity_snapshot()
    assert len(snapshot) == 1
    (entry,) = snapshot.values()
    assert entry["order_id"] == "OID1"
    assert entry["ambiguity_reason"] == "SUBMIT_TIMEOUT"
    assert entry["customer_ref"] == "REF1"

    eventi = [nome for nome, _ in eng.bus.events]
    assert "RECONCILE_AMBIGUITY_ENQUEUED" in eventi


@pytest.mark.unit
def test_enqueue_dedupe_stessa_ambiguita():
    eng = _make_engine()
    for _ in range(3):
        eng.enqueue(
            order_id="OID1",
            ambiguity_reason="SUBMIT_TIMEOUT",
            customer_ref="REF1",
            correlation_id="CID1",
        )
    assert len(eng.ambiguity_snapshot()) == 1


@pytest.mark.unit
def test_enqueue_mai_raise_su_meta_malformata():
    eng = _make_engine()
    # Meta senza identificatori, con tipi strani: mai un'eccezione verso il
    # percorso ordine (il chiamante e' _resolve_ambiguity, in piena submission).
    eng.enqueue()
    eng.enqueue(order_id=None, ambiguity_reason=None, customer_ref=None)
    eng.enqueue(order_id=object(), copy_meta={"nested": object()})
    # Il registro puo' contenere gli scarti normalizzati, ma non deve rompersi.
    assert isinstance(eng.ambiguity_snapshot(), dict)


@pytest.mark.unit
def test_enqueue_cap_limita_la_coda_drop_oldest():
    eng = _make_engine()
    cap = eng.AMBIGUITY_QUEUE_CAP
    for i in range(cap + 25):
        eng.enqueue(
            order_id=f"OID{i}",
            ambiguity_reason="SUBMIT_TIMEOUT",
            customer_ref=f"REF{i}",
            correlation_id=f"CID{i}",
        )
    snapshot = eng.ambiguity_snapshot()
    assert len(snapshot) == cap
    refs = {e["customer_ref"] for e in snapshot.values()}
    # Drop-oldest: i primi 25 sono usciti, l'ultimo inserito c'e'.
    assert "REF0" not in refs
    assert f"REF{cap + 24}" in refs


@pytest.mark.unit
def test_enqueue_thread_safe():
    eng = _make_engine()
    n_thread, per_thread = 8, 25

    def _lavora(tid: int) -> None:
        for i in range(per_thread):
            eng.enqueue(
                order_id=f"OID-{tid}-{i}",
                ambiguity_reason="SUBMIT_TIMEOUT",
                customer_ref=f"REF-{tid}-{i}",
                correlation_id=f"CID-{tid}-{i}",
            )

    threads = [threading.Thread(target=_lavora, args=(t,)) for t in range(n_thread)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(eng.ambiguity_snapshot()) == n_thread * per_thread


@pytest.mark.unit
def test_snapshot_copia_difensiva():
    eng = _make_engine()
    eng.enqueue(order_id="OID1", ambiguity_reason="SUBMIT_TIMEOUT",
                customer_ref="REF1", correlation_id="CID1")
    snap = eng.ambiguity_snapshot()
    snap.clear()
    assert len(eng.ambiguity_snapshot()) == 1


@pytest.mark.unit
def test_drain_su_reconcile_all_open_batches(monkeypatch):
    """Quando il batch di un'ambiguita' pending viene riconciliato (esito ok,
    saga terminale), l'entry esce dal registro; le ambiguita' di ALTRI batch
    restano pending."""
    db = _FakeDB(batch_sagas={"B1": [{"customer_ref": "REF1", "status": "MATCHED"}]})
    bm = _BatchManagerConBatchAperti(aperti=[{"batch_id": "B1"}])
    eng = _make_engine(db=db, batch_manager=bm)

    eng.enqueue(order_id="OID1", ambiguity_reason="SUBMIT_TIMEOUT",
                customer_ref="REF1", correlation_id="CID1")
    eng.enqueue(order_id="OID2", ambiguity_reason="SUBMIT_TIMEOUT",
                customer_ref="REF-ALTRO-BATCH", correlation_id="CID2")

    monkeypatch.setattr(
        eng, "reconcile_batch", lambda batch_id: {"ok": True, "batch_id": batch_id}
    )
    eng.reconcile_all_open_batches()

    refs_pending = {e["customer_ref"] for e in eng.ambiguity_snapshot().values()}
    assert "REF1" not in refs_pending, "ambiguita' del batch riconciliato non drenata"
    assert "REF-ALTRO-BATCH" in refs_pending, "drain troppo largo: ha perso un pending estraneo"


@pytest.mark.unit
def test_drain_non_scatta_su_reconcile_non_riuscito(monkeypatch):
    """Bloccante GPT-5.6 Sol su #444: reconcile_batch puo' tornare senza
    sollevare con esiti NON riconciliati (lock-busy RECONCILE_ALREADY_RUNNING,
    BATCH_NOT_FOUND, fetch failure). Il drain NON deve scattare: l'ambiguita'
    non e' stata guardata, la sua visibilita' resta."""
    db = _FakeDB(batch_sagas={"B1": [{"customer_ref": "REF1", "status": "MATCHED"}]})
    bm = _BatchManagerConBatchAperti(aperti=[{"batch_id": "B1"}])
    eng = _make_engine(db=db, batch_manager=bm)

    eng.enqueue(order_id="OID1", ambiguity_reason="SUBMIT_TIMEOUT",
                customer_ref="REF1", correlation_id="CID1")

    monkeypatch.setattr(
        eng, "reconcile_batch",
        lambda batch_id: {"ok": False, "batch_id": batch_id,
                          "reason_code": "RECONCILE_ALREADY_RUNNING"},
    )
    eng.reconcile_all_open_batches()

    refs_pending = {e["customer_ref"] for e in eng.ambiguity_snapshot().values()}
    assert "REF1" in refs_pending, (
        "drain su esito non riconciliato: ambiguita' persa senza che nessuno "
        "l'abbia mai guardata sull'exchange"
    )


@pytest.mark.unit
def test_drain_solo_saghe_terminali(monkeypatch):
    """Bloccante GPT-5.6 Sol su #444 (precisione a livello ordine): dentro un
    batch riconciliato con esito ok, esce dal registro SOLO l'ambiguita' la
    cui saga e' terminale; una saga ancora pending (PENDING/SUBMITTED/PLACED/
    PARTIAL/ROLLBACK_PENDING) tiene viva la propria entry."""
    db = _FakeDB(batch_sagas={"B1": [
        {"customer_ref": "REF1", "status": "MATCHED"},
        {"customer_ref": "REF3", "status": "PENDING"},
        {"customer_ref": "REF4", "status": ""},  # stato ignoto => conservativo
    ]})
    bm = _BatchManagerConBatchAperti(aperti=[{"batch_id": "B1"}])
    eng = _make_engine(db=db, batch_manager=bm)

    for ref, oid, cid in (("REF1", "OID1", "CID1"),
                          ("REF3", "OID3", "CID3"),
                          ("REF4", "OID4", "CID4")):
        eng.enqueue(order_id=oid, ambiguity_reason="SUBMIT_TIMEOUT",
                    customer_ref=ref, correlation_id=cid)

    monkeypatch.setattr(
        eng, "reconcile_batch", lambda batch_id: {"ok": True, "batch_id": batch_id}
    )
    eng.reconcile_all_open_batches()

    refs_pending = {e["customer_ref"] for e in eng.ambiguity_snapshot().values()}
    assert "REF1" not in refs_pending, "saga terminale non drenata"
    assert "REF3" in refs_pending, "saga ancora pending drenata: visibilita' persa"
    assert "REF4" in refs_pending, "stato saga ignoto drenato: non conservativo"
