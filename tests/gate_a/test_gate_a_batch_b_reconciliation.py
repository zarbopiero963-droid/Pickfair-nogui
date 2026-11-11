"""Gate A — batch B, reconciliation: audit fail-closed e outbox (core/reconciliation_engine.py).

Stub offline di DB e batch manager; si esercitano i metodi reali del ReconciliationEngine."""
import time

from core.reconciliation_engine import ReconcileConfig, ReconciliationEngine
from core.reconciliation_types import DecisionEntry


class _Bus:
    def __init__(self):
        self.events = []

    def publish(self, name, payload=None):
        self.events.append((name, payload))


class _DB:
    def __init__(self, outbox_error=False):
        self.outbox_error = outbox_error
        self.outbox_writes = []

    def persist_decision_log(self, batch_id, entries):
        return None

    def get_pending_sagas(self):
        return []

    def write_outbox(self, entry):
        self.outbox_writes.append(entry)
        if self.outbox_error:
            raise OSError("disk full")


class _BM:
    def __init__(self):
        self.updates = []

    def get_batch(self, batch_id):
        return {"batch_id": batch_id, "market_id": "", "status": "LIVE"}

    def get_batch_legs(self, batch_id):
        return []

    def update_leg_status(self, batch_id, leg_index, status, **kw):
        self.updates.append((batch_id, leg_index, status))

    def recompute_batch_status(self, batch_id):
        return {"status": "LIVE"}

    def release_runtime_artifacts(self, batch_id):
        return None

    def mark_batch_failed(self, batch_id, reason=""):
        return None

    def get_open_batches(self):
        return []


def _engine(db=None, bm=None, bus=None):
    return ReconciliationEngine(db=db or _DB(), bus=bus or _Bus(), batch_manager=bm or _BM(),
                                config=ReconcileConfig(audit_fail_closed=True))


def _decision(persist_ok):
    return DecisionEntry(timestamp=time.time(), batch_id="B1", leg_index=0, case_classification="X",
                         reason_code="TEST", local_status="SUBMITTED", exchange_status="EXECUTION_COMPLETE",
                         resolved_status="MATCHED", merge_winner="EXCHANGE", persist_ok=persist_ok)


def test_trg_628_b_0873():
    """ID: TRG-628-B-0873 / PF-B1-RECON-AUDIT-DENY

    Audit non persistito => l'aggiornamento della gamba e' negato: False, nessun update, nessun evento outbox."""
    for persist_ok in (False, None):
        bm = _BM()
        eng = _engine(bm=bm)
        ok = eng._transactional_leg_update(batch_id="B1", leg_index=0, from_status="SUBMITTED",
                                           to_status="MATCHED", decision=_decision(persist_ok))
        assert ok is False
        assert bm.updates == [], "gamba aggiornata con audit non persistito"
        assert eng._outbox == []
    # controllo: audit persistito => update eseguito
    bm = _BM()
    eng = _engine(bm=bm)
    assert eng._transactional_leg_update(batch_id="B1", leg_index=0, from_status="SUBMITTED",
                                         to_status="MATCHED", decision=_decision(True)) is True
    assert bm.updates == [("B1", 0, "MATCHED")]


def test_trg_628_b_0874():
    """ID: TRG-628-B-0874 / PF-B1-RECON-OUTBOX-ERROR

    write_outbox fallito => l'evento resta comunque nell'outbox in memoria e viene consegnato al drain."""
    db, bus = _DB(outbox_error=True), _Bus()
    eng = _engine(db=db, bus=bus)
    eng._enqueue_outbox(batch_id="B1", event_name="LEG_STATUS_CHANGED", payload={"batch_id": "B1", "leg_index": 0})
    assert len(db.outbox_writes) == 1, "write_outbox non tentato"
    assert len(eng._outbox) == 1 and eng._outbox[0].event_name == "LEG_STATUS_CHANGED"
    assert eng._outbox[0].delivered is False
    assert eng._drain_outbox("B1") == 1
    assert ("LEG_STATUS_CHANGED", {"batch_id": "B1", "leg_index": 0}) in bus.events
