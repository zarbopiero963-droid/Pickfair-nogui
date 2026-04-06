from __future__ import annotations

import copy
import random

from core.reconciliation_engine import ReconcileConfig, ReconciliationEngine


# =========================================================
# FAKES
# =========================================================

class FakeDB:
    def __init__(self):
        self.persisted_logs = {}

    def persist_decision_log(self, batch_id, entries):
        self.persisted_logs.setdefault(batch_id, []).extend(copy.deepcopy(entries))


class FakeBatchManager:
    def __init__(self, batch, legs):
        self._batch = copy.deepcopy(batch)
        self._legs = copy.deepcopy(legs)
        self.update_calls = []

    def get_batch(self, batch_id):
        if batch_id == self._batch["batch_id"]:
            return copy.deepcopy(self._batch)
        return None

    def get_batch_legs(self, batch_id):
        if batch_id == self._batch["batch_id"]:
            return copy.deepcopy(self._legs)
        return []

    def update_leg_status(
        self,
        *,
        batch_id,
        leg_index,
        status,
        bet_id=None,
        raw_response=None,
        error_text=None,
    ):
        self.update_calls.append(
            {
                "batch_id": batch_id,
                "leg_index": leg_index,
                "status": status,
                "bet_id": bet_id,
                "error_text": error_text,
            }
        )
        for leg in self._legs:
            if int(leg["leg_index"]) == int(leg_index):
                leg["status"] = status
                if bet_id is not None:
                    leg["bet_id"] = bet_id
                if raw_response is not None:
                    leg["raw_response"] = copy.deepcopy(raw_response)
                if error_text is not None:
                    leg["error_text"] = error_text
                break

    def recompute_batch_status(self, batch_id):
        statuses = {str(l["status"]).upper() for l in self._legs}
        if statuses == {"MATCHED"}:
            self._batch["status"] = "EXECUTED"
        elif "FAILED" in statuses and "MATCHED" not in statuses:
            self._batch["status"] = "FAILED"
        elif "PARTIAL" in statuses:
            self._batch["status"] = "PARTIAL"
        else:
            self._batch["status"] = "LIVE"
        return copy.deepcopy(self._batch)

    def release_runtime_artifacts(self, **kwargs):
        return None

    def get_open_batches(self):
        return [copy.deepcopy(self._batch)]

    def mark_batch_failed(self, *_args, **_kwargs):
        self._batch["status"] = "FAILED"


class DeterministicEngine(ReconciliationEngine):
    """
    Engine con feed remoto controllabile per test di determinismo/convergenza.
    """

    def __init__(self, *args, remote_cycles=None, pending_refs=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._remote_cycles = list(remote_cycles or [])
        self._remote_idx = 0
        self._pending_refs = set(pending_refs or [])

    def _fetch_current_orders_by_market(self, market_id: str, *, _attempt: int = 0):
        if not self._remote_cycles:
            return [], None
        idx = min(self._remote_idx, len(self._remote_cycles) - 1)
        payload = copy.deepcopy(self._remote_cycles[idx])
        self._remote_idx += 1
        return payload, None

    def _get_pending_saga_refs(self):
        return set(self._pending_refs)


# =========================================================
# HELPERS
# =========================================================

def _batch():
    return {
        "batch_id": "B100",
        "market_id": "1.100",
        "status": "LIVE",
    }


def _legs():
    return [
        {
            "leg_index": 2,
            "status": "SUBMITTED",
            "customer_ref": "REF-2",
            "bet_id": "",
            "selection_id": 22,
            "market_id": "1.100",
            "created_at_ts": 1000.0,
        },
        {
            "leg_index": 1,
            "status": "UNKNOWN",
            "customer_ref": "REF-1",
            "bet_id": "",
            "selection_id": 11,
            "market_id": "1.100",
            "created_at_ts": 0.0,
        },
    ]


def _remote_orders():
    return [
        {
            "customerOrderRef": "REF-1",
            "betId": "BET-1",
            "status": "EXECUTION_COMPLETE",
            "sizeMatched": 10.0,
            "sizeRemaining": 0.0,
            "selectionId": 11,
        },
        {
            "customerOrderRef": "REF-2",
            "betId": "BET-2",
            "status": "EXECUTABLE",
            "sizeMatched": 4.0,
            "sizeRemaining": 6.0,
            "selectionId": 22,
        },
    ]


def _normalize_logs(entries):
    out = []
    for e in entries:
        row = dict(e)
        row.pop("timestamp", None)
        out.append(row)
    return out


def _build_engine(batch=None, legs=None, remote_cycles=None):
    db = FakeDB()
    bm = FakeBatchManager(batch or _batch(), legs or _legs())
    engine = DeterministicEngine(
        db=db,
        batch_manager=bm,
        client_getter=lambda: None,
        config=ReconcileConfig(
            max_convergence_cycles=5,
            convergence_sleep_secs=0.0,
            unknown_grace_secs=0.0,
        ),
        remote_cycles=remote_cycles or [_remote_orders()],
    )
    return engine, db, bm


# =========================================================
# AREA 2 — DETERMINISMO FORTE
# =========================================================

def test_same_input_same_output():
    """
    Cosa uccide:
      output diverso a parità di input.
    Invariant:
      reconcile deterministico rispetto a batch, legs, remote orders, config.
    Mutation che deve fallire:
      introdurre ordine casuale o timestamp dentro decision path.
    """
    remote = [_remote_orders()]

    engine1, db1, bm1 = _build_engine(remote_cycles=remote)
    engine2, db2, bm2 = _build_engine(remote_cycles=remote)

    res1 = engine1.reconcile_batch("B100")
    res2 = engine2.reconcile_batch("B100")

    assert res1 == res2
    assert bm1._legs == bm2._legs
    assert bm1._batch == bm2._batch
    assert _normalize_logs(db1.persisted_logs["B100"]) == _normalize_logs(
        db2.persisted_logs["B100"]
    )


def test_order_independence_local_legs():
    """
    Cosa uccide:
      dipendenza dall’ordine della lista locale.
    Invariant:
      permutare le legs non cambia risultato finale.
    Mutation che deve fallire:
      rimuovere sorting o usare primo match trovato in modo instabile.
    """
    batch = _batch()
    legs_a = _legs()
    legs_b = list(reversed(copy.deepcopy(legs_a)))
    remote = [_remote_orders()]

    engine_a, db_a, bm_a = _build_engine(batch=batch, legs=legs_a, remote_cycles=remote)
    engine_b, db_b, bm_b = _build_engine(batch=batch, legs=legs_b, remote_cycles=remote)

    res_a = engine_a.reconcile_batch("B100")
    res_b = engine_b.reconcile_batch("B100")

    assert res_a == res_b
    assert sorted(bm_a._legs, key=lambda x: x["leg_index"]) == sorted(
        bm_b._legs, key=lambda x: x["leg_index"]
    )
    assert _normalize_logs(db_a.persisted_logs["B100"]) == _normalize_logs(
        db_b.persisted_logs["B100"]
    )


def test_order_independence_remote_orders():
    """
    Cosa uccide:
      dipendenza dall’ordine degli ordini exchange.
    Invariant:
      permutare ordini remoti non cambia merge né fingerprint.
    Mutation che deve fallire:
      confrontare liste raw senza normalizzazione.
    """
    remote_a = [_remote_orders()]
    shuffled = copy.deepcopy(_remote_orders())
    random.Random(123).shuffle(shuffled)
    remote_b = [shuffled]

    engine_a, db_a, bm_a = _build_engine(remote_cycles=remote_a)
    engine_b, db_b, bm_b = _build_engine(remote_cycles=remote_b)

    fp_a = engine_a._compute_fingerprint(bm_a.get_batch_legs("B100"), remote_a[0])
    fp_b = engine_b._compute_fingerprint(bm_b.get_batch_legs("B100"), remote_b[0])

    res_a = engine_a.reconcile_batch("B100")
    res_b = engine_b.reconcile_batch("B100")

    assert fp_a == fp_b
    assert res_a == res_b
    assert sorted(bm_a._legs, key=lambda x: x["leg_index"]) == sorted(
        bm_b._legs, key=lambda x: x["leg_index"]
    )
    assert _normalize_logs(db_a.persisted_logs["B100"]) == _normalize_logs(
        db_b.persisted_logs["B100"]
    )


def test_fingerprint_stability():
    """
    Cosa uccide:
      fingerprint instabile a parità di stato logico.
    Invariant:
      stesso stato canonico produce stesso fingerprint.
    Mutation che deve fallire:
      cambiare struttura hash o includere campi non deterministici.
    """
    engine, _, bm = _build_engine()

    remote1 = _remote_orders()
    remote2 = copy.deepcopy(remote1)

    # stesso contenuto logico, ordine diverso
    remote2 = list(reversed(remote2))

    fp1 = engine._compute_fingerprint(bm.get_batch_legs("B100"), remote1)
    fp2 = engine._compute_fingerprint(bm.get_batch_legs("B100"), remote2)

    assert fp1 == fp2


def test_convergence_not_early_break():
    """
    Cosa uccide:
      break prematuro prima della stabilizzazione reale.
    Invariant:
      si esce solo quando lo stato è davvero stabile o si raggiunge il cap.
    Mutation che deve fallire:
      break appena un ciclo non cambia localmente senza refetch corretto.
    """
    batch = _batch()
    legs = [
        {
            "leg_index": 0,
            "status": "SUBMITTED",
            "customer_ref": "REF-X",
            "bet_id": "",
            "selection_id": 99,
            "market_id": "1.100",
            "created_at_ts": 9999999999.0,
        }
    ]

    # ciclo 1: nessun ordine remoto → nessun cambio locale ancora lecito
    # ciclo 2: ordine remote matched → deve continuare e convergere a MATCHED
    remote_cycles = [
        [],
        [
            {
                "customerOrderRef": "REF-X",
                "betId": "BET-X",
                "status": "EXECUTION_COMPLETE",
                "sizeMatched": 10.0,
                "sizeRemaining": 0.0,
                "selectionId": 99,
            }
        ],
    ]

    engine, db, bm = _build_engine(batch=batch, legs=legs, remote_cycles=remote_cycles)

    result = engine.reconcile_batch("B100")

    # questo test è volutamente severo:
    # se il reconcile breaka troppo presto, la leg resta SUBMITTED/FAILED
    # invece vogliamo che arrivi a MATCHED dopo refetch corretto
    final_leg = bm._legs[0]

    assert result["ok"] is True
    assert final_leg["status"] == "MATCHED"