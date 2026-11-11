"""Gate A T2b batch b4 — recovery: core.state_recovery.StateRecovery (db/engine/guard stub)."""
from __future__ import annotations

import pytest

from core.state_recovery import StateRecovery


class _Engine:
    def __init__(self, orders):
        self.orders = orders

    def fetch_startup_active_orders(self):
        return list(self.orders)


class _Guard:
    def __init__(self, with_register=True):
        self.registered, self.acquired = [], []
        if with_register:
            self.register_startup_order = lambda o: self.registered.append(o)

    def acquire(self, key):
        self.acquired.append(key)


class _FailDb:
    def __init__(self):
        self.upserts = []

    def startup_order_exists(self, order):
        raise RuntimeError("db down")

    def upsert_startup_order(self, order):
        self.upserts.append(order)


def test_trg_628_b_0856():
    """ID: TRG-628-B-0856 / PF-B1-RECOVERY-EXISTS-ERROR

    Un errore di startup_order_exists NON vale "gia' presente": si propaga da _is_missing_in_db e la
    riconciliazione d'avvio risulta non completata (completed False), recover() ok False / BLOCKED;
    nessun ghost persistito ne' seminato (F-08, PF-EXEC-06)."""
    order = {"bet_id": "B1", "event_key": "ek"}
    db, guard = _FailDb(), _Guard()
    sr = StateRecovery(db=db, reconciliation_engine=_Engine([order]), duplication_guard=guard)
    with pytest.raises(RuntimeError):
        sr._is_missing_in_db(order)
    rec = sr._reconcile_startup_ghost_orders()
    assert rec["required"] is True and rec["completed"] is False
    out = sr.recover()
    assert out["ok"] is False and out["startup_mode"] == "BLOCKED"
    assert db.upserts == [] and guard.registered == [] and guard.acquired == []


def test_trg_628_b_0857():
    """ID: TRG-628-B-0857 / PF-B1-RECOVERY-GHOST-SEED

    Il seme della guardia anti-duplicato usa register_startup_order quando disponibile (nessun acquire);
    solo in sua assenza ripiega su acquire(event_key) e, senza event_key, non fa nulla."""
    order = {"bet_id": "B1", "event_key": " ek "}
    g = _Guard(with_register=True)
    StateRecovery(duplication_guard=g)._seed_duplication_guard(order)
    assert g.registered == [order] and g.acquired == []
    g2 = _Guard(with_register=False)
    StateRecovery(duplication_guard=g2)._seed_duplication_guard(order)
    assert g2.acquired == ["ek"]
    g3 = _Guard(with_register=False)
    StateRecovery(duplication_guard=g3)._seed_duplication_guard({"bet_id": "B2"})
    assert g3.acquired == []
