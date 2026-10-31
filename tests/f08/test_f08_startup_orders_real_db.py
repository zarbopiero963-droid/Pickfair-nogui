"""F-08 (#586, RST-02/NF-10/DB-04, P0) — ordini fantasma all'avvio su DB SQLite REALE (RED prima della patch).

Oggi `startup_orders` e `outbox` non sono mai create: `startup_order_exists` solleva OperationalError, che
`StateRecovery._is_missing_in_db` cattura restituendo False ("non mancante") => il ghost non e' persistito ne'
seminato nel DuplicationGuard e `recover()` dichiara `ok=True` (fail-open silenzioso). Il test esistente usa un
FakeDB e non lo vede.
"""
from __future__ import annotations

import sqlite3

import pytest

from core.duplication_guard import DuplicationGuard
from core.state_recovery import StateRecovery
from database import Database

pytestmark = [pytest.mark.recovery, pytest.mark.safety]

GHOST = {"order_id": "BET-GHOST-1", "bet_id": "BET-GHOST-1", "market_id": "1.700", "selection_id": 11,
         "side": "BACK", "price": 2.0, "size": 3.0, "event_key": "1.700:11:BACK:ghost"}


class _Reconcile:
    def __init__(self, orders):
        self.orders = orders

    def fetch_startup_active_orders(self):
        return [dict(o) for o in self.orders]


class _Bus:
    def __init__(self):
        self.events = []

    def publish(self, name, payload):
        self.events.append((name, payload))


def _tables(path):
    con = sqlite3.connect(path)
    try:
        return {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    finally:
        con.close()


def test_startup_orders_and_outbox_tables_exist_on_real_db(tmp_path):
    path = str(tmp_path / "pf.db")
    Database(path)
    assert {"startup_orders", "outbox"} <= _tables(path)


def test_ghost_is_persisted_and_seeded_on_real_db(tmp_path):
    db = Database(str(tmp_path / "pf.db"))
    guard, bus = DuplicationGuard(), _Bus()
    out = StateRecovery(db=db, bus=bus, reconciliation_engine=_Reconcile([GHOST]), duplication_guard=guard).recover()
    assert out["ok"] is True and out["ghost_orders_recovered"] == 1, out
    assert [o["order_id"] for o in db.load_startup_orders()] == ["BET-GHOST-1"]
    assert db.startup_order_exists(GHOST)
    assert any(name == "STARTUP_GHOST_ORDER_RECOVERED" for name, _ in bus.events)
    # al secondo avvio lo stesso ghost e' gia' noto: nessun doppio recupero
    again = StateRecovery(db=db, bus=_Bus(), reconciliation_engine=_Reconcile([GHOST]),
                          duplication_guard=DuplicationGuard()).recover()
    assert again["ok"] is True and again["ghost_orders_recovered"] == 0, again


def test_db_error_on_ghost_lookup_makes_recovery_not_ok(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "pf.db"))

    def boom(_order):
        raise sqlite3.OperationalError("disk I/O error")
    monkeypatch.setattr(db, "startup_order_exists", boom)
    out = StateRecovery(db=db, bus=_Bus(), reconciliation_engine=_Reconcile([GHOST]),
                        duplication_guard=DuplicationGuard()).recover()
    assert out["ok"] is False and out["reason"] == "STARTUP_RECONCILIATION_REQUIRED", out
    assert out["startup_mode"] == "BLOCKED"


def test_existing_db_without_the_tables_is_migrated_without_data_loss(tmp_path):
    path = str(tmp_path / "pf.db")
    db = Database(path)
    db.insert_order({"customer_ref": "sig-1", "correlation_id": "c-1", "status": "SUBMITTED",
                     "payload": {"market_id": "1.1"}})
    con = sqlite3.connect(path)
    con.execute("DROP TABLE IF EXISTS startup_orders")
    con.execute("DROP TABLE IF EXISTS outbox")
    con.commit()
    con.close()
    db2 = Database(path)                                  # riapertura = migrazione idempotente
    assert {"startup_orders", "outbox"} <= _tables(path)
    assert db2.find_duplicate_order(customer_ref="sig-1") is not None, "ordine esistente perso nella migrazione"
