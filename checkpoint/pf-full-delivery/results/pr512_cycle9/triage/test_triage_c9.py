"""Throwaway triage tests, cycle 9 Codex findings on 2c6aa09 (HEAD 916fda4). NOT for the repo."""
import json
import os
import sys
import threading

import pytest

ROOT = "/home/user/Pickfair-nogui"
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tests"))
sys.path.insert(0, os.path.join(ROOT, "tests", "f026"))

from tests.acceptance.test_issue437_pr27 import _catena, _rifiuti  # noqa: E402
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _signal  # noqa: E402
from core import duplication_guard as dg_mod  # noqa: E402


# ------------------------------------------------------------------ F1: dedupe TTL vs live/retained position
def _advance_monotonic(monkeypatch, seconds):
    base = dg_mod.time.monotonic()
    monkeypatch.setattr(dg_mod.time, "monotonic", lambda: base + seconds)


def test_f1_live_position_older_than_ttl_accepts_a_second_order_for_the_same_event_no_reset(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    c.rc._on_signal_received(_signal())
    assert len(c.broker.state.orders) == 1, _rifiuti(c)
    key = c.rc.duplication_guard.build_event_key(_signal())
    assert c.rc.table_manager.find_by_event_key(key) is not None
    _advance_monotonic(monkeypatch, c.rc.duplication_guard.ttl + 1)
    c.rc._on_signal_received(_signal())
    print("REJECTS:", _rifiuti(c))
    occupied = [t for t in c.rc.table_manager.snapshot() if t.get("current_event_key")]
    print("ORDERS:", len(c.broker.state.orders), "OCCUPIED TABLES:", [(t["table_id"], t["current_event_key"]) for t in occupied])
    assert len(c.broker.state.orders) == 1, "second order placed for the same event while the first position is live"


def test_f1_retained_key_after_reset_cycle_expires_by_ttl_and_a_second_order_is_placed(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    c.rc._on_signal_received(_signal())
    assert len(c.broker.state.orders) == 1, _rifiuti(c)
    key = c.rc.duplication_guard.build_event_key(_signal())
    # retained as uncertain (AMBIGUOUS) -> reset keeps it
    c.rc._uncertain_reservations_map()[key] = 1
    c.rc.reset_cycle()
    assert c.rc.duplication_guard.is_duplicate(key), "key not retained by reset"
    assert c.rc.table_manager.find_by_event_key(key) is not None, "table not retained by reset"
    _advance_monotonic(monkeypatch, c.rc.duplication_guard.ttl + 1)
    assert not c.rc.duplication_guard.is_duplicate(key), "(expected on HEAD) retained key expired by TTL"
    c.rc._on_signal_received(_signal())
    print("REJECTS:", _rifiuti(c))
    occupied = [t for t in c.rc.table_manager.snapshot() if t.get("current_event_key")]
    print("ORDERS:", len(c.broker.state.orders), "OCCUPIED:", [(t["table_id"], t["current_event_key"]) for t in occupied])
    assert len(c.broker.state.orders) == 1, "second order for the same event while the retained exposure is unresolved"


# ------------------------------------------------------------------ F2: baseline committed, recovery/marker write fails
def test_f2_baseline_new_but_recovery_state_old_after_crash(tmp_path):
    from test_f032_rsr12_units import _lose, _rc
    rc = _rc(tmp_path / "w.db", funds={"available": 100.0, "exposure": 0.0})
    rc.simulation_mode = False
    rc._daily_loss_state_lock = threading.Lock()
    rc._daily_loss_monitor_state = {"realized_pnl_day_baseline": 0.0, "realized_pnl": 0.0}
    rc._session_pnl_baseline = None
    rc.get_status = lambda: {}
    rc._start_expansion_cycle("first_start", funds={"available": 100.0, "exposure": 0.0}, set_at_ts=1000.0)
    _lose(rc, 1, "A", 3.0)                      # loss persisted in DB on table 1
    assert rc._active_recovery_count() == 1
    real_save = rc.db.save_settings

    def failing(payload):
        if rc._RECOVERY_KEY in payload:
            raise OSError("disk full")
        return real_save(payload)
    rc.db.save_settings = failing
    rc.betfair_service._funds = {"available": 130.0, "exposure": 0.0} if hasattr(rc.betfair_service, "_funds") else None
    out = rc.reset_cycle()
    assert out["persisted"] is False
    print("in-memory after reset: baseline", rc._expansion_baseline, "recovery_state", rc.table_manager.recovery_state(),
          "persist_failed", rc._recovery_persist_failed)
    # crash + restart
    again = _rc(tmp_path / "w.db")
    again._load_expansion_baseline()
    again._restore_recovery_tables()
    settings = again.db.get_settings()
    print("DB keys:", {k: settings.get(k) for k in (again._EXPANSION_KEY, again._RECOVERY_KEY, again._CYCLE_RESET_KEY)})
    print("restart: baseline", again._expansion_baseline, "recovery_state", again.table_manager.recovery_state(),
          "persist_failed", getattr(again, "_recovery_persist_failed", None), "unreadable", again._recovery_state_unreadable)
    coherent = (again._expansion_baseline is None) or (again.table_manager.recovery_state() == rc.table_manager.recovery_state())
    assert coherent, "new-cycle baseline accepted while old-cycle recovery losses were restored"


# ------------------------------------------------------------------ F3: ROLLED_BACK counted as pending
def test_f3_rolled_back_result_is_reported_as_pending():
    from test_microstake_engine_native import _engine
    from core.microstake_sim_port import DbSagaStore
    engine, broker, db = _engine()
    store = DbSagaStore(db)
    store.save("rb", phase="ROLLBACK_PENDING", request={"market_id": "1.100", "selection_id": 7, "side": "BACK"})
    assert store.pending_refs() == ["rb"]

    class _Exec:
        def recover_pending(self):
            store.save("rb", phase="ROLLED_BACK", reservation_held=False)   # as the real coordinator does
            return {"rb": "ROLLED_BACK"}
    engine.native_microstake_executor = lambda: _Exec()
    out = engine.recover_native_microstake_sagas()
    print("OUT:", out, "pending_refs after:", store.pending_refs())
    assert store.pending_refs() == []
    assert out["ok"] is True, "terminal ROLLED_BACK reported as pending -> PAUSED MICROSTAKE_SAGA_PENDING"
