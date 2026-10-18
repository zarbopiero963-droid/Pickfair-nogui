"""Throwaway triage for findings 5 and 6 (runtime_controller recovery state). Not for the repo."""
import glob, json, os, sys, tempfile, threading
import pytest
sys.path.insert(0, "/home/user/Pickfair-nogui")
sys.path.insert(0, "/home/user/Pickfair-nogui/tests/f026")
from test_f032_rsr12_units import _rc, _lose
from test_f026_financial import run
from database import Database

# ---- F5: reset_cycle clears losses in memory; persist fails silently; restart restores old losses
def test_f5_reset_cycle_persist_failure_is_silent_and_old_losses_return(tmp_path):
    rc = _rc(tmp_path / "w.db", funds={"available": 100.0, "exposure": 0.0})
    rc.simulation_mode = False
    rc._daily_loss_state_lock = threading.Lock()
    rc._daily_loss_monitor_state = {"realized_pnl_day_baseline": 0.0, "realized_pnl": 0.0}
    rc._session_pnl_baseline = None
    rc.get_status = lambda: {}
    rc._start_expansion_cycle("first_start", funds={"available": 100.0, "exposure": 0.0})
    _lose(rc, 1, "A", 3.0)                                   # persisted loss 3.0 on table 1
    assert rc._active_recovery_count() == 1
    real_save = rc.db.save_settings
    def failing(payload):
        if "roserpina.recovery_tables_state" in payload:
            raise OSError("disk full")
        return real_save(payload)
    rc.db.save_settings = failing
    out = rc.reset_cycle()
    print("F5 reset ->", out["reset"], "in-memory count:", rc._active_recovery_count(),
          "persist_failed flag:", getattr(rc, "_recovery_persist_failed", None),
          "events:", [e for e, _ in rc.bus.events])
    assert rc._active_recovery_count() == 0                  # memory: forgiven
    assert getattr(rc, "_recovery_persist_failed", False) is False   # nothing fail-closed
    again = _rc(tmp_path / "w.db")
    again._restore_recovery_tables()
    t = {x["table_id"]: x for x in again.table_manager.snapshot()}
    print("F5 after restart table1 loss:", t[1]["loss_amount"], "count:", again._active_recovery_count())
    assert t[1]["loss_amount"] == pytest.approx(3.0)         # old (forgiven) loss is back

# ---- F6: corrupted recovery state at restart -> ACTIVE, normal bet allocated on a table, debt gone
def test_f6_unreadable_recovery_state_does_not_block_admission():
    home = tempfile.mkdtemp(prefix="f6_")
    first = run([{"op": "signal", "m": "1.601", "price": 2.0}, {"op": "settle", "m": "1.601", "win": False}], home=home)
    s = first["snaps"][-1]
    print("F6 run1 tables:", s["tables"])
    assert any(float(t["loss"] or 0) > 0 for t in s["tables"]), "precondition: a recovery table with debt"
    dbs = glob.glob(os.path.join(home, "**", "*.db"), recursive=True) + glob.glob(os.path.join(home, "**", "*.sqlite*"), recursive=True)
    print("F6 dbs:", dbs)
    db = Database(dbs[0])
    print("F6 persisted state before corruption:", db.get_settings().get("roserpina.recovery_tables_state"))
    db.save_settings({"roserpina.recovery_tables_state": "{rotto"})
    second = run([{"op": "signal", "m": "1.602", "price": 2.0}], home=home)
    s2 = second["snaps"][-1]
    print("F6 run2 mode_after_start:", second["mode_after_start"], "rebuild:", second["rebuild"])
    print("F6 run2 tables:", s2["tables"])
    print("F6 run2 orders:", [o for o in s2["orders"] if o["market_id"] == "1.602"])
    print("F6 run2 rejections:", [e for e in second["events"] if e["e"].startswith("SIGNAL_REJ")])
    print("F6 persisted state after run2:", Database(dbs[0]).get_settings().get("roserpina.recovery_tables_state"))
    assert second["mode_after_start"] == "ACTIVE"
    assert any(o["market_id"] == "1.602" and o["status"] == "SUBMITTED" for o in s2["orders"])
