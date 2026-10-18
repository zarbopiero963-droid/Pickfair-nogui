"""Throwaway review tests for PR #512 cycle 7 (a8dbbd2). NOT part of the repo."""
import json
import threading

import pytest

from tests.f026.test_f032_rsr12_units import _rc as _rc32


def _rc_reset(tmp_path):
    rc = _rc32(tmp_path / "r.db")
    rc.simulation_mode = False
    rc._daily_loss_state_lock = threading.Lock()
    rc._daily_loss_monitor_state = {"realized_pnl_day_baseline": 0.0, "realized_pnl": 0.0}
    rc._session_pnl_baseline = None
    rc.get_status = lambda: {}
    return rc


def test_real_reset_cycle_then_restart_keeps_the_fresh_baseline(tmp_path):
    """Real sequence of reset_cycle(): _start_expansion_cycle THEN _persist_recovery_tables(cycle_reset=True).
    The baseline written by this very reset must survive a restart."""
    rc = _rc_reset(tmp_path)
    assert rc.reset_cycle()["reset"] is True
    s = rc.db.get_settings()
    b = json.loads(s[rc._EXPANSION_KEY]); m = json.loads(s[rc._CYCLE_RESET_KEY])
    print("set_at_ts", b["set_at_ts"], "at_ts", m["at_ts"], "delta", m["at_ts"] - b["set_at_ts"])
    assert b["baseline"] == pytest.approx(100.0)
    again = _rc32(tmp_path / "r.db")
    assert again._load_expansion_baseline() is True
    assert again._expansion_baseline == pytest.approx(100.0), "baseline fissata dal reset stesso scartata al riavvio"


def test_auto_reset_sequence_then_restart_keeps_the_fresh_baseline(tmp_path):
    rc = _rc_reset(tmp_path)
    rc._record_account_snapshot({"available": 100.0, "exposure": 0.0}, source="t")
    rc.config.auto_reset_drawdown_pct = 10.0
    rc._record_account_snapshot({"available": 80.0, "exposure": 0.0}, source="t")
    assert rc._maybe_auto_reset(20.0) is True
    again = _rc32(tmp_path / "r.db")
    again._load_expansion_baseline()
    assert again._expansion_baseline == pytest.approx(80.0)


def test_corrupt_marker_is_ignored_fail_open(tmp_path):
    rc = _rc32(tmp_path / "x.db")
    rc.db.save_settings({rc._EXPANSION_KEY: json.dumps({"baseline": 50.0, "set_at_ts": 1.0}),
                         rc._CYCLE_RESET_KEY: json.dumps({"at_ts": "garbage"})})
    rc._load_expansion_baseline()
    print("baseline with corrupt marker:", rc._expansion_baseline)
