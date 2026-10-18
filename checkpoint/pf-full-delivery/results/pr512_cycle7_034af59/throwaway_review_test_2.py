import json
import math
import time

import pytest

from tests.f026.test_f032_rsr12_units import _rc as _rc32
from tests.test_pr512_codex_f62f0e3 import _rc_reset


# ---- (d) would the new tests fail if the ordering fix were reverted? Simulate the a8dbbd2 ordering:
# baseline at time.time() (ignores set_at_ts), marker at a later time.time() (ignores reset_ts).
def _revert_ordering(rc):
    orig_start = type(rc)._start_expansion_cycle
    orig_persist = type(rc)._persist_recovery_tables
    rc._start_expansion_cycle = lambda reason, funds=None, **kw: orig_start(rc, reason, funds)
    rc._persist_recovery_tables = lambda **kw: (time.sleep(0.002),
                                                orig_persist(rc, cycle_reset=kw.get("cycle_reset", False)))[1]


def test_reverted_ordering_makes_reset_cycle_test_fail(tmp_path):
    rc = _rc_reset(tmp_path, with_position=False)
    _revert_ordering(rc)
    assert rc.reset_cycle()["reset"] is True
    again = _rc32(tmp_path / "r.db")
    again._load_expansion_baseline()
    assert again._expansion_baseline is None          # i.e. the repo test WOULD fail on the reverted code


def test_reverted_ordering_makes_auto_reset_test_fail(tmp_path):
    rc = _rc_reset(tmp_path, with_position=False)
    rc.simulation_mode = False
    _revert_ordering(rc)
    rc._record_account_snapshot({"available": 100.0, "exposure": 0.0}, source="t")
    rc.config.auto_reset_drawdown_pct = 10.0
    rc._record_account_snapshot({"available": 80.0, "exposure": 0.0}, source="t")
    assert rc._maybe_auto_reset(20.0) is True
    again = _rc32(tmp_path / "r.db")
    again._load_expansion_baseline()
    assert again._expansion_baseline is None


def test_corrupt_marker_test_discriminates_none_return(tmp_path):
    rc = _rc32(tmp_path / "d.db")
    assert rc._start_expansion_cycle("first_start", funds={"available": 50.0, "exposure": 0.0}) is True
    rc.db.save_settings({rc._CYCLE_RESET_KEY: "not json"})
    again = _rc32(tmp_path / "d.db")
    again._last_durable_cycle_reset_ts = lambda: None   # the fail-open variant
    again._load_expansion_baseline()
    assert again._expansion_baseline == pytest.approx(50.0)   # repo test asserts None => it would fail


# ---- (b) more corrupt marker shapes
@pytest.mark.parametrize("marker", ['[]', '5', 'true', '{"at_ts": true}', '{"at_ts": false}', '{}',
                                    '{"at_ts": "inf"}', '{"at_ts": "-inf"}', '{"at_ts": [1]}', '"x"'])
def test_more_corrupt_marker_shapes(tmp_path, marker):
    rc = _rc32(tmp_path / "e.db")
    assert rc._start_expansion_cycle("first_start", funds={"available": 50.0, "exposure": 0.0}) is True
    rc.db.save_settings({rc._CYCLE_RESET_KEY: marker})
    again = _rc32(tmp_path / "e.db")
    assert again._last_durable_cycle_reset_ts() == float("inf")
    assert again._load_expansion_baseline() is True
    assert again._expansion_baseline is None


def test_unreadable_settings_marker(tmp_path):
    rc = _rc32(tmp_path / "f.db")
    rc.db.get_settings = lambda: (_ for _ in ()).throw(RuntimeError("db"))
    assert rc._last_durable_cycle_reset_ts() == float("inf")


def test_normal_float_marker_and_legacy_baseline(tmp_path):
    rc = _rc32(tmp_path / "g.db")
    rc.db.save_settings({rc._CYCLE_RESET_KEY: json.dumps({"at_ts": 1234.5})})
    assert rc._last_durable_cycle_reset_ts() == 1234.5
    # legacy baseline (no set_at_ts) + NO marker => accepted
    rc2 = _rc32(tmp_path / "h.db")
    rc2.db.save_settings({rc2._EXPANSION_KEY: json.dumps({"baseline": 70.0, "reason": "x"})})
    assert rc2._load_expansion_baseline() is True
    assert rc2._expansion_baseline == pytest.approx(70.0)
    # legacy baseline + marker present => rejected (0.0 < marker)
    rc2.db.save_settings({rc2._CYCLE_RESET_KEY: json.dumps({"at_ts": 1.0})})
    rc3 = _rc32(tmp_path / "h.db")
    rc3._load_expansion_baseline()
    assert rc3._expansion_baseline is None


# ---- suspected gap: baseline-side set_at_ts corruption (NaN / inf / bool) bypasses the freshness check
@pytest.mark.parametrize("bad_ts", ["NaN", "inf", "Infinity", float("nan"), float("inf")])
def test_baseline_set_at_ts_nan_or_inf_bypasses_durable_reset(tmp_path, bad_ts):
    rc = _rc32(tmp_path / "i.db")
    # a durable reset happened at T=5000 whose baseline write failed; the stale baseline on disk has a corrupt ts
    rc.db.save_settings({rc._EXPANSION_KEY: json.dumps({"baseline": 50.0, "reason": "first_start", "set_at_ts": bad_ts}),
                         rc._CYCLE_RESET_KEY: json.dumps({"at_ts": 5000.0})})
    again = _rc32(tmp_path / "i.db")
    again._load_expansion_baseline()
    # fail-closed expectation: None. Record what really happens.
    print("RESULT", bad_ts, again._expansion_baseline)
    assert again._expansion_baseline is None


# ---- (c) baseline write OK, marker (transaction) fails
def test_baseline_ok_marker_write_fails(tmp_path):
    rc = _rc_reset(tmp_path, with_position=False)
    real_save = rc.db.save_settings

    def failing_for_marker(data):
        if rc._CYCLE_RESET_KEY in data:
            raise RuntimeError("disco pieno")
        return real_save(data)

    rc.db.save_settings = failing_for_marker
    out = rc.reset_cycle()
    assert out["reset"] is True                                        # reset reported OK even if marker failed
    assert getattr(rc, "_recovery_persist_failed", False) is False     # flag NOT raised on this path
    again = _rc32(tmp_path / "r.db")
    again._load_expansion_baseline()
    assert again._expansion_baseline == pytest.approx(100.0)          # fresh baseline accepted (no marker)
    assert again._last_durable_cycle_reset_ts() is None
