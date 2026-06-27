"""Unit test per stake_mode.resolve_stake (Fase 2.3 / B7.1)."""
import pytest

from stake_mode import MODE_FIXED, MODE_MASTER, MODE_MM, resolve_stake

pytestmark = pytest.mark.unit


def _r(**kw):
    return resolve_stake(**kw)


# =========================================================
# MM (default / comportamento odierno)
# =========================================================
def test_mm_mode_uses_mm_stake():
    r = _r(mode="MM", mm_stake=12.5, master_stake=99.0, fixed_stake=5.0)
    assert r["stake"] == 12.5
    assert r["mode"] == MODE_MM
    assert r["reason"] == "ok"


def test_unknown_mode_falls_back_to_mm():
    r = _r(mode="WAT", mm_stake=8.0, master_stake=50.0)
    assert r["stake"] == 8.0
    assert r["mode"] == MODE_MM
    assert r["reason"] == "invalid_mode"


def test_empty_mode_falls_back_to_mm():
    assert _r(mode="", mm_stake=3.0)["mode"] == MODE_MM
    assert _r(mode=None, mm_stake=3.0)["mode"] == MODE_MM


# =========================================================
# MASTER
# =========================================================
def test_master_mode_uses_master_stake():
    r = _r(mode="master", master_stake=20.0, mm_stake=10.0)
    assert r["stake"] == 20.0
    assert r["mode"] == MODE_MASTER
    assert r["reason"] == "ok"


def test_master_invalid_falls_back_to_mm():
    for bad in (None, 0.0, -5.0, float("nan"), float("inf"), "x", True):
        r = _r(mode="MASTER", master_stake=bad, mm_stake=7.0)
        assert r["stake"] == 7.0
        assert r["mode"] == MODE_MM
        assert r["reason"] == "invalid_master_stake"


def test_master_capped_to_follower_limit():
    r = _r(mode="MASTER", master_stake=100.0, mm_stake=10.0, cap=25.0)
    assert r["stake"] == 25.0
    assert r["mode"] == MODE_MASTER
    assert r["reason"] == "capped"


def test_master_under_cap_not_clamped():
    r = _r(mode="MASTER", master_stake=15.0, mm_stake=10.0, cap=25.0)
    assert r["stake"] == 15.0
    assert r["reason"] == "ok"


def test_invalid_cap_ignored():
    # cap non valido (0/neg/NaN) => nessun clamp.
    for bad_cap in (0.0, -1.0, float("nan"), None, "x"):
        r = _r(mode="MASTER", master_stake=30.0, mm_stake=10.0, cap=bad_cap)
        assert r["stake"] == 30.0


# =========================================================
# FIXED
# =========================================================
def test_fixed_mode_uses_fixed_stake():
    r = _r(mode="FIXED", fixed_stake=4.0, mm_stake=10.0, master_stake=99.0)
    assert r["stake"] == 4.0
    assert r["mode"] == MODE_FIXED


def test_fixed_invalid_falls_back_to_mm():
    r = _r(mode="FIXED", fixed_stake=0.0, mm_stake=6.0)
    assert r["stake"] == 6.0
    assert r["mode"] == MODE_MM
    assert r["reason"] == "invalid_fixed_stake"


def test_fixed_capped():
    r = _r(mode="FIXED", fixed_stake=50.0, mm_stake=10.0, cap=20.0)
    assert r["stake"] == 20.0
    assert r["reason"] == "capped"


# =========================================================
# Fail-closed estremo: anche MM non valido => nessun bet
# =========================================================
def test_mm_invalid_returns_zero():
    for bad in (None, 0.0, -1.0, float("nan"), "x", True):
        r = _r(mode="MM", mm_stake=bad)
        assert r["stake"] == 0.0
        assert r["mode"] == MODE_MM
        assert r["reason"] == "invalid_mm_stake"


def test_master_invalid_and_mm_invalid_returns_zero():
    # MASTER scelto ma master invalido E mm invalido => 0.0 (nessun bet).
    r = _r(mode="MASTER", master_stake=None, mm_stake=None)
    assert r["stake"] == 0.0
    assert r["mode"] == MODE_MM
    assert r["reason"] == "invalid_mm_stake"


def test_requested_mode_is_reported():
    assert _r(mode="master", master_stake=1.0)["requested_mode"] == "MASTER"
    assert _r(mode="WAT", mm_stake=1.0)["requested_mode"] == "WAT"


def test_numeric_string_stakes_accepted():
    r = _r(mode="MASTER", master_stake="22.5", mm_stake="10")
    assert r["stake"] == 22.5
