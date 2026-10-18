"""Throwaway triage tests for Codex findings 1,3,4,8,9,10 on PR #512 head 034af59. Not for the repo."""
import json, subprocess, sys
from decimal import Decimal as D
from types import SimpleNamespace
import pytest

sys.path.insert(0, "/home/user/Pickfair-nogui")
sys.path.insert(0, "/home/user/Pickfair-nogui/tests/f026")
from test_f018_rebuild_units import _rc as _rc18, _row, _Exchange
from test_f032_rsr12_units import _mm, _calc
from services.betfair_service import BetfairService
from controllers.dutching_controller import DutchingController
from core.system_state import DeskMode
from core.microstake_safe import (JsonSagaStore, MicrostakeCoordinator, MicrostakePolicy, MicrostakeRequest,
                                  Phase, Side, StatefulExchangeSimulator)

# ---- F1: exchange row without betId -> persisted SUBMITTED order treated as closed, ok True
def test_f1_row_without_betid_passes_wellformed_and_closes_live_position():
    rows = [{"marketId": "1.1"}]
    assert BetfairService._well_formed_current_orders(rows) == rows        # passes the shape gate
    rc = _rc18([_row("r1", "SUBMITTED", "1.1", 1, "K1", bet_id="B1")], _Exchange(rows))
    res = rc._rebuild_open_positions()
    print("F1 result:", res)
    assert res["ok"] is True and res["closed"] == ["r1"]                    # no table, no exposure, ACTIVE
    assert rc.table_manager.total_exposure() == 0.0

# ---- F3: malformed ladder -> None -> no shortfall in dutching
@pytest.mark.parametrize("ladder", [{"price": 2.0, "size": 50}, "abc", 7])
def test_f3_malformed_ladder_gives_no_dutching_shortfall(ladder):
    book = {"runners": [{"selectionId": 11, "ex": {"availableToBack": ladder}}]}
    item = {"selectionId": 11, "stake": 10.0, "side": "BACK", "price": 2.0}
    sf = DutchingController._leg_liquidity_shortfall(book, item, 1.0, 1.0)
    print("F3 shortfall for", type(ladder).__name__, "->", sf)
    assert sf is None

# ---- F4: crash after ROLLBACK_PENDING persisted, before cancel_all_remaining -> recover() activates
class _Crash(BaseException):
    pass

class _Exchange4(StatefulExchangeSimulator):
    def __init__(self):
        super().__init__(balance=D("100"))
        self.reject_replace_once = True
        self.crash_on_cancel_all = True
    def replace(self, bet_id, new_price):
        if self.reject_replace_once:
            self.reject_replace_once = False
            raise RuntimeError("REPLACE_REJECTED")
        return super().replace(bet_id, new_price)
    def cancel_all_remaining(self, bet_id):
        if self.crash_on_cancel_all:
            raise _Crash()
        return super().cancel_all_remaining(bet_id)

def _pol():
    return MicrostakePolicy(simulation_only=True, minimum_economic_stake=D("0.10"), exchange_stub_size=D("2.00"),
                            max_transient_exposure=D("10.00"), commission_rate=D("0.045"))

def test_f4_rollback_pending_is_resumed_as_active_on_restart(tmp_path):
    ex = _Exchange4()
    store = JsonSagaStore(tmp_path / "s.json")
    req = MicrostakeRequest(market_id="1.100", selection_id=7, side=Side.BACK, economic_stake=D("0.10"),
                            target_price=D("2.00"), customer_ref="rb-1")
    c = MicrostakeCoordinator(ex, store, _pol())
    with pytest.raises(_Crash):
        c.execute(req, stub_price=D("3.00"))
    assert store.load("rb-1")["phase"] == "ROLLBACK_PENDING"
    # restart: new coordinator, same store/exchange; the exchange now accepts replace
    ex.crash_on_cancel_all = False
    c2 = MicrostakeCoordinator(ex, JsonSagaStore(tmp_path / "s.json"), _pol())
    res = c2.recover("rb-1")
    print("F4 recover ->", res.phase, "ops:", ex.operation_log)
    assert res.phase is Phase.ACTIVE
    assert not any(op[0] == "CANCEL" and len(op) == 3 and op[2] == "0.10" for op in ex.operation_log)

# ---- F8: pf_trace TESTED with evidence [PASS, BLOCKED] validates
def test_f8_pass_then_blocked_still_validates():
    sys.path.insert(0, "/home/user/Pickfair-nogui/tools/traceability")
    import pf_trace
    repo = "/home/user/Pickfair-nogui"
    sha = subprocess.run(["git", "-C", repo, "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    ev = lambda r: {"sha": sha, "result": r, "command": "pytest", "timestamp": "t", "environment": "e"}
    req = {k: "x" for k in pf_trace.REQUIRED}
    req.update(id="R1", activity="ACTIVE", status="TESTED", tests=["tests/f026/test_f030_units.py"],
               evidence=[ev("PASS"), ev("BLOCKED")], findings=[], code=["x"])
    errors, warnings = pf_trace.validate({"requirements": [req]}, repo=repo)
    print("F8 errors:", errors)
    assert errors == []

# ---- F9: pfckpt tests_executed sha never resolved
def test_f9_fabricated_test_sha_not_rejected():
    sys.path.insert(0, "/home/user/Pickfair-nogui/tools/agent_checkpoint")
    import pfckpt
    state = {"schema": pfckpt.SCHEMA, "code": {"sha": "0" * 40},
             "tests_executed": [{"id": "t1", "status": "PASS", "sha": "deadbeefcafe"}],
             "certification_verdict": "PASS"}
    errs = pfckpt.validate_state(state)
    print("F9 errors:", errs)
    assert not any("t1" in e for e in errs)

# ---- F10: threshold 0 + unknown growth -> EXPANSION
def test_f10_unknown_growth_with_zero_threshold_expands():
    mm = _mm(expansion_profit_pct=0.0)
    d = _calc(mm, expansion_growth_snapshot=None)
    d5 = _calc(_mm(expansion_profit_pct=5.0), expansion_growth_snapshot=None)
    print("F10 desk_mode thr=0:", d.desk_mode, "stake", d.recommended_stake, "| thr=5:", d5.desk_mode, d5.recommended_stake)
    assert d.desk_mode is DeskMode.EXPANSION and d5.desk_mode is DeskMode.NORMAL
    assert d.recommended_stake > d5.recommended_stake
