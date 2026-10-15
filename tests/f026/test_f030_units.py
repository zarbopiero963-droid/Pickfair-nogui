"""PF-ALL-F030 — liquidita' eseguibile (funzioni pure) e RiskGate con dato richiesto e assente."""
from types import SimpleNamespace

import pytest

from core import liquidity
from core.risk_gate import RiskGate

BOOK = {"status": "OPEN", "runners": [{"selectionId": 11, "status": "ACTIVE", "ex": {
    "availableToBack": [{"price": 2.0, "size": 30.0}, {"price": 1.98, "size": 70.0}],
    "availableToLay": [{"price": 2.02, "size": 40.0}, {"price": 2.04, "size": 60.0}]}}]}


@pytest.mark.parametrize("side,price,expected", [
    ("BACK", 2.0, 30.0),      # solo i livelli a quota >= 2.0
    ("BACK", 1.98, 100.0),
    ("BACK", 2.1, 0.0),       # quota migliorata prima dell'invio: nulla di eseguibile
    ("LAY", 2.02, 40.0),      # solo i livelli a quota <= 2.02
    ("LAY", 2.04, 100.0),
    ("LAY", 2.0, 0.0),        # spread: nessuna contropartita alla quota
])
def test_executable_liquidity_by_side_and_price(side, price, expected):
    assert liquidity.executable_liquidity(BOOK, 11, side, price) == pytest.approx(expected)


@pytest.mark.parametrize("book,reason", [
    (None, "LIQUIDITY_BOOK_UNAVAILABLE"), ({"runners": "x"}, "LIQUIDITY_BOOK_UNAVAILABLE"),
    (dict(BOOK, status="SUSPENDED"), "LIQUIDITY_MARKET_NOT_OPEN"),
    ({"status": "OPEN", "runners": [{"selectionId": 11, "status": "REMOVED", "ex": {}}]}, "LIQUIDITY_RUNNER_NOT_ACTIVE"),
    ({"status": "OPEN", "runners": [{"selectionId": 11, "status": "ACTIVE", "ex": {}}]}, "LIQUIDITY_LADDER_UNAVAILABLE"),
    ({"status": "OPEN", "runners": [{"selectionId": 99, "status": "ACTIVE", "ex": {"availableToBack": []}}]},
     "LIQUIDITY_LADDER_UNAVAILABLE"),
])
def test_single_entry_unknown_liquidity_is_never_zero(book, reason):
    assert liquidity.single_entry_liquidity(book, 11, "BACK", 2.0) == (None, reason)


def _gate(warning_only=False, enabled=True):
    cfg = SimpleNamespace(MIN_STAKE=0.1, MIN_PRICE=1.01, MAX_WIN=10000.0, BOOK_BLOCK=110.0, LIQUIDITY_MULTIPLIER=3.0,
                          MIN_LIQUIDITY_ABSOLUTE=50.0, LIQUIDITY_GUARD_ENABLED=enabled,
                          LIQUIDITY_WARNING_ONLY=warning_only)
    return RiskGate(cfg)


def _p(**k):
    return dict({"stake": 3.0, "price": 2.0, "bet_type": "BACK"}, **k)


@pytest.mark.parametrize("warning_only", [False, True])
def test_required_but_missing_liquidity_is_denied(warning_only):
    res = _gate(warning_only).check(_p(liquidity_required=True))
    assert res["allowed"] is False and res["reason"] == "RISK_LIQUIDITY_UNKNOWN"


def test_dutching_without_marker_is_unchanged():
    assert _gate().check(_p())["allowed"] is True


def test_guard_disabled_ignores_marker():
    assert _gate(enabled=False).check(_p(liquidity_required=True))["allowed"] is True


@pytest.mark.parametrize("liq,warning_only,allowed", [(49.0, False, False), (50.0, False, True),
                                                      (10.0, True, True)])
def test_threshold_and_warning_only(liq, warning_only, allowed):
    assert _gate(warning_only).check(_p(liquidity_required=True, available_liquidity=liq))["allowed"] is allowed


# ---------------------------------------------------------------- ingresso del ciclo (auto-next): F030 e F027
from core.system_state import RuntimeMode  # noqa: E402
from tests.integration.test_runtime_controller_auto_trade_mm import _canonical_close_payload, _make_controller  # noqa: E402


def _close(rc):
    rc._on_close_position(_canonical_close_payload(
        event_key="evt-f030", batch_id="b-f030", correlation_id="c-f030", auto_trade_enabled=True,
        cycle_executor_enabled=True,
        mm_context={"cycle_active": True, "cycle_id": "cy-f030",
                    "table": {"table_id": 1, "loss_amount": 0.0, "in_recovery": False},
                    "next_signal": {"market_id": "1.234", "selection_id": 11, "price": 2.0}}))


def _cmd(bus):
    return [p for n, p in bus.events if n == "CMD_QUICK_BET"]


def test_auto_next_entry_carries_executable_liquidity():
    rc, bus = _make_controller(responses=[{"available": 150.0}])
    rc.betfair_service.get_market_book_snapshot = lambda market_id, include_prices=False: BOOK
    rc.mode = RuntimeMode.ACTIVE
    rc.risk_desk.sync_bankroll(100.0)
    _close(rc)
    (cmd,) = _cmd(bus)
    assert cmd["liquidity_required"] is True and cmd["available_liquidity"] == pytest.approx(30.0)


def test_auto_next_entry_without_book_is_marked_unknown_for_the_gate():
    rc, bus = _make_controller(responses=[{"available": 150.0}])
    rc.betfair_service.get_market_book_snapshot = lambda market_id, include_prices=False: None
    rc.mode = RuntimeMode.ACTIVE
    rc.risk_desk.sync_bankroll(100.0)
    _close(rc)
    (cmd,) = _cmd(bus)
    assert cmd["liquidity_required"] is True and "available_liquidity" not in cmd
    assert _gate().check(dict(cmd, stake=3.0))["reason"] == "RISK_LIQUIDITY_UNKNOWN"


@pytest.mark.parametrize("mode", [RuntimeMode.PAUSED, RuntimeMode.LOCKDOWN])
def test_auto_next_never_submits_outside_active_F027(mode):
    rc, bus = _make_controller(responses=[{"available": 150.0}])
    rc.mode = mode
    rc.risk_desk.sync_bankroll(100.0)
    _close(rc)
    assert _cmd(bus) == [], "la riconciliazione ha prodotto un nuovo ordine fuori da ACTIVE"
    (res,) = [p for n, p in bus.events if n == "AUTO_TRADE_MM_RESULT"]
    assert res["reason"].startswith("runtime_not_active")


def test_auto_next_pre_submit_recheck_blocks_mode_change_after_first_gate_F027():
    """Il modo cambia (LOCKDOWN) dopo il gate d'ingresso dell'auto-next: il recheck pre-invio blocca."""
    rc, bus = _make_controller(responses=[{"available": 150.0}])
    rc.mode = RuntimeMode.ACTIVE
    rc.risk_desk.sync_bankroll(100.0)
    real = rc._risk_allows_auto_trade

    def gate_then_lockdown():
        out = real()
        rc.mode = RuntimeMode.LOCKDOWN          # LOCKDOWN concorrente fra gate e invio
        return out
    rc._risk_allows_auto_trade = gate_then_lockdown
    _close(rc)
    assert _cmd(bus) == []
    (res,) = [p for n, p in bus.events if n == "AUTO_TRADE_MM_RESULT"]
    assert res["reason"] == "runtime_not_active:LOCKDOWN"
