"""BAL-xx: the REAL SimulationBroker against the independent oracle (``oracle.py``).

Every expected number comes from ``oracle.py`` (Decimal, project-independent),
never from the broker. Registry IDs are in each test name/docstring.
SIM floats are compared to the Decimal oracle with an absolute tolerance of
1e-6 (the SIM does not round commission to cents; recorded, not asserted).
"""
from __future__ import annotations

import copy
import importlib.util
import json
import pathlib
import sys
from decimal import Decimal

import pytest

from simulation_broker import SimulationBroker

_spec = importlib.util.spec_from_file_location(
    "bal_oracle_model", pathlib.Path(__file__).with_name("oracle.py"))
O = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = O
_spec.loader.exec_module(O)

START = 1000
TOL = 1e-6


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------
def _open(market, prices, winners=1):
    """prices: {sel: (best availableToBack, best availableToLay, size)}."""
    book = {
        "marketId": market,
        "status": "OPEN",
        "runners": [
            {"selectionId": s, "status": "ACTIVE",
             "ex": {"availableToBack": [{"price": bk, "size": sz}],
                    "availableToLay": [{"price": ly, "size": sz}]}}
            for s, (bk, ly, sz) in prices.items()
        ],
    }
    if winners is not None:
        book["numberOfWinners"] = winners
    return book


def _closed(market, statuses, winners=1):
    book = {"marketId": market, "status": "CLOSED",
            "runners": [{"selectionId": s, "status": st} for s, st in statuses.items()]}
    if winners is not None:
        book["numberOfWinners"] = winners
    return book


def _near(sim_value, oracle_value):
    return abs(float(sim_value) - float(oracle_value)) <= TOL


def _assert_funds(broker, acct, label=""):
    f = broker.get_account_funds()
    exp = acct.snapshot()
    assert "error" not in f, f
    assert _near(f["available"], exp["available"]), (label, "available", f, exp)
    assert _near(f["exposure"], exp["committed"]), (label, "committed", f, exp)
    assert _near(f["total"], exp["total"]), (label, "total", f, exp)
    # I1 on the broker itself: available + committed == total
    assert _near(f["available"] + f["exposure"], f["total"]), (label, "I1", f)
    # realized (settled) PnL never includes potential / unsettled profit
    assert _near(broker.state.realized_pnl, exp["realized"]), (label, "realized", broker.state.realized_pnl, exp)
    return f


def _place(broker, acct, bet_key, market, sel, side, price, size, expect_matched=None):
    r = broker.place_bet(market_id=market, selection_id=sel, side=side, price=price, size=size)
    rep = r["instructionReports"][0]
    assert r["status"] == "SUCCESS", r
    matched = rep["sizeMatched"]
    if expect_matched is not None:
        assert _near(matched, O.D(expect_matched)), (rep, expect_matched)
    acct.place(bet_key, market, sel, side, price, size,
               matched=O.D(expect_matched if expect_matched is not None else size) if matched else 0,
               matched_price=rep["averagePriceMatched"] if matched else None)
    return rep["betId"]


def _new(market, prices, winners=1):
    broker = SimulationBroker(starting_balance=float(START), commission_pct=4.5)
    broker.update_market_book(_open(market, prices, winners))
    acct = O.Account(O.D(START))
    acct.declare_market(market, prices.keys())
    return broker, acct


def _check_settlement(broker, acct, market):
    rec = [r for r in broker.list_settlements() if r["market_id"] == market]
    assert len(rec) == 1, rec
    exp = acct.settlements[market]
    assert _near(rec[0]["gross_pnl"], exp["gross"]), (rec[0], exp)
    assert _near(rec[0]["commission_amount"], exp["commission"]), (rec[0], exp)
    assert _near(rec[0]["net_pnl"], exp["net"]), (rec[0], exp)
    return rec[0]


STAKES = [("2", "1.5"), ("10", "2.0"), ("7.33", "3.45")]


# --------------------------------------------------------------------------
# Oracle anchors (hand-computed constants; the oracle must reproduce them)
# --------------------------------------------------------------------------
def test_oracle_anchor_hand_computed_values():
    """Anchors the oracle on hand arithmetic (no project code)."""
    back = O.Bet.of("BACK", "10", "2.0", 1)
    assert O.settled([back], [1]) == {"gross": Decimal("10.00"), "commission": Decimal("0.4500"),
                                      "net": Decimal("9.5500")}
    assert O.settled([back], [2])["net"] == Decimal("-10")
    lay = O.Bet.of("LAY", "7.33", "3.45", 1)
    assert O.settled([lay], [1])["gross"] == Decimal("-17.9585")
    assert lay.committed() == Decimal("17.9585")
    # market net, not per bet: +20 and -10 => commission on +10
    bets = [O.Bet.of("BACK", "10", "3", 1), O.Bet.of("BACK", "10", "2", 2)]
    assert O.settled(bets, [1])["commission"] == Decimal("0.450")
    assert O.per_bet_commission_wrong(bets, [1]) == Decimal("0.900")
    # green-up BACK 10@3 + LAY 15@2: +5 whatever happens
    g = [O.Bet.of("BACK", "10", "3", 1), O.Bet.of("LAY", "15", "2", 1)]
    assert set(O.outcomes_pnl(g).values()) == {Decimal("5")}
    assert O.guaranteed_min(g) == Decimal("5")


def test_oracle_invariants_I1_I2_with_early_availability():
    """I1/I2 in the oracle, incl. the owner's EARLY AVAILABILITY bucket."""
    a = O.Account(O.D(1000))
    a.declare_market("m", [1, 2])
    a.place("b", "m", 1, "BACK", "3", "10", matched="10", matched_price="3")
    a.place("l", "m", 1, "LAY", "2", "15", matched="15", matched_price="2")
    assert a.total == 1000 and a.committed == 0 and a.realized == 0
    with pytest.raises(O.OracleError):
        a.early_credit("m", "5.01")  # never above the guaranteed minimum
    a.early_credit("m", "5")
    # spendable +5, but wealth unchanged thanks to the explicit counterpart
    assert a.available == Decimal("1005") and a.realized == 0 and a.early_total == 5
    assert a.early_available_unsettled == 5 and a.total == 1000
    assert a.settle("m", [1]) == "SETTLED"
    # credited exactly once: 1000 + net(5) = 1004.775, early bucket netted
    assert a.total == Decimal("1004.775") and a.early_total == 0
    assert a.credits["m"] == Decimal("4.775")
    assert a.settle("m", [1]) == "DUPLICATE" and a.total == Decimal("1004.775")
    # early stake release is a pure I1 move
    b = O.Account(O.D(1000))
    b.place("x", "n", 7, "BACK", "2", "10", matched="10", matched_price="2")
    assert b.available == 990
    assert b.early_release("n") == 10
    assert b.available == 1000 and b.total == 1000 and b.realized == 0
    b.settle("n", [7])
    assert b.total == Decimal("1009.55")


# --------------------------------------------------------------------------
# BAL-01 / BAL-02: single BACK / LAY, win and lose, no cashout
# --------------------------------------------------------------------------
@pytest.mark.parametrize("stake,price", STAKES)
@pytest.mark.parametrize("outcome", ["WINNER", "LOSER"])
def test_BAL01_back_settle_no_cashout(stake, price, outcome):
    """BAL-01 BACK, no cashout: commit at placement (I1), credit once at settlement."""
    broker, acct = _new("1.01", {11: (float(price), float(price) + 0.1, 1000.0), 22: (5.0, 5.1, 1000.0)})
    _place(broker, acct, "b", "1.01", 11, "BACK", float(price), float(stake), expect_matched=stake)
    f = _assert_funds(broker, acct, "after place")
    assert _near(f["exposure"], O.D(stake))
    assert _near(f["total"], START)  # I1: placing moves money, creates none
    other = "LOSER" if outcome == "WINNER" else "WINNER"
    esito = broker.update_market_book(_closed("1.01", {11: outcome, 22: other}))
    acct.settle("1.01", [11] if outcome == "WINNER" else [22])
    assert esito["settlement_status"] == "SETTLED"
    _check_settlement(broker, acct, "1.01")
    f = _assert_funds(broker, acct, "after settle")
    assert _near(f["exposure"], 0)


@pytest.mark.parametrize("stake,price", STAKES)
@pytest.mark.parametrize("outcome", ["WINNER", "LOSER"])
def test_BAL02_lay_settle(stake, price, outcome):
    """BAL-02 LAY: liability stake*(price-1) committed, released at settlement."""
    broker, acct = _new("1.02", {11: (float(price) - 0.05, float(price), 1000.0), 22: (5.0, 5.1, 1000.0)})
    _place(broker, acct, "l", "1.02", 11, "LAY", float(price), float(stake), expect_matched=stake)
    f = _assert_funds(broker, acct, "after place")
    assert _near(f["exposure"], O.Bet.of("LAY", stake, price, 11).committed())
    other = "LOSER" if outcome == "WINNER" else "WINNER"
    broker.update_market_book(_closed("1.02", {11: outcome, 22: other}))
    acct.settle("1.02", [11] if outcome == "WINNER" else [22])
    _check_settlement(broker, acct, "1.02")
    _assert_funds(broker, acct, "after settle")


# --------------------------------------------------------------------------
# BAL-03 full cashout, BAL-06 green-up multi-order, BAL-07 early min profit
# --------------------------------------------------------------------------
def _greenup(broker, acct, market, backs, lays):
    for i, (st, pr) in enumerate(backs):
        broker.update_market_book(_open(market, {11: (pr, pr + 0.1, 1000.0), 22: (4.0, 4.1, 1000.0)}))
        _place(broker, acct, f"b{i}", market, 11, "BACK", pr, st, expect_matched=str(st))
        _assert_funds(broker, acct, f"back {i}")
    for i, (st, pr) in enumerate(lays):
        broker.update_market_book(_open(market, {11: (pr - 0.02, pr, 1000.0), 22: (4.0, 4.1, 1000.0)}))
        _place(broker, acct, f"l{i}", market, 11, "LAY", pr, st, expect_matched=str(st))
        _assert_funds(broker, acct, f"lay {i}")


CASHOUTS = [
    # (backs, lays) -- full cashout = equal profit on every outcome
    ([(10.0, 3.0)], [(15.0, 2.0)]),               # +5 both outcomes
    ([(7.33, 3.45)], [(12.65, 2.0)]),             # unequal green
    ([(2.0, 1.5)], [(2.5, 1.2)]),                 # tiny stakes
]


@pytest.mark.parametrize("backs,lays", CASHOUTS)
@pytest.mark.parametrize("outcome", ["WINNER", "LOSER"])
def test_BAL03_full_cashout_available_and_settlement(backs, lays, outcome):
    """BAL-03 full cashout: before settlement available == oracle (no unsettled
    profit counted), realized 0; at settlement net credited once."""
    broker, acct = _new("1.03", {11: (3.0, 3.1, 1000.0), 22: (4.0, 4.1, 1000.0)})
    _greenup(broker, acct, "1.03", backs, lays)
    f = _assert_funds(broker, acct, "pre-settle")
    assert _near(f["total"], START)  # nothing settled yet: total wealth unchanged
    broker.update_market_book(_closed("1.03", {11: outcome, 22: "LOSER" if outcome == "WINNER" else "WINNER"}))
    acct.settle("1.03", [11] if outcome == "WINNER" else [22])
    _check_settlement(broker, acct, "1.03")
    _assert_funds(broker, acct, "post-settle")


@pytest.mark.xfail(strict=True, reason="D8 / BAL-03: SIM credits unsettled hedge profit to state.balance "
                                       "(simulation_broker.py _apply_fill_to_position_ledger realized_delta)")
def test_BAL03_D8_unsettled_hedge_profit_not_in_balance():
    """BAL-03 / D8: BACK 10@3 + LAY 10@2 (win +10 / lose 0) is NOT settled:
    the exposed ``balance`` (snapshot -> runtime simulation_snapshot) must not
    contain the conditional +10 nor any realized amount."""
    broker, acct = _new("1.38", {11: (3.0, 3.1, 1000.0)})
    _greenup(broker, acct, "1.38", [(10.0, 3.0)], [(10.0, 2.0)])
    snap = broker.snapshot()
    assert acct.realized == 0
    assert snap["realized_pnl"] == 0.0
    # balance must not exceed settled cash while nothing is settled
    assert float(snap["balance"]) <= float(acct.settled_cash) + TOL, snap["balance"]
    assert float(broker.state.balance) <= float(acct.settled_cash) + TOL


@pytest.mark.parametrize("outcome", ["WINNER", "LOSER"])
def test_BAL06_greenup_multi_order(outcome):
    """BAL-06 green-up with several partial orders on both sides (same market)."""
    broker, acct = _new("1.06", {11: (3.0, 3.1, 1000.0), 22: (4.0, 4.1, 1000.0)})
    _greenup(broker, acct, "1.06", [(6.0, 3.0), (4.0, 3.2)], [(7.0, 2.0), (8.6, 2.1)])
    pnl = acct.potential("1.06")
    assert O.guaranteed_min(acct._matched_bets("1.06"), [11, 22]) == min(pnl.values())
    _assert_funds(broker, acct, "pre-settle")
    broker.update_market_book(_closed("1.06", {11: outcome, 22: "LOSER" if outcome == "WINNER" else "WINNER"}))
    acct.settle("1.06", [11] if outcome == "WINNER" else [22])
    _check_settlement(broker, acct, "1.06")
    _assert_funds(broker, acct, "post-settle")


def test_BAL07_guaranteed_min_profit_not_available_before_settlement():
    """BAL-07: the SIM has no exchange-confirmed early-availability event, so the
    oracle's early bucket stays 0: the guaranteed minimum (+5) must be neither
    available nor realized before settlement, then credited exactly once."""
    broker, acct = _new("1.07", {11: (3.0, 3.1, 1000.0), 22: (4.0, 4.1, 1000.0)})
    _greenup(broker, acct, "1.07", [(10.0, 3.0)], [(15.0, 2.0)])
    assert O.guaranteed_min(acct._matched_bets("1.07"), [11, 22]) == Decimal("5")
    f = _assert_funds(broker, acct, "pre-settle")
    assert _near(f["available"], START)
    broker.update_market_book(_closed("1.07", {11: "WINNER", 22: "LOSER"}))
    acct.settle("1.07", [11])
    _assert_funds(broker, acct, "post-settle")
    assert _near(broker.get_account_funds()["available"], O.D("1004.775"))


# --------------------------------------------------------------------------
# BAL-10 void / removed runner
# --------------------------------------------------------------------------
@pytest.mark.parametrize("side", ["BACK", "LAY"])
def test_BAL10_void_removed_runner_only_bets(side):
    """BAL-10: bets only on the removed runner => void, stake/liability back, no commission."""
    broker, acct = _new("1.10", {11: (3.0, 3.0, 1000.0), 22: (2.5, 2.6, 1000.0)})
    _place(broker, acct, "v", "1.10", 11, side, 3.0, 10.0, expect_matched="10")
    _assert_funds(broker, acct, "pre")
    esito = broker.update_market_book(_closed("1.10", {11: "REMOVED", 22: "WINNER"}))
    acct.settle("1.10", [22], removed=[11])
    assert esito["settlement_status"] == "SETTLED"
    rec = _check_settlement(broker, acct, "1.10")
    assert rec["commission_amount"] == 0.0
    f = _assert_funds(broker, acct, "post")
    assert _near(f["available"], START)


def test_BAL10_whole_market_void():
    """BAL-10: market void (every runner with bets REMOVED) => PnL 0, no commission."""
    broker, acct = _new("1.11", {11: (3.0, 3.0, 1000.0), 22: (2.5, 2.5, 1000.0)})
    _place(broker, acct, "a", "1.11", 11, "BACK", 3.0, 10.0, expect_matched="10")
    _place(broker, acct, "b", "1.11", 22, "LAY", 2.5, 4.0, expect_matched="4")
    broker.update_market_book(_closed("1.11", {11: "REMOVED", 22: "REMOVED"}))
    acct.void("1.11")
    _check_settlement(broker, acct, "1.11")
    _assert_funds(broker, acct, "post")
    assert _near(broker.get_account_funds()["available"], START)


def test_BAL10_removed_runner_with_bets_on_others_fail_closed():
    """BAL-10: reduction factors are NOT modelled: the SIM refuses to settle
    (fail-closed) and keeps everything committed, no credit at all."""
    broker, acct = _new("1.12", {11: (3.0, 3.0, 1000.0), 22: (2.5, 2.5, 1000.0)})
    _place(broker, acct, "a", "1.12", 11, "BACK", 3.0, 10.0, expect_matched="10")
    _place(broker, acct, "b", "1.12", 22, "BACK", 2.5, 4.0, expect_matched="4")
    before = broker.get_account_funds()
    esito = broker.update_market_book(_closed("1.12", {11: "WINNER", 22: "REMOVED"}))
    assert esito["settlement_status"] == "REDUCTION_FACTOR_UNSUPPORTED"
    assert broker.get_account_funds() == before
    assert broker.list_settlements() == []
    _assert_funds(broker, acct, "unsettled")  # oracle: still unsettled, committed 14


# --------------------------------------------------------------------------
# BAL-11 resettlement / duplicate settlement (I2)
# --------------------------------------------------------------------------
@pytest.mark.parametrize("second", [{11: "WINNER", 22: "LOSER"}, {11: "LOSER", 22: "WINNER"}])
def test_BAL11_duplicate_and_resettlement_no_double_credit(second):
    """BAL-11 / I2: a second CLOSED book (identical, or with a different result)
    returns DUPLICATE and credits nothing. A genuine Betfair re-settlement
    correction is NOT modelled (the first result stays)."""
    broker, acct = _new("1.13", {11: (3.0, 3.1, 1000.0), 22: (4.0, 4.1, 1000.0)})
    _place(broker, acct, "a", "1.13", 11, "BACK", 3.0, 10.0, expect_matched="10")
    _place(broker, acct, "b", "1.13", 22, "BACK", 4.0, 5.0, expect_matched="5")
    broker.update_market_book(_closed("1.13", {11: "WINNER", 22: "LOSER"}))
    acct.settle("1.13", [11])
    after_first = _assert_funds(broker, acct, "first")
    for _ in range(2):
        esito = broker.update_market_book(_closed("1.13", second))
        assert esito["settlement_status"] == "DUPLICATE"
        assert acct.settle("1.13", [11]) == "DUPLICATE"
        assert broker.get_account_funds() == after_first
        _assert_funds(broker, acct, "dup")
    assert len(broker.list_settlements()) == 1


# --------------------------------------------------------------------------
# BAL-12 commission on market net
# --------------------------------------------------------------------------
@pytest.mark.parametrize("winner", [11, 22, 33])
def test_BAL12_commission_on_market_net_not_per_bet(winner):
    """BAL-12: 4.5% once on positive market net; zero if market net <= 0."""
    broker, acct = _new("1.14", {11: (3.0, 3.1, 1000.0), 22: (2.0, 2.02, 1000.0), 33: (7.33, 7.4, 1000.0)})
    _place(broker, acct, "a", "1.14", 11, "BACK", 3.0, 10.0, expect_matched="10")
    _place(broker, acct, "b", "1.14", 22, "BACK", 2.0, 10.0, expect_matched="10")
    _place(broker, acct, "c", "1.14", 33, "LAY", 7.4, 2.0, expect_matched="2")
    _assert_funds(broker, acct, "pre")
    statuses = {s: ("WINNER" if s == winner else "LOSER") for s in (11, 22, 33)}
    broker.update_market_book(_closed("1.14", statuses))
    acct.settle("1.14", [winner])
    rec = _check_settlement(broker, acct, "1.14")
    wrong = O.per_bet_commission_wrong(acct._matched_bets("1.14"), [winner])
    if acct.settlements["1.14"]["commission"] != wrong:
        assert not _near(rec["commission_amount"], wrong)
    _assert_funds(broker, acct, "post")


# --------------------------------------------------------------------------
# BAL-13 unmatched / partial
# --------------------------------------------------------------------------
@pytest.mark.parametrize("side,price", [("BACK", 3.0), ("LAY", 3.0)])
def test_BAL13_partial_match_cancel_residual_then_settle(side, price):
    """BAL-13: partial fill (4 of 10); residual reserved; cancel releases it (I1); settle."""
    broker, acct = _new("1.15", {11: (3.0, 3.0, 4.0), 22: (4.0, 4.1, 1000.0)})
    bet = _place(broker, acct, "p", "1.15", 11, side, price, 10.0, expect_matched="4")
    f = _assert_funds(broker, acct, "partial")
    assert _near(f["total"], START)
    r = broker.cancel_order(bet_id=bet, market_id="1.15")
    assert r["instructionReports"][0]["status"] == "SUCCESS"
    assert _near(r["instructionReports"][0]["sizeCancelled"], acct.cancel("p"))
    f = _assert_funds(broker, acct, "cancelled")
    assert _near(f["total"], START)
    broker.update_market_book(_closed("1.15", {11: "WINNER", 22: "LOSER"}))
    acct.settle("1.15", [11])
    _check_settlement(broker, acct, "1.15")
    _assert_funds(broker, acct, "post")


@pytest.mark.parametrize("side,price,size", [("BACK", 5.0, 7.33), ("LAY", 2.0, 10.0)])
def test_BAL13_unmatched_reserve_and_cancel(side, price, size):
    """BAL-13: an unmatched order reserves its stake/liability; cancel restores
    available exactly; total never changes (I1)."""
    broker, acct = _new("1.16", {11: (3.0, 3.1, 1000.0)})
    bet = _place(broker, acct, "u", "1.16", 11, side, price, size, expect_matched="0")
    f = _assert_funds(broker, acct, "resting")
    assert f["available"] < START and _near(f["total"], START)
    broker.cancel_order(bet_id=bet, market_id="1.16")
    acct.cancel("u")
    f = _assert_funds(broker, acct, "cancelled")
    assert _near(f["available"], START)


def test_BAL13_unmatched_lapses_at_settlement_without_credit():
    """BAL-13: resting residual lapses at settlement and releases its reserve."""
    broker, acct = _new("1.17", {11: (3.0, 3.1, 4.0)})
    _place(broker, acct, "p", "1.17", 11, "BACK", 3.0, 10.0, expect_matched="4")
    _place(broker, acct, "r", "1.17", 11, "BACK", 3.5, 5.0, expect_matched="0")
    _assert_funds(broker, acct, "pre")
    broker.update_market_book(_closed("1.17", {11: "WINNER"}))
    acct.settle("1.17", [11])
    _check_settlement(broker, acct, "1.17")
    _assert_funds(broker, acct, "post")


# --------------------------------------------------------------------------
# BAL-15 crash / recovery
# --------------------------------------------------------------------------
def _restart(broker):
    stato = json.loads(json.dumps(broker.state.to_dict()))
    nuovo = SimulationBroker(starting_balance=float(START), commission_pct=4.5)
    nuovo.state.load_from_dict(stato)
    return nuovo


@pytest.mark.parametrize("outcome", ["WINNER", "LOSER"])
def test_BAL15_restart_before_settlement_settles_once(outcome):
    """BAL-15: restart (snapshot JSON roundtrip) mid-position: funds identical,
    settlement credits exactly once."""
    broker, acct = _new("1.18", {11: (3.0, 3.1, 1000.0), 22: (4.0, 4.1, 1000.0)})
    _greenup(broker, acct, "1.18", [(10.0, 3.0)], [(10.0, 2.0)])
    broker = _restart(_restart(broker))
    _assert_funds(broker, acct, "restarted")
    closed = _closed("1.18", {11: outcome, 22: "LOSER" if outcome == "WINNER" else "WINNER"})
    broker.update_market_book(closed)
    acct.settle("1.18", [11] if outcome == "WINNER" else [22])
    _check_settlement(broker, acct, "1.18")
    _assert_funds(broker, acct, "settled")
    broker = _restart(broker)
    assert broker.update_market_book(copy.deepcopy(closed))["settlement_status"] == "DUPLICATE"
    _assert_funds(broker, acct, "dup after restart")


def test_BAL15_restart_after_settlement_no_double_credit():
    """BAL-15 / I2: settle, restart, re-deliver the CLOSED book: no double credit."""
    broker, acct = _new("1.19", {11: (2.0, 2.02, 1000.0), 22: (2.0, 2.02, 1000.0)})
    _place(broker, acct, "a", "1.19", 11, "BACK", 2.0, 10.0, expect_matched="10")
    closed = _closed("1.19", {11: "WINNER", 22: "LOSER"})
    broker.update_market_book(closed)
    acct.settle("1.19", [11])
    f1 = _assert_funds(broker, acct, "settled")
    for _ in range(3):
        broker = _restart(broker)
        assert broker.update_market_book(copy.deepcopy(closed))["settlement_status"] == "DUPLICATE"
        assert broker.get_account_funds()["available"] == pytest.approx(f1["available"])
        _assert_funds(broker, acct, "restart")
    assert len(broker.list_settlements()) == 1


# --------------------------------------------------------------------------
# BAL-16 money management uses only available
# --------------------------------------------------------------------------
def test_BAL16_new_order_limited_by_available_not_by_balance():
    """BAL-16: after a green-up (unsettled +10 conditional) a new order on another
    market is accepted up to the oracle's available, rejected 0.01 above it."""
    broker, acct = _new("1.20", {11: (3.0, 3.1, 1000.0)})
    _greenup(broker, acct, "1.20", [(10.0, 3.0)], [(10.0, 2.0)])
    broker.update_market_book(_open("1.20", {11: (3.0, 3.1, 1000.0)}))
    _place(broker, acct, "o", "1.20", 11, "BACK", 3.0, 7.0, expect_matched="7")  # open exposure on top
    assert broker.state.balance > float(acct.available)  # the trap: balance carries the D8 credit
    avail = acct.available
    broker.update_market_book(_open("1.21", {5: (2.0, 2.02, 1000.0)}))
    acct.declare_market("1.21", [5])
    over = float(avail + O.D("0.01"))
    assert not acct.would_accept("1.21", 5, "BACK", "50", O.D(str(over)))
    r = broker.place_bet(market_id="1.21", selection_id=5, side="BACK", price=50.0, size=over)
    assert r["status"] == "FAILURE" and r["errorCode"] == "INSUFFICIENT_FUNDS", r
    _assert_funds(broker, acct, "after reject")
    assert acct.would_accept("1.21", 5, "BACK", "50", avail)
    _place(broker, acct, "max", "1.21", 5, "BACK", 50.0, float(avail), expect_matched="0")
    f = _assert_funds(broker, acct, "all committed")
    assert _near(f["available"], 0)
