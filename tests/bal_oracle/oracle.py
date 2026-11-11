"""Independent accounting oracle for Betfair Exchange Italia (BAL-01..BAL-18).

Pure Python + ``decimal.Decimal``. It does NOT import project code (no
``core.pnl_engine``, no ``simulation_broker``): expected values come from the
exchange contract written here, never from the code under test.

Contract encoded (owner, Betfair Exchange Italia):

* BACK  wins: +stake*(price-1); loses: -stake.
* LAY   wins (runner wins): -stake*(price-1) (liability); loses: +stake.
* Removed runner (REMOVED/REMOVED_VACANT): bet void, PnL 0, stake returned.
* Commission 4.5% on the POSITIVE NET gross of the MARKET, once per market,
  never per bet; zero if market net <= 0.
* Exposure (committed) of an unsettled market = worst case loss over the
  outcomes. Matched bets count with their full PnL; an unmatched remainder
  can only add to the loss of a scenario (``min(0, pnl)``), never hedge it.
  Single-winner market: scenarios "selection s wins" for each known s plus
  "none of the known selections wins".
* Buckets of the account (all Decimal):

    settled_cash      starting balance + sum of settled market nets
    early_available   guaranteed minimum profit made SPENDABLE by an
                      exchange-CONFIRMED early event before settlement
                      (legitimate, separate bucket, never "settled")
    early_available_unsettled
                      the explicit COUNTERPART of ``early_available``: a
                      liability against the future settlement, same amount
    committed         sum of market exposures (BACK stake / LAY liability)
    available         settled_cash + early_available - committed  (spendable)
    total (wealth)    available + committed - early_available_unsettled
                      (= settled_cash: an early availability makes funds
                      spendable but does NOT increase wealth)

Invariants:
  I1  a pure commit/release (place, cancel, early stake/liability release)
      never changes ``total``.
  I2  a market's profit is credited exactly once: at settlement the
      early-available amount of that market is netted (moved), never added
      twice; a second settlement event changes nothing.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, Iterable, List, Optional, Set

COMMISSION_RATE = Decimal("0.045")
ZERO = Decimal("0")
VOID_STATUSES = {"REMOVED", "REMOVED_VACANT"}


def D(x) -> Decimal:
    """Decimal from str/int/float through ``str`` (no binary float noise)."""
    if isinstance(x, Decimal):
        return x
    return Decimal(str(x))


def cents(x: Decimal) -> Decimal:
    return D(x).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


@dataclass(frozen=True)
class Bet:
    side: str            # "BACK" | "LAY"
    stake: Decimal       # matched size
    price: Decimal       # matched (average) price
    selection: int

    @staticmethod
    def of(side: str, stake, price, selection: int) -> "Bet":
        side = side.upper()
        assert side in ("BACK", "LAY"), side
        return Bet(side, D(stake), D(price), int(selection))

    def pnl(self, wins: bool) -> Decimal:
        if self.side == "BACK":
            return self.stake * (self.price - 1) if wins else -self.stake
        return -self.stake * (self.price - 1) if wins else self.stake

    def committed(self) -> Decimal:
        """Money the bet locks on its own: BACK stake, LAY liability."""
        return self.stake if self.side == "BACK" else self.stake * (self.price - 1)


# --------------------------------------------------------------------------
# Per-market PnL
# --------------------------------------------------------------------------
def outcomes_pnl(matched: Iterable[Bet], selections: Optional[Iterable[int]] = None,
                 unmatched: Iterable[Bet] = ()) -> Dict[Optional[int], Decimal]:
    """Market PnL for each outcome of a single-winner market.

    Key = winning selection id, ``None`` = none of the known selections wins.
    Unmatched remainders only add losses (``min(0, pnl)``)."""
    matched = list(matched)
    unmatched = list(unmatched)
    sels: Set[int] = set(selections or ()) | {b.selection for b in matched} | {b.selection for b in unmatched}
    out: Dict[Optional[int], Decimal] = {}
    for winner in list(sorted(sels)) + [None]:
        total = ZERO
        for b in matched:
            total += b.pnl(b.selection == winner)
        for b in unmatched:
            total += min(ZERO, b.pnl(b.selection == winner))
        out[winner] = total
    return out


def guaranteed_min(matched: Iterable[Bet], selections: Optional[Iterable[int]] = None) -> Decimal:
    """Worst-case market PnL over all outcomes (matched bets only)."""
    return min(outcomes_pnl(matched, selections).values())


def exposure(matched: Iterable[Bet], unmatched: Iterable[Bet] = (),
             selections: Optional[Iterable[int]] = None) -> Decimal:
    """Committed funds of an unsettled market: max(0, -worst case)."""
    matched, unmatched = list(matched), list(unmatched)
    if not matched and not unmatched:
        return ZERO
    return max(ZERO, -min(outcomes_pnl(matched, selections, unmatched).values()))


def settled(matched: Iterable[Bet], winners: Iterable[int], removed: Iterable[int] = ()) -> Dict[str, Decimal]:
    """Settlement of a CLOSED market: gross market net, commission (4.5% only if
    gross > 0, once per market), net. Bets on removed runners are void."""
    winners, removed = set(winners), set(removed)
    gross = ZERO
    for b in matched:
        if b.selection in removed:
            continue
        gross += b.pnl(b.selection in winners)
    commission = gross * COMMISSION_RATE if gross > 0 else ZERO
    return {"gross": gross, "commission": commission, "net": gross - commission}


def per_bet_commission_wrong(matched: Iterable[Bet], winners: Iterable[int]) -> Decimal:
    """The WRONG rule (commission on each winning bet): used only to show a test
    discriminates market-net from per-bet commission."""
    winners = set(winners)
    return sum((max(ZERO, b.pnl(b.selection in winners)) * COMMISSION_RATE for b in matched), ZERO)


# --------------------------------------------------------------------------
# Account ledger driven by events
# --------------------------------------------------------------------------
class OracleError(AssertionError):
    pass


@dataclass
class _Order:
    bet_id: str
    market: str
    selection: int
    side: str
    price: Decimal          # limit price
    size: Decimal
    matched: Decimal = ZERO
    matched_price: Decimal = ZERO
    status: str = "EXECUTABLE"   # EXECUTABLE | COMPLETE | CANCELLED | SETTLED | LAPSED

    @property
    def residual(self) -> Decimal:
        return self.size - self.matched if self.status == "EXECUTABLE" else ZERO


@dataclass
class Account:
    starting: Decimal
    settled_cash: Decimal = ZERO
    early_available: Dict[str, Decimal] = field(default_factory=dict)
    early_released: Set[str] = field(default_factory=set)
    orders: Dict[str, _Order] = field(default_factory=dict)
    settlements: Dict[str, Dict[str, Decimal]] = field(default_factory=dict)
    credits: Dict[str, Decimal] = field(default_factory=dict)  # every credit ever made per market (I2 audit)
    selections: Dict[str, Set[int]] = field(default_factory=dict)

    def __post_init__(self):
        self.starting = D(self.starting)
        self.settled_cash = self.starting

    # ------------------------------------------------------------------ views
    def _matched_bets(self, market: str) -> List[Bet]:
        return [Bet(o.side, o.matched, o.matched_price, o.selection)
                for o in self.orders.values() if o.market == market and o.matched > 0]

    def _unmatched_bets(self, market: str) -> List[Bet]:
        return [Bet(o.side, o.residual, o.price, o.selection)
                for o in self.orders.values() if o.market == market and o.residual > 0]

    def market_committed(self, market: str) -> Decimal:
        if market in self.settlements or market in self.early_released:
            return ZERO
        return exposure(self._matched_bets(market), self._unmatched_bets(market),
                        self.selections.get(market))

    @property
    def committed(self) -> Decimal:
        markets = {o.market for o in self.orders.values()}
        return sum((self.market_committed(m) for m in markets), ZERO)

    @property
    def early_total(self) -> Decimal:
        return sum(self.early_available.values(), ZERO)

    @property
    def available(self) -> Decimal:
        return self.settled_cash + self.early_total - self.committed

    @property
    def early_available_unsettled(self) -> Decimal:
        """Counterpart liability of the early bucket (owed to the settlement)."""
        return self.early_total

    @property
    def total(self) -> Decimal:
        return self.available + self.committed - self.early_available_unsettled

    @property
    def realized(self) -> Decimal:
        """Settled PnL only (never potential or early-available)."""
        return self.settled_cash - self.starting

    def potential(self, market: str) -> Dict[Optional[int], Decimal]:
        return outcomes_pnl(self._matched_bets(market), self.selections.get(market))

    def snapshot(self) -> Dict[str, Decimal]:
        return {"available": self.available, "committed": self.committed, "total": self.total,
                "realized": self.realized, "early_available": self.early_total,
                "early_available_unsettled": self.early_available_unsettled}

    # ----------------------------------------------------------------- events
    def declare_market(self, market: str, selections: Iterable[int]) -> None:
        self.selections[market] = set(selections)

    def place(self, bet_id: str, market: str, selection: int, side: str, price, size,
              matched=ZERO, matched_price=None) -> None:
        """Place an order; ``matched`` of it fills immediately at ``matched_price``.
        Placing is a pure commit: total unchanged (I1)."""
        before = self.total
        size, matched = D(size), D(matched)
        if matched < 0 or matched > size:
            raise OracleError("matched outside [0, size]")
        if market in self.settlements:
            self.orders[bet_id] = _Order(bet_id, market, int(selection), side.upper(), D(price), size,
                                         status="LAPSED")
            return
        o = _Order(bet_id, market, int(selection), side.upper(), D(price), size, matched,
                   D(matched_price if matched_price is not None else price) if matched > 0 else ZERO)
        if matched == size:
            o.status = "COMPLETE"
        if self.available - self._delta_committed_if(o) < 0 and self._delta_committed_if(o) > 0:
            raise OracleError("INSUFFICIENT_FUNDS")
        self.orders[bet_id] = o
        assert self.total == before, "I1 violated by place"

    def _delta_committed_if(self, o: _Order) -> Decimal:
        before = self.committed
        self.orders[o.bet_id] = o
        after = self.committed
        del self.orders[o.bet_id]
        return after - before

    def would_accept(self, market: str, selection: int, side: str, price, size) -> bool:
        probe = _Order("__probe__", market, int(selection), side.upper(), D(price), D(size))
        delta = self._delta_committed_if(probe)
        return delta <= 0 or self.available - delta >= 0

    def fill(self, bet_id: str, size, price) -> None:
        """Later (partial) fill of a resting order: pure commit/re-shape (I1)."""
        before = self.total
        o = self.orders[bet_id]
        size, price = D(size), D(price)
        if o.status != "EXECUTABLE" or size <= 0 or size > o.residual:
            raise OracleError("invalid fill")
        o.matched_price = (o.matched * o.matched_price + size * price) / (o.matched + size)
        o.matched += size
        if o.matched == o.size:
            o.status = "COMPLETE"
        assert self.total == before, "I1 violated by fill"

    def cancel(self, bet_id: str) -> Decimal:
        """Cancel the unmatched remainder: pure release (I1). Returns size cancelled."""
        before = self.total
        o = self.orders[bet_id]
        if o.status != "EXECUTABLE":
            return ZERO
        cancelled = o.residual
        o.size = o.matched
        o.status = "CANCELLED" if o.matched == 0 else "COMPLETE"
        assert self.total == before, "I1 violated by cancel"
        return cancelled

    def early_release(self, market: str) -> Decimal:
        """Exchange-confirmed early release of the market's committed stake /
        liability (e.g. BACK winner already decided). Pure release: committed
        -> available, total unchanged (I1). No profit credited."""
        before = self.total
        released = self.market_committed(market)
        self.early_released.add(market)
        assert self.total == before, "I1 violated by early release"
        return released

    def early_credit(self, market: str, amount) -> None:
        """Exchange-confirmed early availability of (part of) the guaranteed
        minimum profit, BEFORE settlement. Separate bucket; never ``realized``.
        Cannot exceed the guaranteed minimum (never potential profit). Booked
        with its counterpart ``early_available_unsettled``: wealth unchanged."""
        amount = D(amount)
        before = self.total
        if market in self.settlements:
            raise OracleError("early credit after settlement")
        gmin = guaranteed_min(self._matched_bets(market), self.selections.get(market))
        if amount <= 0 or self.early_available.get(market, ZERO) + amount > gmin:
            raise OracleError("early credit above guaranteed minimum")
        self.early_available[market] = self.early_available.get(market, ZERO) + amount
        self.credits[market] = self.credits.get(market, ZERO) + amount
        assert self.total == before, "early availability must not create wealth"

    def settle(self, market: str, winners: Iterable[int], removed: Iterable[int] = ()) -> str:
        """Settle a CLOSED market once. Duplicate => 'DUPLICATE', no effect (I2).
        The early-available amount is netted: the market's total credit equals
        its settled net exactly once."""
        if market in self.settlements:
            return "DUPLICATE"
        res = settled(self._matched_bets(market), winners, removed)
        early = self.early_available.pop(market, ZERO)
        self.settled_cash += res["net"]
        self.credits[market] = self.credits.get(market, ZERO) + (res["net"] - early)
        if self.credits[market] != res["net"]:
            raise OracleError("I2 violated: market credited %s != net %s" % (self.credits[market], res["net"]))
        self.settlements[market] = res
        for o in self.orders.values():
            if o.market != market:
                continue
            if o.matched > 0:
                o.status = "SETTLED"
            elif o.status == "EXECUTABLE":
                o.status = "LAPSED"
        return "SETTLED"

    def void(self, market: str) -> str:
        """Whole market void: every stake returned, PnL 0, no commission."""
        sels = {o.selection for o in self.orders.values() if o.market == market}
        return self.settle(market, winners=(), removed=sels)
