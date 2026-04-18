from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Optional


_D0 = Decimal("0")


def _to_decimal(value: Any, default: str = "0") -> Decimal:
    try:
        if value in (None, ""):
            return Decimal(default)
        return Decimal(str(value))
    except Exception:
        return Decimal(default)


@dataclass(frozen=True)
class PositionSnapshot:
    market_id: str
    runner_id: int
    open_side: str
    open_size: float
    avg_entry_price: float
    realized_pnl: float
    unrealized_pnl: float
    exposure: float
    liability: float


class PositionLedger:
    """
    Authoritative runner/market position ledger.

    Contract:
    - one ledger instance per (market_id, runner_id)
    - same-side fills are weighted-average merged
    - opposite-side fills first close existing open size (partial/full), then
      optionally open residual on the incoming side
    - realized PnL is updated only by closed quantity
    - unrealized PnL is mark-to-market and kept separate from realized
    - exposure/liability is explicit from residual open position
    - duplicate fill_id is idempotent no-op (no double realization)

    PnL convention (aligned to current repo event-driven sign semantics):
    - BACK position closed by LAY at close_price:
        realized_delta = (avg_back_price - close_price) * close_size
    - LAY position closed by BACK at close_price:
        realized_delta = (close_price - avg_lay_price) * close_size
    """

    def __init__(self, *, market_id: str, runner_id: int):
        self.market_id = str(market_id or "")
        self.runner_id = int(runner_id)

        self._open_side: str = ""
        self._open_size: Decimal = _D0
        self._open_notional: Decimal = _D0

        self._realized_pnl: Decimal = _D0
        self._unrealized_pnl: Decimal = _D0

        self._processed_fill_ids: set[str] = set()

    @staticmethod
    def _safe_side(side: Any) -> str:
        normalized = str(side or "").upper().strip()
        if normalized not in {"BACK", "LAY"}:
            raise ValueError("side must be BACK or LAY")
        return normalized

    def _avg_entry_price(self) -> Decimal:
        if self._open_size <= _D0:
            return _D0
        return self._open_notional / self._open_size

    def _compute_realized_delta(self, *, open_side: str, avg_price: Decimal, close_price: Decimal, close_size: Decimal) -> Decimal:
        if close_size <= _D0:
            return _D0
        if open_side == "BACK":
            return (avg_price - close_price) * close_size
        return (close_price - avg_price) * close_size

    def _mark_to_market_unrealized(self, mark_price: Decimal) -> Decimal:
        if self._open_side == "" or self._open_size <= _D0:
            return _D0
        avg_price = self._avg_entry_price()
        if self._open_side == "BACK":
            return (avg_price - mark_price) * self._open_size
        return (mark_price - avg_price) * self._open_size

    def _residual_exposure_liability(self) -> tuple[Decimal, Decimal]:
        if self._open_side == "" or self._open_size <= _D0:
            return (_D0, _D0)
        avg_price = self._avg_entry_price()
        if self._open_side == "BACK":
            exposure = self._open_size
            liability = self._open_size
        else:
            liability = self._open_size * max(_D0, avg_price - Decimal("1"))
            exposure = liability
        return (exposure, liability)

    def apply_fill(self, *, fill_id: str, side: str, price: float, size: float) -> Dict[str, Any]:
        fill_key = str(fill_id or "").strip()
        if not fill_key:
            raise ValueError("fill_id is required")
        if fill_key in self._processed_fill_ids:
            return {
                "applied": False,
                "duplicate": True,
                "realized_delta": 0.0,
                "snapshot": self.snapshot(),
            }

        incoming_side = self._safe_side(side)
        incoming_price = _to_decimal(price)
        incoming_size = _to_decimal(size)

        if incoming_price <= Decimal("1"):
            raise ValueError("price must be > 1")
        if incoming_size <= _D0:
            raise ValueError("size must be > 0")

        realized_delta = _D0
        remaining_incoming = incoming_size

        if self._open_side == "":
            self._open_side = incoming_side
            self._open_size = incoming_size
            self._open_notional = incoming_size * incoming_price
        elif self._open_side == incoming_side:
            self._open_size += incoming_size
            self._open_notional += incoming_size * incoming_price
        else:
            close_size = min(self._open_size, remaining_incoming)
            avg_price = self._avg_entry_price()
            realized_delta = self._compute_realized_delta(
                open_side=self._open_side,
                avg_price=avg_price,
                close_price=incoming_price,
                close_size=close_size,
            )
            self._realized_pnl += realized_delta

            self._open_size -= close_size
            self._open_notional -= close_size * avg_price
            remaining_incoming -= close_size

            if self._open_size <= _D0:
                self._open_side = ""
                self._open_size = _D0
                self._open_notional = _D0

            if remaining_incoming > _D0:
                self._open_side = incoming_side
                self._open_size = remaining_incoming
                self._open_notional = remaining_incoming * incoming_price

        self._processed_fill_ids.add(fill_key)

        return {
            "applied": True,
            "duplicate": False,
            "realized_delta": float(realized_delta),
            "snapshot": self.snapshot(),
        }

    def mark_to_market(self, *, mark_price: float) -> PositionSnapshot:
        mark = _to_decimal(mark_price)
        if mark <= Decimal("1"):
            raise ValueError("mark_price must be > 1")
        self._unrealized_pnl = self._mark_to_market_unrealized(mark)
        return self.snapshot()

    def snapshot(self) -> PositionSnapshot:
        avg = self._avg_entry_price()
        exposure, liability = self._residual_exposure_liability()
        return PositionSnapshot(
            market_id=self.market_id,
            runner_id=self.runner_id,
            open_side=self._open_side,
            open_size=float(self._open_size),
            avg_entry_price=float(avg),
            realized_pnl=float(self._realized_pnl),
            unrealized_pnl=float(self._unrealized_pnl),
            exposure=float(exposure),
            liability=float(liability),
        )
