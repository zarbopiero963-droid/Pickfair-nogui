from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from threading import RLock
from typing import Any, Dict, List, Optional


@dataclass
class SimulationPosition:
    bet_id: str
    market_id: str
    selection_id: int
    side: str
    price: float
    size: float
    matched_size: float = 0.0
    avg_price_matched: float = 0.0
    status: str = "EXECUTABLE"  # EXECUTABLE / PARTIAL / EXECUTION_COMPLETE / CANCELLED / SETTLED / FAILED
    customer_ref: str = ""
    event_key: str = ""
    table_id: Optional[int] = None
    batch_id: str = ""
    event_name: str = ""
    market_name: str = ""
    runner_name: str = ""
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    notes: Dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class SimulationSnapshot:
    bankroll_start: float
    bankroll_available: float
    exposure_open: float
    realized_pnl: float
    unrealized_pnl: float
    equity_current: float
    equity_peak: float
    open_positions_count: int
    updated_at: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class SimulationState:
    """
    Stato centrale del broker simulato.

    Tiene:
    - bankroll iniziale
    - bankroll disponibile
    - esposizione aperta
    - pnl realizzato / unrealizzato
    - equity peak
    - posizioni simulate
    """

    def __init__(
        self,
        *,
        starting_balance: float = 1000.0,
        commission_pct: float = 4.5,
    ):
        self._lock = RLock()

        self.starting_balance = float(starting_balance or 0.0)
        self.bankroll_available = float(starting_balance or 0.0)
        self.exposure_open = 0.0
        self.realized_pnl = 0.0
        self.unrealized_pnl = 0.0
        self.equity_peak = float(starting_balance or 0.0)
        self.commission_pct = float(commission_pct or 0.0)
        self.updated_at = datetime.utcnow().isoformat()

        self._positions: Dict[str, SimulationPosition] = {}

    # =========================================================
    # INTERNAL
    # =========================================================
    def _touch(self) -> None:
        self.updated_at = datetime.utcnow().isoformat()
        current_equity = self.equity_current()
        if current_equity > self.equity_peak:
            self.equity_peak = current_equity

    def _calc_liability(self, side: str, price: float, size: float) -> float:
        side = str(side or "BACK").upper()
        price = float(price or 0.0)
        size = float(size or 0.0)

        if side == "LAY":
            return max(0.0, size * max(price - 1.0, 0.0))
        return max(0.0, size)

    def _recompute_exposure(self) -> None:
        exposure = 0.0
        for pos in self._positions.values():
            if pos.status in {"EXECUTABLE", "PARTIAL", "EXECUTION_COMPLETE"} and pos.matched_size > 0:
                exposure += self._calc_liability(pos.side, pos.avg_price_matched or pos.price, pos.matched_size)
            elif pos.status in {"EXECUTABLE", "PARTIAL"} and pos.matched_size <= 0:
                exposure += self._calc_liability(pos.side, pos.price, pos.size)
        self.exposure_open = float(exposure)

    def _recompute_bankroll_available(self) -> None:
        self.bankroll_available = float(self.starting_balance + self.realized_pnl - self.exposure_open)

    # =========================================================
    # POSITION CRUD
    # =========================================================
    def add_position(
        self,
        *,
        bet_id: str,
        market_id: str,
        selection_id: int,
        side: str,
        price: float,
        size: float,
        customer_ref: str = "",
        event_key: str = "",
        table_id: Optional[int] = None,
        batch_id: str = "",
        event_name: str = "",
        market_name: str = "",
        runner_name: str = "",
    ) -> SimulationPosition:
        with self._lock:
            pos = SimulationPosition(
                bet_id=str(bet_id),
                market_id=str(market_id),
                selection_id=int(selection_id),
                side=str(side).upper(),
                price=float(price),
                size=float(size),
                customer_ref=str(customer_ref or ""),
                event_key=str(event_key or ""),
                table_id=table_id,
                batch_id=str(batch_id or ""),
                event_name=str(event_name or ""),
                market_name=str(market_name or ""),
                runner_name=str(runner_name or ""),
                notes={"customer_ref": str(customer_ref or "")},
            )
            self._positions[pos.bet_id] = pos
            self._recompute_exposure()
            self._recompute_bankroll_available()
            self._touch()
            return SimulationPosition(**pos.to_dict())

    def get_position(self, bet_id: str) -> Optional[SimulationPosition]:
        with self._lock:
            pos = self._positions.get(str(bet_id))
            return SimulationPosition(**pos.to_dict()) if pos else None

    def list_positions(self) -> List[SimulationPosition]:
        with self._lock:
            return [SimulationPosition(**p.to_dict()) for p in self._positions.values()]

    def list_open_positions(self) -> List[SimulationPosition]:
        with self._lock:
            out = []
            for pos in self._positions.values():
                if pos.status in {"EXECUTABLE", "PARTIAL", "EXECUTION_COMPLETE"}:
                    out.append(SimulationPosition(**pos.to_dict()))
            return out

    # =========================================================
    # MATCHING / UPDATE
    # =========================================================
    def update_match(
        self,
        *,
        bet_id: str,
        matched_size: float,
        avg_price_matched: float,
        status: str,
    ) -> Optional[SimulationPosition]:
        with self._lock:
            pos = self._positions.get(str(bet_id))
            if not pos:
                return None

            pos.matched_size = float(matched_size or 0.0)
            pos.avg_price_matched = float(avg_price_matched or 0.0)
            pos.status = str(status or pos.status)
            pos.updated_at = datetime.utcnow().isoformat()

            self._recompute_exposure()
            self._recompute_bankroll_available()
            self._touch()
            return SimulationPosition(**pos.to_dict())

    def cancel_position(self, bet_id: str) -> Optional[SimulationPosition]:
        with self._lock:
            pos = self._positions.get(str(bet_id))
            if not pos:
                return None

            if pos.matched_size > 0:
                pos.status = "EXECUTION_COMPLETE"
            else:
                pos.status = "CANCELLED"

            pos.updated_at = datetime.utcnow().isoformat()

            self._recompute_exposure()
            self._recompute_bankroll_available()
            self._touch()
            return SimulationPosition(**pos.to_dict())

    def settle_position(self, bet_id: str, pnl: float) -> Optional[SimulationPosition]:
        with self._lock:
            pos = self._positions.get(str(bet_id))
            if not pos:
                return None

            pnl = float(pnl or 0.0)
            commission = 0.0
            if pnl > 0 and self.commission_pct > 0:
                commission = pnl * (self.commission_pct / 100.0)

            net_pnl = pnl - commission

            pos.realized_pnl = float(net_pnl)
            pos.unrealized_pnl = 0.0
            pos.status = "SETTLED"
            pos.updated_at = datetime.utcnow().isoformat()

            self.realized_pnl += float(net_pnl)

            self._recompute_exposure()
            self._recompute_bankroll_available()
            self._touch()
            return SimulationPosition(**pos.to_dict())

    def set_unrealized_pnl(self, bet_id: str, pnl: float) -> Optional[SimulationPosition]:
        with self._lock:
            pos = self._positions.get(str(bet_id))
            if not pos:
                return None

            pos.unrealized_pnl = float(pnl or 0.0)
            pos.updated_at = datetime.utcnow().isoformat()

            self._recompute_unrealized_pnl()
            self._touch()
            return SimulationPosition(**pos.to_dict())

    def _recompute_unrealized_pnl(self) -> None:
        self.unrealized_pnl = float(sum(p.unrealized_pnl for p in self._positions.values() if p.status != "SETTLED"))

    # =========================================================
    # METRICS
    # =========================================================
    def equity_current(self) -> float:
        return float(self.bankroll_available + self.exposure_open + self.unrealized_pnl)

    def snapshot(self) -> SimulationSnapshot:
        with self._lock:
            self._recompute_exposure()
            self._recompute_bankroll_available()
            self._recompute_unrealized_pnl()

            return SimulationSnapshot(
                bankroll_start=float(self.starting_balance),
                bankroll_available=float(self.bankroll_available),
                exposure_open=float(self.exposure_open),
                realized_pnl=float(self.realized_pnl),
                unrealized_pnl=float(self.unrealized_pnl),
                equity_current=float(self.equity_current()),
                equity_peak=float(self.equity_peak),
                open_positions_count=len(
                    [
                        p for p in self._positions.values()
                        if p.status in {"EXECUTABLE", "PARTIAL", "EXECUTION_COMPLETE"}
                    ]
                ),
                updated_at=self.updated_at,
            )

    # =========================================================
    # RESET / SERIALIZATION
    # =========================================================
    def reset(
        self,
        *,
        starting_balance: Optional[float] = None,
        commission_pct: Optional[float] = None,
    ) -> None:
        with self._lock:
            if starting_balance is not None:
                self.starting_balance = float(starting_balance or 0.0)
            if commission_pct is not None:
                self.commission_pct = float(commission_pct or 0.0)

            self.bankroll_available = float(self.starting_balance)
            self.exposure_open = 0.0
            self.realized_pnl = 0.0
            self.unrealized_pnl = 0.0
            self.equity_peak = float(self.starting_balance)
            self._positions.clear()
            self._touch()

    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "starting_balance": float(self.starting_balance),
                "bankroll_available": float(self.bankroll_available),
                "exposure_open": float(self.exposure_open),
                "realized_pnl": float(self.realized_pnl),
                "unrealized_pnl": float(self.unrealized_pnl),
                "equity_peak": float(self.equity_peak),
                "commission_pct": float(self.commission_pct),
                "updated_at": self.updated_at,
                "positions": [p.to_dict() for p in self._positions.values()],
            }

    def load_from_dict(self, data: Dict[str, Any]) -> None:
        with self._lock:
            self.starting_balance = float(data.get("starting_balance", 0.0) or 0.0)
            self.bankroll_available = float(data.get("bankroll_available", self.starting_balance) or self.starting_balance)
            self.exposure_open = float(data.get("exposure_open", 0.0) or 0.0)
            self.realized_pnl = float(data.get("realized_pnl", 0.0) or 0.0)
            self.unrealized_pnl = float(data.get("unrealized_pnl", 0.0) or 0.0)
            self.equity_peak = float(data.get("equity_peak", self.starting_balance) or self.starting_balance)
            self.commission_pct = float(data.get("commission_pct", 4.5) or 4.5)
            self.updated_at = str(data.get("updated_at") or datetime.utcnow().isoformat())

            self._positions.clear()
            for item in data.get("positions", []) or []:
                try:
                    pos = SimulationPosition(**item)
                    self._positions[pos.bet_id] = pos
                except Exception:
                    continue

            self._recompute_exposure()
            self._recompute_bankroll_available()
            self._recompute_unrealized_pnl()
            self._touch()