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
    status: str = "EXECUTABLE"
    event_key: str = ""
    table_id: Optional[int] = None
    batch_id: str = ""
    event_name: str = ""
    market_name: str = ""
    runner_name: str = ""
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def remaining_size(self) -> float:
        return max(0.0, float(self.size or 0.0) - float(self.matched_size or 0.0))

    def is_complete(self) -> bool:
        return float(self.matched_size or 0.0) >= float(self.size or 0.0) and float(self.size or 0.0) > 0.0

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
    open_positions: List[Dict[str, Any]]
    updated_at: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class SimulationState:
    """
    Stato centrale del paper trading.

    Tiene traccia di:
    - bankroll simulato disponibile
    - esposizione corrente
    - pnl realizzato
    - equity peak
    - posizioni aperte

    Questo file NON fa matching.
    Tiene solo lo stato economico e delle posizioni.
    """

    def __init__(self, starting_balance: float = 1000.0):
        self._lock = RLock()

        self.bankroll_start = float(starting_balance or 0.0)
        self.bankroll_available = float(starting_balance or 0.0)
        self.exposure_open = 0.0
        self.realized_pnl = 0.0
        self.unrealized_pnl = 0.0
        self.equity_peak = float(starting_balance or 0.0)

        self.positions: Dict[str, SimulationPosition] = {}
        self.updated_at = datetime.utcnow().isoformat()

    # =========================================================
    # INTERNAL
    # =========================================================
    def _touch(self) -> None:
        self.updated_at = datetime.utcnow().isoformat()
        equity = self.equity_current()
        if equity > self.equity_peak:
            self.equity_peak = equity

    def _reserve_needed(self, *, side: str, price: float, size: float) -> float:
        side = str(side or "BACK").upper()
        price = float(price or 0.0)
        size = float(size or 0.0)

        if side == "LAY":
            return max(0.0, size * max(0.0, price - 1.0))
        return max(0.0, size)

    # =========================================================
    # BALANCE / EQUITY
    # =========================================================
    def equity_current(self) -> float:
        return float(self.bankroll_available) + float(self.exposure_open) + float(self.realized_pnl) + float(self.unrealized_pnl)

    def can_reserve(self, *, side: str, price: float, size: float) -> bool:
        reserve = self._reserve_needed(side=side, price=price, size=size)
        return reserve <= self.bankroll_available

    def reserve_for_order(self, *, side: str, price: float, size: float) -> float:
        with self._lock:
            reserve = self._reserve_needed(side=side, price=price, size=size)
            if reserve > self.bankroll_available:
                raise RuntimeError("Saldo simulato insufficiente")

            self.bankroll_available -= reserve
            self.exposure_open += reserve
            self._touch()
            return reserve

    def release_reserved(self, *, side: str, price: float, size: float) -> float:
        with self._lock:
            release = self._reserve_needed(side=side, price=price, size=size)
            self.bankroll_available += release
            self.exposure_open = max(0.0, self.exposure_open - release)
            self._touch()
            return release

    def apply_realized_pnl(self, pnl: float) -> None:
        with self._lock:
            self.realized_pnl += float(pnl or 0.0)
            self._touch()

    def set_unrealized_pnl(self, pnl: float) -> None:
        with self._lock:
            self.unrealized_pnl = float(pnl or 0.0)
            self._touch()

    def reset(self, starting_balance: Optional[float] = None) -> None:
        with self._lock:
            if starting_balance is not None:
                self.bankroll_start = float(starting_balance or 0.0)

            self.bankroll_available = float(self.bankroll_start)
            self.exposure_open = 0.0
            self.realized_pnl = 0.0
            self.unrealized_pnl = 0.0
            self.equity_peak = float(self.bankroll_start)
            self.positions = {}
            self._touch()

    # =========================================================
    # POSITIONS
    # =========================================================
    def add_position(self, position: SimulationPosition) -> None:
        with self._lock:
            position.updated_at = datetime.utcnow().isoformat()
            self.positions[position.bet_id] = position
            self._touch()

    def get_position(self, bet_id: str) -> Optional[SimulationPosition]:
        with self._lock:
            return self.positions.get(str(bet_id))

    def update_position(
        self,
        bet_id: str,
        *,
        matched_size: Optional[float] = None,
        avg_price_matched: Optional[float] = None,
        status: Optional[str] = None,
    ) -> Optional[SimulationPosition]:
        with self._lock:
            pos = self.positions.get(str(bet_id))
            if not pos:
                return None

            if matched_size is not None:
                pos.matched_size = float(matched_size)
            if avg_price_matched is not None:
                pos.avg_price_matched = float(avg_price_matched)
            if status is not None:
                pos.status = str(status)

            pos.updated_at = datetime.utcnow().isoformat()
            self._touch()
            return pos

    def remove_position(self, bet_id: str) -> Optional[SimulationPosition]:
        with self._lock:
            pos = self.positions.pop(str(bet_id), None)
            self._touch()
            return pos

    def list_positions(self) -> List[SimulationPosition]:
        with self._lock:
            return list(self.positions.values())

    def list_open_positions(self) -> List[SimulationPosition]:
        with self._lock:
            return [
                pos
                for pos in self.positions.values()
                if pos.status not in {"CANCELLED", "EXECUTION_COMPLETE", "SETTLED", "CLOSED"}
            ]

    # =========================================================
    # SNAPSHOT / SERIALIZATION
    # =========================================================
    def snapshot(self) -> SimulationSnapshot:
        with self._lock:
            open_positions = [pos.to_dict() for pos in self.list_open_positions()]
            return SimulationSnapshot(
                bankroll_start=float(self.bankroll_start),
                bankroll_available=float(self.bankroll_available),
                exposure_open=float(self.exposure_open),
                realized_pnl=float(self.realized_pnl),
                unrealized_pnl=float(self.unrealized_pnl),
                equity_current=float(self.equity_current()),
                equity_peak=float(self.equity_peak),
                open_positions_count=len(open_positions),
                open_positions=open_positions,
                updated_at=self.updated_at,
            )

    def to_dict(self) -> Dict[str, Any]:
        snap = self.snapshot()
        return snap.to_dict()

    def load_from_dict(self, data: Dict[str, Any]) -> None:
        with self._lock:
            self.bankroll_start = float(data.get("bankroll_start", 1000.0) or 1000.0)
            self.bankroll_available = float(data.get("bankroll_available", self.bankroll_start) or self.bankroll_start)
            self.exposure_open = float(data.get("exposure_open", 0.0) or 0.0)
            self.realized_pnl = float(data.get("realized_pnl", 0.0) or 0.0)
            self.unrealized_pnl = float(data.get("unrealized_pnl", 0.0) or 0.0)
            self.equity_peak = float(data.get("equity_peak", self.bankroll_start) or self.bankroll_start)

            self.positions = {}
            for item in data.get("open_positions", []) or []:
                try:
                    pos = SimulationPosition(
                        bet_id=str(item["bet_id"]),
                        market_id=str(item["market_id"]),
                        selection_id=int(item["selection_id"]),
                        side=str(item.get("side", "BACK")),
                        price=float(item.get("price", 0.0) or 0.0),
                        size=float(item.get("size", 0.0) or 0.0),
                        matched_size=float(item.get("matched_size", 0.0) or 0.0),
                        avg_price_matched=float(item.get("avg_price_matched", 0.0) or 0.0),
                        status=str(item.get("status", "EXECUTABLE")),
                        event_key=str(item.get("event_key", "") or ""),
                        table_id=item.get("table_id"),
                        batch_id=str(item.get("batch_id", "") or ""),
                        event_name=str(item.get("event_name", "") or ""),
                        market_name=str(item.get("market_name", "") or ""),
                        runner_name=str(item.get("runner_name", "") or ""),
                        created_at=str(item.get("created_at", datetime.utcnow().isoformat())),
                        updated_at=str(item.get("updated_at", datetime.utcnow().isoformat())),
                    )
                    self.positions[pos.bet_id] = pos
                except Exception:
                    continue

            self._touch()