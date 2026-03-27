from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from threading import RLock
from typing import Any, Dict

from core.system_state import DeskMode, RuntimeMode


@dataclass
class RiskDeskSnapshot:
    runtime_mode: str
    desk_mode: str
    bankroll_start: float
    bankroll_current: float
    equity_peak: float
    realized_pnl: float
    unrealized_pnl: float
    total_exposure: float
    drawdown_pct: float
    telegram_connected: bool
    betfair_connected: bool
    active_tables: int
    recovery_tables: int
    last_error: str
    last_signal_at: str
    updated_at: str


class RiskDesk:
    """
    Cruscotto finanziario runtime.

    Responsabilità:
    - traccia bankroll iniziale e corrente
    - traccia equity peak
    - calcola drawdown %
    - accumula pnl realizzato / unrealizzato
    - produce snapshot coerenti per GUI / runtime / log

    NON gestisce:
    - allocazione tavoli
    - anti-duplicazione
    - matching ordini
    """

    def __init__(self):
        self._lock = RLock()

        self.bankroll_start = 0.0
        self.bankroll_current = 0.0
        self.equity_peak = 0.0

        self.realized_pnl = 0.0
        self.unrealized_pnl = 0.0

        self.updated_at = datetime.utcnow().isoformat()

    # =========================================================
    # INTERNAL
    # =========================================================
    def _touch(self) -> None:
        self.updated_at = datetime.utcnow().isoformat()
        if self.bankroll_current > self.equity_peak:
            self.equity_peak = self.bankroll_current

    def _safe_mode_value(self, value: RuntimeMode | str) -> str:
        if isinstance(value, RuntimeMode):
            return value.value
        return str(value)

    def _safe_desk_value(self, value: DeskMode | str) -> str:
        if isinstance(value, DeskMode):
            return value.value
        return str(value)

    # =========================================================
    # BANKROLL / EQUITY
    # =========================================================
    def sync_bankroll(self, bankroll_current: float) -> None:
        with self._lock:
            bankroll_current = float(bankroll_current or 0.0)

            if self.bankroll_start <= 0.0:
                self.bankroll_start = bankroll_current

            self.bankroll_current = bankroll_current

            if self.equity_peak <= 0.0:
                self.equity_peak = bankroll_current
            elif bankroll_current > self.equity_peak:
                self.equity_peak = bankroll_current

            self._touch()

    def set_bankroll_start(self, bankroll_start: float) -> None:
        with self._lock:
            bankroll_start = float(bankroll_start or 0.0)
            self.bankroll_start = bankroll_start
            if self.bankroll_current <= 0.0:
                self.bankroll_current = bankroll_start
            if self.equity_peak <= 0.0:
                self.equity_peak = max(bankroll_start, self.bankroll_current)
            self._touch()

    def set_unrealized_pnl(self, pnl: float) -> None:
        with self._lock:
            self.unrealized_pnl = float(pnl or 0.0)
            self._touch()

    def apply_closed_pnl(self, pnl: float) -> None:
        with self._lock:
            pnl = float(pnl or 0.0)
            self.realized_pnl += pnl
            self.bankroll_current += pnl

            if self.bankroll_current > self.equity_peak:
                self.equity_peak = self.bankroll_current

            self._touch()

    def apply_open_exposure_snapshot(self, bankroll_current: float, unrealized_pnl: float = 0.0) -> None:
        """
        Utile quando il broker simulato/live fornisce uno snapshot già pronto.
        """
        with self._lock:
            self.bankroll_current = float(bankroll_current or 0.0)
            self.unrealized_pnl = float(unrealized_pnl or 0.0)

            if self.bankroll_start <= 0.0:
                self.bankroll_start = self.bankroll_current

            if self.equity_peak <= 0.0 or self.bankroll_current > self.equity_peak:
                self.equity_peak = self.bankroll_current

            self._touch()

    # =========================================================
    # METRICS
    # =========================================================
    def equity_current(self) -> float:
        with self._lock:
            return float(self.bankroll_current) + float(self.unrealized_pnl)

    def drawdown_pct(self) -> float:
        with self._lock:
            if self.equity_peak <= 0.0:
                return 0.0

            current_equity = self.equity_current()
            if current_equity >= self.equity_peak:
                return 0.0

            dd = ((self.equity_peak - current_equity) / self.equity_peak) * 100.0
            return max(0.0, float(dd))

    # =========================================================
    # RESET
    # =========================================================
    def reset_recovery_cycle(self) -> None:
        with self._lock:
            self.realized_pnl = 0.0
            self.unrealized_pnl = 0.0

            if self.bankroll_current > 0.0:
                self.bankroll_start = self.bankroll_current
                self.equity_peak = self.bankroll_current
            else:
                self.bankroll_start = 0.0
                self.equity_peak = 0.0

            self._touch()

    # =========================================================
    # SNAPSHOT
    # =========================================================
    def build_snapshot(
        self,
        *,
        runtime_mode: RuntimeMode | str,
        desk_mode: DeskMode | str,
        total_exposure: float,
        telegram_connected: bool,
        betfair_connected: bool,
        active_tables: int,
        recovery_tables: int,
        last_error: str,
        last_signal_at: str,
    ) -> RiskDeskSnapshot:
        with self._lock:
            return RiskDeskSnapshot(
                runtime_mode=self._safe_mode_value(runtime_mode),
                desk_mode=self._safe_desk_value(desk_mode),
                bankroll_start=float(self.bankroll_start),
                bankroll_current=float(self.bankroll_current),
                equity_peak=float(self.equity_peak),
                realized_pnl=float(self.realized_pnl),
                unrealized_pnl=float(self.unrealized_pnl),
                total_exposure=float(total_exposure or 0.0),
                drawdown_pct=float(self.drawdown_pct()),
                telegram_connected=bool(telegram_connected),
                betfair_connected=bool(betfair_connected),
                active_tables=int(active_tables or 0),
                recovery_tables=int(recovery_tables or 0),
                last_error=str(last_error or ""),
                last_signal_at=str(last_signal_at or ""),
                updated_at=self.updated_at,
            )

    def as_dict(self, snapshot: RiskDeskSnapshot) -> Dict[str, Any]:
        return {
            "mode": snapshot.runtime_mode,
            "runtime_mode": snapshot.runtime_mode,
            "desk_mode": snapshot.desk_mode,
            "bankroll_start": snapshot.bankroll_start,
            "bankroll_current": snapshot.bankroll_current,
            "equity_peak": snapshot.equity_peak,
            "realized_pnl": snapshot.realized_pnl,
            "unrealized_pnl": snapshot.unrealized_pnl,
            "total_exposure": snapshot.total_exposure,
            "drawdown_pct": snapshot.drawdown_pct,
            "telegram_connected": snapshot.telegram_connected,
            "betfair_connected": snapshot.betfair_connected,
            "active_tables": snapshot.active_tables,
            "recovery_tables": snapshot.recovery_tables,
            "last_error": snapshot.last_error,
            "last_signal_at": snapshot.last_signal_at,
            "updated_at": snapshot.updated_at,
        }

    def snapshot_dict(
        self,
        *,
        runtime_mode: RuntimeMode | str,
        desk_mode: DeskMode | str,
        total_exposure: float,
        telegram_connected: bool,
        betfair_connected: bool,
        active_tables: int,
        recovery_tables: int,
        last_error: str,
        last_signal_at: str,
    ) -> Dict[str, Any]:
        snap = self.build_snapshot(
            runtime_mode=runtime_mode,
            desk_mode=desk_mode,
            total_exposure=total_exposure,
            telegram_connected=telegram_connected,
            betfair_connected=betfair_connected,
            active_tables=active_tables,
            recovery_tables=recovery_tables,
            last_error=last_error,
            last_signal_at=last_signal_at,
        )
        return self.as_dict(snap)

    # =========================================================
    # SERIALIZATION
    # =========================================================
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "bankroll_start": float(self.bankroll_start),
                "bankroll_current": float(self.bankroll_current),
                "equity_peak": float(self.equity_peak),
                "realized_pnl": float(self.realized_pnl),
                "unrealized_pnl": float(self.unrealized_pnl),
                "updated_at": self.updated_at,
            }

    def load_from_dict(self, data: Dict[str, Any]) -> None:
        with self._lock:
            self.bankroll_start = float(data.get("bankroll_start", 0.0) or 0.0)
            self.bankroll_current = float(data.get("bankroll_current", 0.0) or 0.0)
            self.equity_peak = float(data.get("equity_peak", 0.0) or 0.0)
            self.realized_pnl = float(data.get("realized_pnl", 0.0) or 0.0)
            self.unrealized_pnl = float(data.get("unrealized_pnl", 0.0) or 0.0)
            self.updated_at = str(data.get("updated_at", datetime.utcnow().isoformat()))
            self._touch()

    # =========================================================
    # DEBUG / EXPORT
    # =========================================================
    def debug_snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "bankroll_start": self.bankroll_start,
                "bankroll_current": self.bankroll_current,
                "equity_peak": self.equity_peak,
                "realized_pnl": self.realized_pnl,
                "unrealized_pnl": self.unrealized_pnl,
                "drawdown_pct": self.drawdown_pct(),
                "equity_current": self.equity_current(),
                "updated_at": self.updated_at,
            }

    def snapshot_dataclass(
        self,
        *,
        runtime_mode: RuntimeMode | str,
        desk_mode: DeskMode | str,
        total_exposure: float,
        telegram_connected: bool,
        betfair_connected: bool,
        active_tables: int,
        recovery_tables: int,
        last_error: str,
        last_signal_at: str,
    ) -> Dict[str, Any]:
        snap = self.build_snapshot(
            runtime_mode=runtime_mode,
            desk_mode=desk_mode,
            total_exposure=total_exposure,
            telegram_connected=telegram_connected,
            betfair_connected=betfair_connected,
            active_tables=active_tables,
            recovery_tables=recovery_tables,
            last_error=last_error,
            last_signal_at=last_signal_at,
        )
        return asdict(snap)