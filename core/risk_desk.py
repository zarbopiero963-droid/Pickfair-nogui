from __future__ import annotations

from dataclasses import asdict, dataclass
import time
from datetime import datetime
from threading import RLock
from typing import Any, Dict

from core.system_state import DeskMode, RuntimeMode
from core.validators import finite_number


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
        # PFC-008 (decisione owner #426 c.6369802091): cassa complessiva verificata
        # (disponibile + impegnato) della STESSA lettura che ha dato `bankroll_current`.
        # E' la base dei cap percentuali di capacita'/esposizione; None = non verificata e le
        # nuove entrate sono negate (`cassa_non_verificata`), nessun ripiego.
        self.cash_equity_current = None
        self.cash_equity_reason = "CASH_NOT_READ"
        # Impegnato (|exposure|) della stessa lettura e istante monotonic d'inizio dell'ultima lettura applicata:
        # con le riserve del bot piazzate DOPO quella lettura danno l'esposizione che il cap totale deve vedere
        # (validators.desk_cash_basis). Una lettura iniziata prima dell'ultima applicata non la sovrascrive.
        self.cash_committed_exposure = 0.0
        self._cash_read_at = None

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
            # Nuovo disponibile senza la cassa della stessa lettura: la cassa precedente
            # non e' piu' verificata (`sync_cash_equity` la rifissa).
            self._invalidate_cash_locked("CASH_NOT_READ")

            if self.bankroll_start <= 0.0:
                self.bankroll_start = bankroll_current

            self.bankroll_current = bankroll_current

            if self.equity_peak <= 0.0:
                self.equity_peak = bankroll_current
            elif bankroll_current > self.equity_peak:
                self.equity_peak = bankroll_current

            self._touch()

    def _invalidate_cash_locked(self, reason: str) -> None:
        self.cash_equity_current = None
        self.cash_equity_reason = str(reason)
        self.cash_committed_exposure = 0.0

    def _invalidate_cash_with_barrier_locked(self, reason: str) -> None:
        """PFC-008 (audit PF-FINANCIAL-IMPACT-GATE-01): il conto e' cambiato fuori da una lettura (P&L realizzato,
        snapshot, ripristino). Oltre a invalidare la cassa, sposta la soglia di `read_at` ad adesso: una lettura
        iniziata prima di questo istante e applicata dopo non rimette in vigore la cassa precedente."""
        self._invalidate_cash_locked(reason)
        adesso = time.monotonic()
        if self._cash_read_at is None or self._cash_read_at < adesso:
            self._cash_read_at = adesso

    def sync_funds_reading(self, bankroll: Any, cash_equity: Any, available: Any, *, reason: str = "",
                           committed_exposure: Any = 0.0, read_at: Any = None) -> bool:
        """PFC-008 (rilievo Codex su 4ce44a7): bankroll e cassa della stessa lettura applicati in un'unica operazione.

        Il controllo di `read_at` viene PRIMA di toccare il bankroll: una lettura iniziata prima dell'ultima
        applicata e' ignorata per intero (False), cosi' non riscrive il disponibile ne' invalida la cassa piu'
        recente. `bankroll` None = disponibile invariato (si registra solo la cassa)."""
        with self._lock:
            iniziata, ultima = finite_number(read_at), self._cash_read_at
            if iniziata is not None and ultima is not None and iniziata < ultima:
                return False
            if bankroll is not None:
                self.sync_bankroll(bankroll)
            self.sync_cash_equity(cash_equity, available, reason=reason, committed_exposure=committed_exposure,
                                  read_at=read_at)
            return True

    def sync_cash_equity(self, cash_equity: Any, available: Any, *, reason: str = "",
                         committed_exposure: Any = 0.0, read_at: Any = None) -> None:
        """Registra la cassa verificata letta insieme al disponibile `available` (PFC-008).

        Vale solo se `available` e' il bankroll corrente (stessa lettura) e la cassa e'
        un numero finito > 0 non inferiore al disponibile; altrimenti resta non verificata.
        `reason` non vuoto = la verifica dello snapshot e' gia' fallita: cassa non verificata.
        `read_at` (monotonic d'inizio lettura): una lettura iniziata prima dell'ultima applicata
        arriva tardi e viene ignorata, cosi' non riporta in vita una cassa gia' superata."""
        with self._lock:
            started = finite_number(read_at)
            if started is not None and self._cash_read_at is not None and started < self._cash_read_at:
                return
            if started is not None:
                self._cash_read_at = started
            if reason:
                self._invalidate_cash_locked(reason)
                return
            cash = finite_number(cash_equity)
            avail = finite_number(available)
            committed = finite_number(committed_exposure)
            if avail is None or avail != self.bankroll_current:
                self._invalidate_cash_locked("CASH_READING_MISMATCH")
            elif cash is None or cash <= 0 or cash < avail or committed is None or committed < 0:
                self._invalidate_cash_locked("CASH_INCOHERENT")
            else:
                self.cash_equity_current = cash
                self.cash_equity_reason = ""
                self.cash_committed_exposure = committed

    def cash_basis(self) -> tuple:
        """PFC-008: ``(cassa, impegnato, read_at)`` della stessa lettura, letti insieme sotto lock.
        Cassa None = non verificata."""
        with self._lock:
            return self.cash_equity_current, self.cash_committed_exposure, self._cash_read_at

    def set_bankroll_start(self, bankroll_start: float) -> None:
        with self._lock:
            bankroll_start = float(bankroll_start or 0.0)
            self.bankroll_start = bankroll_start
            if self.bankroll_current <= 0.0:
                self.bankroll_current = bankroll_start
                self._invalidate_cash_locked("CASH_NOT_READ")  # PFC-008: bankroll cambiato senza lettura del conto
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
            self._invalidate_cash_with_barrier_locked("CASH_NOT_READ")  # PFC-008: P&L applicato senza lettura

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
            self._invalidate_cash_with_barrier_locked("CASH_NOT_READ")  # PFC-008: snapshot senza lettura del conto

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
            self._invalidate_cash_with_barrier_locked("CASH_NOT_READ")  # PFC-008: stato ripristinato, da rileggere
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