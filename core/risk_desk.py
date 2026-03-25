from __future__ import annotations

from dataclasses import asdict
from datetime import datetime

from core.system_state import DeskMode, RuntimeSnapshot


class RiskDesk:
    def __init__(self):
        self.bankroll_current = 0.0
        self.equity_peak = 0.0
        self.realized_pnl = 0.0
        self.last_reset_at = datetime.utcnow().isoformat()

    def sync_bankroll(self, bankroll: float) -> None:
        bankroll = float(bankroll or 0.0)
        self.bankroll_current = bankroll
        if bankroll > self.equity_peak:
            self.equity_peak = bankroll

    def apply_closed_pnl(self, pnl: float) -> None:
        pnl = float(pnl or 0.0)
        self.realized_pnl += pnl
        self.bankroll_current += pnl
        if self.bankroll_current > self.equity_peak:
            self.equity_peak = self.bankroll_current

    def drawdown_pct(self) -> float:
        if self.equity_peak <= 0:
            return 0.0
        dd = (self.equity_peak - self.bankroll_current) / self.equity_peak * 100.0
        return max(0.0, dd)

    def reset_recovery_cycle(self) -> None:
        self.equity_peak = max(self.equity_peak, self.bankroll_current)
        self.last_reset_at = datetime.utcnow().isoformat()

    def build_snapshot(
        self,
        *,
        runtime_mode,
        desk_mode: DeskMode,
        total_exposure: float,
        telegram_connected: bool,
        betfair_connected: bool,
        active_tables: int,
        recovery_tables: int,
        last_error: str = "",
        last_signal_at: str = "",
    ) -> RuntimeSnapshot:
        bankroll = float(self.bankroll_current or 0.0)
        exposure = float(total_exposure or 0.0)
        exposure_pct = (exposure / bankroll * 100.0) if bankroll > 0 else 0.0

        return RuntimeSnapshot(
            mode=runtime_mode,
            desk_mode=desk_mode,
            bankroll_current=round(bankroll, 2),
            equity_peak=round(float(self.equity_peak or 0.0), 2),
            realized_pnl=round(float(self.realized_pnl or 0.0), 2),
            total_exposure=round(exposure, 2),
            total_exposure_pct=round(exposure_pct, 2),
            drawdown_pct=round(self.drawdown_pct(), 2),
            telegram_connected=bool(telegram_connected),
            betfair_connected=bool(betfair_connected),
            active_tables=int(active_tables),
            recovery_tables=int(recovery_tables),
            last_error=str(last_error or ""),
            last_signal_at=str(last_signal_at or ""),
        )

    def as_dict(self, snapshot: RuntimeSnapshot) -> dict:
        data = asdict(snapshot)
        data["mode"] = snapshot.mode.value
        data["desk_mode"] = snapshot.desk_mode.value
        return data
