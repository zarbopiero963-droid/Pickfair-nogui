from __future__ import annotations

from typing import Optional

from core.system_state import DeskMode, RiskProfile, RoserpinaConfig, SignalDecision, TableState


class RoserpinaMoneyManagement:
    def __init__(self, config: RoserpinaConfig):
        self.config = config

    def determine_desk_mode(
        self,
        *,
        bankroll_current: float,
        equity_peak: float,
    ) -> DeskMode:
        bankroll_current = float(bankroll_current or 0.0)
        equity_peak = float(equity_peak or 0.0)

        if equity_peak <= 0:
            return DeskMode.NORMAL

        drawdown_pct = max(0.0, (equity_peak - bankroll_current) / equity_peak * 100.0)
        profit_from_peak_base = max(0.0, (bankroll_current - equity_peak) / max(equity_peak, 1.0) * 100.0)

        if drawdown_pct >= self.config.lockdown_drawdown_pct:
            return DeskMode.LOCKDOWN
        if drawdown_pct >= self.config.defense_drawdown_pct:
            return DeskMode.DEFENSE
        if profit_from_peak_base >= self.config.expansion_profit_pct:
            return DeskMode.EXPANSION
        return DeskMode.NORMAL

    def _risk_multiplier(self) -> float:
        profile = RiskProfile(str(self.config.risk_profile))
        if profile == RiskProfile.CONSERVATIVE:
            return 0.8
        if profile == RiskProfile.AGGRESSIVE:
            return 1.2
        return 1.0

    def _desk_multiplier(self, desk_mode: DeskMode) -> float:
        if desk_mode == DeskMode.EXPANSION:
            return float(self.config.expansion_multiplier)
        if desk_mode == DeskMode.DEFENSE:
            return float(self.config.defense_multiplier)
        if desk_mode == DeskMode.LOCKDOWN:
            return 0.0
        return 1.0

    def _net_profit_per_unit(self, price: float, commission_pct: float) -> float:
        price = float(price or 0.0)
        if price <= 1.0:
            return 0.0
        gross = price - 1.0
        net = gross * (1.0 - float(commission_pct or 0.0) / 100.0)
        return max(0.0, net)

    def calculate(
        self,
        *,
        signal: dict,
        bankroll_current: float,
        equity_peak: float,
        current_total_exposure: float,
        event_current_exposure: float,
        table: Optional[TableState],
    ) -> SignalDecision:
        price = float(signal.get("price") or signal.get("odds") or 0.0)
        event_key = str(signal.get("event_key") or "")
        desk_mode = self.determine_desk_mode(
            bankroll_current=bankroll_current,
            equity_peak=equity_peak,
        )

        if desk_mode == DeskMode.LOCKDOWN:
            return SignalDecision(
                approved=False,
                reason="LOCKDOWN attivo",
                event_key=event_key,
                desk_mode=desk_mode,
            )

        if bankroll_current <= 0:
            return SignalDecision(
                approved=False,
                reason="Bankroll non valido",
                event_key=event_key,
                desk_mode=desk_mode,
            )

        net_per_unit = self._net_profit_per_unit(price, self.config.commission_pct)
        if net_per_unit <= 0:
            return SignalDecision(
                approved=False,
                reason="Quota non valida per Roserpina",
                event_key=event_key,
                desk_mode=desk_mode,
            )

        cycle_target = bankroll_current * (float(self.config.target_profit_cycle_pct) / 100.0)
        recovery_loss = float(getattr(table, "loss_amount", 0.0) or 0.0)
        adjusted_target = cycle_target + max(0.0, recovery_loss)

        raw_stake = adjusted_target / net_per_unit
        raw_stake *= self._risk_multiplier()
        raw_stake *= self._desk_multiplier(desk_mode)

        max_single = min(
            bankroll_current * (float(self.config.max_single_bet_pct) / 100.0),
            float(self.config.max_stake_abs),
        )
        max_total_allowed = bankroll_current * (float(self.config.max_total_exposure_pct) / 100.0)
        max_event_allowed = bankroll_current * (float(self.config.max_event_exposure_pct) / 100.0)

        remaining_global = max(0.0, max_total_allowed - current_total_exposure)
        remaining_event = max(0.0, max_event_allowed - event_current_exposure)
        hard_cap = min(max_single, remaining_global, remaining_event)

        if hard_cap <= 0:
            return SignalDecision(
                approved=False,
                reason="Capitale esposto al limite",
                event_key=event_key,
                desk_mode=desk_mode,
                current_exposure=current_total_exposure,
                new_total_exposure=current_total_exposure,
            )

        final_stake = min(raw_stake, hard_cap)

        if final_stake < float(self.config.min_stake):
            return SignalDecision(
                approved=False,
                reason="Stake sotto minimo operativo",
                event_key=event_key,
                desk_mode=desk_mode,
                recommended_stake=round(final_stake, 2),
                requested_target_profit=round(cycle_target, 2),
                adjusted_target_profit=round(adjusted_target, 2),
                current_exposure=round(current_total_exposure, 2),
                new_total_exposure=round(current_total_exposure + final_stake, 2),
            )

        reason = "APPROVED"
        if final_stake + 1e-9 < raw_stake:
            reason = "APPROVED_REDUCED_BY_RISK"

        return SignalDecision(
            approved=True,
            reason=reason,
            event_key=event_key,
            table_id=getattr(table, "table_id", None),
            desk_mode=desk_mode,
            recommended_stake=round(final_stake, 2),
            requested_target_profit=round(cycle_target, 2),
            adjusted_target_profit=round(adjusted_target, 2),
            current_exposure=round(current_total_exposure, 2),
            new_total_exposure=round(current_total_exposure + final_stake, 2),
            metadata={
                "price": price,
                "net_profit_per_unit": round(net_per_unit, 6),
                "raw_stake": round(raw_stake, 2),
                "max_single": round(max_single, 2),
                "remaining_global": round(remaining_global, 2),
                "remaining_event": round(remaining_event, 2),
                "recovery_loss": round(recovery_loss, 2),
            },
        )
