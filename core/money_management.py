from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from core.system_state import DeskMode, RiskProfile, RoserpinaConfig
from core.type_helpers import safe_float


@dataclass
class MoneyManagementDecision:
    approved: bool
    recommended_stake: float
    desk_mode: DeskMode
    reason: str
    table_id: Optional[int]
    metadata: Dict[str, Any] = field(default_factory=dict)


class RoserpinaMoneyManagement:
    """
    Money management Roserpina Hedge AI.

    Obiettivi:
    - calcolare stake target-based
    - rispettare max single bet / max total exposure / max event exposure
    - supportare tavoli recovery indipendenti
    - adattare stake in base a desk mode:
        NORMAL / EXPANSION / DEFENSE / LOCKDOWN
    """

    def __init__(self, config: RoserpinaConfig):
        self.config = config

    # =========================================================
    # SAFE HELPERS
    # =========================================================
    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        return safe_float(value, default)

    def _clamp(self, value: float, min_value: float, max_value: float) -> float:
        return max(min_value, min(float(value), max_value))

    def _target_profit_eur(self, bankroll_current: float) -> float:
        bankroll_current = self._safe_float(bankroll_current, 0.0)
        pct = self._safe_float(self.config.target_profit_cycle_pct, 0.0)
        return max(0.0, bankroll_current * (pct / 100.0))

    def _max_single_stake_abs(self, bankroll_current: float) -> float:
        bankroll_current = self._safe_float(bankroll_current, 0.0)
        pct_cap = bankroll_current * (self._safe_float(self.config.max_single_bet_pct, 0.0) / 100.0)
        abs_cap = self._safe_float(self.config.max_stake_abs, 0.0)
        if abs_cap > 0:
            return max(0.0, min(pct_cap, abs_cap))
        return max(0.0, pct_cap)

    def _max_total_exposure_abs(self, bankroll_current: float) -> float:
        bankroll_current = self._safe_float(bankroll_current, 0.0)
        return max(
            0.0,
            bankroll_current * (self._safe_float(self.config.max_total_exposure_pct, 0.0) / 100.0),
        )

    def _max_event_exposure_abs(self, bankroll_current: float) -> float:
        bankroll_current = self._safe_float(bankroll_current, 0.0)
        return max(
            0.0,
            bankroll_current * (self._safe_float(self.config.max_event_exposure_pct, 0.0) / 100.0),
        )

    # =========================================================
    # DESK MODE
    # =========================================================
    def determine_desk_mode(
        self,
        *,
        bankroll_current: float,
        equity_peak: float,
    ) -> DeskMode:
        bankroll_current = self._safe_float(bankroll_current, 0.0)
        equity_peak = self._safe_float(equity_peak, 0.0)

        if equity_peak <= 0.0:
            return DeskMode.NORMAL

        drawdown_pct = 0.0
        if bankroll_current < equity_peak:
            drawdown_pct = ((equity_peak - bankroll_current) / equity_peak) * 100.0

        if drawdown_pct >= self._safe_float(self.config.lockdown_drawdown_pct, 20.0):
            return DeskMode.LOCKDOWN

        if drawdown_pct >= self._safe_float(self.config.defense_drawdown_pct, 7.5):
            return DeskMode.DEFENSE

        growth_pct = 0.0
        if bankroll_current > 0 and equity_peak > 0 and bankroll_current >= equity_peak:
            base = max(bankroll_current, self._safe_float(self.config.min_stake, 0.10))
            # espansione quando siamo sopra il bankroll iniziale/peak e non in drawdown
            growth_pct = ((equity_peak - bankroll_current) / base) * -100.0 if bankroll_current > equity_peak else 0.0

        if bankroll_current >= equity_peak and self._safe_float(self.config.expansion_profit_pct, 5.0) <= max(0.0, growth_pct):
            return DeskMode.EXPANSION

        return DeskMode.NORMAL

    def _risk_profile_multiplier(self) -> float:
        profile = self.config.risk_profile
        if profile == RiskProfile.CONSERVATIVE:
            return 0.80
        if profile == RiskProfile.AGGRESSIVE:
            return 1.20
        return 1.00

    def _desk_mode_multiplier(self, desk_mode: DeskMode) -> float:
        if desk_mode == DeskMode.EXPANSION:
            return self._safe_float(self.config.expansion_multiplier, 1.10)
        if desk_mode == DeskMode.DEFENSE:
            return self._safe_float(self.config.defense_multiplier, 0.80)
        if desk_mode == DeskMode.LOCKDOWN:
            return 0.0
        return 1.0

    # =========================================================
    # BASE STAKE
    # =========================================================
    def _extract_table_loss(self, table: Any) -> float:
        if table is None:
            return 0.0

        if isinstance(table, dict):
            return self._safe_float(
                table.get("loss_amount", table.get("loss", 0.0)),
                0.0,
            )

        for attr in ("loss_amount", "loss", "current_loss"):
            if hasattr(table, attr):
                return self._safe_float(getattr(table, attr), 0.0)

        return 0.0

    def _extract_table_id(self, table: Any) -> Optional[int]:
        if table is None:
            return None

        if isinstance(table, dict):
            value = table.get("table_id", table.get("id"))
            try:
                return int(value)
            except Exception:
                return None

        for attr in ("table_id", "id"):
            if hasattr(table, attr):
                try:
                    return int(getattr(table, attr))
                except Exception:
                    return None

        return None

    def _extract_table_recovery_state(self, table: Any) -> bool:
        if table is None:
            return False

        if isinstance(table, dict):
            return bool(table.get("in_recovery", False))

        if hasattr(table, "in_recovery"):
            try:
                return bool(getattr(table, "in_recovery"))
            except Exception:
                return False

        return False

    def _calculate_base_stake(
        self,
        *,
        price: float,
        bankroll_current: float,
        table_loss: float,
    ) -> float:
        price = self._safe_float(price, 0.0)
        bankroll_current = self._safe_float(bankroll_current, 0.0)
        table_loss = self._safe_float(table_loss, 0.0)

        if price <= 1.0:
            return 0.0

        target_profit = self._target_profit_eur(bankroll_current)
        to_recover = target_profit + max(0.0, table_loss)

        stake = to_recover / (price - 1.0)
        return max(0.0, stake)

    # =========================================================
    # MAIN DECISION
    # =========================================================
    def calculate(
        self,
        *,
        signal: Dict[str, Any],
        bankroll_current: float,
        equity_peak: float,
        current_total_exposure: float,
        event_current_exposure: float,
        table: Any,
    ) -> MoneyManagementDecision:
        bankroll_current = self._safe_float(bankroll_current, 0.0)
        equity_peak = self._safe_float(equity_peak, bankroll_current)
        current_total_exposure = self._safe_float(current_total_exposure, 0.0)
        event_current_exposure = self._safe_float(event_current_exposure, 0.0)

        table_id = self._extract_table_id(table)
        table_loss = self._extract_table_loss(table)
        in_recovery = self._extract_table_recovery_state(table)

        price = self._safe_float(
            signal.get("price", signal.get("odds")),
            0.0,
        )

        if bankroll_current <= 0.0:
            return MoneyManagementDecision(
                approved=False,
                recommended_stake=0.0,
                desk_mode=DeskMode.LOCKDOWN,
                reason="bankroll_non_valido",
                table_id=table_id,
                metadata={"bankroll_current": bankroll_current},
            )

        if price <= 1.0:
            return MoneyManagementDecision(
                approved=False,
                recommended_stake=0.0,
                desk_mode=DeskMode.NORMAL,
                reason="quota_non_valida",
                table_id=table_id,
                metadata={"price": price},
            )

        desk_mode = self.determine_desk_mode(
            bankroll_current=bankroll_current,
            equity_peak=equity_peak,
        )

        if desk_mode == DeskMode.LOCKDOWN:
            return MoneyManagementDecision(
                approved=False,
                recommended_stake=0.0,
                desk_mode=desk_mode,
                reason="desk_lockdown",
                table_id=table_id,
                metadata={
                    "bankroll_current": bankroll_current,
                    "equity_peak": equity_peak,
                },
            )

        if in_recovery and not bool(self.config.allow_recovery):
            return MoneyManagementDecision(
                approved=False,
                recommended_stake=0.0,
                desk_mode=desk_mode,
                reason="recovery_non_consentito",
                table_id=table_id,
                metadata={"in_recovery": in_recovery},
            )

        base_stake = self._calculate_base_stake(
            price=price,
            bankroll_current=bankroll_current,
            table_loss=table_loss,
        )

        risk_mult = self._risk_profile_multiplier()
        desk_mult = self._desk_mode_multiplier(desk_mode)

        recommended = base_stake * risk_mult * desk_mult

        min_stake = self._safe_float(self.config.min_stake, 0.10)
        max_single = self._max_single_stake_abs(bankroll_current)
        max_total = self._max_total_exposure_abs(bankroll_current)
        max_event = self._max_event_exposure_abs(bankroll_current)

        if recommended <= 0.0:
            return MoneyManagementDecision(
                approved=False,
                recommended_stake=0.0,
                desk_mode=desk_mode,
                reason="stake_calcolato_non_valido",
                table_id=table_id,
                metadata={
                    "base_stake": base_stake,
                    "risk_mult": risk_mult,
                    "desk_mult": desk_mult,
                },
            )

        # clamp tecnico
        recommended = self._clamp(recommended, min_stake, max(max_single, min_stake))

        if recommended > max_single:
            return MoneyManagementDecision(
                approved=False,
                recommended_stake=max_single,
                desk_mode=desk_mode,
                reason="supera_max_single_bet",
                table_id=table_id,
                metadata={
                    "recommended": recommended,
                    "max_single": max_single,
                },
            )

        if current_total_exposure + recommended > max_total:
            return MoneyManagementDecision(
                approved=False,
                recommended_stake=0.0,
                desk_mode=desk_mode,
                reason="supera_max_total_exposure",
                table_id=table_id,
                metadata={
                    "current_total_exposure": current_total_exposure,
                    "recommended": recommended,
                    "max_total": max_total,
                },
            )

        if event_current_exposure + recommended > max_event:
            return MoneyManagementDecision(
                approved=False,
                recommended_stake=0.0,
                desk_mode=desk_mode,
                reason="supera_max_event_exposure",
                table_id=table_id,
                metadata={
                    "event_current_exposure": event_current_exposure,
                    "recommended": recommended,
                    "max_event": max_event,
                },
            )

        return MoneyManagementDecision(
            approved=True,
            recommended_stake=float(recommended),
            desk_mode=desk_mode,
            reason="approved",
            table_id=table_id,
            metadata={
                "price": price,
                "target_profit": self._target_profit_eur(bankroll_current),
                "table_loss": table_loss,
                "base_stake": base_stake,
                "risk_mult": risk_mult,
                "desk_mult": desk_mult,
                "max_single": max_single,
                "max_total": max_total,
                "max_event": max_event,
                "in_recovery": in_recovery,
            },
        )