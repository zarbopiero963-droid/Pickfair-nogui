"""
AI Guardrail - Protezione automatica per trading AI
"""

import logging
import threading
import time
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class GuardrailLevel(Enum):
    NORMAL = "normal"
    WARNING = "warning"
    BLOCKED = "blocked"


class BlockReason(Enum):
    MARKET_NOT_READY = "market_not_ready"
    HIGH_VOLATILITY = "high_volatility"
    OVERTRADE_PROTECTION = "overtrade_protection"
    CONSECUTIVE_ERRORS = "consecutive_errors"
    INSUFFICIENT_DATA = "insufficient_data"
    GRACE_PERIOD = "grace_period"
    MANUAL_BLOCK = "manual_block"


@dataclass
class GuardrailConfig:
    auto_green_grace_sec: float = 3.0
    min_tick_count: int = 10
    max_volatility: float = 0.8
    max_orders_per_minute: int = 10
    consecutive_error_limit: int = 3
    cooldown_after_error_sec: float = 30.0
    min_wom_confidence: float = 0.3
    dutching_ready_market_types: set[str] = field(
        default_factory=lambda: {
            "MATCH_ODDS",
            "WINNER",
            "OVER_UNDER_25",
            "OVER_UNDER_35",
            "CORRECT_SCORE",
            "HALF_TIME",
            "BOTH_TEAMS_TO_SCORE",
        }
    )


@dataclass
class OrderRecord:
    timestamp: float
    market_id: str
    selection_id: int
    side: str
    stake: float
    success: bool = True


@dataclass
class GuardrailState:
    level: GuardrailLevel = GuardrailLevel.NORMAL
    block_reasons: list[BlockReason] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    last_order_time: float = 0.0
    consecutive_errors: int = 0
    blocked_until: float = 0.0
    order_history: list[OrderRecord] = field(default_factory=list)


class AIGuardrail:
    def __init__(self, config: GuardrailConfig | None = None):
        self.config = config or GuardrailConfig()
        self.state = GuardrailState()
        self._lock = threading.RLock()
        self._pending_auto_green: dict[str, float] = {}

    def check_market_ready(self, market_type: str) -> tuple[bool, BlockReason | None]:
        if market_type in self.config.dutching_ready_market_types:
            return True, None
        return False, BlockReason.MARKET_NOT_READY

    def check_wom_data(self, tick_count: int, confidence: float) -> tuple[bool, BlockReason | None]:
        if tick_count < self.config.min_tick_count:
            return False, BlockReason.INSUFFICIENT_DATA
        if confidence < self.config.min_wom_confidence:
            return False, BlockReason.INSUFFICIENT_DATA
        return True, None

    def check_volatility(self, volatility: float) -> tuple[bool, BlockReason | None]:
        if volatility > self.config.max_volatility:
            return False, BlockReason.HIGH_VOLATILITY
        return True, None

    def check_auto_green_grace(self, bet_id: str) -> tuple[bool, float]:
        with self._lock:
            if bet_id not in self._pending_auto_green:
                return True, 0.0

            elapsed = time.time() - self._pending_auto_green[bet_id]
            remaining = self.config.auto_green_grace_sec - elapsed

            if remaining <= 0:
                del self._pending_auto_green[bet_id]
                return True, 0.0

            return False, remaining

    def register_order_for_auto_green(self, bet_id: str, placed_at: float | None = None) -> None:
        with self._lock:
            self._pending_auto_green[bet_id] = placed_at or time.time()

    def check_order_rate(self) -> tuple[bool, BlockReason | None]:
        with self._lock:
            cutoff = time.time() - 60.0
            recent_orders = [
                order for order in self.state.order_history if order.timestamp > cutoff
            ]
            if len(recent_orders) >= self.config.max_orders_per_minute:
                return False, BlockReason.OVERTRADE_PROTECTION
            return True, None

    def check_error_state(self) -> tuple[bool, BlockReason | None]:
        with self._lock:
            if self.state.consecutive_errors >= self.config.consecutive_error_limit:
                if time.time() < self.state.blocked_until:
                    return False, BlockReason.CONSECUTIVE_ERRORS
                self.state.consecutive_errors = 0
                self.state.blocked_until = 0.0
            return True, None

    def record_order(
        self,
        market_id: str,
        selection_id: int,
        side: str,
        stake: float,
        success: bool = True,
    ) -> None:
        with self._lock:
            self.state.order_history.append(
                OrderRecord(
                    timestamp=time.time(),
                    market_id=market_id,
                    selection_id=selection_id,
                    side=side,
                    stake=stake,
                    success=success,
                )
            )

            if len(self.state.order_history) > 100:
                self.state.order_history = self.state.order_history[-100:]

            self.state.last_order_time = time.time()

            if success:
                self.state.consecutive_errors = 0
            else:
                self.state.consecutive_errors += 1
                if self.state.consecutive_errors >= self.config.consecutive_error_limit:
                    self.state.blocked_until = time.time() + self.config.cooldown_after_error_sec

    def full_check(
        self,
        market_type: str,
        tick_count: int = 0,
        wom_confidence: float = 0.5,
        volatility: float = 0.0,
    ) -> dict:
        with self._lock:
            reasons: list[BlockReason] = []
            warnings: list[str] = []

            ok, reason = self.check_market_ready(market_type)
            if not ok and reason is not None:
                reasons.append(reason)

            ok, reason = self.check_wom_data(tick_count, wom_confidence)
            if not ok and reason is not None:
                reasons.append(reason)

            ok, reason = self.check_volatility(volatility)
            if not ok and reason is not None:
                reasons.append(reason)

            ok, reason = self.check_order_rate()
            if not ok and reason is not None:
                reasons.append(reason)

            ok, reason = self.check_error_state()
            if not ok and reason is not None:
                reasons.append(reason)

            if reasons:
                level = GuardrailLevel.BLOCKED
                can_proceed = False
            elif warnings:
                level = GuardrailLevel.WARNING
                can_proceed = True
            else:
                level = GuardrailLevel.NORMAL
                can_proceed = True

            self.state.level = level
            self.state.block_reasons = reasons
            self.state.warnings = warnings

            return {
                "can_proceed": can_proceed,
                "level": level.value,
                "reasons": [reason.value for reason in reasons],
                "warnings": warnings,
                "blocked_until": (
                    self.state.blocked_until if self.state.blocked_until > time.time() else 0
                ),
            }

    def get_auto_green_delay(self, bet_id: str) -> float:
        can_green, remaining = self.check_auto_green_grace(bet_id)
        return remaining if not can_green else 0.0

    def reset(self) -> None:
        with self._lock:
            self.state = GuardrailState()
            self._pending_auto_green.clear()

    def get_status(self) -> dict:
        with self._lock:
            now = time.time()
            recent_orders = [
                order for order in self.state.order_history if order.timestamp > now - 60.0
            ]
            return {
                "level": self.state.level.value,
                "consecutive_errors": self.state.consecutive_errors,
                "orders_last_minute": len(recent_orders),
                "pending_auto_green": len(self._pending_auto_green),
                "blocked_until": self.state.blocked_until if self.state.blocked_until > now else 0,
                "warnings": self.state.warnings,
                "block_reasons": [reason.value for reason in self.state.block_reasons],
            }


_global_guardrail: AIGuardrail | None = None


def get_guardrail() -> AIGuardrail:
    global _global_guardrail
    if _global_guardrail is None:
        _global_guardrail = AIGuardrail()
    return _global_guardrail
