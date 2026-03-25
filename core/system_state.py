from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional


class RuntimeMode(str, Enum):
    STOPPED = "STOPPED"
    READY = "READY"
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    LOCKDOWN = "LOCKDOWN"


class DeskMode(str, Enum):
    NORMAL = "NORMAL"
    EXPANSION = "EXPANSION"
    DEFENSE = "DEFENSE"
    LOCKDOWN = "LOCKDOWN"


class RiskProfile(str, Enum):
    CONSERVATIVE = "CONSERVATIVE"
    BALANCED = "BALANCED"
    AGGRESSIVE = "AGGRESSIVE"


class TableStatus(str, Enum):
    FREE = "FREE"
    ACTIVE = "ACTIVE"
    RECOVERY = "RECOVERY"
    LOCKED = "LOCKED"


@dataclass
class BetfairConfig:
    username: str = ""
    app_key: str = ""
    certificate: str = ""
    private_key: str = ""


@dataclass
class TelegramRuntimeConfig:
    api_id: int = 0
    api_hash: str = ""
    session_string: str = ""
    phone_number: str = ""
    enabled: bool = False
    auto_bet: bool = False
    require_confirmation: bool = True
    auto_stake: float = 1.0
    monitored_chat_ids: list[int] = field(default_factory=list)


@dataclass
class RoserpinaConfig:
    target_profit_cycle_pct: float = 3.0
    max_single_bet_pct: float = 18.0
    max_total_exposure_pct: float = 35.0
    max_event_exposure_pct: float = 18.0
    auto_reset_drawdown_pct: float = 15.0
    defense_drawdown_pct: float = 7.5
    lockdown_drawdown_pct: float = 20.0
    expansion_profit_pct: float = 5.0
    expansion_multiplier: float = 1.10
    defense_multiplier: float = 0.80
    risk_profile: RiskProfile = RiskProfile.BALANCED
    table_count: int = 5
    max_recovery_tables: int = 2
    allow_recovery: bool = True
    anti_duplication_enabled: bool = True
    commission_pct: float = 4.5
    min_stake: float = 0.10
    max_stake_abs: float = 10_000.0


@dataclass
class TableState:
    table_id: int
    status: TableStatus = TableStatus.FREE
    loss_amount: float = 0.0
    current_exposure: float = 0.0
    current_event_key: str = ""
    market_id: str = ""
    selection_id: Optional[int] = None
    opened_at_ts: Optional[float] = None
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RuntimeSnapshot:
    mode: RuntimeMode = RuntimeMode.STOPPED
    desk_mode: DeskMode = DeskMode.NORMAL
    bankroll_current: float = 0.0
    equity_peak: float = 0.0
    realized_pnl: float = 0.0
    total_exposure: float = 0.0
    total_exposure_pct: float = 0.0
    drawdown_pct: float = 0.0
    telegram_connected: bool = False
    betfair_connected: bool = False
    active_tables: int = 0
    recovery_tables: int = 0
    last_error: str = ""
    last_signal_at: str = ""


@dataclass
class SignalDecision:
    approved: bool
    reason: str
    event_key: str = ""
    table_id: Optional[int] = None
    desk_mode: DeskMode = DeskMode.NORMAL
    recommended_stake: float = 0.0
    requested_target_profit: float = 0.0
    adjusted_target_profit: float = 0.0
    current_exposure: float = 0.0
    new_total_exposure: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)
