from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import List


class RiskProfile(Enum):
    CONSERVATIVE = "CONSERVATIVE"
    BALANCED = "BALANCED"
    AGGRESSIVE = "AGGRESSIVE"


class RuntimeMode(Enum):
    STOPPED = "STOPPED"
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    LOCKDOWN = "LOCKDOWN"


class DeskMode(Enum):
    NORMAL = "NORMAL"
    EXPANSION = "EXPANSION"
    DEFENSE = "DEFENSE"
    LOCKDOWN = "LOCKDOWN"


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
    monitored_chat_ids: List[int] = field(default_factory=list)


@dataclass
class RoserpinaConfig:
    target_profit_cycle_pct: float = 3.0
    max_single_bet_pct: float = 18.0
    max_total_exposure_pct: float = 35.0
    max_event_exposure_pct: float = 18.0
    max_daily_loss: float = 250.0
    max_drawdown_hard_stop_pct: float = 20.0
    max_open_exposure: float = 1000.0

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
    max_stake_abs: float = 10000.0
