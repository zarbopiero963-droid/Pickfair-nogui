from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional


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
    bot_token: str = ""
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
    max_daily_loss: Optional[float] = None
    max_drawdown_hard_stop_pct: Optional[float] = None
    max_open_exposure: Optional[float] = None

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

    # Book % (over-round) thresholds — editabili da GUI, applicati come gate reale
    # al submit dutching (controllers/dutching_controller.precheck). Default
    # allineati a trading_config.BOOK_WARNING/BOOK_BLOCK (fonte-dato del fallback
    # fail-safe lato loader/enforcement).
    book_warning: float = 105.0
    book_block: float = 110.0

    # Liquidity guard — editabile da GUI, applicato come gate reale al submit
    # dutching. Default allineati a trading_config.LIQUIDITY_*. Semantica
    # (controllers/dutching_controller): guard_enabled=False disattiva il gate;
    # required = stake * multiplier; blocca se available < max(min_absolute,
    # required); warning_only=True => avvisa invece di bloccare; FAIL-OPEN se la
    # liquidita' non e' ottenibile (book assente).
    liquidity_guard_enabled: bool = True
    liquidity_multiplier: float = 3.0
    min_liquidity_absolute: float = 50.0
    # Default OPT-IN (#383): il blocco liquidita' parte in AVVISO; l'owner lo arma
    # a blocco reale mettendo False dalla GUI.
    liquidity_warning_only: bool = True

    # Quota minima di strategia (floor editabile) applicata al submit dutching,
    # SOPRA il minimo Betfair inviolabile 1.01. Default = trading_config.MIN_PRICE.
    min_price: float = 1.02

    # Cap importo potenziale PER GAMBA (G5), editabile da GUI, applicato al submit
    # dutching e al bet manuale. Default = trading_config.MAX_WIN. Semantica
    # (controllers/dutching_controller): BACK = payout stake*price; LAY = liability
    # stake*(price-1) (il RISCHIO, decisione owner su #393); si valuta la MAX sulle
    # gambe (esiti mutuamente esclusivi).
    # Default OPT-IN (#383-style): max_win_warning_only=True => il cap parte in
    # AVVISO; l'owner lo arma a BLOCCO reale mettendo False dalla GUI. FAIL-SAFE:
    # config assente/corrotta ricade su trading_config.MAX_WIN (mai disattiva il cap).
    max_win: float = 10000.0
    max_win_warning_only: bool = True

    # Soglia WARNING (non blocca mai) sull'esposizione della singola operazione
    # dutching come % del bankroll (G5). Scala PERCENTUALE 0-100 (come gli altri
    # max_*_pct); default 30.0 = trading_config.MAX_STAKE_PCT (0.30). Semantica
    # (controllers/dutching_controller.precheck): se batch_exposure > bankroll*30%
    # => stake_pct_warning nel risultato, MAI un blocco. Distinto dal blocco
    # cumulativo max_total_exposure_pct (35%). FAIL-OPEN su bankroll<=0.
    max_stake_pct: float = 30.0

    # Grace period OPT-IN prima di instradare l'auto-green (cashout), G5.
    # auto_green_delay_enabled=False (default) => NESSUN cambiamento di
    # comportamento (route immediata come oggi); l'owner arma il ritardo dalla
    # GUI. auto_green_delay_sec default 2.5 = trading_config.AUTO_GREEN_DELAY_SEC.
    # Semantica (core/runtime_controller._route_cashout_signal): se abilitato,
    # l'intera route del cashout (book + calcolo green-up + publish) e' differita
    # di N secondi su un threading.Timer (NON blocca il bus) => prezzo ricalcolato
    # FRESCO dopo l'attesa. FAIL-SAFE su sec assente/non-finito/<=0 => costante;
    # clamp a [0, 30]s lato runtime.
    auto_green_delay_enabled: bool = False
    auto_green_delay_sec: float = 2.5
