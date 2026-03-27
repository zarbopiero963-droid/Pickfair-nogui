from __future__ import annotations

"""
Safety Logger - Logging automatico per errori di sicurezza

Registra automaticamente su file .log tutti gli eventi critici:
- MixedDutchingError
- AI bloccata per mercato non compatibile
- Auto-green negato
- Safe mode
- Profit validation
- Market validation
- Liquidity block / warning

I log sono salvati in:
- Windows: %APPDATA%/Pickfair/logs/
- Linux/macOS: ~/.config/Pickfair/logs/
"""

import logging
import os
import threading
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional


class SafetyEventType(Enum):
    MIXED_DUTCHING_ERROR = "MIXED_DUTCH_ERR"
    AI_BLOCKED_MARKET = "AI_BLOCKED"
    AUTO_GREEN_DENIED = "AUTO_GREEN_DENIED"
    SAFE_MODE_TRIGGERED = "SAFE_MODE"
    PROFIT_VALIDATION_FAILED = "PROFIT_VAL_FAIL"
    MARKET_VALIDATION_FAILED = "MARKET_VAL_FAIL"
    LIQUIDITY_BLOCK = "LIQ_BLOCK"
    LIQUIDITY_WARNING = "LIQ_WARN"


class SafetyLogger:
    """
    Logger dedicato per eventi di sicurezza del trading.

    Thread-safe, con rotazione giornaliera automatica.
    """

    _instance: Optional["SafetyLogger"] = None
    _instance_lock = threading.Lock()

    def __new__(cls) -> "SafetyLogger":
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if getattr(self, "_initialized", False):
            return

        self._initialized = True
        self._log_lock = threading.RLock()
        self._log_dir = self._get_log_directory()
        self._current_date: Optional[str] = None
        self._file_handler: Optional[logging.FileHandler] = None

        logger_name = "pickfair.safety"
        self._logger = logging.getLogger(logger_name)
        self._logger.setLevel(logging.INFO)
        self._logger.propagate = False

        self._setup_logger()

    # =========================================================
    # PATH / SETUP
    # =========================================================
    def _get_log_directory(self) -> Path:
        if os.name == "nt":
            base = Path(os.environ.get("APPDATA", "."))
        else:
            base = Path.home() / ".config"

        log_dir = base / "Pickfair" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        return log_dir

    def _get_log_filename(self) -> str:
        return f"safety_{datetime.now().strftime('%Y%m%d')}.log"

    def _setup_logger(self) -> None:
        today = datetime.now().strftime("%Y%m%d")

        if self._current_date == today and self._file_handler is not None:
            return

        if self._file_handler is not None:
            try:
                self._logger.removeHandler(self._file_handler)
            except Exception:
                pass
            try:
                self._file_handler.close()
            except Exception:
                pass

        log_path = self._log_dir / self._get_log_filename()
        handler = logging.FileHandler(log_path, encoding="utf-8")
        handler.setLevel(logging.INFO)

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)

        self._file_handler = handler
        self._logger.addHandler(handler)
        self._current_date = today

    def _rotate_if_needed(self) -> None:
        today = datetime.now().strftime("%Y%m%d")
        if self._current_date != today:
            self._setup_logger()

    # =========================================================
    # CORE LOG
    # =========================================================
    def log_event(
        self,
        event_type: SafetyEventType,
        message: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self._log_lock:
            self._rotate_if_needed()

            detail_str = ""
            if details:
                parts = []
                for key, value in details.items():
                    try:
                        parts.append(f"{key}={value}")
                    except Exception:
                        parts.append(f"{key}=<unprintable>")
                detail_str = " | " + " | ".join(parts)

            log_line = f"[{event_type.value}] {message}{detail_str}"
            self._logger.info(log_line)

    # =========================================================
    # SPECIALIZED HELPERS
    # =========================================================
    def log_mixed_dutching_error(
        self,
        error_message: str,
        market_id: Optional[str] = None,
        stake: Optional[float] = None,
        selections_count: Optional[int] = None,
    ) -> None:
        self.log_event(
            SafetyEventType.MIXED_DUTCHING_ERROR,
            error_message,
            {
                "market_id": market_id or "N/A",
                "stake": f"EUR{float(stake):.2f}" if stake is not None else "N/A",
                "selections": int(selections_count or 0),
            },
        )

    def log_ai_blocked(
        self,
        market_type: str,
        market_id: Optional[str] = None,
        reason: str = "Mercato non compatibile con AI Mixed",
    ) -> None:
        self.log_event(
            SafetyEventType.AI_BLOCKED_MARKET,
            reason,
            {
                "market_type": market_type,
                "market_id": market_id or "N/A",
            },
        )

    def log_auto_green_denied(
        self,
        reason: str,
        order_id: Optional[str] = None,
        market_status: Optional[str] = None,
        elapsed_seconds: Optional[float] = None,
    ) -> None:
        details = {"order_id": order_id or "N/A"}
        if market_status:
            details["market_status"] = market_status
        if elapsed_seconds is not None:
            details["elapsed_sec"] = f"{float(elapsed_seconds):.2f}"

        self.log_event(
            SafetyEventType.AUTO_GREEN_DENIED,
            reason,
            details,
        )

    def log_safe_mode_triggered(self, consecutive_errors: int, last_error: str) -> None:
        self.log_event(
            SafetyEventType.SAFE_MODE_TRIGGERED,
            "Safe Mode attivato - AI disabilitata",
            {
                "consecutive_errors": int(consecutive_errors),
                "last_error": last_error,
            },
        )

    def log_profit_validation_failed(
        self,
        variance: float,
        threshold: float,
        market_id: Optional[str] = None,
    ) -> None:
        self.log_event(
            SafetyEventType.PROFIT_VALIDATION_FAILED,
            f"Varianza profitto {float(variance):.2f} supera soglia {float(threshold):.2f}",
            {
                "market_id": market_id or "N/A",
            },
        )

    def log_market_validation_failed(
        self,
        market_type: str,
        market_id: Optional[str] = None,
    ) -> None:
        self.log_event(
            SafetyEventType.MARKET_VALIDATION_FAILED,
            f"Mercato {market_type} non valido per dutching",
            {
                "market_id": market_id or "N/A",
            },
        )

    def log_liquidity_block(
        self,
        market_id: str,
        selection_id: int,
        runner_name: str,
        stake: float,
        available_liquidity: float,
        required_liquidity: float,
        side: str,
        reason: str = "INSUFFICIENT_LIQUIDITY",
        simulation: bool = False,
    ) -> None:
        self.log_event(
            SafetyEventType.LIQUIDITY_BLOCK,
            f"Ordine bloccato - {runner_name}",
            {
                "market_id": market_id,
                "selection_id": int(selection_id),
                "stake": f"EUR{float(stake):.2f}",
                "available": f"EUR{float(available_liquidity):.2f}",
                "required": f"EUR{float(required_liquidity):.2f}",
                "side": side,
                "reason": reason,
                "simulation": bool(simulation),
            },
        )

    def log_liquidity_warning(
        self,
        market_id: str,
        selection_id: int,
        runner_name: str,
        stake: float,
        available_liquidity: float,
        required_liquidity: float,
        side: str,
        simulation: bool = False,
    ) -> None:
        self.log_event(
            SafetyEventType.LIQUIDITY_WARNING,
            f"Warning liquidita - {runner_name}",
            {
                "market_id": market_id,
                "selection_id": int(selection_id),
                "stake": f"EUR{float(stake):.2f}",
                "available": f"EUR{float(available_liquidity):.2f}",
                "required": f"EUR{float(required_liquidity):.2f}",
                "side": side,
                "simulation": bool(simulation),
            },
        )

    # =========================================================
    # INFO
    # =========================================================
    def get_log_path(self) -> Path:
        return self._log_dir / self._get_log_filename()


_safety_logger: Optional[SafetyLogger] = None


def get_safety_logger() -> SafetyLogger:
    global _safety_logger
    if _safety_logger is None:
        _safety_logger = SafetyLogger()
    return _safety_logger


class LiquidityStatus:
    OK = "OK"
    BORDERLINE = "BORDERLINE"
    DRY = "DRY"


def evaluate_runner_liquidity(
    stake: float,
    available_liquidity: float,
    side: str = "BACK",
    price: float = 1.0,
    multiplier: Optional[float] = None,
    min_absolute: Optional[float] = None,
    warning_only: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    Valuta lo status liquidita di un runner per UI indicator.

    Allineata a trading_config quando disponibile.
    """
    try:
        from trading_config import (
            LIQUIDITY_MULTIPLIER,
            LIQUIDITY_WARNING_ONLY,
            MIN_LIQUIDITY_ABSOLUTE,
        )
    except Exception:
        LIQUIDITY_MULTIPLIER = 1.0
        LIQUIDITY_WARNING_ONLY = False
        MIN_LIQUIDITY_ABSOLUTE = 0.0

    if multiplier is None:
        multiplier = LIQUIDITY_MULTIPLIER
    if min_absolute is None:
        min_absolute = MIN_LIQUIDITY_ABSOLUTE
    if warning_only is None:
        warning_only = LIQUIDITY_WARNING_ONLY

    stake = float(stake or 0.0)
    available_liquidity = float(available_liquidity or 0.0)
    price = float(price or 0.0)
    side = str(side or "BACK").upper()

    if stake <= 0:
        return {
            "status": LiquidityStatus.OK,
            "ratio": float("inf"),
            "color": "#4CAF50",
            "tooltip": "Nessuno stake richiesto",
            "will_block": False,
        }

    if side == "LAY":
        base_required = stake * (price - 1.0) if price > 1.0 else stake
    else:
        base_required = stake

    required_with_multiplier = float(base_required) * float(multiplier or 1.0)

    if available_liquidity < float(min_absolute or 0.0):
        return {
            "status": LiquidityStatus.DRY,
            "ratio": 0.0,
            "color": "#F44336",
            "tooltip": f"Liquidita: EUR{available_liquidity:.0f} < min EUR{float(min_absolute):.0f}",
            "will_block": True,
        }

    if available_liquidity <= 0:
        return {
            "status": LiquidityStatus.DRY,
            "ratio": 0.0,
            "color": "#F44336",
            "tooltip": f"Liquidita: EUR0 - Richiesto: EUR{required_with_multiplier:.2f}",
            "will_block": True,
        }

    ratio = available_liquidity / required_with_multiplier if required_with_multiplier > 0 else float("inf")

    if ratio >= 1.0:
        status = LiquidityStatus.OK
        color = "#4CAF50"
        will_block = False
    elif warning_only:
        status = LiquidityStatus.BORDERLINE
        color = "#FFC107"
        will_block = False
    else:
        status = LiquidityStatus.DRY
        color = "#F44336"
        will_block = True

    return {
        "status": status,
        "ratio": ratio,
        "color": color,
        "tooltip": f"Liquidita: EUR{available_liquidity:.0f} / EUR{required_with_multiplier:.0f} richiesti",
        "will_block": will_block,
    }