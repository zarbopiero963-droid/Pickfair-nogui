"""
Safety Logger - Logging automatico per errori di sicurezza

Registra automaticamente su file .txt tutti gli eventi critici:
- MixedDutchingError (errori calcolo dutching)
- AI bloccata per mercato non compatibile
- Auto-green negato (con motivo specifico)

I log sono salvati in %APPDATA%/Pickfair/logs/safety_YYYYMMDD.log
"""

from __future__ import annotations

import logging
import os
import threading
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional


class SafetyEventType(Enum):
    """Tipi di eventi di sicurezza loggati."""

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
    _lock = threading.Lock()

    def __new__(cls) -> "SafetyLogger":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._initialized = True
        self._log_lock = threading.Lock()
        self._log_dir = self._get_log_directory()
        self._current_date: Optional[str] = None
        self._file_handler: Optional[logging.FileHandler] = None
        self._logger = logging.getLogger("pickfair.safety")
        self._logger.setLevel(logging.INFO)
        self._logger.propagate = False

        self._setup_logger()

    def _get_log_directory(self) -> Path:
        """Ottiene la directory per i log di sicurezza."""
        if os.name == "nt":
            base = Path(os.environ.get("APPDATA", "."))
        else:
            base = Path.home() / ".config"

        log_dir = base / "Pickfair" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        return log_dir

    def _get_log_filename(self) -> str:
        """Genera nome file con data corrente."""
        return f"safety_{datetime.now().strftime('%Y%m%d')}.log"

    def _setup_logger(self):
        """Configura il logger con handler file."""
        today = datetime.now().strftime("%Y%m%d")

        if self._current_date == today and self._file_handler:
            return

        if self._file_handler:
            self._logger.removeHandler(self._file_handler)
            self._file_handler.close()

        log_path = self._log_dir / self._get_log_filename()
        self._file_handler = logging.FileHandler(log_path, encoding="utf-8")
        self._file_handler.setLevel(logging.INFO)

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        self._file_handler.setFormatter(formatter)
        self._logger.addHandler(self._file_handler)
        self._current_date = today

    def _rotate_if_needed(self):
        """Ruota il file di log se è cambiato il giorno."""
        today = datetime.now().strftime("%Y%m%d")
        if self._current_date != today:
            self._setup_logger()

    def log_event(
        self,
        event_type: SafetyEventType,
        message: str,
        details: Optional[Dict[str, Any]] = None,
    ):
        """
        Registra un evento di sicurezza.

        Args:
            event_type: Tipo di evento (da SafetyEventType)
            message: Messaggio descrittivo
            details: Dettagli aggiuntivi (dict opzionale)
        """
        with self._log_lock:
            self._rotate_if_needed()

            detail_str = ""
            if details:
                detail_str = " | " + " | ".join(f"{k}={v}" for k, v in details.items())

            log_line = f"[{event_type.value}] {message}{detail_str}"
            self._logger.info(log_line)

    def log_mixed_dutching_error(
        self,
        error_message: str,
        market_id: Optional[str] = None,
        stake: Optional[float] = None,
        selections_count: Optional[int] = None,
    ):
        """Logga errore MixedDutchingError."""
        self.log_event(
            SafetyEventType.MIXED_DUTCHING_ERROR,
            error_message,
            {
                "market_id": market_id or "N/A",
                "stake": f"€{stake:.2f}" if stake else "N/A",
                "selections": selections_count or 0,
            },
        )

    def log_ai_blocked(
        self,
        market_type: str,
        market_id: Optional[str] = None,
        reason: str = "Mercato non compatibile con AI Mixed",
    ):
        """Logga AI bloccata per mercato non compatibile."""
        self.log_event(
            SafetyEventType.AI_BLOCKED_MARKET,
            reason,
            {"market_type": market_type, "market_id": market_id or "N/A"},
        )

    def log_auto_green_denied(
        self,
        reason: str,
        order_id: Optional[str] = None,
        market_status: Optional[str] = None,
        elapsed_seconds: Optional[float] = None,
    ):
        """Logga auto-green negato con motivo."""
        details = {"order_id": order_id or "N/A"}

        if market_status:
            details["market_status"] = market_status
        if elapsed_seconds is not None:
            details["elapsed_sec"] = f"{elapsed_seconds:.2f}"

        self.log_event(SafetyEventType.AUTO_GREEN_DENIED, reason, details)

    def log_safe_mode_triggered(self, consecutive_errors: int, last_error: str):
        """Logga attivazione Safe Mode."""
        self.log_event(
            SafetyEventType.SAFE_MODE_TRIGGERED,
            "Safe Mode attivato - AI disabilitata",
            {"consecutive_errors": consecutive_errors, "last_error": last_error},
        )

    def log_profit_validation_failed(
        self,
        variance: float,
        threshold: float,
        market_id: Optional[str] = None,
    ):
        """Logga fallimento validazione profitto uniforme."""
        self.log_event(
            SafetyEventType.PROFIT_VALIDATION_FAILED,
            f"Varianza profitto {variance:.2f} supera soglia {threshold:.2f}",
            {"market_id": market_id or "N/A"},
        )

    def log_market_validation_failed(
        self,
        market_type: str,
        market_id: Optional[str] = None,
    ):
        """Logga fallimento validazione mercato."""
        self.log_event(
            SafetyEventType.MARKET_VALIDATION_FAILED,
            f"Mercato {market_type} non valido per dutching",
            {"market_id": market_id or "N/A"},
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
    ):
        """
        Logga blocco ordine per liquidita insufficiente.

        Telemetria completa per analisi e tuning soglie.
        """
        self.log_event(
            SafetyEventType.LIQUIDITY_BLOCK,
            f"Ordine bloccato - {runner_name}",
            {
                "market_id": market_id,
                "selection_id": selection_id,
                "stake": f"EUR{stake:.2f}",
                "available": f"EUR{available_liquidity:.2f}",
                "required": f"EUR{required_liquidity:.2f}",
                "side": side,
                "reason": reason,
                "simulation": simulation,
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
    ):
        """Logga warning liquidita (quando in warning-only mode)."""
        self.log_event(
            SafetyEventType.LIQUIDITY_WARNING,
            f"Warning liquidita - {runner_name}",
            {
                "market_id": market_id,
                "selection_id": selection_id,
                "stake": f"EUR{stake:.2f}",
                "available": f"EUR{available_liquidity:.2f}",
                "required": f"EUR{required_liquidity:.2f}",
                "side": side,
                "simulation": simulation,
            },
        )

    def get_log_path(self) -> Path:
        """Restituisce il path del file log corrente."""
        return self._log_dir / self._get_log_filename()


_safety_logger: Optional[SafetyLogger] = None


def get_safety_logger() -> SafetyLogger:
    """Ottiene l'istanza singleton del SafetyLogger."""
    global _safety_logger
    if _safety_logger is None:
        _safety_logger = SafetyLogger()
    return _safety_logger


class LiquidityStatus:
    """Status liquidita per UI indicator."""

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

    Logica ALLINEATA con Liquidity Guard - usa stesse soglie.

    Args:
        stake: Stake richiesto
        available_liquidity: Liquidita disponibile sul ladder
        side: BACK o LAY
        price: Prezzo (per calcolo liability LAY)
        multiplier: Soglia moltiplicatore (None = usa config)
        min_absolute: Minimo assoluto (None = usa config)
        warning_only: Se True, insufficiente = BORDERLINE; se False = DRY (None = usa config)

    Returns:
        Dict con:
            - status: OK / BORDERLINE / DRY
            - ratio: Rapporto liquidita/richiesto (considerando multiplier)
            - color: Colore per UI (#4CAF50 / #FFC107 / #F44336)
            - tooltip: Testo descrittivo
            - will_block: True se il guard bloccherebbe questo ordine
    """
    from trading_config import (
        LIQUIDITY_MULTIPLIER,
        LIQUIDITY_WARNING_ONLY,
        MIN_LIQUIDITY_ABSOLUTE,
    )

    if multiplier is None:
        multiplier = LIQUIDITY_MULTIPLIER
    if min_absolute is None:
        min_absolute = MIN_LIQUIDITY_ABSOLUTE
    if warning_only is None:
        warning_only = LIQUIDITY_WARNING_ONLY

    if stake <= 0:
        return {
            "status": LiquidityStatus.OK,
            "ratio": float("inf"),
            "color": "#4CAF50",
            "tooltip": "Nessuno stake richiesto",
            "will_block": False,
        }

    if side == "LAY":
        base_required = stake * (price - 1) if price > 1 else stake
    else:
        base_required = stake

    required_with_multiplier = base_required * multiplier

    if available_liquidity < min_absolute:
        return {
            "status": LiquidityStatus.DRY,
            "ratio": 0.0,
            "color": "#F44336",
            "tooltip": f"Liquidita: EUR{available_liquidity:.0f} < min EUR{min_absolute:.0f}",
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

    ratio = available_liquidity / required_with_multiplier

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