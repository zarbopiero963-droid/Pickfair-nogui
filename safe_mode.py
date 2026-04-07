"""
Safe Mode - Auto-disabilitazione AI dopo errori consecutivi

Se si verificano 2 errori consecutivi (configurabile), il sistema:
1. Disabilita automaticamente AI Mixed
2. Richiede reset manuale dall'utente
3. Logga l'evento per audit

Thread-safe e integrato con SafetyLogger.
"""

import threading
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Callable, List, Optional

from safety_logger import get_safety_logger

CONSECUTIVE_ERRORS_THRESHOLD = 2


class SafeModeStatus(Enum):
    """Stati del Safe Mode."""

    NORMAL = "normal"
    TRIGGERED = "triggered"


@dataclass
class ErrorRecord:
    """Record di un singolo errore."""

    timestamp: datetime
    error_type: str
    message: str
    market_id: Optional[str] = None


@dataclass
class SafeModeState:
    """Stato del Safe Mode."""

    status: SafeModeStatus = SafeModeStatus.NORMAL
    consecutive_errors: int = 0
    error_history: List[ErrorRecord] = field(default_factory=list)
    triggered_at: Optional[datetime] = None
    last_reset_at: Optional[datetime] = None


class SafeModeManager:
    """
    Gestisce il Safe Mode per la protezione automatica.

    Pattern: Singleton thread-safe
    """

    _instance: Optional["SafeModeManager"] = None
    _lock = threading.Lock()

    def __new__(cls) -> "SafeModeManager":
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
        self._state_lock = threading.Lock()
        self._state = SafeModeState()
        self._threshold = CONSECUTIVE_ERRORS_THRESHOLD
        self._on_safe_mode_callbacks: List[Callable[[], None]] = []
        self._logger = get_safety_logger()

    @property
    def is_safe_mode_active(self) -> bool:
        """Verifica se Safe Mode è attivo."""
        with self._state_lock:
            return self._state.status == SafeModeStatus.TRIGGERED

    @property
    def consecutive_errors(self) -> int:
        """Numero errori consecutivi correnti."""
        with self._state_lock:
            return self._state.consecutive_errors

    @property
    def threshold(self) -> int:
        """Soglia errori per attivazione."""
        return self._threshold

    def register_callback(self, callback: Callable[[], None]):
        """
        Registra callback da chiamare quando Safe Mode si attiva.

        Args:
            callback: Funzione senza parametri da chiamare
        """
        with self._state_lock:
            self._on_safe_mode_callbacks.append(callback)

    def report_error(
        self, error_type: str, message: str, market_id: Optional[str] = None
    ) -> bool:
        """
        Segnala un errore al SafeModeManager.

        Args:
            error_type: Tipo errore (es. "MixedDutchingError")
            message: Messaggio errore
            market_id: ID mercato (opzionale)

        Returns:
            True se Safe Mode è stato attivato con questo errore
        """
        callbacks: List[Callable[[], None]] = []
        triggered = False

        with self._state_lock:
            if self._state.status == SafeModeStatus.TRIGGERED:
                return False

            record = ErrorRecord(
                timestamp=datetime.now(),
                error_type=error_type,
                message=message,
                market_id=market_id,
            )
            self._state.error_history.append(record)
            self._state.consecutive_errors += 1

            if len(self._state.error_history) > 50:
                self._state.error_history = self._state.error_history[-50:]

            if self._state.consecutive_errors >= self._threshold:
                self._state.status = SafeModeStatus.TRIGGERED
                self._state.triggered_at = datetime.now()
                triggered = True

                self._logger.log_safe_mode_triggered(
                    self._state.consecutive_errors, message
                )

                callbacks = self._on_safe_mode_callbacks.copy()

        if triggered:
            for cb in callbacks:
                try:
                    cb()
                except Exception:
                    pass
            return True

        return False

    def report_success(self):
        """
        Segnala operazione completata con successo.

        Resetta il contatore errori consecutivi.
        """
        with self._state_lock:
            if self._state.status == SafeModeStatus.NORMAL:
                self._state.consecutive_errors = 0

    def reset(self) -> bool:
        """
        Reset manuale del Safe Mode.

        Returns:
            True se il reset è avvenuto, False se non era attivo
        """
        with self._state_lock:
            if self._state.status != SafeModeStatus.TRIGGERED:
                return False

            self._state.status = SafeModeStatus.NORMAL
            self._state.consecutive_errors = 0
            self._state.last_reset_at = datetime.now()

            return True


    def is_enabled(self) -> bool:
        """Compat layer per componenti che aspettano is_enabled()."""
        return self.is_safe_mode_active

    @property
    def enabled(self) -> bool:
        """Compat layer per componenti che leggono attributo enabled."""
        return self.is_safe_mode_active

    def get_status_info(self) -> dict:
        """
        Ottiene informazioni sullo stato corrente.

        Returns:
            Dict con status, errori, timestamp
        """
        with self._state_lock:
            return {
                "status": self._state.status.value,
                "consecutive_errors": self._state.consecutive_errors,
                "threshold": self._threshold,
                "triggered_at": (
                    self._state.triggered_at.isoformat()
                    if self._state.triggered_at
                    else None
                ),
                "last_reset_at": (
                    self._state.last_reset_at.isoformat()
                    if self._state.last_reset_at
                    else None
                ),
                "recent_errors": [
                    {
                        "timestamp": e.timestamp.isoformat(),
                        "type": e.error_type,
                        "message": e.message[:100],
                    }
                    for e in self._state.error_history[-5:]
                ],
            }


_safe_mode_manager: Optional[SafeModeManager] = None


def get_safe_mode_manager() -> SafeModeManager:
    """Ottiene l'istanza singleton del SafeModeManager."""
    global _safe_mode_manager
    if _safe_mode_manager is None:
        _safe_mode_manager = SafeModeManager()
    return _safe_mode_manager


def is_safe_mode_active() -> bool:
    """Shortcut per verificare se Safe Mode è attivo."""
    return get_safe_mode_manager().is_safe_mode_active


def reset_safe_mode() -> bool:
    """Shortcut per reset Safe Mode."""
    return get_safe_mode_manager().reset()

