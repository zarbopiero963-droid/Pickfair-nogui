from __future__ import annotations

"""
Auto Throttle - DEPRECATED / LEGACY COMPAT

Questo modulo è stato ufficialmente dismesso.

Nella nuova architettura OMS:
- la protezione anti-spam (doppi click) è gestita in core/risk_middleware.py
- il rate-limiting applicativo è gestito da timer UI / executor / scheduler
- il blocco chiamate in caso di errore è gestito da circuit breaker e OMS

Manteniamo questo file solo per retrocompatibilità con codice legacy
che potrebbe ancora importarlo o chiamarne alcuni metodi.
"""

import logging
import threading
import time
from typing import List

logger = logging.getLogger("AutoThrottle")


class AutoThrottle:
    """
    Compat layer legacy.

    Obiettivi:
    - comportamento stabile e prevedibile per vecchio codice
    - nessun valore negativo o stato corrotto
    - thread-safe per chiamate concorrenti occasionali
    - nessun blocco attivo: wait() resta no-op per retrocompatibilità
    """

    def __init__(self, max_calls=1, period=1.0, *args, **kwargs):
        logger.warning(
            "[DEPRECATED] AutoThrottle istanziato. "
            "Usare RiskMiddleware / Executor / OMS."
        )

        try:
            parsed_max_calls = int(max_calls)
        except Exception:
            parsed_max_calls = 1

        try:
            parsed_period = float(period)
        except Exception:
            parsed_period = 1.0

        # Invarianti difensive: niente valori negativi o zero invalidanti.
        self.max_calls = max(1, parsed_max_calls)
        self.period = max(0.0, parsed_period)

        self._last_rate = 0.0
        self._blocked = False
        self._call_timestamps: List[float] = []
        self._lock = threading.RLock()

    def _prune_calls(self):
        now = time.time()
        if self.period <= 0:
            self._call_timestamps = []
            return now

        self._call_timestamps = [
            ts for ts in self._call_timestamps if (now - ts) < self.period
        ]
        return now

    def _recompute_rate(self):
        if self.period > 0:
            self._last_rate = max(0.0, len(self._call_timestamps) * (60.0 / self.period))
        else:
            self._last_rate = 0.0

    def allow_call(self):
        """
        Metodo legacy richiesto dai test.
        Consente fino a max_calls nella finestra temporale period.
        """
        if self._blocked:
            return False

        with self._lock:
            now = self._prune_calls()

            if len(self._call_timestamps) < self.max_calls:
                self._call_timestamps.append(now)
                self._recompute_rate()
                return True

            self._recompute_rate()
            return False

    def wait(self):
        """Metodo legacy: non blocca più nulla."""
        return None

    def record_call(self):
        """
        Metodo legacy compatibile.
        Registra comunque la chiamata, anche se oltre soglia.
        """
        with self._lock:
            now = self._prune_calls()
            self._call_timestamps.append(now)
            self._recompute_rate()
        return None

    def get_current_rate(self):
        """Metodo legacy."""
        with self._lock:
            self._prune_calls()
            self._recompute_rate()
            return self._last_rate

    def update(self, *args, **kwargs):
        """
        Metodo legacy compatibile con vecchi punti del codice
        che chiamano throttle.update(...).
        """
        api_calls_min = kwargs.get("api_calls_min")
        with self._lock:
            if api_calls_min is not None:
                try:
                    self._last_rate = max(0.0, float(api_calls_min))
                except Exception:
                    self._last_rate = 0.0
        return None

    def reset(self):
        """Metodo legacy compatibile."""
        with self._lock:
            self._last_rate = 0.0
            self._blocked = False
            self._call_timestamps = []

    def is_blocked(self):
        """Metodo legacy compatibile."""
        with self._lock:
            return self._blocked