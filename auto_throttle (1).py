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
import time

logger = logging.getLogger("AutoThrottle")


class AutoThrottle:
    def __init__(self, max_calls=1, period=1.0, *args, **kwargs):
        logger.warning(
            "[DEPRECATED] AutoThrottle istanziato. "
            "Usare RiskMiddleware / Executor / OMS."
        )
        self.max_calls = int(max_calls)
        self.period = float(period)
        self._last_rate = 0.0
        self._blocked = False
        self._call_timestamps = []

    def _prune_calls(self):
        now = time.time()
        self._call_timestamps = [
            ts for ts in self._call_timestamps if (now - ts) < self.period
        ]
        return now

    def allow_call(self):
        """
        Metodo legacy richiesto dai test.
        Consente fino a max_calls nella finestra temporale period.
        """
        if self._blocked:
            return False

        now = self._prune_calls()

        if len(self._call_timestamps) < self.max_calls:
            self._call_timestamps.append(now)
            if self.period > 0:
                self._last_rate = len(self._call_timestamps) * (60.0 / self.period)
            else:
                self._last_rate = 0.0
            return True

        return False

    def wait(self):
        """Metodo legacy: non blocca più nulla."""
        return None

    def record_call(self):
        """Metodo legacy compatibile."""
        now = self._prune_calls()
        self._call_timestamps.append(now)
        if self.period > 0:
            self._last_rate = len(self._call_timestamps) * (60.0 / self.period)
        else:
            self._last_rate = 0.0
        return None

    def get_current_rate(self):
        """Metodo legacy."""
        return self._last_rate

    def update(self, *args, **kwargs):
        """
        Metodo legacy compatibile con vecchi punti del codice
        che chiamano throttle.update(...).
        """
        api_calls_min = kwargs.get("api_calls_min")
        if api_calls_min is not None:
            try:
                self._last_rate = float(api_calls_min)
            except Exception:
                self._last_rate = 0.0
        return None

    def reset(self):
        """Metodo legacy compatibile."""
        self._last_rate = 0.0
        self._blocked = False
        self._call_timestamps = []

    def is_blocked(self):
        """Metodo legacy compatibile."""
        return self._blocked
