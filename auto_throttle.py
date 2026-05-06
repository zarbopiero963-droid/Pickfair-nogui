from __future__ import annotations

import logging
import threading
import time
from typing import List, Optional

logger = logging.getLogger("AutoThrottle")


class AutoThrottle:
    """
    OMS-Level Rate Limiter con compatibilità legacy.

    Canonical import path in this repository:
    - ``from auto_throttle import AutoThrottle``
    (master-plan mapping sometimes references ``core/auto_throttle.py``).

    Compatibilità legacy:
    - allow_call() fa check + consume atomico
    - wait() blocca e poi registra la chiamata
    - record_call() resta disponibile

    Features:
    - sliding window
    - exponential backoff
    - monotonicità del delay
    - idle reset
    - delay mai negativo
    - thread-safe
    """

    def __init__(
        self,
        max_calls: int = 5,
        period: float = 1.0,
        base_backoff: float = 0.1,
        max_backoff: float = 5.0,
    ):
        try:
            parsed_max_calls = int(max_calls)
        except Exception:
            parsed_max_calls = 5

        try:
            parsed_period = float(period)
        except Exception:
            parsed_period = 1.0

        try:
            parsed_base_backoff = float(base_backoff)
        except Exception:
            parsed_base_backoff = 0.1

        try:
            parsed_max_backoff = float(max_backoff)
        except Exception:
            parsed_max_backoff = 5.0

        self.max_calls = max(1, parsed_max_calls)
        self.period = max(0.0001, parsed_period)

        self.base_backoff = max(0.0, parsed_base_backoff)
        self.max_backoff = max(self.base_backoff, parsed_max_backoff)

        self._lock = threading.RLock()
        self._timestamps: List[float] = []
        self._last_call_time: Optional[float] = None

        self._backoff = self.base_backoff
        self._last_delay = 0.0
        self._blocked = False

    # =========================================================
    # INTERNAL
    # =========================================================
    def _now(self) -> float:
        return time.monotonic()

    def _prune(self, now: float) -> None:
        cutoff = now - self.period
        self._timestamps = [ts for ts in self._timestamps if ts > cutoff]

    def _reset_if_idle(self, now: float) -> None:
        if self._last_call_time is None:
            return

        if (now - self._last_call_time) > (self.period * 2):
            self._timestamps.clear()
            self._backoff = self.base_backoff
            self._last_delay = 0.0

    def _state_now(self) -> tuple[float, int]:
        now = self._now()
        self._reset_if_idle(now)
        self._prune(now)
        return now, len(self._timestamps)

    # =========================================================
    # CORE LOGIC
    # =========================================================
    def acquire_delay(self) -> float:
        """
        Calcola il delay necessario prima della prossima chiamata.
        Non consuma la chiamata.
        NEVER negative.
        Monotonic non-decrescente sotto saturazione.
        """
        with self._lock:
            now, used = self._state_now()

            if self._blocked:
                delay = min(self._backoff if self._backoff > 0 else self.base_backoff, self.max_backoff)
            elif used < self.max_calls:
                delay = 0.0
                self._backoff = self.base_backoff
            else:
                delay = min(self._backoff, self.max_backoff)
                self._backoff = min(max(self.base_backoff, self._backoff * 2), self.max_backoff)

            delay = max(0.0, delay)
            if delay > 0:
                delay = max(delay, self._last_delay)
            else:
                self._last_delay = 0.0

            self._last_delay = delay
            self._last_call_time = now
            return delay

    def allow_call(self) -> bool:
        """
        Compatibilità legacy:
        controlla e, se consentito, registra subito la chiamata.
        """
        with self._lock:
            if self._blocked:
                now = self._now()
                self._last_call_time = now
                self._last_delay = max(0.0, min(self._backoff, self.max_backoff))
                return False

            now, used = self._state_now()

            if used < self.max_calls:
                self._timestamps.append(now)
                self._last_call_time = now
                self._backoff = self.base_backoff
                self._last_delay = 0.0
                return True

            delay = min(self._backoff, self.max_backoff)
            self._backoff = min(max(self.base_backoff, self._backoff * 2), self.max_backoff)
            self._last_delay = max(0.0, delay)
            self._last_call_time = now
            return False

    def record_call(self) -> None:
        with self._lock:
            now, _ = self._state_now()
            self._timestamps.append(now)
            self._last_call_time = now

    def wait(self) -> None:
        delay = self.acquire_delay()
        if delay > 0:
            time.sleep(delay)
        self.record_call()

    # =========================================================
    # STATUS / LEGACY
    # =========================================================
    def get_current_rate(self) -> float:
        with self._lock:
            _, used = self._state_now()
            rate = used * (60.0 / self.period)
            return max(0.0, rate)

    def update(self, *args, **kwargs) -> bool:
        api_calls_min = kwargs.get("api_calls_min")
        with self._lock:
            if api_calls_min is None:
                return False
            try:
                parsed = float(api_calls_min)
            except (TypeError, ValueError):
                return False
            if parsed <= 0:
                return False
            allowed_calls = int((parsed * self.period) / 60.0)
            self.max_calls = max(1, allowed_calls)
            self._timestamps.clear()
            self._last_delay = 0.0
            self._backoff = self.base_backoff
            self._last_call_time = None
            self._blocked = False
            return True

    def reset(self) -> None:
        with self._lock:
            self._timestamps.clear()
            self._backoff = self.base_backoff
            self._last_delay = 0.0
            self._last_call_time = None
            self._blocked = False

    def is_blocked(self) -> bool:
        with self._lock:
            return bool(self._blocked)

    def get_state(self) -> dict:
        with self._lock:
            _, used = self._state_now()
            return {
                "calls": used,
                "backoff": self._backoff,
                "last_delay": self._last_delay,
                "max_calls": self.max_calls,
                "period": self.period,
                "blocked": self._blocked,
            }
