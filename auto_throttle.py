from __future__ import annotations

import threading
import time
import logging
from typing import List, Optional

logger = logging.getLogger("AutoThrottle")


class AutoThrottle:
    """
    OMS-Level Rate Limiter + Backoff Controller

    Features:
    - sliding window rate limiting
    - exponential backoff
    - monotonic delay guarantee
    - thread-safe
    - no negative delay ever
    - idle reset
    """

    def __init__(
        self,
        max_calls: int = 5,
        period: float = 1.0,
        base_backoff: float = 0.1,
        max_backoff: float = 5.0,
    ):
        self.max_calls = max(1, int(max_calls))
        self.period = max(0.0001, float(period))

        self.base_backoff = max(0.0, float(base_backoff))
        self.max_backoff = max(self.base_backoff, float(max_backoff))

        self._lock = threading.RLock()

        self._timestamps: List[float] = []
        self._last_call_time: Optional[float] = None

        self._backoff = self.base_backoff
        self._last_delay = 0.0

    # =========================================================
    # INTERNAL
    # =========================================================
    def _now(self) -> float:
        return time.monotonic()

    def _prune(self, now: float):
        cutoff = now - self.period
        self._timestamps = [ts for ts in self._timestamps if ts > cutoff]

    def _reset_if_idle(self, now: float):
        if self._last_call_time is None:
            return

        if (now - self._last_call_time) > (self.period * 2):
            # idle reset
            self._timestamps.clear()
            self._backoff = self.base_backoff

    # =========================================================
    # CORE LOGIC
    # =========================================================
    def acquire_delay(self) -> float:
        """
        Returns delay required before next call.
        NEVER negative.
        Monotonic increasing if saturated.
        """

        with self._lock:
            now = self._now()

            self._reset_if_idle(now)
            self._prune(now)

            if len(self._timestamps) < self.max_calls:
                delay = 0.0
                self._backoff = self.base_backoff
            else:
                delay = min(self._backoff, self.max_backoff)
                self._backoff = min(self._backoff * 2, self.max_backoff)

            # monotonic guarantee
            delay = max(delay, self._last_delay)
            delay = max(0.0, delay)

            self._last_delay = delay
            self._last_call_time = now

            return delay

    def allow_call(self) -> bool:
        """
        True if call can be executed immediately.
        """
        return self.acquire_delay() == 0.0

    def record_call(self):
        with self._lock:
            now = self._now()
            self._timestamps.append(now)
            self._last_call_time = now

    def wait(self):
        delay = self.acquire_delay()
        if delay > 0:
            time.sleep(delay)
        self.record_call()

    # =========================================================
    # STATUS
    # =========================================================
    def get_state(self):
        with self._lock:
            return {
                "calls": len(self._timestamps),
                "backoff": self._backoff,
                "last_delay": self._last_delay,
            }

    def reset(self):
        with self._lock:
            self._timestamps.clear()
            self._backoff = self.base_backoff
            self._last_delay = 0.0
            self._last_call_time = None