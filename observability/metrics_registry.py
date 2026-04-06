from __future__ import annotations

import threading
import time
from typing import Any, Dict


class MetricsRegistry:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._counters: Dict[str, int] = {}
        self._gauges: Dict[str, float] = {}
        self._metadata: Dict[str, Any] = {}

    def inc(self, name: str, value: int = 1) -> None:
        with self._lock:
            self._counters[name] = self._counters.get(name, 0) + value

    def set_gauge(self, name: str, value: float) -> None:
        with self._lock:
            self._gauges[name] = float(value)

    def set_meta(self, name: str, value: Any) -> None:
        with self._lock:
            self._metadata[name] = value

    def get_counter(self, name: str) -> int:
        with self._lock:
            return int(self._counters.get(name, 0))

    def get_gauge(self, name: str, default: float = 0.0) -> float:
        with self._lock:
            return float(self._gauges.get(name, default))

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "counters": dict(self._counters),
                "gauges": dict(self._gauges),
                "metadata": dict(self._metadata),
                "updated_at": time.time(),
            }
