"""
Tick Dispatcher - Coalescing e Throttling per performance ottimali

Problema: Tick arrivano 10-30/s, ogni tick aggiorna UI/P&L/ladder/automazioni
Soluzione: UI throttled a 4 update/s, tick storage full-speed, automazioni precise

Impatto: -60/70% carico CPU
"""

import logging
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional


class DispatchMode(Enum):
    """Modalità di dispatch."""

    LIVE = "live"
    SIMULATION = "simulation"


@dataclass
class TickData:
    """Dati di un singolo tick."""

    market_id: str
    selection_id: int
    timestamp: float
    back_prices: List[float] = field(default_factory=list)
    lay_prices: List[float] = field(default_factory=list)
    back_sizes: List[float] = field(default_factory=list)
    lay_sizes: List[float] = field(default_factory=list)
    last_traded_price: Optional[float] = None
    total_matched: Optional[float] = None


logger = logging.getLogger(__name__)


class TickDispatcher:
    """Dispatcher tick con coalescing e throttling per UI e automazioni."""

    MIN_UI_UPDATE_INTERVAL = 0.25
    MIN_AUTOMATION_INTERVAL = 0.10
    SIM_UI_UPDATE_INTERVAL = 0.50
    SIM_AUTOMATION_INTERVAL = 1.0

    def __init__(self):
        self._lock = threading.Lock()
        self._mode = DispatchMode.LIVE
        self._last_ui_update: float = 0.0
        self._last_automation_check: float = 0.0
        self._pending_ticks_ui: Dict[tuple[str, int], TickData] = {}
        self._pending_ticks_auto: Dict[tuple[str, int], TickData] = {}
        self._ui_callbacks: List[Callable[[Dict[tuple[str, int], TickData]], None]] = []
        self._storage_callbacks: List[Callable[[TickData], None]] = []
        self._automation_callbacks: List[Callable[[Dict[tuple[str, int], TickData]], None]] = []
        self._tick_count = 0
        self._ui_dispatch_count = 0
        self._automation_dispatch_count = 0
        self._invalid_tick_count = 0

    @property
    def mode(self) -> DispatchMode:
        return self._mode

    @mode.setter
    def mode(self, value: DispatchMode):
        with self._lock:
            self._mode = value

    @property
    def ui_interval(self) -> float:
        return self.SIM_UI_UPDATE_INTERVAL if self._mode == DispatchMode.SIMULATION else self.MIN_UI_UPDATE_INTERVAL

    @property
    def automation_interval(self) -> float:
        return self.SIM_AUTOMATION_INTERVAL if self._mode == DispatchMode.SIMULATION else self.MIN_AUTOMATION_INTERVAL

    @staticmethod
    def _invalid_tick_reason(tick: Any) -> Optional[str]:
        """Return a deterministic invalid reason for malformed ticks."""
        if not isinstance(tick, TickData):
            return "not_tickdata"
        if not tick.market_id:
            return "missing_market_id"
        return None

    def _record_invalid_tick(self, _tick: Any, reason: str) -> None:
        """Count invalid ticks and emit reason for observability."""
        with self._lock:
            self._invalid_tick_count += 1
        logger.warning("Invalid tick dropped: %s", reason)

    def _snapshot_callbacks(self):
        return list(self._storage_callbacks), list(self._ui_callbacks), list(self._automation_callbacks)

    @staticmethod
    def _dispatch_storage_callbacks(callbacks, tick: TickData) -> None:
        """Dispatch storage callbacks while preserving caller flow on errors."""
        for cb in callbacks:
            try:
                cb(tick)
            except Exception:
                logger.exception("Tick batch callback failed")

    @staticmethod
    def _dispatch_batch_callbacks(callbacks, ticks: Optional[Dict[tuple[str, int], TickData]]) -> None:
        """Dispatch UI/automation callbacks while preserving deterministic flow."""
        if not ticks:
            return
        for cb in callbacks:
            try:
                cb(ticks)
            except Exception:
                logger.exception("Tick storage callback failed")

    def register_ui_callback(self, callback: Callable[[Dict[tuple[str, int], TickData]], None]):
        with self._lock:
            self._ui_callbacks.append(callback)

    def register_storage_callback(self, callback: Callable[[TickData], None]):
        with self._lock:
            self._storage_callbacks.append(callback)

    def register_automation_callback(self, callback: Callable[[Dict[tuple[str, int], TickData]], None]):
        with self._lock:
            self._automation_callbacks.append(callback)

    def dispatch_tick(self, tick: TickData):
        invalid_reason = self._invalid_tick_reason(tick)
        if invalid_reason is not None:
            self._record_invalid_tick(tick, invalid_reason)
            return

        now = time.time()
        with self._lock:
            self._tick_count += 1
            storage_cbs, ui_cbs, automation_cbs = self._snapshot_callbacks()
            key = (tick.market_id, tick.selection_id)
            self._pending_ticks_ui[key] = tick
            self._pending_ticks_auto[key] = tick

            ui_ticks = None
            auto_ticks = None

            if (now - self._last_ui_update) >= self.ui_interval:
                ui_ticks = dict(self._pending_ticks_ui)
                self._last_ui_update = now
                self._ui_dispatch_count += 1
                self._pending_ticks_ui.clear()

            if (now - self._last_automation_check) >= self.automation_interval:
                auto_ticks = dict(self._pending_ticks_auto)
                self._last_automation_check = now
                self._automation_dispatch_count += 1
                self._pending_ticks_auto.clear()

        self._dispatch_storage_callbacks(storage_cbs, tick)
        self._dispatch_batch_callbacks(ui_cbs, ui_ticks)
        self._dispatch_batch_callbacks(automation_cbs, auto_ticks)

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            pending = max(len(self._pending_ticks_ui), len(self._pending_ticks_auto))
            return {
                "total_ticks": self._tick_count,
                "ui_dispatches": self._ui_dispatch_count,
                "automation_dispatches": self._automation_dispatch_count,
                "invalid_ticks": self._invalid_tick_count,
                "reduction_ratio": (1 - (self._ui_dispatch_count / max(1, self._tick_count))) * 100,
                "mode": self._mode.value,
                "pending_ticks": pending,
            }

    def reset_stats(self):
        with self._lock:
            self._tick_count = 0
            self._ui_dispatch_count = 0
            self._automation_dispatch_count = 0
            self._invalid_tick_count = 0
            self._pending_ticks_ui.clear()
            self._pending_ticks_auto.clear()


_dispatcher: Optional[TickDispatcher] = None
_dispatcher_lock = threading.Lock()


def get_tick_dispatcher() -> TickDispatcher:
    global _dispatcher
    with _dispatcher_lock:
        if _dispatcher is None:
            _dispatcher = TickDispatcher()
        return _dispatcher
