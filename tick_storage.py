"""
Tick Storage - Storico quote con rolling window

Memorizza tick per costruire grafici e pattern analysis.
"""

import logging
import threading
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class Tick:
    """Singolo tick di mercato."""

    timestamp: datetime
    selection_id: int
    ltp: float
    back_price: float
    lay_price: float
    back_size: float
    lay_size: float
    traded_volume: float


@dataclass
class OHLC:
    """Candela OHLC."""

    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


class TickStorage:
    """Storage per tick con rolling window."""

    def __init__(self, max_ticks: int = 1800, ohlc_interval_sec: int = 5):
        """
        Args:
            max_ticks: Numero massimo tick per selezione (default 30 min @ 1/sec)
            ohlc_interval_sec: Intervallo candele in secondi
        """
        self.max_ticks = max_ticks
        self.ohlc_interval = ohlc_interval_sec
        self.ticks: Dict[int, deque] = {}
        self.ohlc_cache: Dict[int, List[OHLC]] = {}
        self.lock = threading.RLock()

    def push_tick(
        self,
        selection_id: int,
        ltp: float,
        back_price: float,
        lay_price: float,
        back_size: float = 0,
        lay_size: float = 0,
        traded_volume: float = 0,
    ):
        """Aggiunge un tick."""
        with self.lock:
            if selection_id not in self.ticks:
                self.ticks[selection_id] = deque(maxlen=self.max_ticks)

            tick = Tick(
                timestamp=datetime.now(),
                selection_id=selection_id,
                ltp=ltp,
                back_price=back_price,
                lay_price=lay_price,
                back_size=back_size,
                lay_size=lay_size,
                traded_volume=traded_volume,
            )

            self.ticks[selection_id].append(tick)

    def get_ticks(self, selection_id: int, limit: int = 100) -> List[Tick]:
        """Ritorna ultimi N tick."""
        with self.lock:
            if selection_id not in self.ticks:
                return []
            return list(self.ticks[selection_id])[-limit:]

    def get_last_tick(self, selection_id: int) -> Optional[Dict]:
        """
        Ritorna ultimo tick come dict per fallback P&L.

        Returns:
            Dict con 'back', 'lay', 'ltp' o None se non disponibile
        """
        with self.lock:
            if selection_id not in self.ticks or len(self.ticks[selection_id]) == 0:
                return None

            last = self.ticks[selection_id][-1]
            return {
                "back": last.back_price,
                "lay": last.lay_price,
                "ltp": last.ltp,
                "timestamp": last.timestamp,
            }

    def get_ltp_history(self, selection_id: int, limit: int = 100) -> List[float]:
        """Ritorna storico LTP per grafici."""
        ticks = self.get_ticks(selection_id, limit)
        return [t.ltp for t in ticks if t.ltp > 0]

    def aggregate_ohlc(self, selection_id: int, interval_sec: int = 5) -> List[OHLC]:
        """Aggrega tick in candele OHLC."""
        with self.lock:
            if selection_id not in self.ticks or len(self.ticks[selection_id]) == 0:
                return []

            ticks = list(self.ticks[selection_id])
            candles = []
            current_candle_ticks = []
            candle_start = ticks[0].timestamp

            for tick in ticks:
                elapsed = (tick.timestamp - candle_start).total_seconds()

                if elapsed >= interval_sec:
                    if current_candle_ticks:
                        candles.append(
                            self._make_ohlc(candle_start, current_candle_ticks)
                        )
                    current_candle_ticks = [tick]
                    candle_start = tick.timestamp
                else:
                    current_candle_ticks.append(tick)

            if current_candle_ticks:
                candles.append(self._make_ohlc(candle_start, current_candle_ticks))

            return candles

    def _make_ohlc(self, timestamp: datetime, ticks: List[Tick]) -> OHLC:
        """Crea candela da lista tick."""
        prices = [t.ltp for t in ticks if t.ltp > 0]
        if not prices:
            prices = [1.01]

        volumes = [t.traded_volume for t in ticks]

        return OHLC(
            timestamp=timestamp,
            open=prices[0],
            high=max(prices),
            low=min(prices),
            close=prices[-1],
            volume=sum(volumes) if volumes else 0,
        )

    def get_spread_history(self, selection_id: int, limit: int = 100) -> List[float]:
        """Ritorna storico spread BACK-LAY."""
        ticks = self.get_ticks(selection_id, limit)
        return [
            t.lay_price - t.back_price
            for t in ticks
            if t.back_price > 0 and t.lay_price > 0
        ]

    def clear(self, selection_id: Optional[int] = None):
        """
        Pulisce storage.

        FIX #15: the old guard `if selection_id:` is falsy for 0, so
        clear(selection_id=0) would fall into the else branch and wipe
        ALL storage instead of just selection 0.
        Use `is not None` so that 0 is treated as a valid specific ID.
        """
        with self.lock:
            if selection_id is not None:
                self.ticks.pop(selection_id, None)
                self.ohlc_cache.pop(selection_id, None)
            else:
                self.ticks.clear()
                self.ohlc_cache.clear()

