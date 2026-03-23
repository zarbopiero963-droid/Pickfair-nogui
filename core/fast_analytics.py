import math
from collections import deque


class FastWoMState:
    """
    Ultra-fast rolling WoM (Weight of Money) calculator.

    Designed for hot-path analytics with minimal allocations.
    """

    def __init__(self, max_ticks: int = 128):
        self.ticks = deque(maxlen=max_ticks)
        self.sum_back = 0.0
        self.sum_lay = 0.0
        self._evictions_since_rebase = 0
        self._rebase_every_evictions = 32

    def _recompute_sums(self):
        """Recompute sums from scratch using math.fsum to eliminate accumulated drift."""
        self.sum_back = math.fsum(t["back_volume"] for t in self.ticks)
        self.sum_lay = math.fsum(t["lay_volume"] for t in self.ticks)

    def push(self, tick: dict):
        """
        Add tick with fields:
        {
            "back_volume": float,
            "lay_volume": float
        }
        """

        evicting = len(self.ticks) == self.ticks.maxlen
        old = self.ticks[0] if evicting else None

        self.ticks.append(tick)

        new_back = tick["back_volume"]
        new_lay = tick["lay_volume"]

        if old is None:
            self.sum_back += new_back
            self.sum_lay += new_lay
        else:
            self.sum_back -= old["back_volume"]
            self.sum_lay -= old["lay_volume"]

            self.sum_back += new_back
            self.sum_lay += new_lay

            self._evictions_since_rebase += 1
            if self._evictions_since_rebase >= self._rebase_every_evictions:
                self._recompute_sums()
                self._evictions_since_rebase = 0

        # Guard against negative sums caused by residual floating-point error.
        if self.sum_back < 0.0:
            self.sum_back = 0.0
        if self.sum_lay < 0.0:
            self.sum_lay = 0.0

    def wom(self) -> float:
        """
        Returns Weight of Money ratio.
        """

        total = self.sum_back + self.sum_lay
        if total <= 0:
            return 0.5

        return self.sum_back / total

    def imbalance(self) -> float:
        """
        Returns imbalance metric.
        """

        total = self.sum_back + self.sum_lay
        if total <= 0:
            return 0.0

        return (self.sum_back - self.sum_lay) / total

    def snapshot(self) -> dict:
        return {
            "ticks": len(self.ticks),
            "sum_back": self.sum_back,
            "sum_lay": self.sum_lay,
            "wom": self.wom(),
        }
