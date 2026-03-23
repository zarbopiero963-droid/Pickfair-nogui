import time
from collections import deque


class PerfCounters:
    """
    Lightweight performance metrics collector.
    Stores last N samples for each metric.
    """

    def __init__(self, maxlen: int = 2000):
        self.samples = {
            "stream_ingest_ns": deque(maxlen=maxlen),
            "analytics_ns": deque(maxlen=maxlen),
            "controller_ns": deque(maxlen=maxlen),
            "trade_ns": deque(maxlen=maxlen),
        }

    def add(self, key: str, elapsed_ns: int):
        if key in self.samples:
            self.samples[key].append(int(elapsed_ns))

    def stats(self):

        out = {}

        for key, vals in self.samples.items():

            if not vals:
                out[key] = {
                    "count": 0,
                    "avg_us": 0.0,
                    "max_us": 0.0,
                }
                continue

            avg_ns = sum(vals) / len(vals)

            out[key] = {
                "count": len(vals),
                "avg_us": avg_ns / 1000.0,
                "max_us": max(vals) / 1000.0,
            }

        return out

    def reset(self):
        for vals in self.samples.values():
            vals.clear()


def now_ns():
    return time.perf_counter_ns()