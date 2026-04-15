"""
EventBus PRO
- Thread-safe
- Non bloccante
- Worker pool (alta performance)
- Isolamento errori
- Debug opzionale
"""

__all__ = ["EventBus"]

import logging
import time
import threading
from collections import defaultdict
from queue import Empty, Queue

logger = logging.getLogger(__name__)


class EventBus:
    def __init__(self, workers: int = 4, debug: bool = False):
        self._subscribers = defaultdict(list)
        self._lock = threading.Lock()

        self._queue = Queue()
        self._workers = []
        self._running = True
        self._accepting = True

        self.debug = debug

        # Per-subscriber exception counts — used by poison-pill anomaly detector
        self._subscriber_errors: dict = defaultdict(int)

        # Cumulative published-event counter for side-effect gap detection
        self._published_total: int = 0
        # Cumulative successful subscriber callback executions (direct side effects)
        self._delivered_total: int = 0
        # Queue/backpressure observability
        self._enqueued_total: int = 0
        self._dequeued_total: int = 0
        self._queue_high_watermark: int = 0
        self._last_enqueue_ts: float = 0.0
        self._last_dequeue_ts: float = 0.0

        # 🔥 avvio worker pool
        for _ in range(max(1, workers)):
            t = threading.Thread(target=self._worker_loop, daemon=True)
            t.start()
            self._workers.append(t)

    # =========================================================
    # SUBSCRIBE
    # =========================================================
    def subscribe(self, event_type: str, callback: callable):
        with self._lock:
            if callback not in self._subscribers[event_type]:
                self._subscribers[event_type].append(callback)

    # =========================================================
    # UNSUBSCRIBE
    # =========================================================
    def unsubscribe(self, event_type: str, callback: callable):
        with self._lock:
            if event_type in self._subscribers:
                if callback in self._subscribers[event_type]:
                    self._subscribers[event_type].remove(callback)

                if not self._subscribers[event_type]:
                    del self._subscribers[event_type]

    # =========================================================
    # ACCESSORS
    # =========================================================
    def queue_depth(self) -> int:
        """Return current number of pending items in the dispatch queue."""
        return self._queue.qsize()

    def published_total_count(self) -> int:
        """Return cumulative count of events dispatched to at least one subscriber."""
        with self._lock:
            return self._published_total

    def delivered_total_count(self) -> int:
        """Return cumulative successful callback executions."""
        with self._lock:
            return self._delivered_total

    # =========================================================
    # PUBLISH (NON BLOCCANTE)
    # =========================================================
    def publish(self, event_type: str, data=None):
        with self._lock:
            if not self._accepting:
                return
            callbacks = self._subscribers.get(event_type, []).copy()
            if callbacks:
                self._published_total += 1

        if not callbacks:
            return

        if self.debug:
            logger.debug(f"[EventBus] publish → {event_type}")

        for cb in callbacks:
            self._queue.put((event_type, cb, data))
            with self._lock:
                self._enqueued_total += 1
                self._last_enqueue_ts = time.time()
                qd = self._queue.qsize()
                if qd > self._queue_high_watermark:
                    self._queue_high_watermark = qd

    # =========================================================
    # WORKER LOOP
    # =========================================================
    def _worker_loop(self):
        while True:
            try:
                event_type, callback, data = self._queue.get(timeout=1)
                if callback is None:
                    self._queue.task_done()
                    break

                self._safe_execute(event_type, callback, data)
                self._queue.task_done()
                with self._lock:
                    self._dequeued_total += 1
                    self._last_dequeue_ts = time.time()

            except Empty:
                if not self._running:
                    break
                continue
            except Exception:
                logger.exception("Errore worker EventBus")

    # =========================================================
    # SAFE EXECUTION
    # =========================================================
    def subscriber_error_counts(self) -> dict:
        """Return a snapshot of per-subscriber error counts for anomaly detection."""
        with self._lock:
            return dict(self._subscriber_errors)

    def _safe_execute(self, event_type, callback, data):
        try:
            callback(data)
            with self._lock:
                self._delivered_total += 1

        except Exception:
            name = getattr(callback, "__name__", repr(callback))
            with self._lock:
                self._subscriber_errors[name] += 1
            logger.exception(
                f"[EventBus] errore subscriber {name} evento '{event_type}'"
            )

    def _drop_pending(self) -> int:
        dropped = 0
        while True:
            try:
                self._queue.get_nowait()
            except Empty:
                break
            else:
                self._queue.task_done()
                dropped += 1
        return dropped

    # =========================================================
    # SHUTDOWN
    # =========================================================
    def _shutdown(self, *, drain: bool, timeout: float | None = None) -> dict:
        with self._lock:
            if not self._running:
                return {"drain": drain, "dropped_events": 0}
            self._accepting = False

        dropped = 0
        if drain:
            self._queue.join()
        else:
            dropped = self._drop_pending()

        self._running = False

        for _ in self._workers:
            self._queue.put((None, None, None))

        for worker in self._workers:
            worker.join(timeout=timeout)

        return {"drain": drain, "dropped_events": dropped}

    def stop(self):
        """Arresta il bus drenando esplicitamente la coda prima dello stop."""
        return self._shutdown(drain=True)

    def stop_lossy(self, timeout: float | None = None):
        """Arresta il bus scartando esplicitamente gli eventi ancora in coda."""
        return self._shutdown(drain=False, timeout=timeout)

    # =========================================================
    # METRICS (utile debug)
    # =========================================================
    def stats(self):
        return {
            "queue_size": self._queue.qsize(),
            "subscribers": {
                k: len(v) for k, v in self._subscribers.items()
            },
        }

    def pressure_snapshot(self) -> dict:
        with self._lock:
            now = time.time()
            since_enqueue = (now - self._last_enqueue_ts) if self._last_enqueue_ts > 0 else None
            since_dequeue = (now - self._last_dequeue_ts) if self._last_dequeue_ts > 0 else None
            return {
                "queue_depth": int(self._queue.qsize()),
                "queue_high_watermark": int(self._queue_high_watermark),
                "enqueued_total": int(self._enqueued_total),
                "dequeued_total": int(self._dequeued_total),
                "running": bool(self._running),
                "accepting": bool(self._accepting),
                "seconds_since_last_enqueue": float(since_enqueue) if since_enqueue is not None else None,
                "seconds_since_last_dequeue": float(since_dequeue) if since_dequeue is not None else None,
            }
