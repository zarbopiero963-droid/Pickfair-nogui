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
import threading
from collections import defaultdict
from queue import Queue, Empty
import time

logger = logging.getLogger(__name__)


class EventBus:
    def __init__(self, workers: int = 4, debug: bool = False):
        self._subscribers = defaultdict(list)
        self._lock = threading.Lock()

        self._queue = Queue()
        self._workers = []
        self._running = True

        self.debug = debug

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
    # PUBLISH (NON BLOCCANTE)
    # =========================================================
    def publish(self, event_type: str, data=None):
        with self._lock:
            callbacks = self._subscribers.get(event_type, []).copy()

        if not callbacks:
            return

        if self.debug:
            logger.debug(f"[EventBus] publish → {event_type}")

        for cb in callbacks:
            self._queue.put((event_type, cb, data))

    # =========================================================
    # WORKER LOOP
    # =========================================================
    def _worker_loop(self):
        while self._running or not self._queue.empty():
            try:
                event_type, callback, data = self._queue.get(timeout=1)

                self._safe_execute(event_type, callback, data)

                self._queue.task_done()

            except Empty:
                continue
            except Exception:
                logger.exception("Errore worker EventBus")

    # =========================================================
    # SAFE EXECUTION
    # =========================================================
    def _safe_execute(self, event_type, callback, data):
        try:
            callback(data)

        except Exception:
            logger.exception(
                f"[EventBus] errore subscriber {callback.__name__} evento '{event_type}'"
            )

    # =========================================================
    # SHUTDOWN
    # =========================================================
    def stop(self):
        self._running = False

        end = time.time() + 5.0
        while self._queue.unfinished_tasks > 0:
            if time.time() >= end:
                break
            time.sleep(0.01)

        for t in self._workers:
            t.join(timeout=1)

    def stop_lossy(self):
        """Explicitly stop workers without draining queued callbacks."""
        self._running = False

        for t in self._workers:
            t.join(timeout=1)

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
