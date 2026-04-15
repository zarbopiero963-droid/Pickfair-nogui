import logging
import threading
import time
from queue import Empty, Queue
from typing import Any, Callable, Dict

logger = logging.getLogger(__name__)


class AsyncDBWriter:
    """
    Async DB Writer PRO

    - queue thread-safe (no lock manuale)
    - worker loop stabile
    - retry con backoff
    - no starvation
    - support batching
    """

    def __init__(
        self,
        db,
        maxsize: int = 5000,
        workers: int = 1,
        batch_size: int = 10,
        max_retries: int = 3,
        retry_delay: float = 0.25,
    ):
        self.db = db

        self.queue = Queue(maxsize=maxsize)
        self.running = False

        self.workers = workers
        self.batch_size = batch_size

        self.max_retries = max_retries
        self.retry_delay = retry_delay

        self._threads = []

        # stats
        self._written = 0
        self._failed = 0
        self._dropped = 0
        self._submitted = 0
        self._retried = 0
        self._queue_high_watermark = 0
        self._last_submit_ts = 0.0
        self._last_write_ts = 0.0

    # =========================================================
    # START / STOP
    # =========================================================
    def start(self):
        if self.running:
            return

        self.running = True

        for i in range(self.workers):
            t = threading.Thread(
                target=self._worker_loop,
                daemon=True,
                name=f"AsyncDBWriter-{i}",
            )
            t.start()
            self._threads.append(t)

    def stop(self):
        self.running = False

        for t in self._threads:
            t.join(timeout=5)

    # =========================================================
    # SUBMIT
    # =========================================================
    def submit(self, kind: str, payload: dict):
        try:
            self.queue.put_nowait(
                {
                    "kind": kind,
                    "payload": dict(payload or {}),
                    "retries": 0,
                }
            )
            self._submitted += 1
            self._last_submit_ts = time.time()
            current_depth = self.queue.qsize()
            if current_depth > self._queue_high_watermark:
                self._queue_high_watermark = current_depth
            return True

        except Exception:
            self._dropped += 1
            logger.error(
                "[AsyncDBWriter] Queue piena. dropped=%s kind=%s",
                self._dropped,
                kind,
            )
            return False

    def write(self, event: Dict[str, Any]) -> bool:
        """Runtime-facing API used by trading engine audit emission."""
        if not isinstance(event, dict):
            raise TypeError("AsyncDBWriter.write expects a dict event payload")

        self._resolve_audit_writer()
        return self.submit("audit_event", event)

    # =========================================================
    # WORKER LOOP
    # =========================================================
    def _worker_loop(self):
        while self.running or not self.queue.empty():

            batch = []

            try:
                item = self.queue.get(timeout=0.5)
                batch.append(item)
            except Empty:
                continue

            # batching
            while len(batch) < self.batch_size:
                try:
                    batch.append(self.queue.get_nowait())
                except Empty:
                    break

            for item in batch:
                self._process_item(item)

                self.queue.task_done()

    # =========================================================
    # PROCESS
    # =========================================================
    def _process_item(self, item):
        kind = item["kind"]
        payload = item["payload"]
        retries = item["retries"]

        try:
            self._write(kind, payload)
            self._written += 1
            self._last_write_ts = time.time()

        except Exception as e:
            self._failed += 1

            if retries < self.max_retries:
                item["retries"] += 1
                self._retried += 1

                time.sleep(self.retry_delay)

                try:
                    self.queue.put_nowait(item)
                except Exception:
                    self._dropped += 1
                    logger.error("Drop retry item")

            else:
                logger.exception(
                    "[AsyncDBWriter] write fallita definitivamente kind=%s error=%s",
                    kind,
                    e,
                )

    # =========================================================
    # WRITE ROUTER
    # =========================================================
    def _resolve_audit_writer(self) -> Callable[[Dict[str, Any]], Any]:
        for method_name in ("insert_audit_event", "insert_order_event", "append_order_event"):
            method = getattr(self.db, method_name, None)
            if callable(method):
                return method

        raise AttributeError(
            "AsyncDBWriter audit contract mismatch: db must expose one of "
            "insert_audit_event/insert_order_event/append_order_event"
        )

    def _write(self, kind, payload):
        if kind == "bet":
            self.db.save_bet(**payload)

        elif kind == "cashout":
            self.db.save_cashout_transaction(**payload)

        elif kind == "simulation_bet":
            self.db.save_simulation_bet(**payload)

        elif kind == "audit_event":
            self._resolve_audit_writer()(payload)

        else:
            raise ValueError(f"Unknown kind: {kind}")

    # =========================================================
    # STATS
    # =========================================================
    def stats(self):
        now = time.time()
        since_submit = (now - self._last_submit_ts) if self._last_submit_ts > 0 else None
        since_write = (now - self._last_write_ts) if self._last_write_ts > 0 else None
        return {
            "queued": self.queue.qsize(),
            "written": self._written,
            "failed": self._failed,
            "dropped": self._dropped,
            "submitted": self._submitted,
            "retried": self._retried,
            "queue_high_watermark": self._queue_high_watermark,
            "seconds_since_last_submit": since_submit,
            "seconds_since_last_write": since_write,
            "running": self.running,
        }

    def pressure_snapshot(self):
        return self.stats()
