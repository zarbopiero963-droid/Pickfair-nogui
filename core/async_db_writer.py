import logging
import threading
import time
from queue import Queue, Empty

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
    # SUBMIT / WRITE
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
            return True

        except Exception:
            self._dropped += 1
            logger.error(
                "[AsyncDBWriter] Queue piena. dropped=%s kind=%s",
                self._dropped,
                kind,
            )
            return False

    def write(self, payload: dict):
        """TradingEngine-compatible API used as async audit persistence fallback."""
        return self.submit("audit_event", payload or {})

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

        except Exception as e:
            self._failed += 1

            if retries < self.max_retries:
                item["retries"] += 1

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
    def _write(self, kind, payload):
        if kind == "bet":
            self.db.save_bet(**payload)

        elif kind == "cashout":
            self.db.save_cashout_transaction(**payload)

        elif kind == "simulation_bet":
            self.db.save_simulation_bet(**payload)

        elif kind == "audit_event":
            for mn in ("insert_audit_event", "insert_order_event", "append_order_event"):
                fn = getattr(self.db, mn, None)
                if callable(fn):
                    fn(payload)
                    return
            raise ValueError("No audit persistence method available")

        else:
            raise ValueError(f"Unknown kind: {kind}")

    # =========================================================
    # STATS
    # =========================================================
    def stats(self):
        return {
            "queued": self.queue.qsize(),
            "written": self._written,
            "failed": self._failed,
            "dropped": self._dropped,
            "running": self.running,
        }
