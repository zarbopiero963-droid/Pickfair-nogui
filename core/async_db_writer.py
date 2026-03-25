import logging
import threading
import time
from collections import deque


logger = logging.getLogger(__name__)


class AsyncDBWriter:
    """
    Non-blocking DB writer.

    Ensures trading engine never blocks on DB I/O.

    Versione robusta:
    - no silent data loss
    - shutdown con drain della queue
    - retry automatico sui write failure
    - no deque(maxlen=...) auto-drop silenzioso
    - no busy wait aggressivo
    """

    def __init__(
        self,
        db,
        maxlen: int = 5000,
        sleep_idle: float = 0.1,
        max_retries: int = 3,
        retry_delay: float = 0.25,
    ):
        self.db = db
        self.queue = deque()
        self.maxlen = int(maxlen)

        self.running = False
        self.thread = None

        self.sleep_idle = float(sleep_idle)
        self.max_retries = int(max_retries)
        self.retry_delay = float(retry_delay)

        self._lock = threading.Lock()
        self._event = threading.Event()
        self._dropped_count = 0
        self._failed_count = 0
        self._written_count = 0

    def start(self):
        if self.running:
            return

        self.running = True
        self._event.clear()

        self.thread = threading.Thread(
            target=self._loop,
            daemon=True,
            name="AsyncDBWriter",
        )
        self.thread.start()

    def stop(self):
        """
        Ferma il writer ma drena prima la queue residua.
        """
        self.running = False
        self._event.set()

        if self.thread:
            self.thread.join(timeout=10)

    def submit(self, kind: str, payload: dict):
        """
        Accoda un item in modo thread-safe.

        Returns:
            True se accodato, False se scartato per queue piena.
        """
        with self._lock:
            if len(self.queue) >= self.maxlen:
                self._dropped_count += 1
                logger.error(
                    "[AsyncDBWriter] Queue piena (%s). Item scartato kind=%s dropped=%s",
                    self.maxlen,
                    kind,
                    self._dropped_count,
                )
                return False

            self.queue.append(
                {
                    "kind": kind,
                    "payload": dict(payload or {}),
                    "retries": 0,
                    "submitted_at": time.time(),
                }
            )

        self._event.set()
        return True

    def stats(self):
        with self._lock:
            return {
                "queued": len(self.queue),
                "written": self._written_count,
                "failed": self._failed_count,
                "dropped": self._dropped_count,
                "running": self.running,
            }

    def _write_item(self, kind: str, payload: dict):
        if kind == "bet":
            self.db.save_bet(**payload)
        elif kind == "cashout":
            self.db.save_cashout_transaction(**payload)
        elif kind == "simulation_bet":
            self.db.save_simulation_bet(**payload)
        else:
            raise ValueError(f"Unknown async db write kind: {kind}")

    def _requeue_front(self, item: dict):
        with self._lock:
            self.queue.appendleft(item)
        self._event.set()

    def _pop_left(self):
        with self._lock:
            if not self.queue:
                return None
            return self.queue.popleft()

    def _has_pending_items(self):
        with self._lock:
            return bool(self.queue)

    def _loop(self):
        """
        Continua finché running=True oppure finché restano item in queue.
        Così stop() non perde gli ultimi eventi.
        """
        while self.running or self._has_pending_items():
            item = self._pop_left()

            if item is None:
                self._event.wait(timeout=self.sleep_idle)
                self._event.clear()
                continue

            kind = item["kind"]
            payload = item["payload"]
            retries = int(item.get("retries", 0))

            try:
                self._write_item(kind, payload)
                with self._lock:
                    self._written_count += 1

            except Exception as e:
                with self._lock:
                    self._failed_count += 1

                if retries < self.max_retries:
                    item["retries"] = retries + 1
                    logger.warning(
                        "[AsyncDBWriter] Write fallita kind=%s retry=%s/%s error=%s",
                        kind,
                        item["retries"],
                        self.max_retries,
                        e,
                    )
                    # FIX: requeue e attendi PRIMA di riprovare
                    self._requeue_front(item)
                    self._event.wait(timeout=self.retry_delay)
                    self._event.clear()
                else:
                    logger.exception(
                        "[AsyncDBWriter] Write persa definitivamente kind=%s dopo %s tentativi: %s",
                        kind,
                        self.max_retries,
                        e,
                    )
