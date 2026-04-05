from __future__ import annotations

import logging
import threading
from typing import Any


logger = logging.getLogger(__name__)


class CleanupService:
    def __init__(self, *, retention_manager: Any, interval_sec: float = 3600.0) -> None:
        self.retention_manager = retention_manager
        self.interval_sec = float(interval_sec)
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def is_ready(self) -> bool:
        return True

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, name="observability-cleanup", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)

    def _run_loop(self) -> None:
        logger.info("CleanupService started")
        while not self._stop_event.is_set():
            try:
                self.retention_manager.run_once()
            except Exception:
                logger.exception("CleanupService tick failed")
            self._stop_event.wait(self.interval_sec)
        logger.info("CleanupService stopped")
