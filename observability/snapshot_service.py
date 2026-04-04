from __future__ import annotations

import logging
import time
from typing import Any, Dict

logger = logging.getLogger(__name__)


class SnapshotService:
    def __init__(
        self,
        *,
        db: Any,
        probe: Any,
        health_registry: Any,
        metrics_registry: Any,
        alerts_manager: Any,
        incidents_manager: Any,
    ) -> None:
        self.db = db
        self.probe = probe
        self.health_registry = health_registry
        self.metrics_registry = metrics_registry
        self.alerts_manager = alerts_manager
        self.incidents_manager = incidents_manager

    def collect_and_store(self) -> Dict[str, Any]:
        health = self.health_registry.snapshot()
        metrics = self.metrics_registry.snapshot()
        alerts = self.alerts_manager.snapshot()
        incidents = self.incidents_manager.snapshot()
        runtime_state = self.probe.collect_runtime_state()

        payload = {
            "ts": time.time(),
            "health": health,
            "metrics": metrics,
            "alerts": alerts,
            "incidents": incidents,
            "runtime_state": runtime_state,
        }

        saver = getattr(self.db, "save_observability_snapshot", None)
        if callable(saver):
            try:
                saver(payload)
            except Exception:
                logger.exception("save_observability_snapshot failed")

        return payload
