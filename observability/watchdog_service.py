from __future__ import annotations

import logging
import threading
from typing import Any

logger = logging.getLogger(__name__)


class WatchdogService:
    def __init__(
        self,
        *,
        probe: Any,
        health_registry: Any,
        metrics_registry: Any,
        alerts_manager: Any,
        incidents_manager: Any,
        snapshot_service: Any,
        interval_sec: float = 5.0,
    ) -> None:
        self.probe = probe
        self.health_registry = health_registry
        self.metrics_registry = metrics_registry
        self.alerts_manager = alerts_manager
        self.incidents_manager = incidents_manager
        self.snapshot_service = snapshot_service
        self.interval_sec = float(interval_sec)

        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def is_ready(self) -> bool:
        return True

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="observability-watchdog",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)

    def _run_loop(self) -> None:
        logger.info("WatchdogService started")
        while not self._stop_event.is_set():
            try:
                self._tick()
            except Exception:
                logger.exception("WatchdogService tick failed")
            self._stop_event.wait(self.interval_sec)
        logger.info("WatchdogService stopped")

    def _tick(self) -> None:
        health_map = self.probe.collect_health()
        for name, item in health_map.items():
            self.health_registry.set_component(
                name,
                item.get("status", "DEGRADED"),
                reason=item.get("reason"),
                details=item.get("details"),
            )

        metrics = self.probe.collect_metrics()
        for name, value in metrics.items():
            self.metrics_registry.set_gauge(name, value)

        self._evaluate_alerts()
        self.snapshot_service.collect_and_store()

    def _evaluate_alerts(self) -> None:
        health = self.health_registry.snapshot()
        metrics = self.metrics_registry.snapshot()

        overall = health.get("overall_status")
        if overall == "NOT_READY":
            self.alerts_manager.upsert_alert(
                "SYSTEM_NOT_READY",
                "critical",
                "System not ready",
                details={"overall_status": overall},
            )
            self.incidents_manager.open_incident("SYSTEM_NOT_READY", "System Not Ready", "critical")
        else:
            self.alerts_manager.resolve_alert("SYSTEM_NOT_READY")
            self.incidents_manager.close_incident("SYSTEM_NOT_READY")

        memory_rss = float(metrics["gauges"].get("memory_rss_mb", 0.0))
        if memory_rss >= 800:
            self.alerts_manager.upsert_alert(
                "MEMORY_HIGH",
                "critical",
                "Memory usage critically high",
                details={"memory_rss_mb": memory_rss},
            )
        elif memory_rss >= 500:
            self.alerts_manager.upsert_alert(
                "MEMORY_HIGH",
                "warning",
                "Memory usage high",
                details={"memory_rss_mb": memory_rss},
            )
        else:
            self.alerts_manager.resolve_alert("MEMORY_HIGH")

        inflight = float(metrics["gauges"].get("inflight_count", 0.0))
        if inflight >= 50:
            self.alerts_manager.upsert_alert(
                "INFLIGHT_HIGH",
                "warning",
                "Too many inflight orders",
                details={"inflight_count": inflight},
            )
        else:
            self.alerts_manager.resolve_alert("INFLIGHT_HIGH")
