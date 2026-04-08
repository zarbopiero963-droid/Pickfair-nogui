from __future__ import annotations

import logging
import threading
from typing import Any

from .anomaly_engine import AnomalyEngine
from .anomaly_rules import DEFAULT_ANOMALY_RULES
from .forensics_engine import ForensicsEngine
from .forensics_rules import DEFAULT_FORENSICS_RULES

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
        anomaly_engine: Any = None,
        forensics_engine: Any = None,
        anomaly_context_provider: Any = None,
        settings_service: Any = None,
        anomaly_enabled: bool = False,
        interval_sec: float = 5.0,
    ) -> None:
        self.probe = probe
        self.health_registry = health_registry
        self.metrics_registry = metrics_registry
        self.alerts_manager = alerts_manager
        self.incidents_manager = incidents_manager
        self.snapshot_service = snapshot_service
        self.anomaly_engine = anomaly_engine or AnomalyEngine(DEFAULT_ANOMALY_RULES)
        self.forensics_engine = forensics_engine or ForensicsEngine(DEFAULT_FORENSICS_RULES)
        self.anomaly_context_provider = anomaly_context_provider
        self.settings_service = settings_service
        self.anomaly_enabled = bool(anomaly_enabled)
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
        if self._is_anomaly_enabled():
            self._run_anomaly_hook()
        self.snapshot_service.collect_and_store()

    def _is_anomaly_enabled(self) -> bool:
        if self.settings_service is None:
            return bool(self.anomaly_enabled)

        loader = getattr(self.settings_service, "load_anomaly_enabled", None)
        if callable(loader):
            try:
                return bool(loader())
            except Exception:
                logger.exception("load_anomaly_enabled failed, fallback to local flag")

        return bool(self.anomaly_enabled)

    def _run_anomaly_hook(self) -> None:
        self._evaluate_anomalies()
        self._evaluate_invariants()
        self._evaluate_correlations()

    def _evaluate_invariants(self) -> None:
        self._evaluate_forensics()

    def _evaluate_correlations(self) -> None:
        self._evaluate_forensics()

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

    def _evaluate_anomalies(self) -> None:
        if self.anomaly_engine is None:
            return

        runtime_state = {}
        collector = getattr(self.probe, "collect_runtime_state", None)
        if callable(collector):
            try:
                runtime_state = collector() or {}
            except Exception:
                logger.exception("collect_runtime_state failed during anomaly review")

        context = {
            "health": self.health_registry.snapshot(),
            "metrics": self.metrics_registry.snapshot(),
            "alerts": self.alerts_manager.snapshot(),
            "incidents": self.incidents_manager.snapshot(),
            "runtime_state": runtime_state,
        }

        if callable(self.anomaly_context_provider):
            try:
                extra = self.anomaly_context_provider() or {}
                if isinstance(extra, dict):
                    context.update(extra)
            except Exception:
                logger.exception("anomaly_context_provider failed")

        anomalies = self.anomaly_engine.evaluate(context)
        current_codes = set()

        for anomaly in anomalies:
            code = anomaly.get("code") or anomaly.get("name") or anomaly.get("type")
            if code is None:
                continue
            code = str(code)
            current_codes.add(code)
            severity = str(anomaly.get("severity", "warning") or "warning")
            message = str(anomaly.get("description", "") or "")
            details = anomaly.get("details") or {}

            self.alerts_manager.upsert_alert(
                code,
                severity,
                message,
                source="anomaly",
            )
            if severity in {"critical", "error"}:
                self.incidents_manager.open_incident(code, code, severity, details=details)

        alerts_snapshot = self.alerts_manager.snapshot().get("alerts", [])
        for item in alerts_snapshot:
            if str(item.get("source", "")) != "anomaly":
                continue
            code = str(item.get("code", "") or "")
            if code and code not in current_codes:
                self.alerts_manager.resolve_alert(code)

    def _evaluate_forensics(self) -> None:
        if self.forensics_engine is None:
            return

        runtime_state = {}
        collector = getattr(self.probe, "collect_runtime_state", None)
        if callable(collector):
            try:
                runtime_state = collector() or {}
            except Exception:
                logger.exception("collect_runtime_state failed during forensics review")

        context = {
            "health": self.health_registry.snapshot(),
            "metrics": self.metrics_registry.snapshot(),
            "alerts": self.alerts_manager.snapshot(),
            "incidents": self.incidents_manager.snapshot(),
            "runtime_state": runtime_state,
        }

        evidence_getter = getattr(self.probe, "collect_forensics_evidence", None)
        if callable(evidence_getter):
            try:
                evidence = evidence_getter() or {}
                if isinstance(evidence, dict):
                    context.update(evidence)
            except Exception:
                logger.exception("collect_forensics_evidence failed")

        findings = self.forensics_engine.evaluate(context)
        current_codes = set()
        for finding in findings:
            code = str(finding.get("code", "") or "")
            if not code:
                continue
            current_codes.add(code)
            severity = str(finding.get("severity", "warning") or "warning").lower()
            message = str(finding.get("message", code) or code)
            details = finding.get("details") or {}
            self.alerts_manager.upsert_alert(
                code,
                severity,
                message,
                source="forensics_reviewer",
                title=code,
                details=details,
            )
            if severity in {"critical", "error"}:
                self.incidents_manager.open_incident(code, code, severity, details=details)

        active_alerts = []
        active_getter = getattr(self.alerts_manager, "active_alerts", None)
        if callable(active_getter):
            try:
                active_alerts = active_getter() or []
            except Exception:
                logger.exception("active_alerts failed during forensics resolution")
        for item in active_alerts:
            if str(item.get("source", "")) != "forensics_reviewer":
                continue
            code = str(item.get("code", "") or "")
            if code and code not in current_codes:
                self.alerts_manager.resolve_alert(code)
