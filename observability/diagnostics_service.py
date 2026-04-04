from __future__ import annotations

import logging
from typing import Any, Dict

from .diagnostic_bundle_builder import DiagnosticBundleBuilder

logger = logging.getLogger(__name__)


class DiagnosticsService:
    def __init__(
        self,
        *,
        builder: DiagnosticBundleBuilder,
        probe: Any,
        health_registry: Any,
        metrics_registry: Any,
        alerts_manager: Any,
        incidents_manager: Any,
        db: Any = None,
        safe_mode: Any = None,
    ) -> None:
        self.builder = builder
        self.probe = probe
        self.health_registry = health_registry
        self.metrics_registry = metrics_registry
        self.alerts_manager = alerts_manager
        self.incidents_manager = incidents_manager
        self.db = db
        self.safe_mode = safe_mode

    def export_bundle(self) -> str:
        health = self.health_registry.snapshot()
        metrics = self.metrics_registry.snapshot()
        alerts = self.alerts_manager.snapshot()
        incidents = self.incidents_manager.snapshot()
        runtime_state = self.probe.collect_runtime_state()
        safe_mode_state = self._safe_mode_state()
        recent_orders = self._recent_orders()
        recent_audit = self._recent_audit()
        logs_tail_text = self._logs_tail()

        path = self.builder.build(
            health=health,
            metrics=metrics,
            alerts=alerts,
            incidents=incidents,
            runtime_state=runtime_state,
            safe_mode_state=safe_mode_state,
            recent_orders=recent_orders,
            recent_audit=recent_audit,
            logs_tail_text=logs_tail_text,
        )

        register = getattr(self.db, "register_diagnostics_export", None)
        if callable(register):
            try:
                register(path)
            except Exception:
                logger.exception("register_diagnostics_export failed")

        return path

    def _safe_mode_state(self) -> Dict[str, Any]:
        if self.safe_mode is None:
            return {"enabled": False}
        getter = getattr(self.safe_mode, "is_enabled", None)
        enabled = bool(getter()) if callable(getter) else bool(getattr(self.safe_mode, "enabled", False))
        return {"enabled": enabled}

    def _recent_orders(self) -> Any:
        getter = getattr(self.db, "get_recent_orders_for_diagnostics", None)
        if callable(getter):
            try:
                return getter(limit=200)
            except Exception:
                logger.exception("get_recent_orders_for_diagnostics failed")
        return []

    def _recent_audit(self) -> Any:
        getter = getattr(self.db, "get_recent_audit_events_for_diagnostics", None)
        if callable(getter):
            try:
                return getter(limit=500)
            except Exception:
                logger.exception("get_recent_audit_events_for_diagnostics failed")
        return []

    def _logs_tail(self) -> str:
        return ""
