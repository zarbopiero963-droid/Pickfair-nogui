from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .db_diagnostics_adapter import DbDiagnosticsAdapter
from .diagnostic_bundle_builder import DiagnosticBundleBuilder
from .export_helpers import ExportHelpers
from .log_tail import tail_text_from_files

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
        log_paths: Optional[List[str]] = None,
    ) -> None:
        self.builder = builder
        self.probe = probe
        self.health_registry = health_registry
        self.metrics_registry = metrics_registry
        self.alerts_manager = alerts_manager
        self.incidents_manager = incidents_manager
        self.db = db
        self.safe_mode = safe_mode
        self.log_paths = list(log_paths or [])
        self.db_adapter = DbDiagnosticsAdapter(db) if db is not None else None
        self.export_helpers = ExportHelpers(export_dir=str(builder.export_dir))

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

    def export_health_json(self) -> str:
        return self.export_helpers.export_json("health_snapshot", self.health_registry.snapshot())

    def export_alerts_json(self) -> str:
        return self.export_helpers.export_json("alerts_snapshot", self.alerts_manager.snapshot())

    def export_incidents_json(self) -> str:
        return self.export_helpers.export_json("incidents_snapshot", self.incidents_manager.snapshot())

    def export_recent_audit_json(self) -> str:
        return self.export_helpers.export_json("audit_recent", self._recent_audit())

    def _safe_mode_state(self) -> Dict[str, Any]:
        if self.safe_mode is None:
            return {"enabled": False}
        getter = getattr(self.safe_mode, "is_enabled", None)
        enabled = bool(getter()) if callable(getter) else bool(getattr(self.safe_mode, "enabled", False))
        return {"enabled": enabled}

    def _recent_orders(self) -> Any:
        if self.db_adapter is None:
            return []
        return self.db_adapter.get_recent_orders(limit=200)

    def _recent_audit(self) -> Any:
        if self.db_adapter is None:
            return []
        return self.db_adapter.get_recent_audit(limit=500)

    def _logs_tail(self) -> str:
        try:
            return tail_text_from_files(self.log_paths, max_bytes_per_file=200_000)
        except Exception:
            logger.exception("tail_text_from_files failed")
            return ""
