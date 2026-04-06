from __future__ import annotations

from tkinter import ttk
from typing import Any, Callable

from ui_panels.alerts_panel import AlertsPanel
from ui_panels.audit_panel import AuditPanel
from ui_panels.export_panel import ExportPanel
from ui_panels.health_panel import HealthPanel
from ui_panels.incident_timeline_panel import IncidentTimelinePanel
from ui_panels.incidents_panel import IncidentsPanel
from ui_panels.metrics_panel import MetricsPanel
from ui_panels.safe_mode_panel import SafeModePanel


class ObservabilityPanel(ttk.Frame):
    def __init__(
        self,
        master,
        *,
        health_registry: Any,
        metrics_registry: Any,
        alerts_manager: Any,
        incidents_manager: Any,
        diagnostics_service: Any,
        get_recent_audit: Callable[[int], list],
        get_safe_mode_state: Callable[[], dict],
        enable_safe_mode=None,
        disable_safe_mode=None,
        **kwargs,
    ) -> None:
        super().__init__(master, **kwargs)

        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        notebook = ttk.Notebook(self)
        notebook.grid(row=0, column=0, sticky="nsew")

        notebook.add(
            HealthPanel(notebook, health_registry=health_registry),
            text="Health",
        )
        notebook.add(
            MetricsPanel(notebook, metrics_registry=metrics_registry),
            text="Metrics",
        )
        notebook.add(
            AlertsPanel(notebook, alerts_manager=alerts_manager),
            text="Alerts",
        )
        notebook.add(
            IncidentsPanel(notebook, incidents_manager=incidents_manager),
            text="Incidents",
        )
        notebook.add(
            IncidentTimelinePanel(notebook, incidents_manager=incidents_manager),
            text="Incident Timeline",
        )
        notebook.add(
            AuditPanel(notebook, get_recent_audit=get_recent_audit),
            text="Audit",
        )
        notebook.add(
            SafeModePanel(
                notebook,
                get_safe_mode_state=get_safe_mode_state,
                enable_safe_mode=enable_safe_mode,
                disable_safe_mode=disable_safe_mode,
            ),
            text="Safe Mode",
        )
        notebook.add(
            ExportPanel(notebook, diagnostics_service=diagnostics_service),
            text="Diagnostics",
        )
