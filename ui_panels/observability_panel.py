from __future__ import annotations

from tkinter import ttk
from typing import Any

from ui_panels.alerts_panel import AlertsPanel
from ui_panels.export_panel import ExportPanel
from ui_panels.health_panel import HealthPanel
from ui_panels.incidents_panel import IncidentsPanel


class ObservabilityPanel(ttk.Frame):
    def __init__(
        self,
        master,
        *,
        health_registry: Any,
        alerts_manager: Any,
        incidents_manager: Any,
        diagnostics_service: Any,
        **kwargs,
    ) -> None:
        super().__init__(master, **kwargs)

        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        notebook = ttk.Notebook(self)
        notebook.grid(row=0, column=0, sticky="nsew")

        self.health_tab = HealthPanel(notebook, health_registry=health_registry)
        self.alerts_tab = AlertsPanel(notebook, alerts_manager=alerts_manager)
        self.incidents_tab = IncidentsPanel(notebook, incidents_manager=incidents_manager)
        self.export_tab = ExportPanel(notebook, diagnostics_service=diagnostics_service)

        notebook.add(self.health_tab, text="Health")
        notebook.add(self.alerts_tab, text="Alerts")
        notebook.add(self.incidents_tab, text="Incidents")
        notebook.add(self.export_tab, text="Diagnostics")
