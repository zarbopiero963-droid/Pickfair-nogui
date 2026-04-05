from __future__ import annotations

from tkinter import ttk
from typing import Any


class AlertsPanel(ttk.Frame):
    def __init__(self, master, *, alerts_manager: Any, refresh_ms: int = 3000, **kwargs) -> None:
        super().__init__(master, **kwargs)
        self.alerts_manager = alerts_manager
        self.refresh_ms = int(refresh_ms)

        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)

        self.title_label = ttk.Label(self, text="Alerts")
        self.title_label.grid(row=0, column=0, sticky="w", padx=8, pady=(8, 4))

        self.tree = ttk.Treeview(
            self,
            columns=("code", "severity", "message", "active", "count"),
            show="headings",
            height=10,
        )
        self.tree.heading("code", text="Code")
        self.tree.heading("severity", text="Severity")
        self.tree.heading("message", text="Message")
        self.tree.heading("active", text="Active")
        self.tree.heading("count", text="Count")

        self.tree.column("code", width=160, anchor="w")
        self.tree.column("severity", width=100, anchor="center")
        self.tree.column("message", width=380, anchor="w")
        self.tree.column("active", width=70, anchor="center")
        self.tree.column("count", width=70, anchor="center")

        self.tree.grid(row=1, column=0, sticky="nsew", padx=8, pady=(0, 8))

        self._schedule_refresh()

    def _schedule_refresh(self) -> None:
        self.after(self.refresh_ms, self._refresh)

    def _refresh(self) -> None:
        try:
            snap = self.alerts_manager.snapshot()
            items = snap.get("alerts", [])

            for child in self.tree.get_children():
                self.tree.delete(child)

            for row in items:
                self.tree.insert(
                    "",
                    "end",
                    values=(
                        row.get("code"),
                        row.get("severity"),
                        row.get("message"),
                        row.get("active"),
                        row.get("count"),
                    ),
                )
        finally:
            self._schedule_refresh()
