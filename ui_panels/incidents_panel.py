from __future__ import annotations

from tkinter import ttk
from typing import Any


class IncidentsPanel(ttk.Frame):
    def __init__(self, master, *, incidents_manager: Any, refresh_ms: int = 4000, **kwargs) -> None:
        super().__init__(master, **kwargs)
        self.incidents_manager = incidents_manager
        self.refresh_ms = int(refresh_ms)

        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)

        self.title_label = ttk.Label(self, text="Incidents")
        self.title_label.grid(row=0, column=0, sticky="w", padx=8, pady=(8, 4))

        self.tree = ttk.Treeview(
            self,
            columns=("code", "title", "severity", "status"),
            show="headings",
            height=10,
        )
        self.tree.heading("code", text="Code")
        self.tree.heading("title", text="Title")
        self.tree.heading("severity", text="Severity")
        self.tree.heading("status", text="Status")

        self.tree.column("code", width=160, anchor="w")
        self.tree.column("title", width=420, anchor="w")
        self.tree.column("severity", width=90, anchor="center")
        self.tree.column("status", width=90, anchor="center")

        self.tree.grid(row=1, column=0, sticky="nsew", padx=8, pady=(0, 8))

        self._schedule_refresh()

    def _schedule_refresh(self) -> None:
        self.after(self.refresh_ms, self._refresh)

    def _refresh(self) -> None:
        try:
            snap = self.incidents_manager.snapshot()
            items = snap.get("incidents", [])

            for child in self.tree.get_children():
                self.tree.delete(child)

            for row in items:
                self.tree.insert(
                    "",
                    "end",
                    values=(
                        row.get("code"),
                        row.get("title"),
                        row.get("severity"),
                        row.get("status"),
                    ),
                )
        finally:
            self._schedule_refresh()
