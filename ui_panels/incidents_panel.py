from __future__ import annotations

from datetime import datetime
from tkinter import ttk
from typing import Any


class IncidentsPanel(ttk.Frame):
    def __init__(self, master, *, incidents_manager: Any, refresh_ms: int = 4000, **kwargs) -> None:
        super().__init__(master, **kwargs)
        self.incidents_manager = incidents_manager
        self.refresh_ms = int(refresh_ms)

        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)
        self._last_signature = None

        self.title_label = ttk.Label(self, text="Incidents")
        self.title_label.grid(row=0, column=0, sticky="w", padx=8, pady=(8, 4))

        self.tree = ttk.Treeview(
            self,
            columns=("severity", "code", "title", "status", "opened_at", "closed_at"),
            show="headings",
            height=10,
        )
        self.tree.heading("severity", text="Severity")
        self.tree.heading("code", text="Code")
        self.tree.heading("title", text="Title")
        self.tree.heading("status", text="Status")
        self.tree.heading("opened_at", text="Opened")
        self.tree.heading("closed_at", text="Resolved")

        self.tree.column("severity", width=90, anchor="center")
        self.tree.column("code", width=160, anchor="w")
        self.tree.column("title", width=320, anchor="w")
        self.tree.column("status", width=90, anchor="center")
        self.tree.column("opened_at", width=150, anchor="center")
        self.tree.column("closed_at", width=150, anchor="center")

        self.tree.grid(row=1, column=0, sticky="nsew", padx=8, pady=(0, 8))

        self._schedule_refresh()

    def _schedule_refresh(self) -> None:
        self.after(self.refresh_ms, self._refresh)

    def _refresh(self) -> None:
        try:
            snap = self.incidents_manager.snapshot()
            items = snap.get("incidents", [])
            signature = [
                (
                    row.get("code"),
                    row.get("title"),
                    row.get("severity"),
                    row.get("status"),
                    row.get("opened_at"),
                    row.get("closed_at"),
                )
                for row in items
            ]
            if signature == self._last_signature:
                return
            self._last_signature = signature

            for child in self.tree.get_children():
                self.tree.delete(child)

            for row in items:
                self.tree.insert(
                    "",
                    "end",
                    values=(
                        self._severity_badge(row.get("severity")),
                        row.get("code"),
                        row.get("title"),
                        "OPEN" if str(row.get("status")).upper() == "OPEN" else "RESOLVED",
                        self._fmt_ts(row.get("opened_at")),
                        self._fmt_ts(row.get("closed_at")),
                    ),
                )
        finally:
            self._schedule_refresh()

    def _severity_badge(self, severity: Any) -> str:
        value = str(severity or "").upper()
        if value in {"CRITICAL", "HIGH"}:
            return f"🔴 {value}"
        if value == "MEDIUM":
            return f"🟠 {value}"
        if value in {"LOW", "INFO"}:
            return f"🟢 {value}"
        return value

    def _fmt_ts(self, ts: Any) -> str:
        try:
            return datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return "-"
