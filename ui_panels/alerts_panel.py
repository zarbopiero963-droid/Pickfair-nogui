from __future__ import annotations

from datetime import datetime
from tkinter import ttk
from typing import Any


class AlertsPanel(ttk.Frame):
    def __init__(self, master, *, alerts_manager: Any, refresh_ms: int = 3000, **kwargs) -> None:
        super().__init__(master, **kwargs)
        self.alerts_manager = alerts_manager
        self.refresh_ms = int(refresh_ms)

        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)
        self._last_signature = None

        self.title_label = ttk.Label(self, text="Alerts")
        self.title_label.grid(row=0, column=0, sticky="w", padx=8, pady=(8, 4))

        self.tree = ttk.Treeview(
            self,
            columns=("severity", "code", "message", "status", "first_seen", "last_seen", "count"),
            show="headings",
            height=10,
        )
        self.tree.heading("severity", text="Severity")
        self.tree.heading("code", text="Code")
        self.tree.heading("message", text="Message")
        self.tree.heading("status", text="Status")
        self.tree.heading("first_seen", text="First Seen")
        self.tree.heading("last_seen", text="Last Seen")
        self.tree.heading("count", text="Count")

        self.tree.column("severity", width=100, anchor="center")
        self.tree.column("code", width=160, anchor="w")
        self.tree.column("message", width=320, anchor="w")
        self.tree.column("status", width=100, anchor="center")
        self.tree.column("first_seen", width=160, anchor="center")
        self.tree.column("last_seen", width=160, anchor="center")
        self.tree.column("count", width=70, anchor="center")

        self.tree.grid(row=1, column=0, sticky="nsew", padx=8, pady=(0, 8))

        self._schedule_refresh()

    def _schedule_refresh(self) -> None:
        self.after(self.refresh_ms, self._refresh)

    def _refresh(self) -> None:
        try:
            snap = self.alerts_manager.snapshot()
            items = snap.get("alerts", [])
            signature = [
                (
                    row.get("code"),
                    row.get("severity"),
                    row.get("message"),
                    row.get("active"),
                    row.get("count"),
                    row.get("first_seen_at"),
                    row.get("last_seen_at"),
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
                        row.get("message"),
                        "ACTIVE" if row.get("active") else "RESOLVED",
                        self._fmt_ts(row.get("first_seen_at")),
                        self._fmt_ts(row.get("last_seen_at")),
                        row.get("count"),
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
