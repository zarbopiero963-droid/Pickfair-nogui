from __future__ import annotations

from datetime import datetime
from tkinter import ttk
from typing import Any


class HealthPanel(ttk.Frame):
    def __init__(self, master, *, health_registry: Any, refresh_ms: int = 3000, **kwargs) -> None:
        super().__init__(master, **kwargs)
        self.health_registry = health_registry
        self.refresh_ms = int(refresh_ms)
        self._last_signature = None

        self.columnconfigure(0, weight=1)
        self.rowconfigure(2, weight=1)

        self.title_label = ttk.Label(self, text="System Health")
        self.title_label.grid(row=0, column=0, sticky="w", padx=8, pady=(8, 4))

        self.state_var = ttk.Label(self, text="Overall: -")
        self.state_var.grid(row=1, column=0, sticky="w", padx=8, pady=(0, 4))

        self.tree = ttk.Treeview(
            self,
            columns=("component", "status", "reason", "updated_at"),
            show="headings",
            height=12,
        )
        self.tree.heading("component", text="Component")
        self.tree.heading("status", text="Status")
        self.tree.heading("reason", text="Reason")
        self.tree.heading("updated_at", text="Updated")
        self.tree.column("component", width=220, anchor="w")
        self.tree.column("status", width=130, anchor="center")
        self.tree.column("reason", width=320, anchor="w")
        self.tree.column("updated_at", width=180, anchor="center")
        self.tree.grid(row=2, column=0, sticky="nsew", padx=8, pady=(0, 8))

        self._schedule_refresh()

    def _schedule_refresh(self) -> None:
        self.after(self.refresh_ms, self._refresh)

    def _refresh(self) -> None:
        try:
            snap = self.health_registry.snapshot()
            overall = snap.get("overall_status", "-")
            components = snap.get("components", {})
            signature = (
                overall,
                tuple(
                    sorted(
                        (
                            name,
                            data.get("status"),
                            data.get("reason"),
                            data.get("updated_at"),
                        )
                        for name, data in components.items()
                    )
                ),
            )
            if signature == self._last_signature:
                return
            self._last_signature = signature

            self.state_var.configure(text=f"Overall: {self._state_badge(overall)}")

            for child in self.tree.get_children():
                self.tree.delete(child)

            for name, data in sorted(components.items()):
                self.tree.insert(
                    "",
                    "end",
                    values=(
                        name,
                        self._state_badge(data.get("status")),
                        data.get("reason") or "-",
                        self._fmt_ts(data.get("updated_at")),
                    ),
                )
        except Exception as exc:
            self.state_var.configure(text=f"Overall: refresh error ({exc})")
        finally:
            self._schedule_refresh()

    def _state_badge(self, state: Any) -> str:
        value = str(state or "").upper()
        if value == "READY":
            return "🟢 READY"
        if value == "DEGRADED":
            return "🟠 DEGRADED"
        if value == "NOT_READY":
            return "🔴 NOT_READY"
        return value

    def _fmt_ts(self, ts: Any) -> str:
        try:
            return datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return "-"
