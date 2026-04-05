from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk
from typing import Any

from observability.export_helpers import ExportHelpers


class MetricsPanel(ttk.Frame):
    def __init__(
        self,
        master,
        *,
        metrics_registry: Any,
        export_dir: str = "diagnostics_exports",
        refresh_ms: int = 3000,
        **kwargs,
    ) -> None:
        super().__init__(master, **kwargs)
        self.metrics_registry = metrics_registry
        self.refresh_ms = int(refresh_ms)
        self.export_helpers = ExportHelpers(export_dir=export_dir)
        self._last_signature = None

        self.columnconfigure(0, weight=1)
        self.rowconfigure(3, weight=1)

        self.title_label = ttk.Label(self, text="Metrics")
        self.title_label.grid(row=0, column=0, sticky="w", padx=8, pady=(8, 4))

        self.summary_var = tk.StringVar(value="Counters: 0 | Gauges: 0 | Updated: -")
        self.summary_label = ttk.Label(self, textvariable=self.summary_var)
        self.summary_label.grid(row=1, column=0, sticky="w", padx=8, pady=(0, 4))

        self.button_bar = ttk.Frame(self)
        self.button_bar.grid(row=2, column=0, sticky="w", padx=8, pady=(0, 4))

        self.export_json_btn = ttk.Button(self.button_bar, text="Export Metrics JSON", command=self._export_json)
        self.export_json_btn.pack(side="left", padx=(0, 6))

        self.export_csv_btn = ttk.Button(self.button_bar, text="Export Metrics CSV", command=self._export_csv)
        self.export_csv_btn.pack(side="left")

        self.tree = ttk.Treeview(
            self,
            columns=("type", "name", "value"),
            show="headings",
            height=14,
        )
        self.tree.heading("type", text="Type")
        self.tree.heading("name", text="Name")
        self.tree.heading("value", text="Value")
        self.tree.column("type", width=100, anchor="center")
        self.tree.column("name", width=420, anchor="w")
        self.tree.column("value", width=220, anchor="e")
        self.tree.grid(row=3, column=0, sticky="nsew", padx=8, pady=(0, 8))

        self._schedule_refresh()

    def _schedule_refresh(self) -> None:
        self.after(self.refresh_ms, self._refresh)

    def _refresh(self) -> None:
        try:
            snap = self.metrics_registry.snapshot()
            counters = snap.get("counters", {})
            gauges = snap.get("gauges", {})
            signature = (
                tuple(sorted(counters.items())),
                tuple(sorted(gauges.items())),
                tuple(sorted((snap.get("metadata") or {}).items())),
                snap.get("updated_at"),
            )
            if signature == self._last_signature:
                return
            self._last_signature = signature

            updated = snap.get("updated_at")
            updated_str = "-"
            try:
                from datetime import datetime
                updated_str = datetime.fromtimestamp(float(updated)).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass
            self.summary_var.set(
                f"Counters: {len(counters)} | Gauges: {len(gauges)} | Updated: {updated_str}"
            )

            for child in self.tree.get_children():
                self.tree.delete(child)

            for name, value in sorted(counters.items()):
                self.tree.insert("", "end", values=("counter", name, value))
            for name, value in sorted(gauges.items()):
                self.tree.insert("", "end", values=("gauge", name, value))
        except Exception as exc:
            self.summary_var.set(f"Metrics refresh error: {exc}")
        finally:
            self._schedule_refresh()

    def _export_json(self) -> None:
        try:
            path = self.export_helpers.export_json("metrics_snapshot", self.metrics_registry.snapshot())
            messagebox.showinfo("Metrics Export", f"JSON exported:\n{path}")
        except Exception as exc:
            messagebox.showerror("Metrics Export", f"Export failed:\n{exc}")

    def _export_csv(self) -> None:
        try:
            snap = self.metrics_registry.snapshot()
            rows = []

            for k, v in snap.get("counters", {}).items():
                rows.append({"type": "counter", "name": k, "value": v})

            for k, v in snap.get("gauges", {}).items():
                rows.append({"type": "gauge", "name": k, "value": v})

            for k, v in snap.get("metadata", {}).items():
                rows.append({"type": "metadata", "name": k, "value": v})

            path = self.export_helpers.export_csv("metrics_snapshot", rows)
            messagebox.showinfo("Metrics Export", f"CSV exported:\n{path}")
        except Exception as exc:
            messagebox.showerror("Metrics Export", f"Export failed:\n{exc}")
