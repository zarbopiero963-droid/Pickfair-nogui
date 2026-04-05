from __future__ import annotations

from datetime import datetime
import tkinter as tk
from tkinter import messagebox, ttk
from typing import Any


class ExportPanel(ttk.Frame):
    def __init__(self, master, *, diagnostics_service: Any, **kwargs) -> None:
        super().__init__(master, **kwargs)
        self.diagnostics_service = diagnostics_service
        self._exporting = False

        self.columnconfigure(0, weight=1)

        self.title_label = ttk.Label(self, text="Diagnostics Export")
        self.title_label.grid(row=0, column=0, sticky="w", padx=8, pady=(8, 4))

        self.desc_label = ttk.Label(
            self,
            text="Create a ZIP bundle with health, metrics, alerts, incidents, orders, audit and log tail.",
            wraplength=700,
        )
        self.desc_label.grid(row=1, column=0, sticky="we", padx=8, pady=(0, 8))

        self.export_btn = ttk.Button(self, text="Export Diagnostics ZIP", command=self._export_bundle)
        self.export_btn.grid(row=2, column=0, sticky="w", padx=8, pady=(0, 8))

        self.state_var = tk.StringVar(value="State: idle")
        self.state_label = ttk.Label(self, textvariable=self.state_var)
        self.state_label.grid(row=3, column=0, sticky="w", padx=8, pady=(0, 4))

        self.result_var = tk.StringVar(value="No export yet")
        self.result_label = ttk.Label(self, textvariable=self.result_var, wraplength=700)
        self.result_label.grid(row=4, column=0, sticky="we", padx=8, pady=(0, 8))

    def _export_bundle(self) -> None:
        if self._exporting:
            return
        self._exporting = True
        self.export_btn.state(["disabled"])
        self.state_var.set("State: exporting...")
        try:
            path = self.diagnostics_service.export_bundle()
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.result_var.set(f"Last export ({now}): {path}")
            self.state_var.set("State: done")
            messagebox.showinfo("Diagnostics", f"Bundle exported:\n{path}")
        except Exception as exc:
            self.result_var.set(f"Export failed: {exc}")
            self.state_var.set("State: failed")
            messagebox.showerror("Diagnostics", f"Export failed:\n{exc}")
        finally:
            self._exporting = False
            self.export_btn.state(["!disabled"])
