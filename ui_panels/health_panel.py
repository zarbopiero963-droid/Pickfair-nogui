from __future__ import annotations

import json
import tkinter as tk
from tkinter import ttk
from typing import Any


class HealthPanel(ttk.Frame):
    def __init__(self, master, *, health_registry: Any, refresh_ms: int = 3000, **kwargs) -> None:
        super().__init__(master, **kwargs)
        self.health_registry = health_registry
        self.refresh_ms = int(refresh_ms)

        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)

        self.title_label = ttk.Label(self, text="System Health")
        self.title_label.grid(row=0, column=0, sticky="w", padx=8, pady=(8, 4))

        self.text = tk.Text(self, height=16, wrap="none")
        self.text.grid(row=1, column=0, sticky="nsew", padx=8, pady=(0, 8))

        self._schedule_refresh()

    def _schedule_refresh(self) -> None:
        self.after(self.refresh_ms, self._refresh)

    def _refresh(self) -> None:
        try:
            snap = self.health_registry.snapshot()
            self.text.delete("1.0", tk.END)
            self.text.insert(tk.END, json.dumps(snap, indent=2, ensure_ascii=False, default=str))
        except Exception as exc:
            self.text.delete("1.0", tk.END)
            self.text.insert(tk.END, f"Health refresh error: {exc}")
        finally:
            self._schedule_refresh()
