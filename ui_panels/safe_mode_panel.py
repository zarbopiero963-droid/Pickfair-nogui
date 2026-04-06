from __future__ import annotations

import json
import tkinter as tk
from tkinter import messagebox, ttk
from typing import Callable, Optional


class SafeModePanel(ttk.Frame):
    def __init__(
        self,
        master,
        *,
        get_safe_mode_state: Callable[[], dict],
        enable_safe_mode: Optional[Callable[[str], None]] = None,
        disable_safe_mode: Optional[Callable[[], None]] = None,
        refresh_ms: int = 3000,
        **kwargs,
    ) -> None:
        super().__init__(master, **kwargs)
        self.get_safe_mode_state = get_safe_mode_state
        self.enable_safe_mode = enable_safe_mode
        self.disable_safe_mode = disable_safe_mode
        self.refresh_ms = int(refresh_ms)

        self.columnconfigure(0, weight=1)
        self.rowconfigure(2, weight=1)

        self.title_label = ttk.Label(self, text="Safe Mode")
        self.title_label.grid(row=0, column=0, sticky="w", padx=8, pady=(8, 4))

        self.button_bar = ttk.Frame(self)
        self.button_bar.grid(row=1, column=0, sticky="w", padx=8, pady=(0, 4))

        self.enable_btn = ttk.Button(self.button_bar, text="Enable Safe Mode", command=self._enable)
        self.enable_btn.pack(side="left", padx=(0, 6))

        self.disable_btn = ttk.Button(self.button_bar, text="Disable Safe Mode", command=self._disable)
        self.disable_btn.pack(side="left")

        self.text = tk.Text(self, height=12, wrap="none")
        self.text.grid(row=2, column=0, sticky="nsew", padx=8, pady=(0, 8))

        self._schedule_refresh()

    def _schedule_refresh(self) -> None:
        self.after(self.refresh_ms, self._refresh)

    def _refresh(self) -> None:
        try:
            state = self.get_safe_mode_state()
            self.text.delete("1.0", tk.END)
            self.text.insert(tk.END, json.dumps(state, indent=2, ensure_ascii=False, default=str))
        finally:
            self._schedule_refresh()

    def _enable(self) -> None:
        if self.enable_safe_mode is None:
            messagebox.showwarning("Safe Mode", "Enable callback not configured")
            return
        try:
            self.enable_safe_mode("MANUAL_GUI_TRIGGER")
            messagebox.showinfo("Safe Mode", "Safe mode enabled")
        except Exception as exc:
            messagebox.showerror("Safe Mode", f"Enable failed:\n{exc}")

    def _disable(self) -> None:
        if self.disable_safe_mode is None:
            messagebox.showwarning("Safe Mode", "Disable callback not configured")
            return
        try:
            self.disable_safe_mode()
            messagebox.showinfo("Safe Mode", "Safe mode disabled")
        except Exception as exc:
            messagebox.showerror("Safe Mode", f"Disable failed:\n{exc}")
