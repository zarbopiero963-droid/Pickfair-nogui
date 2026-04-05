from __future__ import annotations

from tkinter import messagebox, ttk
from typing import Callable, List

from observability.export_helpers import ExportHelpers


class AuditPanel(ttk.Frame):
    def __init__(
        self,
        master,
        *,
        get_recent_audit: Callable[[int], List[dict]],
        export_dir: str = "diagnostics_exports",
        refresh_ms: int = 4000,
        **kwargs,
    ) -> None:
        super().__init__(master, **kwargs)
        self.get_recent_audit = get_recent_audit
        self.refresh_ms = int(refresh_ms)
        self.export_helpers = ExportHelpers(export_dir=export_dir)

        self.columnconfigure(0, weight=1)
        self.rowconfigure(2, weight=1)

        self.title_label = ttk.Label(self, text="Recent Audit")
        self.title_label.grid(row=0, column=0, sticky="w", padx=8, pady=(8, 4))

        self.top_bar = ttk.Frame(self)
        self.top_bar.grid(row=1, column=0, sticky="we", padx=8, pady=(0, 4))

        self.export_btn = ttk.Button(self.top_bar, text="Export Audit JSON", command=self._export_json)
        self.export_btn.pack(side="left")

        self.tree = ttk.Treeview(
            self,
            columns=("type", "category", "correlation_id", "customer_ref"),
            show="headings",
            height=14,
        )
        self.tree.heading("type", text="Type")
        self.tree.heading("category", text="Category")
        self.tree.heading("correlation_id", text="Correlation ID")
        self.tree.heading("customer_ref", text="Customer Ref")

        self.tree.column("type", width=160, anchor="w")
        self.tree.column("category", width=160, anchor="w")
        self.tree.column("correlation_id", width=220, anchor="w")
        self.tree.column("customer_ref", width=180, anchor="w")

        self.tree.grid(row=2, column=0, sticky="nsew", padx=8, pady=(0, 8))

        self._schedule_refresh()

    def _schedule_refresh(self) -> None:
        self.after(self.refresh_ms, self._refresh)

    def _refresh(self) -> None:
        try:
            rows = self.get_recent_audit(200)

            for child in self.tree.get_children():
                self.tree.delete(child)

            for row in rows:
                self.tree.insert(
                    "",
                    "end",
                    values=(
                        row.get("type"),
                        row.get("category"),
                        row.get("correlation_id"),
                        row.get("customer_ref"),
                    ),
                )
        finally:
            self._schedule_refresh()

    def _export_json(self) -> None:
        try:
            path = self.export_helpers.export_json("audit_recent", self.get_recent_audit(500))
            messagebox.showinfo("Audit Export", f"JSON exported:\n{path}")
        except Exception as exc:
            messagebox.showerror("Audit Export", f"Export failed:\n{exc}")
