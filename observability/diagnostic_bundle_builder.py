from __future__ import annotations

import json
import sys
import threading
import time
import zipfile
from pathlib import Path
from typing import Any, Dict

from .sanitizers import sanitize_value


class DiagnosticBundleBuilder:
    def __init__(self, *, export_dir: str = "diagnostics_exports") -> None:
        self.export_dir = Path(export_dir)
        self.export_dir.mkdir(parents=True, exist_ok=True)

    def build(
        self,
        *,
        health: Dict[str, Any],
        metrics: Dict[str, Any],
        alerts: Dict[str, Any],
        incidents: Dict[str, Any],
        runtime_state: Dict[str, Any],
        safe_mode_state: Dict[str, Any],
        recent_orders: Any,
        recent_audit: Any,
        forensics_review: Any = None,
        logs_tail_text: str = "",
    ) -> str:
        ts = time.strftime("%Y%m%d_%H%M%S")
        out_path = self.export_dir / f"diagnostics_bundle_{ts}.zip"

        manifest = {
            "generated_at": time.time(),
            "python_version": sys.version,
            "platform": sys.platform,
            "files": [
                "manifest.json",
                "health.json",
                "metrics.json",
                "alerts.json",
                "incidents.json",
                "runtime_state.json",
                "safe_mode.json",
                "recent_orders.json",
                "recent_audit.json",
                "forensics_review.json",
                "thread_dump.txt",
                "logs_tail.txt",
            ],
        }

        thread_dump = self._thread_dump()

        with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            self._write_json(zf, "manifest.json", manifest)
            self._write_json(zf, "health.json", sanitize_value(health))
            self._write_json(zf, "metrics.json", sanitize_value(metrics))
            self._write_json(zf, "alerts.json", sanitize_value(alerts))
            self._write_json(zf, "incidents.json", sanitize_value(incidents))
            self._write_json(zf, "runtime_state.json", sanitize_value(runtime_state))
            self._write_json(zf, "safe_mode.json", sanitize_value(safe_mode_state))
            self._write_json(zf, "recent_orders.json", sanitize_value(recent_orders))
            self._write_json(zf, "recent_audit.json", sanitize_value(recent_audit))
            self._write_json(zf, "forensics_review.json", sanitize_value(forensics_review or {}))
            zf.writestr("thread_dump.txt", thread_dump)
            zf.writestr("logs_tail.txt", logs_tail_text or "")

        return str(out_path)

    def _write_json(self, zf: zipfile.ZipFile, name: str, payload: Any) -> None:
        zf.writestr(name, json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False, default=str))

    def _thread_dump(self) -> str:
        lines = []
        for thread in threading.enumerate():
            lines.append(
                f"name={thread.name} ident={thread.ident} daemon={thread.daemon} alive={thread.is_alive()}"
            )
        return "\n".join(lines) + "\n"
