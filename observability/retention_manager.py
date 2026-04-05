from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any


logger = logging.getLogger(__name__)


class RetentionManager:
    def __init__(
        self,
        *,
        db: Any = None,
        diagnostics_export_dir: str = "diagnostics_exports",
        snapshots_max_age_days: int = 7,
        exports_max_age_days: int = 7,
        exports_keep_last: int = 20,
    ) -> None:
        self.db = db
        self.diagnostics_export_dir = Path(diagnostics_export_dir)
        self.snapshots_max_age_days = int(snapshots_max_age_days)
        self.exports_max_age_days = int(exports_max_age_days)
        self.exports_keep_last = int(exports_keep_last)

    def run_once(self) -> None:
        self._cleanup_old_exports()
        self._cleanup_db_snapshots()
        self._cleanup_db_export_rows()

    def _cleanup_old_exports(self) -> None:
        try:
            if not self.diagnostics_export_dir.exists():
                return

            files = [p for p in self.diagnostics_export_dir.glob("*.zip") if p.is_file()]
            files.sort(key=lambda p: p.stat().st_mtime, reverse=True)

            now = time.time()
            keep = set(files[: self.exports_keep_last])

            for path in files:
                try:
                    age_sec = now - path.stat().st_mtime
                    too_old = age_sec > (self.exports_max_age_days * 86400)
                    if too_old and path not in keep:
                        path.unlink(missing_ok=True)
                except Exception:
                    logger.exception("Failed to cleanup export file: %s", path)
        except Exception:
            logger.exception("RetentionManager._cleanup_old_exports failed")

    def _cleanup_db_snapshots(self) -> None:
        if self.db is None:
            return

        cutoff = time.time() - (self.snapshots_max_age_days * 86400)

        delete_fn = getattr(self.db, "delete_old_observability_snapshots", None)
        if callable(delete_fn):
            try:
                delete_fn(cutoff)
                return
            except Exception:
                logger.exception("delete_old_observability_snapshots failed")

        execute = getattr(self.db, "execute", None)
        if callable(execute):
            try:
                execute(
                    "DELETE FROM observability_snapshots WHERE created_at < ?",
                    (cutoff,),
                )
            except Exception:
                logger.exception("SQL delete old observability_snapshots failed")

    def _cleanup_db_export_rows(self) -> None:
        if self.db is None:
            return

        cutoff = time.time() - (self.exports_max_age_days * 86400)

        delete_fn = getattr(self.db, "delete_old_diagnostics_exports", None)
        if callable(delete_fn):
            try:
                delete_fn(cutoff)
                return
            except Exception:
                logger.exception("delete_old_diagnostics_exports failed")

        execute = getattr(self.db, "execute", None)
        if callable(execute):
            try:
                execute(
                    "DELETE FROM diagnostics_exports WHERE created_at < ?",
                    (cutoff,),
                )
            except Exception:
                logger.exception("SQL delete old diagnostics_exports failed")
