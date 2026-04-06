from __future__ import annotations

import logging
from typing import Any, Dict, List


logger = logging.getLogger(__name__)


class DbDiagnosticsAdapter:
    def __init__(self, db: Any) -> None:
        self.db = db

    def get_recent_orders(self, limit: int = 200) -> List[Dict[str, Any]]:
        for method_name in (
            "get_recent_orders_for_diagnostics",
            "get_recent_orders",
            "list_recent_orders",
        ):
            fn = getattr(self.db, method_name, None)
            if callable(fn):
                try:
                    result = fn(limit=limit)
                    if isinstance(result, list):
                        return result
                except TypeError:
                    try:
                        result = fn(limit)
                        if isinstance(result, list):
                            return result
                    except Exception:
                        logger.exception("%s failed", method_name)
                except Exception:
                    logger.exception("%s failed", method_name)
        return []

    def get_recent_audit(self, limit: int = 500) -> List[Dict[str, Any]]:
        for method_name in (
            "get_recent_audit_events_for_diagnostics",
            "get_recent_audit_events",
            "list_recent_audit_events",
        ):
            fn = getattr(self.db, method_name, None)
            if callable(fn):
                try:
                    result = fn(limit=limit)
                    if isinstance(result, list):
                        return result
                except TypeError:
                    try:
                        result = fn(limit)
                        if isinstance(result, list):
                            return result
                    except Exception:
                        logger.exception("%s failed", method_name)
                except Exception:
                    logger.exception("%s failed", method_name)
        return []

    def get_recent_observability_snapshots(self, limit: int = 100) -> List[Dict[str, Any]]:
        fn = getattr(self.db, "get_recent_observability_snapshots", None)
        if callable(fn):
            try:
                result = fn(limit=limit)
                if isinstance(result, list):
                    return result
            except Exception:
                logger.exception("get_recent_observability_snapshots failed")
        return []
