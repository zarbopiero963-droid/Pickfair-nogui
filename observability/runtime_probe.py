from __future__ import annotations

import os
import time
from typing import Any, Dict


class RuntimeProbe:
    def __init__(
        self,
        *,
        db: Any = None,
        trading_engine: Any = None,
        runtime_controller: Any = None,
        betfair_service: Any = None,
        safe_mode: Any = None,
        shutdown_manager: Any = None,
    ) -> None:
        self.db = db
        self.trading_engine = trading_engine
        self.runtime_controller = runtime_controller
        self.betfair_service = betfair_service
        self.safe_mode = safe_mode
        self.shutdown_manager = shutdown_manager

    def collect_health(self) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}

        out["database"] = self._probe_ready_component(self.db, "database")
        out["trading_engine"] = self._probe_trading_engine()
        out["runtime_controller"] = self._probe_ready_component(self.runtime_controller, "runtime_controller")
        out["betfair_service"] = self._probe_betfair()
        out["safe_mode"] = self._probe_safe_mode()
        out["shutdown_manager"] = self._probe_ready_component(self.shutdown_manager, "shutdown_manager")

        return out

    def collect_metrics(self) -> Dict[str, float]:
        metrics: Dict[str, float] = {}

        if self.trading_engine is not None:
            inflight = getattr(self.trading_engine, "_inflight_keys", None)
            if inflight is not None:
                metrics["inflight_count"] = float(len(inflight))

            seen = getattr(self.trading_engine, "_seen_correlation_ids", None)
            if seen is not None:
                metrics["seen_correlation_ids_count"] = float(len(seen))

        try:
            import resource  # type: ignore

            rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            rss_mb = rss_kb / 1024.0
            metrics["memory_rss_mb"] = float(rss_mb)
        except Exception:
            pass

        return metrics

    def collect_runtime_state(self) -> Dict[str, Any]:
        state: Dict[str, Any] = {
            "ts": time.time(),
            "pid": os.getpid(),
        }

        if self.runtime_controller is not None:
            for attr in ("mode", "simulation_mode", "last_error", "last_signal_at"):
                if hasattr(self.runtime_controller, attr):
                    state[attr] = getattr(self.runtime_controller, attr)

        if self.safe_mode is not None:
            state["safe_mode_enabled"] = self._safe_mode_enabled()

        if self.trading_engine is not None:
            state["trading_engine_readiness"] = getattr(self.trading_engine, "readiness", lambda: None)()

        return state

    def _probe_ready_component(self, obj: Any, name: str) -> Dict[str, Any]:
        if obj is None:
            return {"name": name, "status": "NOT_READY", "reason": "missing", "details": {}}
        checker = getattr(obj, "is_ready", None)
        if callable(checker):
            try:
                ok = bool(checker())
                return {
                    "name": name,
                    "status": "READY" if ok else "DEGRADED",
                    "reason": None if ok else "unhealthy",
                    "details": {},
                }
            except Exception as exc:
                return {"name": name, "status": "DEGRADED", "reason": str(exc), "details": {}}
        return {"name": name, "status": "READY", "reason": "no-checker", "details": {}}

    def _probe_trading_engine(self) -> Dict[str, Any]:
        if self.trading_engine is None:
            return {"name": "trading_engine", "status": "NOT_READY", "reason": "missing", "details": {}}

        readiness = getattr(self.trading_engine, "readiness", None)
        if callable(readiness):
            try:
                data = readiness()
                return {
                    "name": "trading_engine",
                    "status": data.get("state", "DEGRADED"),
                    "reason": None,
                    "details": data.get("health", {}),
                }
            except Exception as exc:
                return {"name": "trading_engine", "status": "DEGRADED", "reason": str(exc), "details": {}}

        return {"name": "trading_engine", "status": "READY", "reason": "no-readiness", "details": {}}

    def _probe_betfair(self) -> Dict[str, Any]:
        if self.betfair_service is None:
            return {"name": "betfair_service", "status": "NOT_READY", "reason": "missing", "details": {}}

        details: Dict[str, Any] = {}
        connected = None

        for attr in ("is_connected", "connected"):
            value = getattr(self.betfair_service, attr, None)
            if callable(value):
                try:
                    connected = bool(value())
                    break
                except Exception:
                    connected = False
                    break
            if isinstance(value, bool):
                connected = value
                break

        if connected is None:
            return {
                "name": "betfair_service",
                "status": "DEGRADED",
                "reason": "unknown_connection_state",
                "details": details,
            }

        return {
            "name": "betfair_service",
            "status": "READY" if connected else "DEGRADED",
            "reason": None if connected else "disconnected",
            "details": details,
        }

    def _probe_safe_mode(self) -> Dict[str, Any]:
        if self.safe_mode is None:
            return {"name": "safe_mode", "status": "READY", "reason": "missing_optional", "details": {}}

        enabled = self._safe_mode_enabled()
        return {
            "name": "safe_mode",
            "status": "DEGRADED" if enabled else "READY",
            "reason": "active" if enabled else None,
            "details": {"enabled": enabled},
        }

    def _safe_mode_enabled(self) -> bool:
        if self.safe_mode is None:
            return False
        getter = getattr(self.safe_mode, "is_enabled", None)
        if callable(getter):
            try:
                return bool(getter())
            except Exception:
                return False
        return bool(getattr(self.safe_mode, "enabled", False))
