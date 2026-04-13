from __future__ import annotations

import os
import time
import zipfile
from pathlib import Path
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
        telegram_service: Any = None,
        settings_service: Any = None,
        telegram_alerts_service: Any = None,
        event_bus: Any = None,
        async_db_writer: Any = None,
    ) -> None:
        self.db = db
        self.trading_engine = trading_engine
        self.runtime_controller = runtime_controller
        self.betfair_service = betfair_service
        self.safe_mode = safe_mode
        self.shutdown_manager = shutdown_manager
        self.telegram_service = telegram_service
        self.settings_service = settings_service
        self.telegram_alerts_service = telegram_alerts_service
        self.event_bus = event_bus
        self.async_db_writer = async_db_writer

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

        rss_mb = self._current_rss_mb()
        if rss_mb is not None:
            metrics["memory_rss_mb"] = float(rss_mb)

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

        state["safe_mode_enabled"] = self._safe_mode_enabled()

        if self.trading_engine is not None:
            state["trading_engine_readiness"] = getattr(self.trading_engine, "readiness", lambda: None)()
        state["alert_pipeline"] = self._alert_pipeline_state()
        state["forensics"] = self._forensics_state()

        return state

    def collect_forensics_evidence(self) -> Dict[str, Any]:
        recent_orders = []
        recent_audit = []
        diagnostics_export: Dict[str, Any] = {}

        orders_getter = getattr(self.db, "get_recent_orders_for_diagnostics", None)
        if callable(orders_getter):
            try:
                recent_orders = orders_getter(limit=100) or []
            except Exception:
                recent_orders = []

        audit_getter = getattr(self.db, "get_recent_audit_events_for_diagnostics", None)
        if callable(audit_getter):
            try:
                recent_audit = audit_getter(limit=200) or []
            except Exception:
                recent_audit = []

        try:
            export_dir = Path("diagnostics_exports")
            bundles = sorted(export_dir.glob("diagnostics_bundle_*.zip"), key=lambda p: p.stat().st_mtime, reverse=True)
            if bundles:
                with zipfile.ZipFile(bundles[0], "r") as zf:
                    manifest = zf.read("manifest.json").decode("utf-8")
                import json

                payload = json.loads(manifest)
                diagnostics_export = {
                    "bundle_path": str(bundles[0]),
                    "manifest_files": list(payload.get("files") or []),
                }
        except Exception:
            diagnostics_export = {}

        return {
            "recent_orders": recent_orders,
            "recent_audit": recent_audit,
            "diagnostics_export": diagnostics_export,
        }


    def collect_correlation_context(self) -> Dict[str, Any]:
        """Collect strongly-typed direct evidence from runtime collectors.

        Returns a dict with ``event_bus`` and/or ``db_write_queue`` sub-dicts
        populated from the actual live objects rather than loose injected gauges.
        Callers should merge this into their correlation context so rules can
        prefer typed direct evidence over heuristic gauge snapshots.
        """
        ctx: Dict[str, Any] = {}

        if self.event_bus is not None:
            eb: Dict[str, Any] = {}
            # Direct queue depth
            qd_fn = getattr(self.event_bus, "queue_depth", None)
            if callable(qd_fn):
                try:
                    eb["queue_depth"] = int(qd_fn())
                except Exception:
                    pass
            else:
                stats_fn = getattr(self.event_bus, "stats", None)
                if callable(stats_fn):
                    try:
                        eb["queue_depth"] = int((stats_fn() or {}).get("queue_size", 0))
                    except Exception:
                        pass
            # Direct published_total counter
            pt_fn = getattr(self.event_bus, "published_total_count", None)
            if callable(pt_fn):
                try:
                    eb["published_total"] = int(pt_fn())
                except Exception:
                    pass
            # Direct downstream side-effect count from successful callback execution.
            delivered_fn = getattr(self.event_bus, "delivered_total_count", None)
            if callable(delivered_fn):
                try:
                    eb["side_effects_confirmed"] = int(delivered_fn())
                except Exception:
                    pass
            # Per-subscriber error counts for poison-pill detection
            se_fn = getattr(self.event_bus, "subscriber_error_counts", None)
            if callable(se_fn):
                try:
                    eb["subscriber_errors"] = dict(se_fn())
                except Exception:
                    pass
            # Worker/liveness evidence from live bus internals.
            worker_threads = getattr(self.event_bus, "_workers", None)
            if isinstance(worker_threads, list):
                try:
                    eb["worker_threads_alive"] = int(sum(1 for t in worker_threads if getattr(t, "is_alive", lambda: False)()))
                except Exception:
                    pass
            for attr, key in (("_running", "running"), ("_accepting", "accepting")):
                if hasattr(self.event_bus, attr):
                    try:
                        eb[key] = bool(getattr(self.event_bus, attr))
                    except Exception:
                        pass
            if eb:
                ctx["event_bus"] = eb

        if self.async_db_writer is not None:
            dw: Dict[str, Any] = {}
            q = getattr(self.async_db_writer, "queue", None)
            if q is not None:
                try:
                    dw["queue_depth"] = int(q.qsize())
                except Exception:
                    pass
            for attr in ("_written", "_failed", "_dropped"):
                val = getattr(self.async_db_writer, attr, None)
                if val is not None:
                    try:
                        dw[attr.lstrip("_")] = int(val)
                    except Exception:
                        pass
            if dw:
                ctx["db_write_queue"] = dw

        if self.db is not None:
            db_state: Dict[str, Any] = {}
            orders_getter = getattr(self.db, "get_recent_orders_for_diagnostics", None)
            if callable(orders_getter):
                orders_query_ok = False
                try:
                    orders = orders_getter(limit=500) or []
                    orders_query_ok = True
                except Exception:
                    orders = None
                if orders_query_ok and isinstance(orders, list):
                    terminal = {"FILLED", "CANCELLED", "FAILED", "SETTLED", "VOIDED", "CLOSED", "COMPLETED"}
                    inflight = 0
                    remote_mismatches = 0
                    for order in orders:
                        if not isinstance(order, dict):
                            continue
                        status = str(order.get("status") or "").upper()
                        remote_status = str(order.get("remote_status") or "").upper()
                        if status and status not in terminal:
                            inflight += 1
                        if status and remote_status and status != remote_status:
                            remote_mismatches += 1
                    db_state["inflight_orders_count"] = int(inflight)
                    db_state["remote_mismatch_count"] = int(remote_mismatches)
            if db_state:
                ctx["db_state"] = db_state

        return ctx

    def collect_reviewer_context(self) -> Dict[str, Any]:
        """Collect canonical runtime context blocks for reviewer rules.

        The returned payload is deterministic and typed so anomaly/correlation
        evaluators can run from the default runtime path without relying on
        ad-hoc/manual context injection.
        """
        context: Dict[str, Any] = {}

        risk: Dict[str, Any] = {
            "expected_exposure": 0.0,
            "actual_exposure": 0.0,
            "local_exposure": 0.0,
            "remote_exposure": 0.0,
            "exposure_tolerance": 0.01,
            "source": "default_zero",
        }
        runtime = self.runtime_controller
        table_manager = getattr(runtime, "table_manager", None) if runtime is not None else None
        total_exposure_fn = getattr(table_manager, "total_exposure", None)
        if callable(total_exposure_fn):
            try:
                exposure = float(total_exposure_fn() or 0.0)
                risk["expected_exposure"] = exposure
                risk["actual_exposure"] = exposure
                risk["local_exposure"] = exposure
                risk["remote_exposure"] = exposure
                risk["source"] = "table_manager.total_exposure"
            except Exception:
                pass
        risk_desk = getattr(runtime, "risk_desk", None) if runtime is not None else None
        if risk_desk is not None:
            for attr, key in (
                ("local_exposure", "local_exposure"),
                ("remote_exposure", "remote_exposure"),
                ("exchange_exposure", "remote_exposure"),
            ):
                if hasattr(risk_desk, attr):
                    try:
                        risk[key] = float(getattr(risk_desk, attr) or 0.0)
                    except Exception:
                        pass
            # Keep expected/actual aligned with stronger local/remote fields.
            risk["expected_exposure"] = float(risk.get("local_exposure", risk["expected_exposure"]) or 0.0)
            risk["actual_exposure"] = float(risk.get("remote_exposure", risk["actual_exposure"]) or 0.0)
        cfg = getattr(runtime, "config", None) if runtime is not None else None
        if cfg is not None and hasattr(cfg, "exposure_tolerance"):
            try:
                risk["exposure_tolerance"] = float(getattr(cfg, "exposure_tolerance") or 0.01)
            except Exception:
                pass
        context["risk"] = risk

        financials: Dict[str, Any] = {
            "ledger_balance": 0.0,
            "venue_balance": 0.0,
            "drift_threshold": 0.01,
            "source": "default_zero",
        }
        if risk_desk is not None:
            for attr, key in (
                ("bankroll_current", "ledger_balance"),
                ("ledger_balance", "ledger_balance"),
                ("venue_balance", "venue_balance"),
                ("exchange_balance", "venue_balance"),
            ):
                if hasattr(risk_desk, attr):
                    try:
                        financials[key] = float(getattr(risk_desk, attr) or 0.0)
                    except Exception:
                        pass
            if (
                financials["ledger_balance"] != 0.0
                or financials["venue_balance"] != 0.0
            ):
                financials["source"] = "risk_desk"
        context["financials"] = financials

        db_block: Dict[str, Any] = {
            "lock_wait_ms": 0.0,
            "contention_events": 0,
            "lock_wait_threshold_ms": 200.0,
            "db_writer_backlog": 0,
            "db_writer_failed": 0,
            "db_writer_dropped": 0,
        }
        writer = self.async_db_writer
        if writer is not None:
            q = getattr(writer, "queue", None)
            if q is not None:
                try:
                    db_block["db_writer_backlog"] = int(q.qsize())
                except Exception:
                    pass
            for attr, key in (("_failed", "db_writer_failed"), ("_dropped", "db_writer_dropped")):
                val = getattr(writer, attr, None)
                if val is not None:
                    try:
                        db_block[key] = int(val)
                    except Exception:
                        pass
            db_block["contention_events"] = int(
                db_block["db_writer_failed"] + db_block["db_writer_dropped"]
            )
        context["db"] = db_block

        recent_orders: list[Any] = []
        recent_audit: list[Any] = []
        if self.db is not None:
            orders_getter = getattr(self.db, "get_recent_orders_for_diagnostics", None)
            if callable(orders_getter):
                try:
                    recent_orders = list(orders_getter(limit=200) or [])
                except Exception:
                    recent_orders = []
            audit_getter = getattr(self.db, "get_recent_audit_events_for_diagnostics", None)
            if callable(audit_getter):
                try:
                    recent_audit = list(audit_getter(limit=300) or [])
                except Exception:
                    recent_audit = []
        context["recent_orders"] = recent_orders
        context["recent_audit"] = recent_audit

        submitted_ids = {
            str(o.get("order_id") or o.get("id") or "")
            for o in recent_orders
            if isinstance(o, dict) and str(o.get("status", "")).upper() == "SUBMITTED"
        }
        submitted_ids.discard("")
        reconciled_ids = {
            str(a.get("order_id") or a.get("id") or "")
            for a in recent_audit
            if isinstance(a, dict)
        }
        reconciled_ids.discard("")
        missing = sorted(list(submitted_ids - reconciled_ids))
        context["reconcile_chain"] = {
            "submitted_count": len(submitted_ids),
            "reconciled_count": len(reconciled_ids),
            "missing_count": len(missing),
            "sample_missing_ids": missing[:5],
        }
        finalized_ids = {
            str(o.get("order_id") or o.get("id") or "")
            for o in recent_orders
            if isinstance(o, dict) and str(o.get("status", "")).upper() in {"COMPLETED", "FAILED", "CANCELLED"}
        }
        finalized_ids.discard("")
        finalized_audit_ids = {
            str(a.get("order_id") or a.get("id") or "")
            for a in recent_audit
            if isinstance(a, dict)
            and (
                "FINAL" in str(a.get("type", "")).upper()
                or str(a.get("status", "")).upper() in {"COMPLETED", "FAILED", "CANCELLED"}
            )
        }
        finalized_audit_ids.discard("")
        missing_finalized = sorted(list(finalized_ids - finalized_audit_ids))
        context["reconcile_chain"].update(
            {
                "finalized_count": len(finalized_ids),
                "finalized_audit_count": len(finalized_audit_ids),
                "finalized_missing_count": len(missing_finalized),
                "sample_finalized_missing_ids": missing_finalized[:5],
            }
        )

        event_bus = dict((context.get("event_bus") or {}))
        corr_ctx = self.collect_correlation_context()
        direct_event_bus = corr_ctx.get("event_bus") if isinstance(corr_ctx, dict) else None
        if isinstance(direct_event_bus, dict):
            event_bus.update(direct_event_bus)
        subscriber_errors = event_bus.get("subscriber_errors") or {}
        total_errors = 0
        if isinstance(subscriber_errors, dict):
            try:
                total_errors = int(sum(int(v or 0) for v in subscriber_errors.values()))
            except Exception:
                total_errors = 0
        delivered_total = int(event_bus.get("side_effects_confirmed", 0) or 0)
        expected_total = delivered_total + total_errors
        event_bus["delivered_fanout"] = delivered_total
        event_bus["expected_fanout"] = expected_total
        event_bus["fanout_error_count"] = total_errors
        context["event_bus"] = event_bus

        return context

    def get_live_readiness_report(self) -> Dict[str, Any]:
        health = self.collect_health()

        blockers = []
        degraded = []
        unknown = []

        for name, component in health.items():
            status = component.get("status", "UNKNOWN")
            reason = component.get("reason")
            normalized = str(status).upper() if status is not None else "UNKNOWN"
            blocker_code = self._blocker_code_for_component(
                component_name=name,
                status=normalized,
                reason=reason,
            )
            if normalized in ("NOT_READY", "UNKNOWN"):
                blockers.append(
                    {
                        "name": name,
                        "status": normalized,
                        "reason": reason,
                        "code": blocker_code,
                    }
                )
            elif normalized == "DEGRADED":
                degraded.append(
                    {
                        "name": name,
                        "status": normalized,
                        "reason": reason,
                        "code": blocker_code,
                    }
                )
            elif normalized == "READY":
                pass
            else:
                blockers.append(
                    {
                        "name": name,
                        "status": normalized,
                        "reason": f"UNRECOGNIZED_STATE::{normalized}",
                        "code": "READINESS_SIGNAL_UNKNOWN",
                    }
                )

            if normalized == "UNKNOWN":
                unknown.append(name)

        if blockers:
            level = "NOT_READY"
        elif degraded:
            level = "DEGRADED"
        else:
            level = "READY"

        ready = level == "READY"

        if ready and unknown:
            ready = False
            level = "NOT_READY"
            blockers.extend(
                {
                    "name": name,
                    "status": "UNKNOWN",
                    "reason": "coherence_broken_unknown_promoted",
                    "code": "READINESS_SIGNAL_UNKNOWN",
                }
                for name in unknown
            )

        return {
            "ready": ready,
            "level": level,
            "blockers": blockers,
            "details": {
                "degraded": degraded,
                "components": health,
                "unknown_components": unknown,
            },
        }

    def get_deploy_gate_status(self) -> Dict[str, Any]:
        try:
            report = self.get_live_readiness_report()
        except Exception as exc:
            return {
                "allowed": False,
                "reason": "DEPLOY_BLOCKED_NOT_READY",
                "readiness": "UNKNOWN",
                "details": {"probe_error": str(exc)},
            }

        level = str(report.get("level") or "UNKNOWN").upper()
        blockers = list(report.get("blockers") or [])
        allowed = bool(report.get("ready")) and level == "READY" and not blockers
        reason = "DEPLOY_GO_READY" if allowed else (
            "DEPLOY_BLOCKED_BLOCKERS_PRESENT" if blockers else "DEPLOY_BLOCKED_NOT_READY"
        )
        return {
            "allowed": allowed,
            "reason": reason,
            "readiness": level,
            "details": {
                "blockers": blockers,
                "report": report,
            },
        }

    def _blocker_code_for_component(self, *, component_name: str, status: str, reason: Any) -> str:
        normalized_reason = str(reason or "").strip().lower()
        normalized_status = str(status or "UNKNOWN").strip().upper()

        if normalized_status not in {"READY", "DEGRADED", "NOT_READY", "UNKNOWN"}:
            return "READINESS_SIGNAL_UNKNOWN"
        if normalized_status == "UNKNOWN":
            return "READINESS_SIGNAL_UNKNOWN"

        if component_name == "safe_mode":
            if normalized_reason == "active":
                return "SAFE_MODE_BLOCKING"
            if normalized_reason == "missing":
                return "LIVE_DEPENDENCY_MISSING"
            if normalized_status == "DEGRADED":
                return "SAFE_MODE_BLOCKING"
            return "READINESS_SIGNAL_UNKNOWN"

        if component_name == "runtime_controller":
            if normalized_reason == "missing":
                return "RUNTIME_NOT_INITIALIZED"
            if normalized_reason == "unhealthy":
                return "RUNTIME_HALF_STARTED"
            return "READINESS_SIGNAL_UNKNOWN"

        if component_name in {"betfair_service", "database", "trading_engine", "shutdown_manager"}:
            if normalized_reason == "missing":
                return "LIVE_DEPENDENCY_MISSING"
            if normalized_status == "DEGRADED":
                return "LIVE_DEPENDENCY_MISSING"
            return "READINESS_SIGNAL_UNKNOWN"

        return "READINESS_SIGNAL_UNKNOWN"

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
        return self._unknown_probe(name=name, reason="no-checker")

    def _probe_trading_engine(self) -> Dict[str, Any]:
        if self.trading_engine is None:
            return {"name": "trading_engine", "status": "NOT_READY", "reason": "missing", "details": {}}

        readiness = getattr(self.trading_engine, "readiness", None)
        if callable(readiness):
            try:
                data = readiness()
                state = str(data.get("state", "DEGRADED"))
                health = data.get("health")
                if state == "READY" and not health:
                    return self._unknown_probe(
                        name="trading_engine",
                        reason="ready_without_health",
                        details={"health": {}, "reported_state": state},
                    )
                return {
                    "name": "trading_engine",
                    "status": state,
                    "reason": None,
                    "details": health or {},
                }
            except Exception as exc:
                return {"name": "trading_engine", "status": "DEGRADED", "reason": str(exc), "details": {}}

        return self._unknown_probe(name="trading_engine", reason="no-readiness")

    def _unknown_probe(self, *, name: str, reason: str, details: Dict[str, Any] | None = None) -> Dict[str, Any]:
        payload_details = dict(details or {})
        payload_details.setdefault("fallback_status", "READY")
        return {"name": name, "status": "UNKNOWN", "reason": reason, "details": payload_details}

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
            return {"name": "safe_mode", "status": "DEGRADED", "reason": "missing", "details": {"enabled": False}}

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

        active_prop = getattr(self.safe_mode, "is_safe_mode_active", None)
        if isinstance(active_prop, bool):
            return active_prop

        return bool(getattr(self.safe_mode, "enabled", False))

    def _current_rss_mb(self) -> float | None:
        statm_path = f"/proc/{os.getpid()}/statm"
        try:
            with open(statm_path, "r", encoding="utf-8") as fp:
                parts = fp.read().strip().split()
            if len(parts) >= 2:
                rss_pages = int(parts[1])
                page_size = os.sysconf("SC_PAGE_SIZE")
                return (rss_pages * page_size) / (1024.0 * 1024.0)
        except Exception:
            pass

        try:
            import resource  # type: ignore

            rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            return rss_kb / 1024.0
        except Exception:
            return None

    def _alert_pipeline_state(self) -> Dict[str, Any]:
        alerts_enabled = False
        sender_available = False
        deliverable = False
        status = "DISABLED"
        reason = None
        last_delivery_ok = None
        last_delivery_error = ""

        if self.telegram_alerts_service is not None:
            availability = getattr(self.telegram_alerts_service, "availability_status", None)
            if callable(availability):
                try:
                    availability_state = availability() or {}
                    alerts_enabled = bool(availability_state.get("alerts_enabled", False))
                    sender_available = bool(availability_state.get("sender_available", False))
                    deliverable = bool(availability_state.get("deliverable", False))
                    status = str(availability_state.get("status") or status)
                    reason = availability_state.get("reason")
                    last_delivery_ok = availability_state.get("last_delivery_ok")
                    last_delivery_error = str(availability_state.get("last_delivery_error") or "")
                except Exception:
                    pass

        if not alerts_enabled:
            loader = getattr(self.settings_service, "load_telegram_config_row", None)
            if callable(loader):
                try:
                    row = loader() or {}
                    alerts_enabled = bool(row.get("alerts_enabled", False))
                except Exception:
                    alerts_enabled = False

        if self.telegram_alerts_service is None and not sender_available:
            getter = getattr(self.telegram_service, "get_sender", None)
            if callable(getter):
                try:
                    sender = getter()
                    sender_available = sender is not None
                except Exception:
                    sender_available = False

        if self.telegram_alerts_service is None and not deliverable:
            deliverable = alerts_enabled and sender_available
            if alerts_enabled and not sender_available and reason is None:
                reason = "sender_unavailable"

        if alerts_enabled and not sender_available:
            status = "DEGRADED"
            if reason is None:
                reason = "sender_unavailable"
        elif alerts_enabled and sender_available and deliverable:
            status = "READY"
        elif alerts_enabled and not deliverable:
            status = "DEGRADED"
            if reason is None:
                reason = "not_deliverable"
        else:
            status = "DISABLED"

        return {
            "alerts_enabled": alerts_enabled,
            "sender_available": sender_available,
            "deliverable": deliverable,
            "status": status,
            "reason": reason,
            "last_delivery_ok": last_delivery_ok,
            "last_delivery_error": last_delivery_error,
        }

    def _forensics_state(self) -> Dict[str, Any]:
        snapshot_recent = True
        recent_orders_count = 0
        recent_audit_count = 0
        getter = getattr(self.db, "get_recent_observability_snapshots", None)
        if callable(getter):
            try:
                rows = getter(limit=1) or []
                snapshot_recent = False
                if rows:
                    row = rows[0]
                    ts = None

                    if isinstance(row, dict):
                        ts = row.get("timestamp") or row.get("ts") or row.get("created_at")
                    elif hasattr(row, "timestamp"):
                        ts = getattr(row, "timestamp", None) or getattr(row, "created_at", None)

                    try:
                        now = time.time()
                        if ts is not None:
                            snapshot_recent = (now - float(ts)) <= 60
                    except Exception:
                        snapshot_recent = False
            except Exception:
                snapshot_recent = False
        orders_getter = getattr(self.db, "get_recent_orders_for_diagnostics", None)
        if callable(orders_getter):
            try:
                recent_orders_count = len(orders_getter(limit=20) or [])
            except Exception:
                recent_orders_count = 0
        audit_getter = getattr(self.db, "get_recent_audit_events_for_diagnostics", None)
        if callable(audit_getter):
            try:
                recent_audit_count = len(audit_getter(limit=20) or [])
            except Exception:
                recent_audit_count = 0
        return {
            "observability_snapshot_recent": snapshot_recent,
            "recent_orders_count": recent_orders_count,
            "recent_audit_count": recent_audit_count,
        }
