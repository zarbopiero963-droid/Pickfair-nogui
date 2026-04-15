from __future__ import annotations

import logging
import threading
from typing import Any

from .anomaly_engine import AnomalyEngine
from . import anomaly_rules
from .anomaly_rules import DEFAULT_ANOMALY_RULES
from .correlation_engine import CorrelationEvaluator, evaluate_correlation_rules
from .forensics_engine import ForensicsEngine
from .forensics_rules import DEFAULT_FORENSICS_RULES
from .cto_reviewer import CtoReviewer
from .invariant_guard import evaluate_invariants, DEFAULT_INVARIANT_CHECKS
from .sanitizers import sanitize_value

logger = logging.getLogger(__name__)

_SEVERITY_RANK = {"info": 10, "warning": 20, "error": 30, "high": 35, "critical": 40}
_RANK_SEVERITY = {v: k for k, v in _SEVERITY_RANK.items()}


class ReviewerGovernancePolicy:
    def normalize(self, finding: dict[str, Any]) -> dict[str, Any]:
        source = str(finding.get("source", "unknown") or "unknown")
        code = str(finding.get("code", "UNKNOWN_FINDING") or "UNKNOWN_FINDING").upper()
        base = str(finding.get("severity", "warning") or "warning").lower()
        rank = _SEVERITY_RANK.get(base, _SEVERITY_RANK["warning"])
        if source == "invariant_reviewer":
            rank = max(rank, _SEVERITY_RANK["high"])
        if source == "correlation_reviewer":
            rank = max(rank, _SEVERITY_RANK["high"])
        if "FINANCIAL" in code or "EXPOSURE_MISMATCH" in code:
            rank = max(rank, _SEVERITY_RANK["critical"])
        if "AMBIGUOUS_LOCAL_REMOTE_INCONSISTENCY" in code or "LOCAL_VS_REMOTE_MISMATCH" in code:
            rank = max(rank, _SEVERITY_RANK["high"])
        return {
            "code": code,
            "source": source,
            "normalized_severity": _RANK_SEVERITY[rank],
            "normalized_rank": rank,
            "message": str(finding.get("message") or code),
            "details": dict(finding.get("details") or {}),
        }

    def grouping_key(self, finding: dict[str, Any]) -> str:
        details = finding.get("details") or {}
        for key in ("grouping_key", "order_id", "id", "event_key", "correlation_id", "component", "service"):
            val = details.get(key) if isinstance(details, dict) else None
            if val not in (None, ""):
                return f"entity:{val}"
        sample = details.get("sample") if isinstance(details, dict) else None
        if isinstance(sample, list) and sample:
            first = sample[0] or {}
            ident = first.get("id") or first.get("order_id")
            if ident:
                return f"entity:{ident}"
        return f"code:{finding.get('code', 'UNKNOWN')}"

    def classify_group(self, codes: set[str]) -> tuple[str, str, str, str]:
        def has(*items: str) -> bool:
            return all(i in codes for i in items)

        if (
            ("FINANCIAL_DRIFT_DETECTED" in codes or "EXPOSURE_MISMATCH" in codes or "INVARIANT_EXPOSURE_MISMATCH" in codes)
            and ("LOCAL_VS_REMOTE_MISMATCH" in codes or "GHOST_ORDER_DETECTED" in codes or "GHOST_ORDER_SUSPECTED" in codes)
        ):
            return (
                "financial_integrity_incident",
                "mandatory_delivery_with_degraded_fallback_state",
                "critical",
                "Financial drift with execution contradiction can hide real exposure and PnL risk.",
            )
        if (
            ("GHOST_ORDER_SUSPECTED" in codes or "GHOST_ORDER_DETECTED" in codes)
            and "LOCAL_VS_REMOTE_MISMATCH" in codes
        ):
            return (
                "execution_consistency_incident",
                "mandatory_delivery_with_degraded_fallback_state",
                "critical",
                "Execution consistency contradiction means local state can diverge from exchange truth.",
            )
        if (
            "QUEUE_DEPTH_LIVENESS_CONTRADICTION" in codes
            and ("HEARTBEAT_STALE" in codes or "QUEUE_DEPTH_DISPATCHER_CONTRADICTION" in codes)
        ):
            return (
                "liveness_degradation_incident",
                "incident_and_alert",
                "high",
                "Dispatcher liveness degradation can block reconciliation and event delivery.",
            )
        if "EVENT_SIDE_EFFECT_GAP" in codes and ("POISON_PILL_SUBSCRIBER" in codes or "EVENT_FANOUT_INCOMPLETE" in codes):
            return (
                "dispatch_pipeline_incident",
                "incident_and_alert",
                "high",
                "Event fanout/poison-pill signals indicate downstream side effects are incomplete.",
            )
        if ("DB_CONTENTION_DETECTED" in codes or "DB_VS_MEMORY_MISMATCH" in codes) and (
            "EVENT_SIDE_EFFECT_GAP" in codes or "DIAGNOSTICS_BUNDLE_EVIDENCE_GAP" in codes
        ):
            return (
                "observability_evidence_incident",
                "incident_and_alert",
                "high",
                "Contention plus evidence gaps reduce reviewer auditability and dispatch trust.",
            )
        return ("reviewer_generic_incident", "alert_only", "warning", "Reviewer finding requires operator visibility.")

class WatchdogService:
    def __init__(
        self,
        *,
        probe: Any,
        health_registry: Any,
        metrics_registry: Any,
        alerts_manager: Any,
        incidents_manager: Any,
        snapshot_service: Any,
        anomaly_engine: Any = None,
        forensics_engine: Any = None,
        anomaly_context_provider: Any = None,
        settings_service: Any = None,
        anomaly_enabled: bool = True,
        anomaly_alerts_enabled: bool = False,
        anomaly_actions_enabled: bool = False,
        anomaly_alert_service: Any = None,
        anomaly_escalation_hook: Any = None,
        interval_sec: float = 5.0,
        invariant_checks: Any = None,
    ) -> None:
        self.probe = probe
        self.health_registry = health_registry
        self.metrics_registry = metrics_registry
        self.alerts_manager = alerts_manager
        self.incidents_manager = incidents_manager
        self.snapshot_service = snapshot_service
        self.anomaly_engine = anomaly_engine or AnomalyEngine(DEFAULT_ANOMALY_RULES)
        self.forensics_engine = forensics_engine or ForensicsEngine(DEFAULT_FORENSICS_RULES)
        self.anomaly_context_provider = anomaly_context_provider
        self.settings_service = settings_service
        self.anomaly_enabled = bool(anomaly_enabled)
        self.anomaly_alerts_enabled = bool(anomaly_alerts_enabled)
        self.anomaly_actions_enabled = bool(anomaly_actions_enabled)
        self.anomaly_alert_service = anomaly_alert_service
        self.anomaly_escalation_hook = anomaly_escalation_hook
        self.interval_sec = float(interval_sec)
        self._invariant_checks = invariant_checks

        self.last_anomalies: list[dict[str, Any]] = []
        self.escalation_requested = False
        self.last_escalation_event: dict[str, Any] | None = None
        self._managed_anomaly_alert_codes: set[str] = set()
        self._managed_invariant_alert_codes: set[str] = set()
        self._managed_correlation_alert_codes: set[str] = set()
        self._managed_forensics_alert_codes: set[str] = set()
        self._managed_governance_alert_codes: set[str] = set()
        self._managed_cto_alert_codes: set[str] = set()
        self._correlation_evaluator = CorrelationEvaluator()
        self._reviewer_policy = ReviewerGovernancePolicy()
        self._cto_reviewer = CtoReviewer()
        self.last_forensics_findings: list[dict[str, Any]] = []

        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def is_ready(self) -> bool:
        return True

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="observability-watchdog",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)

    def _run_loop(self) -> None:
        logger.info("WatchdogService started")
        while not self._stop_event.is_set():
            try:
                self._tick()
            except Exception:
                logger.exception("WatchdogService tick failed")
            self._stop_event.wait(self.interval_sec)
        logger.info("WatchdogService stopped")

    def _tick(self) -> None:
        self.tick()

    def tick(self) -> None:
        _DISABLED_CODE = "ANOMALY_REVIEWER_DISABLED"
        health_map = self.probe.collect_health()
        self._publish_health_components(health_map)

        metrics = self.probe.collect_metrics()
        self._publish_metric_gauges(metrics)

        self._evaluate_alerts()
        self._evaluate_invariants()
        self._evaluate_correlations()
        if self._is_anomaly_enabled():
            self.alerts_manager.resolve_alert(_DISABLED_CODE)
            self.incidents_manager.close_incident(
                _DISABLED_CODE,
                reason="anomaly_reviewer_reenabled",
                resolved_by="anomaly_reviewer",
            )
            self._run_anomaly_hook()
        else:
            # Fail-loud + fail-closed: anomaly scanning is explicitly disabled.
            # Emit a structured operational alert and incident so the reviewer-disabled
            # state is surfaced beyond log output — satisfying the fail-closed audit
            # requirement that suppression is an operationally escalated condition.
            logger.warning(
                "anomaly reviewer is DISABLED — anomaly scans are suppressed this tick; "
                "set anomaly_enabled=True or configure load_anomaly_enabled() to re-enable"
            )
            self.alerts_manager.upsert_alert(
                _DISABLED_CODE,
                "warning",
                "Anomaly reviewer is DISABLED: anomaly scans are suppressed each tick. "
                "Reviewer coverage gap is active. Set anomaly_enabled=True to restore coverage.",
                source="anomaly_reviewer_disabled",
                details={"anomaly_enabled": False, "suppressed_this_tick": True},
            )
            self.incidents_manager.open_incident(
                _DISABLED_CODE,
                _DISABLED_CODE,
                "warning",
                details={"reason": "anomaly_reviewer_explicitly_disabled"},
            )
            self.last_anomalies = []
            self.escalation_requested = False
            self.last_escalation_event = None
        self._evaluate_forensics()
        self._evaluate_cto_reviewer()
        self._evaluate_reviewer_governance()
        self.snapshot_service.collect_and_store()

    def _publish_health_components(self, health_map: dict[str, Any]) -> None:
        for name, item in health_map.items():
            self.health_registry.set_component(
                name,
                item.get("status", "DEGRADED"),
                reason=item.get("reason"),
                details=item.get("details"),
            )

    def _publish_metric_gauges(self, metrics: dict[str, Any]) -> None:
        for name, value in metrics.items():
            self.metrics_registry.set_gauge(name, value)

    def _is_anomaly_enabled(self) -> bool:
        return self._load_toggle("load_anomaly_enabled", self.anomaly_enabled)

    def _is_anomaly_alerts_enabled(self) -> bool:
        return self._load_toggle("load_anomaly_alerts_enabled", self.anomaly_alerts_enabled)

    def _is_anomaly_actions_enabled(self) -> bool:
        return self._load_toggle("load_anomaly_actions_enabled", self.anomaly_actions_enabled)

    def _load_toggle(self, loader_name: str, fallback: bool) -> bool:
        if self.settings_service is None:
            return bool(fallback)

        loader = getattr(self.settings_service, loader_name, None)
        if callable(loader):
            try:
                val = loader()
                # None means "not configured in settings" — preserve the default.
                if val is None:
                    return bool(fallback)
                return bool(val)
            except Exception:
                logger.exception("%s failed, using default flag=%s", loader_name, fallback)

        return bool(fallback)

    def _run_anomaly_hook(self) -> None:
        try:
            self._evaluate_anomalies()
            self._maybe_run_anomaly_actions(self.last_anomalies)
        except Exception:
            self.last_anomalies = []
            logger.exception("watchdog anomaly hook failed")

    def _run_anomaly_checks(self) -> list[dict[str, Any]]:
        context = self._build_anomaly_context()
        collected: list[dict[str, Any]] = []
        if not isinstance(context, dict):
            collected.append(
                {
                    "code": "ANOMALY_REVIEWER_UNAVAILABLE",
                    "severity": "critical",
                    "message": "Anomaly reviewer input unavailable",
                    "details": {"reason": "context_not_mapping"},
                }
            )
            return collected

        runtime_state = context.get("runtime_state")
        rule_inputs = context
        strict_fail_closed = bool(context.get("anomaly_fail_closed"))
        if isinstance(runtime_state, dict):
            strict_fail_closed = strict_fail_closed or bool(runtime_state.get("anomaly_fail_closed"))

        if strict_fail_closed:
            if not isinstance(runtime_state, dict):
                collected.append(
                    {
                        "code": "ANOMALY_REVIEWER_MISCONFIGURED",
                        "severity": "critical",
                        "message": "Anomaly reviewer runtime_state is malformed",
                        "details": {"reason": "runtime_state_not_mapping"},
                    }
                )
                return collected
            if not runtime_state:
                collected.append(
                    {
                        "code": "ANOMALY_REVIEWER_INPUT_MISSING",
                        "severity": "critical",
                        "message": "Anomaly reviewer runtime_state input missing",
                        "details": {"reason": "runtime_state_empty"},
                    }
                )
                return collected

        if self.anomaly_engine is None:
            for rule_name in (
                "detect_ghost_order",
                "ghost_order_detected",
                "detect_exposure_mismatch",
                "exposure_mismatch",
                "detect_db_contention",
                "db_contention_detected",
                "detect_event_fanout_failure",
                "event_fanout_incomplete",
                "detect_financial_drift",
                "financial_drift",
            ):
                rule_fn = getattr(anomaly_rules, rule_name, None)
                if not callable(rule_fn):
                    continue
                try:
                    anomaly = rule_fn(rule_inputs, runtime_state if isinstance(runtime_state, dict) else {})
                except Exception:
                    logger.exception("anomaly rule %s failed", rule_name)
                    continue
                if isinstance(anomaly, dict):
                    collected.append(anomaly)

        evaluator = getattr(self.anomaly_engine, "evaluate", None)
        if callable(evaluator):
            try:
                evaluated = evaluator(context) or []
                for anomaly in evaluated:
                    if isinstance(anomaly, dict):
                        collected.append(anomaly)
            except Exception:
                logger.exception("anomaly engine evaluate failed")

        if collected:
            logger.warning(
                "watchdog anomaly hook collected anomalies",
                extra={"anomalies": sanitize_value(collected)},
            )
        return collected

    def _build_anomaly_context(self) -> dict[str, Any]:
        runtime_state: dict[str, Any] = {}
        collector = getattr(self.probe, "collect_runtime_state", None)
        if callable(collector):
            try:
                runtime_state = collector() or {}
            except Exception:
                logger.exception("collect_runtime_state failed during anomaly review")

        context = {
            "health": self.health_registry.snapshot(),
            "metrics": self.metrics_registry.snapshot(),
            "alerts": self.alerts_manager.snapshot(),
            "incidents": self.incidents_manager.snapshot(),
            "runtime_state": runtime_state,
        }
        reviewer_ctx_getter = getattr(self.probe, "collect_reviewer_context", None)
        if callable(reviewer_ctx_getter):
            try:
                reviewer_ctx = reviewer_ctx_getter() or {}
                if isinstance(reviewer_ctx, dict):
                    for key, value in reviewer_ctx.items():
                        existing = context.get(key)
                        if isinstance(existing, dict) and isinstance(value, dict):
                            context[key] = {**existing, **value}
                        else:
                            context[key] = value
            except Exception:
                logger.exception("collect_reviewer_context failed")
        if callable(self.anomaly_context_provider):
            try:
                extra = self.anomaly_context_provider() or {}
                if isinstance(extra, dict):
                    context.update(extra)
            except Exception:
                logger.exception("anomaly_context_provider failed")
        return context

    def _evaluate_invariants(self) -> None:
        # Build state from health/metrics/runtime_state
        state: dict[str, Any] = {}
        health = self.health_registry.snapshot()
        metrics = self.metrics_registry.snapshot()
        gauges = metrics.get("gauges", {}) if isinstance(metrics, dict) else {}
        state["health"] = health
        state["metrics"] = gauges
        state["runtime"] = {"status": health.get("overall_status", "NOT_READY")}
        state["inflight_count"] = float(gauges.get("inflight_count", 0.0))

        runtime_state_missing_reason: str | None = None
        runtime_state_payload: dict[str, Any] = {}

        # Collect runtime state (includes recent_orders if available)
        collector = getattr(self.probe, "collect_runtime_state", None)
        if not callable(collector):
            runtime_state_missing_reason = "collect_runtime_state unavailable"
        else:
            try:
                collected = collector()
                if collected is None:
                    runtime_state_missing_reason = "collect_runtime_state returned null"
                elif not isinstance(collected, dict):
                    runtime_state_missing_reason = "collect_runtime_state returned non-mapping payload"
                elif len(collected) == 0:
                    runtime_state_missing_reason = "collect_runtime_state returned empty payload"
                else:
                    runtime_state_payload = collected
                    state.update(runtime_state_payload)
            except Exception:
                runtime_state_missing_reason = "collect_runtime_state raised exception"
                logger.exception("collect_runtime_state failed during invariant review")

        _INPUT_MISSING_CODE = "INVARIANT_INPUT_MISSING"
        current_codes: set[str] = set()
        if runtime_state_missing_reason:
            current_codes.add(_INPUT_MISSING_CODE)
            self.alerts_manager.upsert_alert(
                _INPUT_MISSING_CODE,
                "critical",
                "Invariant reviewer missing runtime_state input; refusing silent success",
                source="invariant_reviewer",
                details={"reason": runtime_state_missing_reason},
            )
            self.incidents_manager.open_incident(
                _INPUT_MISSING_CODE,
                _INPUT_MISSING_CODE,
                "critical",
                details={"reason": runtime_state_missing_reason},
            )

        # Fail-loud: if a custom checks list was provided but resolves to zero
        # checks, emit a structured operational alert so the misconfiguration
        # is visible rather than silently producing zero findings every tick.
        effective_checks = (
            tuple(self._invariant_checks)
            if self._invariant_checks is not None
            else DEFAULT_INVARIANT_CHECKS
        )
        _MISCONFIG_CODE = "INVARIANT_CHECKS_MISCONFIGURED"
        if len(effective_checks) == 0:
            self.alerts_manager.upsert_alert(
                _MISCONFIG_CODE,
                "warning",
                "Invariant reviewer has zero checks configured — pass is a no-op",
                source="invariant_reviewer",
                details={"checks_count": 0},
            )
            logger.warning(
                "invariant reviewer: zero checks configured, "
                "pass is a silent no-op — check invariant_checks parameter"
            )

        violations = evaluate_invariants(state, enabled=True, checks=self._invariant_checks)
        # Seed current_codes with the misconfiguration sentinel so the stale-cleanup
        # loop does not immediately resolve it in the same tick.
        if len(effective_checks) == 0:
            current_codes.add(_MISCONFIG_CODE)

        for violation in violations:
            violation_code = violation.code
            # Keep runtime invariant code canonical in violation details while
            # preserving distinct alert keys across reviewers in AlertsManager.
            code = (
                "INVARIANT_EXPOSURE_MISMATCH"
                if violation_code == "EXPOSURE_MISMATCH"
                else violation_code
            )
            current_codes.add(code)
            # Severity: critical for regression/inconsistency codes, else warning
            lower_code = violation_code.lower()
            if "regression" in lower_code or "inconsistent" in lower_code:
                severity = "critical"
            else:
                severity = "warning"
            self.alerts_manager.upsert_alert(
                code,
                severity,
                violation.message,
                source="invariant_reviewer",
                details={"violation_code": violation_code},
            )
            if severity == "critical":
                self.incidents_manager.open_incident(code, code, severity)

        # Resolve stale invariant alerts (close incidents too)
        active_alerts: list[dict[str, Any]] = []
        active_getter = getattr(self.alerts_manager, "active_alerts", None)
        if callable(active_getter):
            try:
                active_alerts = active_getter() or []
            except Exception:
                logger.exception("active_alerts failed during invariant resolution")
        for item in active_alerts:
            if str(item.get("source", "")) != "invariant_reviewer":
                continue
            code = str(item.get("code", "") or "")
            if code and code not in current_codes:
                self.alerts_manager.resolve_alert(code)
                self.incidents_manager.close_incident(
                    code,
                    reason="invariant_cleared",
                    resolved_by="invariant_reviewer",
                )

        self._managed_invariant_alert_codes = current_codes

    def _evaluate_correlations(self) -> None:
        context = self._build_anomaly_context()
        current_codes: set[str] = set()

        corr_enabled = context.get("correlation_reviewer_enabled", True)
        if not bool(corr_enabled):
            findings = [{
                "code": "CORRELATION_REVIEWER_DISABLED",
                "severity": "critical",
                "message": "Correlation reviewer is DISABLED; fail-closed blocker raised",
                "details": {"correlation_reviewer_enabled": False, "suppressed_this_tick": True},
            }]
        else:
            findings = None

        # Enrich with strongly-typed direct evidence from live runtime collectors.
        # Direct values (queue depth, published total, subscriber errors, DB write
        # queue stats) take precedence over loose injected gauges when both are present.
        corr_ctx_getter = getattr(self.probe, "collect_correlation_context", None)
        if callable(corr_ctx_getter):
            try:
                direct_evidence = corr_ctx_getter() or {}
                if isinstance(direct_evidence, dict):
                    for key, value in direct_evidence.items():
                        existing = context.get(key)
                        if isinstance(existing, dict) and isinstance(value, dict):
                            context[key] = {**existing, **value}
                        else:
                            context[key] = value
            except Exception:
                logger.exception("collect_correlation_context failed")

        if findings is None:
            evaluator = self._correlation_evaluator
            if evaluator is None:
                code = "CORRELATION_REVIEWER_MISSING"
                findings = [{
                    "code": code,
                    "severity": "critical",
                    "message": "Correlation reviewer evaluator is missing",
                    "details": {"evaluator": None},
                }]
            else:
                evaluate = getattr(evaluator, "evaluate", None)
                if not callable(evaluate):
                    code = "CORRELATION_REVIEWER_UNAVAILABLE"
                    findings = [{
                        "code": code,
                        "severity": "critical",
                        "message": "Correlation reviewer evaluator is unavailable",
                        "details": {"evaluator_type": type(evaluator).__name__},
                    }]
                else:
                    findings = evaluate(context)

        for finding in findings:
            code = str(finding.get("code", "") or "")
            if not code:
                continue
            current_codes.add(code)
            severity = str(finding.get("severity", "warning") or "warning").lower()
            message = str(finding.get("message", code) or code)
            details = finding.get("details") or {}
            self.alerts_manager.upsert_alert(
                code,
                severity,
                message,
                source="correlation_reviewer",
                details=details,
            )
            if severity in {"critical", "error"}:
                self.incidents_manager.open_incident(code, code, severity, details=details)

        # Resolve stale correlation alerts (close incidents too)
        active_alerts: list[dict[str, Any]] = []
        active_getter = getattr(self.alerts_manager, "active_alerts", None)
        if callable(active_getter):
            try:
                active_alerts = active_getter() or []
            except Exception:
                logger.exception("active_alerts failed during correlation resolution")
        for item in active_alerts:
            if str(item.get("source", "")) != "correlation_reviewer":
                continue
            code = str(item.get("code", "") or "")
            if code and code not in current_codes:
                self.alerts_manager.resolve_alert(code)
                self.incidents_manager.close_incident(
                    code,
                    reason="finding_cleared",
                    resolved_by="correlation_reviewer",
                )

        self._managed_correlation_alert_codes = current_codes

    def _evaluate_alerts(self) -> None:
        health = self.health_registry.snapshot()
        metrics = self.metrics_registry.snapshot()
        runtime_state = {}
        collector = getattr(self.probe, "collect_runtime_state", None)
        if callable(collector):
            try:
                runtime_state = collector() or {}
            except Exception:
                logger.exception("collect_runtime_state failed during alert evaluation")

        overall = health.get("overall_status")
        if overall == "NOT_READY":
            self.alerts_manager.upsert_alert(
                "SYSTEM_NOT_READY",
                "critical",
                "System not ready",
                details={"overall_status": overall},
            )
            self.incidents_manager.open_incident("SYSTEM_NOT_READY", "System Not Ready", "critical")
        else:
            self.alerts_manager.resolve_alert("SYSTEM_NOT_READY")
            self.incidents_manager.close_incident(
                "SYSTEM_NOT_READY",
                reason="system_ready",
                resolved_by="alert_reviewer",
            )

        gauges = metrics.get("gauges", {}) if isinstance(metrics, dict) else {}
        memory_rss = float(gauges.get("memory_rss_mb", 0.0))
        if memory_rss >= 800:
            self.alerts_manager.upsert_alert(
                "MEMORY_HIGH",
                "critical",
                "Memory usage critically high",
                details={"memory_rss_mb": memory_rss},
            )
        elif memory_rss >= 500:
            self.alerts_manager.upsert_alert(
                "MEMORY_HIGH",
                "warning",
                "Memory usage high",
                details={"memory_rss_mb": memory_rss},
            )
        else:
            self.alerts_manager.resolve_alert("MEMORY_HIGH")

        inflight = float(gauges.get("inflight_count", 0.0))
        if inflight >= 50:
            self.alerts_manager.upsert_alert(
                "INFLIGHT_HIGH",
                "warning",
                "Too many inflight orders",
                details={"inflight_count": inflight},
            )
        else:
            self.alerts_manager.resolve_alert("INFLIGHT_HIGH")

        session_snapshot = {}
        if isinstance(runtime_state, dict):
            session_snapshot = (
                runtime_state.get("session_manager")
                or runtime_state.get("session")
                or runtime_state.get("betfair_session")
                or {}
            )
        if not isinstance(session_snapshot, dict):
            session_snapshot = {}

        state = str(session_snapshot.get("state") or "").upper()
        keepalive_failures = int(session_snapshot.get("consecutive_keepalive_failures") or 0)
        login_failures = int(session_snapshot.get("consecutive_login_failures") or 0)
        last_error_code = str(session_snapshot.get("last_error_code") or "").upper()
        ttl = float(session_snapshot.get("session_ttl_sec") or 0.0)
        expiry = session_snapshot.get("session_expires_at")
        logged_in_at = session_snapshot.get("logged_in_at")
        token_present = bool(session_snapshot.get("token_present"))

        expired = state == "EXPIRED"
        if ttl > 0 and logged_in_at is not None and expiry is not None:
            try:
                expired = expired or float(expiry) <= float(logged_in_at)
            except Exception:
                pass

        if keepalive_failures > 0 and state not in {"ACTIVE"}:
            self.alerts_manager.upsert_alert(
                "SESSION_KEEPALIVE_FAILED",
                "warning",
                "Betfair session keepalive failure detected",
                source="session_manager",
                details={"consecutive_keepalive_failures": keepalive_failures, "state": state},
            )
        else:
            self.alerts_manager.resolve_alert("SESSION_KEEPALIVE_FAILED")

        if expired or (state in {"LOGGED_OUT", "EXPIRED", "LOCKED_OUT"} and not token_present):
            self.alerts_manager.upsert_alert(
                "SESSION_EXPIRED",
                "warning",
                "Betfair session is expired or unavailable",
                source="session_manager",
                details={"state": state, "last_error_code": last_error_code},
            )
        else:
            self.alerts_manager.resolve_alert("SESSION_EXPIRED")

        if login_failures > 0 and state in {"DEGRADED", "EXPIRED"}:
            self.alerts_manager.upsert_alert(
                "SESSION_RELOGIN_FAILED",
                "warning",
                "Betfair session relogin failed",
                source="session_manager",
                details={"consecutive_login_failures": login_failures, "state": state},
            )
        else:
            self.alerts_manager.resolve_alert("SESSION_RELOGIN_FAILED")

        if state == "LOCKED_OUT" or last_error_code == "TEMPORARY_BAN_TOO_MANY_REQUESTS":
            self.alerts_manager.upsert_alert(
                "SESSION_LOGIN_THROTTLED",
                "warning",
                "Betfair login is throttled/locked out",
                source="session_manager",
                details={"state": state, "last_error_code": last_error_code},
            )
        else:
            self.alerts_manager.resolve_alert("SESSION_LOGIN_THROTTLED")

    def _evaluate_anomalies(self) -> None:
        anomalies = self._run_anomaly_checks()
        self.last_anomalies = anomalies

        current_codes: set[str] = set()
        for anomaly in anomalies:
            code = anomaly.get("code") or anomaly.get("name") or anomaly.get("type")
            if code is None:
                continue
            code = str(code)
            current_codes.add(code)
            severity = str(anomaly.get("severity", "warning") or "warning")
            message = str(anomaly.get("description") or anomaly.get("message") or code)
            details = anomaly.get("details") if isinstance(anomaly.get("details"), dict) else {}

            self.alerts_manager.upsert_alert(
                code,
                severity,
                message,
                source="anomaly",
                description=message,
                details=details,
            )
            self._emit_anomaly_alert(
                code=code,
                severity=severity,
                message=message,
                details=details,
            )
            if str(severity).lower() in {"critical", "error"}:
                self.incidents_manager.open_incident(code, code, severity, details=details)

        stale_codes = self._managed_anomaly_alert_codes - current_codes
        for code in stale_codes:
            self.alerts_manager.resolve_alert(code)
            self.incidents_manager.close_incident(
                code,
                reason="anomaly_cleared",
                resolved_by="anomaly_reviewer",
            )

        self._managed_anomaly_alert_codes = current_codes

    def _emit_anomaly_alert(self, *, code: str, severity: str, message: str, details: Any) -> None:
        if not self._is_anomaly_alerts_enabled():
            return

        notify_alert = getattr(self.anomaly_alert_service, "notify_alert", None)
        if not callable(notify_alert):
            logger.info("Anomaly alert path unavailable; falling back to logs", extra={"code": code})
            return

        payload = {
            "code": code,
            "severity": str(severity or "warning"),
            "source": "watchdog_service",
            "description": str(message or code),
            "details": details if isinstance(details, dict) else {"details": details},
            "type": "anomaly",
        }

        try:
            notify_alert(payload)
        except Exception:
            logger.exception("Anomaly alert emission failed", extra={"code": code})

    def _maybe_run_anomaly_actions(self, anomalies: list[dict[str, Any]]) -> None:
        self.escalation_requested = False
        self.last_escalation_event = None

        if not anomalies or not self._is_anomaly_actions_enabled():
            return

        first = anomalies[0]
        event = {
            "code": str(first.get("code") or first.get("name") or "UNKNOWN_ANOMALY"),
            "severity": str(first.get("severity") or "warning"),
            "source": str(first.get("source") or "watchdog_service"),
            "escalation_requested": True,
            "reason": str(first.get("description") or first.get("message") or "anomaly detected"),
            "details": first.get("details") if isinstance(first.get("details"), dict) else {},
        }

        self.escalation_requested = True
        self.last_escalation_event = event
        logger.warning("Anomaly escalation requested", extra={"event": event})

        hook = getattr(self.anomaly_escalation_hook, "__call__", None)
        if callable(hook):
            try:
                self.anomaly_escalation_hook(dict(event))
            except Exception:
                logger.exception("Anomaly escalation hook failed")

    def _evaluate_forensics(self) -> None:
        if self.forensics_engine is None:
            return

        runtime_state = {}
        collector = getattr(self.probe, "collect_runtime_state", None)
        if callable(collector):
            try:
                runtime_state = collector() or {}
            except Exception:
                logger.exception("collect_runtime_state failed during forensics review")

        context = {
            "health": self.health_registry.snapshot(),
            "metrics": self.metrics_registry.snapshot(),
            "alerts": self.alerts_manager.snapshot(),
            "incidents": self.incidents_manager.snapshot(),
            "runtime_state": runtime_state,
        }

        evidence_getter = getattr(self.probe, "collect_forensics_evidence", None)
        if callable(evidence_getter):
            try:
                evidence = evidence_getter() or {}
                if isinstance(evidence, dict):
                    context.update(evidence)
            except Exception:
                logger.exception("collect_forensics_evidence failed")

        findings = self.forensics_engine.evaluate(context)
        self.last_forensics_findings = [item for item in findings if isinstance(item, dict)]
        current_codes = set()
        for finding in findings:
            code = str(finding.get("code", "") or "")
            if not code:
                continue
            current_codes.add(code)
            severity = str(finding.get("severity", "warning") or "warning").lower()
            message = str(finding.get("message", code) or code)
            details = finding.get("details") or {}
            self.alerts_manager.upsert_alert(
                code,
                severity,
                message,
                source="forensics_reviewer",
                title=code,
                details=details,
            )
            if severity in {"critical", "error"}:
                self.incidents_manager.open_incident(code, code, severity, details=details)

        active_alerts = []
        active_getter = getattr(self.alerts_manager, "active_alerts", None)
        if callable(active_getter):
            try:
                active_alerts = active_getter() or []
            except Exception:
                logger.exception("active_alerts failed during forensics resolution")
        for item in active_alerts:
            if str(item.get("source", "")) != "forensics_reviewer":
                continue
            code = str(item.get("code", "") or "")
            if code and code not in current_codes:
                self.alerts_manager.resolve_alert(code)
                self.incidents_manager.close_incident(
                    code,
                    reason="finding_cleared",
                    resolved_by="forensics_reviewer",
                )

    def _evaluate_cto_reviewer(self) -> None:
        runtime_collector = getattr(self.probe, "collect_runtime_state", None)
        runtime_state = {}
        if callable(runtime_collector):
            try:
                runtime_state = runtime_collector() or {}
            except Exception:
                logger.exception("collect_runtime_state failed during CTO reviewer pass")
                runtime_state = {}
        forensics_getter = getattr(self.probe, "collect_forensics_evidence", None)
        diagnostics_bundle = {}
        if callable(forensics_getter):
            try:
                evidence = forensics_getter() or {}
                if isinstance(evidence, dict):
                    diagnostics_bundle = dict(evidence.get("diagnostics_export") or {})
                    if "available" not in diagnostics_bundle:
                        manifest_files = diagnostics_bundle.get("manifest_files") or []
                        diagnostics_bundle["available"] = bool(manifest_files)
            except Exception:
                diagnostics_bundle = {}
        payload = {
            "health_snapshot": self.health_registry.snapshot(),
            "metrics_snapshot": self.metrics_registry.snapshot(),
            "anomaly_alerts": list(self.last_anomalies or []),
            "forensics_alerts": list(self.last_forensics_findings or []),
            "incidents_snapshot": self.incidents_manager.snapshot(),
            "runtime_probe_state": runtime_state or {},
            "diagnostics_bundle": diagnostics_bundle,
        }
        findings = self._cto_reviewer.evaluate(payload)
        active_rule_names = self._cto_reviewer.current_rule_names(payload)
        current_codes: set[str] = {f"CTO::{name}" for name in active_rule_names}
        for finding in findings:
            rule_name = str(finding.get("rule_name") or "")
            if not rule_name:
                continue
            code = f"CTO::{rule_name}"
            severity = str(finding.get("severity") or "warning")
            self.alerts_manager.upsert_alert(
                code,
                severity,
                str(finding.get("short_explanation") or rule_name),
                source="cto_reviewer",
                details=dict(finding),
            )
            if severity.lower() in {"high", "critical", "error"}:
                self.incidents_manager.open_incident(code, code, severity, details=dict(finding))
        stale_codes = self._managed_cto_alert_codes - current_codes
        for code in stale_codes:
            self.alerts_manager.resolve_alert(code)
            self.incidents_manager.close_incident(code, reason="cto_finding_cleared", resolved_by="cto_reviewer")
        self._managed_cto_alert_codes = current_codes

    def _evaluate_reviewer_governance(self) -> None:
        active = []
        getter = getattr(self.alerts_manager, "active_alerts", None)
        if callable(getter):
            try:
                active = getter() or []
            except Exception:
                logger.exception("active_alerts failed during reviewer governance pass")

        reviewer_sources = {"anomaly", "invariant_reviewer", "correlation_reviewer", "forensics_reviewer"}
        findings = [a for a in active if str(a.get("source", "")) in reviewer_sources]
        normalized = [self._reviewer_policy.normalize(f) for f in findings]
        grouped: dict[str, list[dict[str, Any]]] = {}
        for finding in normalized:
            key = self._reviewer_policy.grouping_key(finding)
            grouped.setdefault(key, []).append(finding)
        all_codes = {item["code"] for item in normalized}
        if {"GHOST_ORDER_DETECTED", "LOCAL_VS_REMOTE_MISMATCH"} <= all_codes or {"GHOST_ORDER_SUSPECTED", "LOCAL_VS_REMOTE_MISMATCH"} <= all_codes:
            preferred_key = next(
                (k for k, rows in grouped.items() if any(r["code"] == "LOCAL_VS_REMOTE_MISMATCH" for r in rows)),
                "global:execution",
            )
            synthetic = [r for r in normalized if r["code"] in {"GHOST_ORDER_DETECTED", "GHOST_ORDER_SUSPECTED", "LOCAL_VS_REMOTE_MISMATCH", "AMBIGUOUS_LOCAL_REMOTE_INCONSISTENCY"}]
            if synthetic:
                existing_rows = list(grouped.get(preferred_key, []))
                merged_rows = existing_rows + synthetic
                deduped: list[dict[str, Any]] = []
                seen: set[tuple[str, str, str]] = set()
                for row in merged_rows:
                    key = (
                        str(row.get("code", "")),
                        str(row.get("source", "")),
                        str((row.get("details") or {}).get("grouping_key", "")),
                    )
                    if key in seen:
                        continue
                    seen.add(key)
                    deduped.append(row)
                grouped[preferred_key] = deduped
        if {"DB_VS_MEMORY_MISMATCH", "DIAGNOSTICS_BUNDLE_EVIDENCE_GAP"} <= all_codes:
            preferred_key = next(
                (k for k, rows in grouped.items() if any(r["code"] == "DB_VS_MEMORY_MISMATCH" for r in rows)),
                "global:observability",
            )
            synthetic = [r for r in normalized if r["code"] in {"DB_VS_MEMORY_MISMATCH", "EVENT_SIDE_EFFECT_GAP", "DIAGNOSTICS_BUNDLE_EVIDENCE_GAP"}]
            if synthetic:
                grouped[preferred_key] = synthetic
        if "QUEUE_DEPTH_LIVENESS_CONTRADICTION" in all_codes and (
            "HEARTBEAT_STALE" in all_codes or "QUEUE_DEPTH_DISPATCHER_CONTRADICTION" in all_codes
        ):
            preferred_key = next(
                (k for k, rows in grouped.items() if any(r["code"] == "QUEUE_DEPTH_LIVENESS_CONTRADICTION" for r in rows)),
                "global:liveness",
            )
            synthetic = [
                r for r in normalized
                if r["code"] in {"QUEUE_DEPTH_LIVENESS_CONTRADICTION", "QUEUE_DEPTH_DISPATCHER_CONTRADICTION", "HEARTBEAT_STALE", "SERVICE_STALLED", "ZOMBIE_WORKER_SUSPECTED"}
            ]
            if synthetic:
                merged_rows = list(grouped.get(preferred_key, [])) + synthetic
                grouped[preferred_key] = merged_rows
        if "EVENT_SIDE_EFFECT_GAP" in all_codes and (
            "POISON_PILL_SUBSCRIBER" in all_codes or "EVENT_FANOUT_INCOMPLETE" in all_codes
        ):
            preferred_key = next(
                (k for k, rows in grouped.items() if any(r["code"] == "EVENT_SIDE_EFFECT_GAP" for r in rows)),
                "global:dispatch",
            )
            synthetic = [
                r for r in normalized
                if r["code"] in {"EVENT_SIDE_EFFECT_GAP", "POISON_PILL_SUBSCRIBER", "EVENT_FANOUT_INCOMPLETE"}
            ]
            if synthetic:
                merged_rows = list(grouped.get(preferred_key, [])) + synthetic
                grouped[preferred_key] = merged_rows
        if (
            ("FINANCIAL_DRIFT" in all_codes or "FINANCIAL_DRIFT_DETECTED" in all_codes or "EXPOSURE_MISMATCH" in all_codes or "INVARIANT_EXPOSURE_MISMATCH" in all_codes)
            and ("LOCAL_VS_REMOTE_MISMATCH" in all_codes or "GHOST_ORDER_SUSPECTED" in all_codes or "GHOST_ORDER_DETECTED" in all_codes)
        ):
            preferred_key = next(
                (k for k, rows in grouped.items() if any(r["code"] in {"LOCAL_VS_REMOTE_MISMATCH", "GHOST_ORDER_SUSPECTED", "GHOST_ORDER_DETECTED"} for r in rows)),
                "global:financial",
            )
            synthetic = [
                r for r in normalized
                if r["code"] in {"FINANCIAL_DRIFT", "FINANCIAL_DRIFT_DETECTED", "EXPOSURE_MISMATCH", "INVARIANT_EXPOSURE_MISMATCH", "LOCAL_VS_REMOTE_MISMATCH", "GHOST_ORDER_SUSPECTED", "GHOST_ORDER_DETECTED"}
            ]
            if synthetic:
                merged_rows = list(grouped.get(preferred_key, [])) + synthetic
                grouped[preferred_key] = merged_rows

        current_codes: set[str] = set()
        delivery = self._delivery_status_snapshot()
        for grouping_key, rows in sorted(grouped.items(), key=lambda x: x[0]):
            codes = {r["code"] for r in rows}
            max_rank = max(r["normalized_rank"] for r in rows)
            incident_class, policy_class, floor_severity, why_it_matters = self._reviewer_policy.classify_group(codes)
            max_rank = max(max_rank, _SEVERITY_RANK.get(floor_severity, _SEVERITY_RANK["warning"]))
            normalized_severity = _RANK_SEVERITY[max_rank]
            alert_code = f"REVIEWER_GOVERNANCE::{incident_class}::{grouping_key}"
            current_codes.add(alert_code)
            requires_delivery = policy_class.startswith("mandatory_delivery")
            degraded_reason = self._delivery_degraded_reason(delivery) if requires_delivery else None
            delivery_status = "ready"
            if requires_delivery and degraded_reason:
                delivery_status = "degraded"
            elif requires_delivery and not degraded_reason:
                delivery_status = "required_ready"
            details = {
                "normalized_severity": normalized_severity,
                "incident_class": incident_class,
                "triggering_finding_codes": sorted(codes),
                "grouping_key": grouping_key,
                "why_it_matters": why_it_matters,
                "recommended_action": f"Follow runbook for {incident_class} and verify signal convergence.",
                "runbook_hint": f"runbook://reviewer/{incident_class}",
                "governance_decision": "centralized_reviewer_policy",
                "source_summary": sorted({r['source'] for r in rows}),
                "contributing_sources": sorted({r['source'] for r in rows}),
                "delivery_required": requires_delivery,
                "delivery_policy_class": policy_class,
                "delivery_status": delivery_status,
                "degraded_reason": degraded_reason,
                "policy_source": "watchdog.reviewer_governance.v1",
                "delivery_affects_governance": bool(requires_delivery and degraded_reason),
            }
            sev = "critical" if normalized_severity == "critical" else ("high" if normalized_severity == "high" else "warning")
            self.alerts_manager.upsert_alert(
                alert_code,
                sev,
                f"{incident_class} detected for {grouping_key}",
                source="reviewer_governance",
                details=details,
            )
            if policy_class != "alert_only":
                self.incidents_manager.open_incident(
                    alert_code,
                    incident_class,
                    sev,
                    details=details,
                )
            if requires_delivery and degraded_reason:
                fail_closed_code = f"REVIEWER_DELIVERY_DEGRADED::{incident_class}::{grouping_key}"
                current_codes.add(fail_closed_code)
                fail_closed_details = dict(details)
                fail_closed_details["governance_decision"] = "mandatory_delivery_degraded_fail_closed"
                fail_closed_details["transport_only_failure"] = True
                self.alerts_manager.upsert_alert(
                    fail_closed_code,
                    "critical",
                    f"Mandatory reviewer delivery degraded: {incident_class}",
                    source="reviewer_governance",
                    details=fail_closed_details,
                )
                self.incidents_manager.open_incident(
                    fail_closed_code,
                    "reviewer_delivery_governance_incident",
                    "critical",
                    details=fail_closed_details,
                )

        stale = self._managed_governance_alert_codes - current_codes
        for code in stale:
            self.alerts_manager.resolve_alert(code)
            self.incidents_manager.close_incident(
                code,
                reason="grouped_reviewer_condition_cleared",
                resolved_by="reviewer_governance",
            )
        self._managed_governance_alert_codes = current_codes

    def _delivery_status_snapshot(self) -> dict[str, Any]:
        availability = {}
        getter = getattr(self.anomaly_alert_service, "availability_status", None)
        if callable(getter):
            try:
                availability = getter() or {}
            except Exception:
                logger.exception("availability_status failed during governance")
        if not availability:
            context = self._build_anomaly_context()
            runtime_state = context.get("runtime_state") if isinstance(context, dict) else {}
            if isinstance(runtime_state, dict):
                availability = dict(runtime_state.get("alert_pipeline") or {})
        return availability

    def _delivery_degraded_reason(self, delivery: dict[str, Any]) -> str | None:
        if not isinstance(delivery, dict) or not delivery:
            return "delivery_state_unknown"
        if not bool(delivery.get("alerts_enabled", False)):
            return "alerts_disabled"
        if not bool(delivery.get("sender_available", False)):
            return "sender_unavailable"
        if not bool(delivery.get("deliverable", False)):
            return str(delivery.get("reason") or "transport_not_deliverable")
        if delivery.get("last_delivery_ok") is False and delivery.get("last_delivery_error"):
            return f"last_delivery_failed:{delivery.get('last_delivery_error')}"
        return None
