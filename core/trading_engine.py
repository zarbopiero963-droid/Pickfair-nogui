from __future__ import annotations

import logging
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, Optional, Set

from order_manager import OrderManager

logger = logging.getLogger(__name__)


# =========================================================
# CONSTANTS
# =========================================================

REQ_QUICK_BET = "REQ_QUICK_BET"
CMD_QUICK_BET = "CMD_QUICK_BET"

STATUS_INFLIGHT = "INFLIGHT"
STATUS_SUBMITTED = "SUBMITTED"
STATUS_MATCHED = "MATCHED"
STATUS_COMPLETED = "COMPLETED"
STATUS_FAILED = "FAILED"
STATUS_AMBIGUOUS = "AMBIGUOUS"
STATUS_DENIED = "DENIED"
STATUS_ACCEPTED_FOR_PROCESSING = "ACCEPTED_FOR_PROCESSING"
STATUS_DUPLICATE_BLOCKED = "DUPLICATE_BLOCKED"

OUTCOME_SUCCESS = "SUCCESS"
OUTCOME_FAILURE = "FAILURE"
OUTCOME_AMBIGUOUS = "AMBIGUOUS"

ERROR_TRANSIENT = "TRANSIENT"
ERROR_PERMANENT = "PERMANENT"
ERROR_AMBIGUOUS = "AMBIGUOUS"

READY = "READY"
DEGRADED = "DEGRADED"
NOT_READY = "NOT_READY"

AMBIGUITY_SUBMIT_TIMEOUT = "SUBMIT_TIMEOUT"
AMBIGUITY_RESPONSE_LOST = "RESPONSE_LOST"
AMBIGUITY_SUBMIT_UNKNOWN = "SUBMIT_UNKNOWN"
AMBIGUITY_PERSISTED_NOT_CONFIRMED = "PERSISTED_NOT_CONFIRMED"
AMBIGUITY_SPLIT_STATE = "SPLIT_STATE"

# =========================================================
# STATE MACHINE
# =========================================================

ALLOWED_TRANSITIONS: Dict[str, Set[str]] = {
    STATUS_INFLIGHT: {STATUS_SUBMITTED, STATUS_FAILED, STATUS_AMBIGUOUS, STATUS_DENIED},
    STATUS_SUBMITTED: {STATUS_MATCHED, STATUS_COMPLETED, STATUS_FAILED, STATUS_AMBIGUOUS},
    STATUS_MATCHED: {STATUS_COMPLETED, STATUS_FAILED},
    STATUS_AMBIGUOUS: {STATUS_COMPLETED, STATUS_FAILED},
    STATUS_DENIED: set(),
    STATUS_FAILED: set(),
    STATUS_COMPLETED: set(),
}

_INTERNAL_TO_PUBLIC_STATUS: Dict[str, str] = {
    STATUS_INFLIGHT: STATUS_INFLIGHT,
    STATUS_SUBMITTED: STATUS_ACCEPTED_FOR_PROCESSING,
    STATUS_ACCEPTED_FOR_PROCESSING: STATUS_ACCEPTED_FOR_PROCESSING,
    STATUS_MATCHED: STATUS_MATCHED,
    STATUS_COMPLETED: STATUS_COMPLETED,
    STATUS_FAILED: STATUS_FAILED,
    STATUS_AMBIGUOUS: STATUS_AMBIGUOUS,
    STATUS_DENIED: STATUS_DENIED,
    STATUS_DUPLICATE_BLOCKED: STATUS_DUPLICATE_BLOCKED,
}

# Point 4: _STATUS_TO_OUTCOME governs all outcome derivation centrally.
# _complete_order_lifecycle uses this — manual outcome override is forbidden
# when status is present in this table.
_STATUS_TO_OUTCOME: Dict[str, str] = {
    STATUS_SUBMITTED: OUTCOME_SUCCESS,
    STATUS_COMPLETED: OUTCOME_SUCCESS,
    STATUS_MATCHED: OUTCOME_SUCCESS,
    STATUS_FAILED: OUTCOME_FAILURE,
    STATUS_DENIED: OUTCOME_FAILURE,
    STATUS_AMBIGUOUS: OUTCOME_AMBIGUOUS,
    STATUS_DUPLICATE_BLOCKED: OUTCOME_SUCCESS,
}

# =========================================================
# EXECUTION CONTEXT / ERRORS
# =========================================================

@dataclass(frozen=True)
class _ExecutionContext:
    """Immutable. Created ONLY inside _submit_via_engine."""
    correlation_id: str
    customer_ref: str
    created_at: float
    event_key: Optional[str] = None
    simulation_mode: Optional[bool] = None


class ExecutionError(Exception):
    def __init__(self, message: str, *, error_type: str = ERROR_PERMANENT,
                 ambiguity_reason: Optional[str] = None) -> None:
        super().__init__(message)
        self.error_type = error_type
        self.ambiguity_reason = ambiguity_reason


# =========================================================
# NO-OP FALLBACKS
# =========================================================

class _NullSafeMode:
    def is_enabled(self) -> bool: return False
    def is_ready(self) -> bool: return True

class _NullRiskMiddleware:
    def check(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return {"allowed": True, "reason": None, "payload": payload}
    def is_ready(self) -> bool: return True

class _NullReconciliationEngine:
    def enqueue(self, **_kw: Any) -> None: return None
    def is_ready(self) -> bool: return True

class _NullStateRecovery:
    def recover(self) -> Dict[str, Any]: return {"ok": True, "reason": None}
    def is_ready(self) -> bool: return True

class _NullAsyncDbWriter:
    def write(self, *_a: Any, **_kw: Any) -> None: return None
    def is_ready(self) -> bool: return True


# =========================================================
# TRADING ENGINE
# =========================================================
#
# ARCHITECTURAL RULES:
#
# 1. ALL terminal paths go through _complete_order_lifecycle().
# 2. ALL state transitions go through _transition_order().
# 3. ALL metadata-only writes go through _write_order_metadata().
# 4. ALL outcomes derived from _STATUS_TO_OUTCOME — no manual override.
# 5. ALL results built by _build_result().
# 6. ALL ambiguity resolution through _resolve_ambiguity().
# 7. ALL ctx-dependent methods call _assert_valid_ctx().
# 8. ALL audit/bus events include _ctx_metadata().
# 9. ALL executor dispatch through _execute_submit().
# 10. _emit() returns persistence status; critical paths warn on memory-only.
#
# DECLARED TRADE-OFFS:
# - Persist INFLIGHT: degraded-capable (fallback local UUID).
# - Audit persistence: best-effort with observability.
# - Dedup correlation_id RAM: bounded-window; permanent via DB.

class TradingEngine:

    def __init__(self, bus: Any, db: Any, client_getter: Any, executor: Any,
                 safe_mode: Any = None, risk_middleware: Any = None,
                 reconciliation_engine: Any = None, state_recovery: Any = None,
                 async_db_writer: Any = None) -> None:
        self.bus = bus
        self.db = db
        self.client_getter = client_getter
        self.executor = executor
        self.safe_mode = safe_mode or _NullSafeMode()
        self.risk_middleware = risk_middleware or _NullRiskMiddleware()
        self.reconciliation_engine = reconciliation_engine or _NullReconciliationEngine()
        self.state_recovery = state_recovery or _NullStateRecovery()
        self.async_db_writer = async_db_writer or _NullAsyncDbWriter()
        self.auto_generate_correlation_id: bool = True

        self.order_manager: Optional[OrderManager] = None
        self.guard: Optional[Any] = None

        self._inflight_keys: Set[str] = set()
        self._seen_correlation_ids: Set[str] = set()
        self._seen_cid_order: Deque[str] = deque()
        self._max_seen_cid_size: int = 50_000
        self._seen_cid_trim_to: int = 40_000

        self._lock = threading.Lock()
        self._runtime_state = NOT_READY
        self._health: Dict[str, Any] = {}

        self._subscribe_bus()
        self.start()

    # ==================================================================
    # READINESS — capability-level health (Point 9 in residual)
    # ==================================================================

    def start(self) -> None:
        self._health = {
            "db": self._dep(self.db, required=True),
            "client_getter": self._dep(self.client_getter, required=True),
            "executor": self._dep(self.executor, required=False),
            "safe_mode": self._dep(self.safe_mode, required=False),
            "risk_middleware": self._dep(self.risk_middleware, required=False),
            "reconciliation_engine": self._dep(self.reconciliation_engine, required=False),
            "state_recovery": self._dep(self.state_recovery, required=False),
            "async_db_writer": self._dep(self.async_db_writer, required=False),
            "db_write": self._cap(self.db, "insert_order"),
            "audit_persistence": self._cap(self.db, "insert_audit_event",
                                           fb_obj=self.async_db_writer, fb_method="write"),
        }
        req_ok = all(self._health[k]["state"] == READY for k in ("db", "client_getter"))
        if req_ok:
            states = [v["state"] for v in self._health.values()]
            self._runtime_state = READY if all(s == READY for s in states) else DEGRADED
        else:
            self._runtime_state = NOT_READY
        logger.info("TradingEngine start -> state=%s", self._runtime_state)

    def stop(self) -> None:
        self._runtime_state = NOT_READY

    def readiness(self) -> Dict[str, Any]:
        return {"state": self._runtime_state, "health": dict(self._health)}

    def _dep(self, dep: Any, *, required: bool) -> Dict[str, Any]:
        if dep is None:
            return {"state": NOT_READY if required else DEGRADED, "reason": "missing"}
        checker = getattr(dep, "is_ready", None)
        if callable(checker):
            try:
                ok = bool(checker())
                return {"state": READY if ok else (NOT_READY if required else DEGRADED),
                        "reason": None if ok else "unhealthy"}
            except Exception as e:
                return {"state": NOT_READY if required else DEGRADED, "reason": f"exception:{e}"}
        return {"state": READY, "reason": "no-checker"}

    def _cap(self, obj: Any, method: str, *, fb_obj: Any = None, fb_method: str = "") -> Dict[str, Any]:
        if obj is not None and callable(getattr(obj, method, None)):
            return {"state": READY, "reason": None}
        if fb_obj is not None and callable(getattr(fb_obj, fb_method, None)):
            return {"state": DEGRADED, "reason": f"fallback:{fb_method}"}
        return {"state": DEGRADED, "reason": f"missing:{method}"}

    def assert_ready(self) -> None:
        if self._runtime_state not in {READY, DEGRADED}:
            raise RuntimeError(f"TRADING_ENGINE_NOT_READY:{self._runtime_state}")

    # ==================================================================
    # ANTI-BYPASS (Point 6) — on ALL critical methods
    # ==================================================================

    @staticmethod
    def _assert_valid_ctx(ctx: Any) -> None:
        if not isinstance(ctx, _ExecutionContext):
            raise RuntimeError("INVALID_EXECUTION_CONTEXT")

    # ==================================================================
    # CTX METADATA (Point 8) — used everywhere
    # ==================================================================

    @staticmethod
    def _ctx_metadata(ctx: _ExecutionContext) -> Dict[str, Any]:
        return {"correlation_id": ctx.correlation_id, "customer_ref": ctx.customer_ref,
                "event_key": ctx.event_key, "simulation_mode": ctx.simulation_mode}

    @staticmethod
    def _public_status(internal: str) -> str:
        return _INTERNAL_TO_PUBLIC_STATUS.get(internal, internal)

    # ==================================================================
    # RESULT BUILDER (Point 3 extended: works for order AND system paths)
    # ==================================================================

    def _build_result(self, ctx: _ExecutionContext, audit: Dict[str, Any], *,
                      status: str, outcome: str,
                      order_id: Optional[Any] = None,
                      reason: Optional[str] = None, error: Optional[str] = None,
                      ambiguity_reason: Optional[str] = None,
                      response: Optional[Any] = None,
                      extra_fields: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        self._assert_valid_ctx(ctx)
        public_audit = {k: v for k, v in audit.items() if not k.startswith("_")}
        result: Dict[str, Any] = {
            "ok": outcome == OUTCOME_SUCCESS,
            "status": self._public_status(status),
            "outcome": outcome,
            **self._ctx_metadata(ctx),
            "audit": public_audit,
            "reason": reason, "error": error,
            "ambiguity_reason": ambiguity_reason, "response": response,
        }
        if extra_fields:
            result.update(extra_fields)
        return result

    # ==================================================================
    # BUS WIRING
    # ==================================================================

    def _subscribe_bus(self) -> None:
        subscribe = getattr(self.bus, "subscribe", None)
        if not callable(subscribe):
            return
        _SYS = {"RECONCILE_NOW", "RECOVER_PENDING"}
        for topic in (REQ_QUICK_BET, CMD_QUICK_BET, "RECONCILE_NOW", "RECOVER_PENDING"):
            handler = self._noop_handler if topic in _SYS else self.submit_quick_bet
            try:
                subscribe(topic, handler)
            except Exception:
                logger.exception("Failed to subscribe to %s", topic)

    def _noop_handler(self, *_a: Any, **_kw: Any) -> None:
        return None

    # Point 10: single bus publish helper — includes ctx metadata always.
    def _publish_bus_event(self, ctx: _ExecutionContext, event_name: str, **extra: Any) -> None:
        self._assert_valid_ctx(ctx)
        publish = getattr(self.bus, "publish", None)
        if callable(publish):
            try:
                publish(event_name, {**self._ctx_metadata(ctx), **extra})
            except Exception:
                logger.exception("Failed to publish %s", event_name)

    # Point 10: terminal bus event — used ONLY by _complete_order_lifecycle.
    def _publish_terminal_event(self, ctx: _ExecutionContext, outcome: str,
                                status: str, order_id: Optional[Any] = None) -> None:
        if outcome == OUTCOME_FAILURE:
            name = "QUICK_BET_FAILED"
        elif outcome == OUTCOME_SUCCESS:
            name = "QUICK_BET_SUCCESS"
        else:
            name = "QUICK_BET_AMBIGUOUS"
        self._publish_bus_event(ctx, name, status=status, outcome=outcome, order_id=order_id)

    # ==================================================================
    # PUBLIC ENTRYPOINTS
    # ==================================================================

    def submit_quick_bet(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._submit_via_engine(payload)

    # Point 7: recovery with standardized audit events.
    def recover_after_restart(self) -> Dict[str, Any]:
        recover = getattr(self.state_recovery, "recover", None)
        if not callable(recover):
            return {"ok": False, "status": "RECOVERY_UNAVAILABLE",
                    "recovery": None, "reconcile": None, "reason": "STATE_RECOVERY_UNAVAILABLE"}

        ram_synced = self._repopulate_inflight_from_db()

        # Point 7: RECOVERY_STARTED audit event
        logger.info("RECOVERY_STARTED ram_synced=%s", ram_synced)

        try:
            recovery_result = recover()
        except Exception as exc:
            logger.exception("state_recovery.recover() raised")
            logger.info("RECOVERY_COMPLETED ok=False reason=exception")
            return {"ok": False, "status": "RECOVERY_FAILED",
                    "recovery": None, "reconcile": None, "reason": f"RECOVERY_EXCEPTION:{exc}"}

        if not isinstance(recovery_result, dict):
            recovery_result = {"ok": bool(recovery_result), "reason": None}

        reconcile_result: Optional[Dict[str, Any]] = None
        for mn in ("enqueue_pending", "notify_restart", "on_restart"):
            fn = getattr(self.reconciliation_engine, mn, None)
            if callable(fn):
                try:
                    reconcile_result = fn() or {"triggered": True}
                except Exception as exc:
                    reconcile_result = {"triggered": False, "error": str(exc)}
                break

        ok = bool(recovery_result.get("ok", True))
        # Point 7: RECOVERY_COMPLETED audit event
        logger.info("RECOVERY_COMPLETED ok=%s reconcile=%s", ok, reconcile_result)

        return {"ok": ok, "status": "RECOVERY_TRIGGERED" if ok else "RECOVERY_FAILED",
                "recovery": recovery_result, "reconcile": reconcile_result, "ram_synced": ram_synced}

    # ==================================================================
    # CORE ENGINE
    # ==================================================================

    def _submit_via_engine(self, request: Dict[str, Any]) -> Dict[str, Any]:
        self.assert_ready()

        try:
            normalized = self._normalize_request(request)
        except (ValueError, TypeError) as exc:
            logger.warning("Request normalization failed: %s", exc)
            raw = request if isinstance(request, dict) else {}
            fake_ctx = _ExecutionContext(
                str(raw.get("correlation_id") or uuid.uuid4()),
                str(raw.get("customer_ref") or "UNKNOWN"), time.time())
            fake_audit = self._new_audit(fake_ctx)
            self._emit(fake_ctx, fake_audit, "VALIDATION_FAILED",
                       {"error": str(exc)}, category="guard")
            # Point 1: even validation failure goes through lifecycle
            return self._complete_order_lifecycle(
                fake_ctx, fake_audit, order_id=None,
                status=STATUS_FAILED, reason="INVALID_REQUEST", error=str(exc))

        ctx = _ExecutionContext(
            correlation_id=normalized["correlation_id"],
            customer_ref=normalized["customer_ref"],
            created_at=time.time(),
            event_key=normalized.get("event_key"),
            simulation_mode=normalized.get("simulation_mode"),
        )
        _PK = ("simulation_mode", "event_key")
        extra_fields: Dict[str, Any] = {k: normalized[k] for k in _PK if k in normalized}
        audit = self._new_audit(ctx)
        order_id: Optional[Any] = None

        try:
            self._emit(ctx, audit, "REQUEST_RECEIVED", {"request": normalized}, category="request")

            # ── SAFE MODE ──
            safe_on = self._is_safe_mode_enabled()
            self._emit(ctx, audit, "SAFE_MODE_CHECK", {"enabled": safe_on}, category="guard")
            if safe_on:
                self._emit(ctx, audit, "SAFE_MODE_DENIED", {}, category="guard")
                # Point 1: through lifecycle
                return self._complete_order_lifecycle(
                    ctx, audit, order_id=None,
                    status=STATUS_DENIED, reason="SAFE_MODE_ACTIVE",
                    extra_fields=extra_fields)

            # ── RISK ──
            risk_result = self._risk_gate(normalized)
            normalized = risk_result.get("payload", normalized)
            for k in _PK:
                if k in normalized:
                    extra_fields[k] = normalized[k]
            self._emit(ctx, audit, "RISK_DECISION", risk_result, category="guard")

            if not bool(risk_result.get("allowed", False)):
                order_id = self._persist_inflight(ctx, normalized)
                self._emit_critical(ctx, audit, "PERSIST_INFLIGHT",
                                    {"order_id": order_id}, category="persistence")
                self._transition_order(ctx, audit, order_id, STATUS_INFLIGHT, STATUS_DENIED,
                                       extra={"risk_reason": risk_result.get("reason")})
                self._emit(ctx, audit, "RISK_DENIED",
                           {"reason": risk_result.get("reason")}, category="guard")
                # Point 1: through lifecycle
                return self._complete_order_lifecycle(
                    ctx, audit, order_id=order_id,
                    status=STATUS_DENIED,
                    reason=str(risk_result.get("reason", "RISK_DENY")),
                    extra_fields=extra_fields)

            # ── DEDUP / PERSIST / SUBMIT ──
            with self._lock:
                if not self._dedup_allow(ctx):
                    self._emit(ctx, audit, "DUPLICATE_BLOCKED",
                               {"customer_ref": ctx.customer_ref}, category="guard")
                    self._release_customer_ref_if_terminal(ctx)
                    # Point 2: duplicate goes through lifecycle too
                    return self._complete_order_lifecycle(
                        ctx, audit, order_id=None,
                        status=STATUS_DUPLICATE_BLOCKED, reason="DUPLICATE_BLOCKED",
                        terminal_bus_event="QUICK_BET_DUPLICATE",
                        extra_fields=extra_fields)

                self._emit(ctx, audit, "DEDUP_DECISION", {"allowed": True}, category="guard")
                order_id = self._persist_inflight(ctx, normalized)
                self._emit_critical(ctx, audit, "PERSIST_INFLIGHT",
                                    {"order_id": order_id}, category="persistence")

            self._publish_bus_event(ctx, "QUICK_BET_ROUTED", order_id=order_id)
            return self._atomic_submit(ctx, audit, order_id, normalized, extra_fields)

        except Exception as exc:
            logger.exception("Fatal error in trading engine")
            if order_id is not None:
                self._safe_mark_failed(ctx, audit, order_id, reason="ENGINE_FATAL", error=str(exc))
            # Point 1: through lifecycle
            return self._complete_order_lifecycle(
                ctx, audit, order_id=order_id,
                status=STATUS_FAILED, error=str(exc),
                extra_fields=extra_fields)

    # ==================================================================
    # Point 1: _complete_order_lifecycle — SINGLE terminal orchestrator
    # ==================================================================
    #
    # Every exit path calls this. It does, in order:
    # 1. Derive outcome from _STATUS_TO_OUTCOME (Point 4)
    # 2. Validate invariants
    # 3. Emit FINALIZED audit event (Point 5: with persistence check)
    # 4. Write order metadata (never status)
    # 5. Release dedup keys if terminal
    # 6. Publish terminal bus event (Point 10)
    # 7. Build and return uniform result (Point 3)

    def _complete_order_lifecycle(
        self, ctx: _ExecutionContext, audit: Dict[str, Any], *,
        order_id: Optional[Any] = None,
        status: str,
        reason: Optional[str] = None,
        error: Optional[str] = None,
        ambiguity_reason: Optional[str] = None,
        response: Optional[Any] = None,
        extra_fields: Optional[Dict[str, Any]] = None,
        terminal_bus_event: Optional[str] = None,
    ) -> Dict[str, Any]:
        self._assert_valid_ctx(ctx)

        # Point 4: outcome derived centrally, never manual — strict enforcement
        if status not in _STATUS_TO_OUTCOME:
            raise RuntimeError(f"UNKNOWN_STATUS_IN_LIFECYCLE:{status}")
        outcome = _STATUS_TO_OUTCOME[status]

        # Invariants
        if status == STATUS_AMBIGUOUS and not ambiguity_reason:
            raise RuntimeError("AMBIGUOUS_FINALIZE_REQUIRES_REASON")
        if status == STATUS_DENIED and error is not None:
            raise RuntimeError("DENIED_SHOULD_NOT_CARRY_TECHNICAL_ERROR")
        if status == STATUS_COMPLETED and ambiguity_reason is not None:
            raise RuntimeError("COMPLETED_CANNOT_KEEP_AMBIGUITY_REASON")

        # Point 5: critical audit with persistence check
        emit_result = self._emit(ctx, audit, "FINALIZED",
                                 {"order_id": order_id, "status": status, "outcome": outcome,
                                  "reason": reason, "error": error,
                                  "ambiguity_reason": ambiguity_reason}, category="final")
        if emit_result.get("memory_only"):
            logger.warning("FINALIZED audit event is memory-only for order_id=%s", order_id)

        # Metadata write (never status)
        if order_id is not None:
            meta: Dict[str, Any] = {
                "updated_at": time.time(), "outcome": outcome,
                "reason": reason, "last_error": error,
                "ambiguity_reason": ambiguity_reason, "finalized": True,
            }
            if response is not None:
                meta["response"] = response
            self._write_order_metadata(order_id, meta)

        # Release keys
        try:
            if outcome in (OUTCOME_SUCCESS, OUTCOME_FAILURE):
                self._release_customer_ref_if_terminal(ctx)
        except Exception:
            logger.exception("Failed to release inflight keys")

        # Point 10: terminal bus publish — custom or derived
        if terminal_bus_event:
            self._publish_bus_event(ctx, terminal_bus_event, order_id=order_id)
        else:
            self._publish_terminal_event(ctx, outcome, status, order_id=order_id)

        return self._build_result(
            ctx, audit, status=status, outcome=outcome, order_id=order_id,
            reason=reason, error=error, ambiguity_reason=ambiguity_reason,
            response=response, extra_fields=extra_fields)

    # ==================================================================
    # ATOMIC SUBMIT
    # ==================================================================

    def _atomic_submit(self, ctx: _ExecutionContext, audit: Dict[str, Any],
                       order_id: Any, request: Dict[str, Any],
                       extra_fields: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        self._assert_valid_ctx(ctx)
        if extra_fields is None:
            extra_fields = {}

        try:
            response = self._execute_submit(ctx, request)
        except Exception as exc:
            return self._handle_submit_exception(ctx, audit, order_id, exc, extra_fields)

        try:
            self._transition_order(ctx, audit, order_id, STATUS_INFLIGHT, STATUS_SUBMITTED,
                                   extra={"response": response})
        except Exception as te:
            return self._resolve_ambiguity(ctx, audit, order_id,
                                           ambiguity_reason=AMBIGUITY_PERSISTED_NOT_CONFIRMED,
                                           trigger_event="SUBMIT_TRANSITION_FAILED",
                                           trigger_error=str(te), extra_fields=extra_fields)

        self._emit(ctx, audit, "SUBMIT_SUCCESS",
                   {"order_id": order_id, "response": response}, category="execution")
        # Point 1: through lifecycle (FINALIZED emitted there, not here)
        # Point 1: through lifecycle
        return self._complete_order_lifecycle(
            ctx, audit, order_id=order_id,
            status=STATUS_SUBMITTED, response=response, extra_fields=extra_fields)

    # ==================================================================
    # EXECUTOR DISPATCH — single policy
    # ==================================================================

    def _execute_submit(self, ctx: _ExecutionContext, request: Dict[str, Any]) -> Any:
        self._assert_valid_ctx(ctx)
        def _do() -> Any:
            return self._submit_to_order_path(ctx, request)
        submit_fn = getattr(self.executor, "submit", None)
        if callable(submit_fn):
            response = submit_fn("quick_bet", _do)
            if response is None:
                # Executor ran but returned None — this is ambiguous: the order
                # may or may not have been placed. Raise to trigger ambiguity path.
                raise ExecutionError(
                    f"EXECUTOR_RETURNED_NONE for cid={ctx.correlation_id}",
                    error_type=ERROR_AMBIGUOUS,
                    ambiguity_reason=AMBIGUITY_SUBMIT_UNKNOWN,
                )
            return response
        return _do()

    # ==================================================================
    # AMBIGUITY RESOLUTION — single codepath
    # ==================================================================

    def _resolve_ambiguity(self, ctx: _ExecutionContext, audit: Dict[str, Any],
                           order_id: Any, *, ambiguity_reason: str,
                           trigger_event: str, trigger_error: str,
                           extra_fields: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        self._assert_valid_ctx(ctx)
        logger.error("Ambiguity: %s – %s", trigger_event, trigger_error)
        self._emit(ctx, audit, trigger_event,
                   {"order_id": order_id, "error": trigger_error,
                    "ambiguity_reason": ambiguity_reason}, category="ambiguity")
        try:
            self._transition_order(ctx, audit, order_id, STATUS_INFLIGHT, STATUS_AMBIGUOUS,
                                   extra={"ambiguity_reason": ambiguity_reason, "last_error": trigger_error})
        except Exception:
            logger.exception("Failed to transition to AMBIGUOUS for order_id=%s", order_id)
        self._enqueue_reconcile(ctx, audit, order_id, ambiguity_reason)
        # Point 1: through lifecycle (FINALIZED emitted there, not here)
        # Point 1: through lifecycle
        return self._complete_order_lifecycle(
            ctx, audit, order_id=order_id,
            status=STATUS_AMBIGUOUS, ambiguity_reason=ambiguity_reason,
            extra_fields=extra_fields)

    # ==================================================================
    # SUBMIT EXCEPTION HANDLER
    # ==================================================================

    def _handle_submit_exception(self, ctx: _ExecutionContext, audit: Dict[str, Any],
                                 order_id: Any, exc: Exception,
                                 extra_fields: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        self._assert_valid_ctx(ctx)
        ambiguity_reason: Optional[str] = None
        error_type = getattr(exc, "error_type", None)

        if error_type == ERROR_AMBIGUOUS:
            ambiguity_reason = getattr(exc, "ambiguity_reason", None) or self._classify_ambiguity(exc)
        elif isinstance(exc, TimeoutError):
            error_type, ambiguity_reason = ERROR_AMBIGUOUS, AMBIGUITY_SUBMIT_TIMEOUT
        elif "timeout" in str(exc).lower():
            error_type, ambiguity_reason = ERROR_AMBIGUOUS, self._classify_ambiguity(exc)
        elif error_type is None:
            error_type = ERROR_PERMANENT

        if error_type == ERROR_AMBIGUOUS:
            return self._resolve_ambiguity(ctx, audit, order_id,
                                           ambiguity_reason=ambiguity_reason or AMBIGUITY_SUBMIT_UNKNOWN,
                                           trigger_event="SUBMIT_AMBIGUOUS", trigger_error=str(exc),
                                           extra_fields=extra_fields)

        payload = {"order_id": order_id, "error": str(exc), "error_type": error_type}
        self._emit(ctx, audit, "SUBMIT_FAILED", payload, category="failure")
        self._transition_order(ctx, audit, order_id, STATUS_INFLIGHT, STATUS_FAILED,
                               extra={"last_error": str(exc), "error_type": error_type})
        # Point 1: through lifecycle (FINALIZED emitted there, not here)
        # Point 1: through lifecycle
        return self._complete_order_lifecycle(
            ctx, audit, order_id=order_id,
            status=STATUS_FAILED, error=str(exc), reason="SUBMIT_FAILED",
            extra_fields=extra_fields)

    # ==================================================================
    # NORMALIZATION
    # ==================================================================

    def _normalize_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(request, dict):
            raise ValueError("REQUEST_MUST_BE_DICT")
        customer_ref = str(request.get("customer_ref") or "").strip()
        if not customer_ref:
            raise ValueError("CUSTOMER_REF_REQUIRED")
        correlation_id = str(request.get("correlation_id") or "").strip()
        if not correlation_id:
            if not self.auto_generate_correlation_id:
                raise ValueError("CORRELATION_ID_REQUIRED")
            correlation_id = str(uuid.uuid4())
            logger.warning("correlation_id auto-generated=%s for customer_ref=%s", correlation_id, customer_ref)
        normalized = dict(request)
        normalized["customer_ref"] = customer_ref
        normalized["correlation_id"] = correlation_id
        return normalized

    def _repopulate_inflight_from_db(self) -> bool:
        synced = False
        with self._lock:
            load_refs = getattr(self.db, "load_pending_customer_refs", None)
            if callable(load_refs):
                try:
                    refs = load_refs()
                    if refs:
                        for ref in list(refs):
                            self._inflight_keys.add(str(ref))
                    synced = True
                except Exception:
                    logger.exception("Failed to repopulate _inflight_keys")
            load_cids = getattr(self.db, "load_pending_correlation_ids", None)
            if callable(load_cids):
                try:
                    cids = load_cids()
                    if cids:
                        for cid in list(cids):
                            cs = str(cid)
                            if cs not in self._seen_correlation_ids:
                                self._seen_correlation_ids.add(cs)
                                self._seen_cid_order.append(cs)
                    synced = True
                except Exception:
                    logger.exception("Failed to repopulate _seen_correlation_ids")
        return synced

    # ==================================================================
    # SAFE MODE / RISK
    # ==================================================================

    def _is_safe_mode_enabled(self) -> bool:
        getter = getattr(self.safe_mode, "is_enabled", None)
        return bool(getter()) if callable(getter) else False

    def _risk_gate(self, request: Dict[str, Any]) -> Dict[str, Any]:
        checker = getattr(self.risk_middleware, "check", None)
        if callable(checker):
            result = checker(request)
            if isinstance(result, dict) and "allowed" in result:
                return result
        return {"allowed": True, "reason": None, "payload": request}

    # ==================================================================
    # DEDUP — decomposed
    # ==================================================================

    def _dedup_allow(self, ctx: _ExecutionContext) -> bool:
        if self.guard is not None:
            allow = getattr(self.guard, "allow", None)
            if callable(allow):
                return bool(allow(ctx.customer_ref))
        if self._is_duplicate_in_memory(ctx):
            return False
        if self._is_duplicate_in_db(ctx):
            return False
        self._register_dedup_keys(ctx)
        return True

    def _is_duplicate_in_memory(self, ctx: _ExecutionContext) -> bool:
        return ctx.customer_ref in self._inflight_keys or ctx.correlation_id in self._seen_correlation_ids

    def _is_duplicate_in_db(self, ctx: _ExecutionContext) -> bool:
        fn = getattr(self.db, "order_exists_inflight", None)
        if callable(fn):
            try:
                if fn(customer_ref=ctx.customer_ref, correlation_id=ctx.correlation_id):
                    logger.warning("DB duplicate: cref=%s cid=%s", ctx.customer_ref, ctx.correlation_id)
                    return True
            except Exception:
                logger.exception("order_exists_inflight failed – fail-open")
        return False

    def _register_dedup_keys(self, ctx: _ExecutionContext) -> None:
        self._inflight_keys.add(ctx.customer_ref)
        self._seen_correlation_ids.add(ctx.correlation_id)
        self._seen_cid_order.append(ctx.correlation_id)
        while len(self._seen_correlation_ids) > self._max_seen_cid_size:
            trim = len(self._seen_correlation_ids) - self._seen_cid_trim_to
            for _ in range(trim):
                if self._seen_cid_order:
                    self._seen_correlation_ids.discard(self._seen_cid_order.popleft())
            break

    def _release_customer_ref_if_terminal(self, ctx: _ExecutionContext) -> None:
        self._assert_valid_ctx(ctx)
        self._inflight_keys.discard(ctx.customer_ref)

    # ==================================================================
    # SUBMIT PATH
    # ==================================================================

    def _submit_to_order_path(self, ctx: _ExecutionContext, request: Dict[str, Any]) -> Any:
        self._assert_valid_ctx(ctx)
        payload = dict(request)
        payload["customer_ref"] = ctx.customer_ref
        payload["correlation_id"] = ctx.correlation_id
        if self.order_manager is not None:
            for mn in ("submit", "place_order"):
                fn = getattr(self.order_manager, mn, None)
                if callable(fn):
                    return fn(payload)
        if callable(self.client_getter):
            client = self.client_getter()
            if client is not None:
                place = getattr(client, "place_bet", None)
                if callable(place):
                    return place(**payload)
        raise RuntimeError("NO_VALID_EXECUTION_PATH")

    def _classify_ambiguity(self, exc: Exception) -> str:
        text = str(exc).lower()
        if "timeout" in text: return AMBIGUITY_SUBMIT_TIMEOUT
        if "response lost" in text or "lost response" in text: return AMBIGUITY_RESPONSE_LOST
        if "persist" in text and "confirm" in text: return AMBIGUITY_PERSISTED_NOT_CONFIRMED
        if "split" in text: return AMBIGUITY_SPLIT_STATE
        return AMBIGUITY_SUBMIT_UNKNOWN

    def _enqueue_reconcile(self, ctx: _ExecutionContext, audit: Dict[str, Any],
                           order_id: Any, ambiguity_reason: str) -> None:
        self._assert_valid_ctx(ctx)
        enqueue = getattr(self.reconciliation_engine, "enqueue", None)
        if callable(enqueue):
            enqueue(order_id=order_id, ambiguity_reason=ambiguity_reason, **self._ctx_metadata(ctx))
        self._emit(ctx, audit, "RECONCILE_ENQUEUED",
                   {"order_id": order_id, "ambiguity_reason": ambiguity_reason}, category="reconcile")

    # ==================================================================
    # STATE MACHINE / PERSISTENCE — strictly separated
    # ==================================================================

    def _persist_inflight(self, ctx: _ExecutionContext, request: Dict[str, Any]) -> Any:
        self._assert_valid_ctx(ctx)
        payload = {"customer_ref": ctx.customer_ref, "correlation_id": ctx.correlation_id,
                    "status": STATUS_INFLIGHT, "payload": request,
                    "created_at": ctx.created_at, "outcome": None}
        insert_order = getattr(self.db, "insert_order", None)
        if callable(insert_order):
            return insert_order(payload)
        # Point 9: declared degraded fallback — not hard-blocking
        order_id = str(uuid.uuid4())
        logger.warning("DB.insert_order unavailable – local order_id=%s (DEGRADED)", order_id)
        return order_id

    def _transition_order(self, ctx: _ExecutionContext, audit: Dict[str, Any],
                          order_id: Any, from_status: str, to_status: str,
                          extra: Optional[Dict[str, Any]] = None) -> None:
        self._assert_valid_ctx(ctx)
        if to_status not in ALLOWED_TRANSITIONS.get(from_status, set()):
            raise RuntimeError(f"ILLEGAL_ORDER_TRANSITION:{from_status}->{to_status}")
        update: Dict[str, Any] = {"status": to_status, "updated_at": time.time()}
        if extra:
            update.update(extra)
        update_order = getattr(self.db, "update_order", None)
        if callable(update_order):
            update_order(order_id, update)
        else:
            logger.warning("DB.update_order unavailable – transition not persisted")
        self._emit_critical(ctx, audit, "ORDER_TRANSITION",
                   {"order_id": order_id, "from_status": from_status,
                    "to_status": to_status, "extra": extra or {}}, category="state")

    def _write_order_metadata(self, order_id: Any, meta: Dict[str, Any]) -> None:
        if "status" in meta:
            raise RuntimeError("METADATA_WRITE_MUST_NOT_CONTAIN_STATUS")
        update_order = getattr(self.db, "update_order", None)
        if callable(update_order):
            update_order(order_id, meta)

    def _safe_mark_failed(self, ctx: _ExecutionContext, audit: Dict[str, Any],
                          order_id: Any, reason: str, error: str,
                          from_status: str = STATUS_INFLIGHT) -> None:
        """Best-effort transition to FAILED. Does NOT finalize — lifecycle handles that.
        Used only in fatal exception path where transition may or may not succeed."""
        self._assert_valid_ctx(ctx)
        try:
            self._transition_order(ctx, audit, order_id, from_status, STATUS_FAILED,
                                   extra={"failure_reason": reason, "last_error": error})
        except Exception:
            logger.exception("safe_mark_failed failed for order_id=%s", order_id)

    # ==================================================================
    # AUDIT — with persistence status (Point 5)
    # ==================================================================

    def _new_audit(self, ctx: _ExecutionContext) -> Dict[str, Any]:
        self._assert_valid_ctx(ctx)
        return {"correlation_id": ctx.correlation_id, "customer_ref": ctx.customer_ref,
                "events": [], "index": 0, "_last_event_id": None}

    def _emit(self, ctx: _ExecutionContext, audit: Dict[str, Any],
              event_type: str, payload: Dict[str, Any], *, category: str) -> Dict[str, bool]:
        self._assert_valid_ctx(ctx)
        event_id = str(uuid.uuid4())
        event = {"event_id": event_id, "parent_event_id": audit["_last_event_id"],
                 "index": audit["index"], "ts": time.time(), "type": event_type,
                 "category": category, "payload": payload, **self._ctx_metadata(ctx)}
        audit["index"] += 1
        audit["_last_event_id"] = event_id
        audit["events"].append(event)

        persisted_db = False
        for mn in ("insert_audit_event", "insert_order_event", "append_order_event"):
            fn = getattr(self.db, mn, None)
            if callable(fn):
                fn(event)
                persisted_db = True
                break
        persisted_async = False
        write_fn = getattr(self.async_db_writer, "write", None)
        if callable(write_fn):
            try:
                write_fn(event)
                persisted_async = True
            except Exception:
                logger.exception("async_db_writer.write failed")
        memory_only = not persisted_db and not persisted_async
        if memory_only:
            logger.debug("Audit in-memory only: %s", event_type)
        return {"persisted_db": persisted_db, "persisted_async": persisted_async, "memory_only": memory_only}

    # Point 5: critical emit — warns if memory-only on important events
    def _emit_critical(self, ctx: _ExecutionContext, audit: Dict[str, Any],
                       event_type: str, payload: Dict[str, Any], *, category: str) -> Dict[str, bool]:
        result = self._emit(ctx, audit, event_type, payload, category=category)
        if result.get("memory_only"):
            logger.warning("CRITICAL audit event %s is memory-only", event_type)
        return result

    # Legacy alias kept for backward compatibility with existing tests
    # that call _finalize() directly on _ExecutionContext instances.
    def _finalize(self, ctx: _ExecutionContext, audit: Dict[str, Any],
                  order_id: Optional[Any], status: str, outcome: str, *,
                  reason: Optional[str] = None, error: Optional[str] = None,
                  ambiguity_reason: Optional[str] = None,
                  response: Optional[Any] = None,
                  extra_fields: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self._complete_order_lifecycle(
            ctx, audit, order_id=order_id, status=status,
            reason=reason, error=error, ambiguity_reason=ambiguity_reason,
            response=response, extra_fields=extra_fields)
