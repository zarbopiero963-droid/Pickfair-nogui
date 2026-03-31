from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Optional, Set

from order_manager import OrderManager


logger = logging.getLogger(__name__)


# =========================================================
# ARCHITECTURE CONVENTIONS
# =========================================================

REQ_QUICK_BET = "REQ_QUICK_BET"
CMD_QUICK_BET = "CMD_QUICK_BET"


# =========================================================
# STATUS / OUTCOME / ERROR TYPES
# =========================================================

STATUS_INFLIGHT = "INFLIGHT"
STATUS_SUBMITTED = "SUBMITTED"
STATUS_MATCHED = "MATCHED"
STATUS_COMPLETED = "COMPLETED"
STATUS_FAILED = "FAILED"
STATUS_AMBIGUOUS = "AMBIGUOUS"
STATUS_DENIED = "DENIED"

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
    STATUS_INFLIGHT: {STATUS_SUBMITTED, STATUS_FAILED, STATUS_AMBIGUOUS},
    STATUS_SUBMITTED: {STATUS_MATCHED, STATUS_COMPLETED, STATUS_FAILED, STATUS_AMBIGUOUS},
    STATUS_MATCHED: {STATUS_COMPLETED, STATUS_FAILED},
    STATUS_AMBIGUOUS: {STATUS_COMPLETED, STATUS_FAILED},
    STATUS_DENIED: set(),
    STATUS_FAILED: set(),
    STATUS_COMPLETED: set(),
}


# =========================================================
# EXECUTION CONTEXT / ERRORS
# =========================================================

@dataclass(frozen=True)
class _ExecutionContext:
    correlation_id: str
    customer_ref: str
    created_at: float


class ExecutionError(Exception):
    def __init__(
        self,
        message: str,
        *,
        error_type: str = ERROR_PERMANENT,
        ambiguity_reason: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.error_type = error_type
        self.ambiguity_reason = ambiguity_reason


# =========================================================
# NO-OP FALLBACKS
# =========================================================

class _NullSafeMode:
    def is_enabled(self) -> bool:
        return False

    def is_ready(self) -> bool:
        return True


class _NullRiskMiddleware:
    def check(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return {"allowed": True, "reason": None, "payload": payload}

    def is_ready(self) -> bool:
        return True


class _NullReconciliationEngine:
    def enqueue(self, **_kwargs: Any) -> None:
        return None

    def is_ready(self) -> bool:
        return True


class _NullStateRecovery:
    def recover(self) -> Dict[str, Any]:
        return {"ok": True, "reason": None}

    def is_ready(self) -> bool:
        return True


class _NullAsyncDbWriter:
    def write(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    def is_ready(self) -> bool:
        return True


# =========================================================
# TRADING ENGINE
# =========================================================

class TradingEngine:
    """
    Trading Engine guardrail-compatible + file-level hardened.
    """

    def __init__(
        self,
        bus: Any,
        db: Any,
        client_getter: Any,
        executor: Any,
        safe_mode: Any = None,
        risk_middleware: Any = None,
        reconciliation_engine: Any = None,
        state_recovery: Any = None,
        async_db_writer: Any = None,
    ) -> None:
        # ---------- contract storico / repo ----------
        self.bus = bus
        self.db = db
        self.client_getter = client_getter
        self.executor = executor
        self.safe_mode = safe_mode if safe_mode is not None else _NullSafeMode()
        self.risk_middleware = risk_middleware if risk_middleware is not None else _NullRiskMiddleware()
        self.reconciliation_engine = (
            reconciliation_engine if reconciliation_engine is not None else _NullReconciliationEngine()
        )
        self.state_recovery = state_recovery if state_recovery is not None else _NullStateRecovery()
        self.async_db_writer = async_db_writer if async_db_writer is not None else _NullAsyncDbWriter()

        # ---------- adapters interni ----------
        self.order_manager: Optional[OrderManager] = None
        self.guard: Optional[Any] = None

        self._runtime_state = NOT_READY
        self._health: Dict[str, Any] = {}

        self._subscribe_bus()
        self.start()

    # =========================================================
    # READINESS
    # =========================================================

    def start(self) -> None:
        self._health = {
            "db": self._dependency_state(self.db, required=True),
            "client_getter": self._dependency_state(self.client_getter, required=True),
            "executor": self._dependency_state(self.executor, required=False),
            "safe_mode": self._dependency_state(self.safe_mode, required=False),
            "risk_middleware": self._dependency_state(self.risk_middleware, required=False),
            "reconciliation_engine": self._dependency_state(self.reconciliation_engine, required=False),
            "state_recovery": self._dependency_state(self.state_recovery, required=False),
            "async_db_writer": self._dependency_state(self.async_db_writer, required=False),
        }

        required_states = [
            self._health["db"]["state"],
            self._health["client_getter"]["state"],
        ]

        if all(s == READY for s in required_states):
            all_states = [v["state"] for v in self._health.values()]
            self._runtime_state = READY if all(s == READY for s in all_states) else DEGRADED
        else:
            self._runtime_state = NOT_READY

        logger.info(
            "TradingEngine start -> state=%s health=%s",
            self._runtime_state,
            self._health,
        )

    def stop(self) -> None:
        self._runtime_state = NOT_READY
        logger.info("TradingEngine stopped")

    def readiness(self) -> Dict[str, Any]:
        return {
            "state": self._runtime_state,
            "health": dict(self._health),
        }

    def _dependency_state(self, dep: Any, *, required: bool) -> Dict[str, Any]:
        if dep is None:
            return {
                "state": NOT_READY if required else DEGRADED,
                "reason": "missing",
            }

        checker = getattr(dep, "is_ready", None)
        if callable(checker):
            try:
                ok = bool(checker())
                return {
                    "state": READY if ok else (NOT_READY if required else DEGRADED),
                    "reason": None if ok else "unhealthy",
                }
            except Exception as exc:
                logger.exception("Dependency readiness check failed")
                return {
                    "state": NOT_READY if required else DEGRADED,
                    "reason": f"exception:{exc}",
                }

        return {"state": READY, "reason": "no-checker"}

    def assert_ready(self) -> None:
        if self._runtime_state not in {READY, DEGRADED}:
            raise RuntimeError(f"TRADING_ENGINE_NOT_READY:{self._runtime_state}")

    # =========================================================
    # BUS / ARCHITECTURE WIRING
    # =========================================================

    def _subscribe_bus(self) -> None:
        subscribe = getattr(self.bus, "subscribe", None)
        if callable(subscribe):
            try:
                subscribe(REQ_QUICK_BET, self.submit_quick_bet)
            except Exception:
                logger.exception("Failed to subscribe to %s", REQ_QUICK_BET)

    # =========================================================
    # PUBLIC ENTRYPOINTS (repo-compatible)
    # =========================================================

    def submit_quick_bet(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._submit_via_engine(payload)

    def recover_after_restart(self) -> Dict[str, Any]:
        recover = getattr(self.state_recovery, "recover", None)
        if callable(recover):
            return recover()
        return {
            "ok": False,
            "reason": "STATE_RECOVERY_UNAVAILABLE",
        }

    # =========================================================
    # SINGLE REAL ENTRYPOINT
    # =========================================================

    def _submit_via_engine(self, request: Dict[str, Any]) -> Dict[str, Any]:
        self.assert_ready()

        normalized = self._normalize_request(request)
        ctx = _ExecutionContext(
            correlation_id=normalized["correlation_id"],
            customer_ref=normalized["customer_ref"],
            created_at=time.time(),
        )

        audit = self._new_audit(ctx)
        order_id: Optional[Any] = None

        try:
            self._audit(ctx, audit, "REQUEST_RECEIVED", {"request": normalized}, category="request")

            safe_mode_enabled = self._is_safe_mode_enabled()
            self._audit(
                ctx,
                audit,
                "SAFE_MODE_CHECK",
                {"enabled": safe_mode_enabled},
                category="guard",
            )
            if safe_mode_enabled:
                return self._finalize(
                    ctx=ctx,
                    audit=audit,
                    order_id=None,
                    status=STATUS_DENIED,
                    outcome=OUTCOME_FAILURE,
                    reason="SAFE_MODE_ACTIVE",
                )

            risk_result = self._risk_gate(normalized)
            self._audit(ctx, audit, "RISK_DECISION", risk_result, category="guard")
            if not bool(risk_result.get("allowed", False)):
                return self._finalize(
                    ctx=ctx,
                    audit=audit,
                    order_id=None,
                    status=STATUS_DENIED,
                    outcome=OUTCOME_FAILURE,
                    reason=str(risk_result.get("reason", "RISK_DENY")),
                )

            dedup_allowed = self._dedup_allow(ctx)
            self._audit(
                ctx,
                audit,
                "DEDUP_DECISION",
                {"allowed": dedup_allowed},
                category="guard",
            )
            if not dedup_allowed:
                return self._finalize(
                    ctx=ctx,
                    audit=audit,
                    order_id=None,
                    status=STATUS_COMPLETED,
                    outcome=OUTCOME_SUCCESS,
                    reason="DUPLICATE_BLOCKED",
                )

            order_id = self._persist_inflight(ctx, normalized)
            self._audit(
                ctx,
                audit,
                "PERSIST_INFLIGHT",
                {"order_id": order_id},
                category="persistence",
            )

            try:
                response = self._submit_to_order_path(ctx, normalized)
                self._audit(
                    ctx,
                    audit,
                    "SUBMIT_SUCCESS",
                    {"order_id": order_id, "response": response},
                    category="execution",
                )

                self._transition_order(
                    ctx=ctx,
                    audit=audit,
                    order_id=order_id,
                    from_status=STATUS_INFLIGHT,
                    to_status=STATUS_SUBMITTED,
                    extra={"response": response},
                )

                return self._finalize(
                    ctx=ctx,
                    audit=audit,
                    order_id=order_id,
                    status=STATUS_SUBMITTED,
                    outcome=OUTCOME_SUCCESS,
                    response=response,
                )

            except Exception as exc:
                return self._handle_submit_exception(
                    ctx=ctx,
                    audit=audit,
                    order_id=order_id,
                    exc=exc,
                )

        except Exception as exc:
            logger.exception("Fatal error in trading engine")
            if order_id is not None:
                self._safe_mark_failed(
                    ctx=ctx,
                    audit=audit,
                    order_id=order_id,
                    reason="ENGINE_FATAL",
                    error=str(exc),
                )

            return self._finalize(
                ctx=ctx,
                audit=audit,
                order_id=order_id,
                status=STATUS_FAILED,
                outcome=OUTCOME_FAILURE,
                error=str(exc),
            )

    # =========================================================
    # REQUEST NORMALIZATION / CONTRACT
    # =========================================================

    def _normalize_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(request, dict):
            raise ValueError("REQUEST_MUST_BE_DICT")

        customer_ref = str(request.get("customer_ref") or "").strip()
        if not customer_ref:
            raise ValueError("CUSTOMER_REF_REQUIRED")

        correlation_id = str(request.get("correlation_id") or "").strip() or str(uuid.uuid4())

        normalized = dict(request)
        normalized["customer_ref"] = customer_ref
        normalized["correlation_id"] = correlation_id

        return normalized

    # =========================================================
    # SAFE MODE / RISK / DEDUP
    # =========================================================

    def _is_safe_mode_enabled(self) -> bool:
        getter = getattr(self.safe_mode, "is_enabled", None)
        if callable(getter):
            return bool(getter())
        return False

    def _risk_gate(self, request: Dict[str, Any]) -> Dict[str, Any]:
        checker = getattr(self.risk_middleware, "check", None)
        if callable(checker):
            result = checker(request)
            if isinstance(result, dict) and "allowed" in result:
                return result

        return {
            "allowed": True,
            "reason": None,
            "payload": request,
        }

    def _dedup_allow(self, ctx: _ExecutionContext) -> bool:
        if self.guard is not None:
            allow = getattr(self.guard, "allow", None)
            if callable(allow):
                return bool(allow(ctx.customer_ref))
        return True

    # =========================================================
    # SUBMIT PATH
    # =========================================================

    def _submit_to_order_path(self, ctx: _ExecutionContext, request: Dict[str, Any]) -> Any:
        request_to_send = dict(request)
        request_to_send["customer_ref"] = ctx.customer_ref
        request_to_send["correlation_id"] = ctx.correlation_id

        if self.order_manager is not None:
            submit = getattr(self.order_manager, "submit", None)
            if callable(submit):
                return submit(request_to_send)

        getter = self.client_getter
        if callable(getter):
            client = getter()
            if client is not None:
                place = getattr(client, "place_bet", None)
                if callable(place):
                    return place(**request_to_send)

        raise RuntimeError("NO_VALID_EXECUTION_PATH")

    # =========================================================
    # AMBIGUITY POLICY
    # =========================================================

    def _classify_ambiguity(self, exc: Exception) -> str:
        text = str(exc).lower()

        if "timeout" in text:
            return AMBIGUITY_SUBMIT_TIMEOUT
        if "response lost" in text or "lost response" in text:
            return AMBIGUITY_RESPONSE_LOST
        if "persist" in text and "confirm" in text:
            return AMBIGUITY_PERSISTED_NOT_CONFIRMED
        if "split" in text:
            return AMBIGUITY_SPLIT_STATE
        return AMBIGUITY_SUBMIT_UNKNOWN

    def _handle_submit_exception(
        self,
        ctx: _ExecutionContext,
        audit: Dict[str, Any],
        order_id: Any,
        exc: Exception,
    ) -> Dict[str, Any]:
        ambiguity_reason = None
        error_type = getattr(exc, "error_type", None)

        if error_type == ERROR_AMBIGUOUS:
            ambiguity_reason = getattr(exc, "ambiguity_reason", None) or self._classify_ambiguity(exc)
        elif isinstance(exc, TimeoutError):
            error_type = ERROR_AMBIGUOUS
            ambiguity_reason = AMBIGUITY_SUBMIT_TIMEOUT
        elif "timeout" in str(exc).lower():
            error_type = ERROR_AMBIGUOUS
            ambiguity_reason = self._classify_ambiguity(exc)
        elif error_type is None:
            error_type = ERROR_PERMANENT

        payload = {
            "order_id": order_id,
            "error": str(exc),
            "error_type": error_type,
            "ambiguity_reason": ambiguity_reason,
        }

        if error_type == ERROR_AMBIGUOUS:
            self._audit(ctx, audit, "SUBMIT_AMBIGUOUS", payload, category="ambiguity")

            self._transition_order(
                ctx=ctx,
                audit=audit,
                order_id=order_id,
                from_status=STATUS_INFLIGHT,
                to_status=STATUS_AMBIGUOUS,
                extra={"ambiguity_reason": ambiguity_reason, "last_error": str(exc)},
            )

            self._enqueue_reconcile(ctx, audit, order_id, ambiguity_reason)

            return self._finalize(
                ctx=ctx,
                audit=audit,
                order_id=order_id,
                status=STATUS_AMBIGUOUS,
                outcome=OUTCOME_AMBIGUOUS,
                ambiguity_reason=ambiguity_reason,
            )

        self._audit(ctx, audit, "SUBMIT_FAILED", payload, category="failure")

        self._transition_order(
            ctx=ctx,
            audit=audit,
            order_id=order_id,
            from_status=STATUS_INFLIGHT,
            to_status=STATUS_FAILED,
            extra={"last_error": str(exc), "error_type": error_type},
        )

        return self._finalize(
            ctx=ctx,
            audit=audit,
            order_id=order_id,
            status=STATUS_FAILED,
            outcome=OUTCOME_FAILURE,
            error=str(exc),
            reason="SUBMIT_FAILED",
        )

    def _enqueue_reconcile(
        self,
        ctx: _ExecutionContext,
        audit: Dict[str, Any],
        order_id: Any,
        ambiguity_reason: str,
    ) -> None:
        enqueue = getattr(self.reconciliation_engine, "enqueue", None)
        if callable(enqueue):
            enqueue(
                order_id=order_id,
                correlation_id=ctx.correlation_id,
                customer_ref=ctx.customer_ref,
                ambiguity_reason=ambiguity_reason,
            )

        self._audit(
            ctx,
            audit,
            "RECONCILE_ENQUEUED",
            {
                "order_id": order_id,
                "ambiguity_reason": ambiguity_reason,
            },
            category="reconcile",
        )

    # =========================================================
    # STATE MACHINE / PERSISTENCE
    # =========================================================

    def _persist_inflight(self, ctx: _ExecutionContext, request: Dict[str, Any]) -> Any:
        payload = {
            "customer_ref": ctx.customer_ref,
            "correlation_id": ctx.correlation_id,
            "status": STATUS_INFLIGHT,
            "payload": request,
            "created_at": ctx.created_at,
            "outcome": None,
        }

        insert_order = getattr(self.db, "insert_order", None)
        if not callable(insert_order):
            raise RuntimeError("DB_INSERT_ORDER_UNAVAILABLE")

        return insert_order(payload)

    def _transition_order(
        self,
        ctx: _ExecutionContext,
        audit: Dict[str, Any],
        order_id: Any,
        from_status: str,
        to_status: str,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        if to_status not in ALLOWED_TRANSITIONS.get(from_status, set()):
            raise RuntimeError(f"ILLEGAL_ORDER_TRANSITION:{from_status}->{to_status}")

        update = {
            "status": to_status,
            "updated_at": time.time(),
        }
        if extra:
            update.update(extra)

        update_order = getattr(self.db, "update_order", None)
        if not callable(update_order):
            raise RuntimeError("DB_UPDATE_ORDER_UNAVAILABLE")

        update_order(order_id, update)

        self._audit(
            ctx,
            audit,
            "ORDER_TRANSITION",
            {
                "order_id": order_id,
                "from_status": from_status,
                "to_status": to_status,
                "extra": extra or {},
            },
            category="state",
        )

    def _safe_mark_failed(
        self,
        ctx: _ExecutionContext,
        audit: Dict[str, Any],
        order_id: Any,
        reason: str,
        error: str,
    ) -> None:
        try:
            update_order = getattr(self.db, "update_order", None)
            if callable(update_order):
                update_order(
                    order_id,
                    {
                        "status": STATUS_FAILED,
                        "updated_at": time.time(),
                        "failure_reason": reason,
                        "last_error": error,
                    },
                )
            self._audit(
                ctx,
                audit,
                "SAFE_MARK_FAILED",
                {
                    "order_id": order_id,
                    "reason": reason,
                    "error": error,
                },
                category="failure",
            )
        except Exception:
            logger.exception("Failed to mark order as failed")

    # =========================================================
    # AUDIT
    # =========================================================

    def _new_audit(self, ctx: _ExecutionContext) -> Dict[str, Any]:
        return {
            "correlation_id": ctx.correlation_id,
            "customer_ref": ctx.customer_ref,
            "events": [],
            "index": 0,
        }

    def _audit(
        self,
        ctx: _ExecutionContext,
        audit: Dict[str, Any],
        event_type: str,
        payload: Dict[str, Any],
        *,
        category: str,
    ) -> None:
        event = {
            "event_id": str(uuid.uuid4()),
            "index": audit["index"],
            "ts": time.time(),
            "type": event_type,
            "category": category,
            "payload": payload,
            "correlation_id": ctx.correlation_id,
            "customer_ref": ctx.customer_ref,
        }

        audit["index"] += 1
        audit["events"].append(event)

        persisted = False
        for method_name in ("insert_audit_event", "insert_order_event", "append_order_event"):
            method = getattr(self.db, method_name, None)
            if callable(method):
                method(event)
                persisted = True
                break

        if not persisted:
            logger.debug("No audit persistence method available")

        logger.debug("AUDIT[%s] %s %s", category, event_type, payload)

    # =========================================================
    # FINALIZE POLICY
    # =========================================================

    def _finalize(
        self,
        ctx: _ExecutionContext,
        audit: Dict[str, Any],
        order_id: Optional[Any],
        status: str,
        outcome: str,
        *,
        reason: Optional[str] = None,
        error: Optional[str] = None,
        ambiguity_reason: Optional[str] = None,
        response: Optional[Any] = None,
    ) -> Dict[str, Any]:
        if status == STATUS_AMBIGUOUS and not ambiguity_reason:
            raise RuntimeError("AMBIGUOUS_FINALIZE_REQUIRES_REASON")

        if status == STATUS_DENIED and error is not None:
            raise RuntimeError("DENIED_SHOULD_NOT_CARRY_TECHNICAL_ERROR")

        if status == STATUS_COMPLETED and ambiguity_reason is not None:
            raise RuntimeError("COMPLETED_CANNOT_KEEP_AMBIGUITY_REASON")

        self._audit(
            ctx,
            audit,
            "FINALIZED",
            {
                "order_id": order_id,
                "status": status,
                "outcome": outcome,
                "reason": reason,
                "error": error,
                "ambiguity_reason": ambiguity_reason,
            },
            category="final",
        )

        if order_id is not None:
            update_order = getattr(self.db, "update_order", None)
            if callable(update_order):
                update_payload = {
                    "updated_at": time.time(),
                    "status": status,
                    "outcome": outcome,
                    "reason": reason,
                    "last_error": error,
                    "ambiguity_reason": ambiguity_reason,
                    "finalized": True,
                }
                if response is not None:
                    update_payload["response"] = response
                update_order(order_id, update_payload)

        # contract UNIFORME SEMPRE
        return {
            "status": status,
            "outcome": outcome,
            "correlation_id": ctx.correlation_id,
            "customer_ref": ctx.customer_ref,
            "audit": audit,
            "reason": reason,
            "error": error,
            "ambiguity_reason": ambiguity_reason,
            "response": response,
        } sistemalo qui چي serve تغيير?