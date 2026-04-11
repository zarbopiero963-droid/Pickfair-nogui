from __future__ import annotations
import inspect
import logging
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, Optional, Set, Tuple
from order_manager import OrderManager
from order_manager import LIFECYCLE_CONTRACT

logger = logging.getLogger(__name__)

# =========================================================
# CONSTANTS
# =========================================================
REQ_QUICK_BET = "REQ_QUICK_BET"
CMD_QUICK_BET = "CMD_QUICK_BET"

# ── ORDER STATES ──
STATUS_INFLIGHT = "INFLIGHT"
STATUS_SUBMITTED = "SUBMITTED"
STATUS_COMPLETED = "COMPLETED"
STATUS_FAILED = "FAILED"
STATUS_AMBIGUOUS = "AMBIGUOUS"
STATUS_DENIED = "DENIED"
STATUS_ACCEPTED_FOR_PROCESSING = "ACCEPTED_FOR_PROCESSING"
STATUS_DUPLICATE_BLOCKED = "DUPLICATE_BLOCKED"

# ── OUTCOMES ──
OUTCOME_SUCCESS = "SUCCESS"
OUTCOME_FAILURE = "FAILURE"
OUTCOME_AMBIGUOUS = "AMBIGUOUS"

# ── ERRORS ──
ERROR_TRANSIENT = "TRANSIENT"
ERROR_PERMANENT = "PERMANENT"
ERROR_AMBIGUOUS = "AMBIGUOUS"

# ── READINESS ──
READY = "READY"
DEGRADED = "DEGRADED"
NOT_READY = "NOT_READY"

# ── AMBIGUITY REASONS ──
AMBIGUITY_SUBMIT_TIMEOUT = "SUBMIT_TIMEOUT"
AMBIGUITY_RESPONSE_LOST = "RESPONSE_LOST"
AMBIGUITY_SUBMIT_UNKNOWN = "SUBMIT_UNKNOWN"
AMBIGUITY_PERSISTED_NOT_CONFIRMED = "PERSISTED_NOT_CONFIRMED"
AMBIGUITY_SPLIT_STATE = "SPLIT_STATE"

# ── ORDER ORIGINS ──
ORIGIN_NORMAL = "NORMAL"
ORIGIN_COPY = "COPY"
ORIGIN_PATTERN = "PATTERN"

# ── COPY META KEYS (File 8) ──
COPY_META_KEYS = {
    "master_id", "master_position_id", "action_id", "action_seq",
    "copy_group_id", "copy_mode"
}

# ── PATTERN META KEYS (File 8) ──
PATTERN_META_KEYS = {
    "pattern_id", "pattern_label", "selection_template", "market_type",
    "bet_side", "live_only", "event_context"
}

# =========================================================
# [P2] CENTRALIZED CONSTANTS
# =========================================================
_PASSTHROUGH_KEYS: Tuple[str, ...] = (
    "simulation_mode",
    "event_key",
    "order_origin",
    "copy_meta",
    "pattern_meta",
)

_ACK_STATES: Set[str] = {STATUS_SUBMITTED}

_TERMINAL_STATES: Set[str] = {
    STATUS_COMPLETED,
    STATUS_FAILED,
    STATUS_DENIED,
    STATUS_AMBIGUOUS,
    STATUS_DUPLICATE_BLOCKED,
}

# =========================================================
# STATE MACHINE
# =========================================================
ALLOWED_TRANSITIONS: Dict[str, Set[str]] = {
    STATUS_INFLIGHT: {STATUS_SUBMITTED, STATUS_FAILED, STATUS_AMBIGUOUS, STATUS_DENIED, STATUS_DUPLICATE_BLOCKED},
    STATUS_SUBMITTED: {STATUS_COMPLETED, STATUS_FAILED, STATUS_AMBIGUOUS},
    STATUS_AMBIGUOUS: {STATUS_COMPLETED, STATUS_FAILED},
    STATUS_DENIED: set(),
    STATUS_FAILED: set(),
    STATUS_COMPLETED: set(),
    STATUS_DUPLICATE_BLOCKED: set(),
}

_INTERNAL_TO_PUBLIC_STATUS: Dict[str, str] = {
    STATUS_INFLIGHT: STATUS_INFLIGHT,
    STATUS_SUBMITTED: LIFECYCLE_CONTRACT["ACCEPTED"]["trading_engine_status"],
    STATUS_ACCEPTED_FOR_PROCESSING: STATUS_ACCEPTED_FOR_PROCESSING,
    STATUS_COMPLETED: STATUS_COMPLETED,
    STATUS_FAILED: STATUS_FAILED,
    STATUS_AMBIGUOUS: STATUS_AMBIGUOUS,
    STATUS_DENIED: STATUS_DENIED,
    STATUS_DUPLICATE_BLOCKED: STATUS_DUPLICATE_BLOCKED,
}

_STATUS_TO_OUTCOME: Dict[str, str] = {
    STATUS_COMPLETED: LIFECYCLE_CONTRACT["FILLED"]["outcome"],
    STATUS_FAILED: LIFECYCLE_CONTRACT["FAILED"]["outcome"],
    STATUS_DENIED: LIFECYCLE_CONTRACT["FAILED"]["outcome"],
    STATUS_AMBIGUOUS: LIFECYCLE_CONTRACT["AMBIGUOUS"]["outcome"],
    STATUS_DUPLICATE_BLOCKED: OUTCOME_SUCCESS,
}

# =========================================================
# EXECUTION CONTEXT
# =========================================================
@dataclass(frozen=True)
class _ExecutionContext:
    """
    Immutable execution context for order lifecycle.
    
    Created ONLY via TradingEngine._new_execution_context().
    
    NOTE: _engine_token is a deterrent against accidental misuse,
    not a security barrier. Real protection:
    1. Do not export this class from the module
    2. Use factory method for creation
    3. Code review + tests verify no bypass
    """
    correlation_id: str
    customer_ref: str
    created_at: float
    event_key: Optional[str] = None
    simulation_mode: Optional[bool] = None
    _engine_token: str = "TRADING_ENGINE_INTERNAL"


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
class TradingEngine:
    """
    Trading Engine — Core Order Lifecycle Orchestrator
    
    RESPONSIBILITIES:
    - Orchestrates order lifecycle (INFLIGHT → SUBMITTED → TERMINAL)
    - Manages dedup (RAM + DB)
    - Handles ambiguity resolution
    - Persists audit trail
    - Enforces state machine transitions
    
    DOES NOT IMPLEMENT:
    - Broker logic (delegated to executor/client_getter)
    - Recovery loop (delegated to state_recovery)
    - Reconcile loop (delegated to reconciliation_engine)
    - Simulation logic (simulation_mode is metadata only)
    - Copy cashout logic (copy_meta is passthrough only)
    - Dutching lifecycle (dutching metadata is passthrough only)
    
    ANTI-BYPASS NOTES:
    - _ExecutionContext has _engine_token as deterrent against accidental misuse
    - Real protection: use _new_execution_context() factory, do not export class
    - Code review and tests should verify no direct context construction
    
    [P2] SIMULATION_MODE GUARD RAIL:
    - simulation_mode is metadata ONLY
    - NO semantic branches based on simulation_mode in this engine
    - Live and Sim differ ONLY in executor/broker implementation
    - No "simplified simulation" paths exist here
    
    DB CONTRACT REQUIRED:
    - insert_order() must persist INFLIGHT before submit paths continue
    - update_order() must be immediately visible to get_order() from TradingEngine perspective
    - get_order() must provide read-your-writes semantics
    - order_exists_inflight() must reflect current persisted state coherently
    """
    
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
        self.runtime_controller = None
        self.simulation_broker = None
        self.betfair_client = None
        self.auto_generate_correlation_id: bool = True
        self.order_manager: Optional[OrderManager] = None
        self.guard: Optional[Any] = None
        self.metrics_registry = None

        # Dedup State
        self._inflight_keys: Set[str] = set()
        self._seen_correlation_ids: Set[str] = set()
        self._seen_cid_order: Deque[str] = deque()
        self._max_seen_cid_size: int = 50_000

        self._lock = threading.Lock()
        self._runtime_state = NOT_READY
        self._health: Dict[str, Any] = {}

        self._subscribe_bus()
        self.start()

    # ==================================================================
    # READINESS
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
    # [D1] FACTORY PATTERN — Centralized context creation
    # ==================================================================
    def _new_execution_context(self, normalized: Dict[str, Any]) -> _ExecutionContext:
        """
        [D1] Factory method for _ExecutionContext creation.
        Centralizes context creation to reduce misuse.
        This is the ONLY place where _ExecutionContext is instantiated.
        """
        return _ExecutionContext(
            correlation_id=normalized["correlation_id"],
            customer_ref=normalized["customer_ref"],
            created_at=time.time(),
            event_key=normalized.get("event_key"),
            simulation_mode=normalized.get("simulation_mode"),
        )

    # ==================================================================
    # CONTEXT VALIDATION
    # ==================================================================
    @staticmethod
    def _assert_valid_ctx(ctx: Any) -> None:
        """
        Validates execution context.
        
        NOTE: This is a deterrent against accidental misuse, not a security barrier.
        Real protection comes from using _new_execution_context() factory
        and not exporting _ExecutionContext from the module.
        """
        if not isinstance(ctx, _ExecutionContext):
            raise RuntimeError("INVALID_EXECUTION_CONTEXT")
        if getattr(ctx, "_engine_token", None) != "TRADING_ENGINE_INTERNAL":
            raise RuntimeError("INVALID_EXECUTION_CONTEXT_TOKEN")

    @staticmethod
    def _ctx_metadata(ctx: _ExecutionContext) -> Dict[str, Any]:
        return {
            "correlation_id": ctx.correlation_id,
            "customer_ref": ctx.customer_ref,
            "event_key": ctx.event_key,
            "simulation_mode": ctx.simulation_mode,
        }

    @staticmethod
    def _public_status(internal: str) -> str:
        return _INTERNAL_TO_PUBLIC_STATUS.get(internal, internal)

    # ==================================================================
    # [P0] GUARD RAILS — Terminal vs Non-Terminal
    # ==================================================================
    @staticmethod
    def _assert_terminal_status(status: str) -> None:
        if status not in _TERMINAL_STATES:
            raise RuntimeError(f"NON_TERMINAL_STATUS:{status}")

    @staticmethod
    def _assert_non_terminal_status(status: str) -> None:
        if status in _TERMINAL_STATES:
            raise RuntimeError(f"TERMINAL_STATUS_NOT_ALLOWED_HERE:{status}")

    # ==================================================================
    # [P10] CONTRACT INVARIANTS — Centralized
    # ==================================================================
    @staticmethod
    def _assert_terminal_invariants(status: str, ambiguity_reason: Optional[str],
                                     error: Optional[str]) -> None:
        if status == STATUS_AMBIGUOUS and not ambiguity_reason:
            raise RuntimeError("AMBIGUOUS_FINALIZE_REQUIRES_REASON")
        if status == STATUS_DENIED and error is not None:
            raise RuntimeError("DENIED_SHOULD_NOT_CARRY_TECHNICAL_ERROR")
        if status == STATUS_COMPLETED and ambiguity_reason is not None:
            raise RuntimeError("COMPLETED_CANNOT_KEEP_AMBIGUITY_REASON")

    @staticmethod
    def _assert_ack_invariants(status: str, ambiguity_reason: Optional[str]) -> None:
        if status not in _ACK_STATES:
            raise RuntimeError(f"ACK_INVALID_STATUS:{status}")
        if ambiguity_reason is not None:
            raise RuntimeError("ACK_CANNOT_HAVE_AMBIGUITY_REASON")

    # ==================================================================
    # [E] RESULT BUILDER — Uniform shape with lifecycle_stage
    # ==================================================================
    def _build_result(self, ctx: _ExecutionContext, audit: Dict[str, Any], *,
                      status: str, outcome: str,
                      order_id: Optional[Any] = None,
                      reason: Optional[str] = None, error: Optional[str] = None,
                      ambiguity_reason: Optional[str] = None,
                      response: Optional[Any] = None,
                      extra_fields: Optional[Dict[str, Any]] = None,
                      is_terminal: bool = True) -> Dict[str, Any]:
        self._assert_valid_ctx(ctx)
        public_audit = {k: v for k, v in audit.items() if not k.startswith("_")}
        
        lifecycle_stage = "finalized" if is_terminal else "accepted"
        
        result: Dict[str, Any] = {
            "ok": outcome == OUTCOME_SUCCESS,
            "status": self._public_status(status),
            "outcome": outcome,
            "is_terminal": is_terminal,
            "lifecycle_stage": lifecycle_stage,
            "order_id": order_id,
            **self._ctx_metadata(ctx),
            "audit": public_audit,
            "reason": reason,
            "error": error,
            "ambiguity_reason": ambiguity_reason,
            "response": response,
        }
        if extra_fields:
            result.update(extra_fields)
        return result

    # ==================================================================
    # [P0] ACK RESULT BUILDER — Non-terminal
    # ==================================================================
    def _build_ack_result(
            self, ctx: _ExecutionContext, audit: Dict[str, Any], *,
            order_id: Optional[Any] = None,
            status: str,
            reason: Optional[str] = None,
            error: Optional[str] = None,
            response: Optional[Any] = None,
            extra_fields: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        self._assert_valid_ctx(ctx)
        self._assert_non_terminal_status(status)
        self._assert_ack_invariants(status, None)

        self._emit(ctx, audit, "ACKNOWLEDGED",
                   {"order_id": order_id, "status": status, "response": response},
                   category="execution")
        
        self._log_ack_state(ctx, order_id, status)
        self._metric_inc("quick_bet_accepted_total")

        return self._build_result(
            ctx, audit,
            status=status,
            outcome=OUTCOME_SUCCESS,
            order_id=order_id,
            reason=reason,
            error=error,
            response=response,
            extra_fields=extra_fields,
            is_terminal=False,
        )

    # ==================================================================
    # [P9] ACK STATE LOGGING
    # ==================================================================
    def _log_ack_state(self, ctx: _ExecutionContext, order_id: Optional[Any], status: str) -> None:
        logger.debug("ACK_STATE order_id=%s status=%s cid=%s", order_id, status, ctx.correlation_id)

    def _metric_inc(self, name: str, value: int = 1) -> None:
        reg = getattr(self, "metrics_registry", None)
        if reg is not None:
            try:
                reg.inc(name, value)
            except Exception:
                logger.exception("metrics_registry.inc failed")

    # ==================================================================
    # [P0] PASSTHROUGH FIELDS MERGE HELPER
    # ==================================================================
    @staticmethod
    def _merge_passthrough_fields(
        current: Dict[str, Any],
        source: Dict[str, Any],
    ) -> Dict[str, Any]:
        merged = dict(current)
        for key in _PASSTHROUGH_KEYS:
            if key in source:
                merged[key] = source[key]
        return merged

    # ==================================================================
    # [P0] TERMINAL METADATA BUILDER
    # ==================================================================
    def _build_terminal_metadata(
        self,
        *,
        outcome: str,
        reason: Optional[str],
        error: Optional[str],
        ambiguity_reason: Optional[str],
        response: Optional[Any],
        extra_fields: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        meta: Dict[str, Any] = {
            "updated_at": time.time(),
            "outcome": outcome,
            "reason": reason,
            "last_error": error,
            "ambiguity_reason": ambiguity_reason,
            "finalized": True,
        }
        if extra_fields:
            if extra_fields.get("copy_meta"):
                meta["copy_meta"] = extra_fields["copy_meta"]
            if extra_fields.get("pattern_meta"):
                meta["pattern_meta"] = extra_fields["pattern_meta"]
            if "order_origin" in extra_fields:
                meta["order_origin"] = extra_fields["order_origin"]
        if response is not None:
            meta["response"] = response
        return meta

    # ==================================================================
    # [B3] DOUBLE FINALIZATION GUARD — Hard
    # ==================================================================
    def _precheck_finalize(self, order_id: Any) -> None:
        """
        [B3] Hard finalize guard:
        - Blocks if already finalized
        - Blocks if DB state is not terminal
        """
        getter = getattr(self.db, "get_order", None)
        if callable(getter) and order_id is not None:
            order = getter(order_id)
            if isinstance(order, dict):
                if order.get("finalized"):
                    raise RuntimeError("ORDER_ALREADY_FINALIZED")
                
                db_status = order.get("status")
                if db_status and db_status not in _TERMINAL_STATES:
                    raise RuntimeError(f"FINALIZE_ON_NON_TERMINAL_DB_STATE:status={db_status}")

    # ==================================================================
    # [A2] EXECUTOR ROUTING HELPER — Explicit contract
    # ==================================================================
    def _execute_via_executor(self, operation_name: str, fn: Any) -> Any:
        """
        [A2] Executor routing with explicit contract.
        
        SUPPORTED MODES:
        1. executor.submit(operation_name, fn) returning final concrete result
        2. direct fn() execution when executor is None or has no submit()
        
        NOT SUPPORTED:
        - raw coroutine return values
        - native async awaiting inside TradingEngine
        
        REQUIREMENT:
        - async executors must normalize internally and return concrete result, not awaitable
        """
        submit_fn = getattr(self.executor, "submit", None)
        if callable(submit_fn):
            return submit_fn(operation_name, fn)
        return fn()

    # ==================================================================
    # [FIX 3] DUPLICATE HANDLER HELPER
    # ==================================================================
    def _handle_duplicate_request(
        self,
        ctx: _ExecutionContext,
        audit: Dict[str, Any],
        normalized: Dict[str, Any],
        extra_fields: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Handle duplicate by persisting a dedicated duplicate-blocked row."""
        self._metric_inc("duplicate_blocked_total")
        self._emit(
            ctx,
            audit,
            "DUPLICATE_BLOCKED",
            {"customer_ref": ctx.customer_ref},
            category="guard",
        )

        duplicate_ref = self._find_duplicate_reference(ctx)
        duplicate_order_id = self._persist_inflight(ctx, normalized)
        self._emit_critical(
            ctx,
            audit,
            "PERSIST_INFLIGHT",
            {"order_id": duplicate_order_id},
            category="persistence",
        )
        self._transition_order(
            ctx,
            audit,
            duplicate_order_id,
            STATUS_INFLIGHT,
            STATUS_DUPLICATE_BLOCKED,
            extra={"duplicate_of": duplicate_ref},
        )

        duplicate_meta: Dict[str, Any] = {"duplicate_of": duplicate_ref}
        for key in ("copy_meta", "pattern_meta", "order_origin"):
            if key in extra_fields:
                duplicate_meta[key] = extra_fields[key]
            elif key in normalized:
                duplicate_meta[key] = normalized[key]
        self._safe_write_order_metadata(duplicate_order_id, duplicate_meta)

        duplicate_extra_fields = dict(extra_fields)
        duplicate_extra_fields["duplicate_of"] = duplicate_ref
        self._publish_bus_event(
            ctx,
            "QUICK_BET_DUPLICATE",
            order_id=duplicate_order_id,
            duplicate_of=duplicate_ref,
        )

        return self._complete_order_lifecycle(
            ctx,
            audit,
            order_id=duplicate_order_id,
            status=STATUS_DUPLICATE_BLOCKED,
            reason="DUPLICATE_BLOCKED",
            extra_fields=duplicate_extra_fields,
        )

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

    def _publish_bus_event(self, ctx: _ExecutionContext, event_name: str, **extra: Any) -> None:
        self._assert_valid_ctx(ctx)
        publish = getattr(self.bus, "publish", None)
        if callable(publish):
            try:
                publish(event_name, {**self._ctx_metadata(ctx), **extra})
            except Exception:
                logger.exception("Failed to publish %s", event_name)

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

    # ==================================================================
    # [P5] RECOVERY — Normalized result contract
    # ==================================================================
    def recover_after_restart(self) -> Dict[str, Any]:
        recover = getattr(self.state_recovery, "recover", None)
        if not callable(recover):
            return {
                "ok": False,
                "status": "RECOVERY_UNAVAILABLE",
                "recovery": None,
                "reconcile": None,
                "ram_synced": False,
                "reason": "STATE_RECOVERY_UNAVAILABLE"
            }

        ram_synced = self._repopulate_inflight_from_db()
        logger.info("RECOVERY_STARTED ram_synced=%s", ram_synced)

        try:
            recovery_result = recover()
        except Exception as exc:
            logger.exception("state_recovery.recover() raised")
            logger.info("RECOVERY_COMPLETED ok=False reason=exception")
            return {
                "ok": False,
                "status": "RECOVERY_FAILED",
                "recovery": None,
                "reconcile": None,
                "ram_synced": ram_synced,
                "reason": f"RECOVERY_EXCEPTION:{exc}"
            }

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
        logger.info("RECOVERY_COMPLETED ok=%s reconcile=%s", ok, reconcile_result)
        
        return {
            "ok": ok,
            "status": "RECOVERY_TRIGGERED" if ok else "RECOVERY_FAILED",
            "recovery": recovery_result,
            "reconcile": reconcile_result,
            "ram_synced": ram_synced,
            "reason": recovery_result.get("reason")
        }

    # ==================================================================
    # CORE ENGINE
    # ==================================================================
    def _submit_via_engine(self, request: Dict[str, Any]) -> Dict[str, Any]:
        self.assert_ready()
        self._metric_inc("quick_bet_requests_total")

        normalization_error: Optional[Exception] = None
        normalized: Optional[Dict[str, Any]] = None
        try:
            normalized = self._normalize_request(request)
        except (ValueError, TypeError) as exc:
            normalization_error = exc
            logger.warning("Request normalization failed: %s", exc)
            raw = request if isinstance(request, dict) else {}
            
            # [FIX] Best-effort origin preservation for invalid requests
            origin_fields = self._extract_origin_fields_best_effort(raw)
            
            # [FIX] Proper customer_ref handling to avoid empty string
            raw_customer_ref = str(raw.get("customer_ref") or "").strip()
            customer_ref = raw_customer_ref or "UNKNOWN"
            
            # [D1] Construct normalized dict for factory, even on error
            normalized = {
                "customer_ref": customer_ref,
                "correlation_id": str(raw.get("correlation_id") or uuid.uuid4()),
                **origin_fields,
            }

        # [D1] Use factory for ALL context creation
        ctx = self._new_execution_context(normalized)

        # [P0] Centralized passthrough merge
        extra_fields: Dict[str, Any] = {}
        extra_fields = self._merge_passthrough_fields(extra_fields, normalized)

        audit = self._new_audit(ctx)
        audit["order_origin"] = normalized.get("order_origin", ORIGIN_NORMAL)

        order_id: Optional[Any] = None

        if normalization_error is not None:
            self._emit(ctx, audit, "VALIDATION_FAILED",
                       {"error": str(normalization_error)}, category="guard")
            return self._complete_order_lifecycle(
                ctx, audit, order_id=None,
                status=STATUS_FAILED, reason="INVALID_REQUEST", error=str(normalization_error),
                extra_fields=extra_fields)

        try:
            self._emit(ctx, audit, "REQUEST_RECEIVED", {"request": normalized}, category="request")

            safe_on = self._is_safe_mode_enabled()
            self._emit(ctx, audit, "SAFE_MODE_CHECK", {"enabled": safe_on}, category="guard")
            if safe_on:
                self._emit(ctx, audit, "SAFE_MODE_DENIED", {}, category="guard")
                return self._complete_order_lifecycle(
                    ctx, audit, order_id=None,
                    status=STATUS_DENIED, reason="SAFE_MODE_ACTIVE",
                    extra_fields=extra_fields)

            risk_result = self._risk_gate(normalized)
            normalized = risk_result.get("payload", normalized)
            # [P0] Merge passthrough after risk
            extra_fields = self._merge_passthrough_fields(extra_fields, normalized)

            self._emit(ctx, audit, "RISK_DECISION", risk_result, category="guard")
            if not bool(risk_result.get("allowed", False)):
                order_id = self._persist_inflight(ctx, normalized)
                self._emit_critical(ctx, audit, "PERSIST_INFLIGHT",
                                    {"order_id": order_id}, category="persistence")
                self._transition_order(ctx, audit, order_id, STATUS_INFLIGHT, STATUS_DENIED,
                                       extra={"risk_reason": risk_result.get("reason")})
                self._emit(ctx, audit, "RISK_DENIED",
                           {"reason": risk_result.get("reason")}, category="guard")
                return self._complete_order_lifecycle(
                    ctx, audit, order_id=order_id,
                    status=STATUS_DENIED,
                    reason=str(risk_result.get("reason", "RISK_DENY")),
                    extra_fields=extra_fields)

            with self._lock:
                if not self._dedup_allow(ctx):
                    # [FIX 3] Use dedicated helper with NO DB INSERT
                    return self._handle_duplicate_request(ctx, audit, normalized, extra_fields)

                self._emit(ctx, audit, "DEDUP_DECISION", {"allowed": True}, category="guard")
                order_id = self._persist_inflight(ctx, normalized)
                self._emit_critical(ctx, audit, "PERSIST_INFLIGHT",
                                    {"order_id": order_id}, category="persistence")
                self._publish_bus_event(ctx, "QUICK_BET_ROUTED", order_id=order_id)
                return self._atomic_submit(ctx, audit, order_id, normalized, extra_fields)

        except Exception as exc:
            logger.exception("Fatal error in trading engine")
            
            # [FIX 1] HARD FIX: degraded se DB non aggiornabile
            marked_failed = False
            if order_id is not None:
                marked_failed = self._safe_mark_failed(
                    ctx, audit, order_id, reason="ENGINE_FATAL", error=str(exc)
                )

            if order_id is not None and not marked_failed:
                return self._build_degraded_fatal_result(
                    ctx, audit, order_id, exc, extra_fields
                )

            return self._complete_order_lifecycle(
                ctx, audit, order_id=order_id,
                status=STATUS_FAILED,
                error=str(exc),
                extra_fields=extra_fields
            )

    # ==================================================================
    # TERMINAL LIFECYCLE ORCHESTRATOR
    # ==================================================================
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

        # [P0] Guard rail: only terminal states allowed
        self._assert_terminal_status(status)

        if status not in _STATUS_TO_OUTCOME:
            raise RuntimeError(f"UNKNOWN_STATUS_IN_LIFECYCLE:{status}")
        outcome = _STATUS_TO_OUTCOME[status]

        # [P10] Centralized invariants
        self._assert_terminal_invariants(status, ambiguity_reason, error)

        # [P1/P2] Stronger finalize guard
        if order_id is not None:
            self._precheck_finalize(order_id)

        # Terminal state logging
        self._log_terminal_state(ctx, audit, order_id, status)

        # [FIX 2] TERMINAL EVENT NAME MAPPING
        if status == STATUS_COMPLETED:
            final_event = "FINAL_SUCCESS"
        elif status == STATUS_FAILED:
            final_event = "FINAL_FAILURE"
        elif status == STATUS_AMBIGUOUS:
            final_event = "FINAL_AMBIGUOUS"
        elif status == STATUS_DENIED:
            final_event = "FINAL_DENIED"
        elif status == STATUS_DUPLICATE_BLOCKED:
            final_event = "FINAL_DUPLICATE"
        else:
            final_event = "FINALIZED"

        self._emit(
            ctx,
            audit,
            final_event,
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

        # [FINAL FIX] Metadata write is best-effort in terminal path
        finalization_persisted = True
        if order_id is not None:
            meta = self._build_terminal_metadata(
                outcome=outcome,
                reason=reason,
                error=error,
                ambiguity_reason=ambiguity_reason,
                response=response,
                extra_fields=extra_fields,
            )
            finalization_persisted = self._safe_write_order_metadata(order_id, meta)

        # Release keys (ONLY on terminal)
        try:
            if outcome in (OUTCOME_SUCCESS, OUTCOME_FAILURE):
                self._release_customer_ref_if_terminal(ctx)
        except Exception:
            logger.exception("Failed to release inflight keys")

        if terminal_bus_event:
            self._publish_bus_event(ctx, terminal_bus_event, order_id=order_id)
        else:
            self._publish_terminal_event(ctx, outcome, status, order_id=order_id)

        result = self._build_result(
            ctx, audit, status=status, outcome=outcome, order_id=order_id,
            reason=reason, error=error, ambiguity_reason=ambiguity_reason,
            response=response, extra_fields=extra_fields,
            is_terminal=True,
        )
        
        # [FINAL FIX] Honest contract: report persistence status
        result["finalization_persisted"] = finalization_persisted
        if not finalization_persisted:
            result["lifecycle_stage"] = "degraded"
            result["is_terminal"] = False
        if not finalization_persisted:
            self._metric_inc("finalization_degraded_total")
        else:
            self._metric_inc("quick_bet_finalized_total")
        
        return result

    # ==================================================================
    # [FINAL FIX] DEGRADED FATAL RESULT — Honest contract
    # ==================================================================
    def _build_degraded_fatal_result(self, ctx: _ExecutionContext, audit: Dict[str, Any],
                                     order_id: Optional[Any], exc: Exception,
                                     extra_fields: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Build result when engine cannot safely finalize due to DB failure.
        Bypasses _complete_order_lifecycle to avoid precheck crashes.
        
        Returns honest contract:
        - is_terminal=False (engine knows it failed, but DB didn't confirm)
        - lifecycle_stage="degraded"
        - finalization_persisted=False
        """
        self._assert_valid_ctx(ctx)
        logger.error("DEGRADED_FATAL_RESULT order_id=%s cid=%s error=%s",
                     order_id, ctx.correlation_id, exc)
        
        # Emit memory-only audit since DB is likely unavailable
        self._emit(ctx, audit, "FATAL_DEGRADED",
                   {"order_id": order_id, "error": str(exc)}, category="failure")
        
        result = self._build_result(
            ctx, audit,
            status=STATUS_FAILED,
            outcome=OUTCOME_FAILURE,
            order_id=order_id,
            reason="ENGINE_FATAL_DB_UNAVAILABLE",
            error=str(exc),
            response=None,
            extra_fields=extra_fields,
            is_terminal=False,
        )
        # Override lifecycle_stage to be explicit about degraded state
        result["lifecycle_stage"] = "degraded"
        result["finalization_persisted"] = False
        return result

    # ==================================================================
    # TERMINAL STATE LOGGING
    # ==================================================================
    def _log_terminal_state(
            self, ctx: _ExecutionContext, audit: Dict[str, Any],
            order_id: Optional[Any], final_status: str) -> None:
        """
        Logs terminal state for observability.
        Note: This does NOT verify DB state or block incoherent transitions.
        """
        self._assert_valid_ctx(ctx)
        if final_status in _TERMINAL_STATES:
            logger.debug("TERMINAL_STATE_LOG order_id=%s status=%s", order_id, final_status)

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

        # [ACK vs TERMINAL] Return ACK, NOT terminal lifecycle
        return self._build_ack_result(
            ctx, audit, order_id=order_id,
            status=STATUS_SUBMITTED, response=response, extra_fields=extra_fields)

    # ==================================================================
    # [A1] EXECUTOR DISPATCH — Async blocked
    # ==================================================================
    def _execute_submit(self, ctx: _ExecutionContext, request: Dict[str, Any]) -> Any:
        self._assert_valid_ctx(ctx)

        def _do() -> Any:
            return self._submit_to_order_path(ctx, request)

        # [P0] Use cleaner routing helper
        response = self._execute_via_executor("quick_bet", _do)
        
        # [A1] Block coroutine not normalized
        if inspect.isawaitable(response):
            raise RuntimeError("ASYNC_EXECUTOR_NOT_NORMALIZED")
        
        # [A1] None is ambiguity
        if response is None:
            raise ExecutionError(
                f"EXECUTOR_RETURNED_NONE for cid={ctx.correlation_id}",
                error_type=ERROR_AMBIGUOUS,
                ambiguity_reason=AMBIGUITY_SUBMIT_UNKNOWN,
            )
        self._raise_if_failed_semantic_response(response)
        return response

    def _raise_if_failed_semantic_response(self, response: Any) -> None:
        if not isinstance(response, dict):
            return

        status = str(response.get("status") or "").upper()
        reason_code = str(response.get("reason_code") or "").upper()
        error_class = str(response.get("error_class") or "").upper()
        has_error = bool(response.get("error"))

        if (
            error_class == ERROR_AMBIGUOUS
            or status == STATUS_AMBIGUOUS
            or reason_code in {"SUBMIT_TIMEOUT", "UNKNOWN"}
        ):
            raise ExecutionError(
                f"DOWNSTREAM_AMBIGUOUS_RESPONSE:{response}",
                error_type=ERROR_AMBIGUOUS,
                ambiguity_reason=AMBIGUITY_SUBMIT_UNKNOWN,
            )

        semantic_failure = (
            response.get("ok") is False
            or status in {STATUS_FAILED, STATUS_DENIED, "REJECTED", "ERROR", "FAILURE"}
            or reason_code in {"BROKER_REJECTED", "CANCEL_REJECTED", "REPLACE_REJECTED"}
            or has_error
        )
        if semantic_failure:
            raise RuntimeError(f"DOWNSTREAM_SEMANTIC_FAILURE:{response}")

    # ==================================================================
    # [G] AMBIGUITY RESOLUTION — Single enqueue
    # ==================================================================
    def _resolve_ambiguity(self, ctx: _ExecutionContext, audit: Dict[str, Any],
                           order_id: Any, *, ambiguity_reason: str,
                           trigger_event: str, trigger_error: str,
                           extra_fields: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        self._assert_valid_ctx(ctx)
        self._metric_inc("quick_bet_ambiguous_total")
        logger.error("Ambiguity: %s – %s", trigger_event, trigger_error)
        self._emit(ctx, audit, trigger_event,
                   {"order_id": order_id, "error": trigger_error,
                    "ambiguity_reason": ambiguity_reason}, category="ambiguity")

        try:
            self._transition_order(ctx, audit, order_id, STATUS_INFLIGHT, STATUS_AMBIGUOUS,
                                   extra={"ambiguity_reason": ambiguity_reason, "last_error": trigger_error})
        except Exception:
            logger.exception("Failed to transition to AMBIGUOUS for order_id=%s", order_id)

        # [G] Single enqueue - always called exactly once
        self._enqueue_reconcile(ctx, audit, order_id, ambiguity_reason, extra_fields)

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
        marked_failed = self._safe_mark_failed(
            ctx,
            audit,
            order_id,
            reason="SUBMIT_FAILED",
            error=str(exc),
        )
        if not marked_failed:
            return self._build_degraded_fatal_result(ctx, audit, order_id, exc, extra_fields)
        self._metric_inc("quick_bet_failed_total")
        return self._complete_order_lifecycle(
            ctx, audit, order_id=order_id,
            status=STATUS_FAILED, error=str(exc), reason="SUBMIT_FAILED",
            extra_fields=extra_fields)

    # ==================================================================
    # ORIGIN FIELD EXTRACTION
    # ==================================================================
    def _extract_origin_fields_best_effort(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """
        Best-effort extraction for invalid-request paths.
        Preserves origin/copy/pattern metadata without pretending full normalization succeeded.
        Only preserves structurally valid metadata (dict type required).
        """
        if not isinstance(raw, dict):
            return {"order_origin": ORIGIN_NORMAL}

        extra: Dict[str, Any] = {"order_origin": ORIGIN_NORMAL}

        copy_meta = raw.get("copy_meta")
        pattern_meta = raw.get("pattern_meta")

        if isinstance(copy_meta, dict) and not isinstance(pattern_meta, dict):
            extra["copy_meta"] = {k: copy_meta[k] for k in COPY_META_KEYS if k in copy_meta}
            extra["order_origin"] = ORIGIN_COPY
        elif isinstance(pattern_meta, dict) and not isinstance(copy_meta, dict):
            extra["pattern_meta"] = {k: pattern_meta[k] for k in PATTERN_META_KEYS if k in pattern_meta}
            extra["order_origin"] = ORIGIN_PATTERN

        if "event_key" in raw:
            extra["event_key"] = raw.get("event_key")
        if "simulation_mode" in raw:
            extra["simulation_mode"] = raw.get("simulation_mode")

        return extra

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
            logger.warning("correlation_id auto-generated=%s for customer_ref=%s",
                           correlation_id, customer_ref)

        normalized = dict(request)
        normalized["customer_ref"] = customer_ref
        normalized["correlation_id"] = correlation_id

        copy_meta = request.get("copy_meta")
        pattern_meta = request.get("pattern_meta")

        if copy_meta and not isinstance(copy_meta, dict):
            raise ValueError("COPY_META_MUST_BE_DICT")
        if pattern_meta and not isinstance(pattern_meta, dict):
            raise ValueError("PATTERN_META_MUST_BE_DICT")

        if copy_meta and pattern_meta:
            raise ValueError("COPY_AND_PATTERN_MUTUALLY_EXCLUSIVE")

        if copy_meta:
            normalized["copy_meta"] = {k: copy_meta[k] for k in COPY_META_KEYS if k in copy_meta}
            normalized["order_origin"] = ORIGIN_COPY
        elif pattern_meta:
            normalized["pattern_meta"] = {k: pattern_meta[k] for k in PATTERN_META_KEYS if k in pattern_meta}
            normalized["order_origin"] = ORIGIN_PATTERN
        else:
            normalized["order_origin"] = ORIGIN_NORMAL

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
    # DEDUP
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

        # [FIX] Correct trimming without dead config
        while len(self._seen_correlation_ids) > self._max_seen_cid_size:
            if self._seen_cid_order:
                oldest_cid = self._seen_cid_order.popleft()
                self._seen_correlation_ids.discard(oldest_cid)

    def _release_customer_ref_if_terminal(self, ctx: _ExecutionContext) -> None:
        self._assert_valid_ctx(ctx)
        self._inflight_keys.discard(ctx.customer_ref)

    def _find_duplicate_reference(self, ctx: _ExecutionContext) -> Optional[str]:
        fn = getattr(self.db, "find_duplicate_order", None)
        if callable(fn):
            try:
                return fn(customer_ref=ctx.customer_ref, correlation_id=ctx.correlation_id)
            except Exception:
                logger.exception("find_duplicate_order failed")
        return None

    # ==================================================================
    # SUBMIT PATH
    # ==================================================================
    def _submit_to_order_path(self, ctx: _ExecutionContext, request: Dict[str, Any]) -> Any:
        self._assert_valid_ctx(ctx)
        payload = dict(request)
        payload["customer_ref"] = ctx.customer_ref
        payload["correlation_id"] = ctx.correlation_id

        runtime = self.runtime_controller
        if runtime is not None and callable(getattr(runtime, "get_effective_execution_mode", None)):
            mode = str(runtime.get_effective_execution_mode() or "SIMULATION").upper()
            if mode == "SIMULATION":
                if self.simulation_broker is not None and callable(getattr(self.simulation_broker, "execute", None)):
                    return self.simulation_broker.execute(payload)
            elif mode == "LIVE":
                if not bool(getattr(runtime, "is_live_allowed", lambda: False)()):
                    raise RuntimeError("LIVE_EXECUTION_BLOCKED")

                live_client = self.betfair_client
                if live_client is not None:
                    place_order = getattr(live_client, "place_order", None)
                    if callable(place_order):
                        return place_order(payload)

                    place_bet = getattr(live_client, "place_bet", None)
                    if callable(place_bet):
                        return place_bet(**payload)

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
                           order_id: Any, ambiguity_reason: str,
                           extra_fields: Optional[Dict[str, Any]] = None) -> None:
        self._assert_valid_ctx(ctx)
        enqueue = getattr(self.reconciliation_engine, "enqueue", None)
        if callable(enqueue):
            meta = {
                "order_id": order_id,
                "ambiguity_reason": ambiguity_reason,
                **self._ctx_metadata(ctx)
            }
            if extra_fields:
                if "copy_meta" in extra_fields and extra_fields["copy_meta"]:
                    meta["copy_meta"] = extra_fields["copy_meta"]
                if "pattern_meta" in extra_fields and extra_fields["pattern_meta"]:
                    meta["pattern_meta"] = extra_fields["pattern_meta"]
                if "order_origin" in extra_fields:
                    meta["order_origin"] = extra_fields["order_origin"]

            enqueue(**meta)
            self._emit(ctx, audit, "RECONCILE_ENQUEUED",
                       {"order_id": order_id, "ambiguity_reason": ambiguity_reason},
                       category="reconcile")

    # ==================================================================
    # STATE MACHINE / PERSISTENCE
    # ==================================================================
    def _persist_inflight(self, ctx: _ExecutionContext, request: Dict[str, Any]) -> Any:
        self._assert_valid_ctx(ctx)
        payload = {"customer_ref": ctx.customer_ref, "correlation_id": ctx.correlation_id,
                   "status": STATUS_INFLIGHT, "payload": request,
                   "created_at": ctx.created_at, "outcome": None}
        insert_order = getattr(self.db, "insert_order", None)
        if callable(insert_order):
            return insert_order(payload)

        order_id = str(uuid.uuid4())
        logger.warning("DB.insert_order unavailable – local order_id=%s (DEGRADED)", order_id)
        return order_id

    # ==================================================================
    # [B2] TRANSITION WITH DB STATE PRECHECK — Hard
    # ==================================================================
    def _transition_order(self, ctx: _ExecutionContext, audit: Dict[str, Any],
                          order_id: Any, from_status: str, to_status: str,
                          extra: Optional[Dict[str, Any]] = None) -> None:
        self._assert_valid_ctx(ctx)
        if to_status not in ALLOWED_TRANSITIONS.get(from_status, set()):
            raise RuntimeError(f"ILLEGAL_ORDER_TRANSITION:{from_status}->{to_status}")

        # [B2] Hard precheck: raise on mismatch DB
        getter = getattr(self.db, "get_order", None)
        if callable(getter) and order_id is not None:
            try:
                current = getter(order_id)
                if isinstance(current, dict):
                    db_status = current.get("status")
                    if db_status and db_status != from_status:
                        raise RuntimeError(
                            f"STATE_MISMATCH:order_id={order_id} expected={from_status} db={db_status}"
                        )
            except RuntimeError:
                raise
            except Exception:
                logger.exception("get_order failed during transition precheck")

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

    # ==================================================================
    # [FINAL FIX] SAFE WRITE METADATA — Best effort
    # ==================================================================
    def _safe_write_order_metadata(self, order_id: Any, meta: Dict[str, Any]) -> bool:
        """
        Best-effort metadata write for terminal paths.
        Returns True if write succeeded, False otherwise.
        Does NOT raise — absorbs all exceptions.
        """
        try:
            self._write_order_metadata(order_id, meta)
            return True
        except Exception:
            logger.exception("safe_write_order_metadata failed for order_id=%s", order_id)
            return False

    def _safe_mark_failed(self, ctx: _ExecutionContext, audit: Dict[str, Any],
                          order_id: Any, reason: str, error: str,
                          from_status: str = STATUS_INFLIGHT) -> bool:
        """
        Best-effort transition to FAILED.
        Returns True if transition succeeded, False otherwise.
        Does NOT finalize — lifecycle handles that.
        """
        self._assert_valid_ctx(ctx)
        try:
            self._transition_order(ctx, audit, order_id, from_status, STATUS_FAILED,
                                   extra={"failure_reason": reason, "last_error": error})
            return True
        except Exception:
            logger.exception("safe_mark_failed failed for order_id=%s", order_id)
            return False

    # ==================================================================
    # AUDIT — Non-breaking
    # ==================================================================
    def _new_audit(self, ctx: _ExecutionContext) -> Dict[str, Any]:
        self._assert_valid_ctx(ctx)
        return {"correlation_id": ctx.correlation_id, "customer_ref": ctx.customer_ref,
                "events": [], "index": 0, "_last_event_id": None,
                "order_origin": ORIGIN_NORMAL}

    def _emit(self, ctx: _ExecutionContext, audit: Dict[str, Any],
              event_type: str, payload: Dict[str, Any], *, category: str) -> Dict[str, bool]:
        self._assert_valid_ctx(ctx)
        event_id = str(uuid.uuid4())

        origin = audit.get("order_origin", ORIGIN_NORMAL)
        audit_category = f"{category}_{origin.lower()}" if origin != ORIGIN_NORMAL else category

        event = {"event_id": event_id, "parent_event_id": audit["_last_event_id"],
                 "index": audit["index"], "ts": time.time(), "type": event_type,
                 "category": audit_category, "payload": {**payload, "order_origin": origin},
                 **self._ctx_metadata(ctx)}

        audit["index"] += 1
        audit["_last_event_id"] = event_id
        audit["events"].append(event)

        persisted_db = False
        for mn in ("insert_audit_event", "insert_order_event", "append_order_event"):
            fn = getattr(self.db, mn, None)
            if callable(fn):
                # [FINAL FIX] Audit persistence must NEVER break business flow.
                try:
                    fn(event)
                    persisted_db = True
                    break
                except Exception:
                    logger.exception("audit persistence failed via %s", mn)

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

    def _emit_critical(self, ctx: _ExecutionContext, audit: Dict[str, Any],
                       event_type: str, payload: Dict[str, Any], *, category: str) -> Dict[str, bool]:
        result = self._emit(ctx, audit, event_type, payload, category=category)
        if result.get("memory_only"):
            self._metric_inc("audit_memory_only_total")
            logger.warning("CRITICAL audit event %s is memory-only", event_type)
        return result

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
