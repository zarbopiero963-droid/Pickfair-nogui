from __future__ import annotations

import inspect
import logging
import threading
import time
import uuid
from collections import deque
from typing import Any, Deque, Dict, Optional, Set

from circuit_breaker import CircuitBreaker
from core.trading_constants import (  # noqa: F401 – re-exported for backward compat
    _ACK_STATES,
    _PASSTHROUGH_KEYS,
    _TERMINAL_STATES,
    ALLOWED_TRANSITIONS,
    AMBIGUITY_PERSISTED_NOT_CONFIRMED,
    AMBIGUITY_RESPONSE_LOST,
    AMBIGUITY_SPLIT_STATE,
    AMBIGUITY_SUBMIT_TIMEOUT,
    AMBIGUITY_SUBMIT_UNKNOWN,
    CMD_QUICK_BET,
    COPY_META_KEYS,
    DEGRADED,
    ERROR_AMBIGUOUS,
    ERROR_PERMANENT,
    ERROR_TRANSIENT,
    NOT_READY,
    ORIGIN_COPY,
    ORIGIN_NORMAL,
    ORIGIN_PATTERN,
    OUTCOME_AMBIGUOUS,
    OUTCOME_FAILURE,
    OUTCOME_SUCCESS,
    PATTERN_META_KEYS,
    READY,
    REQ_QUICK_BET,
    STATUS_ACCEPTED_FOR_PROCESSING,
    STATUS_AMBIGUOUS,
    STATUS_COMPLETED,
    STATUS_DENIED,
    STATUS_DUPLICATE_BLOCKED,
    STATUS_FAILED,
    STATUS_INFLIGHT,
    STATUS_SUBMITTED,
    _ExecutionContext,
)

# Forma canonica richiesta da tests/guardrails/test_architecture_guardrails.py
# isort: off
from order_manager import LIFECYCLE_CONTRACT
from order_manager import OrderManager
# isort: on

logger = logging.getLogger(__name__)

# =============================================================================
# NOTE: String constants, set/dict literals, ALLOWED_TRANSITIONS, and
#       _ExecutionContext are defined in core/trading_constants.py and imported
#       above. Existing callers that import from trading_engine are unaffected.
# =============================================================================

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

# =============================================================================
# NOTE: _ExecutionContext is defined in core/trading_constants.py and imported
#       above. Existing callers that reference it via trading_engine are
#       unaffected (no public API change).
# =============================================================================


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
    """Segnaposto usato quando NESSUN risk gate reale e' stato cablato.

    Approva tutto: e' il fail-open storico di H-04/R2. Resta approvante per non
    fermare test e simulazione, ma da qui in poi si DICHIARA non cablato via
    `is_wired()`, cosi' che l'engine possa esporlo nella readiness invece di
    farlo sembrare un gate funzionante.

    ATTENZIONE: nel repo NON esiste ancora un gate reale da mettere al suo
    posto. `core/risk_middleware.py` NON e' un sostituto: e' un sottoscrittore
    di eventi sul bus e non espone `check()`, quindi cablarlo qui lascerebbe
    `_risk_gate` senza `check` — cioe' fail-open travestito da risolto.
    """

    def check(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return {"allowed": True, "reason": None, "payload": payload, "gate": "UNWIRED"}
    def is_ready(self) -> bool: return True
    def is_wired(self) -> bool: return False


class _NullReconciliationEngine:
    def enqueue(self, **_kw: Any) -> None: return None
    def ghost_evidence_snapshot(self) -> Dict[str, Any]: return {}
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
        if not self.risk_gate_wired:
            logger.warning(
                "RISK GATE NON CABLATO: TradingEngine sta usando il segnaposto che "
                "approva ogni richiesta. Nessun limite di rischio viene applicato. "
                "Vedi readiness()['health']['risk_middleware']."
            )
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

        # Circuit breaker for live order submission path.
        # Trips after 3 consecutive failures; blocks for 120s before probe.
        self._order_submission_breaker = CircuitBreaker(max_failures=3, reset_timeout=120.0)

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
            "risk_middleware": self._risk_gate_health(),
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

    @property
    def risk_gate_wired(self) -> bool:
        """Se il gate ATTUALE e' reale o e' il segnaposto (H-04).

        Calcolata al momento, non memorizzata in __init__: se qualcuno
        sostituisce `risk_middleware` dopo la costruzione, un valore congelato
        direbbe il falso proprio nel punto in cui conta (rilievo Fable 5).
        `is_wired()` esiste solo sul segnaposto; un gate reale non lo espone e
        vale come cablato. Ma se lo espone, si accetta SOLO `True`: `is not False`
        avrebbe contato come cablati anche `None`, `0`, `""` — cioe' un gate con
        `is_wired()` difettoso sarebbe risultato operativo nella readiness
        (rilievo GPT-5.6 Sol e Fable 5). Se `is_wired` esplode, si assume NON
        cablato. In ogni ramo ambiguo la risposta e' "non cablato": qui il dubbio
        si risolve verso la prudenza, non verso la comodita'.
        """
        probe = getattr(self.risk_middleware, "is_wired", None)
        if not callable(probe):
            return True
        try:
            return probe() is True
        except Exception:
            logger.exception("is_wired() del risk gate ha sollevato -> assumo NON cablato")
            return False

    def _risk_gate_health(self) -> Dict[str, Any]:
        """Salute del risk gate, che distingue CABLATO da SEGNAPOSTO (H-04).

        `_dep()` non basta: il segnaposto risponde `is_ready() -> True` e
        risulterebbe READY come un gate vero. Un gate che non c'e' e' DEGRADED,
        e il motivo lo dice a chi legge la readiness.
        """
        base = self._dep(self.risk_middleware, required=False)
        if not self.risk_gate_wired:
            return {
                "state": DEGRADED,
                "reason": "risk_gate_not_wired",
                "detail": "segnaposto che approva ogni richiesta: nessun limite applicato",
                "wired": False,
            }
        base["wired"] = True
        return base

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

    @staticmethod
    def _copy_best_price_meta(
        target: Dict[str, Any],
        source: Optional[Dict[str, Any]],
    ) -> None:
        """Copia la provenienza best-price (source/reason) nei record/audit.

        Greptile P2: senza questo, ``best_price_source``/``best_price_reason``
        restano solo nel log e non entrano nei record di lifecycle.
        """
        if not source:
            return
        for key in ("best_price_source", "best_price_reason"):
            if key in source:
                target[key] = source[key]

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
            self._copy_best_price_meta(meta, extra_fields)
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
        # Hard Block Kill Switch (#284)
        if self.safe_mode and hasattr(self.safe_mode, "is_lockdown") and self.safe_mode.is_lockdown():
            logger.warning("TRADING_ENGINE_HARD_BLOCK: System in LOCKDOWN mode. Rejecting order.")
            return {
                "status": STATUS_DENIED,
                "outcome": LIFECYCLE_CONTRACT["FAILED"]["outcome"],
                "reason": "system_lockdown",
                "correlation_id": payload.get("correlation_id"),
            }
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
            # Preserva la provenienza best-price anche sul percorso di
            # normalizzazione fallita: senza questo, audit/result/terminal
            # perderebbero best_price_source/reason di un raw che li aveva
            # (CodeRabbit P-Major). `_extract_origin_fields_best_effort` non li copia.
            self._copy_best_price_meta(normalized, raw)

        # [D1] Use factory for ALL context creation
        ctx = self._new_execution_context(normalized)

        # [P0] Centralized passthrough merge
        extra_fields: Dict[str, Any] = {}
        extra_fields = self._merge_passthrough_fields(extra_fields, normalized)

        audit = self._new_audit(ctx)
        audit["order_origin"] = normalized.get("order_origin", ORIGIN_NORMAL)
        self._copy_best_price_meta(audit, extra_fields)

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
            # order_unknown e' il contratto del client Betfair: risposta persa
            # dopo l'invio, l'ordine puo' esistere sull'exchange => mai FAILED
            # definitivo, sempre AMBIGUOUS + reconciliation.
            or response.get("order_unknown") is True
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

    @staticmethod
    def _risk_deny(request: Dict[str, Any], reason: str) -> Dict[str, Any]:
        return {"allowed": False, "reason": reason, "payload": request}

    def _risk_gate(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """FAIL-CLOSED: qualsiasi anomalia del gate NEGA l'ordine (H-04).

        Prima ogni percorso anomalo — `check` assente, non callable, risultato
        non-dict, dict senza `allowed`, eccezione sollevata — cadeva sullo
        stesso `return {"allowed": True}`. Cioe' un gate ROTTO lasciava passare
        denaro, ed era indistinguibile da un gate che aveva davvero approvato:
        l'evento RISK_DECISION riportava un permesso che nessuno aveva dato.

        Ora ogni percorso anomalo nega con un motivo suo, cosi' il rifiuto e'
        leggibile in RISK_DENIED e nell'esito dell'ordine.
        """
        checker = getattr(self.risk_middleware, "check", None)
        if not callable(checker):
            logger.error("Risk gate senza check() callable -> DENY (fail-closed)")
            return self._risk_deny(request, "RISK_GATE_MISSING_CHECK")

        try:
            result = checker(request)
        except Exception:
            # Un gate che esplode non e' un gate che approva.
            logger.exception("Risk gate ha sollevato -> DENY (fail-closed)")
            return self._risk_deny(request, "RISK_GATE_RAISED")

        if not isinstance(result, dict):
            logger.error("Risk gate ha restituito %s invece di dict -> DENY",
                         type(result).__name__)
            return self._risk_deny(request, "RISK_GATE_BAD_RESULT_TYPE")
        if "allowed" not in result:
            logger.error("Risk gate ha restituito un dict senza 'allowed' -> DENY")
            return self._risk_deny(request, "RISK_GATE_NO_VERDICT")

        # Il verdetto deve essere un bool VERO, non qualcosa di truthy.
        # Il chiamante legge `bool(risk_result.get("allowed", False))`, e in Python
        # bool("false") e bool("no") valgono True: un gate che rispondesse la
        # stringa "false" AUTORIZZEREBBE l'ordine. Stessa trappola con 1, con le
        # liste non vuote e con qualunque oggetto che definisca __bool__.
        # `is True` / `is False` accetta solo i due singleton: niente coercizione.
        verdict = result["allowed"]
        if verdict is not True and verdict is not False:
            logger.error("Risk gate ha restituito allowed=%r (%s) invece di bool -> DENY",
                         verdict, type(verdict).__name__)
            return self._risk_deny(request, "RISK_GATE_NON_BOOLEAN_VERDICT")
        return result

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
    @staticmethod
    def _kwargs_per_place_bet(place_bet: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Adatta il payload interno alla firma dichiarata da ``place_bet``.

        Il payload che gira dentro l'engine usa i nomi del DOMINIO
        (``bet_type``, ``stake``) e porta con se' chiavi di servizio
        (``correlation_id``) e di audit. La firma del client vero e' un'altra
        cosa — la firma keyword-only del client Betfair::

            (*, market_id, selection_id, side, price, size, customer_ref="")

        Senza adattamento `place_bet(**payload)` solleva ``TypeError`` sul
        PRIMO kwarg fuori contratto e l'ordine non parte: in produzione
        entrambi gli entrypoint passano ``client_getter=betfair_service.get_client``,
        che in LIVE restituisce proprio il ``BetfairClient`` — privo di
        ``place_order``, quindi la submission cade qui. Riprodotto in Phase 0
        della PR02/#461: con il client reale zero invii al trasporto, con i
        doppi ``place_bet(**payload)`` dei test il percorso sembrava sano.

        Stessa forma gia' usata da ``OrderManager._raw_place_bet`` e da
        ``OrderRouter.place``: mappa i nomi e taglia per firma, cosi' il client
        live riceve i suoi kwargs e un broker di simulazione non perde i
        metadata di audit. La riscrittura degli adapter NON si fa qui: e' PR26.
        """
        # Campi core assenti: si passano come `None` e li RIFIUTA il client, che
        # e' gia' fail-closed e con errori piu' precisi di un KeyError generico
        # (`INVALID_MARKET_ID`, `INVALID_SELECTION_ID`, `INVALID_PRICE`,
        # `INVALID_SIZE`). Claude Fable 5.1 sulla #478 chiedeva di alzare il
        # controllo qui; misurato, costa piu' di quanto rende: degrada quegli
        # errori a `KeyError('market_id')` e rompe 12 test di ciclo di vita
        # dell'engine che usano payload parziali di proposito. Un ordine
        # incompleto non raggiunge comunque Betfair.
        #
        # Il LATO no: il client non lo rifiuta, lo converte (`safe_side`: tutto
        # cio' che non e' BACK/LAY diventa BACK). Quindi un `bet_type` presente
        # ma vuoto non deve coprire un `side` valido — sarebbe la scommessa
        # opposta. `bet_type or side`, come `BetfairService.place_order`.
        # Rilievo P2 di Codex sulla #478.
        completi = {
            "market_id": payload.get("market_id"),
            "selection_id": payload.get("selection_id"),
            "side": payload.get("bet_type") or payload.get("side"),
            "price": payload.get("price"),
            "size": payload.get("stake", payload.get("size")),
            "customer_ref": payload.get("customer_ref", ""),
            "event_key": payload.get("event_key", ""),
            "table_id": payload.get("table_id"),
            "batch_id": payload.get("batch_id", ""),
            "event_name": payload.get("event_name", ""),
            "market_name": payload.get("market_name", ""),
            "runner_name": payload.get("runner_name", ""),
        }
        try:
            parametri = inspect.signature(place_bet).parameters
        except (TypeError, ValueError):
            # Firma non ispezionabile: si manda il contratto minimo. Inoltrare
            # tutto rischierebbe il TypeError che questa funzione esiste per
            # evitare. `customer_ref` RESTA: e' la chiave di de-dup Betfair
            # (60s) e scartarla proprio su un client avvolto/nativo
            # riaprirebbe la doppia bet che la #452 ha chiuso. Rilievo
            # convergente di GPT-5.6 Sol e Fugu Ultra sulla #478.
            return {k: completi[k] for k in
                    ("market_id", "selection_id", "side", "price", "size",
                     "customer_ref")}
        if any(p.kind is inspect.Parameter.VAR_KEYWORD for p in parametri.values()):
            return dict(completi)
        return {k: v for k, v in completi.items() if k in parametri}

    def _resolve_live_client(self) -> Any:
        """Client live per la submission: override esplicito se impostato
        (test/injection), altrimenti risoluzione LAZY dal getter — il client
        Betfair nasce DOPO la connessione, quindi un valore congelato al
        build resterebbe None e farebbe cadere la submission LIVE sul
        percorso raw client_getter in coda, scavalcando circuit breaker e
        gestione sessione scaduta. Eccezioni del getter propagano
        (fail-closed: meglio nessun ordine che un ordine non protetto)."""
        if self.betfair_client is not None:
            return self.betfair_client
        if callable(self.client_getter):
            return self.client_getter()
        return None

    @staticmethod
    def _e_il_client_betfair_reale(client: Any) -> bool:
        """E' il client che muove denaro vero?

        Identificazione per CLASSE, non per comportamento: `BetfairClient` e'
        l'unico oggetto che parla davvero con Betfair, e riconoscerlo non
        dipende da come e' cablato il runtime. Si guarda la gerarchia della
        classe dell'oggetto (sottoclassi comprese, come `MarketBetfairClient`)
        senza importare nulla.

        La prima versione importava `betfair_client` e, a import fallito,
        rispondeva "si'". Era un ragionamento sbagliato: se la classe non si
        puo' caricare nessun oggetto puo' esserne istanza, quindi la risposta
        giusta e' no. Nel job `trading-engine-hard-tests`, che installa solo
        pytest e quindi non ha `requests`, bloccava anche i doppi dei test.

        Limite dichiarato (GPT-5.6 Sol e Claude Fable 5.1 sulla #478): un
        oggetto che AVVOLGE il client non e' riconosciuto. Oggi e' teorico —
        fuori dai test solo `BetfairClient` e `SimulationBroker` definiscono
        `place_bet`, e `test_premessa_dello_sbarramento_...` diventa rosso se
        ne compare una terza — e un wrapper incontrerebbe comunque il guard
        per modalita' dichiarata, che in produzione c'e' sempre. Riconoscerlo
        per struttura sarebbe duck-typing, il criterio che qui si evita.
        """
        for classe in type(client).__mro__:
            modulo = getattr(classe, "__module__", "") or ""
            if classe.__name__ == "BetfairClient" and (
                modulo == "betfair_client" or modulo.endswith(".betfair_client")
            ):
                return True
        return False

    def _e_il_broker_di_simulazione(self, client: Any) -> bool:
        """Il client risolto e' DIMOSTRABILMENTE il broker di simulazione?

        Identita', non duck-typing: un oggetto che "sembra" un sim e invece
        parla con Betfair piazzerebbe denaro vero. Se non lo si puo'
        dimostrare, la risposta e' no.
        """
        if self.simulation_broker is not None and client is self.simulation_broker:
            return True
        servizio = getattr(self.runtime_controller, "betfair_service", None)
        sim = getattr(servizio, "simulation_broker", None)
        return sim is not None and client is sim

    # Errori che `BetfairClient.place_bet` solleva PRIMA di costruire la
    # richiesta (validazione degli argomenti): il trasporto non e' stato
    # toccato. Sono gli unici che provano che nulla e' partito.
    _ERRORI_PRIMA_DELL_INVIO = frozenset({
        "INVALID_MARKET_ID", "INVALID_SELECTION_ID", "INVALID_PRICE", "INVALID_SIZE",
    })

    @classmethod
    def _esito_certo_o_gia_classificato(cls, exc: Exception) -> bool:
        """L'eccezione del client live puo' arrivare al gestore cosi' com'e'?

        Si', in due casi. Se il gestore la classifica gia' AMBIGUA — un
        `error_type` AMBIGUOUS dichiarato, un `TimeoutError`, "timeout" nel
        testo: `_handle_submit_exception` le tratta cosi', con la ragione
        giusta. Oppure se e' un errore che il client solleva prima di
        costruire la richiesta: li' FAILED e' la verita'.

        Tutto il resto ha posizione IGNOTA rispetto all'invio: un `TypeError`
        nel post-processing di una risposta arrivata, un `AttributeError` su un
        report malformato. Il gestore lo chiamerebbe `ERROR_PERMANENT`,
        finalizzerebbe FAILED e rilascerebbe il lock sul customer_ref — su un
        ordine che puo' essere vivo su Betfair. Senza la prova che nulla sia
        partito, l'esito non si dichiara fallito. Rilievo convergente di GPT-6
        Astra, Claude Fable 5.1, Fugu Ultra e Codex sulla #478; e' la scheda
        PR02 di #461: «risultato ambiguo da riconciliare».
        """
        if getattr(exc, "error_type", None) == ERROR_AMBIGUOUS:
            return True
        if isinstance(exc, TimeoutError) or "timeout" in str(exc).lower():
            return True
        return isinstance(exc, RuntimeError) and str(exc) in cls._ERRORI_PRIMA_DELL_INVIO

    def _submit_to_order_path(self, ctx: _ExecutionContext, request: Dict[str, Any]) -> Any:
        self._assert_valid_ctx(ctx)
        payload = dict(request)
        payload["customer_ref"] = ctx.customer_ref
        payload["correlation_id"] = ctx.correlation_id

        runtime = self.runtime_controller
        # Risoluzione UNICA per submission (anti-TOCTOU): quando il ramo LIVE
        # ha gia' risolto il client, il fallback raw in coda NON deve
        # ri-interrogare il getter — un client comparso tra le due letture
        # (connessione appena stabilita) piazzerebbe l'ordine SENZA breaker
        # ne' gestione sessione. Meglio nessun ordine ora e un ordine
        # protetto alla submission successiva.
        live_client_risolto = False
        # Hard block d'emergenza al chokepoint di submission: vale per OGNI
        # modalita' e percorso (manuale, dutching, copy, fallback). La sola
        # demotion a SIMULATION non basta: senza sim broker configurato il
        # ramo SIMULATION cadrebbe su order_manager/client_getter (live).
        # `is True` per evitare falsi positivi con MagicMock nei test.
        if runtime is not None:
            # Hard block d'emergenza: solleva se emergency definitivo attivo
            # OPPURE se pending-stop sincrono (finestra settlement concorrente) armato.
            # Il recheck e' atomico sotto lock nel runtime_controller.
            entry_blocked_fn = getattr(runtime, "_daily_loss_entry_blocked", None)
            if callable(entry_blocked_fn):
                if bool(entry_blocked_fn()):
                    raise RuntimeError("EMERGENCY_STOP_ACTIVE")
            elif getattr(runtime, "is_emergency_stopped", False) is True:
                raise RuntimeError("EMERGENCY_STOP_ACTIVE")

        modalita_dichiarata = ""
        if runtime is not None and callable(getattr(runtime, "get_effective_execution_mode", None)):
            mode = str(runtime.get_effective_execution_mode() or "SIMULATION").upper()
            modalita_dichiarata = mode
            if mode == "SIMULATION":
                if self.simulation_broker is not None and callable(getattr(self.simulation_broker, "execute", None)):
                    return self.simulation_broker.execute(payload)
            elif mode == "LIVE":
                if not bool(getattr(runtime, "is_live_allowed", lambda: False)()):
                    raise RuntimeError("LIVE_EXECUTION_BLOCKED")

                # Fail-closed: block submission if BetfairService session is invalid.
                # Use `is True` to avoid false-positive on MagicMock / non-bool attributes.
                _betfair_svc = getattr(runtime, "betfair_service", None)
                if _betfair_svc is not None and getattr(_betfair_svc, "_session_invalid", False) is True:
                    raise RuntimeError("LIVE_BLOCKED_SESSION_INVALID")

                live_client = self._resolve_live_client()
                live_client_risolto = True
                if live_client is not None:
                    if self._order_submission_breaker.is_open():
                        raise RuntimeError("ORDER_SUBMISSION_CIRCUIT_BREAKER_OPEN")

                    place_order = getattr(live_client, "place_order", None)
                    place_fn = place_order if callable(place_order) else None
                    if place_fn is None:
                        place_bet = getattr(live_client, "place_bet", None)
                        # Adattamento alla firma del client: vedi
                        # `_kwargs_per_place_bet`. Lo splat diretto del payload
                        # rompeva il ramo LIVE (PR02/#461).
                        place_fn = (
                            (lambda p: place_bet(**self._kwargs_per_place_bet(place_bet, p)))
                            if callable(place_bet) else None
                        )

                    if place_fn is not None:
                        try:
                            result = place_fn(payload)
                        except Exception as _exc:
                            self._order_submission_breaker.record_failure(_exc)
                            if self._esito_certo_o_gia_classificato(_exc):
                                raise
                            # Esito IGNOTO: il trasporto puo' aver gia'
                            # spedito. AMBIGUOUS tiene il lock sul
                            # customer_ref e manda l'ordine in riconciliazione;
                            # FAILED lo rilascerebbe e un nuovo tentativo
                            # potrebbe piazzare una seconda bet reale.
                            raise ExecutionError(
                                f"LIVE_SUBMIT_ESITO_IGNOTO:{type(_exc).__name__}:{_exc}",
                                error_type=ERROR_AMBIGUOUS,
                                ambiguity_reason=AMBIGUITY_SUBMIT_UNKNOWN,
                            ) from _exc

                        # Any ok=False is a submission failure for the breaker.
                        # Session-expiry errors additionally trigger session recovery.
                        if isinstance(result, dict) and not result.get("ok", True):
                            _err = str(result.get("error", "")).upper()
                            if "SESSION_EXPIRED" in _err or "INVALID_SESSION" in _err:
                                if _betfair_svc is not None and callable(
                                    getattr(_betfair_svc, "handle_session_expiry", None)
                                ):
                                    _betfair_svc.handle_session_expiry(
                                        reason=str(result.get("error", "SESSION_EXPIRED"))
                                    )
                                _exc2 = RuntimeError(
                                    str(result.get("error", "SESSION_EXPIRED"))
                                )
                                self._order_submission_breaker.record_failure(_exc2)
                                raise _exc2

                            # Non-session ok=False: count as breaker failure but do not raise
                            # so the caller receives the error dict and can handle it.
                            _exc3 = RuntimeError(
                                str(result.get("error", "SUBMISSION_FAILED"))
                            )
                            self._order_submission_breaker.record_failure(_exc3)
                            return result

                        self._order_submission_breaker.record_success()
                        return result

        if self.order_manager is not None:
            for mn in ("submit", "place_order"):
                fn = getattr(self.order_manager, mn, None)
                if callable(fn):
                    return fn(payload)

        if callable(self.client_getter) and not live_client_risolto:
            client = self.client_getter()
            if client is not None:
                # Fail-closed: se la modalita' e' DICHIARATA, questo fallback
                # puo' usare solo un client dimostrabilmente di simulazione.
                # Ci si arriva per tre strade, tutte pericolose:
                #   SIMULATION  il gate live ha negato e non c'e' ne' sim broker
                #               ne' order manager => il getter restituisce il
                #               client LIVE e parte una bet REALE con
                #               `is_live_allowed()` falso;
                #   LIVE        il ramo protetto non ha risolto il client, ma un
                #               client comparso nel frattempo (connessione
                #               appena stabilita) piazzerebbe senza breaker ne'
                #               gestione sessione — la TOCTOU che
                #               `_resolve_live_client` descrive;
                #   altro       modalita' non riconosciuta: non si puo' sapere
                #               se piazzare sia lecito, quindi non si piazza.
                # Su `main` tutto questo lo impediva per caso il `TypeError` del
                # payload non mappato; l'adattatore della PR02 ha reso quella
                # chiamata valida e ha tolto la rete. Rilievi P1 di Codex sulla
                # #478 (SIMULATION, poi modalita' non riconosciuta), riprodotti.
                #
                # Modalita' NON dichiarata (nessun runtime_controller) resta
                # permessa: e' il percorso normale dell'armatura di test, e in
                # produzione non si verifica — `headless_main.py:299` e
                # `mini_gui.py:444` cablano il runtime subito dopo l'engine.
                # Sbarramento che non dipende dal cablaggio: il client che
                # muove denaro vero non passa MAI di qui, in nessuna modalita'
                # e anche senza `runtime_controller`. Questo ramo e' privo di
                # `is_live_allowed`, controllo di sessione e circuit breaker:
                # il `BetfairClient` reale deve arrivare all'ordine solo dal
                # ramo protetto. GPT-5.6 Sol e Claude Fable 5.1 sulla #478
                # hanno osservato che il guard per-modalita' lasciava scoperto
                # il caso senza runtime; legare la sicurezza al cablaggio era
                # il punto debole, qui la si lega alla CLASSE.
                if self._e_il_client_betfair_reale(client):
                    raise RuntimeError("FALLBACK_NON_PROTETTO_CLIENT_BETFAIR_REALE")
                # In piu': se la modalita' e' DICHIARATA, solo un client
                # dimostrabilmente di simulazione puo' piazzare da qui.
                if modalita_dichiarata and not self._e_il_broker_di_simulazione(client):
                    raise RuntimeError(
                        f"FALLBACK_NON_PROTETTO_IN_MODO_{modalita_dichiarata}"
                    )
                place = getattr(client, "place_bet", None)
                if callable(place):
                    return place(**self._kwargs_per_place_bet(place, payload))

        raise RuntimeError("NO_VALID_EXECUTION_PATH")

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

    def _resolve_reconciliation_engine(self) -> Any:
        """Motore di riconciliazione per l'enqueue delle ambiguita':
        iniezione esplicita se fornita (test/DI), altrimenti risoluzione
        LIVE dal runtime — start() RICOSTRUISCE il motore a ogni avvio,
        quindi un riferimento catturato al build andrebbe stantio (stesso
        principio della risoluzione lazy del client Betfair). Il motore del
        runtime e' accettato solo se espone un enqueue chiamabile."""
        iniettato = self.reconciliation_engine
        if iniettato is not None and not isinstance(iniettato, _NullReconciliationEngine):
            return iniettato
        dal_runtime = getattr(self.runtime_controller, "reconciliation_engine", None)
        if dal_runtime is not None and callable(getattr(dal_runtime, "enqueue", None)):
            return dal_runtime
        return iniettato

    def _enqueue_reconcile(self, ctx: _ExecutionContext, audit: Dict[str, Any],
                           order_id: Any, ambiguity_reason: str,
                           extra_fields: Optional[Dict[str, Any]] = None) -> None:
        self._assert_valid_ctx(ctx)
        enqueue = getattr(self._resolve_reconciliation_engine(), "enqueue", None)
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
                self._copy_best_price_meta(meta, extra_fields)

            # L'intake non deve MAI interrompere il lifecycle dell'ordine
            # ambiguo (transizione gia' persistita su DB): un motore rotto
            # lascia l'ambiguita' affidata allo stato AMBIGUOUS durabile.
            try:
                enqueue(**meta)
            except Exception:
                logger.exception(
                    "reconciliation enqueue fallita per order_id=%s: "
                    "ambiguita' affidata allo stato AMBIGUOUS su DB", order_id)
                self._emit(ctx, audit, "RECONCILE_ENQUEUE_FAILED",
                           {"order_id": order_id, "ambiguity_reason": ambiguity_reason},
                           category="reconcile")
                return
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
