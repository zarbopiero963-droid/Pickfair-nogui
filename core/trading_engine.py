from __future__ import annotations

import time
import uuid
import logging
from typing import Any, Dict, Optional, Set
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# =========================================================
# STATUS / OUTCOME / TYPES
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

AMBIGUITY_MAP = {
    "timeout": "SUBMIT_TIMEOUT",
    "lost": "RESPONSE_LOST",
    "unknown": "SUBMIT_UNKNOWN",
    "persist_split": "PERSISTED_NOT_CONFIRMED",
}

# =========================================================
# STATE MACHINE
# =========================================================

ALLOWED_TRANSITIONS: Dict[str, Set[str]] = {
    STATUS_INFLIGHT: {STATUS_SUBMITTED, STATUS_FAILED, STATUS_AMBIGUOUS},
    STATUS_SUBMITTED: {STATUS_MATCHED, STATUS_COMPLETED, STATUS_FAILED, STATUS_AMBIGUOUS},
    STATUS_MATCHED: {STATUS_COMPLETED},
    STATUS_AMBIGUOUS: {STATUS_COMPLETED, STATUS_FAILED},
    STATUS_FAILED: set(),
    STATUS_COMPLETED: set(),
    STATUS_DENIED: set(),
}

# =========================================================
# CONTEXT
# =========================================================

@dataclass(frozen=True)
class _ExecutionContext:
    correlation_id: str
    customer_ref: str
    ts: float


# =========================================================
# ENGINE
# =========================================================

class TradingEngine:

    def __init__(self, db, risk, guard, service, reconcile):
        self.db = db
        self.risk = risk
        self.guard = guard
        self.service = service
        self.reconcile = reconcile

        self._state = "NOT_READY"
        self._health = {}

    # =========================================================
    # READINESS (ADVANCED)
    # =========================================================

    def start(self):
        self._health = {
            "db": self._check(self.db),
            "risk": self._check(self.risk),
            "guard": self._check(self.guard),
            "service": self._check(self.service),
            "reconcile": self._check(self.reconcile),
        }

        if all(self._health.values()):
            self._state = "READY"
        elif any(self._health.values()):
            self._state = "DEGRADED"
        else:
            self._state = "NOT_READY"

        if self._state != "READY":
            raise RuntimeError(f"ENGINE_NOT_READY: {self._health}")

    def _check(self, dep):
        fn = getattr(dep, "is_ready", None)
        if callable(fn):
            return fn()
        return dep is not None

    def assert_ready(self):
        if self._state != "READY":
            raise RuntimeError(f"ENGINE_STATE_{self._state}")

    # =========================================================
    # ENTRYPOINT
    # =========================================================

    def submit_order(self, request: Dict[str, Any]) -> Dict[str, Any]:
        self.assert_ready()

        req = self._normalize(request)
        ctx = _ExecutionContext(req["correlation_id"], req["customer_ref"], time.time())

        audit = self._audit_init(ctx)
        order_id = None

        try:
            self._audit(ctx, audit, "REQUEST", req)

            # RISK
            r = self.risk.check(req)
            if not r["allowed"]:
                return self._final(ctx, audit, STATUS_DENIED, OUTCOME_FAILURE, reason="RISK_DENY")

            # DEDUP
            if not self.guard.allow(ctx.customer_ref):
                return self._final(ctx, audit, STATUS_COMPLETED, OUTCOME_SUCCESS, reason="DUPLICATE")

            # PERSIST
            order_id = self._persist(ctx, req)

            # SUBMIT
            try:
                resp = self.service.place_order(req)
                self._transition(order_id, STATUS_INFLIGHT, STATUS_SUBMITTED)

                return self._final(ctx, audit, STATUS_SUBMITTED, OUTCOME_SUCCESS, response=resp)

            except Exception as e:
                return self._handle_error(ctx, audit, order_id, e)

        except Exception as e:
            return self._final(ctx, audit, STATUS_FAILED, OUTCOME_FAILURE, error=str(e))

    # =========================================================
    # ERROR + AMBIGUITY
    # =========================================================

    def _handle_error(self, ctx, audit, order_id, error):

        err_type = getattr(error, "error_type", ERROR_PERMANENT)
        reason = self._classify_ambiguity(error)

        if err_type == ERROR_AMBIGUOUS:
            self._transition(order_id, STATUS_INFLIGHT, STATUS_AMBIGUOUS)
            self.reconcile.enqueue(order_id)

            return self._final(
                ctx, audit,
                STATUS_AMBIGUOUS,
                OUTCOME_AMBIGUOUS,
                ambiguity_reason=reason
            )

        self._transition(order_id, STATUS_INFLIGHT, STATUS_FAILED)

        return self._final(
            ctx, audit,
            STATUS_FAILED,
            OUTCOME_FAILURE,
            error=str(error)
        )

    def _classify_ambiguity(self, error):
        msg = str(error).lower()
        for k, v in AMBIGUITY_MAP.items():
            if k in msg:
                return v
        return "UNKNOWN"

    # =========================================================
    # STATE MACHINE
    # =========================================================

    def _transition(self, order_id, from_s, to_s):
        if to_s not in ALLOWED_TRANSITIONS.get(from_s, set()):
            raise RuntimeError(f"INVALID_TRANSITION {from_s}->{to_s}")

        self.db.update_order(order_id, {"status": to_s})

    # =========================================================
    # PERSIST
    # =========================================================

    def _persist(self, ctx, req):
        return self.db.insert_order({
            "customer_ref": ctx.customer_ref,
            "correlation_id": ctx.correlation_id,
            "status": STATUS_INFLIGHT,
            "payload": req,
            "ts": ctx.ts
        })

    # =========================================================
    # AUDIT (ADVANCED)
    # =========================================================

    def _audit_init(self, ctx):
        return {
            "correlation_id": ctx.correlation_id,
            "events": [],
            "index": 0
        }

    def _audit(self, ctx, audit, event, payload):
        ev = {
            "id": str(uuid.uuid4()),
            "idx": audit["index"],
            "ts": time.time(),
            "type": event,
            "payload": payload,
            "correlation_id": ctx.correlation_id,
        }
        audit["index"] += 1
        audit["events"].append(ev)

        fn = getattr(self.db, "insert_audit_event", None)
        if callable(fn):
            fn(ev)

    # =========================================================
    # FINALIZE (STRICT POLICY)
    # =========================================================

    def _final(self, ctx, audit, status, outcome, **extra):

        result = {
            "status": status,
            "outcome": outcome,
            "correlation_id": ctx.correlation_id,
            "customer_ref": ctx.customer_ref,
            "audit": audit,
        }

        # uniform contract
        for k in ["reason", "error", "ambiguity_reason", "response"]:
            result[k] = extra.get(k)

        return result

    # =========================================================
    # NORMALIZATION
    # =========================================================

    def _normalize(self, req):
        if "customer_ref" not in req:
            raise ValueError("customer_ref required")

        return {
            **req,
            "customer_ref": str(req["customer_ref"]),
            "correlation_id": req.get("correlation_id") or str(uuid.uuid4())
        }