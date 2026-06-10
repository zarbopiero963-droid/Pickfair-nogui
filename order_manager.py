from __future__ import annotations

import logging
import time
import uuid
from enum import Enum, unique
from typing import Any, Dict, FrozenSet, Optional, Set, Tuple

from core.type_helpers import safe_float, safe_int, safe_side

logger = logging.getLogger("OrderManager")

# =============================================================
# 0. CANONICAL LIFECYCLE CONTRACT (cross-module)
# =============================================================
LIFECYCLE_CONTRACT: Dict[str, Dict[str, Any]] = {
    "ACCEPTED": {
        "order_status": "PLACED",
        "trading_engine_status": "ACCEPTED_FOR_PROCESSING",
        "event": "QUICK_BET_ACCEPTED",
        "terminal": False,
        "outcome": "SUCCESS",
    },
    "PARTIAL": {
        "order_status": "PARTIALLY_MATCHED",
        "trading_engine_status": "ACCEPTED_FOR_PROCESSING",
        "event": "QUICK_BET_PARTIAL",
        "terminal": False,
        "outcome": "SUCCESS",
    },
    "FILLED": {
        "order_status": "MATCHED",
        "trading_engine_status": "COMPLETED",
        "event": "QUICK_BET_FILLED",
        "terminal": True,
        "outcome": "SUCCESS",
    },
    "FAILED": {
        "order_status": "FAILED",
        "trading_engine_status": "FAILED",
        "event": "QUICK_BET_FAILED",
        "terminal": True,
        "outcome": "FAILURE",
    },
    "AMBIGUOUS": {
        "order_status": "AMBIGUOUS",
        "trading_engine_status": "AMBIGUOUS",
        "event": "QUICK_BET_AMBIGUOUS",
        "terminal": True,
        "outcome": "AMBIGUOUS",
    },
}

ORDER_STATUS_EVENT_MAP: Dict[str, str] = {
    row["order_status"]: row["event"] for row in LIFECYCLE_CONTRACT.values()
}

TERMINAL_LIFECYCLE_EVENTS: FrozenSet[str] = frozenset(
    row["event"] for row in LIFECYCLE_CONTRACT.values() if row["terminal"]
) | frozenset({"QUICK_BET_SUCCESS", "QUICK_BET_ROLLBACK_DONE"})


# =============================================================
# 1. ORDER STATE MACHINE
# =============================================================
@unique
class OrderStatus(str, Enum):
    """Explicit order lifecycle states."""
    PENDING = "PENDING"
    PLACED = "PLACED"
    PARTIALLY_MATCHED = "PARTIALLY_MATCHED"
    MATCHED = "MATCHED"
    CANCEL_PENDING = "CANCEL_PENDING"
    CANCELLED = "CANCELLED"
    REPLACE_PENDING = "REPLACE_PENDING"
    FAILED = "FAILED"
    AMBIGUOUS = "AMBIGUOUS"
    ROLLBACK_PENDING = "ROLLBACK_PENDING"
    ROLLED_BACK = "ROLLED_BACK"
    EXPIRED = "EXPIRED"


# --- terminal states: once here, the order is sealed ---
TERMINAL_STATES: FrozenSet[OrderStatus] = frozenset({
    OrderStatus.MATCHED,
    OrderStatus.CANCELLED,
    OrderStatus.FAILED,
    OrderStatus.AMBIGUOUS,
    OrderStatus.ROLLED_BACK,
    OrderStatus.EXPIRED,
})

# --- valid transitions (from → set of allowed to) ---
VALID_TRANSITIONS: Dict[OrderStatus, FrozenSet[OrderStatus]] = {
    OrderStatus.PENDING: frozenset({
        OrderStatus.PLACED,
        OrderStatus.PARTIALLY_MATCHED,
        OrderStatus.MATCHED,
        OrderStatus.FAILED,
        OrderStatus.AMBIGUOUS,
    }),
    OrderStatus.PLACED: frozenset({
        OrderStatus.PARTIALLY_MATCHED,
        OrderStatus.MATCHED,
        OrderStatus.CANCEL_PENDING,
        OrderStatus.REPLACE_PENDING,
        OrderStatus.FAILED,
        OrderStatus.EXPIRED,
        OrderStatus.ROLLBACK_PENDING,
    }),
    OrderStatus.PARTIALLY_MATCHED: frozenset({
        OrderStatus.PARTIALLY_MATCHED,   # size update
        OrderStatus.MATCHED,
        OrderStatus.CANCEL_PENDING,
        OrderStatus.REPLACE_PENDING,
        OrderStatus.FAILED,
        OrderStatus.EXPIRED,
        OrderStatus.ROLLBACK_PENDING,
    }),
    OrderStatus.CANCEL_PENDING: frozenset({
        OrderStatus.CANCELLED,
        OrderStatus.FAILED,              # cancel rejected
        OrderStatus.MATCHED,             # filled before cancel
        OrderStatus.PARTIALLY_MATCHED,   # partial cancel
    }),
    OrderStatus.REPLACE_PENDING: frozenset({
        OrderStatus.PLACED,
        OrderStatus.PARTIALLY_MATCHED,
        OrderStatus.MATCHED,
        OrderStatus.FAILED,
    }),
    OrderStatus.ROLLBACK_PENDING: frozenset({
        OrderStatus.ROLLED_BACK,
        OrderStatus.FAILED,
    }),
    # terminal states → nothing
    OrderStatus.MATCHED: frozenset(),
    OrderStatus.CANCELLED: frozenset(),
    OrderStatus.FAILED: frozenset(),
    OrderStatus.AMBIGUOUS: frozenset(),
    OrderStatus.ROLLED_BACK: frozenset(),
    OrderStatus.EXPIRED: frozenset(),
}


def validate_transition(current: OrderStatus, target: OrderStatus) -> None:
    """Raise if the transition is illegal."""
    allowed = VALID_TRANSITIONS.get(current, frozenset())
    if target not in allowed:
        raise InvalidTransitionError(
            f"Transition {current.value} → {target.value} not allowed"
        )


# =============================================================
# 2. ERROR CLASSIFICATION
# =============================================================
@unique
class ErrorClass(str, Enum):
    TRANSIENT = "TRANSIENT"
    PERMANENT = "PERMANENT"
    AMBIGUOUS = "AMBIGUOUS"


# Betfair-level reason codes → classification
_ERROR_MAP: Dict[str, ErrorClass] = {
    # transient
    "TIMEOUT": ErrorClass.TRANSIENT,
    "SERVICE_UNAVAILABLE": ErrorClass.TRANSIENT,
    "TOO_MANY_REQUESTS": ErrorClass.TRANSIENT,
    "RATE_LIMIT": ErrorClass.TRANSIENT,
    "CONNECTION_RESET": ErrorClass.TRANSIENT,
    "TEMPORARY_FAILURE": ErrorClass.TRANSIENT,
    # permanent
    "INVALID_MARKET_ID": ErrorClass.PERMANENT,
    "MARKET_NOT_OPEN_FOR_BETTING": ErrorClass.PERMANENT,
    "MARKET_SUSPENDED": ErrorClass.PERMANENT,
    "INSUFFICIENT_FUNDS": ErrorClass.PERMANENT,
    "BET_TAKEN_OR_LAPSED": ErrorClass.PERMANENT,
    "INVALID_ODDS": ErrorClass.PERMANENT,
    "BELOW_MINIMUM_STAKE": ErrorClass.PERMANENT,
    "RUNNER_REMOVED": ErrorClass.PERMANENT,
    "DUPLICATE_TRANSACTION": ErrorClass.PERMANENT,
    "INVALID_ACCOUNT_STATE": ErrorClass.PERMANENT,
    "PERMISSION_DENIED": ErrorClass.PERMANENT,
    "BET_ACTION_ERROR": ErrorClass.PERMANENT,
    "INVALID_INPUT": ErrorClass.PERMANENT,
    "LOSS_LIMIT_EXCEEDED": ErrorClass.PERMANENT,
    # ambiguous
    "UNKNOWN": ErrorClass.AMBIGUOUS,
    "PROCESSED_WITH_ERRORS": ErrorClass.AMBIGUOUS,
    "ERROR_IN_ORDER": ErrorClass.AMBIGUOUS,
}


def classify_error(reason_code: str, exc: Optional[Exception] = None) -> ErrorClass:
    """Central error classifier."""
    code = (reason_code or "").upper().strip()
    if code in _ERROR_MAP:
        return _ERROR_MAP[code]
    # heuristic for network-class exceptions
    if exc is not None:
        etype = type(exc).__name__
        if etype in ("ConnectionError", "TimeoutError", "OSError", "BrokenPipeError"):
            return ErrorClass.TRANSIENT
    return ErrorClass.AMBIGUOUS


# =============================================================
# 3. REASON CODES
# =============================================================
@unique
class ReasonCode(str, Enum):
    """Standardised reason codes emitted in events / saga."""
    PLACED_OK = "PLACED_OK"
    FULLY_MATCHED = "FULLY_MATCHED"
    PARTIALLY_MATCHED = "PARTIALLY_MATCHED"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    BROKER_UNAVAILABLE = "BROKER_UNAVAILABLE"
    BROKER_REJECTED = "BROKER_REJECTED"
    BROKER_TIMEOUT = "BROKER_TIMEOUT"
    DUPLICATE_ORDER = "DUPLICATE_ORDER"
    CANCEL_OK = "CANCEL_OK"
    CANCEL_REJECTED = "CANCEL_REJECTED"
    REPLACE_OK = "REPLACE_OK"
    REPLACE_REJECTED = "REPLACE_REJECTED"
    ROLLBACK_REQUESTED = "ROLLBACK_REQUESTED"
    ROLLBACK_COMPLETE = "ROLLBACK_COMPLETE"
    RETRY_EXHAUSTED = "RETRY_EXHAUSTED"
    AMBIGUOUS_OUTCOME = "AMBIGUOUS_OUTCOME"
    EXPIRED = "EXPIRED"


# =============================================================
# 4. RETRY POLICY
# =============================================================
class RetryPolicy:
    """Codified retry policy for transient errors."""

    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 0.5,
        backoff_factor: float = 2.0,
        max_delay: float = 10.0,
    ):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.backoff_factor = backoff_factor
        self.max_delay = max_delay

    def delays(self):
        """Yield delay seconds for each retry attempt (0-indexed)."""
        for attempt in range(self.max_attempts):
            delay = min(
                self.base_delay * (self.backoff_factor ** attempt),
                self.max_delay,
            )
            yield attempt, delay


DEFAULT_RETRY = RetryPolicy()


# =============================================================
# 5. BETFAIR STATUS → INTERNAL STATUS MAPPING
# =============================================================
_BETFAIR_LEG_TO_INTERNAL: Dict[str, OrderStatus] = {
    "SUCCESS": OrderStatus.PLACED,       # may be upgraded to MATCHED/PARTIAL
    "FAILURE": OrderStatus.FAILED,
    "FAILED": OrderStatus.FAILED,
    "ERROR": OrderStatus.FAILED,
}

_BETFAIR_OVERALL_OK: FrozenSet[str] = frozenset({
    "SUCCESS", "PROCESSED",
})

_BETFAIR_OVERALL_AMBIGUOUS: FrozenSet[str] = frozenset({
    "PROCESSED_WITH_ERRORS",
})


def map_betfair_status(
    leg_status: str,
    overall_status: str,
    size_matched: float,
    requested_stake: float,
) -> Tuple[OrderStatus, ReasonCode]:
    """Pure function: Betfair response → (OrderStatus, ReasonCode)."""
    leg = leg_status.upper().strip()
    overall = overall_status.upper().strip()

    def _from_sizes() -> Tuple[OrderStatus, ReasonCode]:
        if requested_stake > 0 and size_matched >= requested_stake:
            return OrderStatus.MATCHED, ReasonCode.FULLY_MATCHED
        if size_matched > 0:
            return OrderStatus.PARTIALLY_MATCHED, ReasonCode.PARTIALLY_MATCHED
        return OrderStatus.PLACED, ReasonCode.PLACED_OK

    internal = _BETFAIR_LEG_TO_INTERNAL.get(leg)
    if internal is not None:
        if internal == OrderStatus.FAILED:
            return OrderStatus.FAILED, ReasonCode.BROKER_REJECTED
        return _from_sizes()

    # leg_status unknown → fall back to overall
    if overall in _BETFAIR_OVERALL_OK:
        return _from_sizes()

    # overall is ambiguous (e.g. PROCESSED_WITH_ERRORS + unknown leg)
    if overall in _BETFAIR_OVERALL_AMBIGUOUS:
        return OrderStatus.AMBIGUOUS, ReasonCode.AMBIGUOUS_OUTCOME

    return OrderStatus.FAILED, ReasonCode.BROKER_REJECTED


# =============================================================
# 6. CUSTOM EXCEPTIONS
# =============================================================
class OrderError(Exception):
    """Base."""

class InvalidTransitionError(OrderError):
    pass

class DuplicateOrderError(OrderError):
    pass

class ValidationError(OrderError):
    pass


# =============================================================
# 7. ORDER MANAGER
# =============================================================
class OrderManager:
    """
    Headless order manager — real / simulated.

    Features added:
      • explicit state machine with validated transitions (incl. AMBIGUOUS as real state)
      • DB-level idempotency on customer_ref + composite logical key
      • centralised error classification (TRANSIENT / PERMANENT / AMBIGUOUS)
      • codified retry policy
      • persisted PARTIALLY_MATCHED with residual tracking
      • full update lifecycle (place / cancel / replace / rollback)
      • standardised reason codes
      • simulation ↔ live contract parity (same code path)
      • clear Betfair status → internal status mapping
    """

    def __init__(
        self,
        app: Any = None,
        bus: Any = None,
        db: Any = None,
        client_getter=None,
        retry_policy: Optional[RetryPolicy] = None,
        sleep_fn=None,
    ):
        self.app = app
        self.bus = bus if bus is not None else getattr(app, "bus", None)
        self.db = db if db is not None else getattr(app, "db", None)
        self.client_getter = client_getter
        self.retry_policy = retry_policy or DEFAULT_RETRY
        self._sleep = sleep_fn or time.sleep

    # ---------------------------------------------------------
    # HELPERS
    # ---------------------------------------------------------
    def _publish(self, event_name: str, payload: Dict[str, Any]) -> None:
        if not self.bus:
            return
        try:
            self.bus.publish(event_name, payload)
        except Exception:
            logger.exception("Publish error %s", event_name)

    def _client(self):
        if callable(self.client_getter):
            return self.client_getter()
        if self.app is not None:
            return getattr(self.app, "betfair_client", None)
        return None

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        return safe_float(value, default)

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        return safe_int(value, default)

    @staticmethod
    def _safe_side(value: Any) -> str:
        return safe_side(value)

    @staticmethod
    def _extract_customer_ref(payload: Dict[str, Any]) -> str:
        ref = payload.get("customer_ref")
        return str(ref) if ref else uuid.uuid4().hex

    @staticmethod
    def _extract_instruction_report(response: Dict[str, Any]) -> Dict[str, Any]:
        reports = (
            response.get("instructionReports")
            or response.get("instruction_reports")
            or []
        )
        if not reports:
            return {}
        return reports[0] or {}

    # ---------------------------------------------------------
    # NORMALISE & VALIDATE
    # ---------------------------------------------------------
    def _normalize_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        n = dict(payload or {})
        n["market_id"] = str(
            n.get("market_id") or n.get("marketId") or ""
        ).strip()
        n["selection_id"] = self._safe_int(
            n.get("selection_id", n.get("selectionId"))
        )
        n["bet_type"] = self._safe_side(
            n.get("bet_type") or n.get("side") or n.get("action") or "BACK"
        )
        n["price"] = self._safe_float(n.get("price", n.get("odds")))
        n["stake"] = self._safe_float(n.get("stake", n.get("size")))
        n["simulation_mode"] = bool(n.get("simulation_mode", False))
        n["event_name"] = str(
            n.get("event_name") or n.get("event") or n.get("match") or ""
        )
        n["market_name"] = str(
            n.get("market_name") or n.get("market") or n.get("market_type") or ""
        )
        n["runner_name"] = str(
            n.get("runner_name") or n.get("runnerName") or n.get("selection") or ""
        )
        n["event_key"] = str(n.get("event_key") or "")
        n["batch_id"] = str(n.get("batch_id") or "")
        n["customer_ref"] = self._extract_customer_ref(n)
        n["table_id"] = (
            None if n.get("table_id") in (None, "")
            else self._safe_int(n.get("table_id"))
        )
        return n

    def _validate_payload(self, payload: Dict[str, Any]) -> None:
        if not payload["market_id"]:
            raise ValidationError("market_id mancante")
        if payload["selection_id"] <= 0:
            raise ValidationError("selection_id non valido")
        if payload["price"] <= 1.0:
            raise ValidationError("price non valido (deve essere > 1.0)")
        if payload["stake"] <= 0.0:
            raise ValidationError("stake non valido (deve essere > 0)")

    # ---------------------------------------------------------
    # SAGA PERSISTENCE  (idempotent on two axes)
    # ---------------------------------------------------------
    @staticmethod
    def _logical_order_key(payload: Dict[str, Any]) -> str:
        """
        Composite dedup key: market + selection + side + price + stake.
        Prevents the same *logical* bet from being placed twice even if
        customer_ref differs (e.g. UUID auto-generated on each call).
        """
        return "|".join([
            str(payload.get("market_id", "")),
            str(payload.get("selection_id", "")),
            str(payload.get("bet_type", "")),
            str(payload.get("price", "")),
            str(payload.get("stake", "")),
        ])

    def _save_saga_pending(self, payload: Dict[str, Any]) -> None:
        if not self.db or not hasattr(self.db, "create_order_saga"):
            return

        customer_ref = payload["customer_ref"]

        # --- guard 1: customer_ref uniqueness ---
        if hasattr(self.db, "get_order_saga"):
            existing = self.db.get_order_saga(customer_ref)
            if existing is not None:
                raise DuplicateOrderError(
                    f"customer_ref {customer_ref} already exists"
                )

        # --- guard 2: logical order key uniqueness ---
        logical_key = self._logical_order_key(payload)
        if hasattr(self.db, "get_order_saga_by_logical_key"):
            existing = self.db.get_order_saga_by_logical_key(logical_key)
            if existing is not None:
                existing_status = str(existing.get("status", "")).upper()
                # allow re-placing only if previous attempt has status FAILED
                if existing_status not in ("FAILED",):
                    raise DuplicateOrderError(
                        f"Logical order {logical_key} already exists "
                        f"(status={existing_status}, "
                        f"ref={existing.get('customer_ref')})"
                    )

        self.db.create_order_saga(
            customer_ref=customer_ref,
            batch_id=payload["batch_id"],
            event_key=payload["event_key"],
            table_id=payload["table_id"],
            market_id=payload["market_id"],
            selection_id=payload["selection_id"],
            bet_type=payload["bet_type"],
            price=payload["price"],
            stake=payload["stake"],
            payload=payload,
            status=OrderStatus.PENDING.value,
            logical_key=logical_key,
        )

    def _transition_saga(
        self,
        *,
        customer_ref: str,
        new_status: OrderStatus,
        bet_id: str = "",
        error_text: str = "",
        reason_code: Optional[ReasonCode] = None,
        matched_size: Optional[float] = None,
        remaining_size: Optional[float] = None,
    ) -> None:
        """Validate transition then persist."""
        current_status = self._current_status(customer_ref)
        if current_status is not None:
            validate_transition(current_status, new_status)
        if self.db and hasattr(self.db, "update_order_saga"):
            update_kwargs: Dict[str, Any] = dict(
                customer_ref=customer_ref,
                status=new_status.value,
                bet_id=bet_id,
                error_text=error_text,
            )
            if reason_code is not None:
                update_kwargs["reason_code"] = reason_code.value
            if matched_size is not None:
                update_kwargs["matched_size"] = matched_size
            if remaining_size is not None:
                update_kwargs["remaining_size"] = remaining_size
            self.db.update_order_saga(**update_kwargs)

    def _current_status(self, customer_ref: str) -> Optional[OrderStatus]:
        if not self.db or not hasattr(self.db, "get_order_saga"):
            return None
        saga = self.db.get_order_saga(customer_ref)
        if saga is None:
            return None
        raw = saga.get("status") or saga if isinstance(saga, str) else saga.get("status")
        try:
            return OrderStatus(str(raw).upper())
        except ValueError:
            return None

    # ---------------------------------------------------------
    # BROKER CALL WITH RETRY
    # ---------------------------------------------------------
    def _call_broker_with_retry(
        self, client, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Call client.place_bet with the codified retry policy.
        Only TRANSIENT errors are retried.
        """
        last_exc: Optional[Exception] = None

        for attempt, delay in self.retry_policy.delays():
            try:
                return self._raw_place_bet(client, payload)
            except Exception as exc:
                last_exc = exc
                reason = self._reason_from_exc(exc)
                ec = classify_error(reason, exc)

                if ec == ErrorClass.PERMANENT:
                    raise
                if ec == ErrorClass.AMBIGUOUS:
                    # do NOT retry ambiguous — escalate immediately
                    raise
                # TRANSIENT → retry after delay
                logger.warning(
                    "Transient error (attempt %d/%d): %s — retrying in %.1fs",
                    attempt + 1,
                    self.retry_policy.max_attempts,
                    exc,
                    delay,
                )
                if attempt < self.retry_policy.max_attempts - 1:
                    self._sleep(delay)

        # exhausted
        raise last_exc  # type: ignore[misc]

    def _raw_place_bet(self, client, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Attempt broker call, with legacy-fallback."""
        try:
            return client.place_bet(
                market_id=payload["market_id"],
                selection_id=int(payload["selection_id"]),
                side=payload["bet_type"],
                price=float(payload["price"]),
                size=float(payload["stake"]),
                customer_ref=payload["customer_ref"],
                event_key=payload.get("event_key", ""),
                table_id=payload.get("table_id"),
                batch_id=payload.get("batch_id", ""),
                event_name=payload.get("event_name", ""),
                market_name=payload.get("market_name", ""),
                runner_name=payload.get("runner_name", ""),
            )
        except TypeError:
            # legacy client that doesn't accept extra kwargs
            return client.place_bet(
                market_id=payload["market_id"],
                selection_id=int(payload["selection_id"]),
                side=payload["bet_type"],
                price=float(payload["price"]),
                size=float(payload["stake"]),
            )

    @staticmethod
    def _reason_from_exc(exc: Exception) -> str:
        msg = str(exc).upper()
        for key in _ERROR_MAP:
            if key in msg:
                return key
        return "UNKNOWN"

    # ---------------------------------------------------------
    # MAIN API: PLACE ORDER
    # ---------------------------------------------------------
    def place_order(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        payload = self._normalize_payload(payload)
        self._validate_payload(payload)

        client = self._client()
        if client is None:
            raise RuntimeError("Broker client non disponibile")

        customer_ref = payload["customer_ref"]
        stake = float(payload["stake"])

        # --- idempotency: saga creation (rejects duplicates) ---
        self._save_saga_pending(payload)

        self._publish("QUICK_BET_SUBMITTED", {**payload})

        # --- broker call with retry ---
        try:
            response = self._call_broker_with_retry(client, payload)
        except DuplicateOrderError:
            raise
        except Exception as exc:
            reason = self._reason_from_exc(exc)
            ec = classify_error(reason, exc)

            if ec == ErrorClass.AMBIGUOUS:
                rc = ReasonCode.AMBIGUOUS_OUTCOME
                saga_status = OrderStatus.AMBIGUOUS
            elif ec == ErrorClass.TRANSIENT:
                rc = ReasonCode.RETRY_EXHAUSTED
                saga_status = OrderStatus.FAILED
            else:
                rc = ReasonCode.BROKER_REJECTED
                saga_status = OrderStatus.FAILED

            self._transition_saga(
                customer_ref=customer_ref,
                new_status=saga_status,
                error_text=str(exc),
                reason_code=rc,
            )
            exc_event = (
                "QUICK_BET_AMBIGUOUS"
                if saga_status == OrderStatus.AMBIGUOUS
                else "QUICK_BET_FAILED"
            )
            self._publish(exc_event, {
                **payload, "error": str(exc),
                "error_class": ec.value, "reason_code": rc.value,
            })
            return {
                "ok": False,
                "status": saga_status.value,
                "customer_ref": customer_ref,
                "error": str(exc),
                "error_class": ec.value,
                "reason_code": rc.value,
            }

        # --- interpret response ---
        instruction_report = self._extract_instruction_report(response)
        leg_status = str(instruction_report.get("status") or "").upper()
        bet_id = str(instruction_report.get("betId") or "")
        size_matched = self._safe_float(
            instruction_report.get("sizeMatched"), 0.0
        )
        overall_status = str(response.get("status") or "").upper()

        saga_status, reason_code = map_betfair_status(
            leg_status, overall_status, size_matched, stake
        )

        remaining = max(0.0, stake - size_matched)

        self._transition_saga(
            customer_ref=customer_ref,
            new_status=saga_status,
            bet_id=bet_id,
            error_text="" if saga_status not in (OrderStatus.FAILED, OrderStatus.AMBIGUOUS) else str(response),
            reason_code=reason_code,
            matched_size=size_matched,
            remaining_size=remaining if saga_status == OrderStatus.PARTIALLY_MATCHED else None,
        )

        event_name = ORDER_STATUS_EVENT_MAP.get(
            saga_status.value,
            LIFECYCLE_CONTRACT["ACCEPTED"]["event"],
        )

        out = {
            **payload,
            "customer_ref": customer_ref,
            "bet_id": bet_id,
            "response": response,
            "order_status": saga_status.value,
            "matched_size": size_matched,
            "remaining_size": remaining,
            "reason_code": reason_code.value,
            "simulation_mode": bool(payload.get("simulation_mode", False)),
        }
        self._publish(event_name, out)

        return {
            "ok": saga_status not in (OrderStatus.FAILED, OrderStatus.AMBIGUOUS),
            "status": saga_status.value,
            "customer_ref": customer_ref,
            "bet_id": bet_id,
            "matched_size": size_matched,
            "remaining_size": remaining,
            "reason_code": reason_code.value,
            "response": response,
        }

    # ---------------------------------------------------------
    # CANCEL ORDER
    # ---------------------------------------------------------
    def cancel_order(
        self,
        customer_ref: str,
        bet_id: str = "",
        market_id: str = "",
        size_reduction: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Cancel (or reduce) an existing order."""
        self._transition_saga(
            customer_ref=customer_ref,
            new_status=OrderStatus.CANCEL_PENDING,
            reason_code=ReasonCode.CANCEL_OK,
        )
        self._publish("QUICK_BET_CANCEL_PENDING", {
            "customer_ref": customer_ref,
            "bet_id": bet_id,
        })

        client = self._client()
        if client is None:
            self._transition_saga(
                customer_ref=customer_ref,
                new_status=OrderStatus.FAILED,
                error_text="Broker client non disponibile",
                reason_code=ReasonCode.BROKER_UNAVAILABLE,
            )
            return {"ok": False, "status": OrderStatus.FAILED.value,
                    "reason_code": ReasonCode.BROKER_UNAVAILABLE.value}

        try:
            cancel_kwargs: Dict[str, Any] = {
                "market_id": market_id, "bet_id": bet_id,
            }
            if size_reduction is not None:
                cancel_kwargs["size_reduction"] = size_reduction
            response = client.cancel_orders(**cancel_kwargs)
        except Exception as exc:
            self._transition_saga(
                customer_ref=customer_ref,
                new_status=OrderStatus.FAILED,
                error_text=str(exc),
                reason_code=ReasonCode.CANCEL_REJECTED,
            )
            self._publish("QUICK_BET_CANCEL_FAILED", {
                "customer_ref": customer_ref, "error": str(exc),
            })
            return {"ok": False, "status": OrderStatus.FAILED.value,
                    "error": str(exc), "reason_code": ReasonCode.CANCEL_REJECTED.value}

        instruction_report = self._extract_instruction_report(response)
        cancel_status = str(instruction_report.get("status") or "").upper()

        if cancel_status == "SUCCESS":
            size_cancelled = self._safe_float(
                instruction_report.get("sizeCancelled"), 0.0
            )
            self._transition_saga(
                customer_ref=customer_ref,
                new_status=OrderStatus.CANCELLED,
                reason_code=ReasonCode.CANCEL_OK,
                remaining_size=0.0,
            )
            self._publish("QUICK_BET_CANCELLED", {
                "customer_ref": customer_ref,
                "bet_id": bet_id,
                "size_cancelled": size_cancelled,
                "response": response,
            })
            return {"ok": True, "status": OrderStatus.CANCELLED.value,
                    "size_cancelled": size_cancelled,
                    "reason_code": ReasonCode.CANCEL_OK.value,
                    "response": response}
        else:
            self._transition_saga(
                customer_ref=customer_ref,
                new_status=OrderStatus.FAILED,
                error_text=str(response),
                reason_code=ReasonCode.CANCEL_REJECTED,
            )
            return {"ok": False, "status": OrderStatus.FAILED.value,
                    "reason_code": ReasonCode.CANCEL_REJECTED.value,
                    "response": response}

    # ---------------------------------------------------------
    # REPLACE ORDER  (cancel + re-place, same customer_ref saga)
    # ---------------------------------------------------------
    def replace_order(
        self,
        customer_ref: str,
        bet_id: str,
        market_id: str,
        new_price: float,
    ) -> Dict[str, Any]:
        """
        Replace = change the price of an unmatched order.
        Betfair replaces in-place; we do NOT clone a new saga row.
        """
        self._transition_saga(
            customer_ref=customer_ref,
            new_status=OrderStatus.REPLACE_PENDING,
            reason_code=ReasonCode.REPLACE_OK,
        )
        self._publish("QUICK_BET_REPLACE_PENDING", {
            "customer_ref": customer_ref, "bet_id": bet_id,
            "new_price": new_price,
        })

        client = self._client()
        if client is None:
            self._transition_saga(
                customer_ref=customer_ref,
                new_status=OrderStatus.FAILED,
                error_text="Broker client non disponibile",
                reason_code=ReasonCode.BROKER_UNAVAILABLE,
            )
            return {"ok": False, "status": OrderStatus.FAILED.value,
                    "reason_code": ReasonCode.BROKER_UNAVAILABLE.value}

        try:
            response = client.replace_orders(
                market_id=market_id,
                bet_id=bet_id,
                new_price=new_price,
            )
        except Exception as exc:
            self._transition_saga(
                customer_ref=customer_ref,
                new_status=OrderStatus.FAILED,
                error_text=str(exc),
                reason_code=ReasonCode.REPLACE_REJECTED,
            )
            self._publish("QUICK_BET_REPLACE_FAILED", {
                "customer_ref": customer_ref, "error": str(exc),
            })
            return {"ok": False, "status": OrderStatus.FAILED.value,
                    "error": str(exc),
                    "reason_code": ReasonCode.REPLACE_REJECTED.value}

        instruction_report = self._extract_instruction_report(response)
        rep_status = str(instruction_report.get("status") or "").upper()

        if rep_status == "SUCCESS":
            new_bet_id = str(instruction_report.get("betId") or bet_id)
            self._transition_saga(
                customer_ref=customer_ref,
                new_status=OrderStatus.PLACED,
                bet_id=new_bet_id,
                reason_code=ReasonCode.REPLACE_OK,
            )
            self._publish("QUICK_BET_REPLACED", {
                "customer_ref": customer_ref,
                "old_bet_id": bet_id,
                "new_bet_id": new_bet_id,
                "new_price": new_price,
                "response": response,
            })
            return {"ok": True, "status": OrderStatus.PLACED.value,
                    "bet_id": new_bet_id,
                    "reason_code": ReasonCode.REPLACE_OK.value,
                    "response": response}
        else:
            self._transition_saga(
                customer_ref=customer_ref,
                new_status=OrderStatus.FAILED,
                error_text=str(response),
                reason_code=ReasonCode.REPLACE_REJECTED,
            )
            return {"ok": False, "status": OrderStatus.FAILED.value,
                    "reason_code": ReasonCode.REPLACE_REJECTED.value,
                    "response": response}

    # ---------------------------------------------------------
    # ROLLBACK
    # ---------------------------------------------------------
    def mark_rollback_pending(self, customer_ref: str, reason: str = "") -> None:
        self._transition_saga(
            customer_ref=customer_ref,
            new_status=OrderStatus.ROLLBACK_PENDING,
            error_text=reason,
            reason_code=ReasonCode.ROLLBACK_REQUESTED,
        )
        self._publish("QUICK_BET_ROLLBACK_PENDING", {
            "customer_ref": customer_ref, "reason": reason,
        })

    def mark_rolled_back(self, customer_ref: str, reason: str = "") -> None:
        self._transition_saga(
            customer_ref=customer_ref,
            new_status=OrderStatus.ROLLED_BACK,
            error_text=reason,
            reason_code=ReasonCode.ROLLBACK_COMPLETE,
        )
        self._publish("QUICK_BET_ROLLBACK_DONE", {
            "customer_ref": customer_ref, "reason": reason,
        })

    # ---------------------------------------------------------
    # RESIDUAL EXPOSURE QUERY
    # ---------------------------------------------------------
    def get_residual_exposure(self, customer_ref: str) -> Dict[str, Any]:
        """Return residual unmatched size for a partially matched order."""
        if not self.db or not hasattr(self.db, "get_order_saga"):
            return {"customer_ref": customer_ref, "remaining_size": None}
        saga = self.db.get_order_saga(customer_ref)
        if saga is None:
            return {"customer_ref": customer_ref, "remaining_size": None}
        return {
            "customer_ref": customer_ref,
            "status": saga.get("status"),
            "matched_size": saga.get("matched_size", 0.0),
            "remaining_size": saga.get("remaining_size", 0.0),
            "stake": saga.get("stake", 0.0),
        }
