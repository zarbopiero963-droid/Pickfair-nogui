"""
core/reconciliation_types.py

Public types, enums, data-classes and configuration for the reconciliation
engine. Extracted from core/reconciliation_engine.py to keep that module
focused on the execution logic.

All names are re-exported from core/reconciliation_engine for backward
compatibility: existing imports such as
    from core.reconciliation_engine import ReasonCode, ReconcileConfig
continue to work unchanged.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum, unique
from typing import Any, Dict, FrozenSet, Optional, Tuple


# =============================================================================
# REASON CODES – standardised, machine-readable
# =============================================================================

@unique
class ReasonCode(str, Enum):
    """Standardised reconcile reason codes."""

    # ── classification cases ────────────────────────────────────
    LOCAL_INFLIGHT_EXCHANGE_ABSENT   = "LOCAL_INFLIGHT_EXCHANGE_ABSENT"
    LOCAL_AMBIGUOUS_EXCHANGE_MATCHED = "LOCAL_AMBIGUOUS_EXCHANGE_MATCHED"
    LOCAL_ABSENT_EXCHANGE_PRESENT    = "LOCAL_ABSENT_EXCHANGE_PRESENT"
    SPLIT_STATE                      = "SPLIT_STATE"

    # ── resolution outcomes ─────────────────────────────────────
    EXCHANGE_WINS_MATCHED            = "EXCHANGE_WINS_MATCHED"
    EXCHANGE_WINS_PARTIAL            = "EXCHANGE_WINS_PARTIAL"
    EXCHANGE_WINS_CANCELLED          = "EXCHANGE_WINS_CANCELLED"
    EXCHANGE_WINS_LAPSED             = "EXCHANGE_WINS_LAPSED"
    LOCAL_WINS_SAGA_PENDING          = "LOCAL_WINS_SAGA_PENDING"
    LOCAL_WINS_TERMINAL              = "LOCAL_WINS_TERMINAL"
    GHOST_ORDER_DETECTED             = "GHOST_ORDER_DETECTED"
    GHOST_REPLACED_ORDER             = "GHOST_REPLACED_ORDER"
    RESOLVED_UNKNOWN_TO_FAILED       = "RESOLVED_UNKNOWN_TO_FAILED"
    RESOLVED_PLACED_TO_FAILED_TIMEOUT = "RESOLVED_PLACED_TO_FAILED_TIMEOUT"
    RESOLVED_UNKNOWN_TO_MATCHED      = "RESOLVED_UNKNOWN_TO_MATCHED"
    CONVERGED                        = "CONVERGED"
    CONVERGENCE_TIMEOUT              = "CONVERGENCE_TIMEOUT"
    NO_LEGS                          = "NO_LEGS"
    BATCH_NOT_FOUND                  = "BATCH_NOT_FOUND"
    ALREADY_TERMINAL                 = "ALREADY_TERMINAL"
    TRANSIENT_ERROR                  = "TRANSIENT_ERROR"
    PERMANENT_ERROR                  = "PERMANENT_ERROR"
    AUTH_ERROR                       = "AUTH_ERROR"
    MAX_CYCLES_EXCEEDED              = "MAX_CYCLES_EXCEEDED"
    ROLLBACK_REQUESTED               = "ROLLBACK_REQUESTED"
    TERMINAL_FINALIZED               = "TERMINAL_FINALIZED"
    PARTIAL_ROLLBACK                 = "PARTIAL_ROLLBACK"
    IDEMPOTENT_SKIP                  = "IDEMPOTENT_SKIP"
    RECONCILE_ALREADY_RUNNING        = "RECONCILE_ALREADY_RUNNING"
    AUDIT_PERSIST_FAILED             = "AUDIT_PERSIST_FAILED"
    RECOVERY_MARKER_SET              = "RECOVERY_MARKER_SET"
    FETCH_PERMANENT_FAILURE          = "FETCH_PERMANENT_FAILURE"


# =============================================================================
# ERROR CLASSIFICATION
# =============================================================================

@unique
class ErrorClass(str, Enum):
    """Classification of fetch/API errors for retry decisions."""
    TRANSIENT  = "TRANSIENT"    # timeout, connection reset, 5xx
    PERMANENT  = "PERMANENT"   # invalid market, 4xx non-auth
    AUTH       = "AUTH"         # 401, 403, session expired
    UNKNOWN    = "UNKNOWN"     # unclassifiable


# well-known exception substrings/types → classification
_PERMANENT_ERROR_MARKERS: Tuple[str, ...] = (
    "invalid market",
    "invalid_market",
    "market not found",
    "market_not_found",
    "no such market",
    "invalid selection",
    "invalid_selection",
    "bad request",
    "bad_request",
    "not found",
    "not_found",
    "invalid argument",
    "invalid_argument",
    "invalid_input",
)

_AUTH_ERROR_MARKERS: Tuple[str, ...] = (
    "unauthorized",
    "authentication",
    "permission denied",
    "forbidden",
    "session expired",
    "not logged in",
    "invalid session",
    "no session",
    "ssoid",
    "401",
    "403",
)


def classify_error(exc: BaseException) -> ErrorClass:
    """Classify an exception into TRANSIENT / PERMANENT / AUTH."""
    msg = str(exc).lower()
    exc_type = type(exc).__name__.lower()

    for marker in _AUTH_ERROR_MARKERS:
        if marker in msg or marker in exc_type:
            return ErrorClass.AUTH

    for marker in _PERMANENT_ERROR_MARKERS:
        if marker in msg or marker in exc_type:
            return ErrorClass.PERMANENT

    if any(t in msg or t in exc_type for t in (
        "timeout", "timed out", "connection", "reset", "unavailable",
        "throttl", "rate limit", "retry", "temporary", "503", "502",
        "504", "eof", "broken pipe",
    )):
        return ErrorClass.TRANSIENT

    # default: treat as transient (safer — will retry)
    return ErrorClass.TRANSIENT


# =============================================================================
# DECISION LOG ENTRY
# =============================================================================

@dataclass
class DecisionEntry:
    """Single persisted decision taken during reconciliation."""

    timestamp: float
    batch_id: str
    leg_index: Optional[int]
    case_classification: str
    reason_code: str
    local_status: str
    exchange_status: Optional[str]
    resolved_status: str
    merge_winner: str                 # "LOCAL" | "EXCHANGE" | "NONE"
    details: Dict[str, Any] = field(default_factory=dict)
    persisted: bool = False           # True if already written to DB
    persist_ok: Optional[bool] = None # result of last persist attempt

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d.pop("persisted", None)      # internal flags, not for DB
        d.pop("persist_ok", None)
        return d


# =============================================================================
# LEG STATUS CONSTANTS
# =============================================================================

TERMINAL_LEG_STATUSES: FrozenSet[str] = frozenset({
    "MATCHED", "FAILED", "CANCELLED", "ROLLED_BACK", "LAPSED", "VOIDED",
})

NON_TERMINAL_LEG_STATUSES: FrozenSet[str] = frozenset({
    "CREATED", "SUBMITTED", "PLACED", "PARTIAL", "UNKNOWN",
})

TERMINAL_BATCH_STATUSES: FrozenSet[str] = frozenset({
    "EXECUTED", "ROLLED_BACK", "FAILED", "CANCELLED",
})

ALL_LEG_STATUSES: FrozenSet[str] = TERMINAL_LEG_STATUSES | NON_TERMINAL_LEG_STATUSES


# =============================================================================
# OUTBOX ENTRY — transactional event pattern
# =============================================================================

@dataclass
class OutboxEntry:
    """Event queued for reliable delivery via outbox pattern."""
    timestamp: float
    batch_id: str
    event_name: str
    payload: Dict[str, Any]
    delivered: bool = False

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d.pop("delivered", None)
        return d


# =============================================================================
# RECONCILE RESULT — structured, multi-layer
# =============================================================================

@dataclass
class ReconcileResult:
    """
    Structured reconcile outcome separating:
      - technical: did reconcile complete?
      - business:  what is the batch state?
      - fetch:     was exchange reachable?
      - audit:     was audit trail persisted?
      - recovery:  was recovery marker handled?
    """
    ok: bool
    batch_id: str
    reason_code: str = ""
    status: str = ""

    # technical
    cycles: int = 0
    fingerprint: str = ""
    converged: bool = False

    # fetch
    fetch_ok: bool = True
    fetch_failure: Optional[str] = None

    # audit
    audit_ok: bool = True
    audit_failure: Optional[str] = None

    # recovery
    recovery_marker_cleared: bool = True

    error: str = ""

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        return {k: v for k, v in d.items() if v is not None}


# =============================================================================
# FORMAL STATE MACHINE — leg transition matrix
# =============================================================================

_ALLOWED_LEG_TRANSITIONS: FrozenSet[Tuple[str, str]] = frozenset({
    # from CREATED
    ("CREATED",   "SUBMITTED"),
    ("CREATED",   "PLACED"),
    ("CREATED",   "FAILED"),
    ("CREATED",   "CANCELLED"),
    # from SUBMITTED
    ("SUBMITTED", "PLACED"),
    ("SUBMITTED", "PARTIAL"),
    ("SUBMITTED", "MATCHED"),
    ("SUBMITTED", "FAILED"),
    ("SUBMITTED", "CANCELLED"),
    ("SUBMITTED", "UNKNOWN"),
    # from PLACED
    ("PLACED",    "PARTIAL"),
    ("PLACED",    "MATCHED"),
    ("PLACED",    "FAILED"),
    ("PLACED",    "CANCELLED"),
    ("PLACED",    "LAPSED"),
    ("PLACED",    "VOIDED"),
    # from PARTIAL
    ("PARTIAL",   "MATCHED"),
    ("PARTIAL",   "FAILED"),
    ("PARTIAL",   "CANCELLED"),
    ("PARTIAL",   "ROLLED_BACK"),
    # from UNKNOWN
    ("UNKNOWN",   "PLACED"),
    ("UNKNOWN",   "PARTIAL"),
    ("UNKNOWN",   "MATCHED"),
    ("UNKNOWN",   "FAILED"),
    ("UNKNOWN",   "CANCELLED"),
    ("UNKNOWN",   "LAPSED"),
    ("UNKNOWN",   "VOIDED"),
    # identity (idempotent no-op, not a real transition)
    ("MATCHED",   "MATCHED"),
    ("FAILED",    "FAILED"),
    ("CANCELLED", "CANCELLED"),
    ("LAPSED",    "LAPSED"),
    ("VOIDED",    "VOIDED"),
    ("ROLLED_BACK", "ROLLED_BACK"),
})


class IllegalTransitionError(Exception):
    """Raised when a leg status transition violates the FSM."""
    def __init__(self, batch_id: str, leg_index: int, from_status: str, to_status: str):
        self.batch_id = batch_id
        self.leg_index = leg_index
        self.from_status = from_status
        self.to_status = to_status
        super().__init__(
            f"Illegal leg transition batch={batch_id} leg={leg_index}: "
            f"{from_status} → {to_status}"
        )


def validate_leg_transition(
    from_status: str, to_status: str,
    batch_id: str = "", leg_index: int = -1,
) -> None:
    """Raise IllegalTransitionError if the transition is not in the FSM."""
    if from_status == to_status:
        return  # identity — always allowed
    if (from_status, to_status) not in _ALLOWED_LEG_TRANSITIONS:
        raise IllegalTransitionError(batch_id, leg_index, from_status, to_status)


# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class ReconcileConfig:
    """Tunables for reconciliation behaviour."""

    # convergence
    max_convergence_cycles: int = 10
    convergence_sleep_secs: float = 0.5

    # retry policy for transient errors
    max_transient_retries: int = 3
    transient_retry_base_delay: float = 1.0
    transient_retry_max_delay: float = 30.0

    # caps
    max_batches_per_run: int = 500

    # ghost order handling
    ghost_order_action: str = "LOG_AND_FLAG"  # LOG_AND_FLAG | CANCEL | IGNORE

    # UNKNOWN resolution grace
    unknown_grace_secs: float = 120.0
    # PLACED resolution timeout (local PLACED + exchange absent)
    placed_order_timeout_secs: float = 300.0

    # audit: fail-closed → abort reconcile if audit persist fails
    audit_fail_closed: bool = True

    # recovery: persist in-progress marker
    persist_recovery_marker: bool = True

    # recovery marker TTL: markers older than this are considered stale
    recovery_marker_ttl_secs: float = 300.0

    # Point 2: transactional updates — require DB to support atomic ops
    require_transactional_db: bool = False

    # Point 3: fencing token for cross-process recovery ownership
    enable_fencing_token: bool = True

    # Point 4: validate batch_manager contract on init
    validate_batch_manager_contract: bool = True

    # Point 5: DB layer hints
    require_wal_mode: bool = False

    # Point 8: runtime invariant checks after each reconcile
    enable_runtime_invariants: bool = True
