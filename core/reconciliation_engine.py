from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from enum import Enum, unique
from typing import Any, Dict, FrozenSet, Generator, List, Optional, Sequence, Set, Tuple

from core.dutching_batch_manager import DutchingBatchManager

logger = logging.getLogger(__name__)


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
#
# Key: (from_status, to_status) → allowed?
# Any transition NOT in this table is FORBIDDEN.
# Terminal → non-terminal is always blocked.
#

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


# =============================================================================
# BATCH LOCK MANAGER
# =============================================================================

class _BatchLockManager:
    """
    Per-batch non-reentrant lock with zombie protection.

    Guarantees:
      - Only one reconcile_batch() runs per batch_id at a time
      - Different batch_ids can reconcile in parallel
      - Exception during reconcile → lock always released
      - After crash/restart → no zombie locks (in-memory only,
        combined with recovery marker in DB for cross-process)
    """

    def __init__(self) -> None:
        self._global_lock = threading.Lock()
        self._batch_locks: Dict[str, threading.Lock] = {}
        self._batch_owners: Dict[str, int] = {}  # batch_id → thread id

    def _get_lock(self, batch_id: str) -> threading.Lock:
        with self._global_lock:
            if batch_id not in self._batch_locks:
                self._batch_locks[batch_id] = threading.Lock()
            return self._batch_locks[batch_id]

    @contextmanager
    def acquire(self, batch_id: str) -> Generator[bool, None, None]:
        """
        Context manager that yields True if lock acquired, False if
        the batch is already being reconciled by another thread.
        Lock is always released on exit.
        """
        lock = self._get_lock(batch_id)
        acquired = lock.acquire(blocking=False)
        if acquired:
            self._batch_owners[batch_id] = threading.get_ident()
        try:
            yield acquired
        finally:
            if acquired:
                self._batch_owners.pop(batch_id, None)
                lock.release()

    def is_locked(self, batch_id: str) -> bool:
        lock = self._get_lock(batch_id)
        if lock.acquire(blocking=False):
            lock.release()
            return False
        return True

    def cleanup_batch(self, batch_id: str) -> None:
        """Remove lock for a terminal batch to avoid memory leak."""
        with self._global_lock:
            self._batch_locks.pop(batch_id, None)
            self._batch_owners.pop(batch_id, None)


# =============================================================================
# RECONCILIATION ENGINE
# =============================================================================

class ReconciliationEngine:
    """
    Riconcilia i batch dutching al riavvio o su richiesta.

    Production-hardened features
    ----------------------------
    1. Per-batch locking (no concurrent reconcile on same batch)
    2. Strong deterministic convergence (fingerprint + change double-check)
    3. Robust multi-key ghost detection (ref, bet_id, market+selection)
    4. Guaranteed audit persistence (fail-closed or outbox)
    5. Classified retry policy (TRANSIENT / PERMANENT / AUTH)
    6. Full recovery consistency (markers, snapshot reload, idempotency)
    7. Explicit convergence algorithm with cycle cap
    8. Formalised merge policy (exchange wins / local wins)
    9. Standardised reason codes (ReasonCode enum)
    10. Full status handling: partial, cancelled, lapsed, matched,
        absent, ghost, voided, unknown
    11. Multi-key lookup (customer_ref, bet_id, market_id)
    12. 4 canonical case classifications + sub-variants
    """

    def __init__(
        self,
        *,
        db,
        bus=None,
        batch_manager: Optional[DutchingBatchManager] = None,
        betfair_service=None,
        client_getter=None,
        table_manager=None,
        duplication_guard=None,
        config: Optional[ReconcileConfig] = None,
    ):
        self.db = db
        self.bus = bus
        self.batch_manager = batch_manager or DutchingBatchManager(db, bus=bus)
        self.betfair_service = betfair_service
        self.client_getter = client_getter
        self.table_manager = table_manager
        self.duplication_guard = duplication_guard
        self.cfg = config or ReconcileConfig()

        # ── per-batch locking ───────────────────────────────────
        self._lock_mgr = _BatchLockManager()

        # ── persisted decision log (buffer, flushed to DB) ──────
        self._decision_log: List[DecisionEntry] = []
        self._decision_log_lock = threading.Lock()

        # ── idempotency fingerprints ────────────────────────────
        self._reconcile_fingerprints: Dict[str, str] = {}

        # ── outbox for reliable event delivery (Point 6) ────────
        self._outbox: List[OutboxEntry] = []
        self._outbox_lock = threading.Lock()

        # ── fencing token counter (Point 3) ─────────────────────
        self._fencing_counter: int = 0
        self._fencing_lock = threading.Lock()

        # ── lifecycle hooks for crash-recovery testing (Point 10) ──
        self._hooks: Dict[str, Any] = {}

        # ── validate contracts on init (Point 4) ────────────────
        if self.cfg.validate_batch_manager_contract:
            self._validate_batch_manager_contract()

        # ── enforce DB layer requirements (Point 5) ─────────────
        if self.cfg.require_transactional_db:
            for method in ("begin_transaction", "commit_transaction", "rollback_transaction"):
                if not callable(getattr(self.db, method, None)):
                    raise TypeError(
                        f"DB contract violation: require_transactional_db=True "
                        f"but db.{method}() is missing"
                    )

        if self.cfg.require_wal_mode:
            checker = getattr(self.db, "is_wal_mode", None)
            if callable(checker):
                if not checker():
                    raise RuntimeError(
                        "DB is not in WAL mode but require_wal_mode=True"
                    )
            else:
                logger.warning(
                    "require_wal_mode=True but db.is_wal_mode() not available — "
                    "cannot verify WAL mode"
                )

    # ─────────────────────────────────────────────────────────────
    # BATCH MANAGER CONTRACT VALIDATION (Point 4)
    # ─────────────────────────────────────────────────────────────

    _REQUIRED_BM_METHODS = (
        "get_batch", "get_batch_legs", "update_leg_status",
        "recompute_batch_status", "release_runtime_artifacts",
        "mark_batch_failed", "get_open_batches",
    )

    def _validate_batch_manager_contract(self) -> None:
        """Verify batch_manager exposes all required methods."""
        missing = [
            m for m in self._REQUIRED_BM_METHODS
            if not callable(getattr(self.batch_manager, m, None))
        ]
        if missing:
            raise TypeError(
                f"BatchManager contract violation: missing methods {missing}"
            )

    # ─────────────────────────────────────────────────────────────
    # FSM TRANSITION GUARD (Point 1)
    # ─────────────────────────────────────────────────────────────

    def _validate_and_update_leg(
        self,
        *,
        batch_id: str,
        leg_index: int,
        from_status: str,
        to_status: str,
        bet_id: Optional[str] = None,
        raw_response: Optional[Dict[str, Any]] = None,
        error_text: Optional[str] = None,
    ) -> None:
        """
        Validate FSM transition, then delegate to batch_manager.
        Raises IllegalTransitionError if transition is forbidden.
        """
        validate_leg_transition(from_status, to_status, batch_id, leg_index)
        self.batch_manager.update_leg_status(
            batch_id=batch_id,
            leg_index=leg_index,
            status=to_status,
            bet_id=bet_id,
            raw_response=raw_response,
            error_text=error_text,
        )

    # ─────────────────────────────────────────────────────────────
    # TRANSACTIONAL UPDATE (Point 2)
    # ─────────────────────────────────────────────────────────────

    def _transactional_leg_update(
        self,
        *,
        batch_id: str,
        leg_index: int,
        from_status: str,
        to_status: str,
        decision: DecisionEntry,
        bet_id: Optional[str] = None,
        raw_response: Optional[Dict[str, Any]] = None,
        error_text: Optional[str] = None,
    ) -> bool:
        """
        Atomically: persist audit + update leg + enqueue outbox event.
        If DB supports begin_transaction/commit, uses it.
        Returns True on success, False on failure.
        """
        txn_begin = getattr(self.db, "begin_transaction", None)
        txn_commit = getattr(self.db, "commit_transaction", None)
        txn_rollback = getattr(self.db, "rollback_transaction", None)
        has_txn = all(callable(f) for f in (txn_begin, txn_commit, txn_rollback))

        try:
            if has_txn and self.cfg.require_transactional_db:
                txn_begin()

            # 1. persist decision
            if self.cfg.audit_fail_closed:
                if not decision.persist_ok:
                    if has_txn and self.cfg.require_transactional_db:
                        txn_rollback()
                    return False

            # 2. FSM-validated leg update
            self._validate_and_update_leg(
                batch_id=batch_id,
                leg_index=leg_index,
                from_status=from_status,
                to_status=to_status,
                bet_id=bet_id,
                raw_response=raw_response,
                error_text=error_text,
            )

            # 3. enqueue outbox event
            self._enqueue_outbox(
                batch_id=batch_id,
                event_name="LEG_STATUS_CHANGED",
                payload={
                    "batch_id": batch_id,
                    "leg_index": leg_index,
                    "from_status": from_status,
                    "to_status": to_status,
                    "reason_code": decision.reason_code,
                },
            )

            if has_txn and self.cfg.require_transactional_db:
                txn_commit()

            # invoke lifecycle hook (Point 10)
            self._invoke_hook("after_leg_update", batch_id=batch_id,
                              leg_index=leg_index, to_status=to_status)
            return True

        except IllegalTransitionError:
            if has_txn and self.cfg.require_transactional_db:
                txn_rollback()
            raise
        except Exception:
            logger.exception(
                "Transactional update failed batch=%s leg=%d",
                batch_id, leg_index,
            )
            if has_txn and self.cfg.require_transactional_db:
                txn_rollback()
            return False

    # ─────────────────────────────────────────────────────────────
    # OUTBOX — reliable event delivery (Point 6)
    # ─────────────────────────────────────────────────────────────

    def _enqueue_outbox(
        self, *, batch_id: str, event_name: str, payload: Dict[str, Any]
    ) -> None:
        entry = OutboxEntry(
            timestamp=time.time(),
            batch_id=batch_id,
            event_name=event_name,
            payload=payload,
        )
        # persist to DB if available
        writer = getattr(self.db, "write_outbox", None)
        if callable(writer):
            try:
                writer(entry.to_dict())
            except Exception:
                logger.exception("Failed to write outbox entry batch=%s", batch_id)

        with self._outbox_lock:
            self._outbox.append(entry)

    def _drain_outbox(self, batch_id: Optional[str] = None) -> int:
        """Publish pending outbox events via bus, mark delivered. Returns count."""
        with self._outbox_lock:
            pending = [
                e for e in self._outbox
                if not e.delivered and (batch_id is None or e.batch_id == batch_id)
            ]

        delivered = 0
        for entry in pending:
            try:
                self._publish(entry.event_name, entry.payload)
                entry.delivered = True
                delivered += 1
            except Exception:
                logger.exception(
                    "Outbox delivery failed event=%s batch=%s",
                    entry.event_name, entry.batch_id,
                )

        # cleanup delivered entries
        with self._outbox_lock:
            self._outbox = [e for e in self._outbox if not e.delivered]

        return delivered

    # ─────────────────────────────────────────────────────────────
    # FENCING TOKEN (Point 3)
    # ─────────────────────────────────────────────────────────────

    def _next_fencing_token(self) -> int:
        with self._fencing_lock:
            self._fencing_counter += 1
            return self._fencing_counter

    # ─────────────────────────────────────────────────────────────
    # RUNTIME INVARIANT CHECKS (Point 8)
    # ─────────────────────────────────────────────────────────────

    def _check_post_reconcile_invariants(
        self, batch_id: str, *, batch_status: str
    ) -> List[str]:
        """Convenience: fetches legs then delegates."""
        if not self.cfg.enable_runtime_invariants:
            return []
        legs = self.batch_manager.get_batch_legs(batch_id)
        return self._check_post_reconcile_invariants_on_legs(
            batch_id, legs, batch_status=batch_status,
        )

    def _check_post_reconcile_invariants_on_legs(
        self, batch_id: str, legs: List[Dict[str, Any]], *, batch_status: str
    ) -> List[str]:
        """
        Verify post-reconcile invariants on provided legs snapshot.
        Returns list of violations (empty = ok).

        Invariants checked:
          1. No leg status outside ALL_LEG_STATUSES
          2. Terminal batch → all legs terminal
          3. No UNKNOWN beyond TTL without a decision
          4. Leg count > 0 for non-empty batch
        """
        if not self.cfg.enable_runtime_invariants:
            return []

        violations: List[str] = []

        # 1. valid statuses
        for lg in legs:
            st = str(lg.get("status") or "").upper()
            if st and st not in ALL_LEG_STATUSES:
                violations.append(
                    f"leg {lg.get('leg_index')}: invalid status '{st}'"
                )

        # 2. terminal batch consistency
        if batch_status.upper() in TERMINAL_BATCH_STATUSES:
            non_terminal = [
                lg for lg in legs
                if str(lg.get("status") or "").upper() not in TERMINAL_LEG_STATUSES
            ]
            if non_terminal:
                violations.append(
                    f"terminal batch '{batch_status}' has {len(non_terminal)} "
                    f"non-terminal legs"
                )

        # 3. no UNKNOWN beyond TTL without resolution
        for lg in legs:
            st = str(lg.get("status") or "").upper()
            ts = float(lg.get("created_at_ts", 0) or 0)
            if st == "UNKNOWN" and ts > 0:
                age = time.time() - ts
                if age > self.cfg.unknown_grace_secs:
                    violations.append(
                        f"leg {lg.get('leg_index')}: UNKNOWN beyond TTL "
                        f"(age={age:.0f}s > {self.cfg.unknown_grace_secs}s)"
                    )

        if violations:
            logger.error(
                "POST-RECONCILE INVARIANT VIOLATIONS batch=%s: %s",
                batch_id, violations,
            )

        return violations

    # ─────────────────────────────────────────────────────────────
    # LIFECYCLE HOOKS (Point 10)
    # ─────────────────────────────────────────────────────────────

    def register_hook(self, name: str, callback) -> None:
        """Register a lifecycle hook for crash-recovery testing."""
        self._hooks[name] = callback

    def _invoke_hook(self, name: str, **kwargs) -> None:
        hook = self._hooks.get(name)
        if callable(hook):
            try:
                hook(**kwargs)
            except Exception:
                logger.exception("Lifecycle hook '%s' raised", name)

    # ─────────────────────────────────────────────────────────────
    # PUBLISH / CLIENT HELPERS
    # ─────────────────────────────────────────────────────────────

    def _publish(self, event_name: str, payload: Dict[str, Any]) -> None:
        if not self.bus:
            return
        try:
            self.bus.publish(event_name, payload)
        except Exception:
            logger.exception("Errore publish %s", event_name)

    def _get_client(self):
        for source_fn in (self.client_getter, self._service_client):
            if source_fn is None:
                continue
            try:
                client = source_fn()
                if client:
                    return client
            except Exception:
                logger.exception("Errore ottenimento client")
        return None

    def _service_client(self):
        if self.betfair_service and hasattr(self.betfair_service, "get_client"):
            return self.betfair_service.get_client()
        return None

    # ─────────────────────────────────────────────────────────────
    # SAGA HELPERS
    # ─────────────────────────────────────────────────────────────

    def _get_pending_saga_refs(self) -> Set[str]:
        getter = getattr(self.db, "get_pending_sagas", None)
        if not callable(getter):
            return set()
        try:
            return {
                str(r.get("customer_ref") or "").strip()
                for r in (getter() or [])
                if str(r.get("customer_ref") or "").strip()
            }
        except Exception:
            logger.exception("Errore get_pending_sagas")
            return set()

    # ─────────────────────────────────────────────────────────────
    # FETCH WITH CLASSIFIED RETRY
    # ─────────────────────────────────────────────────────────────

    def _fetch_current_orders_by_market(
        self,
        market_id: str,
        *,
        _attempt: int = 0,
    ) -> Tuple[List[Dict[str, Any]], Optional[ReasonCode]]:
        """
        Returns (orders, failure_reason).
        failure_reason is None on success, a ReasonCode on permanent failure.
        Retries only on TRANSIENT errors.  Iterative (no stack overflow).
        """
        client = self._get_client()
        if not client:
            return [], ReasonCode.PERMANENT_ERROR

        attempt = _attempt
        while True:
            try:
                orders = client.get_current_orders(market_ids=[market_id])
                return (list(orders) if orders else []), None
            except Exception as exc:
                err_class = classify_error(exc)

                # ── PERMANENT / AUTH → no retry ─────────────────
                if err_class == ErrorClass.PERMANENT:
                    logger.error(
                        "Permanent error fetching orders market=%s: %s",
                        market_id, exc,
                    )
                    return [], ReasonCode.FETCH_PERMANENT_FAILURE

                if err_class == ErrorClass.AUTH:
                    logger.error(
                        "Auth error fetching orders market=%s: %s",
                        market_id, exc,
                    )
                    return [], ReasonCode.AUTH_ERROR

                # ── TRANSIENT → retry with backoff ──────────────
                if attempt < self.cfg.max_transient_retries:
                    delay = min(
                        self.cfg.transient_retry_base_delay * (2 ** attempt),
                        self.cfg.transient_retry_max_delay,
                    )
                    logger.warning(
                        "Transient error fetching orders market=%s attempt=%d, "
                        "retrying in %.1fs: %s",
                        market_id, attempt, delay, exc,
                    )
                    time.sleep(delay)
                    attempt += 1
                    continue

                logger.exception(
                    "Transient error exhausted retries market=%s attempts=%d",
                    market_id, attempt + 1,
                )
                return [], ReasonCode.TRANSIENT_ERROR

    # ─────────────────────────────────────────────────────────────
    # EXTRACTION HELPERS
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def _first_of(d: Dict[str, Any], *keys: str) -> str:
        for k in keys:
            v = d.get(k)
            if v:
                return str(v).strip()
        return ""

    def _extract_customer_ref(self, order: Dict[str, Any]) -> str:
        return self._first_of(
            order,
            "customerOrderRef", "customer_ref",
            "customerRef", "customerOrderReference",
        )

    def _extract_bet_id(self, order: Dict[str, Any]) -> str:
        return self._first_of(order, "betId", "bet_id", "betID")

    def _extract_order_status(self, order: Dict[str, Any]) -> str:
        return self._first_of(
            order, "status", "orderStatus", "currentOrderStatus"
        ).upper()

    def _extract_selection_id(self, order: Dict[str, Any]) -> str:
        return self._first_of(order, "selectionId", "selection_id")

    def _extract_market_id(self, order: Dict[str, Any]) -> str:
        return self._first_of(order, "marketId", "market_id")

    # ─────────────────────────────────────────────────────────────
    # STATUS MAPPING
    # ─────────────────────────────────────────────────────────────

    def _map_remote_status_to_leg_status(self, order: Dict[str, Any]) -> str:
        status = self._extract_order_status(order)
        size_matched = float(order.get("sizeMatched", 0) or 0)
        size_remaining = float(order.get("sizeRemaining", 0) or 0)

        if status in {"EXECUTION_COMPLETE", "EXECUTABLE"}:
            if size_matched > 0 and size_remaining > 0:
                return "PARTIAL"
            if size_matched > 0 and size_remaining <= 0:
                return "MATCHED"
            return "PLACED"

        if status == "CANCELLED":
            return "CANCELLED"
        if status == "LAPSED":
            return "LAPSED"
        if status == "VOIDED":
            return "VOIDED"
        if status in {"FAILED", "REJECTED"}:
            return "FAILED"

        return "UNKNOWN"

    # ─────────────────────────────────────────────────────────────
    # INDEX BUILDERS – robust multi-key
    # ─────────────────────────────────────────────────────────────

    def _build_exchange_indices(
        self, orders: Sequence[Dict[str, Any]]
    ) -> Tuple[
        Dict[str, Dict[str, Any]],   # by customer_ref
        Dict[str, Dict[str, Any]],   # by bet_id
        Dict[str, Dict[str, Any]],   # by market_id::selection_id
    ]:
        by_ref: Dict[str, Dict[str, Any]] = {}
        by_bet: Dict[str, Dict[str, Any]] = {}
        by_sel: Dict[str, Dict[str, Any]] = {}
        for o in orders:
            ref = self._extract_customer_ref(o)
            bid = self._extract_bet_id(o)
            mid = self._extract_market_id(o)
            sid = self._extract_selection_id(o)
            if ref:
                by_ref[ref] = o
            if bid:
                by_bet[bid] = o
            if mid and sid:
                by_sel[f"{mid}::{sid}"] = o
        return by_ref, by_bet, by_sel

    def _lookup_remote_order(
        self,
        leg: Dict[str, Any],
        by_ref: Dict[str, Dict[str, Any]],
        by_bet: Dict[str, Dict[str, Any]],
        by_sel: Dict[str, Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        """
        Multi-key lookup with priority:
          1. customer_ref  (strongest — user-assigned)
          2. bet_id        (exchange-assigned, survives replaces)
          3. market_id+selection_id  (structural — catches replaced orders)
        """
        cref = str(leg.get("customer_ref") or "").strip()
        if cref and cref in by_ref:
            return by_ref[cref]

        bid = str(leg.get("bet_id") or "").strip()
        if bid and bid in by_bet:
            return by_bet[bid]

        mid = str(leg.get("market_id") or "").strip()
        sid = str(leg.get("selection_id") or "").strip()
        if mid and sid:
            compound = f"{mid}::{sid}"
            if compound in by_sel:
                return by_sel[compound]

        return None

    # ─────────────────────────────────────────────────────────────
    # DECISION LOG – thread-safe, fail-closed
    # ─────────────────────────────────────────────────────────────

    def _log_decision(
        self,
        *,
        batch_id: str,
        leg_index: Optional[int],
        case_classification: str,
        reason_code: ReasonCode,
        local_status: str,
        exchange_status: Optional[str],
        resolved_status: str,
        merge_winner: str,
        details: Optional[Dict[str, Any]] = None,
        persist_immediate: bool = False,
    ) -> DecisionEntry:
        entry = DecisionEntry(
            timestamp=time.time(),
            batch_id=batch_id,
            leg_index=leg_index,
            case_classification=case_classification,
            reason_code=reason_code.value,
            local_status=local_status,
            exchange_status=exchange_status,
            resolved_status=resolved_status,
            merge_winner=merge_winner,
            details=details or {},
            persisted=False,
        )
        # append under lock (fast, no I/O)
        with self._decision_log_lock:
            self._decision_log.append(entry)

        # persist OUTSIDE lock (avoids DB I/O contention)
        persist_ok = True
        if persist_immediate:
            persist_ok = self._persist_decision_immediate(entry)
            if persist_ok:
                entry.persisted = True

        entry.persist_ok = persist_ok

        logger.info(
            "DECISION batch=%s leg=%s case=%s reason=%s winner=%s => %s",
            batch_id, leg_index, case_classification,
            reason_code.value, merge_winner, resolved_status,
        )
        return entry

    def _persist_decision_immediate(self, entry: DecisionEntry) -> bool:
        """
        Write a single critical decision to DB immediately.
        Returns True if persisted, False on failure.
        Used for state-changing decisions to guarantee audit trail.
        """
        persister = getattr(self.db, "persist_decision_log", None)
        if not callable(persister):
            logger.warning(
                "No persist_decision_log on DB — audit entry NOT persisted "
                "batch=%s leg=%s reason=%s",
                entry.batch_id, entry.leg_index, entry.reason_code,
            )
            return not self.cfg.audit_fail_closed

        try:
            persister(entry.batch_id, [entry.to_dict()])
            return True
        except Exception:
            logger.exception(
                "CRITICAL: Failed to persist audit entry batch=%s leg=%s",
                entry.batch_id, entry.leg_index,
            )
            return False

    def _flush_decision_log(self, batch_id: str) -> bool:
        """
        Persist all buffered decisions for *batch_id* to DB.
        Skips entries already marked persisted=True (written by persist_immediate).
        Returns True if all persisted, False on failure.
        """
        with self._decision_log_lock:
            pending = [
                e for e in self._decision_log
                if e.batch_id == batch_id and not e.persisted
            ]
            if not pending:
                self._decision_log = [
                    e for e in self._decision_log if e.batch_id != batch_id
                ]
                return True

        persister = getattr(self.db, "persist_decision_log", None)
        if not callable(persister):
            logger.error("persist_decision_log missing for batch=%s", batch_id)
            if self.cfg.audit_fail_closed:
                return False
            with self._decision_log_lock:
                self._decision_log = [
                    e for e in self._decision_log if e.batch_id != batch_id
                ]
            return True

        try:
            persister(batch_id, [e.to_dict() for e in pending])
            for e in pending:
                e.persisted = True
        except Exception:
            logger.exception("Errore persist_decision_log batch=%s", batch_id)
            if self.cfg.audit_fail_closed:
                return False
            with self._decision_log_lock:
                self._decision_log = [
                    e for e in self._decision_log if e.batch_id != batch_id
                ]
            return True

        with self._decision_log_lock:
            self._decision_log = [
                e for e in self._decision_log if e.batch_id != batch_id
            ]
        return True

    def get_decision_log(
        self, batch_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Return in-memory decision log, optionally filtered."""
        with self._decision_log_lock:
            entries = list(self._decision_log)
        if batch_id:
            entries = [e for e in entries if e.batch_id == batch_id]
        return [e.to_dict() for e in entries]

    # ─────────────────────────────────────────────────────────────
    # IDEMPOTENCY – deterministic, order-independent fingerprint
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def _compute_fingerprint(
        legs: List[Dict[str, Any]],
        remote_orders: List[Dict[str, Any]],
    ) -> str:
        """
        Deterministic hash of local legs + remote orders state.
        Canonical sorting guarantees same result regardless of input order.
        """
        canonical = json.dumps(
            {
                "legs": [
                    {
                        "idx": lg.get("leg_index"),
                        "st": str(lg.get("status") or "").upper(),
                        "ref": str(lg.get("customer_ref") or "").strip(),
                        "bid": str(lg.get("bet_id") or "").strip(),
                        "sid": str(lg.get("selection_id") or "").strip(),
                    }
                    for lg in sorted(
                        legs,
                        key=lambda x: (
                            int(x.get("leg_index", 0)),
                            str(x.get("customer_ref") or ""),
                        ),
                    )
                ],
                "remote": [
                    {
                        "ref": o.get("customerOrderRef", o.get("customer_ref", "")),
                        "bid": o.get("betId", o.get("bet_id", "")),
                        "st": str(o.get("status", "")).upper(),
                        "sm": str(o.get("sizeMatched", "")),
                        "sr": str(o.get("sizeRemaining", "")),
                        "sid": str(
                            o.get("selectionId", o.get("selection_id", ""))
                        ),
                    }
                    for o in sorted(
                        remote_orders,
                        key=lambda x: (
                            str(x.get("customerOrderRef", x.get("customer_ref", ""))),
                            str(x.get("betId", x.get("bet_id", ""))),
                        ),
                    )
                ],
            },
            sort_keys=True,
        )
        return hashlib.sha256(canonical.encode()).hexdigest()[:16]

    # ─────────────────────────────────────────────────────────────
    # RECOVERY MARKERS
    # ─────────────────────────────────────────────────────────────

    def _set_recovery_marker(self, batch_id: str) -> None:
        """Mark batch as 'reconcile in progress' with timestamp for TTL."""
        if not self.cfg.persist_recovery_marker:
            return
        setter = getattr(self.db, "set_reconcile_marker", None)
        if callable(setter):
            try:
                setter(batch_id, time.time())
            except Exception:
                logger.exception("Failed to set recovery marker batch=%s", batch_id)

    def _clear_recovery_marker(self, batch_id: str) -> None:
        if not self.cfg.persist_recovery_marker:
            return
        setter = getattr(self.db, "set_reconcile_marker", None)
        if callable(setter):
            try:
                setter(batch_id, False)
            except Exception:
                logger.exception("Failed to clear recovery marker batch=%s", batch_id)

    def _has_recovery_marker(self, batch_id: str) -> bool:
        """True if a recovery marker exists (any age). Used by lock logic."""
        getter = getattr(self.db, "get_reconcile_marker", None)
        if not callable(getter):
            return False
        try:
            value = getter(batch_id)
            return bool(value)
        except Exception:
            logger.exception("Failed to read recovery marker batch=%s", batch_id)
        return False

    def _is_recovery_marker_stale(self, batch_id: str) -> bool:
        """
        True if marker exists AND is older than TTL (crashed reconcile).
        False if no marker, or marker is fresh (active reconcile).
        """
        getter = getattr(self.db, "get_reconcile_marker", None)
        if not callable(getter):
            return False
        try:
            value = getter(batch_id)
            if not value:
                return False
            # timestamp-based TTL
            if isinstance(value, (int, float)) and value > 1:
                age = time.time() - float(value)
                if age > self.cfg.recovery_marker_ttl_secs:
                    logger.warning(
                        "Recovery marker for batch=%s is stale "
                        "(age=%.1fs > TTL=%.1fs)",
                        batch_id, age, self.cfg.recovery_marker_ttl_secs,
                    )
                    return True
                return False  # fresh → active reconcile, not stale
            # legacy bool marker without timestamp → treat as stale
            return True
        except Exception:
            logger.exception("Failed to read recovery marker batch=%s", batch_id)
        return False

    # ─────────────────────────────────────────────────────────────
    # CASE CLASSIFICATION
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def _classify_case(
        local_status: str,
        remote_order: Optional[Dict[str, Any]],
        remote_status: Optional[str],
        saga_pending: bool,
    ) -> str:
        has_remote = remote_order is not None
        local_is_terminal = local_status in TERMINAL_LEG_STATUSES
        local_is_inflight = local_status in NON_TERMINAL_LEG_STATUSES

        if local_is_inflight and not has_remote:
            return "LOCAL_INFLIGHT_EXCHANGE_ABSENT"

        if local_is_inflight and has_remote and remote_status == "MATCHED":
            return "LOCAL_AMBIGUOUS_EXCHANGE_MATCHED"

        if (not local_status or local_status == "ABSENT") and has_remote:
            return "LOCAL_ABSENT_EXCHANGE_PRESENT"

        if has_remote and local_is_inflight and remote_status != "MATCHED":
            return "SPLIT_STATE"

        if local_is_terminal and has_remote:
            return "SPLIT_STATE"

        return "UNCLASSIFIED"

    # ─────────────────────────────────────────────────────────────
    # MERGE POLICY (formalised)
    #
    #   1. Exchange ALWAYS wins on definitive terminal status.
    #   2. Exchange wins on PARTIAL (dominant non-terminal).
    #   3. Local wins when saga is still pending.
    #   4. Local wins when already terminal and exchange agrees.
    #   5. UNKNOWN resolved after grace period ONLY if created_at_ts
    #      is present; missing timestamp → safe hold (not instant FAIL).
    #   6. Ghost orders flagged/cancelled per config.
    # ─────────────────────────────────────────────────────────────

    def _apply_merge_policy(
        self,
        *,
        batch_id: str,
        leg: Dict[str, Any],
        remote_order: Optional[Dict[str, Any]],
        saga_pending: bool,
    ) -> Tuple[Optional[str], ReasonCode, str]:
        """
        Returns (new_status | None, reason_code, merge_winner).
        None means "no change".
        """
        leg_index = int(leg.get("leg_index", -1))
        local_status = str(leg.get("status") or "").upper()

        remote_status: Optional[str] = None
        if remote_order:
            remote_status = self._map_remote_status_to_leg_status(remote_order)

        classification = self._classify_case(
            local_status, remote_order, remote_status, saga_pending
        )

        # ── CASE 1: local inflight, exchange absent ─────────────
        if classification == "LOCAL_INFLIGHT_EXCHANGE_ABSENT":
            if saga_pending:
                self._log_decision(
                    batch_id=batch_id, leg_index=leg_index,
                    case_classification=classification,
                    reason_code=ReasonCode.LOCAL_WINS_SAGA_PENDING,
                    local_status=local_status, exchange_status=None,
                    resolved_status=local_status, merge_winner="LOCAL",
                )
                return None, ReasonCode.LOCAL_WINS_SAGA_PENDING, "LOCAL"

            # deterministic UNKNOWN resolution — safe timestamp check
            created_ts = float(leg.get("created_at_ts", 0) or 0)

            # missing timestamp → do NOT auto-fail, hold safe
            if created_ts <= 0:
                self._log_decision(
                    batch_id=batch_id, leg_index=leg_index,
                    case_classification=classification,
                    reason_code=ReasonCode.LOCAL_WINS_SAGA_PENDING,
                    local_status=local_status, exchange_status=None,
                    resolved_status=local_status, merge_winner="LOCAL",
                    details={"reason": "missing_created_at_ts"},
                )
                return None, ReasonCode.LOCAL_WINS_SAGA_PENDING, "LOCAL"

            age = time.time() - created_ts

            if local_status == "UNKNOWN" or age > self.cfg.unknown_grace_secs:
                self._log_decision(
                    batch_id=batch_id, leg_index=leg_index,
                    case_classification=classification,
                    reason_code=ReasonCode.RESOLVED_UNKNOWN_TO_FAILED,
                    local_status=local_status, exchange_status=None,
                    resolved_status="FAILED", merge_winner="NONE",
                    details={"age_secs": round(age, 2)},
                )
                return "FAILED", ReasonCode.RESOLVED_UNKNOWN_TO_FAILED, "NONE"

            # within grace period — hold
            return None, ReasonCode.LOCAL_WINS_SAGA_PENDING, "LOCAL"

        # ── CASE 2: local ambiguous, exchange matched ───────────
        if classification == "LOCAL_AMBIGUOUS_EXCHANGE_MATCHED":
            self._log_decision(
                batch_id=batch_id, leg_index=leg_index,
                case_classification=classification,
                reason_code=ReasonCode.EXCHANGE_WINS_MATCHED,
                local_status=local_status, exchange_status=remote_status,
                resolved_status="MATCHED", merge_winner="EXCHANGE",
            )
            return "MATCHED", ReasonCode.EXCHANGE_WINS_MATCHED, "EXCHANGE"

        # ── CASE 3: ghost (local absent, exchange present) ──────
        if classification == "LOCAL_ABSENT_EXCHANGE_PRESENT":
            self._log_decision(
                batch_id=batch_id, leg_index=leg_index,
                case_classification=classification,
                reason_code=ReasonCode.GHOST_ORDER_DETECTED,
                local_status=local_status, exchange_status=remote_status,
                resolved_status="GHOST", merge_winner="EXCHANGE",
                details={
                    "action": self.cfg.ghost_order_action,
                    "bet_id": self._extract_bet_id(remote_order) if remote_order else "",
                },
            )
            return None, ReasonCode.GHOST_ORDER_DETECTED, "EXCHANGE"

        # ── CASE 4: split state ─────────────────────────────────
        if classification == "SPLIT_STATE" and remote_order and remote_status:
            reason_map = {
                "MATCHED":   ReasonCode.EXCHANGE_WINS_MATCHED,
                "PARTIAL":   ReasonCode.EXCHANGE_WINS_PARTIAL,
                "CANCELLED": ReasonCode.EXCHANGE_WINS_CANCELLED,
                "LAPSED":    ReasonCode.EXCHANGE_WINS_LAPSED,
                "VOIDED":    ReasonCode.EXCHANGE_WINS_CANCELLED,
                "FAILED":    ReasonCode.EXCHANGE_WINS_CANCELLED,
            }
            reason = reason_map.get(remote_status, ReasonCode.CONVERGED)

            # exchange terminal → exchange wins unconditionally
            if remote_status in TERMINAL_LEG_STATUSES:
                if local_status == remote_status:
                    return None, ReasonCode.IDEMPOTENT_SKIP, "NONE"
                self._log_decision(
                    batch_id=batch_id, leg_index=leg_index,
                    case_classification=classification,
                    reason_code=reason,
                    local_status=local_status, exchange_status=remote_status,
                    resolved_status=remote_status, merge_winner="EXCHANGE",
                )
                return remote_status, reason, "EXCHANGE"

            # PARTIAL is dominant non-terminal — exchange always wins
            if remote_status == "PARTIAL":
                if local_status == "PARTIAL":
                    return None, ReasonCode.IDEMPOTENT_SKIP, "NONE"
                self._log_decision(
                    batch_id=batch_id, leg_index=leg_index,
                    case_classification=classification,
                    reason_code=ReasonCode.EXCHANGE_WINS_PARTIAL,
                    local_status=local_status, exchange_status=remote_status,
                    resolved_status="PARTIAL", merge_winner="EXCHANGE",
                )
                return "PARTIAL", ReasonCode.EXCHANGE_WINS_PARTIAL, "EXCHANGE"

            # other non-terminal (PLACED, UNKNOWN)
            if remote_status != local_status:
                self._log_decision(
                    batch_id=batch_id, leg_index=leg_index,
                    case_classification=classification,
                    reason_code=reason,
                    local_status=local_status, exchange_status=remote_status,
                    resolved_status=remote_status, merge_winner="EXCHANGE",
                )
                return remote_status, reason, "EXCHANGE"

        # ── Already terminal locally ────────────────────────────
        if local_status in TERMINAL_LEG_STATUSES:
            return None, ReasonCode.ALREADY_TERMINAL, "LOCAL"

        return None, ReasonCode.CONVERGED, "NONE"

    # ─────────────────────────────────────────────────────────────
    # GHOST ORDER DETECTION – robust multi-key
    # ─────────────────────────────────────────────────────────────

    def _detect_ghost_orders(
        self,
        batch_id: str,
        legs: List[Dict[str, Any]],
        remote_orders: List[Dict[str, Any]],
        by_ref: Dict[str, Dict[str, Any]],
        by_bet: Dict[str, Dict[str, Any]],
        by_sel: Dict[str, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Ghost = order on exchange with NO matching local leg.

        Multi-key matching to avoid false positives:
          1. customer_ref
          2. bet_id
          3. market_id + selection_id (catches replace scenarios)
        """
        local_refs: Set[str] = set()
        local_bets: Set[str] = set()
        local_sels: Set[str] = set()
        for lg in legs:
            ref = str(lg.get("customer_ref") or "").strip()
            bid = str(lg.get("bet_id") or "").strip()
            mid = str(lg.get("market_id") or "").strip()
            sid = str(lg.get("selection_id") or "").strip()
            if ref:
                local_refs.add(ref)
            if bid:
                local_bets.add(bid)
            if mid and sid:
                local_sels.add(f"{mid}::{sid}")

        ghosts: List[Dict[str, Any]] = []
        for order in remote_orders:
            ref = self._extract_customer_ref(order)
            bid = self._extract_bet_id(order)
            mid = self._extract_market_id(order)
            sid = self._extract_selection_id(order)
            compound = f"{mid}::{sid}" if mid and sid else ""

            # multi-key match: any hit → NOT a ghost
            if ref and ref in local_refs:
                continue
            if bid and bid in local_bets:
                continue
            if compound and compound in local_sels:
                # structural match → replaced order, not a true ghost
                logger.info(
                    "Exchange order matched by selection compound key "
                    "batch=%s market=%s selection=%s bet_id=%s — "
                    "treating as replaced, not ghost",
                    batch_id, mid, sid, bid,
                )
                self._log_decision(
                    batch_id=batch_id, leg_index=None,
                    case_classification="LOCAL_ABSENT_EXCHANGE_PRESENT",
                    reason_code=ReasonCode.GHOST_REPLACED_ORDER,
                    local_status="ABSENT",
                    exchange_status=self._extract_order_status(order),
                    resolved_status="REPLACED",
                    merge_winner="EXCHANGE",
                    details={
                        "bet_id": bid, "selection_id": sid,
                        "market_id": mid, "customer_ref": ref,
                    },
                )
                continue

            # true ghost
            ghost_info = {
                "batch_id": batch_id,
                "customer_ref": ref,
                "bet_id": bid,
                "exchange_status": self._extract_order_status(order),
                "selection_id": sid,
                "market_id": mid,
                "size_matched": order.get("sizeMatched"),
                "size_remaining": order.get("sizeRemaining"),
            }
            ghosts.append(ghost_info)

            self._log_decision(
                batch_id=batch_id, leg_index=None,
                case_classification="LOCAL_ABSENT_EXCHANGE_PRESENT",
                reason_code=ReasonCode.GHOST_ORDER_DETECTED,
                local_status="ABSENT",
                exchange_status=self._extract_order_status(order),
                resolved_status="GHOST", merge_winner="EXCHANGE",
                details=ghost_info,
            )

        if ghosts:
            logger.warning(
                "Ghost orders detected batch=%s count=%d action=%s",
                batch_id, len(ghosts), self.cfg.ghost_order_action,
            )
            self._publish("RECONCILIATION_GHOST_ORDERS", {
                "batch_id": batch_id,
                "ghosts": ghosts,
                "action": self.cfg.ghost_order_action,
            })
            if self.cfg.ghost_order_action == "CANCEL":
                self._cancel_ghost_orders(ghosts)

        return ghosts

    def _cancel_ghost_orders(self, ghosts: List[Dict[str, Any]]) -> None:
        client = self._get_client()
        if not client:
            return
        for g in ghosts:
            bid = g.get("bet_id", "")
            if not bid:
                continue
            try:
                if hasattr(client, "cancel_order"):
                    client.cancel_order(bet_id=bid)
                    logger.info("Cancelled ghost order bet_id=%s", bid)
            except Exception:
                logger.exception("Failed to cancel ghost bet_id=%s", bid)

    # ─────────────────────────────────────────────────────────────
    # CONVERGENCE ALGORITHM — strong deterministic
    # ─────────────────────────────────────────────────────────────

    def reconcile_batch(self, batch_id: str) -> Dict[str, Any]:
        """
        Explicit convergence loop with:
          - Per-batch lock (reject if already running)
          - Recovery marker (cross-process consistency)
          - Re-fetch + re-evaluate until fingerprint-stable AND no changes
          - Fail-closed audit persistence
          - Snapshot reload before every critical decision
        """
        with self._lock_mgr.acquire(batch_id) as acquired:
            if not acquired:
                logger.warning(
                    "Reconcile already running for batch=%s, skipping",
                    batch_id,
                )
                return self._result(
                    False, batch_id,
                    reason_code=ReasonCode.RECONCILE_ALREADY_RUNNING,
                )
            return self._reconcile_batch_locked(batch_id)

    def _reconcile_batch_locked(self, batch_id: str) -> Dict[str, Any]:
        """Core reconcile logic, called only under batch lock."""
        self._set_recovery_marker(batch_id)
        try:
            return self._reconcile_batch_inner(batch_id)
        finally:
            self._clear_recovery_marker(batch_id)

    def _reconcile_batch_inner(self, batch_id: str) -> Dict[str, Any]:
        # ── fresh snapshot (never use stale data after restart) ──
        batch = self.batch_manager.get_batch(batch_id)
        if not batch:
            return self._result(
                False, batch_id, reason_code=ReasonCode.BATCH_NOT_FOUND
            )

        if str(batch.get("status") or "").upper() in TERMINAL_BATCH_STATUSES:
            return self._result(
                True, batch_id,
                status=str(batch.get("status") or ""),
                reason_code=ReasonCode.ALREADY_TERMINAL,
            )

        legs = self.batch_manager.get_batch_legs(batch_id)
        if not legs:
            self.batch_manager.mark_batch_failed(
                batch_id, reason="Batch senza legs"
            )
            self._release(batch_id)
            return self._result(
                True, batch_id, status="FAILED",
                reason_code=ReasonCode.NO_LEGS,
            )

        # ── convergence loop ────────────────────────────────────
        market_id = str(batch.get("market_id") or "")
        prev_fingerprint: Optional[str] = None
        last_remote_orders: List[Dict[str, Any]] = []
        last_cycle = 0
        fetch_failure: Optional[ReasonCode] = None

        for cycle in range(1, self.cfg.max_convergence_cycles + 1):
            last_cycle = cycle

            # ── re-fetch exchange state ─────────────────────────
            if market_id:
                remote_orders, fetch_err = (
                    self._fetch_current_orders_by_market(market_id)
                )
                if fetch_err is not None:
                    fetch_failure = fetch_err
                    self._log_decision(
                        batch_id=batch_id, leg_index=None,
                        case_classification="FETCH_ERROR",
                        reason_code=fetch_err,
                        local_status="", exchange_status=None,
                        resolved_status="", merge_winner="NONE",
                        details={"market_id": market_id, "cycle": cycle},
                    )
                    if fetch_err in (
                        ReasonCode.FETCH_PERMANENT_FAILURE,
                        ReasonCode.AUTH_ERROR,
                    ):
                        break
                    break  # transient exhausted
            else:
                remote_orders = []

            last_remote_orders = remote_orders

            # ── deterministic fingerprint (canonical sort) ──────
            legs_sorted = sorted(
                legs, key=lambda x: int(x.get("leg_index", 0))
            )
            fp = self._compute_fingerprint(legs_sorted, remote_orders)

            # first-cycle idempotency against last full reconcile
            if (
                cycle == 1
                and fp == self._reconcile_fingerprints.get(batch_id)
            ):
                self._log_decision(
                    batch_id=batch_id, leg_index=None,
                    case_classification="IDEMPOTENT",
                    reason_code=ReasonCode.IDEMPOTENT_SKIP,
                    local_status="", exchange_status=None,
                    resolved_status="", merge_winner="NONE",
                )
                return self._result(
                    True, batch_id,
                    status=str(batch.get("status") or ""),
                    reason_code=ReasonCode.IDEMPOTENT_SKIP,
                )

            # snapshot prev BEFORE overwrite for post-merge comparison
            snapshot_prev_fp = prev_fingerprint

            pending_saga_refs = self._get_pending_saga_refs()
            by_ref, by_bet, by_sel = self._build_exchange_indices(
                remote_orders
            )

            # ghost detection
            self._detect_ghost_orders(
                batch_id, legs, remote_orders, by_ref, by_bet, by_sel
            )

            # ── per-leg merge (deterministic order) ─────────────
            changed = False
            for leg in legs_sorted:
                leg_index = int(leg.get("leg_index", -1))
                current_status = str(leg.get("status") or "").upper()

                if current_status in TERMINAL_LEG_STATUSES:
                    continue

                remote_order = self._lookup_remote_order(
                    leg, by_ref, by_bet, by_sel
                )
                cref = str(leg.get("customer_ref") or "").strip()
                saga_pending = bool(cref and cref in pending_saga_refs)

                new_status, reason, winner = self._apply_merge_policy(
                    batch_id=batch_id,
                    leg=leg,
                    remote_order=remote_order,
                    saga_pending=saga_pending,
                )

                if new_status and new_status != current_status:
                    # audit-first: persist decision atomically with log
                    decision = self._log_decision(
                        batch_id=batch_id, leg_index=leg_index,
                        case_classification="STATE_CHANGE",
                        reason_code=reason,
                        local_status=current_status,
                        exchange_status=(
                            self._map_remote_status_to_leg_status(remote_order)
                            if remote_order else None
                        ),
                        resolved_status=new_status,
                        merge_winner=winner,
                        persist_immediate=self.cfg.audit_fail_closed,
                    )

                    if self.cfg.audit_fail_closed:
                        if not decision.persist_ok:
                            logger.error(
                                "ABORT reconcile: audit persist failed "
                                "batch=%s leg=%d — refusing state change",
                                batch_id, leg_index,
                            )
                            return self._result(
                                False, batch_id,
                                reason_code=ReasonCode.AUDIT_PERSIST_FAILED,
                            )

                    bet_id = (
                        self._extract_bet_id(remote_order)
                        if remote_order
                        else str(leg.get("bet_id") or "")
                    )
                    error_text = (
                        reason.value if new_status == "FAILED" else ""
                    )

                    # FSM-validated + transactional update (Points 1, 2, 6)
                    update_ok = self._transactional_leg_update(
                        batch_id=batch_id,
                        leg_index=leg_index,
                        from_status=current_status,
                        to_status=new_status,
                        decision=decision,
                        bet_id=bet_id or None,
                        raw_response=remote_order,
                        error_text=error_text or None,
                    )
                    if not update_ok:
                        logger.error(
                            "Transactional leg update failed batch=%s leg=%d",
                            batch_id, leg_index,
                        )
                        if self.cfg.audit_fail_closed:
                            return self._result(
                                False, batch_id,
                                reason_code=ReasonCode.AUDIT_PERSIST_FAILED,
                            )

                    leg["status"] = new_status
                    changed = True

            # convergence: no changes AND fingerprint same as PREVIOUS cycle
            if not changed and snapshot_prev_fp is not None and fp == snapshot_prev_fp:
                logger.info(
                    "Converged: no changes + stable fingerprint cycle=%d batch=%s",
                    cycle, batch_id,
                )
                break

            # update prev_fingerprint AFTER the convergence check
            prev_fingerprint = fp

            if not changed:
                logger.info(
                    "No local changes but fp changed cycle=%d batch=%s",
                    cycle, batch_id,
                )

            if cycle < self.cfg.max_convergence_cycles:
                # only sleep + reload when state changed (need exchange to settle)
                if changed:
                    time.sleep(self.cfg.convergence_sleep_secs)
                    legs = self.batch_manager.get_batch_legs(batch_id)
                    if not legs:
                        break
        else:
            self._log_decision(
                batch_id=batch_id, leg_index=None,
                case_classification="CONVERGENCE",
                reason_code=ReasonCode.MAX_CYCLES_EXCEEDED,
                local_status="", exchange_status=None,
                resolved_status="", merge_winner="NONE",
                details={"max_cycles": self.cfg.max_convergence_cycles},
            )

        # ── recompute batch status ──────────────────────────────
        new_batch = self.batch_manager.recompute_batch_status(batch_id)
        status = str((new_batch or {}).get("status") or "")

        if status.upper() in TERMINAL_BATCH_STATUSES:
            self._release(batch_id)
            self._lock_mgr.cleanup_batch(batch_id)

        # final legs: use in-memory snapshot (current under batch lock).
        # Only reload from DB if legs became empty (edge case: loop broke on empty).
        if legs:
            final_legs = legs
        else:
            final_legs = self.batch_manager.get_batch_legs(batch_id) or []

        # ── runtime invariant checks (Point 8) ──────────────────
        violations = self._check_post_reconcile_invariants_on_legs(
            batch_id, final_legs, batch_status=status,
        )
        self._invoke_hook("after_recompute", batch_id=batch_id, status=status)

        # persist final fingerprint + flush remaining decisions
        self._reconcile_fingerprints[batch_id] = self._compute_fingerprint(
            sorted(final_legs, key=lambda x: int(x.get("leg_index", 0))),
            last_remote_orders,
        )

        flush_ok = self._flush_decision_log(batch_id)
        if not flush_ok and self.cfg.audit_fail_closed:
            result = self._result(
                False,
                batch_id,
                status=status,
                reason_code=ReasonCode.AUDIT_PERSIST_FAILED,
                extra={
                    "cycles": last_cycle,
                    "fingerprint": self._reconcile_fingerprints.get(batch_id, ""),
                },
            )
            self._publish("RECONCILIATION_BATCH_DONE", result)
            return result

        # ── drain outbox events (Point 6) ────────────────────────
        self._drain_outbox(batch_id)

        # if fetch failed permanently, return failure — not CONVERGED
        if fetch_failure in (
            ReasonCode.FETCH_PERMANENT_FAILURE,
            ReasonCode.AUTH_ERROR,
            ReasonCode.TRANSIENT_ERROR,
        ):
            result = self._result(
                False, batch_id,
                status=status,
                reason_code=fetch_failure,
                extra={
                    "cycles": last_cycle,
                    "fingerprint": self._reconcile_fingerprints.get(batch_id, ""),
                    "invariant_violations": violations,
                },
            )
            self._publish("RECONCILIATION_BATCH_DONE", result)
            return result

        result = self._result(
            True, batch_id,
            status=status,
            reason_code=ReasonCode.CONVERGED,
            extra={
                "cycles": last_cycle,
                "fingerprint": self._reconcile_fingerprints.get(batch_id, ""),
                "invariant_violations": violations,
            },
        )
        self._publish("RECONCILIATION_BATCH_DONE", result)
        return result

    # ─────────────────────────────────────────────────────────────
    # RECONCILE ALL OPEN BATCHES
    # ─────────────────────────────────────────────────────────────

    def reconcile_all_open_batches(self) -> Dict[str, Any]:
        batches = self.batch_manager.get_open_batches()
        if len(batches) > self.cfg.max_batches_per_run:
            logger.warning(
                "Capping reconciliation to %d batches (found %d)",
                self.cfg.max_batches_per_run, len(batches),
            )
            batches = batches[: self.cfg.max_batches_per_run]

        reconciled: List[Dict[str, Any]] = []
        failed: List[Dict[str, Any]] = []

        for batch in batches:
            batch_id = str(batch.get("batch_id") or "")

            # stale recovery marker → previous reconcile crashed
            if self._is_recovery_marker_stale(batch_id):
                logger.warning(
                    "Stale recovery marker found for batch=%s — "
                    "previous reconcile crashed. Clearing.",
                    batch_id,
                )
                self._clear_recovery_marker(batch_id)

            try:
                result = self.reconcile_batch(batch_id)
                reconciled.append(result)
            except Exception as exc:
                logger.exception("Errore reconcile batch_id=%s", batch_id)
                self._log_decision(
                    batch_id=batch_id, leg_index=None,
                    case_classification="ERROR",
                    reason_code=ReasonCode.TRANSIENT_ERROR,
                    local_status="", exchange_status=None,
                    resolved_status="ERROR", merge_winner="NONE",
                    details={"error": str(exc)},
                )
                self._flush_decision_log(batch_id)
                failed.append({"batch_id": batch_id, "error": str(exc)})

        summary = {
            "ok": len(failed) == 0,
            "reconciled_count": len(reconciled),
            "failed_count": len(failed),
            "reconciled": reconciled,
            "failed": failed,
        }
        self._publish("RECONCILIATION_ALL_DONE", summary)
        return summary

    def fetch_startup_active_orders(self) -> List[Dict[str, Any]]:
        """
        Startup hook: snapshot ordini attivi remoti PRIMA del normale intake live.
        Nessuna chiamata reale in test: i test iniettano un service fake.
        """
        fn = getattr(self.betfair_service, "list_active_orders", None)
        if callable(fn):
            orders = fn() or []
            return [o for o in orders if isinstance(o, dict)]

        fn = getattr(self.betfair_service, "list_current_orders", None)
        if callable(fn):
            orders = fn() or []
            return [o for o in orders if isinstance(o, dict)]

        return []

    def merge_startup_active_orders(self, remote_orders: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Normalizza e preserva il payload degli ordini remoti per recovery startup.
        """
        normalized: List[Dict[str, Any]] = []
        for order in list(remote_orders or []):
            if not isinstance(order, dict):
                continue
            normalized.append(dict(order))
        return {"orders": normalized, "count": len(normalized)}

    # ─────────────────────────────────────────────────────────────
    # POLICY HELPERS
    # ─────────────────────────────────────────────────────────────

    def mark_partial_as_rollback_pending(
        self, batch_id: str, reason: str = ""
    ) -> Dict[str, Any]:
        batch = self.batch_manager.get_batch(batch_id)
        if not batch:
            return self._result(
                False, batch_id, reason_code=ReasonCode.BATCH_NOT_FOUND
            )

        if str(batch.get("status") or "") not in {"PARTIAL", "LIVE"}:
            return self._result(
                False, batch_id,
                reason_code=ReasonCode.ALREADY_TERMINAL,
                extra={"current_status": batch.get("status")},
            )

        self.batch_manager.mark_batch_rollback_pending(
            batch_id, reason=reason or "Rollback richiesto"
        )
        self._log_decision(
            batch_id=batch_id, leg_index=None,
            case_classification="POLICY",
            reason_code=ReasonCode.ROLLBACK_REQUESTED,
            local_status=str(batch.get("status") or ""),
            exchange_status=None,
            resolved_status="ROLLBACK_PENDING",
            merge_winner="LOCAL",
        )
        self._flush_decision_log(batch_id)

        result = self._result(
            True, batch_id, status="ROLLBACK_PENDING",
            reason_code=ReasonCode.ROLLBACK_REQUESTED,
        )
        self._publish("RECONCILIATION_ROLLBACK_PENDING", result)
        return result

    def finalize_terminal_batch(
        self,
        batch_id: str,
        *,
        status: str,
        reason: str = "",
        pnl: float = 0.0,
    ) -> Dict[str, Any]:
        batch = self.batch_manager.get_batch(batch_id)
        if not batch:
            return self._result(
                False, batch_id, reason_code=ReasonCode.BATCH_NOT_FOUND
            )

        status = str(status or "").upper()
        if status not in TERMINAL_BATCH_STATUSES:
            return self._result(
                False, batch_id,
                reason_code=ReasonCode.ALREADY_TERMINAL,
                extra={"invalid_status": status},
            )

        self.batch_manager.update_batch_status(batch_id, status, notes=reason)
        self._release(batch_id, pnl=pnl)

        self._log_decision(
            batch_id=batch_id, leg_index=None,
            case_classification="FINALIZE",
            reason_code=ReasonCode.TERMINAL_FINALIZED,
            local_status=str(batch.get("status") or ""),
            exchange_status=None,
            resolved_status=status,
            merge_winner="LOCAL",
            details={"pnl": pnl, "reason": reason},
        )
        self._flush_decision_log(batch_id)

        result = self._result(
            True, batch_id, status=status,
            reason_code=ReasonCode.TERMINAL_FINALIZED,
            extra={"pnl": pnl, "reason": reason},
        )
        self._publish("RECONCILIATION_TERMINALIZED", result)
        return result

    # ─────────────────────────────────────────────────────────────
    # QUERY / LOOKUP API
    # ─────────────────────────────────────────────────────────────

    def lookup_by_customer_ref(
        self, customer_ref: str
    ) -> List[Dict[str, Any]]:
        finder = getattr(self.db, "find_legs_by_customer_ref", None)
        if callable(finder):
            try:
                return list(finder(customer_ref) or [])
            except Exception:
                logger.exception(
                    "lookup_by_customer_ref failed ref=%s", customer_ref
                )
        return []

    def lookup_by_bet_id(self, bet_id: str) -> List[Dict[str, Any]]:
        finder = getattr(self.db, "find_legs_by_bet_id", None)
        if callable(finder):
            try:
                return list(finder(bet_id) or [])
            except Exception:
                logger.exception("lookup_by_bet_id failed bid=%s", bet_id)
        return []

    def lookup_by_market_id(self, market_id: str) -> List[Dict[str, Any]]:
        finder = getattr(self.db, "find_batches_by_market_id", None)
        if callable(finder):
            try:
                return list(finder(market_id) or [])
            except Exception:
                logger.exception(
                    "lookup_by_market_id failed mid=%s", market_id
                )
        return []

    # ─────────────────────────────────────────────────────────────
    # INTERNAL UTILITIES
    # ─────────────────────────────────────────────────────────────

    def _release(self, batch_id: str, *, pnl: float = 0.0) -> None:
        self.batch_manager.release_runtime_artifacts(
            batch_id=batch_id,
            duplication_guard=self.duplication_guard,
            table_manager=self.table_manager,
            pnl=float(pnl or 0.0),
        )

    @staticmethod
    def _result(
        ok: bool,
        batch_id: str,
        *,
        status: str = "",
        reason_code: Optional[ReasonCode] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        r: Dict[str, Any] = {"ok": ok, "batch_id": batch_id}
        if status:
            r["status"] = status
        if reason_code:
            r["reason_code"] = reason_code.value
        if not ok and reason_code:
            r["error"] = reason_code.value
        if extra:
            r.update(extra)

        # structured result (Point 9) — available via result["_structured"]
        structured = ReconcileResult(
            ok=ok,
            batch_id=batch_id,
            reason_code=reason_code.value if reason_code else "",
            status=status,
            cycles=int(r.get("cycles", 0)),
            fingerprint=str(r.get("fingerprint", "")),
            converged=(reason_code == ReasonCode.CONVERGED) if reason_code else False,
            fetch_ok=r.get("fetch_failure") is None,
            fetch_failure=str(r.get("fetch_failure", "")) or None,
            audit_ok=reason_code != ReasonCode.AUDIT_PERSIST_FAILED if reason_code else True,
            audit_failure=(
                ReasonCode.AUDIT_PERSIST_FAILED.value
                if reason_code == ReasonCode.AUDIT_PERSIST_FAILED else None
            ),
            error=str(r.get("error", "")),
        )
        r["_structured"] = structured

        return r
