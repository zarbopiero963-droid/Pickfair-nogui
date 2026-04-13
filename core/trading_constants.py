"""
Pure string constants, set/dict literals, and the _ExecutionContext dataclass
extracted from core/trading_engine.py to reduce module size.

trading_engine.py imports everything from here; existing callers that import
from trading_engine are unaffected (no public API change).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Set, Tuple

# =========================================================
# COMMAND / REQUEST NAMES
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

# ── COPY META KEYS ──
COPY_META_KEYS: Set[str] = {
    "master_id", "master_position_id", "action_id", "action_seq",
    "copy_group_id", "copy_mode"
}

# ── PATTERN META KEYS ──
PATTERN_META_KEYS: Set[str] = {
    "pattern_id", "pattern_label", "selection_template", "market_type",
    "bet_side", "live_only", "event_context"
}

# ── PASSTHROUGH KEYS ──
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

# ── STATE MACHINE ──
ALLOWED_TRANSITIONS: Dict[str, Set[str]] = {
    STATUS_INFLIGHT: {STATUS_SUBMITTED, STATUS_FAILED, STATUS_AMBIGUOUS, STATUS_DENIED, STATUS_DUPLICATE_BLOCKED},
    STATUS_SUBMITTED: {STATUS_COMPLETED, STATUS_FAILED, STATUS_AMBIGUOUS},
    STATUS_AMBIGUOUS: {STATUS_COMPLETED, STATUS_FAILED},
    STATUS_DENIED: set(),
    STATUS_FAILED: set(),
    STATUS_COMPLETED: set(),
    STATUS_DUPLICATE_BLOCKED: set(),
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
