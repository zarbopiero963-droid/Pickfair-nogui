from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Callable


class TelegramAutohealAction(str, Enum):
    NO_ACTION = "NO_ACTION"
    SCHEDULE_RESTART = "SCHEDULE_RESTART"
    SUPPRESS_RESTART = "SUPPRESS_RESTART"
    ENTER_FAILED_LOCKOUT = "ENTER_FAILED_LOCKOUT"


class TelegramFailureClass(str, Enum):
    RECOVERABLE_DISCONNECT = "recoverable_disconnect"
    STALE_RUNTIME = "stale_runtime"
    RETRY_EXHAUSTION = "retry_exhaustion"
    INVARIANT_VIOLATION_RECOVERABLE = "invariant_violation_recoverable"
    INVARIANT_VIOLATION_NONRECOVERABLE = "invariant_violation_nonrecoverable"
    INTENTIONAL_STOP = "intentional_stop"
    RESTART_BUDGET_EXHAUSTED = "restart_budget_exhausted"
    HEALTHY = "healthy"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class TelegramAutohealSnapshot:
    state: str
    invariant_ok: bool
    active_alert_codes: tuple[str, ...]
    reconnect_attempts: int
    restart_attempts_total: int
    restart_in_progress: bool
    intentional_stop: bool
    startup_grace_active: bool
    reconnect_grace_active: bool
    lockout_active: bool
    last_error_category: str
    failure_escalated: bool
    listener_stale: bool
    now_ts: float


@dataclass(frozen=True)
class TelegramAutohealHistory:
    restart_timestamps: tuple[float, ...]
    lockout_since_ts: float | None


@dataclass(frozen=True)
class TelegramAutohealDecision:
    action: TelegramAutohealAction
    reason: str
    failure_class: TelegramFailureClass
    recovery_allowed: bool


class TelegramAutohealPolicy:
    def __init__(
        self,
        *,
        max_restarts_in_window: int = 3,
        restart_window_sec: float = 300.0,
        restart_cooldown_sec: float = 20.0,
        lockout_sec: float = 300.0,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.max_restarts_in_window = int(max_restarts_in_window)
        self.restart_window_sec = float(restart_window_sec)
        self.restart_cooldown_sec = float(restart_cooldown_sec)
        self.lockout_sec = float(lockout_sec)
        self._clock = clock

    def evaluate(
        self,
        snapshot: TelegramAutohealSnapshot,
        history: TelegramAutohealHistory,
    ) -> TelegramAutohealDecision:
        if snapshot.intentional_stop and snapshot.state == "STOPPED":
            return TelegramAutohealDecision(
                action=TelegramAutohealAction.NO_ACTION,
                reason="intentional_stop",
                failure_class=TelegramFailureClass.INTENTIONAL_STOP,
                recovery_allowed=False,
            )

        if snapshot.lockout_active and self._lockout_is_active(snapshot.now_ts, history.lockout_since_ts):
            return TelegramAutohealDecision(
                action=TelegramAutohealAction.SUPPRESS_RESTART,
                reason="lockout_active",
                failure_class=TelegramFailureClass.RESTART_BUDGET_EXHAUSTED,
                recovery_allowed=False,
            )

        if snapshot.restart_in_progress:
            return TelegramAutohealDecision(
                action=TelegramAutohealAction.SUPPRESS_RESTART,
                reason="restart_already_in_progress",
                failure_class=TelegramFailureClass.UNKNOWN,
                recovery_allowed=False,
            )

        if snapshot.startup_grace_active or snapshot.reconnect_grace_active:
            return TelegramAutohealDecision(
                action=TelegramAutohealAction.SUPPRESS_RESTART,
                reason="grace_period_active",
                failure_class=TelegramFailureClass.UNKNOWN,
                recovery_allowed=False,
            )

        failure_class = self._classify_failure(snapshot)
        if failure_class == TelegramFailureClass.HEALTHY:
            return TelegramAutohealDecision(
                action=TelegramAutohealAction.NO_ACTION,
                reason="healthy_runtime",
                failure_class=failure_class,
                recovery_allowed=True,
            )

        if failure_class in {
            TelegramFailureClass.INTENTIONAL_STOP,
            TelegramFailureClass.INVARIANT_VIOLATION_NONRECOVERABLE,
        }:
            return TelegramAutohealDecision(
                action=TelegramAutohealAction.NO_ACTION,
                reason=f"nonrecoverable_{failure_class.value}",
                failure_class=failure_class,
                recovery_allowed=False,
            )

        in_window = self._restart_attempts_in_window(snapshot.now_ts, history.restart_timestamps)
        if in_window >= self.max_restarts_in_window:
            return TelegramAutohealDecision(
                action=TelegramAutohealAction.ENTER_FAILED_LOCKOUT,
                reason="restart_budget_exhausted",
                failure_class=TelegramFailureClass.RESTART_BUDGET_EXHAUSTED,
                recovery_allowed=False,
            )

        if in_window > 0:
            latest = max(history.restart_timestamps)
            if (snapshot.now_ts - latest) < self.restart_cooldown_sec:
                return TelegramAutohealDecision(
                    action=TelegramAutohealAction.SUPPRESS_RESTART,
                    reason="restart_cooldown_active",
                    failure_class=failure_class,
                    recovery_allowed=False,
                )

        return TelegramAutohealDecision(
            action=TelegramAutohealAction.SCHEDULE_RESTART,
            reason=f"recoverable_{failure_class.value}",
            failure_class=failure_class,
            recovery_allowed=True,
        )

    def _classify_failure(self, snapshot: TelegramAutohealSnapshot) -> TelegramFailureClass:
        codes = set(snapshot.active_alert_codes)

        if snapshot.intentional_stop:
            return TelegramFailureClass.INTENTIONAL_STOP

        if not snapshot.invariant_ok:
            nonrecoverable = {
                "CONNECTED_REQUIRES_SINGLE_HANDLER",
                "DUPLICATE_HANDLER_REGISTRATION",
                "INVALID_STATE",
            }
            if codes.intersection(nonrecoverable):
                return TelegramFailureClass.INVARIANT_VIOLATION_NONRECOVERABLE
            return TelegramFailureClass.INVARIANT_VIOLATION_RECOVERABLE

        if snapshot.state == "FAILED":
            if snapshot.reconnect_attempts >= self.max_restarts_in_window:
                return TelegramFailureClass.RETRY_EXHAUSTION
            return TelegramFailureClass.RECOVERABLE_DISCONNECT

        if snapshot.listener_stale or "STALE_RUNTIME" in codes:
            return TelegramFailureClass.STALE_RUNTIME

        if snapshot.state in {"CREATED", "CONNECTING", "STOPPED", "RECONNECTING"}:
            return TelegramFailureClass.RECOVERABLE_DISCONNECT

        return TelegramFailureClass.HEALTHY

    def _restart_attempts_in_window(self, now_ts: float, restart_timestamps: tuple[float, ...]) -> int:
        return len([ts for ts in restart_timestamps if (now_ts - ts) <= self.restart_window_sec])

    def _lockout_is_active(self, now_ts: float, lockout_since_ts: float | None) -> bool:
        if lockout_since_ts is None:
            return True
        return (now_ts - lockout_since_ts) < self.lockout_sec

    def now(self) -> float:
        if self._clock is not None:
            return float(self._clock())
        import time

        return time.time()
