from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

_ALLOWED_STATES = {"CREATED", "CONNECTING", "CONNECTED", "RECONNECTING", "STOPPED", "FAILED"}
_EXPECTED_RUNTIME_HANDLER_COUNT = 2


@dataclass(frozen=True)
class TelegramInvariantSnapshot:
    state: str
    listener_started: bool
    client_alive: bool
    handlers_registered: int
    reconnect_in_progress: bool
    reconnect_attempts: int
    active_network_resources: int
    intentional_stop: bool
    retry_loop_active: bool
    running: bool
    last_error: str
    last_successful_message_ts: str | None
    now_ts: str | None = None
    stale_after_seconds: int = 300


@dataclass(frozen=True)
class TelegramInvariantViolation:
    code: str
    message: str


@dataclass(frozen=True)
class TelegramInvariantResult:
    ok: bool
    violations: tuple[TelegramInvariantViolation, ...]


class TelegramInvariantGuard:
    """Pure invariant evaluator for Telegram runtime state."""

    def evaluate(self, snapshot: TelegramInvariantSnapshot) -> TelegramInvariantResult:
        violations: list[TelegramInvariantViolation] = []

        if snapshot.state not in _ALLOWED_STATES:
            violations.append(
                TelegramInvariantViolation(
                    code="INVALID_STATE",
                    message=f"state must be one of {sorted(_ALLOWED_STATES)}",
                )
            )

        if snapshot.state == "CONNECTED" and not snapshot.client_alive:
            violations.append(
                TelegramInvariantViolation(
                    code="CONNECTED_REQUIRES_CLIENT_ALIVE",
                    message="CONNECTED requires client_alive=True",
                )
            )

        if snapshot.state == "CONNECTED" and snapshot.handlers_registered != _EXPECTED_RUNTIME_HANDLER_COUNT:
            violations.append(
                TelegramInvariantViolation(
                    code="CONNECTED_REQUIRES_CALLBACK_PAIR",
                    message="CONNECTED requires exactly two runtime callbacks (on_signal, on_status)",
                )
            )

        if snapshot.handlers_registered > _EXPECTED_RUNTIME_HANDLER_COUNT:
            violations.append(
                TelegramInvariantViolation(
                    code="DUPLICATE_HANDLER_REGISTRATION",
                    message="handlers_registered above the expected callback pair indicates duplicate registration",
                )
            )

        if snapshot.state == "RECONNECTING" and not snapshot.reconnect_in_progress:
            violations.append(
                TelegramInvariantViolation(
                    code="RECONNECTING_FLAG_MISSING",
                    message="RECONNECTING requires reconnect_in_progress=True",
                )
            )

        if snapshot.state == "STOPPED" and snapshot.active_network_resources != 0:
            violations.append(
                TelegramInvariantViolation(
                    code="STOPPED_WITH_ACTIVE_NETWORK_RESOURCES",
                    message="STOPPED requires active_network_resources=0",
                )
            )

        if snapshot.state == "FAILED" and snapshot.retry_loop_active:
            violations.append(
                TelegramInvariantViolation(
                    code="FAILED_RETRY_LOOP_ACTIVE",
                    message="FAILED requires retry_loop_active=False",
                )
            )

        if snapshot.listener_started and snapshot.state == "CREATED":
            violations.append(
                TelegramInvariantViolation(
                    code="LISTENER_STARTED_IN_CREATED",
                    message="listener_started=True cannot coexist with CREATED state",
                )
            )

        if snapshot.state == "CONNECTED" and snapshot.active_network_resources == 0:
            violations.append(
                TelegramInvariantViolation(
                    code="GHOST_CONNECTED_STATE",
                    message="CONNECTED with zero network resources is a ghost-connected state",
                )
            )

        if snapshot.intentional_stop and snapshot.state == "FAILED":
            violations.append(
                TelegramInvariantViolation(
                    code="INTENTIONAL_STOP_MARKED_FAILED",
                    message="intentional_stop must not be classified as FAILED",
                )
            )

        if snapshot.intentional_stop and snapshot.state == "STOPPED" and snapshot.client_alive:
            violations.append(
                TelegramInvariantViolation(
                    code="INTENTIONAL_STOP_GHOST_CONNECTED",
                    message="intentional STOPPED runtime must not report client_alive=True",
                )
            )

        if snapshot.state in {"STOPPED", "FAILED"} and snapshot.reconnect_in_progress:
            violations.append(
                TelegramInvariantViolation(
                    code="IMPOSSIBLE_RECONNECT_COMBINATION",
                    message="STOPPED/FAILED cannot have reconnect_in_progress=True",
                )
            )

        stale_violation = self._evaluate_stale_runtime(snapshot)
        if stale_violation is not None:
            violations.append(stale_violation)

        return TelegramInvariantResult(ok=not violations, violations=tuple(violations))

    def _evaluate_stale_runtime(self, snapshot: TelegramInvariantSnapshot) -> TelegramInvariantViolation | None:
        if snapshot.state not in {"CONNECTED", "RECONNECTING"}:
            return None
        if not snapshot.last_successful_message_ts or not snapshot.now_ts:
            return TelegramInvariantViolation(
                code="STALE_RUNTIME_NO_TIMESTAMP",
                message="operational states require last_successful_message_ts and now_ts",
            )

        last_ts = _parse_iso_ts(snapshot.last_successful_message_ts)
        now_ts = _parse_iso_ts(snapshot.now_ts)
        if last_ts is None or now_ts is None:
            return TelegramInvariantViolation(
                code="STALE_RUNTIME_BAD_TIMESTAMP",
                message="timestamps must be valid ISO-8601 values",
            )

        age_seconds = (now_ts - last_ts).total_seconds()
        if age_seconds > int(snapshot.stale_after_seconds):
            return TelegramInvariantViolation(
                code="STALE_RUNTIME",
                message=f"runtime stale: age_seconds={age_seconds:.1f} exceeds {snapshot.stale_after_seconds}",
            )
        return None


def _parse_iso_ts(value: str) -> datetime | None:
    try:
        normalized = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def evaluate(snapshot: TelegramInvariantSnapshot) -> TelegramInvariantResult:
    """Convenience functional wrapper for pure evaluation."""
    return TelegramInvariantGuard().evaluate(snapshot)


def violation_codes(result: TelegramInvariantResult) -> tuple[str, ...]:
    return tuple(v.code for v in result.violations)


def has_violations(result: TelegramInvariantResult) -> bool:
    return len(result.violations) > 0
