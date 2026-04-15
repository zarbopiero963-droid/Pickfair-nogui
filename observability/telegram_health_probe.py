from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from observability.telegram_invariant_guard import (
    TelegramInvariantGuard,
    TelegramInvariantResult,
    TelegramInvariantSnapshot,
)


@dataclass(frozen=True)
class TelegramHealthSnapshot:
    state: str
    last_error: str
    reconnect_attempts: int
    reconnect_in_progress: bool
    last_successful_message_ts: str | None
    handlers_registered: int
    client_alive: bool
    intentional_stop: bool
    checked_at: str
    healthy: bool
    degraded: bool
    failed: bool
    invariant_ok: bool
    active_alert_codes: tuple[str, ...]


class TelegramHealthProbe:
    """Pure health probe for Telegram runtime truth."""

    def __init__(self, guard: TelegramInvariantGuard | None = None):
        self.guard = guard or TelegramInvariantGuard()

    def evaluate(
        self,
        snapshot: TelegramInvariantSnapshot,
        *,
        checked_at: str | None = None,
    ) -> TelegramHealthSnapshot:
        invariant_result = self.guard.evaluate(snapshot)
        codes = tuple(v.code for v in invariant_result.violations)
        normalized_checked_at = checked_at or _utc_now_iso()

        failed, degraded, healthy = _classify_health(snapshot, invariant_result)

        return TelegramHealthSnapshot(
            state=snapshot.state,
            last_error=snapshot.last_error,
            reconnect_attempts=int(snapshot.reconnect_attempts),
            reconnect_in_progress=bool(snapshot.reconnect_in_progress),
            last_successful_message_ts=snapshot.last_successful_message_ts,
            handlers_registered=int(snapshot.handlers_registered),
            client_alive=bool(snapshot.client_alive),
            intentional_stop=bool(snapshot.intentional_stop),
            checked_at=normalized_checked_at,
            healthy=healthy,
            degraded=degraded,
            failed=failed,
            invariant_ok=invariant_result.ok,
            active_alert_codes=codes,
        )


def _classify_health(
    snapshot: TelegramInvariantSnapshot,
    invariant_result: TelegramInvariantResult,
) -> tuple[bool, bool, bool]:
    if snapshot.intentional_stop and snapshot.state == "STOPPED":
        return False, False, False

    if snapshot.state == "FAILED" or not invariant_result.ok:
        return True, False, False

    if snapshot.state == "RECONNECTING" or snapshot.reconnect_in_progress:
        return False, True, False

    if snapshot.state == "STOPPED":
        return False, True, False

    if snapshot.state == "CONNECTED" and invariant_result.ok:
        return False, False, True

    return False, True, False


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
