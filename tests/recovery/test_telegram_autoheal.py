from __future__ import annotations

from recovery.telegram_autoheal import (
    TelegramAutohealAction,
    TelegramAutohealHistory,
    TelegramAutohealPolicy,
    TelegramAutohealSnapshot,
)


def _snapshot(**overrides):
    base = {
        "state": "FAILED",
        "invariant_ok": True,
        "active_alert_codes": tuple(),
        "reconnect_attempts": 0,
        "restart_attempts_total": 0,
        "restart_in_progress": False,
        "intentional_stop": False,
        "startup_grace_active": False,
        "reconnect_grace_active": False,
        "lockout_active": False,
        "last_error_category": "",
        "failure_escalated": False,
        "listener_stale": False,
        "now_ts": 1000.0,
    }
    base.update(overrides)
    return TelegramAutohealSnapshot(**base)


def _history(*, restart_timestamps=(), lockout_since_ts=None):
    return TelegramAutohealHistory(restart_timestamps=tuple(restart_timestamps), lockout_since_ts=lockout_since_ts)


def test_intentional_stop_never_restarts():
    policy = TelegramAutohealPolicy()
    decision = policy.evaluate(
        _snapshot(state="STOPPED", intentional_stop=True),
        _history(),
    )
    assert decision.action == TelegramAutohealAction.NO_ACTION
    assert decision.recovery_allowed is False


def test_failed_state_can_schedule_bounded_restart():
    policy = TelegramAutohealPolicy(max_restarts_in_window=3, restart_window_sec=300)
    decision = policy.evaluate(_snapshot(state="FAILED", reconnect_attempts=1), _history(restart_timestamps=(500.0,)))
    assert decision.action == TelegramAutohealAction.SCHEDULE_RESTART


def test_restart_budget_exhaustion_enters_lockout():
    policy = TelegramAutohealPolicy(max_restarts_in_window=2, restart_window_sec=300)
    decision = policy.evaluate(_snapshot(now_ts=1000.0), _history(restart_timestamps=(900.0, 950.0)))
    assert decision.action == TelegramAutohealAction.ENTER_FAILED_LOCKOUT


def test_restart_cooldown_suppresses_burst_restarts():
    policy = TelegramAutohealPolicy(max_restarts_in_window=3, restart_cooldown_sec=30)
    decision = policy.evaluate(_snapshot(now_ts=1000.0), _history(restart_timestamps=(985.0,)))
    assert decision.action == TelegramAutohealAction.SUPPRESS_RESTART


def test_nonrecoverable_invariant_violation_does_not_restart():
    policy = TelegramAutohealPolicy()
    decision = policy.evaluate(
        _snapshot(invariant_ok=False, active_alert_codes=("DUPLICATE_HANDLER_REGISTRATION",)),
        _history(),
    )
    assert decision.action == TelegramAutohealAction.NO_ACTION


def test_recoverable_stale_runtime_schedules_restart():
    policy = TelegramAutohealPolicy()
    decision = policy.evaluate(
        _snapshot(state="CONNECTED", invariant_ok=False, active_alert_codes=("STALE_RUNTIME",), listener_stale=True),
        _history(),
    )
    assert decision.action == TelegramAutohealAction.SCHEDULE_RESTART


def test_duplicate_restart_attempt_blocked():
    policy = TelegramAutohealPolicy()
    decision = policy.evaluate(_snapshot(restart_in_progress=True), _history())
    assert decision.action == TelegramAutohealAction.SUPPRESS_RESTART


def test_lockout_state_is_observable():
    policy = TelegramAutohealPolicy(lockout_sec=60)
    decision = policy.evaluate(
        _snapshot(lockout_active=True, now_ts=1000.0),
        _history(lockout_since_ts=990.0),
    )
    assert decision.action == TelegramAutohealAction.SUPPRESS_RESTART
    assert decision.reason == "lockout_active"


def test_long_horizon_restart_budget_resets_after_window_expires():
    policy = TelegramAutohealPolicy(max_restarts_in_window=2, restart_window_sec=300)

    stale_history = _history(restart_timestamps=(100.0, 200.0))
    decision = policy.evaluate(
        _snapshot(now_ts=1000.0, state="FAILED", reconnect_attempts=1),
        stale_history,
    )

    assert decision.action == TelegramAutohealAction.SCHEDULE_RESTART
    assert decision.recovery_allowed is True


def test_long_horizon_lockout_expires_and_recovery_becomes_allowed():
    policy = TelegramAutohealPolicy(lockout_sec=60)

    locked = policy.evaluate(
        _snapshot(lockout_active=True, now_ts=1000.0, state="FAILED"),
        _history(lockout_since_ts=980.0),
    )
    released = policy.evaluate(
        _snapshot(lockout_active=True, now_ts=1100.0, state="FAILED"),
        _history(lockout_since_ts=980.0),
    )

    assert locked.action == TelegramAutohealAction.SUPPRESS_RESTART
    assert released.action == TelegramAutohealAction.SCHEDULE_RESTART
    assert released.recovery_allowed is True


def test_reconnect_storm_progresses_from_cooldown_to_lockout_deterministically():
    policy = TelegramAutohealPolicy(max_restarts_in_window=2, restart_window_sec=300, restart_cooldown_sec=30)

    cooldown = policy.evaluate(
        _snapshot(now_ts=1000.0, state="FAILED", reconnect_attempts=1),
        _history(restart_timestamps=(985.0,)),
    )
    second_restart = policy.evaluate(
        _snapshot(now_ts=1020.0, state="FAILED", reconnect_attempts=1),
        _history(restart_timestamps=(950.0,)),
    )
    lockout = policy.evaluate(
        _snapshot(now_ts=1030.0, state="FAILED", reconnect_attempts=2),
        _history(restart_timestamps=(900.0, 1005.0)),
    )

    assert cooldown.action == TelegramAutohealAction.SUPPRESS_RESTART
    assert second_restart.action == TelegramAutohealAction.SCHEDULE_RESTART
    assert lockout.action == TelegramAutohealAction.ENTER_FAILED_LOCKOUT
