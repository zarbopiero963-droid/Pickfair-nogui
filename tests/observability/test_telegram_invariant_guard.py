from __future__ import annotations

from observability.telegram_invariant_guard import TelegramInvariantGuard, TelegramInvariantSnapshot, violation_codes


def _snapshot(**overrides):
    base = TelegramInvariantSnapshot(
        state="STOPPED",
        listener_started=True,
        client_alive=False,
        handlers_registered=1,
        reconnect_in_progress=False,
        reconnect_attempts=0,
        active_network_resources=0,
        intentional_stop=False,
        retry_loop_active=False,
        running=False,
        last_error="",
        last_successful_message_ts="2026-04-15T00:00:00+00:00",
        now_ts="2026-04-15T00:00:30+00:00",
        stale_after_seconds=120,
    )
    return TelegramInvariantSnapshot(**{**base.__dict__, **overrides})


def test_connected_requires_live_client():
    result = TelegramInvariantGuard().evaluate(_snapshot(state="CONNECTED", client_alive=False, active_network_resources=1))
    assert "CONNECTED_REQUIRES_CLIENT_ALIVE" in violation_codes(result)


def test_connected_requires_single_handler_registration():
    result = TelegramInvariantGuard().evaluate(
        _snapshot(state="CONNECTED", client_alive=True, active_network_resources=1, handlers_registered=2)
    )
    codes = set(violation_codes(result))
    assert "CONNECTED_REQUIRES_SINGLE_HANDLER" in codes
    assert "DUPLICATE_HANDLER_REGISTRATION" in codes


def test_reconnecting_requires_reconnect_in_progress():
    result = TelegramInvariantGuard().evaluate(_snapshot(state="RECONNECTING", reconnect_in_progress=False))
    assert "RECONNECTING_FLAG_MISSING" in violation_codes(result)


def test_stopped_requires_no_network_resources():
    result = TelegramInvariantGuard().evaluate(_snapshot(state="STOPPED", active_network_resources=1))
    assert "STOPPED_WITH_ACTIVE_NETWORK_RESOURCES" in violation_codes(result)


def test_failed_requires_retry_loop_inactive():
    result = TelegramInvariantGuard().evaluate(_snapshot(state="FAILED", retry_loop_active=True))
    assert "FAILED_RETRY_LOOP_ACTIVE" in violation_codes(result)


def test_ghost_connected_state_detected():
    result = TelegramInvariantGuard().evaluate(_snapshot(state="CONNECTED", client_alive=True, active_network_resources=0))
    assert "GHOST_CONNECTED_STATE" in violation_codes(result)


def test_stale_runtime_detected_when_timestamp_expired():
    result = TelegramInvariantGuard().evaluate(
        _snapshot(
            state="RECONNECTING",
            reconnect_in_progress=True,
            last_successful_message_ts="2026-04-15T00:00:00+00:00",
            now_ts="2026-04-15T00:05:01+00:00",
            stale_after_seconds=300,
        )
    )
    assert "STALE_RUNTIME" in violation_codes(result)


# --- PR-5b (#374): invariante generalizzato CONNECTED => handlers == expected ---

def test_connected_n_bot_expected_handlers_ok():
    # N bot sani: CONNECTED con handlers_registered == expected_handlers (=2) NON
    # deve violare né SINGLE_HANDLER né DUPLICATE (generalizzazione N-bot).
    result = TelegramInvariantGuard().evaluate(
        _snapshot(
            state="CONNECTED", client_alive=True, running=True,
            active_network_resources=2, handlers_registered=2, expected_handlers=2,
        )
    )
    codes = set(violation_codes(result))
    assert "CONNECTED_REQUIRES_SINGLE_HANDLER" not in codes
    assert "DUPLICATE_HANDLER_REGISTRATION" not in codes


def test_connected_n_bot_missing_handler_is_violation():
    # handlers (1) < expected (2) => mismatch handler quando CONNECTED.
    result = TelegramInvariantGuard().evaluate(
        _snapshot(
            state="CONNECTED", client_alive=True, running=True,
            active_network_resources=1, handlers_registered=1, expected_handlers=2,
        )
    )
    assert "CONNECTED_REQUIRES_SINGLE_HANDLER" in set(violation_codes(result))


def test_connected_n_bot_duplicate_over_expected_is_violation():
    # handlers (3) > expected (2) => duplicato oltre l'atteso.
    result = TelegramInvariantGuard().evaluate(
        _snapshot(
            state="CONNECTED", client_alive=True, running=True,
            active_network_resources=3, handlers_registered=3, expected_handlers=2,
        )
    )
    codes = set(violation_codes(result))
    assert "DUPLICATE_HANDLER_REGISTRATION" in codes
    assert "CONNECTED_REQUIRES_SINGLE_HANDLER" in codes


def test_single_bot_backward_compat_default_expected_is_one():
    # Default expected_handlers=1 (single-bot/Telethon): 1 handler CONNECTED è OK.
    ok = TelegramInvariantGuard().evaluate(
        _snapshot(
            state="CONNECTED", client_alive=True, running=True,
            active_network_resources=1, handlers_registered=1,
        )
    )
    codes = set(violation_codes(ok))
    assert "CONNECTED_REQUIRES_SINGLE_HANDLER" not in codes
    assert "DUPLICATE_HANDLER_REGISTRATION" not in codes
