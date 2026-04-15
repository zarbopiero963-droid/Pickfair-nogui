from __future__ import annotations

from observability.telegram_health_probe import TelegramHealthProbe
from observability.telegram_invariant_guard import TelegramInvariantSnapshot


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
        now_ts="2026-04-15T00:00:10+00:00",
        stale_after_seconds=600,
    )
    return TelegramInvariantSnapshot(**{**base.__dict__, **overrides})


def test_health_probe_reports_healthy_operational_state():
    probe = TelegramHealthProbe()
    health = probe.evaluate(
        _snapshot(state="CONNECTED", client_alive=True, active_network_resources=1),
        checked_at="2026-04-15T00:00:10+00:00",
    )

    assert health.healthy is True
    assert health.failed is False
    assert health.degraded is False
    assert health.invariant_ok is True


def test_health_probe_reports_degraded_while_reconnecting():
    probe = TelegramHealthProbe()
    health = probe.evaluate(
        _snapshot(state="RECONNECTING", reconnect_in_progress=True),
        checked_at="2026-04-15T00:00:10+00:00",
    )

    assert health.healthy is False
    assert health.degraded is True
    assert health.failed is False


def test_health_probe_reports_failed_on_invariant_break():
    probe = TelegramHealthProbe()
    health = probe.evaluate(
        _snapshot(state="CONNECTED", client_alive=False, active_network_resources=1),
        checked_at="2026-04-15T00:00:10+00:00",
    )

    assert health.failed is True
    assert health.invariant_ok is False
    assert "CONNECTED_REQUIRES_CLIENT_ALIVE" in health.active_alert_codes


def test_health_probe_includes_last_error_and_attempts():
    probe = TelegramHealthProbe()
    health = probe.evaluate(
        _snapshot(
            state="FAILED",
            last_error="boom",
            reconnect_attempts=3,
            reconnect_in_progress=False,
            retry_loop_active=False,
        ),
        checked_at="2026-04-15T00:00:10+00:00",
    )

    assert health.last_error == "boom"
    assert health.reconnect_attempts == 3
    assert health.failed is True


def test_health_probe_is_pure_and_non_blocking():
    probe = TelegramHealthProbe()
    snapshot = _snapshot(state="STOPPED", intentional_stop=True)

    first = probe.evaluate(snapshot, checked_at="2026-04-15T00:00:10+00:00")
    second = probe.evaluate(snapshot, checked_at="2026-04-15T00:00:10+00:00")

    assert first == second
    assert first.failed is False
    assert first.healthy is False
    assert first.degraded is False
