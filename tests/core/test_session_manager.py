import pytest

from core.session_manager import SessionManager


class FakeClock:
    def __init__(self, now=1000.0):
        self.now = float(now)

    def __call__(self):
        return self.now

    def advance(self, sec):
        self.now += float(sec)


class ScriptStub:
    def __init__(self, responses=None):
        self.responses = list(responses or [])
        self.calls = 0

    def __call__(self):
        self.calls += 1
        if self.responses:
            return self.responses.pop(0)
        return {"status": "FAIL", "errorCode": "INTERNAL_ERROR"}


REQUIRED_SNAPSHOT_KEYS = {
    "state",
    "has_token",
    "token_present",
    "logged_in_at",
    "last_keepalive_at",
    "session_expires_at",
    "keepalive_interval_sec",
    "session_ttl_sec",
    "consecutive_keepalive_failures",
    "consecutive_login_failures",
    "last_login_attempt_at",
    "last_keepalive_attempt_at",
    "locked_out_until",
    "last_error",
    "last_error_code",
    "last_error_category",
    "last_error_detail",
}


def build_manager(clock, login_responses=None, keepalive_responses=None, **kwargs):
    return SessionManager(
        login_func=ScriptStub(login_responses),
        keepalive_func=ScriptStub(keepalive_responses),
        clock=clock,
        **kwargs,
    )


def test_login_success_sets_active_state_and_expiry():
    clock = FakeClock()
    m = build_manager(clock, login_responses=[{"loginStatus": "SUCCESS", "sessionToken": "TOK"}])
    m.tick()
    assert m.state == "ACTIVE"
    assert m.get_session_token() == "TOK"
    assert m.logged_in_at == 1000.0
    assert m.session_expires_at == 2200.0
    assert m.consecutive_login_failures == 0


def test_snapshot_contract_has_all_required_keys():
    m = build_manager(FakeClock())
    assert REQUIRED_SNAPSHOT_KEYS == set(m.snapshot().keys())


def test_should_keepalive_false_before_safe_window():
    clock = FakeClock()
    m = build_manager(clock)
    m.mark_logged_in("TOK")
    clock.advance(599)
    assert m.should_keepalive() is False


def test_should_keepalive_true_when_safe_window_reached():
    clock = FakeClock()
    m = build_manager(clock)
    m.mark_logged_in("TOK")
    clock.advance(600)
    assert m.should_keepalive() is True


def test_tick_triggers_exactly_one_keepalive_when_due():
    clock = FakeClock()
    keepalive = ScriptStub([{"status": "SUCCESS"}])
    m = SessionManager(login_func=ScriptStub([]), keepalive_func=keepalive, clock=clock)
    m.mark_logged_in("TOK")
    clock.advance(600)
    m.tick()
    assert keepalive.calls == 1
    m.tick()
    assert keepalive.calls == 1


def test_keepalive_success_refreshes_expiry_and_resets_failures():
    clock = FakeClock()
    m = build_manager(clock, session_ttl_sec=3600)
    m.mark_logged_in("TOK")
    m.state = "REFRESHING"
    m.on_keepalive_result({"status": "FAIL", "errorCode": "INTERNAL_ERROR"})
    assert m.consecutive_keepalive_failures == 1
    clock.advance(1200)
    m.state = "REFRESHING"
    m.on_keepalive_result({"status": "SUCCESS"})
    assert m.last_keepalive_at == clock.now
    assert m.session_expires_at == clock.now + 3600
    assert m.consecutive_keepalive_failures == 0
    assert m.state == "ACTIVE"


def test_keepalive_no_session_marks_session_invalid():
    clock = FakeClock()
    m = build_manager(clock, keepalive_responses=[{"status": "FAIL", "errorCode": "NO_SESSION"}])
    m.mark_logged_in("TOK")
    clock.advance(600)
    m.tick()
    assert m.state == "EXPIRED"
    assert m.last_error_code == "NO_SESSION"
    assert m.has_valid_session() is False


def test_session_expires_after_20_minutes_without_keepalive():
    clock = FakeClock()
    m = build_manager(clock)
    m.mark_logged_in("TOK")
    clock.advance(1201)
    assert m.has_valid_session() is False


def test_api_success_does_not_extend_session_expiry():
    clock = FakeClock()
    m = build_manager(clock)
    m.mark_logged_in("TOK")
    expiry = m.session_expires_at
    clock.advance(200)
    m.on_api_success(raw={"ok": True})
    assert m.session_expires_at == expiry


def test_repeated_tick_before_due_does_not_spam_keepalive():
    clock = FakeClock()
    keepalive = ScriptStub([])
    m = SessionManager(login_func=ScriptStub([]), keepalive_func=keepalive, clock=clock)
    m.mark_logged_in("TOK")
    for _ in range(5):
        clock.advance(60)
        m.tick()
    assert keepalive.calls == 0


def test_one_login_attempt_max_per_tick():
    clock = FakeClock()
    login = ScriptStub([{"loginStatus": "FAIL", "errorCode": "INTERNAL_ERROR"}])
    m = SessionManager(login_func=login, keepalive_func=ScriptStub([]), clock=clock, login_backoff_sec=0)
    m.tick()
    m.tick()
    assert login.calls == 1


def test_repeated_login_failures_increment_counter_and_enter_degraded_or_lockout():
    clock = FakeClock()
    m = build_manager(clock, login_responses=[{"loginStatus": "FAIL", "errorCode": "INTERNAL_ERROR"}], login_backoff_sec=0)
    m.tick()
    assert m.consecutive_login_failures == 1
    assert m.state == "DEGRADED"


def test_temporary_ban_too_many_requests_enters_locked_out_when_no_valid_session():
    clock = FakeClock()
    m = build_manager(clock, login_responses=[{"loginStatus": "FAIL", "errorCode": "TEMPORARY_BAN_TOO_MANY_REQUESTS"}])
    m.tick()
    assert m.state == "LOCKED_OUT"
    assert m.locked_out_until == clock.now + 1200
    assert m.can_attempt_login() is False


def test_temporary_ban_preserves_existing_valid_session_if_one_already_exists():
    clock = FakeClock()
    m = build_manager(clock)
    m.mark_logged_in("TOK")
    m.on_login_result({"loginStatus": "FAIL", "errorCode": "TEMPORARY_BAN_TOO_MANY_REQUESTS"})
    assert m.get_session_token() == "TOK"
    assert m.last_error_code == "TEMPORARY_BAN_TOO_MANY_REQUESTS"


def test_locked_out_state_blocks_login_until_lockout_elapsed():
    clock = FakeClock()
    m = build_manager(clock, login_responses=[{"loginStatus": "FAIL", "errorCode": "TEMPORARY_BAN_TOO_MANY_REQUESTS"}])
    m.tick()
    assert m.can_attempt_login() is False
    clock.advance(1201)
    m.tick()
    assert m.state in {"LOGGED_OUT", "DEGRADED", "LOCKED_OUT"}


def test_no_session_api_auth_error_marks_expired_or_degraded():
    m = build_manager(FakeClock())
    m.mark_logged_in("TOK")
    m.on_api_auth_error("NO_SESSION")
    assert m.state == "EXPIRED"
    assert m.last_error_code == "NO_SESSION"


def test_invalid_session_api_auth_error_marks_expired_or_degraded():
    m = build_manager(FakeClock())
    m.mark_logged_in("TOK")
    m.on_api_auth_error("INVALID_SESSION")
    assert m.state == "EXPIRED"
    assert m.last_error_code == "INVALID_SESSION"


def test_unknown_or_malformed_auth_error_fails_closed_as_degraded():
    m = build_manager(FakeClock())
    m.mark_logged_in("TOK")
    m.on_api_auth_error("")
    assert m.state == "DEGRADED"
    assert m.last_error_category in {"AUTH_UNKNOWN", "AUTH_MALFORMED"}


def test_keepalive_failure_counter_increments_and_resets_after_success():
    clock = FakeClock()
    m = build_manager(clock, session_ttl_sec=3600)
    m.mark_logged_in("TOK")
    m.state = "REFRESHING"
    m.on_keepalive_result({"status": "FAIL", "errorCode": "INTERNAL_ERROR"})
    m.state = "REFRESHING"
    m.on_keepalive_result({"status": "FAIL", "errorCode": "INTERNAL_ERROR"})
    assert m.consecutive_keepalive_failures == 2
    clock.advance(1200)
    m.state = "REFRESHING"
    m.on_keepalive_result({"status": "SUCCESS"})
    assert m.consecutive_keepalive_failures == 0


def test_login_failure_counter_resets_after_successful_login():
    clock = FakeClock()
    m = build_manager(clock, login_responses=[
        {"loginStatus": "FAIL", "errorCode": "INTERNAL_ERROR"},
        {"loginStatus": "SUCCESS", "sessionToken": "TOK"},
    ], login_backoff_sec=0)
    m.tick()
    assert m.consecutive_login_failures == 1
    clock.advance(1)
    m.tick()
    assert m.consecutive_login_failures == 0


def test_illegal_transition_is_blocked_or_never_observed():
    m = build_manager(FakeClock())
    with pytest.raises(RuntimeError):
        m._enter_state("ACTIVE")
