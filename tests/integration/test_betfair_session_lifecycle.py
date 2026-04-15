import pytest

from betfair_client import BetfairClient


class ResponseStub:
    def __init__(self, json_data, raise_http=False, status_code=200):
        self._json_data = json_data
        self._raise_http = raise_http
        self.status_code = status_code

    def raise_for_status(self):
        if self._raise_http:
            import requests
            raise requests.exceptions.HTTPError(response=self)

    def json(self):
        return self._json_data


class SessionStub:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append({"url": url, "kwargs": kwargs})
        if not self.responses:
            raise RuntimeError("no more responses")
        return self.responses.pop(0)


def build_client(session):
    c = BetfairClient(
        username="u",
        app_key="a",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
        max_retries=0,
    )
    c._last_login_password = "pw"
    c.session_manager.login_backoff_sec = 0
    return c


def _market_ok(mid="1.2"):
    return ResponseStub([{"result": [{"marketId": mid, "runners": []}]}])


def _auth_err(code):
    return ResponseStub([{"error": {"errorCode": code, "message": code}}])


def test_client_sources_auth_headers_from_session_manager():
    session = SessionStub([_market_ok()])
    client = build_client(session)
    client.session_manager.mark_logged_in("TOK")
    client.get_market_book("1.2")
    headers = session.calls[0]["kwargs"]["headers"]
    assert headers["X-Authentication"] == "TOK"


def test_client_does_not_send_authenticated_request_when_session_invalid_and_recovery_fails():
    session = SessionStub([])
    client = build_client(session)
    with pytest.raises(RuntimeError, match="NOT_AUTHENTICATED"):
        client.get_market_book("1.2")
    assert len(session.calls) == 0


def test_client_allows_one_controlled_recovery_attempt_then_retries_once(monkeypatch):
    session = SessionStub([_auth_err("NO_SESSION"), _market_ok("1.3")])
    client = build_client(session)
    client.session_manager.mark_logged_in("TOK")

    called = {"n": 0}

    def fake_recover():
        called["n"] += 1
        client.session_manager.mark_logged_in("TOK2")
        client.session_token = "TOK2"
        return True

    monkeypatch.setattr(client, "_attempt_controlled_recovery", fake_recover)
    out = client.get_market_book("1.3")
    assert out["marketId"] == "1.3"
    assert called["n"] == 1
    assert len(session.calls) == 2


def test_client_stops_after_one_recovery_attempt_if_second_attempt_also_auth_fails(monkeypatch):
    session = SessionStub([_auth_err("NO_SESSION"), _auth_err("INVALID_SESSION")])
    client = build_client(session)
    client.session_manager.mark_logged_in("TOK")

    called = {"n": 0}

    def fake_recover():
        called["n"] += 1
        client.session_manager.mark_logged_in("TOK2")
        client.session_token = "TOK2"
        return True

    monkeypatch.setattr(client, "_attempt_controlled_recovery", fake_recover)
    with pytest.raises(RuntimeError, match="SESSION_EXPIRED"):
        client.get_market_book("1.3")
    assert called["n"] == 1
    assert len(session.calls) == 2


def test_client_notifies_session_manager_exactly_once_on_no_session_response(monkeypatch):
    session = SessionStub([_auth_err("NO_SESSION")])
    client = build_client(session)
    client.session_manager.mark_logged_in("TOK")
    seen = {"n": 0}

    def hook(code, raw=None, now=None):
        seen["n"] += 1
        return original(code, raw=raw, now=now)

    original = client.session_manager.on_api_auth_error
    monkeypatch.setattr(client.session_manager, "on_api_auth_error", hook)
    monkeypatch.setattr(client, "_attempt_controlled_recovery", lambda: False)
    with pytest.raises(RuntimeError):
        client.get_market_book("1.3")
    assert seen["n"] == 1


def test_client_notifies_session_manager_exactly_once_on_invalid_session_response(monkeypatch):
    session = SessionStub([_auth_err("INVALID_SESSION")])
    client = build_client(session)
    client.session_manager.mark_logged_in("TOK")
    seen = {"n": 0}
    original = client.session_manager.on_api_auth_error

    def hook(code, raw=None, now=None):
        seen["n"] += 1
        return original(code, raw=raw, now=now)

    monkeypatch.setattr(client.session_manager, "on_api_auth_error", hook)
    monkeypatch.setattr(client, "_attempt_controlled_recovery", lambda: False)
    with pytest.raises(RuntimeError):
        client.get_market_book("1.3")
    assert seen["n"] == 1


def test_client_does_not_retry_on_temporary_ban_too_many_requests():
    session = SessionStub([_auth_err("TEMPORARY_BAN_TOO_MANY_REQUESTS")])
    client = build_client(session)
    client.session_manager.mark_logged_in("TOK")
    with pytest.raises(RuntimeError, match="AUTH_THROTTLED"):
        client.get_market_book("1.3")
    assert len(session.calls) == 1


def test_client_does_not_send_request_while_locked_out():
    session = SessionStub([])
    client = build_client(session)
    client.session_manager.on_api_auth_error("TEMPORARY_BAN_TOO_MANY_REQUESTS")
    with pytest.raises(RuntimeError, match="NOT_AUTHENTICATED"):
        client.get_market_book("1.3")
    assert len(session.calls) == 0


def test_client_preserves_business_payload_when_adding_auth_headers(monkeypatch):
    session = SessionStub([ResponseStub([{"result": {"status": "SUCCESS", "instructionReports": [{"status": "SUCCESS"}]}}])])
    client = build_client(session)
    client.session_manager.mark_logged_in("TOK")
    client.place_bet(market_id="1.1", selection_id=11, side="BACK", price=2.0, size=5.0)
    payload = session.calls[0]["kwargs"]["data"]
    assert '"marketId": "1.1"' in payload
    assert '"selectionId": 11' in payload
    assert '"price": 2.0' in payload
    assert '"size": 5.0' in payload


def test_client_does_not_treat_failed_recovery_as_success(monkeypatch):
    session = SessionStub([_auth_err("NO_SESSION")])
    client = build_client(session)
    client.session_manager.mark_logged_in("TOK")
    monkeypatch.setattr(client, "_attempt_controlled_recovery", lambda: False)
    out = client.place_bet(market_id="1.1", selection_id=11, side="BACK", price=2.0, size=5.0)
    assert out["ok"] is False


def test_client_request_path_has_no_recursive_retry_pattern(monkeypatch):
    session = SessionStub([_auth_err("NO_SESSION"), _auth_err("NO_SESSION")])
    client = build_client(session)
    client.session_manager.mark_logged_in("TOK")
    monkeypatch.setattr(client, "_attempt_controlled_recovery", lambda: True)
    with pytest.raises(RuntimeError):
        client.get_market_book("1.3")
    assert len(session.calls) <= 2


def test_client_handles_missing_error_field_as_auth_failure_when_context_indicates_session_problem():
    session = SessionStub([ResponseStub([{"error": {"message": "session issue"}}])])
    client = build_client(session)
    client.session_manager.mark_logged_in("TOK")
    with pytest.raises(RuntimeError):
        client.get_market_book("1.3")
    assert client.session_manager.last_error_category in {"AUTH_UNKNOWN", "AUTH_MALFORMED"}
