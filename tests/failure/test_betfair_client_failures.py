import pytest
import requests
import os
import shutil
import subprocess
from requests.exceptions import Timeout


class FakeResponse:
    def __init__(self, *, status_code=200, json_data=None, text="", raise_http=False):
        self.status_code = status_code
        self._json_data = json_data
        self.text = text
        self._raise_http = raise_http

    def raise_for_status(self):
        if self._raise_http:
            raise requests.exceptions.HTTPError(response=self)

    def json(self):
        if isinstance(self._json_data, Exception):
            raise self._json_data
        return self._json_data


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)

    def post(self, url, **kwargs):
        _ = url, kwargs
        if not self.responses:
            raise RuntimeError("no more fake responses")
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class RecordingSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if not self.responses:
            raise RuntimeError("no more fake responses")
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def _write_self_signed_cert(tmp_path):
    cert = tmp_path / "client.crt"
    key = tmp_path / "client.key"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-sha256",
            "-days",
            "365",
            "-nodes",
            "-subj",
            "/CN=localhost",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    cert.chmod(0o600)
    key.chmod(0o600)
    return cert, key


@pytest.mark.failure
@pytest.mark.skipif(shutil.which("openssl") is None, reason="openssl required to generate local test cert")
def test_login_timeout(monkeypatch, tmp_path):
    from betfair_client import BetfairClient

    cert, key = _write_self_signed_cert(tmp_path)

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem=str(cert),
        key_pem=str(key),
        session=FakeSession([Timeout()]),
    )

    with pytest.raises(RuntimeError, match="LOGIN_TIMEOUT"):
        client.login("pw")


@pytest.mark.failure
@pytest.mark.skipif(shutil.which("openssl") is None, reason="openssl required to generate local test cert")
def test_login_http_error(monkeypatch, tmp_path):
    from betfair_client import BetfairClient

    cert, key = _write_self_signed_cert(tmp_path)

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem=str(cert),
        key_pem=str(key),
        session=FakeSession([
            FakeResponse(status_code=500, raise_http=True, text="boom")
        ]),
    )

    with pytest.raises(RuntimeError, match="LOGIN_HTTP_ERROR"):
        client.login("pw")


@pytest.mark.failure
@pytest.mark.skipif(shutil.which("openssl") is None, reason="openssl required to generate local test cert")
def test_invalid_login_json(monkeypatch, tmp_path):
    from betfair_client import BetfairClient

    cert, key = _write_self_signed_cert(tmp_path)

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem=str(cert),
        key_pem=str(key),
        session=FakeSession([
            FakeResponse(json_data=ValueError("bad json"))
        ]),
    )

    with pytest.raises(RuntimeError, match="INVALID_LOGIN_JSON"):
        client.login("pw")


@pytest.mark.failure
def test_session_expired_clears_state():
    from betfair_client import BetfairClient

    session = FakeSession([
        FakeResponse(
            json_data=[{
                "error": {"code": -32099, "message": "INVALID_SESSION"}
            }]
        )
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
    )
    client.session_token = "TOK"
    client.connected = True

    with pytest.raises(RuntimeError, match="SESSION_EXPIRED"):
        client.get_market_book("1.100")

    assert client.session_token == ""
    assert client.connected is False


@pytest.mark.failure
def test_invalid_json_in_jsonrpc():
    from betfair_client import BetfairClient

    session = FakeSession([
        FakeResponse(json_data=ValueError("bad json"))
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
    )
    client.session_token = "TOK"

    with pytest.raises(RuntimeError, match="INVALID_JSON"):
        client.get_market_book("1.100")


@pytest.mark.failure
def test_invalid_json_rpc_shape():
    from betfair_client import BetfairClient

    session = FakeSession([
        FakeResponse(json_data={"not": "a list"})
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
    )
    client.session_token = "TOK"

    with pytest.raises(RuntimeError, match="INVALID_JSON_RPC"):
        client.get_market_book("1.100")


@pytest.mark.failure
def test_http_error_on_market_book_retries_and_fails():
    from betfair_client import BetfairClient

    session = FakeSession([
        FakeResponse(status_code=502, raise_http=True, text="bad gateway"),
        FakeResponse(status_code=502, raise_http=True, text="bad gateway"),
        FakeResponse(status_code=502, raise_http=True, text="bad gateway"),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
        max_retries=2,
    )
    client.session_token = "TOK"

    with pytest.raises(RuntimeError, match=r"REQUEST_FAILED: HTTP_502"):
        client.get_market_book("1.100")


@pytest.mark.failure
def test_place_bet_timeout_ambiguous_returns_order_unknown_true():
    from betfair_client import BetfairClient

    session = FakeSession([
        Timeout(),
        Timeout(),
        Timeout(),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
        max_retries=2,
    )
    client.session_token = "TOK"

    out = client.place_bet(
        market_id="1.100",
        selection_id=11,
        side="BACK",
        price=2.0,
        size=5.0,
    )

    assert out["ok"] is False
    assert "REQUEST_FAILED: TIMEOUT" in out["error"] or "TIMEOUT" in out["error"]
    assert out["classification"] == "TRANSIENT"
    assert out["order_unknown"] is True


@pytest.mark.failure
def test_cert_preflight_missing_cert_file_fails_closed_and_skips_network(tmp_path):
    from betfair_client import BetfairClient

    key = tmp_path / "client.key"
    key.write_text("KEY")
    key.chmod(0o600)
    session = RecordingSession([])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem=str(tmp_path / "missing.crt"),
        key_pem=str(key),
        session=session,
    )

    with pytest.raises(RuntimeError, match="CERT_FILE_MISSING"):
        client.login("pw")
    assert len(session.calls) == 0


@pytest.mark.failure
@pytest.mark.skipif(shutil.which("openssl") is None, reason="openssl required to generate local test cert")
def test_cert_preflight_missing_key_file_fails_closed_and_skips_network(tmp_path):
    from betfair_client import BetfairClient

    cert, _ = _write_self_signed_cert(tmp_path)
    session = RecordingSession([])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem=str(cert),
        key_pem=str(tmp_path / "missing.key"),
        session=session,
    )

    with pytest.raises(RuntimeError, match="CERT_KEY_MISSING"):
        client.login("pw")
    assert len(session.calls) == 0


@pytest.mark.failure
@pytest.mark.skipif(shutil.which("openssl") is None, reason="openssl required to generate local test cert")
def test_cert_preflight_unreadable_cert_fails_closed_and_skips_network(tmp_path):
    from betfair_client import BetfairClient

    _, key = _write_self_signed_cert(tmp_path)
    session = RecordingSession([])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem=str(tmp_path),
        key_pem=str(key),
        session=session,
    )

    with pytest.raises(RuntimeError, match="CERT_UNREADABLE"):
        client.login("pw")
    assert len(session.calls) == 0


@pytest.mark.failure
def test_cert_preflight_invalid_format_fails_closed_and_skips_network(tmp_path):
    from betfair_client import BetfairClient

    cert = tmp_path / "client.crt"
    key = tmp_path / "client.key"
    cert.write_text("NOT A CERT")
    key.write_text("KEY")
    cert.chmod(0o600)
    key.chmod(0o600)
    session = RecordingSession([])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem=str(cert),
        key_pem=str(key),
        session=session,
    )

    with pytest.raises(RuntimeError, match="CERT_INVALID_FORMAT"):
        client.login("pw")
    assert len(session.calls) == 0


@pytest.mark.failure
@pytest.mark.skipif(shutil.which("openssl") is None, reason="openssl required to generate local test cert")
def test_cert_preflight_expired_cert_fails_closed_and_skips_network(tmp_path, monkeypatch):
    from betfair_client import BetfairClient

    cert, key = _write_self_signed_cert(tmp_path)
    session = RecordingSession([])
    monkeypatch.setattr(
        "ssl._ssl._test_decode_cert",
        lambda _path: {"notAfter": "Jan 01 00:00:00 2000 GMT"},
    )

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem=str(cert),
        key_pem=str(key),
        session=session,
    )

    with pytest.raises(RuntimeError, match="CERT_EXPIRED"):
        client.login("pw")
    assert len(session.calls) == 0


@pytest.mark.failure
@pytest.mark.skipif(os.name != "posix", reason="POSIX permission semantics required")
@pytest.mark.skipif(shutil.which("openssl") is None, reason="openssl required to generate local test cert")
def test_cert_preflight_unsafe_key_permissions_fail_closed_and_skip_network(tmp_path):
    from betfair_client import BetfairClient

    cert, key = _write_self_signed_cert(tmp_path)
    key.chmod(0o644)
    session = RecordingSession([])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem=str(cert),
        key_pem=str(key),
        session=session,
    )

    with pytest.raises(RuntimeError, match="CERT_PERMISSIONS_UNSAFE"):
        client.login("pw")
    assert len(session.calls) == 0


@pytest.mark.failure
@pytest.mark.skipif(shutil.which("openssl") is None, reason="openssl required to generate local test cert")
def test_cert_preflight_valid_material_allows_login_request(tmp_path):
    from betfair_client import BetfairClient

    cert, key = _write_self_signed_cert(tmp_path)
    session = RecordingSession([
        FakeResponse(
            json_data={
                "loginStatus": "SUCCESS",
                "sessionToken": "TOK",
                "sessionExpiryTime": "2035-01-01T00:00:00Z",
            }
        )
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem=str(cert),
        key_pem=str(key),
        session=session,
    )

    out = client.login("pw")
    assert out["connected"] is True
    assert len(session.calls) == 1
    _, kwargs = session.calls[0]
    assert kwargs["cert"] == (str(cert), str(key))
