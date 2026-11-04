"""Control API: server HTTP loopback, autenticazione, metodi, percorsi, configurazione.

Server reale su 127.0.0.1 (porta effimera), runtime Pickfair reale (conftest).
"""

from __future__ import annotations

import http.client
import json
import logging
import os
import stat
import threading
from pathlib import Path
from typing import Any

import pytest

from control_api.contract import CAPABILITIES, CONTRACT_VERSION, load_descriptor, resolve_git_sha, schema_hash
from control_api.server import ControlApiConfigError, ControlApiServer, issue_token_file
from core.runtime_controller import RuntimeController

TOKEN = "t" * 43
SHA = "a" * 40


@pytest.fixture
def server(runtime: RuntimeController) -> Any:
    srv = ControlApiServer(runtime, port=0, token=TOKEN, git_sha=SHA, allow_ephemeral_port=True)
    srv.start()
    yield srv
    srv.stop()


def request(
    srv: ControlApiServer,
    method: str,
    path: str,
    *,
    token: str | None = TOKEN,
    host: str | None = None,
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, dict[str, str], Any]:
    conn = http.client.HTTPConnection("127.0.0.1", srv.port, timeout=5)
    hdrs = {"Host": host or f"127.0.0.1:{srv.port}"}
    if token is not None:
        hdrs["Authorization"] = f"Bearer {token}"
    hdrs.update(headers or {})
    conn.request(method, path, body=body, headers=hdrs)
    resp = conn.getresponse()
    raw = resp.read()
    conn.close()
    return resp.status, {k.lower(): v for k, v in resp.getheaders()}, json.loads(raw) if raw else None


def test_meta_matches_the_shared_contract(server: ControlApiServer) -> None:
    status, headers, body = request(server, "GET", "/v1/meta")
    assert status == 200
    assert body == {
        "contract_version": CONTRACT_VERSION,
        "capabilities": list(CAPABILITIES),
        "git_sha": SHA,
        "schema_hash": schema_hash(),
    }
    assert headers["content-type"] == "application/json; charset=utf-8"
    assert headers["cache-control"] == "no-store"
    assert "python" not in headers.get("server", "").lower()


def test_status_and_reconciliation_served_from_the_real_runtime(server: ControlApiServer) -> None:
    status, _, body = request(server, "GET", "/v1/status")
    assert status == 200
    assert body["source"] == "core" and body["execution_mode"] == "SIM"
    status, _, body = request(server, "GET", "/v1/reconciliation/status")
    assert status == 200
    assert body["startup"]["state"] == "NOT_RUN"


@pytest.mark.parametrize("token", [None, "", "wrong" * 10, TOKEN + "x", TOKEN[:-1]])
def test_unauthorized_requests_are_rejected(server: ControlApiServer, token: str | None) -> None:
    status, _, body = request(server, "GET", "/v1/status", token=token)
    assert (status, body) == (401, {"error": {"code": "UNAUTHORIZED"}})


def test_basic_auth_scheme_rejected(server: ControlApiServer) -> None:
    status, _, _ = request(server, "GET", "/v1/status", token=None, headers={"Authorization": f"Basic {TOKEN}"})
    assert status == 401


@pytest.mark.parametrize("host", ["localhost", "evil.example", "127.0.0.1", "127.0.0.1:1", "[::1]:80"])
def test_wrong_host_header_rejected(server: ControlApiServer, host: str) -> None:
    status, _, body = request(server, "GET", "/v1/status", host=host)
    assert (status, body) == (400, {"error": {"code": "BAD_HOST"}})


@pytest.mark.parametrize("method", ["POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
@pytest.mark.parametrize("path", ["/v1/status", "/v1/orders", "/v1/mode", "/v1/stop", "/v1/cashout"])
def test_no_mutating_method_is_served(server: ControlApiServer, method: str, path: str) -> None:
    status, _, body = request(server, method, path, body=b"{}")
    assert (status, body) == (405, {"error": {"code": "METHOD_NOT_ALLOWED"}})


@pytest.mark.parametrize(
    "path",
    ["/v1/orders", "/v1/account", "/v1/status/", "/v1/status?x=1", "/v1/meta#a", "/", "/v1", "/V1/STATUS",
     "/v1/../v1/status", "//v1/status"],
)
def test_unknown_paths_are_not_found(server: ControlApiServer, path: str) -> None:
    status, _, body = request(server, "GET", path)
    assert (status, body) == (404, {"error": {"code": "NOT_FOUND"}})


def test_get_with_body_rejected(server: ControlApiServer) -> None:
    status, _, body = request(server, "GET", "/v1/status", body=b'{"mode":"LIVE"}')
    assert (status, body) == (400, {"error": {"code": "BODY_NOT_ALLOWED"}})


def test_internal_failure_returns_fixed_body(server: ControlApiServer, monkeypatch: pytest.MonkeyPatch) -> None:
    import control_api.server as srv_mod

    def boom(*_a: Any, **_k: Any) -> Any:
        raise RuntimeError("password=hunter2")

    monkeypatch.setitem(server._httpd.app.routes, "/v1/status", boom)
    status, _, body = request(server, "GET", "/v1/status")
    assert (status, body) == (500, {"error": {"code": "INTERNAL"}})
    assert srv_mod is not None


def test_token_never_logged(server: ControlApiServer, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    request(server, "GET", "/v1/status")
    request(server, "GET", "/v1/status", token="wrong-token-value-1234567890")
    text = caplog.text
    assert TOKEN not in text and "wrong-token-value" not in text


def test_counts_record_outcomes_without_secrets(server: ControlApiServer) -> None:
    request(server, "GET", "/v1/status")
    request(server, "GET", "/v1/status", token="x")
    counts = server.request_counts()
    assert counts.get("OK") == 1 and counts.get("UNAUTHORIZED") == 1


def test_concurrent_clients_all_get_valid_answers(server: ControlApiServer) -> None:
    results: list[int] = []
    lock = threading.Lock()

    def worker() -> None:
        for path in ("/v1/status", "/v1/reconciliation/status", "/v1/meta"):
            status, _, _ = request(server, "GET", path)
            with lock:
                results.append(status)

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert results == [200] * 30


def test_stopped_server_refuses_connections(runtime: RuntimeController) -> None:
    srv = ControlApiServer(runtime, port=0, token=TOKEN, git_sha=SHA, allow_ephemeral_port=True)
    srv.start()
    port = srv.port
    srv.stop()
    srv.stop()  # idempotente
    with pytest.raises(OSError):
        http.client.HTTPConnection("127.0.0.1", port, timeout=2).request("GET", "/v1/meta")


@pytest.mark.parametrize("host", ["0.0.0.0", "localhost", "::1", "192.168.1.10", "127.0.0.2", ""])
def test_non_loopback_literal_host_refused(runtime: RuntimeController, host: str) -> None:
    with pytest.raises(ControlApiConfigError, match="HOST_NOT_LOOPBACK_LITERAL"):
        ControlApiServer(runtime, host=host, port=0, token=TOKEN, git_sha=SHA, allow_ephemeral_port=True)


@pytest.mark.parametrize("port", [0, -1, 70000, "8765", True])
def test_port_must_be_explicit(runtime: RuntimeController, port: Any) -> None:
    with pytest.raises(ControlApiConfigError, match="PORT_NOT_EXPLICIT"):
        ControlApiServer(runtime, port=port, token=TOKEN, git_sha=SHA)


@pytest.mark.parametrize("token", ["", "short", "é" * 40, None])
def test_weak_token_refused(runtime: RuntimeController, token: Any) -> None:
    with pytest.raises(ControlApiConfigError, match="TOKEN_TOO_WEAK"):
        ControlApiServer(runtime, port=0, token=token, git_sha=SHA, allow_ephemeral_port=True)


@pytest.mark.parametrize("sha", ["", "abc", "A" * 40, "g" * 40, None])
def test_invalid_git_sha_refused(runtime: RuntimeController, sha: Any) -> None:
    with pytest.raises(ControlApiConfigError, match="GIT_SHA_INVALID"):
        ControlApiServer(runtime, port=0, token=TOKEN, git_sha=sha, allow_ephemeral_port=True)


def test_missing_runtime_refused() -> None:
    with pytest.raises(ControlApiConfigError, match="RUNTIME_MISSING"):
        ControlApiServer(None, port=0, token=TOKEN, git_sha=SHA, allow_ephemeral_port=True)


def test_token_file_is_private_and_never_overwritten(tmp_path: Path) -> None:
    path = tmp_path / "control_api.token"
    token = issue_token_file(path)
    assert len(token) >= 32
    assert path.read_text() == token
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
    with pytest.raises(FileExistsError):
        issue_token_file(path)
    assert path.read_text() == token


def test_token_file_refuses_symlink(tmp_path: Path) -> None:
    target = tmp_path / "elsewhere"
    link = tmp_path / "control_api.token"
    link.symlink_to(target)
    with pytest.raises(OSError):
        issue_token_file(link)
    assert not target.exists()


def test_descriptor_and_capabilities_are_consistent() -> None:
    descriptor = load_descriptor()
    routes = {f"{m} {p}" for _kind, m, p in descriptor["routes"]}
    assert len(descriptor["routes"]) == 25
    assert set(CAPABILITIES) <= routes
    assert all(c.startswith("GET ") for c in CAPABILITIES)  # nessun comando servito
    assert len(schema_hash()) == 64


def _git(tmp_path: Path, head: str) -> Path:
    (tmp_path / ".git" / "refs" / "heads").mkdir(parents=True)
    (tmp_path / ".git" / "HEAD").write_text(head + "\n")
    return tmp_path


def test_git_sha_from_loose_ref(tmp_path: Path) -> None:
    root = _git(tmp_path, "ref: refs/heads/main")
    (root / ".git" / "refs" / "heads" / "main").write_text("c" * 40 + "\n")
    assert resolve_git_sha(root) == "c" * 40


def test_git_sha_from_packed_refs(tmp_path: Path) -> None:
    root = _git(tmp_path, "ref: refs/heads/main")
    (root / ".git" / "packed-refs").write_text("# pack-refs\n" + "d" * 40 + " refs/heads/main\n")
    assert resolve_git_sha(root) == "d" * 40


def test_git_sha_detached_head(tmp_path: Path) -> None:
    assert resolve_git_sha(_git(tmp_path, "e" * 40)) == "e" * 40


@pytest.mark.parametrize("head", ["ref: refs/heads/missing", "not-a-sha", "E" * 40, ""])
def test_git_sha_unresolvable_is_an_error(tmp_path: Path, head: str) -> None:
    with pytest.raises(ValueError):
        resolve_git_sha(_git(tmp_path, head))


def test_git_sha_without_repository_is_an_error(tmp_path: Path) -> None:
    with pytest.raises(OSError):
        resolve_git_sha(tmp_path)
