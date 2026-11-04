"""Server HTTP della Control API v1 — sola lettura, solo loopback, autenticato.

- Bind ammesso soltanto su ``127.0.0.1`` (IP letterale) con porta esplicita; la porta 0
  (effimera) solo con ``allow_ephemeral_port=True``, usata dai test.
- Ogni richiesta deve portare ``Authorization: Bearer <token>`` (confronto a tempo costante)
  e ``Host: 127.0.0.1:<porta>`` (difesa da DNS rebinding).
- Solo ``GET`` sulle route di ``contract.CAPABILITIES``, senza query string né corpo.
  Ogni altro metodo => 405; ogni altro percorso => 404.
- Corpi di errore fissi; il token e gli header non compaiono mai nei log.
- Autenticazione PROVISIONAL (decisione D-11 aperta): token casuale per avvio, consegnato
  al MCP tramite file con permessi 0600 (``issue_token_file``).
"""

from __future__ import annotations

import hmac
import json
import logging
import os
import secrets
import threading
import time
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Final

from control_api.contract import CAPABILITIES, CONTRACT_VERSION, GIT_SHA_RE, schema_hash
from control_api.views import reconciliation_view, status_view

logger = logging.getLogger(__name__)

LOOPBACK_HOST: Final = "127.0.0.1"
MIN_TOKEN_CHARS: Final = 32
REQUEST_TIMEOUT_S: Final = 5.0
MAX_REQUEST_BODY: Final = 0


class ControlApiConfigError(Exception):
    """Configurazione non sicura o incompleta: il server non parte."""


def issue_token_file(path: Path) -> str:
    """Crea un token nuovo in un file nuovo con permessi 0600 (mai sovrascritto, mai symlink)."""
    token = secrets.token_urlsafe(32)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, 0o600)
    try:
        os.write(fd, token.encode("ascii"))
        os.fsync(fd)
    finally:
        os.close(fd)
    return token


class _Handler(BaseHTTPRequestHandler):
    server: _Server
    protocol_version = "HTTP/1.1"
    server_version = "pickfair-control-api"
    sys_version = ""

    def setup(self) -> None:
        super().setup()
        self.connection.settimeout(REQUEST_TIMEOUT_S)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002 - firma della base
        """Nessun log per richiesta: il percorso e gli header non escono."""

    def _send(self, status: int, body: dict[str, Any]) -> None:
        payload = json.dumps(body, sort_keys=True, allow_nan=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(payload)
        self.close_connection = True

    def _error(self, status: int, code: str) -> None:
        self.server.app.count(code)
        self._send(status, {"error": {"code": code}})

    def _authorized(self) -> bool:
        header = self.headers.get("Authorization", "")
        if not header.startswith("Bearer "):
            return False
        presented = header[len("Bearer "):].encode("utf-8", "surrogateescape")
        return hmac.compare_digest(presented, self.server.app.token_bytes)

    def do_GET(self) -> None:  # noqa: N802 - nome imposto da BaseHTTPRequestHandler
        app = self.server.app
        if self.headers.get("Host") != app.expected_host:
            return self._error(400, "BAD_HOST")
        if not self._authorized():
            return self._error(401, "UNAUTHORIZED")
        length = self.headers.get("Content-Length")
        if self.headers.get("Transfer-Encoding") or (length not in (None, "0")):
            return self._error(400, "BODY_NOT_ALLOWED")
        # Percorso grezzo della request line: http.server normalizza «//x» in «/x», e il
        # contratto ammette solo i percorsi esatti delle route approvate.
        parts = self.requestline.split(" ")
        raw_path = parts[1] if len(parts) == 3 else ""
        handler = app.routes.get(raw_path) if raw_path == self.path else None
        if handler is None:
            return self._error(404, "NOT_FOUND")
        try:
            body = handler()
        except Exception:  # noqa: BLE001 - mai testo di eccezione verso il client
            logger.exception("control_api: lettura fallita")
            return self._error(500, "INTERNAL")
        app.count("OK")
        self._send(200, body)

    def _method_not_allowed(self) -> None:
        self._error(405, "METHOD_NOT_ALLOWED")

    do_POST = do_PUT = do_PATCH = do_DELETE = do_OPTIONS = do_HEAD = _method_not_allowed  # noqa: N815


class _App:
    def __init__(self, routes: dict[str, Callable[[], dict[str, Any]]], token: str, port: int):
        self.routes = routes
        self.token_bytes = token.encode("utf-8")
        self.expected_host = f"{LOOPBACK_HOST}:{port}"
        self._counts: dict[str, int] = {}
        self._lock = threading.Lock()

    def count(self, code: str) -> None:
        with self._lock:
            self._counts[code] = self._counts.get(code, 0) + 1

    def counts(self) -> dict[str, int]:
        with self._lock:
            return dict(self._counts)


class _Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = False
    app: _App


class ControlApiServer:
    """Control API v1 di sola lettura sopra un ``RuntimeController`` già costruito."""

    def __init__(
        self,
        runtime: object,
        *,
        port: int,
        token: str,
        git_sha: str,
        host: str = LOOPBACK_HOST,
        allow_ephemeral_port: bool = False,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if host != LOOPBACK_HOST:
            raise ControlApiConfigError("HOST_NOT_LOOPBACK_LITERAL")
        if type(port) is not int or not (0 <= port <= 65535) or (port == 0 and not allow_ephemeral_port):
            raise ControlApiConfigError("PORT_NOT_EXPLICIT")
        if not isinstance(token, str) or len(token) < MIN_TOKEN_CHARS or not token.isascii():
            raise ControlApiConfigError("TOKEN_TOO_WEAK")
        if not isinstance(git_sha, str) or not GIT_SHA_RE.fullmatch(git_sha):
            raise ControlApiConfigError("GIT_SHA_INVALID")
        if runtime is None:
            raise ControlApiConfigError("RUNTIME_MISSING")
        self._runtime = runtime
        self._token = token
        self._git_sha = git_sha
        self._clock = clock
        self._meta = {
            "contract_version": CONTRACT_VERSION,
            "capabilities": list(CAPABILITIES),
            "git_sha": git_sha,
            "schema_hash": schema_hash(),
        }
        self._httpd = _Server((host, port), _Handler, bind_and_activate=True)
        self.port = int(self._httpd.server_address[1])
        self._httpd.app = _App(self._routes(), token, self.port)
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

    def _routes(self) -> dict[str, Callable[[], dict[str, Any]]]:
        return {
            "/v1/meta": lambda: dict(self._meta),
            "/v1/status": lambda: status_view(self._runtime, clock=self._clock),
            "/v1/reconciliation/status": lambda: reconciliation_view(self._runtime, clock=self._clock),
        }

    @property
    def base_url(self) -> str:
        return f"http://{LOOPBACK_HOST}:{self.port}"

    def request_counts(self) -> dict[str, int]:
        return self._httpd.app.counts()

    def start(self) -> None:
        with self._lock:
            if self._thread is not None:
                return
            self._thread = threading.Thread(
                target=self._httpd.serve_forever, name="control-api", daemon=True
            )
            self._thread.start()

    def stop(self) -> None:
        with self._lock:
            thread, self._thread = self._thread, None
        if thread is not None:
            self._httpd.shutdown()
            thread.join(timeout=5)
        self._httpd.server_close()

    def __enter__(self) -> ControlApiServer:
        self.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.stop()
