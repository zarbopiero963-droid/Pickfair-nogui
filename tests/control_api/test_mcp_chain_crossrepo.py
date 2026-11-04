"""Catena cross-repository: client MCP → server pickfair-mcp → Control API → RuntimeController.

Cosa è REALE: client MCP dell'SDK ufficiale, server MCP di pickfair-mcp (tool, autorizzazione,
handshake, parsing strict, envelope, audit), server HTTP della Control API, RuntimeController
con Database SQLite, EventBus e servizi reali.

Cosa NON è prodotto: il trasporto HTTP lato MCP è un HARNESS di test (``HarnessHttpAdapter``),
perché l'adapter HTTP di pickfair-mcp (scheda MCP-01d) non esiste ancora e le guardie del
repository MCP vietano l'import di moduli di rete. Questa prova NON certifica MCP-01d.

Richiede ``pickfair_mcp`` installato nello stesso ambiente; altrimenti il modulo è SKIPPED
(NOT_RUN, mai PASS).
"""

from __future__ import annotations

import http.client
import itertools
import json
import socket
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest

pickfair_mcp = pytest.importorskip("pickfair_mcp")

import anyio  # noqa: E402
from mcp import ClientSession  # noqa: E402
from mcp.shared.memory import create_connected_server_and_client_session  # noqa: E402
from pickfair_mcp.adapters.base import UpstreamTimeout, UpstreamUnavailable  # noqa: E402
from pickfair_mcp.auth import Principal, Scope  # noqa: E402
from pickfair_mcp.contract.version import contract_schema_hash  # noqa: E402
from pickfair_mcp.server import build_server  # noqa: E402

from control_api.contract import resolve_git_sha, schema_hash  # noqa: E402
from control_api.server import ControlApiServer, issue_token_file  # noqa: E402
from core.runtime_controller import RuntimeController  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
READER = Principal(principal_id="chain-reader", scopes=frozenset({Scope.READ}))


class HarnessHttpAdapter:
    """Trasporto HTTP di PROVA verso la Control API (non è l'adapter MCP-01d)."""

    def __init__(self, port: int, token: str, *, timeout_s: float = 2.0) -> None:
        self._port = port
        self._token = token
        self._timeout_s = timeout_s
        self.requests = 0

    @property
    def kind(self) -> str:
        return "http"

    @property
    def certified(self) -> bool:
        return False

    @property
    def connection_epoch(self) -> int:
        return 1

    def _get_blocking(self, path: str) -> bytes:
        self.requests += 1
        conn = http.client.HTTPConnection("127.0.0.1", self._port, timeout=self._timeout_s)
        try:
            conn.request(
                "GET",
                path,
                headers={"Host": f"127.0.0.1:{self._port}", "Authorization": f"Bearer {self._token}"},
            )
            resp = conn.getresponse()
            body = resp.read(64 * 1024 + 1)
        except socket.timeout:
            raise UpstreamTimeout from None
        except OSError:
            raise UpstreamUnavailable from None
        finally:
            conn.close()
        if resp.status != 200:
            raise UpstreamUnavailable
        return body

    async def _get(self, path: str) -> bytes:
        return await anyio.to_thread.run_sync(self._get_blocking, path)

    async def get_meta_raw(self) -> bytes:
        return await self._get("/v1/meta")

    async def get_status_raw(self) -> bytes:
        return await self._get("/v1/status")

    async def get_reconciliation_status_raw(self) -> bytes:
        return await self._get("/v1/reconciliation/status")


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
def chain(runtime: RuntimeController, tmp_path: Path) -> Any:
    token = issue_token_file(tmp_path / "control_api.token")
    api = ControlApiServer(
        runtime, port=0, token=token, git_sha=resolve_git_sha(REPO_ROOT), allow_ephemeral_port=True
    )
    api.start()
    yield runtime, api, token
    api.stop()


def _ids() -> Callable[[], str]:
    counter = itertools.count(1)
    return lambda: f"req_{next(counter):032x}"


@asynccontextmanager
async def mcp_session(adapter: HarnessHttpAdapter, principal: Principal | None = READER) -> AsyncIterator[ClientSession]:
    server = build_server(adapter, timeout_s=2.0, principal=principal, request_id_factory=_ids())
    with anyio.fail_after(30):
        async with create_connected_server_and_client_session(server) as session:
            yield session


async def _call(session: ClientSession, name: str) -> tuple[bool, dict[str, Any]]:
    result = await session.call_tool(name, {})
    assert result.structuredContent is not None
    return bool(result.isError), result.structuredContent


def test_shared_contract_hash_is_identical_in_both_repositories() -> None:
    assert schema_hash() == contract_schema_hash()


@pytest.mark.anyio
async def test_full_chain_reads_real_core_state(chain: Any) -> None:
    runtime, api, token = chain
    adapter = HarnessHttpAdapter(api.port, token)
    async with mcp_session(adapter) as session:
        meta_err, meta = await _call(session, "get_meta")
        st_err, status = await _call(session, "get_status")
        rc_err, recon = await _call(session, "get_reconciliation_status")
    assert (meta_err, st_err, rc_err) == (False, False, False)
    assert meta["data"]["handshake"]["status"] == "COMPATIBLE"
    assert meta["data"]["git_sha"] == resolve_git_sha(REPO_ROOT)
    assert status["data"]["upstream"] == {"kind": "http", "certified": False}
    data = status["data"]["status"]
    assert data["source"] == "core"
    assert data["runtime_state"] == runtime.mode.value == "STOPPED"
    assert data["execution_mode"] == "SIM" and runtime.execution_mode == "SIMULATION"
    assert data["readiness"]["level"] == "DEGRADED"
    assert recon["data"]["reconciliation"]["startup"]["state"] == "NOT_RUN"
    assert api.request_counts() == {"OK": 3}   # un solo handshake, poi due letture


@pytest.mark.anyio
async def test_chain_reflects_a_real_emergency_stop_in_the_core(chain: Any) -> None:
    runtime, api, token = chain
    adapter = HarnessHttpAdapter(api.port, token)
    async with mcp_session(adapter) as session:
        _, before = await _call(session, "get_status")
        runtime.emergency_stop("DAILY_LOSS_BREACH: soglia superata")
        _, after = await _call(session, "get_status")
    assert before["data"]["status"]["stop"]["emergency_stopped"] is False
    stop = after["data"]["status"]["stop"]
    assert stop["emergency_stopped"] is True
    assert stop["emergency_reason"] == "DAILY_LOSS_BREACH"
    assert "soglia" not in json.dumps(after)


@pytest.mark.anyio
async def test_chain_reports_live_blockers_from_the_core(chain: Any) -> None:
    runtime, api, token = chain
    runtime.execution_mode = "LIVE"
    async with mcp_session(HarnessHttpAdapter(api.port, token)) as session:
        _, status = await _call(session, "get_status")
    readiness = status["data"]["status"]["readiness"]
    assert readiness["ready"] is False and "LIVE_NOT_ENABLED" in readiness["blockers"]


@pytest.mark.anyio
async def test_wrong_token_fails_closed_end_to_end(chain: Any) -> None:
    _runtime, api, _token = chain
    adapter = HarnessHttpAdapter(api.port, "x" * 43)
    async with mcp_session(adapter) as session:
        is_error, payload = await _call(session, "get_status")
    assert is_error is True
    assert payload["error"]["code"] == "UPSTREAM_UNAVAILABLE"
    assert payload["data"] is None
    assert api.request_counts() == {"UNAUTHORIZED": 1}   # handshake rifiutato, nessuna lettura


@pytest.mark.anyio
async def test_mcp_authorization_blocks_before_reaching_the_core(chain: Any) -> None:
    _runtime, api, token = chain
    adapter = HarnessHttpAdapter(api.port, token)
    writer_only = Principal(principal_id="writer", scopes=frozenset({Scope.WRITE}))
    async with mcp_session(adapter, principal=writer_only) as session:
        is_error, payload = await _call(session, "get_status")
    assert is_error is True and payload["error"]["code"] == "FORBIDDEN"
    assert adapter.requests == 0 and api.request_counts() == {}


@pytest.mark.anyio
async def test_control_api_down_is_retryable_unavailable(runtime: RuntimeController, tmp_path: Path) -> None:
    token = issue_token_file(tmp_path / "t.token")
    api = ControlApiServer(runtime, port=0, token=token, git_sha="b" * 40, allow_ephemeral_port=True)
    port = api.port
    api.stop()                              # porta chiusa: Control API indisponibile
    async with mcp_session(HarnessHttpAdapter(port, token)) as session:
        is_error, payload = await _call(session, "get_status")
    assert is_error is True
    assert payload["error"]["code"] == "UPSTREAM_UNAVAILABLE"
    assert payload["error"]["retryable"] is True


@pytest.mark.anyio
async def test_chain_has_no_side_effects_on_the_core(chain: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    runtime, api, token = chain
    calls: list[str] = []

    def record(name: str) -> Callable[..., Any]:
        def _f(*_a: Any, **_k: Any) -> Any:
            calls.append(name)
            raise AssertionError(name)

        return _f

    for name in ("get_status", "start", "stop", "pause", "resume", "reload_config",
                 "set_simulation_mode", "emergency_stop", "risk_stop", "force_lockdown"):
        monkeypatch.setattr(runtime, name, record(name))
    published = list(runtime.bus.published)
    async with mcp_session(HarnessHttpAdapter(api.port, token)) as session:
        for _ in range(10):
            await _call(session, "get_status")
            await _call(session, "get_reconciliation_status")
    assert calls == []
    assert runtime.bus.published == published
