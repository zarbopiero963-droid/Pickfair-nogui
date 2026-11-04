"""Fase F: avvio OPZIONALE della Control API, disattivato per impostazione predefinita.

Include una prova sul processo reale `headless_main.py --sim`: senza flag nessun socket in
ascolto e nessun file del token; con i flag ascolto SOLO su 127.0.0.1, token 0600, porta
chiusa allo spegnimento.
"""

from __future__ import annotations

import logging
import os
import signal
import socket
import stat
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest

from control_api import launcher

REPO_ROOT = Path(__file__).resolve().parents[2]


def private_dir(tmp_path: Path) -> Path:
    d = tmp_path / "secure"
    d.mkdir(mode=0o700)
    return d


def free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = int(s.getsockname()[1])
    s.close()
    return port


@pytest.mark.parametrize("argv", [[], ["--sim"], ["--live"], ["--control-api"], ["--port", "8765"]])
def test_not_requested_without_explicit_flags(argv: list[str]) -> None:
    assert launcher.requested(argv) is False


def test_default_start_does_nothing(runtime: Any, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
                                    caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    binds: list[Any] = []
    real_bind = socket.socket.bind
    monkeypatch.setattr(socket.socket, "bind", lambda self, a: (binds.append(a), real_bind(self, a))[1])
    assert launcher.maybe_start(runtime, ["--sim"], app_root=tmp_path) is None
    assert binds == []
    assert "CONTROL_API" not in caplog.text  # nessun tentativo, nemmeno fallito
    launcher.stop(None)


def test_default_start_does_not_import_the_package() -> None:
    probe = (
        "import sys; sys.path.insert(0, %r)\n"
        "from control_api import launcher\n"
        "assert launcher.maybe_start(object(), ['--sim'], app_root=None) is None\n"
        "print('pickfair_control_api' in sys.modules)\n"
    ) % str(REPO_ROOT)
    out = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, check=True,
                         timeout=60)
    assert out.stdout.strip() == "False"


@pytest.mark.parametrize(
    ("argv", "code"),
    [
        (["--control-api-port", "8765"], "BOTH_FLAGS_REQUIRED"),
        (["--control-api-token-file", "/x/t"], "BOTH_FLAGS_REQUIRED"),
        (["--control-api-port"], "FLAG_WITHOUT_VALUE"),
        (["--control-api-port", "0", "--control-api-token-file", "/x/t"], "PORT_INVALID"),
        (["--control-api-port", "-1", "--control-api-token-file", "/x/t"], "PORT_INVALID"),
        (["--control-api-port", "70000", "--control-api-token-file", "/x/t"], "PORT_INVALID"),
        (["--control-api-port", "8765", "--control-api-token-file", "rel/t"], "TOKEN_PATH_INVALID"),
        (["--control-api-port", "8765", "--control-api-port=8766", "--control-api-token-file", "/x/t"],
         "FLAG_REPEATED"),
    ],
)
def test_misconfiguration_never_starts(runtime: Any, caplog: pytest.LogCaptureFixture,
                                       tmp_path: Path, argv: list[str], code: str) -> None:
    caplog.set_level(logging.INFO)
    assert launcher.maybe_start(runtime, argv, app_root=tmp_path) is None
    assert f"CONTROL_API_NOT_STARTED: {code}" in caplog.text


@pytest.mark.parametrize("dir_mode", [0o777, 0o770, 0o757])
def test_unsafe_token_directory_never_starts(runtime: Any, caplog: pytest.LogCaptureFixture,
                                             tmp_path: Path, dir_mode: int) -> None:
    d = private_dir(tmp_path)
    os.chmod(d, dir_mode)
    (tmp_path / "BUILD_SHA").write_text("e" * 40)
    argv = ["--control-api-port", str(free_port()), "--control-api-token-file", str(d / "t")]
    assert launcher.maybe_start(runtime, argv, app_root=tmp_path) is None
    assert "CONTROL_API_NOT_STARTED: TOKEN_DIR_UNSAFE" in caplog.text
    assert not (d / "t").exists()


def test_unprovable_code_sha_never_starts(runtime: Any, caplog: pytest.LogCaptureFixture, tmp_path: Path) -> None:
    d = private_dir(tmp_path)
    argv = ["--control-api-port", str(free_port()), "--control-api-token-file", str(d / "t")]
    assert launcher.maybe_start(runtime, argv, app_root=tmp_path) is None  # niente .git, niente BUILD_SHA
    (tmp_path / "BUILD_SHA").write_text("not-a-sha")
    assert launcher.maybe_start(runtime, argv, app_root=tmp_path) is None
    assert caplog.text.count("CONTROL_API_NOT_STARTED: GIT_SHA_UNRESOLVABLE") == 2


def test_busy_port_never_crashes_the_runtime(runtime: Any, caplog: pytest.LogCaptureFixture, tmp_path: Path) -> None:
    d = private_dir(tmp_path)
    (tmp_path / "BUILD_SHA").write_text("e" * 40)
    busy = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    busy.bind(("127.0.0.1", 0))
    busy.listen(1)
    try:
        argv = ["--control-api-port", str(busy.getsockname()[1]), "--control-api-token-file", str(d / "t")]
        assert launcher.maybe_start(runtime, argv, app_root=tmp_path) is None
    finally:
        busy.close()
    assert "CONTROL_API_NOT_STARTED: BIND_FAILED" in caplog.text
    assert not (d / "t").exists()  # bind fallito: nessun token pubblicato, nessun servizio parziale


def test_explicit_flags_start_and_stop(runtime: Any, tmp_path: Path) -> None:
    d = private_dir(tmp_path)
    (tmp_path / "BUILD_SHA").write_text("e" * 40)
    port = free_port()
    server = launcher.maybe_start(runtime, ["--control-api-port", str(port),
                                            "--control-api-token-file", str(d / "t")], app_root=tmp_path)
    assert server is not None and server.port == port
    assert stat.S_IMODE(os.stat(d / "t").st_mode) == 0o600
    launcher.stop(server)
    with pytest.raises(OSError):
        socket.create_connection(("127.0.0.1", port), timeout=1)


# ---------------------------------------------------------------- processo reale


def listening_ports(pid: int) -> set[tuple[str, int]]:
    """Socket TCP IPv4/IPv6 in ascolto appartenenti a QUESTO processo (inode dei suoi fd)."""
    inodes = set()
    for fd in os.listdir(f"/proc/{pid}/fd"):
        try:
            target = os.readlink(f"/proc/{pid}/fd/{fd}")
        except OSError:
            continue
        if target.startswith("socket:["):
            inodes.add(target[8:-1])
    found = set()
    for table in ("tcp", "tcp6"):
        path = Path(f"/proc/{pid}/net/{table}")
        if not path.exists():  # famiglia non abilitata nel kernel: nessun socket possibile
            continue
        with path.open() as f:
            for row in f.readlines()[1:]:
                parts = row.split()
                if parts[3] == "0A" and parts[9] in inodes:
                    host, port = parts[1].split(":")
                    found.add((host, int(port, 16)))
    return found


def run_headless(tmp_path: Path, extra: list[str]) -> tuple[subprocess.Popen[str], Path]:
    work = tmp_path / "work"
    work.mkdir()
    env = {"PATH": os.environ.get("PATH", ""), "PYTHONPATH": os.environ.get("PYTHONPATH", ""),
           "HOME": str(tmp_path)}
    proc = subprocess.Popen([sys.executable, str(REPO_ROOT / "headless_main.py"), "--sim", *extra],
                            cwd=work, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True)
    return proc, work


def wait_for(predicate: Any, timeout: float = 30.0) -> bool:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return True
        time.sleep(0.2)
    return False


@pytest.mark.skipif(not os.path.isdir("/proc/self/fd"), reason="richiede /proc (Linux)")
def test_headless_main_without_flags_opens_no_port(tmp_path: Path) -> None:
    proc, work = run_headless(tmp_path, [])
    try:
        assert wait_for(lambda: (work / "pickfair.db").exists())
        time.sleep(4)  # avvio completo del runtime
        assert proc.poll() is None
        assert listening_ports(proc.pid) == set()
        assert not any(p.name.endswith(".token") for p in tmp_path.rglob("*"))
    finally:
        proc.send_signal(signal.SIGTERM)
        out = proc.communicate(timeout=30)[0]
    assert "CONTROL_API" not in out


@pytest.mark.skipif(not os.path.isdir("/proc/self/fd"), reason="richiede /proc (Linux)")
def test_headless_main_with_flags_listens_only_on_loopback_and_closes(tmp_path: Path) -> None:
    pytest.importorskip("pickfair_control_api")
    sha_provable = (REPO_ROOT / ".git").exists() or (REPO_ROOT / "BUILD_SHA").is_file()
    d = private_dir(tmp_path)
    port = free_port()
    proc, work = run_headless(tmp_path, ["--control-api-port", str(port),
                                         "--control-api-token-file", str(d / "control_api.token")])
    try:
        if sha_provable:
            assert wait_for(lambda: (d / "control_api.token").exists())
            assert wait_for(lambda: listening_ports(proc.pid) == {("0100007F", port)})
            assert stat.S_IMODE(os.stat(d / "control_api.token").st_mode) == 0o600
        else:
            assert wait_for(lambda: (work / "pickfair.db").exists())
            time.sleep(4)
            assert listening_ports(proc.pid) == set()
    finally:
        proc.send_signal(signal.SIGTERM)
        out = proc.communicate(timeout=30)[0]
    if not sha_provable:
        assert "CONTROL_API_NOT_STARTED: GIT_SHA_UNRESOLVABLE" in out
        assert not (d / "control_api.token").exists()
        return
    assert f"CONTROL_API_STARTED port={port}" in out
    assert (d / "control_api.token").read_text() not in out
    with pytest.raises(OSError):
        socket.create_connection(("127.0.0.1", port), timeout=1)


@pytest.mark.parametrize("attr", ["__version__", "schema_hash"])
def test_unpinned_package_never_starts(runtime: Any, caplog: pytest.LogCaptureFixture, tmp_path: Path,
                                       monkeypatch: pytest.MonkeyPatch, attr: str) -> None:
    pkg = pytest.importorskip("pickfair_control_api")
    if attr == "__version__":
        monkeypatch.setattr(pkg, "__version__", "9.9.9")
    else:
        monkeypatch.setattr(pkg, "schema_hash", lambda: "0" * 64)
    d = private_dir(tmp_path)
    (tmp_path / "BUILD_SHA").write_text("e" * 40)
    argv = ["--control-api-port", str(free_port()), "--control-api-token-file", str(d / "t")]
    binds: list[Any] = []
    real_bind = socket.socket.bind
    monkeypatch.setattr(socket.socket, "bind", lambda self, a: (binds.append(a), real_bind(self, a))[1])
    assert launcher.maybe_start(runtime, argv, app_root=tmp_path) is None
    assert "CONTROL_API_NOT_STARTED: CONTRACT_PIN_MISMATCH" in caplog.text
    assert not (d / "t").exists()  # nessun token pubblicato con un package non verificato
    assert binds == []             # il pin è verificato PRIMA di aprire il listener


def test_pinned_values_match_the_installed_package() -> None:
    pkg = pytest.importorskip("pickfair_control_api")
    from control_api import bridge

    assert pkg.__version__ == bridge.EXPECTED_CONTROL_API_VERSION
    assert pkg.schema_hash() == bridge.EXPECTED_SCHEMA_HASH


def test_failed_token_publication_closes_the_listener(runtime: Any, tmp_path: Path,
                                                      monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("pickfair_control_api")
    import pickfair_control_api.tokens as tokens_mod

    from control_api.bridge import start_control_api

    def broken(self: Any, path: Path) -> None:
        raise OSError("disk full")

    import pickfair_control_api.server as server_mod

    created: list[Any] = []  # riferimento esplicito: il GC non può chiudere il socket al posto nostro
    real_init = server_mod.ControlApiServer.__init__

    def recording_init(self: Any, *a: Any, **k: Any) -> None:
        real_init(self, *a, **k)
        created.append(self)

    monkeypatch.setattr(server_mod.ControlApiServer, "__init__", recording_init)
    monkeypatch.setattr(tokens_mod.TokenStore, "publish", broken)
    d = private_dir(tmp_path)
    port = free_port()
    with pytest.raises(OSError):
        start_control_api(runtime, port=port, token_path=d / "t", git_sha="e" * 40)
    assert len(created) == 1
    assert created[0]._httpd.socket.fileno() == -1  # listener chiuso esplicitamente
    with pytest.raises(OSError):
        socket.create_connection(("127.0.0.1", port), timeout=1)
    assert not (d / "t").exists()
