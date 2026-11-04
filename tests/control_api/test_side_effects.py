"""Fase D: le letture della Control API non hanno effetti sul runtime SIM AVVIATO.

Runtime reale (SQLite, EventBus, SimulationBroker connesso, riconciliazione d'avvio eseguita).
Verifica: DB, bus, chiamate Betfair/broker, saldi, monitor di rischio, stato profondo,
assenza di blocchi del runtime. Le letture passano sia dalle viste sia dal server HTTP reale.
"""

from __future__ import annotations

import hashlib
import http.client
import threading
import time
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("pickfair_control_api")

from control_api.bridge import make_providers, start_control_api  # noqa: E402
from control_api.views import (  # noqa: E402
    account_view,
    exposure_view,
    reconciliation_view,
    status_view,
)

OBJECTS = ("risk_desk", "table_manager", "duplication_guard", "mm", "reconciliation_engine")
# Ammessi dopo lettura del codice (services/betfair_service.py): `status()` costruisce un dict
# dallo stato locale e chiama `get_client()`, che restituisce il broker/cliente già creato.
# Nessuno dei due contatta l'exchange. Ogni altro metodo del servizio e TUTTO il broker sono vietati.
# MCP-01f: `keepalive_status()` legge solo stato locale del thread di keepalive sotto il suo lock
# (services/betfair_service.py, `keepalive_status`): nessuna rete, nessuna modifica. Lo stato della
# connessione si legge dagli attributi, senza chiamare `status()`.
ALLOWED_BETFAIR = {"status", "get_client", "keepalive_status"}


def deep_state(rc: Any) -> dict[str, str]:
    out = {f"rc.{k}": repr(v) for k, v in vars(rc).items() if not callable(v)}
    for name in OBJECTS:
        obj = getattr(rc, name)
        out.update({f"{name}.{k}": repr(v) for k, v in vars(obj).items() if not callable(v)})
    return out


def db_digest(root: Path) -> str:
    h = hashlib.sha256()
    for path in sorted(root.glob("pickfair.db*")):
        h.update(path.name.encode())
        h.update(path.read_bytes())
    return h.hexdigest()


def record_calls(monkeypatch: pytest.MonkeyPatch, obj: Any, prefix: str, calls: list[str],
                 allowed: set[str] = frozenset()) -> None:  # type: ignore[assignment]
    for name in dir(obj):
        if name.startswith("_") or name in allowed:
            continue
        try:
            attr = getattr(obj, name)
        except Exception:
            continue
        if not callable(attr) or isinstance(attr, type):
            continue

        def make(n: str, fn: Any) -> Any:
            def spy(*a: Any, **k: Any) -> Any:
                calls.append(f"{prefix}.{n}")
                return fn(*a, **k)
            return spy

        try:
            monkeypatch.setattr(obj, name, make(name, attr))
        except (AttributeError, TypeError):
            continue


def read_http(port: int, token: str, path: str) -> int:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    conn.request("GET", path, headers={"Authorization": f"Bearer {token}"})
    status = conn.getresponse().status
    conn.close()
    return status


def test_reads_have_no_effect_on_a_started_sim_runtime(
    sim_runtime: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rc = sim_runtime
    broker = rc.betfair_service.get_client()
    assert type(broker).__name__ == "SimulationBroker"
    balance_before = rc.betfair_service.simulation_snapshot()
    token_dir = tmp_path / "secure"
    token_dir.mkdir(mode=0o700)
    server = start_control_api(rc, port=0, token_path=token_dir / "t", git_sha="d" * 40,
                               allow_ephemeral_port=True)
    try:
        token = (token_dir / "t").read_text()
        calls: list[str] = []
        record_calls(monkeypatch, rc.betfair_service, "betfair", calls, ALLOWED_BETFAIR)
        record_calls(monkeypatch, broker, "broker", calls)
        for name in ("get_status", "_monitor_daily_loss_breach", "_monitor_sl_tp_trailing",
                     "_apply_funds_reading", "_record_runtime_io", "start", "stop", "pause",
                     "resume", "reload_config", "set_simulation_mode", "emergency_stop",
                     "risk_stop", "force_lockdown", "reset_cycle", "reconcile_held_settlements"):
            original = getattr(rc, name)

            def spy(*a: Any, _n: str = name, _f: Any = original, **k: Any) -> Any:
                calls.append(f"rc.{_n}")
                return _f(*a, **k)

            monkeypatch.setattr(rc, name, spy)
        db_before, bus_before, state_before = db_digest(tmp_path), list(rc.bus.published), deep_state(rc)

        for _ in range(50):
            status_view(rc)
            reconciliation_view(rc)
            account_view(rc)
            exposure_view(rc)
            assert read_http(server.port, token, "/v1/status") == 200
            assert read_http(server.port, token, "/v1/reconciliation/status") == 200
            assert read_http(server.port, token, "/v1/account") == 200
            assert read_http(server.port, token, "/v1/exposure") == 200

        assert calls == []                                   # 3, 5: nessuna query, nessun monitor
        assert db_digest(tmp_path) == db_before              # 1
        assert rc.bus.published == bus_before                # 2
        assert deep_state(rc) == state_before                # stato profondo invariato
    finally:
        server.stop()
    assert rc.betfair_service.simulation_snapshot() == balance_before  # 4: saldi invariati


def test_reads_never_wait_on_runtime_locks(sim_runtime: Any) -> None:
    rc = sim_runtime
    held = threading.Event()
    release = threading.Event()

    def holder() -> None:
        with rc._risk_admission_lock, rc.table_manager.locked():
            held.set()
            release.wait(10)

    t = threading.Thread(target=holder)
    t.start()
    assert held.wait(5)
    try:
        started = time.monotonic()
        status_view(rc)
        reconciliation_view(rc)
        account = account_view(rc)
        exposure = exposure_view(rc)
        elapsed = time.monotonic() - started
    finally:
        release.set()
        t.join()
    # Il lock resta tenuto fino a 10 s: se le letture lo attendessero durerebbero ~10 s.
    # Soglia larga e stabile anche sotto carico (mutation testing in parallelo).
    assert elapsed < 2.0  # 6: le letture non attendono i lock di ammissione e dei tavoli
    # MCP-01e: con il lock dei tavoli tenuto dal runtime le sezioni dei tavoli sono indisponibili
    # (attesa massima limitata), mai bloccate né stimate; quelle del RiskDesk restano leggibili.
    assert exposure["tables"] is None and exposure["total_exposure"] is None
    assert exposure["recovery"] is None
    assert exposure["unavailable"] == ["tables", "total_exposure", "recovery"]
    assert account["bankroll"] is not None


def test_runtime_operations_are_not_slowed_by_continuous_reads(sim_runtime: Any) -> None:
    rc = sim_runtime
    providers = make_providers(rc)
    stop = threading.Event()

    def hammer() -> None:
        while not stop.is_set():
            providers["status"]()
            providers["reconciliation_status"]()
            providers["account"]()
            providers["exposure"]()

    readers = [threading.Thread(target=hammer) for _ in range(4)]
    for t in readers:
        t.start()
    worst = 0.0
    try:
        for _ in range(200):
            started = time.monotonic()
            rc.reconciliation_engine.ambiguity_snapshot()   # stesso lock usato dalle letture
            with rc.table_manager.locked():
                pass
            worst = max(worst, time.monotonic() - started)
    finally:
        stop.set()
        for t in readers:
            t.join()
    # Soglia stabile sotto carico: un blocco significativo (attesa di un lock tenuto da una lettura
    # lunga) supererebbe di molto 0,5 s; le letture tengono il lock delle ambiguità solo per la copia.
    assert worst < 0.5  # 6: nessun blocco significativo del runtime


def test_read_latency_is_bounded(sim_runtime: Any) -> None:
    timings = []
    for _ in range(200):
        started = time.monotonic()
        status_view(sim_runtime)
        reconciliation_view(sim_runtime)
        timings.append(time.monotonic() - started)
    timings.sort()
    assert timings[int(len(timings) * 0.99) - 1] < 0.2  # p99, stabile anche sotto carico


def test_started_runtime_is_reported_as_reconciled_and_active(sim_runtime: Any) -> None:
    status = status_view(sim_runtime)
    recon = reconciliation_view(sim_runtime)
    assert status["runtime_state"] == "ACTIVE" and status["execution_mode"] == "SIM"
    assert recon["startup"] == {"state": "OK", "reason": None, "recovery_failure": None}
    # `_recovery_persist_failed` nasce solo alla prima scrittura dello stato di recovery: prima
    # il dato non esiste e la vista lo dichiara indisponibile invece di inventare False
    # (segnalato a Claude B: il core potrebbe inizializzarlo all'avvio).
    assert recon["recovery_state"] is None and recon["unavailable"] == ["recovery_state"]


def test_incoherent_core_snapshot_never_leaves_the_server(
    sim_runtime: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import control_api.bridge as bridge

    def incoherent(rc: Any, *, clock: Any = time.time) -> dict[str, Any]:
        view = status_view(rc, clock=clock)
        view["readiness"] = None                              # null non dichiarato
        return view

    monkeypatch.setattr(bridge, "status_view", incoherent)
    token_dir = tmp_path / "secure"
    token_dir.mkdir(mode=0o700)
    server = start_control_api(sim_runtime, port=0, token_path=token_dir / "t", git_sha="d" * 40,
                               allow_ephemeral_port=True)
    try:
        assert read_http(server.port, (token_dir / "t").read_text(), "/v1/status") == 500
        assert server.request_counts() == {"CONTRACT_SELF_CHECK": 1}  # 7
    finally:
        server.stop()
