"""Test del preflight go-live headless (`--preflight`).

Il preflight e' read-only: valuta i prerequisiti LIVE con lo STESSO gate che
usa start() (RuntimeController.get_deploy_gate_status) e stampa una checklist
leggibile (stdout + log), senza connettersi a Betfair, senza avviare il trading
e senza avviare i servizi di osservabilita' (build start_services=False).
Exit code: 0 pronto per LIVE, 2 non pronto, 1 se il runtime non e' costruito.
"""

import pytest


def _status(allowed, blockers, *, ready=None, level=None, probe_ok=True, probe_reason="", reasons=None):
    if ready is None:
        ready = allowed
    if level is None:
        level = "READY" if allowed else "NOT_READY"
    return {
        "allowed": allowed,
        "reasons": reasons or (["DEPLOY_GO_READY"] if allowed else ["DEPLOY_BLOCKED_NOT_READY"]),
        "readiness": level,
        "details": {
            "gate_reason_code": "" if allowed else "not_ready",
            "readiness_payload": {
                "ready": ready,
                "level": level,
                "blockers": list(blockers),
                "details": {"probe": {"ok": probe_ok, "reason": probe_reason}},
            },
        },
    }


class _FakeRuntimeReady:
    def __init__(self, **kwargs):
        self.calls = []

    def get_deploy_gate_status(self, *, execution_mode=None, live_enabled=None, live_readiness_ok=None):
        self.calls.append((execution_mode, live_enabled, live_readiness_ok))
        return _status(True, [])


class _FakeRuntimeBlocked:
    def __init__(self, **kwargs):
        self.calls = []

    def get_deploy_gate_status(self, *, execution_mode=None, live_enabled=None, live_readiness_ok=None):
        self.calls.append((execution_mode, live_enabled, live_readiness_ok))
        return _status(False, ["LIVE_READINESS_FLAG_NOT_OK", "LIVE_HARD_STOP_CONFIG_MISSING"])


def _make_app(monkeypatch, runtime, argv):
    import headless_main

    monkeypatch.setattr(headless_main.sys, "argv", ["headless_main.py", *argv])
    app = headless_main.HeadlessApp()  # __init__ e' leggero (solo attributi None)
    app.runtime = runtime
    return app


@pytest.mark.unit
def test_preflight_requested_detects_flag(monkeypatch):
    app = _make_app(monkeypatch, _FakeRuntimeReady(), ["--live", "--preflight"])
    assert app._preflight_requested() is True

    app2 = _make_app(monkeypatch, _FakeRuntimeReady(), ["--live"])
    assert app2._preflight_requested() is False


@pytest.mark.unit
def test_preflight_blocked_prints_checklist_and_returns_2(monkeypatch, capsys):
    runtime = _FakeRuntimeBlocked()
    app = _make_app(monkeypatch, runtime, ["--live", "--live-enabled", "--preflight"])

    rc = app._run_preflight()

    out = capsys.readouterr().out
    assert rc == 2
    assert "NON PRONTO" in out
    assert "PRONTO PER LIVE" not in out
    # Blocker per CODICE + RIMEDIO (checklist azionabile)
    assert "LIVE_READINESS_FLAG_NOT_OK" in out
    assert "LIVE_HARD_STOP_CONFIG_MISSING" in out
    assert "live_readiness_ok=True" in out
    assert "hard-stop" in out
    # Ha interrogato il gate reale con i flag passati (--live)
    assert runtime.calls and runtime.calls[0][0] == "LIVE"


@pytest.mark.unit
def test_preflight_ready_returns_0(monkeypatch, capsys):
    app = _make_app(monkeypatch, _FakeRuntimeReady(), ["--live", "--live-enabled", "--preflight"])

    rc = app._run_preflight()

    out = capsys.readouterr().out
    assert rc == 0
    assert "PRONTO PER LIVE" in out


@pytest.mark.unit
def test_preflight_non_live_does_not_claim_ready(monkeypatch, capsys):
    # CodeRabbit: senza --live (SIMULATION) NON deve stampare "PRONTO PER LIVE"
    # (sarebbe fuorviante). Mostra la nota non-LIVE ed esce 0 (nulla da bloccare).
    app = _make_app(monkeypatch, _FakeRuntimeReady(), ["--preflight"])

    rc = app._run_preflight()

    out = capsys.readouterr().out
    assert rc == 0
    assert "PRONTO PER LIVE" not in out
    assert "NON e' LIVE" in out


@pytest.mark.unit
def test_preflight_returns_1_when_runtime_missing(monkeypatch, capsys):
    app = _make_app(monkeypatch, None, ["--live", "--preflight"])
    app.runtime = None

    rc = app._run_preflight()

    out = capsys.readouterr().out
    assert rc == 1  # errore di bootstrap/build
    assert "Runtime non disponibile" in out


@pytest.mark.unit
def test_start_preflight_intercepts_before_services_and_recovery(monkeypatch):
    # Codacy: start() con --preflight deve costruire SENZA avviare i servizi
    # (start_services=False), NON eseguire boot recovery e NON avviare il trading.
    import headless_main

    monkeypatch.setattr(
        headless_main.sys, "argv", ["headless_main.py", "--live", "--live-enabled", "--preflight"]
    )
    app = headless_main.HeadlessApp()
    calls = {"build_start_services": None, "boot_recovery": False}

    def fake_build(start_services=True):
        calls["build_start_services"] = start_services
        app.runtime = _FakeRuntimeBlocked()

    monkeypatch.setattr(app, "build", fake_build)
    monkeypatch.setattr(app, "_run_boot_recovery", lambda: calls.__setitem__("boot_recovery", True))

    rc = app.start()

    assert calls["build_start_services"] is False  # preflight NON avvia i servizi
    assert calls["boot_recovery"] is False          # niente recovery / trading
    assert rc == 2                                   # runtime bloccato -> non pronto
