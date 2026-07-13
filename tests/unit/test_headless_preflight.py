"""Test del preflight go-live headless (`--preflight`).

Il preflight e' read-only: valuta i prerequisiti LIVE via
RuntimeController.evaluate_live_readiness e stampa una checklist leggibile
(stdout + log), senza connettersi a Betfair ne' avviare il trading. Questi
test iniettano un runtime fake con una readiness controllata e verificano che
i blocker (con il relativo rimedio) finiscano a schermo e che l'exit code sia
0 (pronto) / 2 (non pronto).
"""

import pytest


class _FakeRuntimeReady:
    def __init__(self, **kwargs):
        self.calls = []

    def evaluate_live_readiness(self, *, execution_mode=None, live_enabled=None, live_readiness_ok=None):
        self.calls.append((execution_mode, live_enabled, live_readiness_ok))
        return {"ready": True, "level": "READY", "blockers": [], "details": {}}


class _FakeRuntimeBlocked:
    def __init__(self, **kwargs):
        self.calls = []

    def evaluate_live_readiness(self, *, execution_mode=None, live_enabled=None, live_readiness_ok=None):
        self.calls.append((execution_mode, live_enabled, live_readiness_ok))
        return {
            "ready": False,
            "level": "NOT_READY",
            "blockers": ["LIVE_READINESS_FLAG_NOT_OK", "LIVE_HARD_STOP_CONFIG_MISSING"],
            "details": {},
        }


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
    # I blocker devono comparire per CODICE...
    assert "LIVE_READINESS_FLAG_NOT_OK" in out
    assert "LIVE_HARD_STOP_CONFIG_MISSING" in out
    # ...e con il RIMEDIO (la checklist deve essere azionabile, non solo il codice)
    assert "live_readiness_ok=True" in out
    assert "hard-stop" in out
    # Il preflight ha davvero interrogato la readiness con i flag passati (--live)
    assert runtime.calls and runtime.calls[0][0] == "LIVE"


@pytest.mark.unit
def test_preflight_ready_returns_0(monkeypatch, capsys):
    app = _make_app(monkeypatch, _FakeRuntimeReady(), ["--live", "--live-enabled", "--preflight"])

    rc = app._run_preflight()

    out = capsys.readouterr().out
    assert rc == 0
    assert "PRONTO PER LIVE" in out


@pytest.mark.unit
def test_preflight_returns_2_when_runtime_missing(monkeypatch, capsys):
    app = _make_app(monkeypatch, None, ["--live", "--preflight"])
    app.runtime = None

    rc = app._run_preflight()

    out = capsys.readouterr().out
    assert rc == 2
    assert "Runtime non disponibile" in out
