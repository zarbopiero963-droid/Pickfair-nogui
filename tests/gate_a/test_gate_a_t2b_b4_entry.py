"""Gate A T2b batch b4 — entry: exit code del GO-LIVE preflight (headless_main.HeadlessApp).

HeadlessApp non costruito (__new__), runtime stub: nessuna connessione Betfair, nessun ordine."""
from __future__ import annotations

from headless_main import HeadlessApp


class _Runtime:
    def __init__(self, status):
        self.status = status
        self.calls = []

    def get_deploy_gate_status(self, **kw):
        self.calls.append(kw)
        return self.status


def _app(args, runtime):
    app = HeadlessApp.__new__(HeadlessApp)
    app._parse_args = lambda: dict(args)
    app.runtime = runtime
    return app


def test_trg_628_b_0723(capsys):
    """ID: TRG-628-B-0723 / PF-B1-PREFLIGHT-EXIT

    Exit 3 se non e' richiesto LIVE (anche con gate 'allowed'), 0 solo con LIVE e gate allowed, 2 con LIVE
    negato (anche senza blocker granulari), 1 senza runtime. Il gate e' interrogato con boot=True."""
    ok = {"allowed": True, "details": {"readiness_payload": {"level": "READY", "details": {"probe": {"ok": True}}}}}
    no = {"allowed": False, "reasons": ["DEPLOY_BLOCKED_INVALID_STATE"], "details": {}}

    rt = _Runtime(ok)
    assert _app({"execution_mode": "SIMULATION"}, rt)._run_preflight() == 3
    assert rt.calls[-1]["boot"] is True
    assert _app({"execution_mode": "LIVE", "live_enabled": True, "live_readiness_ok": True},
                _Runtime(ok))._run_preflight() == 0
    assert _app({"execution_mode": "LIVE", "live_enabled": True}, _Runtime(no))._run_preflight() == 2
    # allowed assente => mai GO (fail-closed), anche senza blocker
    assert _app({"execution_mode": "LIVE"}, _Runtime({"details": {}}))._run_preflight() == 2
    assert _app({"execution_mode": "LIVE"}, None)._run_preflight() == 1
    capsys.readouterr()
