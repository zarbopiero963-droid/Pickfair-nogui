"""Gate A T2b batch b4 — runtime: start gate LIVE, stop, pause, resume (core/runtime_controller.py).

RuntimeController non costruito (__new__), servizi stub che registrano le chiamate: nessuna rete, nessun ordine."""
from __future__ import annotations

import types

import pytest

from core.runtime_controller import RuntimeController, RuntimeMode


class _Bus:
    def __init__(self):
        self.events = []

    def publish(self, name, payload=None):
        self.events.append((name, payload))


def _names(rc):
    return [n for n, _ in rc.bus.events]


def _base():
    rc = RuntimeController.__new__(RuntimeController)
    rc.bus = _Bus()
    rc.calls = []
    rc.get_status = lambda: {"mode": getattr(rc.mode, "value", None)}
    rc._safe_status_snapshot = rc.get_status
    rc._blackbox_gate_a = lambda component: None
    return rc


def test_trg_628_b_0738():
    """ID: TRG-628-B-0738 / PF-B1-RUNTIME-START-GATE

    Avvio LIVE con deploy gate (boot=True) che non consente: rifiuto deploy_gate_no_go, LIVE_EXECUTION_REFUSED
    pubblicato, nessun connect Betfair, nessun avvio Telegram, execution_mode effettivo SIMULATION."""
    rc = _base()
    rc.mode = RuntimeMode.STOPPED
    rc.reload_config = lambda **k: None
    rc.risk_desk = types.SimpleNamespace(realized_pnl=0.0)
    rc.reset_risk_stop = lambda **k: {"risk_stop_reset": True}
    rc._monitor_daily_loss_breach = lambda **k: {"breached": False}
    rc.set_simulation_mode = lambda v: rc.calls.append(("sim", v))
    gate_calls = []

    def _gate(**kw):
        gate_calls.append(kw)
        return {"allowed": False, "reason": "DEPLOY_BLOCKED", "effective_execution_mode": "SIMULATION",
                "details": {"readiness_payload": {"ready": False}}}

    rc.enforce_deploy_gate = _gate
    rc.betfair_service = types.SimpleNamespace(connect=lambda **k: rc.calls.append("connect"))
    rc.telegram_service = types.SimpleNamespace(start=lambda: rc.calls.append("telegram"))
    out = rc._start_with_instance_lock(execution_mode="LIVE", live_enabled=True, live_readiness_ok=True)
    assert out["started"] is False and out["refused"] is True and out["reason"] == "deploy_gate_no_go"
    assert gate_calls and gate_calls[0]["boot"] is True and gate_calls[0]["execution_mode"] == "LIVE"
    assert "connect" not in rc.calls and "telegram" not in rc.calls
    assert "LIVE_EXECUTION_REFUSED" in _names(rc)
    assert rc.execution_mode == "SIMULATION"


def _stoppable(fail_telegram=False):
    rc = _base()
    rc.mode = RuntimeMode.ACTIVE
    order = []
    rc._stop_settlement_poller = lambda: order.append(("poller", rc.mode))
    rc._stop_market_data_feed = lambda: order.append(("feed", rc.mode))

    def _tg_stop():
        order.append(("telegram", rc.mode))
        if fail_telegram:
            raise RuntimeError("tg")

    rc.telegram_service = types.SimpleNamespace(stop=_tg_stop)
    rc.betfair_service = types.SimpleNamespace(disconnect=lambda: order.append(("disconnect", rc.mode)))
    rc._release_runtime_instance_lock_if_stopped = lambda: order.append(("release", rc.mode))
    return rc, order


def test_trg_628_b_0740():
    """ID: TRG-628-B-0740 / PF-B1-RUNTIME-STOP-EARLY

    stop(): il modo e' STOPPED gia' prima del primo smontaggio; Betfair si disconnette anche se uno smontaggio
    solleva (finally) e in quel caso il teardown resta pendente (lock non considerato liberabile)."""
    rc, order = _stoppable()
    out = rc._stop_serialized()
    assert out["stopped"] is True
    assert all(mode is RuntimeMode.STOPPED for _, mode in order), order
    assert [n for n, _ in order][:4] == ["poller", "feed", "telegram", "disconnect"]
    assert rc._instance_teardown_pending is False and "RUNTIME_STOPPED" in _names(rc)

    rc, order = _stoppable(fail_telegram=True)
    with pytest.raises(RuntimeError):
        rc._stop_serialized()
    assert ("disconnect", RuntimeMode.STOPPED) in order
    assert rc._instance_teardown_pending is True and rc.mode is RuntimeMode.STOPPED


@pytest.mark.xfail(strict=True, reason="TRG-628-B-0740: STOP (pulsante STOP BOT -> runtime.stop) smonta e disconnette "
                                       "senza cancel degli unmatched pertinenti ne' tentativo di cashout, contro il "
                                       "contratto P38 STOP/RISK_STOP (mcp_operational_contract §Contratti prodotto vigenti)")
def test_trg_628_b_0740_p38_stop_risk_reduction():
    """ID: TRG-628-B-0740 / PF-B1-RUNTIME-STOP-EARLY

    Contratto ratificato P38: STOP = blocca nuovi ordini + cancella unmatched pertinenti + tenta cashout via
    authority Pickfair, prima di disconnettere Betfair."""
    rc, order = _stoppable()
    rc.risk_stop = lambda reason: order.append(("risk_stop", rc.mode)) or {"risk_stopped": True}
    rc._route_cashout_signal = lambda sig: order.append(("cashout", rc.mode))
    rc.betfair_service.cancel_orders = lambda **k: order.append(("cancel", rc.mode))
    rc._stop_serialized()
    names = [n for n, _ in order]
    reduction = [i for i, n in enumerate(names) if n in ("risk_stop", "cashout", "cancel")]
    assert reduction and reduction[0] < names.index("disconnect"), names


def test_trg_628_b_0741():
    """ID: TRG-628-B-0741 / PF-B1-RUNTIME-PAUSE

    pause(): modo PAUSED e RUNTIME_PAUSED pubblicato con lo stato; nessun cancel/cashout (PAUSE blocca solo
    gli ingressi, P38)."""
    rc = _base()
    rc.mode = RuntimeMode.ACTIVE
    rc.betfair_service = types.SimpleNamespace(cancel_orders=lambda **k: rc.calls.append("cancel"))
    rc._route_cashout_signal = lambda s: rc.calls.append("cashout")
    out = rc.pause()
    assert out["paused"] is True and rc.mode is RuntimeMode.PAUSED
    assert rc.bus.events == [("RUNTIME_PAUSED", {"mode": RuntimeMode.PAUSED.value})]
    assert rc.calls == []


def _paused(**over):
    rc = _base()
    rc.mode = RuntimeMode.PAUSED
    rc._reconciliation_required = False
    rc._monitor_daily_loss_breach = lambda **k: {"breached": False}
    rc._enforce_daily_loss_hard_stop = lambda *_a, **_k: rc.calls.append("hard_stop")
    rc._loss_block_reason = lambda: ""
    for k, v in over.items():
        setattr(rc, k, v)
    return rc


def test_trg_628_b_0742():
    """ID: TRG-628-B-0742 / PF-B1-RUNTIME-RESUME-BLOCK

    resume() rifiutato con LOCKDOWN (resta LOCKDOWN), con perdita giornaliera sfondata (hard stop applicato)
    o con blocco di perdita sessione; in tutti i casi nessun RUNTIME_RESUMED e nessun ACTIVE."""
    rc = _paused()
    rc.mode = RuntimeMode.LOCKDOWN
    out = rc.resume()
    assert out["resumed"] is False and out["reason"] == "lockdown_attivo"
    assert rc.mode is RuntimeMode.LOCKDOWN and "RUNTIME_RESUMED" not in _names(rc)

    rc = _paused(_monitor_daily_loss_breach=lambda **k: {"breached": True})
    out = rc.resume()
    assert out["resumed"] is False and out["reason"] == "daily_loss_breached" and rc.calls == ["hard_stop"]
    assert rc.mode is RuntimeMode.PAUSED and "RUNTIME_RESUMED" not in _names(rc)

    rc = _paused(_loss_block_reason=lambda: "session_loss_limit")
    out = rc.resume()
    assert out["resumed"] is False and out["reason"] == "session_loss_limit"
    assert rc.mode is RuntimeMode.PAUSED and "RUNTIME_RESUMED" not in _names(rc)

    ok = _paused()
    assert ok.resume()["resumed"] is True and ok.mode is RuntimeMode.ACTIVE
