"""Gate A — batch B, runtime: resume() riattiva e pubblica RUNTIME_RESUMED solo dopo i gate
(core/runtime_controller.py). RuntimeController non avviato, dipendenze stub, nessuna rete."""
from core.runtime_controller import RuntimeController, RuntimeMode


class _Bus:
    def __init__(self):
        self.events = []

    def publish(self, name, payload=None):
        self.events.append((name, payload))


def _paused(**over):
    rc = RuntimeController.__new__(RuntimeController)
    rc.mode = RuntimeMode.PAUSED
    rc.bus = _Bus()
    rc._reconciliation_required = False
    rc._monitor_daily_loss_breach = lambda **k: {"breached": False}
    rc._enforce_daily_loss_hard_stop = lambda *_a, **_k: None
    rc._loss_block_reason = lambda: ""
    rc._recover_native_microstake = lambda: {"ok": True, "status": "RECONCILED", "pending": []}
    rc._rebuild_open_positions = lambda: {"ok": True, "reason": "", "rebuilt": [], "uncertain": [], "held": []}
    rc.get_status = lambda: {"mode": rc.mode.value}
    rc._safe_status_snapshot = lambda: {"mode": rc.mode.value}
    for k, v in over.items():
        setattr(rc, k, v)
    return rc


def _names(rc):
    return [n for n, _ in rc.bus.events]


def test_trg_628_b_0743():
    """ID: TRG-628-B-0743 / PF-B1-RUNTIME-RESUME

    Superati i gate, resume() porta il runtime in ACTIVE e pubblica RUNTIME_RESUMED con lo stato; un gate che
    blocca (perdita giornaliera, blocco sessione, riconciliazione incompleta) non riattiva e non pubblica."""
    rc = _paused()
    out = rc.resume()
    assert out["resumed"] is True and rc.mode is RuntimeMode.ACTIVE, out
    assert ("RUNTIME_RESUMED", {"mode": RuntimeMode.ACTIVE.value}) in rc.bus.events

    blocked = [
        _paused(_monitor_daily_loss_breach=lambda **k: {"breached": True}),
        _paused(_loss_block_reason=lambda: "session_loss_limit"),
        _paused(_reconciliation_required=True,
                _rebuild_open_positions=lambda: {"ok": False, "reason": "UNCERTAIN", "rebuilt": [],
                                                 "uncertain": ["x"], "held": []}),
    ]
    for rc in blocked:
        out = rc.resume()
        assert out["resumed"] is False, out
        assert rc.mode is RuntimeMode.PAUSED
        assert "RUNTIME_RESUMED" not in _names(rc)
