"""Gate A — batch B, recovery: riconciliazione degli ordini attivi all'avvio (core/state_recovery.py).

Stub offline del motore di riconciliazione; si esercita `StateRecovery.recover()` reale."""
from core.state_recovery import StateRecovery


class _Bus:
    def __init__(self):
        self.events = []

    def publish(self, name, payload=None):
        self.events.append((name, payload))


class _Engine:
    def __init__(self, *, fetch=None, merge=None):
        if fetch is not None:
            self.fetch_startup_active_orders = fetch
        if merge is not None:
            self.merge_startup_active_orders = merge


def _boom(*_a, **_k):
    raise RuntimeError("exchange non raggiungibile")


def test_trg_628_b_0854():
    """ID: TRG-628-B-0854 / PF-B1-RECOVERY-REMOTE-ERROR

    Fetch remoto degli ordini attivi fallito => riconciliazione richiesta ma incompleta, avvio BLOCKED."""
    rec = StateRecovery(db=None, bus=_Bus(), reconciliation_engine=_Engine(fetch=_boom))
    startup = rec._reconcile_startup_ghost_orders()
    assert startup["required"] is True and startup["completed"] is False, startup
    out = rec.recover()
    assert out["ok"] is False, out
    assert out["reason"] == "STARTUP_RECONCILIATION_REQUIRED"
    assert out["startup_mode"] == "BLOCKED"


def test_trg_628_b_0855():
    """ID: TRG-628-B-0855 / PF-B1-RECOVERY-NO-REMOTE

    Senza fetch callable la riconciliazione startup e' dichiarata non richiesta: avvio LIVE_FLAT, ok."""
    for engine in (None, _Engine(), _Engine(fetch="non-callable")):
        rec = StateRecovery(db=None, bus=_Bus(), reconciliation_engine=engine)
        startup = rec._reconcile_startup_ghost_orders()
        assert startup == {"required": False, "completed": False, "ghost_orders_recovered": 0}, startup
        out = rec.recover()
        assert out["ok"] is True and out["reason"] == "RECOVERED", out
        assert out["startup_mode"] == "LIVE_FLAT"
        assert out["ghost_orders_recovered"] == 0


def test_trg_628_b_0894():
    """ID: TRG-628-B-0894 / PF-B1-RECOVERY-MERGE-ERROR

    Merge callback fallito dopo un fetch riuscito => startup incompleto (ok False, BLOCKED), nessun ghost persistito."""
    orders = [{"bet_id": "BET-1", "market_id": "1.1", "selection_id": 7}]
    rec = StateRecovery(db=None, bus=_Bus(), reconciliation_engine=_Engine(fetch=lambda: list(orders), merge=_boom))
    startup = rec._reconcile_startup_ghost_orders()
    assert startup["required"] is True and startup["completed"] is False, startup
    assert startup["ghost_orders_recovered"] == 0
    out = rec.recover()
    assert out["ok"] is False and out["reason"] == "STARTUP_RECONCILIATION_REQUIRED", out
    assert out["startup_mode"] == "BLOCKED"
    # controllo: stesso fetch con merge riuscito completa la riconciliazione
    ok = StateRecovery(db=None, bus=_Bus(), reconciliation_engine=_Engine(
        fetch=lambda: list(orders), merge=lambda o: {"orders": o}))._reconcile_startup_ghost_orders()
    assert ok["completed"] is True, ok
