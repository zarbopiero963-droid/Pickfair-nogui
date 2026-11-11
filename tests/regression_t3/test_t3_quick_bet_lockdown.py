"""Claude D (disposizione owner PF-OWNER-GATEA-FIVE-DECISIONS-20261111, «system_lockdown»): con il kill switch in
LOCKDOWN nessuna puntata rapida raggiunge il broker, nessuna scrittura d'ordine, esito DENIED system_lockdown.
ID: T3-LOCKDOWN-QUICKBET (core/trading_engine.py submit_quick_bet, Hard Block Kill Switch #284)."""
import importlib

_base = importlib.import_module("tests.unit.test_trading_engine_safe_risk")


class _Lockdown:
    def is_enabled(self):
        return False

    def is_ready(self):
        return True

    def is_lockdown(self):
        return True


class _SpyClient:
    def __init__(self):
        self.calls = []

    def __getattr__(self, name):
        def _record(*a, **k):
            self.calls.append(name)
            return {"ok": True, "bet_id": "SHOULD-NOT-HAPPEN"}
        return _record


def _engine(client, db):
    from core.trading_engine import TradingEngine
    return TradingEngine(bus=_base.FakeBus(), db=db, client_getter=lambda: client,
                         executor=_base.InlineExecutor(), safe_mode=_Lockdown())


def test_t3_lockdown_quick_bet_reaches_no_broker():
    """ID: T3-LOCKDOWN-QUICKBET"""
    client, db = _SpyClient(), _base.FakeDB()
    getter_calls = []
    engine = _engine(client, db)
    engine.client_getter = lambda: (getter_calls.append(1), client)[1]
    for side in ("BACK", "LAY"):
        r = engine.submit_quick_bet({"market_id": "1.400", "selection_id": 44, "price": 2.0, "stake": 5.0,
                                     "side": side, "customer_ref": f"LD-{side}"})
        assert r["status"] == "DENIED" and r["reason"] == "system_lockdown", r
    assert client.calls == [] and getter_calls == []
    assert not getattr(db, "orders", None)
