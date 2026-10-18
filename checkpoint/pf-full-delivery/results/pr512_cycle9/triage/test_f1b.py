import os, sys
ROOT="/home/user/Pickfair-nogui"; sys.path.insert(0, ROOT); sys.path.insert(0, os.path.join(ROOT,"tests"))
from tests.acceptance.test_issue437_pr27 import _catena, _rifiuti
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _signal
from core import duplication_guard as dg_mod

def _events(c):
    ev = getattr(c.bus, "events", None) or getattr(c.bus, "published", None) or []
    return [(e, (p.get("reason") or p.get("status") or p.get("customer_ref")) if isinstance(p, dict) else p) for e, p in ev]

def _run(tmp_path, monkeypatch, retain):
    c = _catena(tmp_path)
    c.rc._on_signal_received(_signal())
    assert len(c.broker.state.orders) == 1, _rifiuti(c)
    key = c.rc.duplication_guard.build_event_key(_signal())
    n0 = len(getattr(c.bus, "events", []))
    if retain:
        c.rc._uncertain_reservations_map()[key] = 1
        c.rc.reset_cycle()
        assert c.rc.duplication_guard.is_duplicate(key)
    base = dg_mod.time.monotonic()
    monkeypatch.setattr(dg_mod.time, "monotonic", lambda: base + c.rc.duplication_guard.ttl + 1)
    assert not c.rc.duplication_guard.is_duplicate(key)
    c.rc._on_signal_received(_signal(price=2.1, received_at="2026-10-08T10:05:00.000000+00:00"))
    print("\nEVENTS after 2nd signal:", _events(c)[n0:])
    occ = [(t["table_id"], t["current_event_key"], t["current_exposure"]) for t in c.rc.table_manager.snapshot() if t.get("current_event_key")]
    refs = [o.customer_ref if hasattr(o,'customer_ref') else getattr(o,'customer_ref',None) for o in c.broker.state.orders.values()]
    print("ORDERS:", len(c.broker.state.orders), refs, "OCCUPIED:", occ, "total_exposure:", c.rc.table_manager.total_exposure())
    return c

def test_no_reset(tmp_path, monkeypatch):
    c = _run(tmp_path, monkeypatch, retain=False)
    assert len(c.broker.state.orders) == 1

def test_after_reset_retained(tmp_path, monkeypatch):
    c = _run(tmp_path, monkeypatch, retain=True)
    assert len(c.broker.state.orders) == 1
