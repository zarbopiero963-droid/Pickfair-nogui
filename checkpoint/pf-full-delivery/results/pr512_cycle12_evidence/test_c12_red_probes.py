"""Prove RED (ciclo 12 NON autorizzato): rilievi Codex P1 su 3d3f5cc. Non committate sul branch."""
import json, os, sqlite3, sys
ROOT = "/home/user/Pickfair-nogui"
sys.path.insert(0, ROOT); sys.path.insert(0, os.path.join(ROOT, "tests"))


def test_red_p1_sqlite_error_after_stub_send_is_never_failed():
    from test_microstake_engine_native import _engine, _payload, _orders
    engine, broker, db = _engine()
    real = db.save_settings
    def flaky(data):
        raw = next((v for k, v in data.items() if "saga" in str(k).lower()), None)
        if raw and '"STUB_ACCEPTED"' in raw:
            raise sqlite3.OperationalError("database is locked")
        return real(data)
    db.save_settings = flaky
    res = engine.submit_quick_bet(_payload("p1", size=0.10, price=3.0))
    live = [o for o in _orders(broker) if o.get("customerOrderRef") == "p1" or True]
    print("RESULT", json.dumps(res, default=str)[:600]); print("BROKER_ORDERS", live)
    assert live, "lo stub non e' stato inviato: probe non significativo"
    assert str(res.get("status")).upper() != "FAILED", f"stub vivo sul broker ma ordine FAILED: {res}"


def test_red_p1_emergency_settlement_source_survives_restart():
    from core import runtime_controller as m
    src = open(m.__file__).read()
    start = src.index("def _persist_emergency_state"); body = src[start:src.index("def ", start + 10)]
    reload_ = src[src.index("def _reload_persisted_emergency_state"):start]
    assert "settlement_source" in body and "settlement_source" in reload_, \
        "marker _settlement_source_before_emergency non persistito ne' ripristinato"
