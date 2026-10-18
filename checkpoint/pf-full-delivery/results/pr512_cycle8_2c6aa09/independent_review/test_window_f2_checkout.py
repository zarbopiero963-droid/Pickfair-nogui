"""Throwaway (review only): crash between coordinator._activate (saves ACTIVE) and executor demotion."""
import os
import sys

WT = "/home/user/Pickfair-nogui"
sys.path.insert(0, WT)
sys.path.insert(0, os.path.join(WT, "tests"))
os.chdir(WT)

import pytest  # noqa: E402

from test_microstake_engine_native import _engine, _payload  # noqa: E402
from core.microstake_sim_port import NativeSimMicrostakeExecutor  # noqa: E402
from simulation_broker import SimulationBroker  # noqa: E402


class _Crash(BaseException):
    pass


def test_crash_between_activate_and_demotion_leaves_active_with_stale_reload_source(monkeypatch):
    engine, broker, _ = _engine()
    engine._default_native_microstake_executor("SIMULATION")
    ex = engine.microstake_executor
    db = engine.db
    broker.persist_snapshot = lambda: False          # nessuno snapshot mai persistito (fonte di reload stale)

    def crash(self, payload, out):
        raise _Crash()
    monkeypatch.setattr(NativeSimMicrostakeExecutor, "_demote_unpersisted_active", crash)
    try:
        res = ex.execute(_payload("w1", size=0.10, price=3.0)); print("no crash (demotion not reached), result:", res.get("status"), res.get("order_unknown"))
    except _Crash:
        print("CRASH reached demotion")
    saga = ex.store.load("w1")
    print("saga after crash:", saga["phase"], "held:", saga.get("reservation_held"))
    print("pending_refs:", ex.store.pending_refs())
    print("reload source (db.simulation_state):", db.simulation_state)
    # "riavvio": broker ricaricato dalla fonte di reload (vuota/stale) + recover
    assert saga["phase"] == "AMBIGUOUS" and saga["reservation_held"] is True
    assert "w1" in ex.store.pending_refs() and ex.port.has_reservation("w1")
