"""Gate A — T2b batch b1, componente cashout_wiring (cabla_catena_cashout reale)."""
from __future__ import annotations

from typing import Any, Dict, List

import cashout_wiring
from cashout_executor import CashoutExecutor
from cashout_request_bridge import CashoutRequestBridge
from cashout_residual_handler import CashoutResidualHandler
from core.safety_layer import SafetyLayer
from core.trading_constants import (
    CASHOUT_FAILED,
    CASHOUT_SUCCESS,
    CMD_EXECUTE_CASHOUT,
    REQ_EXECUTE_CASHOUT,
)


class _Bus:
    def __init__(self) -> None:
        self.subs: Dict[str, List[Any]] = {}
        self.events: List[tuple] = []

    def subscribe(self, topic, cb):
        self.subs.setdefault(topic, []).append(cb)

    def publish(self, topic, payload=None):
        self.events.append((topic, payload))
        for cb in list(self.subs.get(topic, [])):
            cb(payload)


class _Service:
    def __init__(self, *, sim: bool = False, live_client: Any = None, sim_broker: Any = None) -> None:
        self.calls: List[str] = []
        self._sim, self._live, self._broker = sim, live_client, sim_broker

    def get_client(self):
        self.calls.append("get_client")
        return None

    def is_simulation_mode(self):
        self.calls.append("is_simulation_mode")
        return self._sim

    def get_live_client(self):
        self.calls.append("get_live_client")
        return self._live

    def get_simulation_broker(self):
        self.calls.append("get_simulation_broker")
        return self._broker

    def list_current_orders(self):
        self.calls.append("list_current_orders")
        return []


def _cabla(bus, svc, records, notes):
    return cashout_wiring.cabla_catena_cashout(
        bus=bus, betfair_service=svc, persist=records.append,
        notify=lambda text, *, severity="HIGH": notes.append((text, severity)))


def test_trg_628_b_1061():
    """ID: TRG-628-B-1061 / PF-B1-R2-WIRE-CASHOUT"""
    bus, svc = _Bus(), _Service()
    catena = _cabla(bus, svc, [], [])
    assert isinstance(catena.cashout_executor, CashoutExecutor)
    assert isinstance(catena.cashout_executor.safety_layer, SafetyLayer)
    assert catena.cashout_executor.order_router is catena.order_router
    assert catena.order_router.service is svc
    assert [cb.__self__ for cb in bus.subs[REQ_EXECUTE_CASHOUT]] == [catena.cashout_request_bridge]
    assert isinstance(catena.cashout_request_bridge, CashoutRequestBridge)
    assert [cb.__self__ for cb in bus.subs[CMD_EXECUTE_CASHOUT]] == [catena.cashout_executor]
    assert isinstance(catena.cashout_residual_handler, CashoutResidualHandler)
    assert [cb.__self__ for cb in bus.subs[CASHOUT_FAILED]] == [catena.cashout_residual_handler]
    assert [cb.__self__ for cb in bus.subs[CASHOUT_SUCCESS]] == [catena.cashout_residual_handler]


def test_trg_628_b_1062():
    """ID: TRG-628-B-1062 / PF-B1-R2-WIRE-CANCEL-LAZY"""
    for sim in (False, True):
        bus, svc = _Bus(), _Service(sim=sim)     # client LIVE / broker SIM assenti
        records: List[Dict[str, Any]] = []
        _cabla(bus, svc, records, [])
        assert svc.calls == []                     # nessun accesso al service al build
        bus.publish(CASHOUT_FAILED, {"status": "UNMATCHED", "bet_id": "H1", "market_id": "1.1"})
        expected = "get_simulation_broker" if sim else "get_live_client"
        assert svc.calls == ["is_simulation_mode", expected]   # risolto a cancel-time
        assert records[-1]["cancel_attempted"] is True
        assert records[-1]["cancel_ok"] is False   # client assente => non confermato
