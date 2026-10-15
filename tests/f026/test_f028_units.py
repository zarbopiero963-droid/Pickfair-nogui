"""PF-ALL-F028 — rilascio di un ordine mai abbinato: codice reale, servizio exchange stub con la semantica Betfair
dichiarata (listCurrentOrders / listClearedOrders per betStatus). Il ramo LIVE e' verificato solo con stub: la prova
contro Betfair reale e' MANUAL_ONLY.
"""
import json

import pytest

from core.duplication_guard import DuplicationGuard
from core.runtime_controller import RuntimeController
from core.system_state import RuntimeMode
from core.table_manager import TableManager


class _Bus:
    def __init__(self):
        self.events = []

    def publish(self, name, payload=None):
        self.events.append((name, dict(payload or {})))


class _DB:
    def __init__(self, rows):
        self.rows = rows

    def _execute(self, sql, *a, **k):
        return [dict(r) for r in self.rows]


def _row(ref, status, market, key, bet_id):
    resp = {"instructionReports": [{"betId": bet_id}]} if bet_id else {}
    return {"customer_ref": ref, "status": status, "response_json": json.dumps(resp),
            "payload_json": json.dumps({"market_id": market, "selection_id": 11, "event_key": key, "table_id": 1,
                                        "bet_type": "BACK", "stake": 3.0, "price": 2.0})}


class _Live:
    """listCurrentOrders + listClearedOrders(betStatus=...)."""

    def __init__(self, current=(), cleared=None, fail=False):
        self.current, self.cleared, self.fail = list(current), dict(cleared or {}), fail

    def list_current_orders(self, market_ids=None):
        if self.fail:
            raise RuntimeError("NO_LIVE_CLIENT")
        return [{"bet_id": b} for b in self.current]

    def list_cleared_orders(self, *, bet_status, bet_ids=None, **k):
        return [{"betId": b} for b in self.cleared.get(bet_status, []) if b in (bet_ids or [])]


def _rc(rows, service, mode=RuntimeMode.LOCKDOWN):
    rc = RuntimeController.__new__(RuntimeController)
    rc.mode = mode
    rc.simulation_mode = False
    rc.bus = _Bus()
    rc.db = _DB(rows)
    rc.betfair_service = service
    rc.table_manager = TableManager(table_count=2)
    rc.duplication_guard = DuplicationGuard()
    rc.table_manager.activate(table_id=1, event_key="K1", exposure=3.0, market_id="1.1", selection_id=11)
    rc.duplication_guard.acquire("K1")
    return rc


def test_live_lapsed_without_settled_part_is_released_keeping_dedupe_key():
    rc = _rc([_row("r1", "SUBMITTED", "1.1", "K1", "101")], _Live(cleared={"LAPSED": ["101"]}))
    assert rc._release_final_unmatched_orders() == ["r1"]
    assert rc.table_manager.total_exposure() == 0.0
    assert not rc.duplication_guard.acquire("K1"), "la chiave anti-duplicato resta: nessun replay"
    assert [n for n, _ in rc.bus.events] == ["UNMATCHED_ORDER_RELEASED"]


@pytest.mark.parametrize("service", [
    _Live(current=["101"], cleared={"CANCELLED": ["101"]}),          # ancora fra gli ordini correnti
    _Live(cleared={"CANCELLED": ["101"], "SETTLED": ["101"]}),       # parte abbinata regolata/da regolare
    _Live(cleared={}),                                                # nessuno stato finale confermato
    _Live(fail=True),                                                 # exchange illeggibile
])
def test_live_release_requires_confirmed_unmatched_final_state(service):
    rc = _rc([_row("r1", "SUBMITTED", "1.1", "K1", "101")], service)
    assert rc._release_final_unmatched_orders() == []
    assert rc.table_manager.total_exposure() == 3.0


@pytest.mark.parametrize("status", ["AMBIGUOUS", "INFLIGHT"])
def test_uncertain_outcome_is_never_released(status):
    rc = _rc([_row("r1", status, "1.1", "K1", "101")], _Live(cleared={"LAPSED": ["101"]}))
    assert rc._release_final_unmatched_orders() == [] and rc.table_manager.total_exposure() == 3.0


def test_uncertain_reservation_is_never_released():
    rc = _rc([_row("r1", "SUBMITTED", "1.1", "K1", "101")], _Live(cleared={"LAPSED": ["101"]}))
    rc._uncertain_reservations_map()["K1"] = 1
    assert rc._release_final_unmatched_orders() == []


def test_no_release_when_stopped_and_release_is_idempotent():
    rc = _rc([_row("r1", "SUBMITTED", "1.1", "K1", "101")], _Live(cleared={"LAPSED": ["101"]}),
             mode=RuntimeMode.STOPPED)
    assert rc._release_final_unmatched_orders() == []
    rc.mode = RuntimeMode.ACTIVE
    assert rc._release_final_unmatched_orders() == ["r1"]
    assert rc._release_final_unmatched_orders() == [], "secondo giro: nessun doppio rilascio"


def test_recovery_loss_survives_release():
    rc = _rc([], _Live())
    rc.table_manager.release(1, pnl=-4.0)
    rc.table_manager.activate(table_id=1, event_key="K2", exposure=2.0, market_id="1.2", selection_id=11)
    rc.db = _DB([_row("r2", "SUBMITTED", "1.2", "K2", "102")])
    rc.betfair_service = _Live(cleared={"CANCELLED": ["102"]})
    assert rc._release_final_unmatched_orders() == ["r2"]
    t = rc.table_manager.snapshot()[0]
    assert t["loss_amount"] == pytest.approx(4.0) and t["status"] == "RECOVERY"


def test_sim_partial_cancelled_stays_current_F031():
    from simulation_broker import SimulationBroker
    b = SimulationBroker(starting_balance=100.0)
    b.update_market_book({"marketId": "1.9", "status": "OPEN", "inplay": False, "runners": [
        {"selectionId": 11, "status": "ACTIVE", "ex": {"availableToBack": [{"price": 2.0, "size": 1.0}],
                                                       "availableToLay": [{"price": 2.02, "size": 100.0}]}}]})
    r = b.place_bet(market_id="1.9", selection_id=11, side="BACK", price=2.0, size=3.0, customer_ref="c1")
    bet = str(r.get("betId") or (r.get("instructionReports") or [{}])[0].get("betId"))
    b.cancel_orders(market_id="1.9")
    cur = {o["betId"]: o for o in b.get_current_orders(["1.9"])}
    assert bet in cur and cur[bet]["status"] == "EXECUTION_COMPLETE" and cur[bet]["sizeMatched"] == pytest.approx(1.0)
