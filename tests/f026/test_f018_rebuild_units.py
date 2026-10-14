"""PF-ALL-F018 — ricostruzione delle posizioni aperte al riavvio (codice reale; DB e exchange sono stub di dati).

Oracolo: stato persistente (righe ordine) + ordini correnti dell'exchange => tavoli, esposizione, chiavi
anti-duplicato e riserve incerte attesi. Nessun ingresso finche' la riconciliazione non e' completa.
"""
import json

import pytest

from core.duplication_guard import DuplicationGuard
from core.runtime_controller import RuntimeController
from core.system_state import RuntimeMode
from core.table_manager import TableManager


class _DB:
    def __init__(self, rows):
        self.rows = rows

    def _execute(self, sql, *a, **k):
        assert "FROM orders" in sql
        return [dict(r) for r in self.rows]


class _Exchange:
    def __init__(self, current=None, fail=False):
        self.current, self.fail, self.calls = current or [], fail, 0

    def list_current_orders(self, market_ids=None):
        self.calls += 1
        if self.fail:
            raise RuntimeError("NO_LIVE_CLIENT")
        return list(self.current)


def _row(ref, status, market, table_id, key, side="BACK", stake=3.0, price=2.0, bet_id=None):
    resp = {"instructionReports": [{"betId": bet_id, "status": "SUCCESS"}]} if bet_id else {}
    return {"customer_ref": ref, "status": status, "response_json": json.dumps(resp),
            "payload_json": json.dumps({"market_id": market, "selection_id": 11, "event_key": key, "table_id": table_id,
                                        "bet_type": side, "stake": stake, "price": price, "event_name": "A v B"})}


def _rc(rows, exchange, tables=5):
    rc = RuntimeController.__new__(RuntimeController)
    rc.db = _DB(rows)
    rc.betfair_service = exchange
    rc.table_manager = TableManager(table_count=tables)
    rc.duplication_guard = DuplicationGuard()
    return rc


def _tabs(rc):
    return {t["table_id"]: t for t in rc.table_manager.snapshot()}


def test_live_matched_order_rebuilds_its_table_exposure_and_dedupe_key():
    rc = _rc([_row("r1", "SUBMITTED", "1.1", 3, "K1", bet_id="B1"),
              _row("r2", "SUBMITTED", "1.2", 4, "K2", side="LAY", stake=4.0, price=2.5, bet_id="B2")],
             _Exchange([{"betId": "B1", "marketId": "1.1"}, {"betId": "B2", "marketId": "1.2"}]))
    res = rc._rebuild_open_positions()
    assert res["ok"] and sorted(res["rebuilt"]) == ["r1", "r2"] and not res["uncertain"]
    t = _tabs(rc)
    assert t[3]["current_event_key"] == "K1" and t[3]["current_exposure"] == pytest.approx(3.0)
    assert t[4]["current_event_key"] == "K2" and t[4]["current_exposure"] == pytest.approx(6.0)   # LAY: liability 4*1.5
    assert not rc.duplication_guard.acquire("K1") and not rc.duplication_guard.acquire("K2")


def test_submitted_order_absent_from_exchange_is_closed_and_takes_no_table():
    rc = _rc([_row("r1", "SUBMITTED", "1.1", 1, "K1", bet_id="B1")], _Exchange([]))
    res = rc._rebuild_open_positions()
    assert res["ok"] and res["closed"] == ["r1"]
    assert rc.table_manager.total_exposure() == 0.0
    assert rc.duplication_guard.acquire("K1")


@pytest.mark.parametrize("status", ["AMBIGUOUS", "INFLIGHT"])
def test_uncertain_order_is_held_as_uncertain_reservation(status):
    rc = _rc([_row("r1", status, "1.1", 2, "K1")], _Exchange([]))
    res = rc._rebuild_open_positions()
    assert res["ok"] and res["uncertain"] == ["r1"]
    assert _tabs(rc)[2]["current_event_key"] == "K1"
    assert rc._uncertain_reservations_map() == {"K1": 2}


def test_submitted_without_bet_id_on_live_market_is_uncertain_not_closed():
    rc = _rc([_row("r1", "SUBMITTED", "1.1", 1, "K1")], _Exchange([{"betId": "X", "marketId": "1.1"}]))
    res = rc._rebuild_open_positions()
    assert res["uncertain"] == ["r1"] and "K1" in rc._uncertain_reservations_map()


def test_exchange_unreadable_blocks_and_holds_every_candidate_as_uncertain():
    rc = _rc([_row("r1", "SUBMITTED", "1.1", 1, "K1", bet_id="B1"), _row("r2", "AMBIGUOUS", "1.2", 2, "K2")],
             _Exchange(fail=True))
    res = rc._rebuild_open_positions()
    assert res["ok"] is False and res["reason"] == "EXCHANGE_ORDERS_UNAVAILABLE:RuntimeError", "causa diagnosticabile"
    assert sorted(res["uncertain"]) == ["r1", "r2"], "un dato mancante non vale «posizione chiusa»"
    assert rc.table_manager.total_exposure() == pytest.approx(6.0)


def test_no_candidates_needs_no_exchange_call():
    ex = _Exchange(fail=True)
    rc = _rc([], ex)
    assert rc._rebuild_open_positions()["ok"] is True and ex.calls == 0


def test_preferred_table_taken_uses_another_free_table():
    rc = _rc([_row("r1", "SUBMITTED", "1.1", 1, "K1", bet_id="B1")], _Exchange([{"betId": "B1", "marketId": "1.1"}]))
    rc.table_manager.activate(table_id=1, event_key="OTHER", exposure=1.0, market_id="9.9", selection_id=1)
    res = rc._rebuild_open_positions()
    assert res["ok"] and [t for t in _tabs(rc).values() if t["current_event_key"] == "K1"][0]["table_id"] != 1


def test_more_open_positions_than_tables_blocks_entries():
    rows = [_row(f"r{i}", "AMBIGUOUS", f"1.{i}", i, f"K{i}") for i in range(1, 4)]
    rc = _rc(rows, _Exchange([]), tables=2)
    res = rc._rebuild_open_positions()
    assert res["ok"] is False and res["reason"] == "NO_TABLE_FOR_OPEN_POSITION"


def test_already_held_reservation_is_not_rebuilt_twice():
    rc = _rc([_row("r1", "AMBIGUOUS", "1.1", 1, "K1")], _Exchange([]))
    rc._rebuild_open_positions()
    res = rc._rebuild_open_positions()
    assert res["uncertain"] == [] and rc.table_manager.total_exposure() == pytest.approx(3.0)


def test_row_without_identity_is_reported_not_guessed():
    bad = _row("r1", "SUBMITTED", "", 1, "")
    rc = _rc([bad], _Exchange([]))
    res = rc._rebuild_open_positions()
    assert res["unattributed"] == ["r1"] and rc.table_manager.total_exposure() == 0.0


def test_resume_retries_reconciliation_and_refuses_while_incomplete():
    rc = _rc([_row("r1", "AMBIGUOUS", "1.1", 1, "K1")], _Exchange(fail=True))
    rc.mode = RuntimeMode.PAUSED
    rc._reconciliation_required = True
    rc._monitor_daily_loss_breach = lambda **k: {"breached": False}
    rc._loss_block_reason = lambda: ""
    rc._safe_status_snapshot = lambda: {}
    out = rc.resume()
    assert out["resumed"] is False and out["reason"].startswith("reconciliation_required")
    assert rc.mode is RuntimeMode.PAUSED
