"""F-05 (CSH-06) — unita': write-ahead dell'hedge, esiti, fail-closed, identita' I1 e nessun secondo hedge.

PASS + BLOCK: ogni test fallisce se l'hedge torna invisibile al ledger o se la riga viene scambiata per un ingresso.
"""
from __future__ import annotations

import json

import pytest

from cashout_executor import CASHOUT_FAILED, CASHOUT_SUCCESS, CashoutExecutor
from cashout_resolver import reconstruct_open_positions
from core import hedge_ref as H
from database import Database

pytestmark = [pytest.mark.unit, pytest.mark.safety]


class _Bus:
    def __init__(self):
        self.published = []

    def subscribe(self, *_a):
        pass

    def publish(self, topic, payload):
        self.published.append((topic, payload))

    def topics(self):
        return [t for t, _ in self.published]


class _Router:
    """Registra lo stato del ledger AL MOMENTO dell'invio: la riga deve esistere prima."""

    def __init__(self, db, result=None, raises=None):
        self.db, self.result, self.raises, self.calls, self.rows_at_place = db, result, raises, [], None

    def place(self, payload):
        self.calls.append(dict(payload))
        self.rows_at_place = _rows(self.db) if self.db is not None else None
        if self.raises:
            raise self.raises
        return self.result


def _rows(db):
    return [dict(r) for r in db._execute("SELECT customer_ref, status, payload_json, response_json FROM orders",
                                         fetch=True, commit=False) or []]


def _cmd(**over):
    p = {"market_id": "1.222", "selection_id": 55, "side": "LAY", "stake": 4.0, "price": 1.8, "green_up": 2.5,
         "event_key": "1.222:55:BACK:x", "source": "UI", "cashout_request_id": "req-1"}
    p.update(over)
    return p


def _ok(matched=4.0, bet_id="CB1"):
    return {"ok": True, "placed": True, "order_unknown": False, "status": "SUCCESS", "matched": matched,
            "bet_id": bet_id, "error": None}


@pytest.fixture()
def db(tmp_path):
    return Database(str(tmp_path / "f05.db"))


def _run(db, result=None, raises=None, **cmd):
    bus, router = _Bus(), _Router(db, result, raises)
    CashoutExecutor(bus, router, ledger=db).on_cmd_execute_cashout(_cmd(**cmd))
    return bus, router


def test_hedge_row_is_written_before_the_order_is_sent(db):
    bus, router = _run(db, _ok())
    assert len(router.rows_at_place) == 1 and router.rows_at_place[0]["status"] == H.HEDGE_PENDING
    ref = router.rows_at_place[0]["customer_ref"]
    assert H.is_hedge_order_ref(ref) and len(ref) <= 32 and router.calls[0]["customer_ref"] == ref
    row = _rows(db)[0]
    p = json.loads(row["payload_json"])
    assert row["status"] == H.HEDGE_PLACED
    assert p["role"] == H.HEDGE_ROLE and p["order_origin"] == H.HEDGE_ORIGIN and p["cashout_request_id"] == "req-1"
    assert "event_key" not in p and "table_id" not in p and "batch_id" not in p
    assert (p["market_id"], p["selection_id"], p["bet_type"], p["price"], p["stake"]) == ("1.222", 55, "LAY", 1.8, 4.0)
    assert json.loads(row["response_json"])["bet_id"] == "CB1"
    assert CASHOUT_SUCCESS in bus.topics()


@pytest.mark.parametrize("result,raises,status,event", [
    ({"placed": True, "matched": 0.0, "bet_id": "R1"}, None, H.HEDGE_UNMATCHED, CASHOUT_FAILED),
    ({"placed": False, "error": "INSUFFICIENT_FUNDS"}, None, H.HEDGE_FAILED, CASHOUT_FAILED),
    ({"placed": True, "order_unknown": True, "matched": 0.0}, None, H.HEDGE_AMBIGUOUS, CASHOUT_FAILED),
    (None, TimeoutError("lost"), H.HEDGE_AMBIGUOUS, CASHOUT_FAILED),
    (_ok(matched=1.5), None, H.HEDGE_PLACED, CASHOUT_SUCCESS),
])
def test_every_outcome_is_recorded_on_the_same_row(db, result, raises, status, event):
    bus, router = _run(db, result, raises)
    rows = _rows(db)
    assert len(rows) == 1 and rows[0]["status"] == status, rows
    assert len(router.calls) == 1, "mai retry"
    assert event in bus.topics()


def test_ledger_not_writable_means_no_hedge_is_sent(db, monkeypatch):
    def boom(_payload):
        raise RuntimeError("disk full")
    monkeypatch.setattr(db, "insert_order", boom)
    bus, router = _run(db, _ok())
    assert router.calls == [], "hedge inviato senza traccia nel ledger"
    failed = [p for t, p in bus.published if t == CASHOUT_FAILED]
    assert failed and failed[0]["status"] == "REJECTED" and "ledger_write_failed" in failed[0]["reason"]


def test_outcome_update_failure_keeps_the_write_ahead_row_and_the_real_outcome(db, monkeypatch):
    def boom(*_a, **_k):
        raise RuntimeError("locked")
    monkeypatch.setattr(db, "update_order", boom)
    bus, _router = _run(db, _ok())
    assert [r["status"] for r in _rows(db)] == [H.HEDGE_PENDING], "la traccia write-ahead deve restare"
    assert CASHOUT_SUCCESS in bus.topics(), "l'hedge e' sull'exchange: l'esito non si nasconde"


def test_hedge_rows_are_not_open_orders_of_the_bot(db):
    _run(db, _ok())
    assert db.load_pending_customer_refs() == [] and db.load_pending_correlation_ids() == []
    ref = _rows(db)[0]["customer_ref"]
    assert not db.order_exists_inflight(customer_ref=ref)


def test_hedge_bet_is_part_of_the_bot_identity_and_makes_the_position_mixed(db):
    """LIVE: la bet dell'hedge entra nell'identita' I1 (daily-loss e settlement la vedono). Con l'ingresso BACK e
    l'hedge LAY sulla stessa selezione la posizione e' MISTA: il router non piazza un secondo hedge."""
    db.insert_order({"customer_ref": "sig-entry", "correlation_id": "c-entry", "status": "SUBMITTED",
                     "payload": {"market_id": "1.222", "selection_id": 55, "bet_type": "BACK", "price": 2.0,
                                 "stake": 3.0, "event_name": "A v B", "event_key": "1.222:55:BACK:x"},
                     "response": {"bet_id": "E1"}})
    _run(db, _ok(matched=3.33, bet_id="CB1"))
    bot = {o["bet_id"] for o in db.get_bot_active_orders()}
    assert {"E1", "CB1"} <= bot
    current = [{"betId": "E1", "marketId": "1.222", "selectionId": 55, "side": "BACK", "sizeMatched": 3.0,
                "averagePriceMatched": 2.0, "sizeRemaining": 0.0, "priceSize": {"price": 2.0, "size": 3.0}},
               {"betId": "CB1", "marketId": "1.222", "selectionId": 55, "side": "LAY", "sizeMatched": 3.33,
                "averagePriceMatched": 1.8, "sizeRemaining": 0.0, "priceSize": {"price": 1.8, "size": 3.33}}]
    positions = reconstruct_open_positions(current, is_bot_order=lambda r: r.get("betId") in bot)
    assert positions == [], f"posizione hedgiata riaperta al cashout: {positions}"
