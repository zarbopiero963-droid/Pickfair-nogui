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


# ------------------------------------------------------------------ idempotenza e riavvio (F-05, owner 31/10/2026)

def test_same_cashout_request_never_hedges_twice(db):
    bus, router = _run(db, _ok())
    bus2, router2 = _Bus(), _Router(db, _ok(bet_id="CB2"))
    CashoutExecutor(bus2, router2, ledger=db).on_cmd_execute_cashout(_cmd())
    assert router2.calls == [], "secondo hedge per lo stesso comando"
    failed = [p for t, p in bus2.published if t == CASHOUT_FAILED]
    assert failed[0]["reason"] == "duplicate_cashout_request" and failed[0]["status"] == "REJECTED"
    assert len(_rows(db)) == 1


def test_failed_hedge_can_be_retried_with_the_same_request(db):
    _run(db, {"placed": False, "error": "MARKET_SUSPENDED"})
    bus, router = _run(db, _ok())
    assert len(router.calls) == 1 and CASHOUT_SUCCESS in bus.topics(), "un hedge mai piazzato non blocca"


def test_uncertain_hedge_blocks_a_new_hedge_on_the_same_selection(db):
    _run(db, {"placed": True, "order_unknown": True, "matched": 0.0})        # HEDGE_AMBIGUOUS
    bus, router = _run(db, _ok(), cashout_request_id="req-2")
    assert router.calls == [] and [p["reason"] for t, p in bus.published if t == CASHOUT_FAILED] == [
        "hedge_outcome_uncertain"]
    bus3, router3 = _run(db, _ok(), cashout_request_id="req-3", selection_id=77)   # altra selezione: consentito
    assert len(router3.calls) == 1


def test_unreadable_ledger_means_no_hedge(db, monkeypatch):
    def boom(*_a, **_k):
        raise RuntimeError("database is locked")
    monkeypatch.setattr(db, "_execute", boom)
    bus, router = _run(db, _ok())
    assert router.calls == [] and [p["reason"] for t, p in bus.published if t == CASHOUT_FAILED] == [
        "ledger_read_failed"]


def test_crash_between_write_ahead_and_outcome_is_ambiguous_after_restart(db):
    """Crash simulato: la riga write-ahead resta HEDGE_PENDING (l'hedge PUO' essere sull'exchange). Al riavvio
    l'executor la marca HEDGE_AMBIGUOUS; un nuovo cashout della stessa selezione e' rifiutato (mai retry)."""
    class _CrashRouter(_Router):
        def place(self, payload):
            raise SystemExit("processo ucciso durante l'invio")

    with pytest.raises(SystemExit):
        CashoutExecutor(_Bus(), _CrashRouter(db), ledger=db).on_cmd_execute_cashout(_cmd())
    assert [r["status"] for r in _rows(db)] == [H.HEDGE_PENDING]
    restarted = CashoutExecutor(_Bus(), _Router(db, _ok()), ledger=db)
    restarted.wire()
    rows = _rows(db)
    assert [r["status"] for r in rows] == [H.HEDGE_AMBIGUOUS]
    assert db.load_pending_customer_refs() == [], "l'hedge ambiguo non diventa un ingresso aperto"
    bus = _Bus()
    router = _Router(db, _ok())
    CashoutExecutor(bus, router, ledger=db).on_cmd_execute_cashout(_cmd(cashout_request_id="req-after-restart"))
    assert router.calls == [] and [p["reason"] for t, p in bus.published if t == CASHOUT_FAILED] == [
        "hedge_outcome_uncertain"]


def test_concurrent_duplicate_commands_place_exactly_one_hedge(db):
    import threading
    bus = _Bus()
    router = _Router(db, _ok())
    ex = CashoutExecutor(bus, router, ledger=db)
    barrier = threading.Barrier(8)

    def go():
        barrier.wait()
        ex.on_cmd_execute_cashout(_cmd())

    threads = [threading.Thread(target=go) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(router.calls) == 1, f"{len(router.calls)} hedge per lo stesso comando"
    assert len(_rows(db)) == 1
