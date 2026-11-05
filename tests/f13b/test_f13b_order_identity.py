"""F-13b (#586, ID-01, D-582-01) — l'ordine del bot si riconosce anche dal suo ``customerOrderRef``.

Dopo F-13a ogni place diretto invia ``customerOrderRef`` = ``customer_ref``. Un replace cambia il ``betId`` ma conserva
il ref; una risposta persa lascia la riga senza ``betId``. Prima di F-13b cashout (I1), ricostruzione al riavvio (F018)
e hedge del cashout riconoscevano l'ordine solo dal ``betId`` registrato: dopo un replace o una risposta persa l'ordine
vivo era invisibile (hedge sottodimensionato, resting che sopravvive al cashout, posizione chiusa al riavvio).

PASS + BLOCK: ogni test fallisce se l'identita' torna al solo ``betId``, o se un ref estraneo viene adottato.
"""
from __future__ import annotations

import json

import pytest

from cashout_executor import CASHOUT_FAILED, CashoutExecutor
from cashout_router import CASHOUT_FAILED as ROUTER_FAILED, REQ_EXECUTE_CASHOUT, CashoutRouter
from core import hedge_ref as H
from core.duplication_guard import DuplicationGuard
from core.runtime_controller import RuntimeController
from core.table_manager import TableManager
from database import Database

pytestmark = [pytest.mark.unit, pytest.mark.safety]

REF = "a" * 32


# ── Database: i ref del bot ─────────────────────────────────────────────────────────────────────────────────────────

def _insert(db, ref, *, status="SUBMITTED", bet_id=None, market="1.1", event="inter vs milan"):
    oid = db.insert_order({"customer_ref": ref, "correlation_id": f"c-{ref}", "status": "INFLIGHT",
                           "payload": {"market_id": market, "event_name": event, "selection_id": 7}})
    if status != "INFLIGHT":
        db.update_order(oid, {"status": status, "response": {"bet_id": bet_id} if bet_id else {}})
    return oid


@pytest.fixture()
def db(tmp_path):
    return Database(str(tmp_path / "f13b.db"))


def test_bot_order_refs_include_lost_response_and_inflight_rows(db):
    _insert(db, REF, status="AMBIGUOUS")                      # risposta persa: nessun betId
    _insert(db, "b" * 32, status="INFLIGHT")                   # mai confermato: l'ordine puo' esistere
    _insert(db, "c" * 32, bet_id="B3")
    refs = {r["customer_ref"]: r for r in db.get_bot_order_refs()}
    assert set(refs) == {REF, "b" * 32, "c" * 32}
    assert refs[REF]["market_id"] == "1.1" and refs[REF]["event_name"] == "inter vs milan"


@pytest.mark.parametrize("bad", ["", "x" * 33, "ref con spazi", "€uro"])
def test_non_conforming_refs_are_never_an_identity(db, bad):
    # un ref non conforme non viene inviato come customerOrderRef (core.order_ref): non puo' identificare un ordine
    _insert(db, bad or "placeholder", bet_id="B1") if bad else None
    assert all(r["customer_ref"] != bad for r in db.get_bot_order_refs())
    if bad:
        assert db.get_bot_order_refs() == []


# ── CashoutRouter (I1) ──────────────────────────────────────────────────────────────────────────────────────────────

def _curr(bet_id, *, ref="", matched=10.0, remaining=0.0, market="1.1", sel=7, side="BACK", avg=2.0):
    return {"betId": bet_id, "marketId": market, "selectionId": sel, "side": side, "customerOrderRef": ref,
            "sizeMatched": matched, "sizeRemaining": remaining, "averagePriceMatched": avg}


def _book(market="1.1", sel=7):
    return {"marketId": market, "status": "OPEN", "runners": [{
        "selectionId": sel, "availableToBack": [{"price": 2.6, "size": 100.0}],
        "availableToLay": [{"price": 1.5, "size": 100.0}]}]}


class _Router:
    def __init__(self, current, bot, refs, raise_refs=False):
        self.published, self.cancelled = [], []
        self.current, self.bot, self.refs, self.raise_refs = current, bot, refs, raise_refs

    def _refs(self):
        if self.raise_refs:
            raise RuntimeError("DB_LOCKED")
        return self.refs

    def router(self, with_refs=True):
        kw = {"fetch_bot_refs": self._refs} if with_refs else {}
        return CashoutRouter(fetch_current_orders=lambda: self.current, fetch_bot_orders=lambda: self.bot,
                             fetch_market_book=lambda m: _book(m),
                             cancel_orders=lambda m, ids: self.cancelled.append((m, list(ids))) or True,
                             publish=lambda t, p: self.published.append((t, p)), commission_pct=4.5, **kw)

    def requests(self):
        return [p for t, p in self.published if t == REQ_EXECUTE_CASHOUT]


def _bot(bet_id, market="1.1", event="inter vs milan"):
    return {"bet_id": bet_id, "market_id": market, "event_name": event}


def _ref(ref, market="1.1", event="inter vs milan"):
    return {"customer_ref": ref, "market_id": market, "event_name": event}


def test_replaced_order_is_still_the_bots_for_the_cashout():
    # replace: il DB conosce il vecchio betId (non piu' vivo), l'exchange ha il sostituto con lo stesso ref
    h = _Router([_curr("NEW", ref=REF, matched=10.0)], [_bot("OLD")], [_ref(REF)])
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 1 and len(h.requests()) == 1, out


def test_replaced_partial_position_is_hedged_in_full_not_undersized():
    # originale (betId nel DB) abbinato 4 + sostituto (solo ref) abbinato 6: la posizione e' 10, mai 4
    h = _Router([_curr("OLD", ref=REF, matched=4.0), _curr("NEW", ref=REF, matched=6.0)], [_bot("OLD")], [_ref(REF)])
    h.router().route({"signal_type": "CASHOUT_ALL"})
    without = _Router([_curr("OLD", ref=REF, matched=4.0), _curr("NEW", ref=REF, matched=6.0)], [_bot("OLD")], [])
    without.router(with_refs=False).route({"signal_type": "CASHOUT_ALL"})
    full, partial = h.requests(), without.requests()
    assert len(full) == 1 and len(partial) == 1
    assert full[0]["stake"] > partial[0]["stake"], (full, partial)


def test_resting_part_of_a_replaced_order_is_cancelled_by_the_cashout():
    h = _Router([_curr("NEW", ref=REF, matched=0.0, remaining=5.0)], [_bot("OLD")], [_ref(REF)])
    h.router().route({"signal_type": "CASHOUT_ALL"})
    assert ("1.1", ["NEW"]) in h.cancelled, "resting del sostituto sopravvissuto al cashout"


def test_lost_response_order_is_visible_to_the_cashout():
    # nessuna riga con betId: prima 'no_bot_orders' e l'ordine vivo restava aperto
    h = _Router([_curr("LIVE", ref=REF, matched=10.0)], [], [_ref(REF)])
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 1, out


def test_single_cashout_finds_the_event_of_a_lost_response_order():
    h = _Router([_curr("LIVE", ref=REF, matched=10.0)], [], [_ref(REF, event="inter vs milan")])
    out = h.router().route({"signal_type": "CASHOUT", "event_name": "Inter vs Milan"})
    assert out["published"] == 1, out


def test_foreign_order_with_another_ref_is_never_adopted():
    h = _Router([_curr("FOREIGN", ref="z" * 32, matched=10.0, remaining=5.0)], [_bot("OLD")], [_ref(REF)])
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 0 and h.cancelled == [], "ordine altrui chiuso o cancellato"


def test_order_without_ref_is_not_adopted_by_an_empty_ref():
    h = _Router([_curr("FOREIGN", ref="", matched=10.0)], [_bot("OLD")], [_ref(REF)])
    assert h.router().route({"signal_type": "CASHOUT_ALL"})["published"] == 0


def test_unreadable_bot_refs_abort_the_cashout_fail_closed():
    h = _Router([_curr("NEW", ref=REF)], [_bot("OLD")], [], raise_refs=True)
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 0 and out.get("reason") == "bot_orders_error"
    assert [t for t, _ in h.published] == [ROUTER_FAILED]


# ── F018: ricostruzione al riavvio ──────────────────────────────────────────────────────────────────────────────────

class _OrdersDB:
    def __init__(self, rows):
        self.rows = rows

    def _execute(self, sql, *a, **k):
        assert "FROM orders" in sql
        return [dict(r) for r in self.rows]


class _Exchange:
    def __init__(self, current):
        self.current = current

    def list_current_orders(self, market_ids=None):
        return list(self.current)


def _row(ref, *, bet_id=None, market="1.1", table_id=3, key="K1"):
    resp = {"instructionReports": [{"betId": bet_id, "status": "SUCCESS"}]} if bet_id else {}
    return {"customer_ref": ref, "status": "SUBMITTED", "response_json": json.dumps(resp),
            "payload_json": json.dumps({"market_id": market, "selection_id": 7, "event_key": key, "table_id": table_id,
                                        "bet_type": "BACK", "stake": 3.0, "price": 2.0, "event_name": "A v B"})}


def _rc(rows, current):
    rc = RuntimeController.__new__(RuntimeController)
    rc.db, rc.betfair_service = _OrdersDB(rows), _Exchange(current)
    rc.table_manager, rc.duplication_guard = TableManager(table_count=5), DuplicationGuard()
    return rc


def test_replaced_order_is_rebuilt_after_restart_not_closed():
    # la riga ha il vecchio betId, l'exchange il sostituto con lo stesso ref: posizione viva, mai 'closed'
    rc = _rc([_row(REF, bet_id="OLD")], [{"betId": "NEW", "marketId": "1.1", "customerOrderRef": REF}])
    res = rc._rebuild_open_positions()
    assert res["rebuilt"] == [REF] and not res["closed"], res
    assert not rc.duplication_guard.acquire("K1"), "chiave anti-duplicato non ripresa: il segnale si ripeterebbe"


def test_lost_response_order_found_by_ref_is_rebuilt_as_certain():
    rc = _rc([_row(REF)], [{"betId": "LIVE", "marketId": "1.1", "customerOrderRef": REF}])
    res = rc._rebuild_open_positions()
    assert res["rebuilt"] == [REF] and not res["uncertain"], res


def test_another_ref_on_the_market_keeps_the_lost_response_order_uncertain():
    rc = _rc([_row(REF)], [{"betId": "X", "marketId": "1.1", "customerOrderRef": "z" * 32}])
    res = rc._rebuild_open_positions()
    assert res["uncertain"] == [REF] and not res["rebuilt"], res


def test_stale_bet_id_without_any_identity_on_the_exchange_is_still_closed():
    rc = _rc([_row(REF, bet_id="OLD")], [{"betId": "X", "marketId": "9.9", "customerOrderRef": "z" * 32}])
    assert rc._rebuild_open_positions()["closed"] == [REF]


# ── Hedge del cashout: liveness dell'hedge precedente ───────────────────────────────────────────────────────────────

class _Bus:
    def __init__(self):
        self.published = []

    def subscribe(self, *_a):
        pass

    def publish(self, topic, payload):
        self.published.append((topic, payload))


class _PlaceRouter:
    def __init__(self, result):
        self.result, self.calls = result, []

    def place(self, payload):
        self.calls.append(dict(payload))
        return self.result


def _cmd(**over):
    p = {"market_id": "1.222", "selection_id": 55, "side": "LAY", "stake": 4.0, "price": 1.8, "green_up": 2.5,
         "event_key": "1.222:55:BACK:x", "source": "UI", "cashout_request_id": "req-1",
         "original_pos": {"side": "BACK", "stake": 3.6, "price": 2.0}}
    p.update(over)
    return p


def _entry():
    return {"betId": "E1", "marketId": "1.222", "selectionId": 55, "side": "BACK", "sizeMatched": 3.6,
            "sizeRemaining": 0.0, "priceSize": {"price": 2.0}, "customerOrderRef": "ENTRY1"}


def test_replaced_previous_hedge_still_live_blocks_a_second_hedge(db):
    # primo hedge resting (betId R1); poi il suo replace sull'exchange: nuovo betId, stesso ref hedge:...
    first = CashoutExecutor(_Bus(), _PlaceRouter({"placed": True, "matched": 0.0, "bet_id": "R1"}), ledger=db,
                            fetch_current_orders=lambda: [_entry()])
    first.on_cmd_execute_cashout(_cmd())
    (row,) = [dict(r) for r in db._execute("SELECT customer_ref FROM orders", fetch=True, commit=False)]
    hedge_ref = row["customer_ref"]
    assert H.is_hedge_order_ref(hedge_ref)
    replaced = {"betId": "R2", "marketId": "1.222", "selectionId": 55, "side": "LAY", "sizeMatched": 0.0,
                "sizeRemaining": 4.0, "customerOrderRef": hedge_ref}
    bus, router = _Bus(), _PlaceRouter({"ok": True, "placed": True, "matched": 4.0, "bet_id": "CB2"})
    CashoutExecutor(bus, router, ledger=db, fetch_current_orders=lambda: [_entry(), replaced]).on_cmd_execute_cashout(
        _cmd(cashout_request_id="req-2"))
    assert router.calls == [], "secondo hedge con il primo (sostituito) ancora abbinabile"
    assert [p["reason"] for t, p in bus.published if t == CASHOUT_FAILED] == ["previous_hedge_still_live"]


# ── E2E sul percorso reale: SimulationBroker + Database + CashoutRouter ─────────────────────────────────────────────

def test_e2e_replaced_partial_order_is_fully_closed_by_the_cashout(db):
    from simulation_broker import SimulationBroker
    broker = SimulationBroker(starting_balance=100.0)
    broker.update_market_book("1.1", {"marketId": "1.1", "status": "OPEN", "runners": [
        {"selectionId": 7, "ex": {"availableToBack": [{"price": 2.0, "size": 2.0}], "availableToLay": []}}]})
    placed = broker.place_bet(market_id="1.1", selection_id=7, side="BACK", price=2.0, size=5.0,
                              customer_ref=REF, customer_order_ref=REF)
    old_bet = placed["instructionReports"][0]["betId"]
    _insert(db, REF, bet_id=old_bet)                       # il ledger del bot conosce solo il betId originale
    new_bet = broker.replace_orders(market_id="1.1", bet_id=old_bet, new_price=3.0)["instructionReports"][0]["betId"]
    broker.update_market_book("1.1", {"marketId": "1.1", "status": "OPEN", "runners": [
        {"selectionId": 7, "ex": {"availableToBack": [{"price": 3.0, "size": 1.0}], "availableToLay": []}}]})
    current = broker.get_current_orders(["1.1"])
    assert {o["betId"] for o in current} == {old_bet, new_bet}
    matched = sum(float(o["sizeMatched"]) for o in current)            # 2 a 2.0 + 1 a 3.0
    resting = [o["betId"] for o in current if float(o["sizeRemaining"]) > 0]
    assert matched == 3.0 and resting == [new_bet]

    published, cancelled = [], []
    router = CashoutRouter(
        fetch_current_orders=lambda: broker.get_current_orders(["1.1"]),
        fetch_bot_orders=db.get_bot_active_orders, fetch_bot_refs=db.get_bot_order_refs,
        fetch_market_book=lambda m: _book(m),
        cancel_orders=lambda m, ids: cancelled.append((m, list(ids))) or True,
        publish=lambda t, p: published.append((t, p)), commission_pct=4.5)
    out = router.route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 1, out
    assert ("1.1", [new_bet]) in cancelled, "resting del sostituto sopravvissuto al cashout"
    (req,) = [p for t, p in published if t == REQ_EXECUTE_CASHOUT]
    # la posizione chiusa comprende l'abbinato di ENTRAMBI gli ordini (3.00), non solo dell'originale (2.00)
    assert req["original_pos"]["stake"] == pytest.approx(3.0), req["original_pos"]
