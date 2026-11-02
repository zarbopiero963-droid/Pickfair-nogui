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
         "event_key": "1.222:55:BACK:x", "source": "UI", "cashout_request_id": "req-1",
         # F-06: l'hedge LAY 4@1.8 chiude una posizione BACK reale (3.6@2.0 => 7.2 abbinati)
         "original_pos": {"side": "BACK", "stake": 3.6, "price": 2.0}}
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


# --- Sol su 59c7eca (#593): un hedge precedente ancora vivo sull'exchange blocca un nuovo hedge sulla selezione ---
# Percorso reale: il primo hedge resta a riposo (HEDGE_UNMATCHED, CASHOUT_FAILED), l'operatore rimanda il CASHOUT con
# un nuovo cashout_request_id, il router pubblica il nuovo hedge PRIMA del flatten best-effort del resting: senza
# questo blocco due hedge abbinabili sovracoprono la posizione (PF-EXEC-01: si ripunta solo dopo cancellazione
# confermata).

def _live(bet_id, remaining=4.0, market="1.222", selection=55):
    return {"betId": bet_id, "bet_id": bet_id, "marketId": market, "selectionId": selection, "side": "LAY",
            "sizeMatched": 0.0, "sizeRemaining": remaining}       # F-06: ogni ordine dell'exchange ha il lato


def _entry(market="1.222", selection=55):
    """F-06: l'ordine d'ingresso abbinato (BACK 3.6@2.0) della posizione che l'hedge chiude."""
    return {"betId": "E1", "marketId": market, "selectionId": selection, "side": "BACK", "sizeMatched": 3.6,
            "sizeRemaining": 0.0, "priceSize": {"price": 2.0}, "customerOrderRef": "ENTRY1"}


def _with_entry(fetch):
    """La fonte ordini dei test F-05 piu' la posizione d'ingresso, solo sulle risposte ben formate (lista)."""
    if fetch is None:
        return None

    def wrapped():
        reply = fetch()
        return [_entry(), *reply] if isinstance(reply, list) else reply
    return wrapped


def _executor(db, result, fetch):
    bus, router = _Bus(), _Router(db, result)
    return bus, router, CashoutExecutor(bus, router, ledger=db, fetch_current_orders=_with_entry(fetch))


def _first_hedge_resting(db):
    _, _, ex = _executor(db, {"placed": True, "matched": 0.0, "bet_id": "R1"}, lambda: [])
    ex.on_cmd_execute_cashout(_cmd())
    assert [r["status"] for r in _rows(db)] == [H.HEDGE_UNMATCHED]


def test_resting_hedge_still_live_blocks_a_new_hedge_on_the_same_selection(db):
    _first_hedge_resting(db)
    bus, router, ex = _executor(db, _ok(bet_id="CB2"), lambda: [_live("R1")])
    ex.on_cmd_execute_cashout(_cmd(cashout_request_id="req-2"))
    assert router.calls == [], "secondo hedge con il primo ancora abbinabile sull'exchange"
    assert [p["reason"] for t, p in bus.published if t == CASHOUT_FAILED] == ["previous_hedge_still_live"]


def test_new_hedge_is_allowed_once_the_resting_hedge_is_gone(db):
    _first_hedge_resting(db)
    bus, router, ex = _executor(db, _ok(bet_id="CB2"), lambda: [_live("OTHER", selection=99)])
    ex.on_cmd_execute_cashout(_cmd(cashout_request_id="req-2"))
    assert len(router.calls) == 1 and CASHOUT_SUCCESS in bus.topics(), "hedge precedente cancellato: si ripunta"


def test_unverifiable_liveness_of_a_previous_hedge_means_no_new_hedge(db):
    _first_hedge_resting(db)

    def boom():
        raise RuntimeError("LIVE_BLOCKED_SESSION_INVALID")
    for fetch in (boom, None):
        bus, router, ex = _executor(db, _ok(bet_id="CB2"), fetch)
        ex.on_cmd_execute_cashout(_cmd(cashout_request_id="req-2"))
        assert router.calls == [] and [p["reason"] for t, p in bus.published if t == CASHOUT_FAILED] == [
            "hedge_liveness_unverifiable"]


def test_previous_hedge_on_another_selection_never_blocks(db):
    _first_hedge_resting(db)
    bus, router, ex = _executor(db, _ok(bet_id="CB2"), None)
    ex.on_cmd_execute_cashout(_cmd(cashout_request_id="req-2", selection_id=77))
    assert len(router.calls) == 1


@pytest.mark.parametrize("remaining", [None, "x", float("nan"), float("inf")])
def test_unreadable_remaining_size_of_the_previous_hedge_counts_as_live(db, remaining):
    _first_hedge_resting(db)
    row = _live("R1")
    row["sizeRemaining"] = remaining
    bus, router, ex = _executor(db, _ok(bet_id="CB2"), lambda: [row])
    ex.on_cmd_execute_cashout(_cmd(cashout_request_id="req-2"))
    assert router.calls == [] and [p["reason"] for t, p in bus.published if t == CASHOUT_FAILED] == [
        "previous_hedge_still_live"]


@pytest.mark.parametrize("reply", [None, {"currentOrders": [_live("R1")]}, "R1", [_live("R1"), "garbage"], [None]])
def test_malformed_current_orders_reply_never_reads_as_closed_hedge(db, reply):
    # Sol su f5a7982: None => [] e gli elementi non-dict ignorati leggevano l'hedge precedente come chiuso (fail-open)
    _first_hedge_resting(db)
    bus, router, ex = _executor(db, _ok(bet_id="CB2"), lambda: reply)
    ex.on_cmd_execute_cashout(_cmd(cashout_request_id="req-2"))
    assert router.calls == [], "risposta malformata letta come 'hedge precedente chiuso'"
    assert [p["reason"] for t, p in bus.published if t == CASHOUT_FAILED] in (
        ["hedge_liveness_unverifiable"], ["previous_hedge_still_live"])


# Grok su f5a7982: fill e cancellazione non vanno confusi. listCurrentOrders (LIVE, e il SIM) riporta l'hedge eseguito
# come EXECUTION_COMPLETE con sizeMatched > 0 e sizeRemaining 0: la posizione e' gia' coperta, un nuovo hedge la
# sovracoprirebbe. Solo matched 0 e remaining 0 (o l'ordine sparito) e' una cancellazione senza abbinamento.

def _done(bet_id, matched):
    row = _live(bet_id, remaining=0.0)
    row["sizeMatched"] = matched
    return row


def test_previous_hedge_already_matched_blocks_a_new_hedge(db):
    _first_hedge_resting(db)
    bus, router, ex = _executor(db, _ok(bet_id="CB2"), lambda: [_done("R1", 4.0)])
    ex.on_cmd_execute_cashout(_cmd(cashout_request_id="req-2"))
    assert router.calls == [], "hedge precedente abbinato letto come cancellato: sovracopertura"
    assert [p["reason"] for t, p in bus.published if t == CASHOUT_FAILED] == ["previous_hedge_matched"]


def test_previous_hedge_cancelled_without_any_match_allows_a_new_hedge(db):
    _first_hedge_resting(db)
    bus, router, ex = _executor(db, _ok(bet_id="CB2"), lambda: [_done("R1", 0.0)])
    ex.on_cmd_execute_cashout(_cmd(cashout_request_id="req-2"))
    assert len(router.calls) == 1 and CASHOUT_SUCCESS in bus.topics()


@pytest.mark.parametrize("matched", [None, "x", float("nan"), float("inf")])
def test_unreadable_matched_size_of_the_previous_hedge_blocks(db, matched):
    _first_hedge_resting(db)
    bus, router, ex = _executor(db, _ok(bet_id="CB2"), lambda: [_done("R1", matched)])
    ex.on_cmd_execute_cashout(_cmd(cashout_request_id="req-2"))
    assert router.calls == [] and [p["reason"] for t, p in bus.published if t == CASHOUT_FAILED] == [
        "hedge_liveness_unverifiable"]


@pytest.mark.parametrize("bad", [{}, {"betId": ""}, {"betId": None}, {"bet_id": "  "}, {"marketId": "1.222"}])
def test_current_order_without_a_bet_id_makes_liveness_unverifiable(db, bad):
    # Sol su 4a6a843: un ordine senza identificativo veniva ignorato e l'hedge precedente risultava chiuso
    _first_hedge_resting(db)
    bus, router, ex = _executor(db, _ok(bet_id="CB2"), lambda: [_live("OTHER", selection=99), bad])
    ex.on_cmd_execute_cashout(_cmd(cashout_request_id="req-2"))
    assert router.calls == [] and [p["reason"] for t, p in bus.published if t == CASHOUT_FAILED] == [
        "hedge_liveness_unverifiable"]


def test_sim_flag_is_captured_after_the_ledger_preflight(db):
    # Codex P1 su 4a6a843: il controllo dell'hedge precedente fa I/O (list_current_orders) prima del place; il mode
    # SIM/LIVE va catturato DOPO, immediatamente prima dell'invio, altrimenti un cambio LIVE->SIM nel frattempo
    # pubblica un CASHOUT_SUCCESS simulato con sim=False (broadcast ai follower reali).
    class _Svc:
        simulation = False

        def is_simulation_mode(self):
            return self.simulation

    svc = _Svc()
    _first_hedge_resting(db)

    def fetch_and_switch():
        svc.simulation = True                      # LIVE -> SIM durante il preflight
        return [_live("OTHER", selection=99)]

    bus, router = _Bus(), _Router(db, _ok(bet_id="CB2"))
    router.service = svc
    CashoutExecutor(bus, router, ledger=db, fetch_current_orders=_with_entry(fetch_and_switch)).on_cmd_execute_cashout(
        _cmd(cashout_request_id="req-2"))
    success = [p for t, p in bus.published if t == CASHOUT_SUCCESS]
    assert len(router.calls) == 1 and len(success) == 1
    assert success[0]["sim"] is True, "CASHOUT_SUCCESS simulato pubblicato come LIVE (flag sim stantio)"
