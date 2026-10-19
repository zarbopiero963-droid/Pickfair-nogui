"""#524 — un ordine forse inviato non diventa mai FAILED senza prova del rifiuto (PF-POST522-ENGINE-RECOVERY-01).

Difetto (prova c1 della review #522): esito AMBIGUOUS, transizione INFLIGHT→AMBIGUOUS fallita sul DB =>
``_precheck_finalize`` sollevava ``FINALIZE_ON_NON_TERMINAL_DB_STATE``, l'``except`` esterno di
``_submit_via_engine`` marcava l'ordine FAILED: tavolo, dedupe e fence liberati su un ordine che puo' essere
vivo su Betfair. Stessa sorte per qualunque eccezione arrivata DOPO l'avvio dell'invio.

Catena reale: RuntimeController + TradingEngine + Database sqlite + SimulationBroker; fault injection sul DB
(``update_order``/``get_order``) e sul broker (timeout, risposta persa, crash dopo l'invio).
"""
from __future__ import annotations

import functools
import threading

import pytest

from core.trading_engine import TradingEngine
from tests.acceptance.test_issue437_pr27 import _catena, _rifiuti
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _signal

pytestmark = [pytest.mark.integration, pytest.mark.safety]


def _fail_db_transition_to(c, *statuses, also_get_order=False):
    """Fault injection: ``update_order`` solleva quando prova a scrivere uno degli stati indicati."""
    original_update = c.db.update_order
    original_get = c.db.get_order

    def update_order(order_id, update, *args, **kwargs):
        if str((update or {}).get("status") or "") in statuses:
            raise RuntimeError("database is locked")
        return original_update(order_id, update, *args, **kwargs)

    def get_order(order_id, *args, **kwargs):
        if also_get_order:
            raise RuntimeError("database is locked")
        return original_get(order_id, *args, **kwargs)

    c.db.update_order = update_order
    c.db.get_order = get_order
    c.engine.db = c.db


def _broker_times_out_after_sending(c):
    """Il broker riceve e PIAZZA l'ordine, poi la risposta si perde (timeout del trasporto)."""
    original = c.broker.place_bet

    @functools.wraps(original)
    def place_then_lose_response(*args, **kwargs):
        original(*args, **kwargs)
        raise TimeoutError("read timeout dopo l'invio")

    c.broker.place_bet = place_then_lose_response


def _db_status(c, customer_ref):
    rows = c.db._execute("SELECT status FROM orders WHERE customer_ref = ?", (customer_ref,), fetch=True,
                         commit=False) or []
    return [r["status"] for r in rows]


def _quick_bet_results(c):
    return [(t, p.get("status")) for t, p in c.bus.events
            if t in ("QUICK_BET_FAILED", "QUICK_BET_AMBIGUOUS", "QUICK_BET_SUCCESS")]


def _occupied_tables(c):
    return [t.table_id for t in c.rc.table_manager._tables.values() if t.status != "FREE"]


# ---------------------------------------------------------------- 1. difetto originale (c1)
def test_ambiguous_send_with_failed_db_transition_is_never_failed(tmp_path):
    c = _catena(tmp_path)
    _broker_times_out_after_sending(c)
    _fail_db_transition_to(c, "AMBIGUOUS", "FAILED")

    c.rc._on_signal_received(_signal())

    assert len(c.broker.state.orders) == 1                       # l'ordine E' sull'exchange
    events = _quick_bet_results(c)
    assert ("QUICK_BET_FAILED", "FAILED") not in events
    assert any(t == "QUICK_BET_AMBIGUOUS" for t, _ in events)
    ref = c.bus.payloads("CMD_QUICK_BET")[-1]["customer_ref"]
    assert "FAILED" not in _db_status(c, ref)                    # mai FAILED persistito
    assert _occupied_tables(c)                                   # tavolo trattenuto fino alla riconciliazione


def test_db_recovering_for_failed_write_still_never_marks_failed(tmp_path):
    """La variante della prova c1: solo la transizione ad AMBIGUOUS fallisce, quella a FAILED riuscirebbe."""
    c = _catena(tmp_path)
    _broker_times_out_after_sending(c)
    _fail_db_transition_to(c, "AMBIGUOUS")

    c.rc._on_signal_received(_signal())

    ref = c.bus.payloads("CMD_QUICK_BET")[-1]["customer_ref"]
    assert _db_status(c, ref) == ["INFLIGHT"]                    # incerto durevole: mai FAILED
    assert not any(t == "QUICK_BET_FAILED" for t, _ in _quick_bet_results(c))
    assert _occupied_tables(c)


def test_engine_result_is_ambiguous_and_not_terminal_when_ambiguity_is_not_persisted(tmp_path):
    c = _catena(tmp_path)
    _broker_times_out_after_sending(c)
    _fail_db_transition_to(c, "AMBIGUOUS")
    payload = _queue_entry(c)

    result = c.engine.submit_quick_bet(payload)

    assert result["status"] == "AMBIGUOUS" and result["outcome"] == "AMBIGUOUS"
    assert result["is_terminal"] is False and result["finalization_persisted"] is False


# ---------------------------------------------------------------- 2. altre eccezioni dopo l'avvio dell'invio
def test_failed_submitted_transition_after_a_real_send_is_ambiguous_not_failed(tmp_path):
    """Il broker ha accettato, la transizione INFLIGHT→SUBMITTED fallisce, e fallisce anche quella ad AMBIGUOUS."""
    c = _catena(tmp_path)
    _fail_db_transition_to(c, "SUBMITTED", "AMBIGUOUS")

    c.rc._on_signal_received(_signal())

    assert len(c.broker.state.orders) == 1
    assert not any(t == "QUICK_BET_FAILED" for t, _ in _quick_bet_results(c))
    ref = c.bus.payloads("CMD_QUICK_BET")[-1]["customer_ref"]
    assert "FAILED" not in _db_status(c, ref)
    assert _occupied_tables(c)


def test_crash_after_the_send_in_the_engine_is_ambiguous_not_failed(tmp_path, monkeypatch):
    """Eccezione dopo un invio riuscito (costruzione dell'ack): l'ordine esiste, il risultato non e' FAILED."""
    c = _catena(tmp_path)

    def boom(*_a, **_k):
        raise RuntimeError("crash dopo l'invio")

    monkeypatch.setattr(TradingEngine, "_build_ack_result", boom)

    c.rc._on_signal_received(_signal())

    assert len(c.broker.state.orders) == 1
    assert not any(t == "QUICK_BET_FAILED" for t, _ in _quick_bet_results(c))
    assert any(t == "QUICK_BET_AMBIGUOUS" for t, _ in _quick_bet_results(c))
    assert _occupied_tables(c)


def test_database_fully_unavailable_after_the_send_is_ambiguous(tmp_path):
    c = _catena(tmp_path)
    _broker_times_out_after_sending(c)
    _fail_db_transition_to(c, "AMBIGUOUS", "FAILED", "SUBMITTED", also_get_order=True)
    payload = _queue_entry(c)

    result = c.engine.submit_quick_bet(payload)

    assert result["status"] == "AMBIGUOUS"
    assert not any(t == "QUICK_BET_FAILED" for t, _ in _quick_bet_results(c))


# ---------------------------------------------------------------- 3. dedupe, nessun secondo invio, riavvio
def test_same_intent_is_never_sent_twice_after_an_unpersisted_ambiguity(tmp_path):
    c = _catena(tmp_path)
    _broker_times_out_after_sending(c)
    _fail_db_transition_to(c, "AMBIGUOUS")
    c.rc._on_signal_received(_signal())
    assert len(c.broker.state.orders) == 1

    c.rc._on_signal_received(_signal())                          # riconsegna dello stesso messaggio

    assert len(c.broker.state.orders) == 1                       # nessun secondo invio cieco


def test_unpersisted_ambiguity_survives_a_restart_and_blocks_the_same_intent(tmp_path):
    c = _catena(tmp_path)
    _broker_times_out_after_sending(c)
    _fail_db_transition_to(c, "AMBIGUOUS")
    c.rc._on_signal_received(_signal())
    ref = c.bus.payloads("CMD_QUICK_BET")[-1]["customer_ref"]
    assert _db_status(c, ref) == ["INFLIGHT"]

    restarted = TradingEngine(bus=c.bus.__class__(), db=c.db, client_getter=lambda: c.broker,
                              executor=c.engine.executor)
    assert ref in (restarted.db.load_pending_customer_refs() or [])     # dedupe ricostruito dal DB
    result = restarted.submit_quick_bet({**c.bus.payloads("CMD_QUICK_BET")[-1]})
    assert result["status"] in {"DUPLICATE_BLOCKED", "DENIED"}
    assert len(c.broker.state.orders) == 1


# ---------------------------------------------------------------- 4. fence/cashout
def test_unpersisted_ambiguity_makes_a_cashout_in_scope_uncertain(tmp_path, monkeypatch):
    from tests.acceptance.test_issue437_pr28d_integration_512 import _EVENT_CASHOUT, _snapshotting_router

    c = _catena(tmp_path)
    _snapshotting_router(c, monkeypatch)
    _broker_times_out_after_sending(c)
    _fail_db_transition_to(c, "AMBIGUOUS")
    c.rc._on_signal_received(_signal())

    c.rc._route_cashout_signal(dict(_EVENT_CASHOUT))

    assert c.rc.get_status()["cashout_barriers_uncertain"] == 1


# ---------------------------------------------------------------- 5. non regressione: rifiuto certo resta FAILED
def test_proven_pre_send_rejection_is_still_failed(tmp_path):
    c = _catena(tmp_path)

    def rejected(*_a, **_k):
        return {"ok": False, "status": "FAILED", "error": "INSUFFICIENT_FUNDS", "reason_code": "BROKER_REJECTED"}

    c.broker.place_bet = functools.wraps(c.broker.place_bet)(rejected)

    c.rc._on_signal_received(_signal())

    assert c.broker.state.orders == {}
    assert any(t == "QUICK_BET_FAILED" for t, _ in _quick_bet_results(c))
    assert _occupied_tables(c) == []                             # rifiuto certo: tavolo liberato


# ---------------------------------------------------------------- 6. concorrenza multi-tavolo
def test_concurrent_ambiguous_entries_on_several_tables_all_stay_held(tmp_path):
    c = _catena(tmp_path)
    _broker_times_out_after_sending(c)
    _fail_db_transition_to(c, "AMBIGUOUS")
    payloads = []
    original_publish = c.bus.publish

    def queueing_publish(topic, payload=None):
        if topic == "CMD_QUICK_BET":
            payloads.append(payload)
            c.bus.events.append((topic, dict(payload)))
            return None
        return original_publish(topic, payload)

    c.bus.publish = queueing_publish
    for i, (event, market) in enumerate((("Alpha v Beta", "1.234"), ("Gamma v Delta", "1.777"),
                                         ("Eps v Zeta", "1.888"))):
        c.rc._on_signal_received(_signal(event=event, market_id=market,
                                         received_at=f"2026-10-08T10:0{i}:00+00:00"))
    c.bus.publish = original_publish
    assert len(payloads) == 3, _rifiuti(c)

    workers = [threading.Thread(target=lambda p=p: [h(p) for h in list(c.bus.subscribers["CMD_QUICK_BET"])])
               for p in payloads]
    for w in workers:
        w.start()
    for w in workers:
        w.join(10)

    assert len(c.broker.state.orders) == 3
    assert not any(t == "QUICK_BET_FAILED" for t, _ in _quick_bet_results(c))
    assert len(_occupied_tables(c)) == 3


def _queue_entry(c):
    held = []
    original_publish = c.bus.publish

    def queueing_publish(topic, payload=None):
        if topic == "CMD_QUICK_BET":
            held.append(payload)
            c.bus.events.append((topic, dict(payload)))
            return None
        return original_publish(topic, payload)

    c.bus.publish = queueing_publish
    c.rc._on_signal_received(_signal())
    c.bus.publish = original_publish
    assert len(held) == 1, _rifiuti(c)
    return held[0]


# ---------------------------------------------------------------- 7. contratto di _resolve_ambiguity, per livello
def _engine_order(c):
    payload = _queue_entry(c)
    engine = c.engine
    normalized = engine._normalize_request(payload)
    ctx = engine._new_execution_context(normalized)
    audit = engine._new_audit(ctx)
    order_id = engine._persist_inflight(ctx, normalized)
    return engine, ctx, audit, order_id


def test_unpersisted_ambiguity_reports_its_original_cause_without_touching_the_finalize(tmp_path):
    c = _catena(tmp_path)
    engine, ctx, audit, order_id = _engine_order(c)
    _fail_db_transition_to(c, "AMBIGUOUS")

    result = engine._resolve_ambiguity(ctx, audit, order_id, ambiguity_reason="SUBMIT_TIMEOUT",
                                       trigger_event="SUBMIT_AMBIGUOUS", trigger_error="read timeout")

    assert result["status"] == "AMBIGUOUS" and result["is_terminal"] is False
    assert result["error"] == "read timeout"                     # la causa vera, non l'errore del finalize
    assert not any(e["type"].startswith("FINAL_") for e in audit["events"])


def test_finalize_raising_after_a_persisted_ambiguity_still_returns_ambiguous(tmp_path):
    c = _catena(tmp_path)
    engine, ctx, audit, order_id = _engine_order(c)

    def boom(*_a, **_k):
        raise RuntimeError("ORDER_ALREADY_FINALIZED")

    engine._precheck_finalize = boom

    result = engine._resolve_ambiguity(ctx, audit, order_id, ambiguity_reason="SUBMIT_TIMEOUT",
                                       trigger_event="SUBMIT_AMBIGUOUS", trigger_error="read timeout")

    assert result["status"] == "AMBIGUOUS" and result["is_terminal"] is False
    assert _db_status(c, ctx.customer_ref) == ["AMBIGUOUS"]


# ---------------------------------------------------------------- 8. riavvio del runtime (F018) sul DB reale
def test_restarted_runtime_rebuilds_the_unpersisted_ambiguity_as_an_uncertain_held_table(tmp_path):
    from core.duplication_guard import DuplicationGuard
    from core.runtime_controller import RuntimeController
    from core.table_manager import TableManager

    c = _catena(tmp_path)
    _broker_times_out_after_sending(c)
    _fail_db_transition_to(c, "AMBIGUOUS")
    c.rc._on_signal_received(_signal())
    ref = c.bus.payloads("CMD_QUICK_BET")[-1]["customer_ref"]
    assert _db_status(c, ref) == ["INFLIGHT"]

    restarted = RuntimeController.__new__(RuntimeController)        # processo nuovo: solo DB ed exchange
    restarted.db = c.db
    restarted.betfair_service = c.rc.betfair_service
    restarted.table_manager = TableManager(table_count=5)
    restarted.duplication_guard = DuplicationGuard()

    result = restarted._rebuild_open_positions()

    assert result["ok"] is True and result["uncertain"] == [ref]        # riserva incerta, non ricostruita libera
    (held,) = [t for t in restarted.table_manager.snapshot() if t.get("status") != "FREE"]
    assert held["market_id"] == "1.234" and held["current_exposure"] > 0   # tavolo ed esposizione trattenuti
