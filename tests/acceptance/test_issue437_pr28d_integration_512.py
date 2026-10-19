"""PR28-d (#505) integrata sopra la #512: difetti d'integrazione (fase D, PF-505-RECOVERY-INTEGRATION-01).

1. Il fetch del book di PF-ALL-F030 (#512, attivo di default) girava con `_risk_admission_lock` (#505)
   tenuto: un CASHOUT/CASHOUT_ALL, `risk_stop` o `reload_config` aspettavano la rete dell'ingresso.
   Ora il fetch gira fuori dal lock, come il best price, e un cashout arrivato nel frattempo invalida
   l'ingresso (fence) prima di ogni ordine.
2. La ricostruzione F018 (#512) rifaceva il meta del tavolo senza `event_id`/`event_identity` (#505):
   dopo un riavvio il cap evento confrontava solo i nomi e due segnali dello stesso `event_id` con nomi
   scritti diversamente sfuggivano al cap (fail-open).
"""
from __future__ import annotations

import json
import threading

import pytest

from core.duplication_guard import DuplicationGuard
from core.runtime_controller import RuntimeController
from core.system_state import RuntimeMode
from core.table_manager import TableManager
from tests.acceptance.test_issue437_pr27 import _catena, _rifiuti
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _signal
from tests.integration.test_runtime_controller_auto_trade_mm import (
    _canonical_close_payload,
    _make_controller,
)

pytestmark = [pytest.mark.integration, pytest.mark.safety]


def _wrap_book(rc, on_fetch):
    original = getattr(rc.betfair_service, "get_market_book_snapshot", None)

    def book(market_id, *args, **kwargs):
        on_fetch()
        return original(market_id, *args, **kwargs) if original else None

    rc.betfair_service.get_market_book_snapshot = book


# ---------------------------------------------------------------- 1. lock di ammissione e fetch F030
def test_signal_entry_fetches_liquidity_book_outside_admission_lock(tmp_path):
    c = _catena(tmp_path)
    assert getattr(c.rc.config, "liquidity_guard_enabled", True)
    owned = []
    _wrap_book(c.rc, lambda: owned.append(c.rc._risk_admission_lock._is_owned()))

    c.rc._on_signal_received(_signal())

    assert owned == [False]
    assert len(c.broker.state.orders) == 1


def test_cashout_is_not_blocked_while_entry_fetches_liquidity_book(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    in_fetch, release = threading.Event(), threading.Event()

    def slow():
        in_fetch.set()
        release.wait(5)

    _wrap_book(c.rc, slow)
    routed = []
    # `_route_cashout_signal` chiama il metodo sulla classe: si intercetta li' (nessun broker toccato).
    monkeypatch.setattr(RuntimeController, "_route_cashout_signal_under_barrier",
                        lambda self, s, b: routed.append(b) or False)
    entry = threading.Thread(target=c.rc._on_signal_received, args=(_signal(),))
    entry.start()
    assert in_fetch.wait(5)
    cashout = threading.Thread(target=c.rc._route_cashout_signal, args=({"signal_type": "CASHOUT_ALL"},))
    cashout.start()
    cashout.join(2.0)
    blocked = cashout.is_alive()
    release.set()
    entry.join(5)
    cashout.join(5)

    assert not blocked
    assert len(routed) == 1
    # Il cashout arrivato durante il fetch invalida l'ingresso: nessun ordine, tavolo liberato.
    assert c.broker.state.orders == {}
    assert c.bus.payloads("CMD_QUICK_BET") == []
    assert _rifiuti(c) == ["cashout_invalidation_stale"]
    assert all(t.status == "FREE" for t in c.rc.table_manager._tables.values())


def _auto_next_close(**overrides):
    return _canonical_close_payload(
        event_key="evt-d-1",
        batch_id="batch-d-1",
        correlation_id="corr-d-1",
        auto_trade_enabled=True,
        cycle_executor_enabled=True,
        mm_context={
            "cycle_active": True,
            "cycle_id": "cycle-d",
            "table": {"table_id": 1, "loss_amount": 0.0, "in_recovery": False},
            "next_signal": {"market_id": "1.234", "selection_id": 8, "price": 2.0, "side": "BACK"},
        },
        **overrides,
    )


def _auto_next_result(bus):
    events = [p for t, p in bus.events if t == "AUTO_TRADE_MM_RESULT"]
    assert len(events) == 1
    return events[0]


def test_auto_next_fetches_liquidity_book_outside_admission_lock():
    rc, bus = _make_controller(responses=[{"available": 150.0}])
    rc.mode = RuntimeMode.ACTIVE
    rc.risk_desk.sync_bankroll(100.0)
    owned = []
    _wrap_book(rc, lambda: owned.append(rc._risk_admission_lock._is_owned()))

    rc._on_close_position(_auto_next_close())

    assert owned == [False]
    assert _auto_next_result(bus)["auto_trade_status"] == "AUTO_TRADE_SUBMITTED"


def test_auto_next_cashout_during_liquidity_fetch_blocks_the_entry():
    rc, bus = _make_controller(responses=[{"available": 150.0}])
    rc.mode = RuntimeMode.ACTIVE
    rc.risk_desk.sync_bankroll(100.0)

    def cashout_arrives():
        with rc._risk_admission_lock:
            rc._begin_cashout_barrier({"signal_type": "CASHOUT_ALL"})

    _wrap_book(rc, cashout_arrives)

    rc._on_close_position(_auto_next_close())

    result = _auto_next_result(bus)
    assert result["submitted"] is False
    assert result["reason"] == "cashout_invalidation_stale"
    assert [t for t, _ in bus.events if t == "CMD_QUICK_BET"] == []
    assert all(t.status == "FREE" for t in rc.table_manager._tables.values())


# ---------------------------------------------------------------- 2. identita' evento dopo il riavvio
def _rows(c):
    return [dict(r) for r in c.db._execute(
        "SELECT customer_ref, status, payload_json FROM orders", fetch=True, commit=False)]


def _restart(c):
    bets = [{"betId": b, "marketId": o.market_id} for b, o in c.broker.state.orders.items()]
    c.rc.table_manager = TableManager(table_count=c.rc.table_manager.table_count)
    c.rc.duplication_guard = DuplicationGuard()
    original = c.rc.betfair_service

    class _Exchange:
        def list_current_orders(self, market_ids=None):
            return list(bets)

    c.rc.betfair_service = _Exchange()
    try:
        return c.rc._rebuild_open_positions()
    finally:
        c.rc.betfair_service = original


def test_order_payload_carries_event_identity(tmp_path):
    c = _catena(tmp_path)
    c.rc._on_signal_received(_signal(event_id="E1", event="Alpha v Beta"))
    assert len(c.broker.state.orders) == 1, _rifiuti(c)
    persisted = json.loads(_rows(c)[0]["payload_json"])
    assert persisted["event_id"] == "E1"
    assert persisted["event_identity"] == "id:E1"


@pytest.mark.parametrize("restart", [False, True])
def test_event_cap_recognizes_event_id_after_restart(tmp_path, restart):
    c = _catena(tmp_path, max_event_exposure_abs=50.0)
    c.rc._on_signal_received(_signal(event_id="E1", event="Alpha v Beta"))
    assert len(c.broker.state.orders) == 1, _rifiuti(c)
    if restart:
        rebuilt = _restart(c)
        assert rebuilt["ok"] is True
        metas = [t.get("meta") for t in c.rc.table_manager.snapshot() if t.get("current_event_key")]
        assert [m.get("event_id") for m in metas] == ["E1"]
        assert [m.get("event_identity") for m in metas] == ["id:E1"]

    # Stesso event_id, nome scritto diversamente, altro mercato: e' lo stesso evento.
    c.rc._on_signal_received(_signal(event_id="E1", event="Alpha vs Beta", market_id="1.999",
                                     received_at="2026-10-08T10:05:00+00:00"))

    assert len(c.broker.state.orders) == 1
    assert _rifiuti(c)[-1].startswith("max_event_exposure_abs_exceeded")


def test_restart_without_persisted_event_id_keeps_name_matching(tmp_path):
    """Ordini salvati prima di questa correzione (nessun event_id nel payload): restano riconosciuti
    per nome, come prima, senza inventare un id."""
    c = _catena(tmp_path, max_event_exposure_abs=50.0)
    c.rc._on_signal_received(_signal(event="Alpha v Beta"))
    assert len(c.broker.state.orders) == 1, _rifiuti(c)
    _restart(c)
    metas = [t.get("meta") for t in c.rc.table_manager.snapshot() if t.get("current_event_key")]
    assert [m.get("event_id") for m in metas] == [None]

    c.rc._on_signal_received(_signal(event="alpha  v beta", market_id="1.999",
                                     received_at="2026-10-08T10:05:00+00:00"))

    assert len(c.broker.state.orders) == 1
    assert _rifiuti(c)[-1].startswith("max_event_exposure_abs_exceeded")


def test_outside_admission_lock_releases_single_hold_but_never_a_reentrant_one():
    rc = object.__new__(RuntimeController)
    rc._risk_admission_lock = threading.RLock()
    lock = rc._risk_admission_lock
    owned = lambda: lock._is_owned()  # noqa: E731

    assert rc._call_outside_admission_lock(owned) is False          # senza lock: esegue e basta
    with lock:
        assert rc._call_outside_admission_lock(owned) is False      # un livello: rilasciato
        assert lock._is_owned()                                      # e ripreso
        with lock:
            assert rc._call_outside_admission_lock(owned) is True   # rientrante: resta tenuto
        assert lock._is_owned()
    assert not lock._is_owned()

    with lock:
        with pytest.raises(RuntimeError):
            rc._call_outside_admission_lock(lambda: (_ for _ in ()).throw(RuntimeError("rete")))
        assert lock._is_owned()                                      # ripreso anche su errore


def test_auto_next_payload_carries_event_identity():
    rc, bus = _make_controller(responses=[{"available": 150.0}])
    rc.mode = RuntimeMode.ACTIVE
    rc.risk_desk.sync_bankroll(100.0)
    close = _auto_next_close()
    close["mm_context"]["next_signal"].update({"event_id": "E9", "event": "Gamma v Delta"})

    rc._on_close_position(close)

    published = [p for t, p in bus.events if t == "CMD_QUICK_BET"]
    assert len(published) == 1
    assert published[0]["event_id"] == "E9"
    assert published[0]["event_identity"] == "id:E9"


def test_auto_next_cashout_invalidation_checkpoints_the_cycle_as_not_attempted():
    rc, bus = _make_controller(responses=[{"available": 150.0}])
    rc.mode = RuntimeMode.ACTIVE
    rc.risk_desk.sync_bankroll(100.0)
    checkpoints = []
    original = rc._persist_cycle_checkpoint

    def record(**kwargs):
        checkpoints.append(kwargs)
        return original(**kwargs)

    rc._persist_cycle_checkpoint = record

    def cashout_arrives():
        with rc._risk_admission_lock:
            rc._begin_cashout_barrier({"signal_type": "CASHOUT_ALL"})

    _wrap_book(rc, cashout_arrives)

    rc._on_close_position(_auto_next_close())

    last = checkpoints[-1]
    assert last["checkpoint_stage"] == "CYCLE_BLOCKED"
    assert last["next_trade_submission_status"] == "NOT_ATTEMPTED"
    assert last["reason"] == "cashout_invalidation_stale"
