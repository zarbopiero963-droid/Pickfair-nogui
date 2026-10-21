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
    rc, bus = _make_controller(responses=[{"available": 150.0, "exposure": 0.0}])
    rc.mode = RuntimeMode.ACTIVE
    rc.risk_desk.sync_bankroll(100.0)
    owned = []
    _wrap_book(rc, lambda: owned.append(rc._risk_admission_lock._is_owned()))

    rc._on_close_position(_auto_next_close())

    assert owned == [False]
    assert _auto_next_result(bus)["auto_trade_status"] == "AUTO_TRADE_SUBMITTED"


def test_auto_next_cashout_during_liquidity_fetch_blocks_the_entry():
    rc, bus = _make_controller(responses=[{"available": 150.0, "exposure": 0.0}])
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
    rc, bus = _make_controller(responses=[{"available": 150.0, "exposure": 0.0}])
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
    rc, bus = _make_controller(responses=[{"available": 150.0, "exposure": 0.0}])
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


# ---------------------------------------------------------------- rilievi Codex su ab0df6e (#522)
def _barrier(rc, signal):
    with rc._risk_admission_lock:
        return rc._begin_cashout_barrier(signal)


def test_event_only_cashout_fences_entries_of_the_same_event(tmp_path):
    """Codex r4315283877: il CashoutRouter chiude per `event_name` tutti i mercati della partita; la
    barriera deve coprire la stessa partita anche quando il CASHOUT porta solo il nome evento."""
    c = _catena(tmp_path)
    barrier_id, scope = _barrier(c.rc, {"signal_type": "CASHOUT", "event_name": "Alpha v Beta"})
    assert scope

    c.rc._on_signal_received(_signal(event="Alpha v Beta"))

    assert c.broker.state.orders == {}
    assert _rifiuti(c) == ["cashout_invalidation_active"]

    # Altra partita: nessun blocco.
    c.rc._on_signal_received(_signal(event="Gamma v Delta", market_id="1.777",
                                     received_at="2026-10-08T10:06:00+00:00"))
    assert len(c.broker.state.orders) == 1


def test_market_cashout_also_fences_other_markets_of_the_same_event(tmp_path):
    c = _catena(tmp_path)
    _barrier(c.rc, {"signal_type": "CASHOUT", "event_name": "Alpha v Beta",
                    "market_id": "1.234", "selection_id": 11})

    c.rc._on_signal_received(_signal(event="Alpha v Beta", market_id="1.999"))

    assert c.broker.state.orders == {}
    assert _rifiuti(c) == ["cashout_invalidation_active"]


def test_config_reload_during_network_window_blocks_the_entry(tmp_path):
    """Codex r4315283893: un reload_config durante la finestra di rete puo' installare cap piu' bassi;
    l'ingresso approvato con la config precedente non deve partire."""
    c = _catena(tmp_path)
    _wrap_book(c.rc, c.rc.reload_config)

    c.rc._on_signal_received(_signal())

    assert c.broker.state.orders == {}
    assert c.bus.payloads("CMD_QUICK_BET") == []
    assert _rifiuti(c) == ["config_cambiata_durante_ammissione"]
    assert all(t.status == "FREE" for t in c.rc.table_manager._tables.values())


def test_auto_next_config_reload_during_network_window_blocks_the_entry():
    rc, bus = _make_controller(responses=[{"available": 150.0, "exposure": 0.0}])
    rc.mode = RuntimeMode.ACTIVE
    rc.risk_desk.sync_bankroll(100.0)
    _wrap_book(rc, rc.reload_config)

    rc._on_close_position(_auto_next_close())

    result = _auto_next_result(bus)
    assert result["submitted"] is False
    assert result["reason"] == "config_cambiata_durante_ammissione"
    assert [t for t, _ in bus.events if t == "CMD_QUICK_BET"] == []
    assert all(t.status == "FREE" for t in rc.table_manager._tables.values())


def test_match_alias_is_a_valid_event_name_for_the_event_cap(tmp_path):
    """Codex r4315283886: `match` e' un alias supportato del nome evento (payload d'ordine)."""
    c = _catena(tmp_path, max_event_exposure_abs=50.0)
    sig = _signal(match="Alpha v Beta")
    sig.pop("event")

    c.rc._on_signal_received(sig)

    assert len(c.broker.state.orders) == 1, _rifiuti(c)


# ---------------------------------------------------------------- barriera viva fino all'esito dell'hedge
_CASHOUT = {"signal_type": "CASHOUT", "event_name": "Alpha v Beta", "market_id": "1.234", "selection_id": 11}


def _async_cashout_chain(c, monkeypatch, *, hedges=(("1.234", 11),)):
    """Router reale nel runtime, ma la sua `route` pubblica le REQ_EXECUTE_CASHOUT indicate; l'executor
    e' asincrono (bus di produzione non bloccante): la REQ resta in coda finche' il test non pubblica l'esito."""
    import cashout_router

    queued = []
    c.bus.subscribe("REQ_EXECUTE_CASHOUT", queued.append)
    monkeypatch.setattr(RuntimeController, "_cashout_chain_wired", lambda self: True)

    def route(router, signal):
        for market_id, selection_id in hedges:
            router.publish("REQ_EXECUTE_CASHOUT", {"market_id": market_id, "selection_id": selection_id,
                                                   "side": "LAY", "stake": 0.5, "price": 2.0})
        return {"published": len(hedges), "positions": len(hedges), "skipped": 0}

    monkeypatch.setattr(cashout_router.CashoutRouter, "route", route)
    svc = c.rc.betfair_service
    for name, value in (("is_simulation_mode", lambda: True), ("get_live_client", lambda: None),
                        ("get_simulation_broker", lambda: None), ("list_current_orders", lambda **_: [])):
        if not hasattr(svc, name):
            monkeypatch.setattr(svc, name, value, raising=False)
    return queued


def test_barrier_stays_active_until_the_hedge_execution_completes(tmp_path, monkeypatch):
    """Codex r4315283906 / mandato PF-522-FINAL-HARDENING-01: il publish di REQ_EXECUTE_CASHOUT non e'
    l'esecuzione. Finche' l'hedge non ha esito, nessun ingresso nello stesso ambito."""
    c = _catena(tmp_path)
    queued = _async_cashout_chain(c, monkeypatch)

    c.rc._route_cashout_signal(dict(_CASHOUT))
    assert len(queued) == 1

    c.rc._on_signal_received(_signal())                       # stesso mercato/selezione/partita
    assert c.broker.state.orders == {}
    assert _rifiuti(c) == ["cashout_invalidation_active"]
    assert c.rc.get_status()["cashout_barriers_active"] == 1

    c.bus.publish("CASHOUT_SUCCESS", {"partial": False, "market_id": "1.234", "selection_id": 11, "status": "DONE",
                                      "cashout_request_id": queued[0]["cashout_request_id"]})

    assert c.rc.get_status()["cashout_barriers_active"] == 0
    c.rc._on_signal_received(_signal(received_at="2026-10-08T10:09:00+00:00"))
    assert len(c.broker.state.orders) == 1


@pytest.mark.parametrize("status", ["FAILURE", "REJECTED"])
def test_barrier_released_by_any_certain_hedge_outcome(tmp_path, monkeypatch, status):
    c = _catena(tmp_path)
    queued = _async_cashout_chain(c, monkeypatch)
    c.rc._route_cashout_signal(dict(_CASHOUT))

    c.bus.publish("CASHOUT_FAILED", {"market_id": "1.234", "selection_id": "11", "status": status,
                                     "cashout_request_id": queued[0]["cashout_request_id"]})

    assert c.rc.get_status()["cashout_barriers_active"] == 0


@pytest.mark.parametrize("status", ["AMBIGUOUS", "ERROR"])
def test_uncertain_hedge_outcome_keeps_the_barrier(tmp_path, monkeypatch, status):
    c = _catena(tmp_path)
    queued = _async_cashout_chain(c, monkeypatch)
    c.rc._route_cashout_signal(dict(_CASHOUT))

    c.bus.publish("CASHOUT_FAILED", {"market_id": "1.234", "selection_id": 11, "status": status,
                                     "cashout_request_id": queued[0]["cashout_request_id"]})

    status_now = c.rc.get_status()
    assert status_now["cashout_barriers_active"] == 1
    assert status_now["cashout_barriers_uncertain"] == 1


def test_suppressed_request_releases_its_leg(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    queued = _async_cashout_chain(c, monkeypatch)
    c.rc._route_cashout_signal(dict(_CASHOUT))

    c.bus.publish("CASHOUT_REQUEST_SUPPRESSED", {"market_id": "1.234", "selection_id": 11,
                                                 "cashout_request_id": queued[0]["cashout_request_id"]})

    assert c.rc.get_status()["cashout_barriers_active"] == 0


def test_barrier_waits_for_every_hedge_of_the_cashout(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    queued = _async_cashout_chain(c, monkeypatch, hedges=(("1.234", 11), ("1.999", 7)))
    c.rc._route_cashout_signal(dict(_CASHOUT))
    first, second = (q["cashout_request_id"] for q in queued)

    c.bus.publish("CASHOUT_SUCCESS", {"partial": False, "market_id": "1.234", "selection_id": 11, "cashout_request_id": first})
    assert c.rc.get_status()["cashout_barriers_active"] == 1    # manca l'esito della seconda gamba
    c.rc._on_signal_received(_signal(event="Alpha v Beta", market_id="1.999"))
    assert c.broker.state.orders == {}

    c.bus.publish("CASHOUT_FAILED", {"market_id": "1.999", "selection_id": 7, "status": "FAILURE",
                                     "cashout_request_id": second})
    assert c.rc.get_status()["cashout_barriers_active"] == 0


def test_outcome_of_another_hedge_does_not_release_the_barrier(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    _async_cashout_chain(c, monkeypatch)
    c.rc._route_cashout_signal(dict(_CASHOUT))

    c.bus.publish("CASHOUT_SUCCESS", {"partial": False, "market_id": "1.234", "selection_id": 12, "cashout_request_id": "crq-x"})
    c.bus.publish("CASHOUT_SUCCESS", {"partial": False, "market_id": "1.555", "selection_id": 11})
    c.bus.publish("CASHOUT_FAILED", {"reason": "fetch_bot_orders:x", "status": "FAILURE"})

    assert c.rc.get_status()["cashout_barriers_active"] == 1


def test_cashout_without_hedges_releases_the_barrier_when_routing_ends(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    _async_cashout_chain(c, monkeypatch, hedges=())
    c.rc._route_cashout_signal(dict(_CASHOUT))

    assert c.rc.get_status()["cashout_barriers_active"] == 0
    c.rc._on_signal_received(_signal())
    assert len(c.broker.state.orders) == 1


def test_synchronous_hedge_outcome_during_routing_releases_the_barrier(tmp_path, monkeypatch):
    """Esito pubblicato prima che il routing ritorni (bus sincrono): la barriera non resta appesa."""
    c = _catena(tmp_path)
    _async_cashout_chain(c, monkeypatch)
    c.bus.subscribe("REQ_EXECUTE_CASHOUT", lambda req: c.bus.publish(
        "CASHOUT_SUCCESS", {"partial": False, "market_id": req["market_id"], "selection_id": req["selection_id"],
                            "cashout_request_id": req["cashout_request_id"]}))

    c.rc._route_cashout_signal(dict(_CASHOUT))

    assert c.rc.get_status()["cashout_barriers_active"] == 0


def test_hedge_outcome_arriving_from_another_thread_releases_the_barrier(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    queued = _async_cashout_chain(c, monkeypatch)
    c.rc._route_cashout_signal(dict(_CASHOUT))

    worker = threading.Thread(target=c.bus.publish,
                              args=("CASHOUT_SUCCESS", {"partial": False, "market_id": "1.234", "selection_id": 11,
                                                        "cashout_request_id": queued[0]["cashout_request_id"]}))
    worker.start()
    worker.join(5)

    assert not worker.is_alive()
    assert c.rc.get_status()["cashout_barriers_active"] == 0


# ---------------------------------------------------------------- catena cashout REALE (bridge + executor)
class _UnmatchedRouter:
    """OrderRouter finto: l'hedge viene piazzato ma resta non abbinato (esito UNMATCHED reale dell'executor)."""

    def __init__(self, result=None):
        self.calls = []
        self.result = result or {"placed": True, "matched": 0.0, "bet_id": "H1"}

    def place(self, payload):
        self.calls.append(payload)
        return dict(self.result)


def _real_cashout_chain(c, monkeypatch, *, result=None, residual_cancel_ok=True, persisted=None):
    """Router del runtime + CashoutRequestBridge, CashoutExecutor e CashoutResidualHandler reali sullo stesso bus."""
    import cashout_router
    from cashout_executor import CashoutExecutor
    from cashout_request_bridge import CashoutRequestBridge
    from cashout_residual_handler import CashoutResidualHandler

    bridge = CashoutRequestBridge(c.bus)
    bridge.wire()
    order_router = _UnmatchedRouter(result)
    CashoutExecutor(c.bus, order_router).wire()
    order_router.cancels = []

    def cancel(market_id, bet_ids):
        order_router.cancels.append((market_id, list(bet_ids)))
        if isinstance(residual_cancel_ok, Exception):
            raise residual_cancel_ok
        return residual_cancel_ok

    CashoutResidualHandler(cancel=cancel, persist=(persisted.append if persisted is not None else lambda r: None),
                           notify=lambda text, severity=None: None).wire(c.bus)
    monkeypatch.setattr(RuntimeController, "_cashout_chain_wired", lambda self: True)

    def route(router, signal):
        router.publish("REQ_EXECUTE_CASHOUT", {"market_id": "1.234", "selection_id": 11, "side": "LAY",
                                               "stake": 0.5, "price": 2.0, "green_up": 0.1})
        return {"published": 1, "positions": 1, "skipped": 0}

    monkeypatch.setattr(cashout_router.CashoutRouter, "route", route)
    svc = c.rc.betfair_service
    for name, value in (("is_simulation_mode", lambda: True), ("get_live_client", lambda: None),
                        ("get_simulation_broker", lambda: None), ("list_current_orders", lambda **_: [])):
        if not hasattr(svc, name):
            monkeypatch.setattr(svc, name, value, raising=False)
    return order_router


def test_bridge_duplicate_suppression_never_leaves_the_barrier_stuck(tmp_path, monkeypatch):
    """Verifica indipendente (P1): secondo CASHOUT sulla stessa gamba entro la finestra di dedup del bridge.
    Il bridge scarta la REQ duplicata; la barriera deve chiudersi comunque, non restare appesa fino al riavvio."""
    c = _catena(tmp_path)
    order_router = _real_cashout_chain(c, monkeypatch)

    c.rc._route_cashout_signal(dict(_CASHOUT))          # 1a: hedge UNMATCHED
    c.rc._route_cashout_signal(dict(_CASHOUT))          # 2a: REQ scartata come duplicata dal bridge

    assert len(order_router.calls) == 1
    assert c.rc.get_status()["cashout_barriers_active"] == 0
    c.rc._on_signal_received(_signal())
    assert len(c.broker.state.orders) == 1


def test_cashout_all_duplicate_suppression_never_blocks_every_entry(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    _real_cashout_chain(c, monkeypatch)

    c.rc._route_cashout_signal({"signal_type": "CASHOUT_ALL"})
    c.rc._route_cashout_signal({"signal_type": "CASHOUT_ALL"})

    assert c.rc.get_status()["cashout_barriers_active"] == 0


def test_ambiguous_hedge_keeps_the_barrier_until_restart(tmp_path, monkeypatch):
    """Esito ignoto dell'hedge (AMBIGUOUS): lo stato della posizione non e' noto, nessun ingresso nell'ambito."""
    c = _catena(tmp_path)
    _real_cashout_chain(c, monkeypatch, result={"order_unknown": True, "bet_id": None})

    c.rc._route_cashout_signal(dict(_CASHOUT))

    assert c.rc.get_status()["cashout_barriers_active"] == 1
    c.rc._on_signal_received(_signal())
    assert c.broker.state.orders == {}
    assert _rifiuti(c) == ["cashout_invalidation_active"]


def test_foreign_outcome_on_the_same_leg_does_not_release_the_barrier(tmp_path, monkeypatch):
    """Verifica indipendente (P2): l'esito di un hedge diverso (auto-green, UI) sulla stessa gamba non chiude la
    barriera del router: conta solo l'esito della richiesta pubblicata dal router."""
    c = _catena(tmp_path)
    queued = _async_cashout_chain(c, monkeypatch)
    c.rc._route_cashout_signal(dict(_CASHOUT))
    assert len(queued) == 1

    c.bus.publish("CASHOUT_SUCCESS", {"partial": False, "market_id": "1.234", "selection_id": 11, "status": "DONE"})

    assert c.rc.get_status()["cashout_barriers_active"] == 1
    c.bus.publish("CASHOUT_SUCCESS", {"partial": False, "market_id": "1.234", "selection_id": "11",
                                      "cashout_request_id": queued[0]["cashout_request_id"]})
    assert c.rc.get_status()["cashout_barriers_active"] == 0


# ---------------------------------------------------------------- barriera sul percorso grace (Timer)
class _FakeTimer:
    def __init__(self, barrier_id):
        self._cashout_admission_barrier_id = barrier_id
        self.cancelled = False

    def cancel(self):
        self.cancelled = True


def _grace_setup(c, monkeypatch):
    queued = _async_cashout_chain(c, monkeypatch)
    monkeypatch.setattr(RuntimeController, "_cashout_gates_still_open", lambda self, s, m="": (True, ""))
    barrier_id, _ = _barrier(c.rc, dict(_CASHOUT))
    return queued, barrier_id


def test_deferred_grace_route_registers_its_hedges_on_the_barrier(tmp_path, monkeypatch):
    """Mutazione M9 della verifica indipendente: il percorso differito deve registrare le gambe."""
    c = _catena(tmp_path)
    queued, barrier_id = _grace_setup(c, monkeypatch)

    c.rc._deferred_cashout_route(dict(_CASHOUT), ("1.234", "11"), "", None, barrier_id)

    assert len(queued) == 1
    assert c.rc.get_status()["cashout_barriers_active"] == 1
    c.bus.publish("CASHOUT_SUCCESS", {"partial": False, "market_id": "1.234", "selection_id": 11,
                                      "cashout_request_id": queued[0]["cashout_request_id"]})
    assert c.rc.get_status()["cashout_barriers_active"] == 0


def test_cancel_before_the_grace_timer_starts_closes_the_barrier_and_routes_nothing(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    queued, barrier_id = _grace_setup(c, monkeypatch)
    c.rc._auto_green_pending[("1.234", "11")] = _FakeTimer(barrier_id)

    c.rc._cancel_pending_auto_green()
    c.rc._deferred_cashout_route(dict(_CASHOUT), ("1.234", "11"), "", None, barrier_id)   # corsa del Timer

    assert queued == []
    assert c.rc.get_status()["cashout_barriers_active"] == 0


def test_cancel_after_the_grace_timer_started_keeps_the_barrier_until_the_hedge_outcome(tmp_path, monkeypatch):
    """Verifica indipendente (P2): il cancel non deve chiudere la barriera di un Timer gia' partito."""
    c = _catena(tmp_path)
    queued, barrier_id = _grace_setup(c, monkeypatch)
    c.rc._auto_green_pending[("1.234", "11")] = _FakeTimer(barrier_id)

    def route_with_cancel_in_between(router, signal):
        c.rc._cancel_pending_auto_green()          # CASHOUT_ALL concorrente mentre il Timer instrada
        router.publish("REQ_EXECUTE_CASHOUT", {"market_id": "1.234", "selection_id": 11, "side": "LAY",
                                               "stake": 0.5, "price": 2.0})
        return {"published": 1}

    import cashout_router
    monkeypatch.setattr(cashout_router.CashoutRouter, "route", route_with_cancel_in_between)

    c.rc._deferred_cashout_route(dict(_CASHOUT), ("1.234", "11"), "", None, barrier_id)

    assert len(queued) == 1
    assert c.rc.get_status()["cashout_barriers_active"] == 1
    c.bus.publish("CASHOUT_FAILED", {"market_id": "1.234", "selection_id": 11, "status": "FAILURE",
                                     "cashout_request_id": queued[0]["cashout_request_id"]})
    assert c.rc.get_status()["cashout_barriers_active"] == 0


def test_request_id_travels_through_the_real_bridge_and_executor(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    order_router = _real_cashout_chain(c, monkeypatch)

    c.rc._route_cashout_signal(dict(_CASHOUT))

    req = c.bus.payloads("REQ_EXECUTE_CASHOUT")[-1]
    cmd = c.bus.payloads("CMD_EXECUTE_CASHOUT")[-1]
    outcome = c.bus.payloads("CASHOUT_FAILED")[-1]
    assert req["cashout_request_id"].startswith("crq-")
    assert cmd["cashout_request_id"] == req["cashout_request_id"]
    assert outcome["cashout_request_id"] == req["cashout_request_id"]
    assert outcome["status"] == "UNMATCHED"
    assert len(order_router.calls) == 1


@pytest.mark.parametrize("signal", [None, "BACK", 3, ["LAY"]])
def test_signal_side_with_a_non_dict_signal_is_a_value_error_or_the_default(signal):
    """Verifica indipendente (P2): un segnale non-dict non deve mai sollevare AttributeError."""
    from core import validators

    assert validators.signal_side(signal, default="BACK") == "BACK"
    with pytest.raises(ValueError):
        validators.signal_side(signal)


def test_bridge_rejection_reports_the_request_id_and_releases_the_barrier(tmp_path, monkeypatch):
    """Mutazione M24: un REJECTED del bridge (REQ non valida) deve riportare il cashout_request_id, altrimenti la
    barriera resterebbe appesa fino al riavvio."""
    import cashout_router

    c = _catena(tmp_path)
    _real_cashout_chain(c, monkeypatch)

    def route(router, signal):
        router.publish("REQ_EXECUTE_CASHOUT", {"market_id": "1.234", "selection_id": 11, "side": "BOTH",
                                               "stake": 0.5, "price": 2.0, "green_up": 0.1})
        return {"published": 1}

    monkeypatch.setattr(cashout_router.CashoutRouter, "route", route)
    c.rc._route_cashout_signal(dict(_CASHOUT))

    req = c.bus.payloads("REQ_EXECUTE_CASHOUT")[-1]
    rejected = c.bus.payloads("CASHOUT_FAILED")[-1]
    assert rejected["status"] == "REJECTED"
    assert rejected["cashout_request_id"] == req["cashout_request_id"]
    assert c.rc.get_status()["cashout_barriers_active"] == 0



# ---------------------------------------------------------------- rilievi Codex su c4ac69e
def test_unmatched_hedge_keeps_the_barrier_until_its_cancel_is_confirmed(tmp_path, monkeypatch):
    """Codex r4316502584: UNMATCHED = hedge a riposo, puo' ancora abbinarsi. La barriera si chiude solo quando il
    CashoutResidualHandler reale conferma il cancel."""
    c = _catena(tmp_path)
    order_router = _real_cashout_chain(c, monkeypatch, residual_cancel_ok=True)

    c.rc._route_cashout_signal(dict(_CASHOUT))

    assert order_router.cancels == [("1.234", ["H1"])]
    assert c.bus.payloads("CASHOUT_RESIDUAL_HANDLED")[-1]["cancel_ok"] is True
    assert c.rc.get_status()["cashout_barriers_active"] == 0


@pytest.mark.parametrize("cancel_result", [False, RuntimeError("exchange")])
def test_unmatched_hedge_with_unconfirmed_cancel_keeps_entries_blocked(tmp_path, monkeypatch, cancel_result):
    c = _catena(tmp_path)
    _real_cashout_chain(c, monkeypatch, residual_cancel_ok=cancel_result)

    c.rc._route_cashout_signal(dict(_CASHOUT))

    status = c.rc.get_status()
    assert status["cashout_barriers_active"] == 1
    assert status["cashout_barriers_uncertain"] == 1
    c.rc._on_signal_received(_signal())
    assert c.broker.state.orders == {}
    assert _rifiuti(c) == ["cashout_invalidation_active"]


def test_unmatched_outcome_alone_does_not_release_the_barrier(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    queued = _async_cashout_chain(c, monkeypatch)
    c.rc._route_cashout_signal(dict(_CASHOUT))

    c.bus.publish("CASHOUT_FAILED", {"market_id": "1.234", "selection_id": 11, "status": "UNMATCHED",
                                     "cashout_request_id": queued[0]["cashout_request_id"]})

    assert c.rc.get_status()["cashout_barriers_active"] == 1


@pytest.mark.parametrize("aliases", [
    {"event": "Alpha v Beta", "event_name": "Gamma v Delta"},
    {"event": "Alpha v Beta", "match": "Gamma v Delta"},
    {"match": "Alpha v Beta", "event_name": "Gamma v Delta"},
])
def test_conflicting_event_name_aliases_are_rejected(tmp_path, aliases):
    """Codex r4316502602: cap e barriera devono misurare lo stesso evento che l'ordine registra."""
    c = _catena(tmp_path, max_event_exposure_abs=50.0)
    sig = _signal()
    sig.pop("event")
    sig.update(aliases)

    c.rc._on_signal_received(sig)

    assert c.broker.state.orders == {}
    assert _rifiuti(c) == ["evento_ambiguo"]


def test_same_event_written_differently_across_aliases_is_one_event(tmp_path):
    c = _catena(tmp_path, max_event_exposure_abs=50.0)
    sig = _signal(event="Alpha v Beta", event_name="  alpha  V beta ")

    c.rc._on_signal_received(sig)

    assert len(c.broker.state.orders) == 1, _rifiuti(c)
    (table,) = [t for t in c.rc.table_manager._tables.values() if t.status != "FREE"]
    assert table.meta["event_name"] == "Alpha v Beta"


def test_queued_entry_is_not_placed_after_a_cashout_started_in_its_scope(tmp_path):
    """Codex r4316502592: col bus non bloccante CMD_QUICK_BET e' solo accodato. Un cashout che parte prima che
    l'engine lo esegua invalida l'ingresso: il TradingEngine lo nega prima del piazzamento."""
    c = _catena(tmp_path)
    held = []
    original_publish = c.bus.publish

    def queueing_publish(topic, payload=None):
        if topic == "CMD_QUICK_BET":
            held.append(payload)                       # accodato, non ancora eseguito
            c.bus.events.append((topic, dict(payload)))
            return None
        return original_publish(topic, payload)

    c.bus.publish = queueing_publish
    c.rc._on_signal_received(_signal())
    assert len(held) == 1 and c.broker.state.orders == {}

    _barrier(c.rc, {"signal_type": "CASHOUT", "event_name": "Alpha v Beta"})   # cashout nel suo ambito
    c.bus.publish = original_publish
    for handler in list(c.bus.subscribers.get("CMD_QUICK_BET", [])):
        handler(held[0])                               # l'engine esegue ora l'ordine in coda

    assert c.broker.state.orders == {}
    failed = c.bus.payloads("QUICK_BET_FAILED")
    assert failed and failed[-1]["status"] == "DENIED"
    assert c.rc._queued_entry_fences == {}             # registrazione consumata dal controllo dell'engine
    assert all(t.status == "FREE" for t in c.rc.table_manager._tables.values())


def test_queued_entry_without_a_cashout_is_placed_normally(tmp_path):
    c = _catena(tmp_path)
    c.rc._on_signal_received(_signal())
    assert len(c.broker.state.orders) == 1


def _dutching(c):
    from controllers.dutching_controller import DutchingController

    return DutchingController(bus=c.bus, runtime_controller=c.rc)


def test_manual_bet_respects_the_configured_absolute_order_cap(tmp_path):
    """Codex r4316502610: i cap assoluti configurati valgono anche per il bet manuale."""
    c = _catena(tmp_path, max_order_exposure=1.0)
    res = _dutching(c).manual_bet({"market_id": "1.234", "selection_id": 11, "price": 2.0, "stake": 5.0,
                                   "bet_type": "BACK", "event_name": "Alpha v Beta"})
    assert res["ok"] is False
    assert "max_order_exposure_exceeded" in res["error"]
    assert c.bus.payloads("CMD_QUICK_BET") == []


def test_manual_bet_respects_the_configured_market_cap_with_existing_exposure(tmp_path):
    from tests.acceptance.test_issue437_pr28d import _existing_position

    c = _catena(tmp_path, max_market_exposure=2.0)
    _existing_position(c, _signal(selection_id=12), exposure=1.5)
    res = _dutching(c).manual_bet({"market_id": "1.234", "selection_id": 11, "price": 2.0, "stake": 0.6,
                                   "bet_type": "BACK", "event_name": "Alpha v Beta"})
    assert res["ok"] is False and "max_market_exposure_exceeded" in res["error"]


def test_dutching_batch_respects_the_configured_absolute_caps(tmp_path):
    c = _catena(tmp_path, max_market_exposure=2.0)
    reason = c.rc.absolute_cap_reason_for_orders([
        {"market_id": "1.234", "selection_id": 11, "bet_type": "BACK", "price": 3.0, "stake": 1.0,
         "event_name": "Alpha v Beta"},
        {"market_id": "1.234", "selection_id": 12, "bet_type": "BACK", "price": 3.0, "stake": 1.5,
         "event_name": "Alpha v Beta"},
    ])
    assert reason.startswith("max_market_exposure_exceeded")      # 1.0 + 1.5 > 2.0 sullo stesso mercato


def test_direct_orders_without_configured_caps_are_not_limited(tmp_path):
    c = _catena(tmp_path)
    assert c.rc.absolute_cap_reason_for_orders([
        {"market_id": "1.234", "selection_id": 11, "bet_type": "LAY", "price": 50.0, "stake": 100.0}]) == ""


def test_dutching_precheck_applies_the_cap_before_any_side_effect(monkeypatch):
    """Il precheck Dutching consulta i cap assoluti del runtime PRIMA del dedupe: rifiuto, nessuna chiave presa."""
    from tests.integration.test_dutching_max_win_guard import _Runtime, _controller, _patch_results, _payload

    _patch_results(monkeypatch, [{"selectionId": 1, "price": 2.0, "stake": 60.0, "side": "BACK"},
                                 {"selectionId": 2, "price": 3.0, "stake": 40.0, "side": "BACK"}])
    rt = _Runtime()
    seen = []
    rt.absolute_cap_reason_for_orders = lambda orders: seen.append(orders) or "max_order_exposure_exceeded:limit=1€"
    acquired = []
    rt.duplication_guard.acquire = lambda key: acquired.append(key) or True

    res = _controller(rt).precheck(_payload())

    assert res["ok"] is False and "max_order_exposure_exceeded" in res["error"]
    assert acquired == []
    assert [(o["selection_id"], o["bet_type"], o["stake"], o["price"]) for o in seen[0]] == [
        (1, "BACK", 60.0, 2.0), (2, "BACK", 40.0, 3.0)]


# ---------------------------------------------------------------- verifica indipendente su 93c67cf (#522)
def _queue_one_entry(c):
    held = []
    original_publish = c.bus.publish

    def queueing_publish(topic, payload=None):
        if topic == "CMD_QUICK_BET":
            held.append(payload)
            return None
        return original_publish(topic, payload)

    c.bus.publish = queueing_publish
    c.rc._on_signal_received(_signal())
    c.bus.publish = original_publish
    assert len(held) == 1 and len(c.rc._queued_entry_fences) == 1
    return held[0]


def _run_queued(c, payload):
    for handler in list(c.bus.subscribers.get("CMD_QUICK_BET", [])):
        handler(payload)


def test_fence_registration_is_consumed_when_safe_mode_denies_the_order(tmp_path):
    """Un ordine negato dal safe mode dell'engine non lascia la sua registrazione del fence in memoria."""
    c = _catena(tmp_path)
    payload = _queue_one_entry(c)
    c.engine._is_safe_mode_enabled = lambda: True

    _run_queued(c, payload)

    assert c.broker.state.orders == {}
    assert c.bus.payloads("QUICK_BET_FAILED")[-1]["status"] == "DENIED"
    assert c.rc._queued_entry_fences == {}


def test_fence_registration_is_consumed_when_the_risk_gate_denies_the_order(tmp_path):
    c = _catena(tmp_path)
    payload = _queue_one_entry(c)
    c.engine.risk_middleware.check = lambda request: {"allowed": False, "reason": "negato", "payload": request}

    _run_queued(c, payload)

    assert c.broker.state.orders == {}
    assert c.rc._queued_entry_fences == {}


@pytest.mark.parametrize("verdict,reason", [
    (RuntimeError("fence rotto"), "ADMISSION_FENCE_RAISED"),
    (True, "ADMISSION_FENCE_BAD_RESULT"),
    (["x"], "ADMISSION_FENCE_BAD_RESULT"),
])
def test_engine_denies_the_order_when_the_fence_check_is_not_verifiable(tmp_path, verdict, reason):
    """Fence che solleva o risponde un tipo inatteso => DENY (fail-closed), mai ordine ammesso."""
    c = _catena(tmp_path)

    def broken(request):
        if isinstance(verdict, Exception):
            raise verdict
        return verdict

    c.rc.admission_fence_verdict = broken
    c.rc._on_signal_received(_signal())

    assert c.broker.state.orders == {}
    assert c.engine._consume_admission_fence({"customer_ref": "r"}) == reason
    assert c.bus.payloads("QUICK_BET_FAILED")[-1]["status"] == "DENIED"


def test_auto_next_entry_registers_its_fence_for_the_engine():
    rc, bus = _make_controller(responses=[{"available": 150.0, "exposure": 0.0}])
    rc.mode = RuntimeMode.ACTIVE
    rc.risk_desk.sync_bankroll(100.0)

    rc._on_close_position(_auto_next_close())

    (published,) = [p for t, p in bus.events if t == "CMD_QUICK_BET"]
    assert published["customer_ref"] in rc._queued_entry_fences
    _barrier(rc, {"signal_type": "CASHOUT_ALL"})                    # cashout prima dell'esecuzione in coda
    assert rc.admission_fence_verdict(published) == "cashout_invalidation_stale"
    assert rc._queued_entry_fences == {}


def test_direct_order_cap_measures_lay_liability_not_the_stake(tmp_path):
    c = _catena(tmp_path, max_order_exposure=1.5)
    lay = {"market_id": "1.234", "selection_id": 11, "bet_type": "LAY", "price": 3.0, "stake": 1.0}
    back = dict(lay, bet_type="BACK")
    assert c.rc.absolute_cap_reason_for_orders([lay]).startswith("max_order_exposure_exceeded")  # liability 2.0
    assert c.rc.absolute_cap_reason_for_orders([back]) == ""                                      # rischio 1.0


@pytest.mark.parametrize("result", [RuntimeError("runtime rotto"), True, 0])
def test_manual_bet_is_blocked_when_the_cap_check_is_not_verifiable(tmp_path, result):
    c = _catena(tmp_path)

    def broken(orders):
        if isinstance(result, Exception):
            raise result
        return result

    c.rc.absolute_cap_reason_for_orders = broken
    res = _dutching(c).manual_bet({"market_id": "1.234", "selection_id": 11, "price": 2.0, "stake": 1.0,
                                   "bet_type": "BACK", "event_name": "Alpha v Beta"})
    assert res["ok"] is False and "cap_assoluti_non_verificabili" in res["error"]
    assert c.bus.payloads("CMD_QUICK_BET") == []


def test_direct_order_without_event_identity_is_refused_when_the_event_cap_is_active(tmp_path):
    c = _catena(tmp_path, max_event_exposure_abs=50.0)
    order = {"market_id": "1.234", "selection_id": 11, "bet_type": "BACK", "price": 2.0, "stake": 1.0}
    assert c.rc.absolute_cap_reason_for_orders([order]) == "max_event_exposure_abs_non_verificabile"
    assert c.rc.absolute_cap_reason_for_orders([dict(order, event_name="Alpha v Beta")]) == ""


def test_direct_orders_respect_the_configured_absolute_drawdown(tmp_path):
    order = {"market_id": "1.234", "selection_id": 11, "bet_type": "BACK", "price": 2.0, "stake": 1.0}
    c = _catena(tmp_path, max_drawdown_abs=200.0)
    c.rc.risk_desk.bankroll_current = 900.0                         # drawdown 100 < 200
    assert c.rc.absolute_cap_reason_for_orders([order]) == ""

    c.rc.config.max_drawdown_abs = 50.0                             # drawdown 100 >= 50
    assert c.rc.absolute_cap_reason_for_orders([order]) == "max_drawdown_abs_active"
    assert c.rc.mode == RuntimeMode.LOCKDOWN


@pytest.mark.parametrize("alias", ["event", "match"])
def test_cashout_naming_the_event_by_alias_fences_entries_of_that_event(tmp_path, alias):
    c = _catena(tmp_path)
    _barrier_id, scope = _barrier(c.rc, {"signal_type": "CASHOUT", alias: "Alpha v Beta"})
    assert ("event", "alpha v beta") in scope

    c.rc._on_signal_received(_signal(event="Alpha v Beta"))

    assert c.broker.state.orders == {}
    assert _rifiuti(c) == ["cashout_invalidation_active"]


# ---------------------------------------------------------------- verifica owner pre-merge (#522)
# 1. Finestra fra il controllo del fence nell'engine e l'invio: con il bus di produzione l'engine piazza fuori
#    dal lock d'ammissione. Un cashout che parte in quella finestra attende l'esito dell'invio prima di
#    fotografare l'exchange, cosi' nessun ordine viene piazzato senza essere visto dal router.
def _entry_paused_inside_the_broker(c):
    import functools

    in_place, release = threading.Event(), threading.Event()
    original = c.broker.place_bet

    @functools.wraps(original)
    def slow_place(*args, **kwargs):
        in_place.set()
        assert release.wait(10)
        return original(*args, **kwargs)

    c.broker.place_bet = slow_place
    payload = _queue_one_entry(c)
    worker = threading.Thread(target=_run_queued, args=(c, payload))        # worker del bus, senza lock
    return worker, in_place, release


def _snapshotting_router(c, monkeypatch):
    import cashout_router

    snapshots = []
    monkeypatch.setattr(RuntimeController, "_cashout_chain_wired", lambda self: True)

    def route(router, signal):
        snapshots.append(list(c.broker.get_current_orders(None) or []))     # la fotografia del router
        return {"published": 0, "positions": 0, "skipped": 0}

    monkeypatch.setattr(cashout_router.CashoutRouter, "route", route)
    for name, value in (("is_simulation_mode", lambda: True), ("get_simulation_broker", lambda: c.broker)):
        monkeypatch.setattr(c.rc.betfair_service, name, value, raising=False)
    return snapshots


_EVENT_CASHOUT = {"signal_type": "CASHOUT", "event_name": "Alpha v Beta", "market_id": "1.234", "selection_id": 11}


@pytest.mark.parametrize("run", range(5))
def test_cashout_starting_while_an_admitted_entry_is_being_sent_sees_that_order(tmp_path, monkeypatch, run):
    c = _catena(tmp_path)
    snapshots = _snapshotting_router(c, monkeypatch)
    entry, in_place, release = _entry_paused_inside_the_broker(c)
    entry.start()
    assert in_place.wait(10)                                    # fence superato, ordine dentro l'invio
    cashout = threading.Thread(target=c.rc._route_cashout_signal, args=(dict(_EVENT_CASHOUT),))
    cashout.start()
    cashout.join(0.3)
    assert cashout.is_alive() and snapshots == []               # il router attende l'esito dell'invio
    release.set()
    entry.join(10)
    cashout.join(10)
    assert not entry.is_alive() and not cashout.is_alive()

    assert len(c.broker.state.orders) == 1
    assert len(snapshots) == 1 and len(snapshots[0]) == 1      # la fotografia contiene l'ordine piazzato
    assert c.rc._entries_in_flight == {}
    assert c.rc.get_status()["cashout_barriers_uncertain"] == 0


def test_entry_sent_out_of_the_cashout_scope_does_not_delay_the_cashout(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    snapshots = _snapshotting_router(c, monkeypatch)
    entry, in_place, release = _entry_paused_inside_the_broker(c)
    entry.start()
    assert in_place.wait(10)
    c.rc._route_cashout_signal({"signal_type": "CASHOUT", "event_name": "Gamma v Delta",
                                "market_id": "1.999", "selection_id": 7})
    assert snapshots == [[]]                                    # altro ambito: nessuna attesa
    release.set()
    entry.join(10)
    assert len(c.broker.state.orders) == 1


def test_entry_still_in_flight_after_the_wait_makes_the_barrier_uncertain(tmp_path, monkeypatch):
    """Il cashout non viene mai bloccato (PF-EXEC-03): attesa scaduta => fotografia comunque, barriera incerta."""
    c = _catena(tmp_path)
    snapshots = _snapshotting_router(c, monkeypatch)
    monkeypatch.setattr(RuntimeController, "_ENTRIES_IN_FLIGHT_WAIT_S", 0.05)
    entry, in_place, release = _entry_paused_inside_the_broker(c)
    entry.start()
    assert in_place.wait(10)

    c.rc._route_cashout_signal(dict(_EVENT_CASHOUT))

    assert len(snapshots) == 1
    status = c.rc.get_status()
    assert status["cashout_barriers_active"] == 1 and status["cashout_barriers_uncertain"] == 1
    release.set()
    entry.join(10)
    c.rc._on_signal_received(_signal(received_at="2026-10-08T10:09:00+00:00"))
    assert _rifiuti(c)[-1] == "cashout_invalidation_active"   # nessun nuovo ingresso nell'ambito


def test_cashout_routed_while_holding_the_admission_lock_does_not_wait_and_is_uncertain(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    _snapshotting_router(c, monkeypatch)
    barrier_id, _scope = _barrier(c.rc, dict(_EVENT_CASHOUT))
    c.rc._entries_in_flight = {"ref-in-volo": (frozenset(c.rc._entry_admission_keys(_signal())), {})}
    import time as _time

    started = _time.monotonic()
    with c.rc._risk_admission_lock:
        c.rc._await_entries_in_flight(barrier_id)               # l'engine non potrebbe segnalare l'esito
    assert _time.monotonic() - started < 1.0                    # nessuna attesa: sarebbe un deadlock a tempo
    assert c.rc.get_status()["cashout_barriers_uncertain"] == 1


@pytest.mark.parametrize("failure", ["raises", "denied"])
def test_in_flight_registration_is_closed_whatever_the_send_outcome(tmp_path, failure):
    c = _catena(tmp_path)
    payload = _queue_one_entry(c)
    if failure == "raises":
        def boom(*_a, **_k):
            raise RuntimeError("rete")
        c.broker.place_bet = boom
    else:
        c.engine.risk_middleware.check = lambda request: {"allowed": False, "reason": "no", "payload": request}

    _run_queued(c, payload)

    assert getattr(c.rc, "_entries_in_flight", {}) == {}


def test_in_flight_registration_is_closed_after_a_normal_placement(tmp_path):
    c = _catena(tmp_path)
    c.rc._on_signal_received(_signal())
    assert len(c.broker.state.orders) == 1
    assert getattr(c.rc, "_entries_in_flight", {}) == {}


# 2. Hedge abbinato in parte: il residuo a riposo resta registrato (record PARTIAL) e protetto (barriera) fino
#    al cancel confermato; cancel non confermato => barriera incerta fino alla riconciliazione al riavvio.
def test_partially_matched_hedge_keeps_the_barrier_until_the_residual_cancel_is_confirmed(tmp_path, monkeypatch):
    persisted = []
    c = _catena(tmp_path)
    order_router = _real_cashout_chain(c, monkeypatch, persisted=persisted,
                                       result={"placed": True, "matched": 0.2, "bet_id": "H1"})
    c.rc._route_cashout_signal(dict(_CASHOUT))

    success = c.bus.payloads("CASHOUT_SUCCESS")[-1]
    assert success["partial"] is True and success["requested"] == 0.5 and abs(success["residual"] - 0.3) < 1e-9
    assert order_router.cancels == [("1.234", ["H1"])]                 # un solo cancel del residuo
    (record,) = [r for r in persisted if r.get("status") == "PARTIAL"]
    assert record["bet_id"] == "H1" and abs(record["residual"] - 0.3) < 1e-9 and record["cancel_ok"] is True
    assert c.rc.get_status()["cashout_barriers_active"] == 0           # residuo cancellato: barriera chiusa


@pytest.mark.parametrize("cancel_result", [False, RuntimeError("rete")])
def test_partially_matched_hedge_with_unconfirmed_cancel_keeps_entries_blocked(tmp_path, monkeypatch, cancel_result):
    c = _catena(tmp_path)
    _real_cashout_chain(c, monkeypatch, residual_cancel_ok=cancel_result,
                        result={"placed": True, "matched": 0.2, "bet_id": "H1"})
    c.rc._route_cashout_signal(dict(_CASHOUT))

    status = c.rc.get_status()
    assert status["cashout_barriers_active"] == 1 and status["cashout_barriers_uncertain"] == 1
    c.rc._on_signal_received(_signal())
    assert c.broker.state.orders == {} and _rifiuti(c) == ["cashout_invalidation_active"]


def test_partially_matched_hedge_without_bet_id_keeps_the_barrier(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    order_router = _real_cashout_chain(c, monkeypatch, result={"placed": True, "matched": 0.2, "bet_id": ""})
    c.rc._route_cashout_signal(dict(_CASHOUT))
    assert order_router.cancels == []
    assert c.rc.get_status()["cashout_barriers_uncertain"] == 1


def test_fully_matched_hedge_releases_the_barrier_without_any_cancel(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    order_router = _real_cashout_chain(c, monkeypatch, result={"placed": True, "matched": 0.5, "bet_id": "H1"})
    c.rc._route_cashout_signal(dict(_CASHOUT))
    assert c.bus.payloads("CASHOUT_SUCCESS")[-1]["partial"] is False
    assert order_router.cancels == []
    assert c.rc.get_status()["cashout_barriers_active"] == 0


def test_success_without_a_declared_match_completeness_keeps_the_barrier(tmp_path, monkeypatch):
    """Fail-closed: un CASHOUT_SUCCESS che non dichiara `partial=False` non chiude la richiesta da solo."""
    c = _catena(tmp_path)
    queued = _async_cashout_chain(c, monkeypatch)
    c.rc._route_cashout_signal(dict(_CASHOUT))
    c.bus.publish("CASHOUT_SUCCESS", {"market_id": "1.234", "selection_id": 11, "status": "DONE",
                                      "cashout_request_id": queued[0]["cashout_request_id"]})
    assert c.rc.get_status()["cashout_barriers_active"] == 1


# Review indipendente su bdf6ece: esito AMBIGUOUS (l'engine smette di attendere, il task puo' ancora inviare) e
# registrazione chiusa da un'altra richiesta con lo stesso customer_ref.
class _PoolExecutor:
    """Come ExecutorManager: submit() restituisce un Future; default_timeout corto per il test."""
    thread_name_prefix = "pr28d-exec"
    default_timeout = 0.2

    def __init__(self):
        import concurrent.futures

        self._pool = concurrent.futures.ThreadPoolExecutor(max_workers=2, thread_name_prefix="pr28d-exec")

    def submit(self, _name, fn, *args, **kwargs):
        return self._pool.submit(fn, *args, **kwargs)


def test_ambiguous_send_makes_a_later_cashout_in_scope_uncertain(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    snapshots = _snapshotting_router(c, monkeypatch)
    c.engine.executor = _PoolExecutor()
    entry, in_place, release = _entry_paused_inside_the_broker(c)
    entry.start()
    assert in_place.wait(10)
    entry.join(10)                                       # attesa dell'executor scaduta => AMBIGUOUS
    assert not entry.is_alive() and c.bus.payloads("QUICK_BET_AMBIGUOUS")

    import time as _time

    started = _time.monotonic()
    c.rc._route_cashout_signal(dict(_EVENT_CASHOUT))     # il task del broker puo' ancora inviare
    assert _time.monotonic() - started < 5.0             # esito ambiguo: incerta subito, nessuna attesa inutile
    release.set()

    assert len(snapshots) == 1                           # il cashout non viene bloccato (PF-EXEC-03)
    status = c.rc.get_status()
    assert status["cashout_barriers_active"] == 1 and status["cashout_barriers_uncertain"] == 1
    c.rc._on_signal_received(_signal(received_at="2026-10-08T10:09:00+00:00"))
    assert _rifiuti(c)[-1] == "cashout_invalidation_active"


def test_cashout_out_of_scope_of_an_ambiguous_entry_stays_certain(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    _snapshotting_router(c, monkeypatch)
    c.rc._entries_ambiguous = {"ref-ambiguo": (frozenset(c.rc._entry_admission_keys(_signal())), "corr-a")}
    c.rc._route_cashout_signal({"signal_type": "CASHOUT", "event_name": "Gamma v Delta",
                                "market_id": "1.999", "selection_id": 7})
    assert c.rc.get_status()["cashout_barriers_uncertain"] == 0


def test_known_outcome_of_the_order_resolves_the_ambiguous_entry(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    _snapshotting_router(c, monkeypatch)
    c.rc._entries_ambiguous = {"ref-ambiguo": (frozenset(c.rc._entry_admission_keys(_signal())), "corr-a")}
    c.bus.publish("QUICK_BET_AMBIGUOUS", {"customer_ref": "ref-ambiguo", "correlation_id": "corr-a"})
    assert "ref-ambiguo" in c.rc._entries_ambiguous         # ancora ambiguo: nessun esito noto
    c.bus.publish("QUICK_BET_FILLED", {"customer_ref": "ref-ambiguo", "correlation_id": "corr-a",
                                       "market_id": "1.234"})
    assert c.rc._entries_ambiguous == {}
    c.rc._route_cashout_signal(dict(_EVENT_CASHOUT))
    assert c.rc.get_status()["cashout_barriers_uncertain"] == 0


def test_another_request_with_the_same_ref_does_not_close_the_registration(tmp_path):
    c = _catena(tmp_path)
    registered = {"customer_ref": "ref-1"}
    keys = frozenset(c.rc._entry_admission_keys(_signal()))
    c.rc._entries_in_flight = {"ref-1": (keys, registered)}

    c.rc.admission_placement_done({"customer_ref": "ref-1"})        # duplicato negato prima dell'invio
    assert "ref-1" in c.rc._entries_in_flight
    c.rc.admission_placement_done(registered)                        # la richiesta che si era registrata
    assert c.rc._entries_in_flight == {}


def test_uncertain_send_outcome_moves_the_entry_among_the_ambiguous(tmp_path):
    c = _catena(tmp_path)
    registered = {"customer_ref": "ref-2"}
    c.rc._entries_in_flight = {"ref-2": (frozenset({("global",)}), registered)}
    c.rc.admission_placement_done(registered, certain=False)
    assert c.rc._entries_in_flight == {} and c.rc._entries_ambiguous == {"ref-2": (frozenset({("global",)}), "")}


@pytest.mark.parametrize("status", ["DUPLICATE_BLOCKED", "DENIED"])
def test_pre_send_rejection_of_a_duplicate_does_not_resolve_the_ambiguous_entry(tmp_path, status):
    """Review indipendente su 4f223ea (b1/b2): un duplicato con lo stesso ref, bloccato o negato prima dell'invio,
    pubblica un esito che non riguarda l'ordine ambiguo."""
    c = _catena(tmp_path)
    c.rc._entries_ambiguous = {"ref-a": (frozenset({("global",)}), "corr-a")}
    topic = "QUICK_BET_SUCCESS" if status == "DUPLICATE_BLOCKED" else "QUICK_BET_FAILED"
    c.bus.publish(topic, {"customer_ref": "ref-a", "correlation_id": "corr-a", "status": status})
    assert "ref-a" in c.rc._entries_ambiguous


def test_outcome_of_another_correlation_does_not_resolve_the_ambiguous_entry(tmp_path):
    c = _catena(tmp_path)
    c.rc._entries_ambiguous = {"ref-a": (frozenset({("global",)}), "corr-a")}
    c.bus.publish("QUICK_BET_FILLED", {"customer_ref": "ref-a", "correlation_id": "corr-altra"})
    assert "ref-a" in c.rc._entries_ambiguous


@pytest.mark.parametrize("state", ["in_flight", "ambiguous"])
def test_second_admission_with_the_same_ref_is_denied_and_keeps_the_first(tmp_path, state):
    """Review indipendente su 4f223ea (b3): la seconda ammissione non sovrascrive la registrazione del primo."""
    c = _catena(tmp_path)
    first = {"customer_ref": "ref-1"}
    keys = frozenset(c.rc._entry_admission_keys(_signal()))
    if state == "in_flight":
        c.rc._entries_in_flight = {"ref-1": (keys, first)}
    else:
        c.rc._entries_ambiguous = {"ref-1": (keys, "")}
    with c.rc._risk_admission_lock:
        fence = c.rc._capture_admission_fence(_signal())
    second = {"customer_ref": "ref-1"}
    c.rc._register_queued_entry_fence(second, fence)

    assert c.rc.admission_fence_verdict(second) == "ingresso_gia_in_invio"
    if state == "in_flight":
        assert c.rc._entries_in_flight == {"ref-1": (keys, first)}
    else:
        assert c.rc._entries_ambiguous == {"ref-1": (keys, "")}



# Codex r4318304393 (fcbaad7): due ingressi in coda con lo stesso customer_ref non si scambiano il fence.
def test_two_queued_entries_with_the_same_ref_are_both_denied_and_never_swap_fences(tmp_path):
    c = _catena(tmp_path)
    with c.rc._risk_admission_lock:
        fence_a = c.rc._capture_admission_fence(_signal())
        fence_b = c.rc._capture_admission_fence(_signal(event="Gamma v Delta", market_id="1.777"))
    first, second = {"customer_ref": "ref-x"}, {"customer_ref": "ref-x"}
    c.rc._register_queued_entry_fence(first, fence_a)
    c.rc._register_queued_entry_fence(second, fence_b)
    _barrier(c.rc, {"signal_type": "CASHOUT", "event_name": "Gamma v Delta"})   # cashout sull'evento del secondo

    assert c.rc.admission_fence_verdict(first) == "customer_ref_in_conflitto"
    assert c.rc.admission_fence_verdict(second) == "customer_ref_in_conflitto"   # mai "" senza fence
    assert c.rc._queued_entry_fences == {} and c.rc._queued_ref_conflicts == set()
    assert getattr(c.rc, "_entries_in_flight", {}) == {}      # nessuno dei due risulta in invio


def test_two_queued_entries_with_the_same_ref_place_no_order_through_the_engine(tmp_path):
    c = _catena(tmp_path)
    held = []
    original_publish = c.bus.publish

    def queueing_publish(topic, payload=None):
        if topic == "CMD_QUICK_BET":
            held.append(payload)
            return None
        return original_publish(topic, payload)

    c.bus.publish = queueing_publish
    c.rc._on_signal_received(_signal(customer_ref="ref-dup"))
    c.rc._on_signal_received(_signal(customer_ref="ref-dup", event="Gamma v Delta", market_id="1.777",
                                     received_at="2026-10-08T10:06:00+00:00"))
    c.bus.publish = original_publish
    # #461 PR29-a: lo stesso ref e' lo stesso intento: il secondo ingresso e' rifiutato gia' all'ammissione
    # (riserva viva dell'originale), quindi in coda c'e' un solo ingresso, con il suo fence: nessun conflitto.
    assert len(held) == 1, _rifiuti(c)
    assert "ingresso_gia_in_volo" in _rifiuti(c), _rifiuti(c)
    _run_queued(c, held[0])
    assert len(c.broker.state.orders) == 1                    # un solo ordine, quello ammesso col proprio fence


def test_a_single_queued_entry_is_unaffected_by_the_conflict_rule(tmp_path):
    c = _catena(tmp_path)
    c.rc._on_signal_received(_signal(customer_ref="ref-solo"))
    assert len(c.broker.state.orders) == 1



# Review indipendente su 7a715c6: un ingresso rifiutato dall'engine prima del controllo del fence non lascia un
# fence orfano che metterebbe in conflitto permanente le riconsegne con lo stesso customer_ref.
def test_entry_refused_in_lockdown_leaves_no_orphan_fence_and_a_redelivery_is_not_in_conflict(tmp_path):
    c = _catena(tmp_path)
    payload = _queue_one_entry(c)
    ref = payload["customer_ref"]
    c.engine.safe_mode = type("_Lockdown", (), {"is_lockdown": lambda self: True})()

    _run_queued(c, payload)                                   # rifiutato in lockdown, prima del fence

    assert c.rc._queued_entry_fences == {}
    c.engine.safe_mode = None
    with c.rc._risk_admission_lock:
        fence = c.rc._capture_admission_fence(_signal())
    redelivered = {"customer_ref": ref}
    c.rc._register_queued_entry_fence(redelivered, fence)
    assert c.rc.admission_fence_verdict(redelivered) != "customer_ref_in_conflitto"


def test_entry_failing_normalization_leaves_no_orphan_fence(tmp_path):
    c = _catena(tmp_path)
    payload = _queue_one_entry(c)
    payload["price"] = "non-un-prezzo"                        # normalizzazione fallita nell'engine

    _run_queued(c, payload)

    assert c.broker.state.orders == {}
    assert c.rc._queued_entry_fences == {}


def test_discard_never_removes_the_fence_of_another_request(tmp_path):
    c = _catena(tmp_path)
    with c.rc._risk_admission_lock:
        fence = c.rc._capture_admission_fence(_signal())
    owner = {"customer_ref": "ref-o"}
    c.rc._register_queued_entry_fence(owner, fence)

    c.rc.admission_fence_discard({"customer_ref": "ref-o"})    # altra richiesta, stesso ref: nessun effetto
    assert len(c.rc._queued_entry_fences["ref-o"]) == 1
    c.rc.admission_fence_discard(owner)
    assert c.rc._queued_entry_fences == {}
