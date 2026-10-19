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
    rc, bus = _make_controller(responses=[{"available": 150.0}])
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

    c.bus.publish("CASHOUT_SUCCESS", {"market_id": "1.234", "selection_id": 11, "status": "DONE",
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

    c.bus.publish("CASHOUT_SUCCESS", {"market_id": "1.234", "selection_id": 11, "cashout_request_id": first})
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

    c.bus.publish("CASHOUT_SUCCESS", {"market_id": "1.234", "selection_id": 12, "cashout_request_id": "crq-x"})
    c.bus.publish("CASHOUT_SUCCESS", {"market_id": "1.555", "selection_id": 11})
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
        "CASHOUT_SUCCESS", {"market_id": req["market_id"], "selection_id": req["selection_id"],
                            "cashout_request_id": req["cashout_request_id"]}))

    c.rc._route_cashout_signal(dict(_CASHOUT))

    assert c.rc.get_status()["cashout_barriers_active"] == 0


def test_hedge_outcome_arriving_from_another_thread_releases_the_barrier(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    queued = _async_cashout_chain(c, monkeypatch)
    c.rc._route_cashout_signal(dict(_CASHOUT))

    worker = threading.Thread(target=c.bus.publish,
                              args=("CASHOUT_SUCCESS", {"market_id": "1.234", "selection_id": 11,
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


def _real_cashout_chain(c, monkeypatch, *, result=None, residual_cancel_ok=True):
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

    CashoutResidualHandler(cancel=cancel, persist=lambda record: None,
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

    c.bus.publish("CASHOUT_SUCCESS", {"market_id": "1.234", "selection_id": 11, "status": "DONE"})

    assert c.rc.get_status()["cashout_barriers_active"] == 1
    c.bus.publish("CASHOUT_SUCCESS", {"market_id": "1.234", "selection_id": "11",
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
    c.bus.publish("CASHOUT_SUCCESS", {"market_id": "1.234", "selection_id": 11,
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
