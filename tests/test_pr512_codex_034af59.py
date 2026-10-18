"""PR #512 — rilievi Codex sul head 034af59 (ciclo 8). Un test per rilievo, scritto PRIMA della patch
(RED sul codice precedente), codice reale del progetto, deterministico e offline.

1. `BetfairService._well_formed_current_orders`: riga senza `betId`/`marketId` => CURRENT_ORDERS_MALFORMED
   (la ricostruzione F018 la leggeva come "posizione chiusa").
2. `NativeSimMicrostakeExecutor`: snapshot finale del broker non persistito => saga AMBIGUOUS, mai ACTIVE/SUCCESS.
3. `DutchingController._leg_liquidity_shortfall`: liquidita' ignota in modalita' blocco => shortfall LIQUIDITY_UNKNOWN.
4. `MicrostakeCoordinator.recover`: ROLLBACK_PENDING => cancel_all_remaining -> ROLLED_BACK, mai replace.
5. `reset_cycle`/`_maybe_auto_reset`: persistenza fallita => `_recovery_persist_failed`, evento, `persisted: False`.
6. `start()`: stato di recovery illeggibile => PAUSED (RECOVERY_STATE_UNREADABLE) e nessuna sovrascrittura.
7. docstring di `core/microstake_safe.py` veritiero.
10. `RoserpinaMoneyManagement._desk_mode`: crescita ignota => NORMAL anche con soglia 0.
"""
import json
import types
import os
import sys
import threading
from decimal import Decimal as D

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tests", "f026"))

from services.betfair_service import BetfairService  # noqa: E402


# ------------------------------------------------------------------ 1. currentOrders senza identita'
class _OrdersBroker:
    def __init__(self, orders):
        self._orders = orders

    def get_current_orders(self, market_ids=None):
        return self._orders


@pytest.mark.parametrize("row", [
    {"marketId": "1.1"},                       # senza betId
    {"betId": "B1"},                           # senza marketId
    {"betId": "", "marketId": "1.1"},          # betId vuoto
    {"betId": "B1", "marketId": "  "},         # marketId vuoto
    {"betId": None, "marketId": "1.1"},
])
def test_f1_current_order_row_without_identity_is_malformed(row):
    with pytest.raises(RuntimeError, match="CURRENT_ORDERS_MALFORMED"):
        BetfairService._well_formed_current_orders([{"betId": "B0", "marketId": "1.0"}, row])


def test_f1_rows_with_identity_pass_in_both_spellings():
    rows = [{"betId": "B1", "marketId": "1.1"}, {"bet_id": "B2", "market_id": "1.2"}]
    assert BetfairService._well_formed_current_orders(rows) == rows


def test_f1_sim_facade_raises_on_row_without_bet_id_instead_of_closing_positions():
    svc = BetfairService.__new__(BetfairService)
    svc.simulation_mode = True
    svc.get_client = lambda: _OrdersBroker([{"marketId": "1.1"}])
    with pytest.raises(RuntimeError, match="CURRENT_ORDERS_MALFORMED"):
        svc.list_current_orders()


def test_f1_rebuild_goes_paused_on_row_without_bet_id():
    """Il percorso reale: F018 con una riga senza betId deve dare EXCHANGE_ORDERS_UNAVAILABLE, non 'closed'."""
    from test_f018_rebuild_units import _Exchange, _rc, _row

    class _Facade(_Exchange):
        def list_current_orders(self, market_ids=None):
            self.calls += 1
            return [BetfairService._normalize_startup_order(o)
                    for o in BetfairService._well_formed_current_orders(list(self.current))]

    rc = _rc([_row("r1", "SUBMITTED", "1.1", 1, "K1", bet_id="B1")], _Facade([{"marketId": "1.1"}]))
    res = rc._rebuild_open_positions()
    assert res["ok"] is False and res["reason"].startswith("EXCHANGE_ORDERS_UNAVAILABLE"), res
    assert res["closed"] == []


# ------------------------------------------------------------------ 2. snapshot finale non persistito
def _native_engine():
    sys.path.insert(0, os.path.join(ROOT, "tests"))
    from test_microstake_engine_native import _engine, _payload
    engine, broker, _ = _engine()
    engine._default_native_microstake_executor("SIMULATION")
    return engine, broker, _payload


def test_f2_failed_final_snapshot_leaves_the_saga_ambiguous_with_reservation_held():
    engine, broker, payload = _native_engine()
    ex = engine.microstake_executor
    broker.persist_snapshot = lambda: False
    res = ex.execute(payload("snap", size=0.10, price=3.0))
    assert res["status"] == "AMBIGUOUS" and res["order_unknown"] is True, res
    assert res["phase"] == "AMBIGUOUS" and res["reservation_held"] is True and res["snapshot_persisted"] is False
    assert "instructionReports" not in res
    saga = ex.store.load("snap")
    assert saga["phase"] == "AMBIGUOUS" and saga["reservation_held"] is True, saga
    assert "snap" in ex.store.pending_refs(), "la saga deve restare da riconciliare al riavvio"
    assert ex.port.has_reservation("snap")


def test_f2_failed_final_snapshot_is_reconciled_by_recover_without_a_new_place():
    engine, broker, payload = _native_engine()
    ex = engine.microstake_executor
    broker.persist_snapshot = lambda: False
    ex.execute(payload("snap-r", size=0.10, price=3.0))
    broker.persist_snapshot = lambda: True
    out = ex.recover_pending()
    assert out == {"snap-r": "ACTIVE"}, out
    assert [op[0] for op in ex.port.operation_log] == ["PLACE", "CANCEL"]
    assert ex.store.load("snap-r")["reservation_held"] is False and not ex.port.has_reservation("snap-r")


def test_f2_intermediate_snapshot_failure_recovered_by_the_final_one_stays_active():
    engine, broker, payload = _native_engine()
    calls = []
    broker.persist_snapshot = lambda: (calls.append(1), len(calls) > 1)[1]   # fallisce solo dopo il PLACE
    res = engine.microstake_executor.execute(payload("snap-ok", size=0.10, price=3.0))
    assert res["status"] == "SUCCESS" and res["phase"] == "ACTIVE", res
    assert res["snapshot_persisted"] is False and engine.microstake_executor.port.snapshot_failures == 1


class _Crash2(BaseException):
    pass


def test_f2_coordinator_never_persists_active_while_the_reload_source_is_stale(monkeypatch):
    """Riesame indipendente del ciclo 8: la retrocessione ad AMBIGUOUS avveniva nell'esecutore DOPO che il
    coordinatore aveva gia' persistito ACTIVE e rilasciato la riserva. Un crash in quella finestra lasciava
    una saga conclusa fuori da pending_refs sopra una fonte di reload senza l'ordine. Ora il coordinatore
    persiste AMBIGUOUS (riserva tenuta) quando l'ultimo snapshot non e' stato persistito."""
    from core.microstake_sim_port import NativeSimMicrostakeExecutor
    engine, broker, payload = _native_engine()
    ex = engine.microstake_executor
    broker.persist_snapshot = lambda: False

    def crash(self, payload, out):
        raise _Crash2()
    monkeypatch.setattr(NativeSimMicrostakeExecutor, "_demote_unpersisted_active", crash)
    try:
        ex.execute(payload("win", size=0.10, price=3.0))
    except _Crash2:
        pass
    saga = ex.store.load("win")
    assert saga["phase"] == "AMBIGUOUS" and saga["reservation_held"] is True, saga
    assert "win" in ex.store.pending_refs() and ex.port.has_reservation("win")


def test_f2_rolled_back_with_unpersisted_snapshot_stays_ambiguous_with_reservation(tmp_path):
    """Stessa famiglia: anche ROLLED_BACK e' terminale (escluso da pending_refs). Con la fonte di reload
    stantia la saga resta AMBIGUOUS con riserva tenuta, invece di chiudersi sopra uno stato che al riavvio
    ricaricherebbe lo stub ancora aperto."""
    from core.microstake_safe import Phase
    ex, path, policy, Coordinator, Store = _rollback_world(tmp_path)
    ex.cancel_all_mode = "ok"
    ex.last_snapshot_persisted = False
    res = Coordinator(ex, Store(path), policy).recover("rb-1")
    assert res.phase is Phase.AMBIGUOUS and res.reservation_held is True and res.reason == "snapshot_not_persisted"
    saved = Store(path).load("rb-1")
    assert saved["phase"] == "AMBIGUOUS" and saved["reservation_held"] is True and ex.has_reservation("rb-1")
    ex.last_snapshot_persisted = True
    res2 = Coordinator(ex, Store(path), policy).recover("rb-1")
    assert res2.phase in (Phase.ROLLED_BACK, Phase.AMBIGUOUS)
    assert not any(op[0] == "REPLACE" for op in ex.operation_log)


def test_f2_recover_pending_does_not_promote_while_the_snapshot_still_fails():
    engine, broker, payload = _native_engine()
    ex = engine.microstake_executor
    broker.persist_snapshot = lambda: False
    ex.execute(payload("snap-x", size=0.10, price=3.0))
    ops = list(ex.port.operation_log)
    assert ex.recover_pending() == {"snap-x": "AMBIGUOUS"}
    assert ex.store.load("snap-x")["phase"] == "AMBIGUOUS" and ex.port.operation_log == ops


# ------------------------------------------------------------------ 3. liquidita' ignota in modalita' blocco
def _dutching_leg_shortfall(ladder, *, warning_only):
    from controllers.dutching_controller import DutchingController
    book = {"runners": [{"selectionId": 11, "ex": {"availableToBack": ladder}}]}
    item = {"selectionId": 11, "stake": 10.0, "side": "BACK", "price": 2.0}
    return DutchingController._leg_liquidity_shortfall(book, item, 1.0, 1.0, warning_only=warning_only)


@pytest.mark.parametrize("ladder", [{"price": 2.0, "size": 50}, "abc", 7])
def test_f3_unknown_liquidity_in_block_mode_is_a_shortfall(ladder):
    sf = _dutching_leg_shortfall(ladder, warning_only=False)
    assert sf is not None and sf["reason"] == "LIQUIDITY_UNKNOWN" and sf["available"] is None, sf
    assert sf["selectionId"] == 11 and sf["required"] == 10.0


def test_f3_selection_missing_from_the_book_is_unknown_in_block_mode():
    from controllers.dutching_controller import DutchingController
    book = {"runners": [{"selectionId": 99, "ex": {"availableToBack": [{"price": 2.0, "size": 50}]}}]}
    item = {"selectionId": 11, "stake": 10.0, "side": "BACK", "price": 2.0}
    sf = DutchingController._leg_liquidity_shortfall(book, item, 1.0, 1.0, warning_only=False)
    assert sf is not None and sf["reason"] == "LIQUIDITY_UNKNOWN"


@pytest.mark.parametrize("ladder", [{"price": 2.0, "size": 50}, "abc", 7])
def test_f3_unknown_liquidity_in_warning_only_mode_stays_unobserved(ladder):
    assert _dutching_leg_shortfall(ladder, warning_only=True) is None


def test_f3_known_sufficient_liquidity_is_never_a_shortfall_in_block_mode():
    assert _dutching_leg_shortfall([{"price": 2.0, "size": 50.0}], warning_only=False) is None


def test_f3_evaluate_guard_blocks_on_unknown_liquidity_only_when_armed():
    from types import SimpleNamespace
    from controllers.dutching_controller import DutchingController
    book = {"runners": [{"selectionId": 11, "ex": {"availableToBack": "rotto"}}]}
    ctl = DutchingController.__new__(DutchingController)
    ctl._market_book = lambda market_id: book
    results = [{"selectionId": 11, "stake": 10.0, "side": "BACK", "price": 2.0}]
    armed = ctl._evaluate_liquidity_guard(SimpleNamespace(liquidity_warning_only=False), {"market_id": "1.1"}, results)
    assert armed["warning"] is True and armed["shortfall"][0]["reason"] == "LIQUIDITY_UNKNOWN"
    observe = ctl._evaluate_liquidity_guard(SimpleNamespace(liquidity_warning_only=True), {"market_id": "1.1"}, results)
    assert observe == {"warning": False, "shortfall": []}


# ------------------------------------------------------------------ 4. ROLLBACK_PENDING al riavvio
class _Crash(BaseException):
    """Crash di processo (non Exception: nessun handler della saga lo intercetta)."""


def _rollback_world(tmp_path):
    from core.microstake_safe import (JsonSagaStore, MicrostakeCoordinator, MicrostakePolicy, MicrostakeRequest,
                                      Side, StatefulExchangeSimulator)

    class _Exchange(StatefulExchangeSimulator):
        def __init__(self):
            super().__init__(balance=D("100"))
            self.reject_replace_once = True
            self.cancel_all_mode = "crash"      # crash | fail | ok

        def replace(self, bet_id, new_price):
            if self.reject_replace_once:
                self.reject_replace_once = False
                raise RuntimeError("REPLACE_REJECTED")
            return super().replace(bet_id, new_price)

        def cancel_all_remaining(self, bet_id):
            if self.cancel_all_mode == "crash":
                raise _Crash()
            if self.cancel_all_mode == "fail":
                raise RuntimeError("CANCEL_REJECTED")
            return super().cancel_all_remaining(bet_id)

    policy = MicrostakePolicy(simulation_only=True, minimum_economic_stake=D("0.10"), exchange_stub_size=D("2.00"),
                              max_transient_exposure=D("10.00"), commission_rate=D("0.045"))
    ex = _Exchange()
    path = tmp_path / "saga.json"
    req = MicrostakeRequest(market_id="1.100", selection_id=7, side=Side.BACK, economic_stake=D("0.10"),
                            target_price=D("2.00"), customer_ref="rb-1")
    with pytest.raises(_Crash):
        MicrostakeCoordinator(ex, JsonSagaStore(path), policy).execute(req, stub_price=D("3.00"))
    assert JsonSagaStore(path).load("rb-1")["phase"] == "ROLLBACK_PENDING"
    assert ex.has_reservation("rb-1")
    return ex, path, policy, MicrostakeCoordinator, JsonSagaStore


def test_f4_rollback_pending_is_cancelled_at_restart_never_replaced(tmp_path):
    from core.microstake_safe import Phase
    ex, path, policy, Coordinator, Store = _rollback_world(tmp_path)
    ex.cancel_all_mode = "ok"
    before = len(ex.operation_log)
    res = Coordinator(ex, Store(path), policy).recover("rb-1")
    assert res.phase is Phase.ROLLED_BACK and res.reservation_held is False, res
    new_ops = ex.operation_log[before:]
    assert [op[0] for op in new_ops] == ["CANCEL"] and new_ops[0][2] == "0.10", new_ops   # il residuo, non lo stub
    assert ex.order(res.bet_id).remaining == D("0")
    saved = Store(path).load("rb-1")
    assert saved["phase"] == "ROLLED_BACK" and saved["reservation_held"] is False
    assert not ex.has_reservation("rb-1")


def test_f4_rollback_pending_with_failed_cancel_stays_ambiguous_with_reservation(tmp_path):
    from core.microstake_safe import Phase
    ex, path, policy, Coordinator, Store = _rollback_world(tmp_path)
    ex.cancel_all_mode = "fail"
    res = Coordinator(ex, Store(path), policy).recover("rb-1")
    assert res.phase is Phase.AMBIGUOUS and res.reservation_held is True and res.reason == "rollback_unknown"
    # rilievo Sol (ciclo 8): l'intento di rollback resta persistito, non viene sostituito da AMBIGUOUS
    assert Store(path).load("rb-1")["phase"] == "ROLLBACK_PENDING" and ex.has_reservation("rb-1")
    assert not any(op[0] == "REPLACE" for op in ex.operation_log)


def test_f4_rollback_intent_survives_a_failed_cancel_and_the_next_recover_only_cancels(tmp_path):
    """Sol, range del ciclo 8: dopo una cancellazione fallita il recover successivo deve ritentare SOLO la
    cancellazione del residuo (ROLLED_BACK), mai rientrare nel percorso riduzione/replace che riattiverebbe
    l'ordine che la saga aveva deciso di annullare."""
    from core.microstake_safe import Phase
    ex, path, policy, Coordinator, Store = _rollback_world(tmp_path)
    ex.cancel_all_mode = "fail"
    assert Coordinator(ex, Store(path), policy).recover("rb-1").phase is Phase.AMBIGUOUS
    ex.cancel_all_mode = "ok"
    before = len(ex.operation_log)
    res = Coordinator(ex, Store(path), policy).recover("rb-1")
    assert res.phase is Phase.ROLLED_BACK and res.reservation_held is False, res
    new_ops = [op[0] for op in ex.operation_log[before:]]
    assert new_ops == ["CANCEL"], new_ops
    assert ex.order(res.bet_id).remaining == D("0") and not ex.has_reservation("rb-1")
    assert Store(path).load("rb-1")["phase"] == "ROLLED_BACK"


# ------------------------------------------------------------------ 5. reset con persistenza fallita
def _reset_controller(tmp_path, *, fail_persist):
    from test_f032_rsr12_units import _lose, _rc
    rc = _rc(tmp_path / "w.db", funds={"available": 100.0, "exposure": 0.0})
    rc.simulation_mode = False
    rc._daily_loss_state_lock = threading.Lock()
    rc._daily_loss_monitor_state = {"realized_pnl_day_baseline": 0.0, "realized_pnl": 0.0}
    rc._session_pnl_baseline = None
    rc.get_status = lambda: {}
    rc._start_expansion_cycle("first_start", funds={"available": 100.0, "exposure": 0.0})
    _lose(rc, 1, "A", 3.0)                                   # perdita 3.0 persistita sul tavolo 1
    assert rc._active_recovery_count() == 1
    if fail_persist:
        real_save = rc.db.save_settings

        def failing(payload):
            if rc._RECOVERY_KEY in payload:
                raise OSError("disk full")
            return real_save(payload)
        rc.db.save_settings = failing
    return rc


def _events(rc, name):
    return [p for e, p in rc.bus.events if e == name]


def test_f5_reset_cycle_persist_failure_is_declared_and_fail_closed(tmp_path):
    rc = _reset_controller(tmp_path, fail_persist=True)
    out = rc.reset_cycle()
    assert out["reset"] is True and out["persisted"] is False, out
    assert rc._recovery_persist_failed is True
    assert _events(rc, "RECOVERY_STATE_PERSIST_FAILED") == [{"source": "reset_cycle"}]
    assert rc._active_recovery_count() == rc.config.max_recovery_tables, "capacita' di recovery non esaurita"


def test_f5_reset_cycle_persist_success_is_declared(tmp_path):
    rc = _reset_controller(tmp_path, fail_persist=False)
    out = rc.reset_cycle()
    assert out["persisted"] is True and rc._recovery_persist_failed is False
    assert _events(rc, "RECOVERY_STATE_PERSIST_FAILED") == [] and rc._active_recovery_count() == 0


def test_f5_auto_reset_persist_failure_is_declared_and_fail_closed(tmp_path):
    rc = _reset_controller(tmp_path, fail_persist=True)
    rc._account_snapshot = {"ok": True, "cash_equity": 80.0, "available": 80.0, "committed": 0.0}
    rc._cash_equity_peak = 100.0
    rc.config.auto_reset_drawdown_pct = 15.0
    assert rc._maybe_auto_reset(20.0) is True
    assert rc._recovery_persist_failed is True
    assert _events(rc, "RECOVERY_STATE_PERSIST_FAILED") == [{"source": "auto_reset"}]
    assert len(_events(rc, "ROSERPINA_AUTO_RESET")) == 1
    assert rc._active_recovery_count() == rc.config.max_recovery_tables


# ------------------------------------------------------------------ 6. stato di recovery illeggibile al riavvio
def test_f6_persist_refuses_to_overwrite_an_unreadable_recovery_state(tmp_path):
    from test_f032_rsr12_units import _rc
    rc = _rc(tmp_path / "u.db")
    rc.db.save_settings({rc._RECOVERY_KEY: "{rotto"})
    assert rc._restore_recovery_tables() is False and rc._recovery_state_unreadable is True
    assert rc._persist_recovery_tables() is False, "stato illeggibile sovrascritto"
    assert rc._persist_recovery_tables(cycle_reset=True, reset_ts=1.0) is False
    assert rc.db.get_settings().get(rc._RECOVERY_KEY) == "{rotto"
    assert rc._active_recovery_count() == rc.config.max_recovery_tables


def test_f6_resume_does_not_bypass_an_unreadable_recovery_state():
    from types import SimpleNamespace
    from core.runtime_controller import RuntimeController, RuntimeMode
    rc = RuntimeController.__new__(RuntimeController)
    rc.mode = RuntimeMode.PAUSED
    rc.bus = SimpleNamespace(publish=lambda *a, **k: None)
    rc._reconciliation_required = True
    rc._recovery_state_unreadable = True
    rc._monitor_daily_loss_breach = lambda **k: {"breached": False}
    rc._loss_block_reason = lambda: ""
    rc._recover_native_microstake = lambda: {"ok": True, "status": "RECONCILED", "pending": []}
    rc._rebuild_open_positions = lambda: {"ok": True, "reason": "", "rebuilt": [], "uncertain": [], "held": []}
    rc.get_status = lambda: {"mode": rc.mode.value}
    rc._safe_status_snapshot = lambda: {"mode": rc.mode.value}
    out = rc.resume()
    assert out["resumed"] is False and "RECOVERY_STATE_UNREADABLE" in out["reason"], out
    assert rc.mode is RuntimeMode.PAUSED and rc._reconciliation_required is True


def test_f6_start_with_unreadable_recovery_state_goes_paused_and_keeps_the_stored_state():
    """Percorso reale (harness F026 in sottoprocesso): run 1 lascia un tavolo in recovery; lo stato persistito
    viene corrotto; run 2 deve partire PAUSED (RECOVERY_STATE_UNREADABLE), non ammettere il segnale e non
    sovrascrivere lo stato illeggibile."""
    import glob
    import tempfile
    from database import Database
    from test_f026_financial import run
    home = tempfile.mkdtemp(prefix="f6_")
    first = run([{"op": "signal", "m": "1.601", "price": 2.0}, {"op": "settle", "m": "1.601", "win": False}], home=home)
    assert any(float(t["loss"] or 0) > 0 for t in first["snaps"][-1]["tables"]), "precondizione: tavolo in recovery"
    dbs = glob.glob(os.path.join(home, "**", "*.db"), recursive=True)
    assert len(dbs) == 1, dbs
    Database(dbs[0]).save_settings({"roserpina.recovery_tables_state": "{rotto"})
    second = run([{"op": "signal", "m": "1.602", "price": 2.0}], home=home)
    assert second["mode_after_start"] == "PAUSED", second["mode_after_start"]
    assert second["rebuild"]["ok"] is False and second["rebuild"]["reason"] == "RECOVERY_STATE_UNREADABLE", second["rebuild"]
    assert not any(o["market_id"] == "1.602" and o["status"] == "SUBMITTED" for o in second["snaps"][-1]["orders"])
    assert Database(dbs[0]).get_settings().get("roserpina.recovery_tables_state") == "{rotto"


# ------------------------------------------------------------------ 7. docstring veritiero di microstake_safe
def test_f7_microstake_safe_docstring_declares_its_real_role():
    import core.microstake_safe as ms
    doc = ms.__doc__ or ""
    assert "no import or call site" not in doc, "la docstring nega un uso che esiste (core/microstake_sim_port.py)"
    assert "microstake_sim_port" in doc and "TradingEngine" in doc


# ------------------------------------------------------------------ 10. crescita ignota => mai espansione
@pytest.mark.parametrize("threshold", [0.0, -1.0, 5.0])
def test_f10_unknown_growth_never_expands_whatever_the_threshold(threshold):
    from core.system_state import DeskMode
    from test_f032_rsr12_units import _calc, _mm
    d = _calc(_mm(expansion_profit_pct=threshold), expansion_growth_snapshot=None)
    assert d.desk_mode is DeskMode.NORMAL, (threshold, d.desk_mode)
    assert d.recommended_stake == pytest.approx(2.0)          # nessun moltiplicatore di espansione


@pytest.mark.parametrize("growth", [float("nan"), float("inf"), "abc"])
def test_f10_non_finite_growth_is_unknown_and_never_expands(growth):
    from core.system_state import DeskMode
    from test_f032_rsr12_units import _calc, _mm
    assert _calc(_mm(expansion_profit_pct=0.0), expansion_growth_snapshot=growth).desk_mode is DeskMode.NORMAL


def test_f10_known_growth_at_zero_threshold_still_expands():
    from core.system_state import DeskMode
    from test_f032_rsr12_units import _calc, _mm
    assert _calc(_mm(expansion_profit_pct=0.0), expansion_growth_snapshot=0.0).desk_mode is DeskMode.EXPANSION


# ================================================================== Codex su 2c6aa09 / ciclo 9
# ------------------------------------------------------------------ c9 #1 (r4307328207, P1): secondo segnale per lo
# stesso evento dopo la scadenza TTL della chiave anti-duplicato, con la prima posizione ancora aperta
def _c9_chain(tmp_path):
    from tests.acceptance.test_issue437_pr27 import _catena
    return _catena(tmp_path)


def _c9_rejects(c):
    from tests.acceptance.test_issue437_pr27 import _rifiuti
    return _rifiuti(c)


def _c9_signal(**over):
    from tests.integration.test_issue461_pr26a_customer_ref_provenance import _signal
    return _signal(**over)


def _c9_occupied(c):
    return [(t["table_id"], t["current_event_key"]) for t in c.rc.table_manager.snapshot() if t.get("current_event_key")]


def _c9_expire_ttl(monkeypatch, guard):
    from core import duplication_guard as dg_mod
    base = dg_mod.time.monotonic()
    monkeypatch.setattr(dg_mod.time, "monotonic", lambda: base + guard.ttl + 1)


@pytest.mark.parametrize("retain_by_reset", [False, True])
def test_c9_1_second_signal_for_an_open_event_after_ttl_expiry_places_no_second_order(tmp_path, monkeypatch, retain_by_reset):
    c = _c9_chain(tmp_path)
    c.rc._on_signal_received(_c9_signal())
    assert len(c.broker.state.orders) == 1, _c9_rejects(c)
    key = c.rc.duplication_guard.build_event_key(_c9_signal())
    if retain_by_reset:
        c.rc._uncertain_reservations_map()[key] = 1          # esito incerto (AMBIGUOUS): il reset lo trattiene
        c.rc.reset_cycle()
        assert c.rc.duplication_guard.is_duplicate(key) and c.rc.table_manager.find_by_event_key(key) is not None
    _c9_expire_ttl(monkeypatch, c.rc.duplication_guard)
    assert c.rc.table_manager.find_by_event_key(key) is not None      # la posizione e' ancora sul tavolo
    c.rc._on_signal_received(_c9_signal(price=2.1, received_at="2026-10-08T10:05:00.000000+00:00"))
    # strato 2 (chiave trattenuta dal reset: appuntata, mai scaduta) => `duplicato_bloccato`;
    # strato 1 (chiave ordinaria scaduta, tavolo ancora occupato) => `evento_gia_in_posizione`
    expected = "duplicato_bloccato" if retain_by_reset else "evento_gia_in_posizione"
    assert _c9_rejects(c) == [expected], _c9_rejects(c)
    assert len(c.broker.state.orders) == 1, "secondo ordine piazzato per lo stesso evento con la prima posizione aperta"
    assert len(_c9_occupied(c)) == 1 and _c9_occupied(c)[0][1] == key
    assert c.rc.table_manager.find_by_event_key(key) is not None, "tavolo occupato liberato dal rifiuto"
    assert c.rc.duplication_guard.is_duplicate(key), "chiave rilasciata/scaduta con la posizione ancora aperta"


def test_c9_1_a_different_event_still_goes_through_after_ttl_expiry(tmp_path, monkeypatch):
    c = _c9_chain(tmp_path)
    c.rc._on_signal_received(_c9_signal())
    assert len(c.broker.state.orders) == 1, _c9_rejects(c)
    _c9_expire_ttl(monkeypatch, c.rc.duplication_guard)
    c.rc._on_signal_received(_c9_signal(market_id="1.999", selection_id=12))
    assert len(c.broker.state.orders) == 2, _c9_rejects(c)
    assert "evento_gia_in_posizione" not in _c9_rejects(c)
    assert len(_c9_occupied(c)) == 2


def test_c9_1_retained_keys_never_expire_by_ttl_but_startup_seeds_still_do(monkeypatch):
    """Strato 2: una chiave trattenuta dal reset (esito incerto) non scade in silenzio; `release` la libera.
    Il seed di avvio senza tavolo (`register_startup_order`) conserva il contratto documentato e scade con il
    TTL (tests/unit/test_duplication_guard_ttl_and_seed.py): un seed eterno bloccherebbe per sempre la
    selezione di un ordine poi annullato; la posizione con tavolo e' protetta dallo strato 1."""
    from core import duplication_guard as dg_mod
    g = dg_mod.DuplicationGuard(ttl_seconds=120)
    assert g.acquire("K-live") and g.acquire("K-free") and g.acquire("K-seed-ref")
    g.clear(keep_keys={"K-live"})
    assert g.register_startup_order({"event_key": "K-seed"}) is True
    assert g.acquire("K-plain")
    base = dg_mod.time.monotonic()
    monkeypatch.setattr(dg_mod.time, "monotonic", lambda: base + g.ttl + 1)
    assert g.is_duplicate("K-live"), "chiave trattenuta dal reset scaduta per TTL"
    assert not g.is_duplicate("K-seed"), "il seed di avvio senza tavolo deve scadere come ogni chiave"
    assert not g.is_duplicate("K-plain"), "chiave ordinaria non piu' soggetta al TTL (over-blocking)"
    assert not g.is_duplicate("K-free")
    g.release("K-live")
    assert not g.is_duplicate("K-live") and g.acquire("K-live")        # dopo il rilascio e' di nuovo ordinaria
    monkeypatch.setattr(dg_mod.time, "monotonic", lambda: base + 2 * g.ttl + 2)
    assert not g.is_duplicate("K-live")
    g.acquire("K-again")
    g.clear(keep_keys={"K-again"})
    g.clear()
    assert not g.is_duplicate("K-again"), "clear() senza chiavi trattenute libera anche le chiavi appuntate"


def test_c9_1_rebuilt_open_position_key_does_not_expire_by_ttl(monkeypatch):
    from core import duplication_guard as dg_mod
    from tests.f026.test_f018_rebuild_units import _Exchange, _rc, _row
    rc = _rc([_row("r1", "SUBMITTED", "1.1", 1, "K1", side="BACK", bet_id="77")],
             _Exchange([{"betId": "77", "marketId": "1.1"}]))
    assert rc._rebuild_open_positions()["rebuilt"] == ["r1"]
    base = dg_mod.time.monotonic()
    monkeypatch.setattr(dg_mod.time, "monotonic", lambda: base + rc.duplication_guard.ttl + 1)
    assert rc.duplication_guard.is_duplicate("K1"), "chiave della posizione ricostruita scaduta per TTL"



def test_c9_review_pinned_rebuilt_key_expires_normally_after_f028_release(monkeypatch):
    """Riesame indipendente c9 (P2): posizione ricostruita (chiave fissata) e poi liberata da F028 =>
    la chiave torna a scadere per TTL; prima del TTL resta duplicato (BLOCK), dopo il TTL non piu'."""
    from core import duplication_guard as dg_mod
    from tests.f026.test_f018_rebuild_units import _Exchange, _rc, _row
    rc = _rc([_row("r1", "SUBMITTED", "1.1", 1, "K1", side="BACK", bet_id="77")],
             _Exchange([{"betId": "77", "marketId": "1.1"}]))
    assert rc._rebuild_open_positions()["rebuilt"] == ["r1"]
    rc._final_unmatched_bets = lambda markets, bets: set(bets)       # stato finale confermato: mai abbinato
    rc._settlement_reconciliation_allowed = lambda: True
    rc.bus = types.SimpleNamespace(publish=lambda *a, **k: None)
    assert rc._release_final_unmatched_orders() == ["r1"]
    assert rc.table_manager.find_by_event_key("K1") is None
    assert rc.duplication_guard.is_duplicate("K1"), "la chiave deve restare per il TTL dopo il rilascio"
    base = dg_mod.time.monotonic()
    monkeypatch.setattr(dg_mod.time, "monotonic", lambda: base + rc.duplication_guard.ttl + 1)
    assert not rc.duplication_guard.is_duplicate("K1"), "chiave fissata sopravvive alla posizione liberata"


def test_c9_review_unpin_restarts_ttl_from_now_and_ignores_unpinned_keys(monkeypatch):
    from core import duplication_guard as dg_mod
    from core.duplication_guard import DuplicationGuard
    g = DuplicationGuard(ttl_seconds=10)
    t = [1000.0]
    monkeypatch.setattr(dg_mod.time, "monotonic", lambda: t[0])
    assert g.acquire("P", pin=True) and g.acquire("N")
    t[0] = 1100.0                                  # ben oltre il TTL: N scade, P no
    assert not g.is_duplicate("N") and g.is_duplicate("P")
    g.unpin("P"); g.unpin("assente"); g.unpin("")
    t[0] = 1105.0
    assert g.is_duplicate("P"), "il TTL riparte dallo sblocco, non dall'acquisizione"
    t[0] = 1111.0
    assert not g.is_duplicate("P")


# ------------------------------------------------------------------ c9 #2 (r4307328199, P1): baseline di espansione e
# stato di recovery + marker del reset scritti in DUE transazioni: se la seconda fallisce, dopo un crash la nuova
# baseline e' accettata (nessun marker) mentre tornano le perdite del ciclo precedente
def _c9_reset_controller(tmp_path, *, fail_on):
    """`fail_on`: insieme di chiavi la cui presenza nel payload fa fallire `save_settings` (None = mai)."""
    rc = _reset_controller(tmp_path, fail_persist=False)
    if fail_on:
        real_save = rc.db.save_settings

        def failing(payload):
            if set(payload) & set(fail_on):
                raise OSError("disk full")
            return real_save(payload)
        rc.db.save_settings = failing
    rc.betfair_service.funds = {"available": 130.0, "exposure": 0.0}      # il nuovo ciclo partirebbe da 130
    return rc


def _c9_restart(tmp_path):
    from test_f032_rsr12_units import _rc
    again = _rc(tmp_path / "w.db")
    again._load_expansion_baseline()
    again._restore_recovery_tables()
    return again


@pytest.mark.parametrize("fail_on", [("roserpina.recovery_tables_state",), ("roserpina.cycle_reset_state",),
                                     ("roserpina.expansion_baseline_state",)])
def test_c9_2_reset_cycle_baseline_and_recovery_state_are_one_transaction(tmp_path, fail_on):
    rc = _c9_reset_controller(tmp_path, fail_on=fail_on)
    out = rc.reset_cycle()
    assert out["persisted"] is False and rc._recovery_persist_failed is True
    assert rc._expansion_baseline is None, "baseline del nuovo ciclo tenuta in memoria con reset non durevole"
    assert _events(rc, "RECOVERY_STATE_PERSIST_FAILED") == [{"source": "reset_cycle"}]
    again = _c9_restart(tmp_path)
    assert again.table_manager.recovery_state() == {1: pytest.approx(3.0)}, "perdite del ciclo precedente perse"
    assert again._expansion_baseline == pytest.approx(100.0), (
        "baseline del NUOVO ciclo accettata al riavvio mentre tornano le perdite del ciclo VECCHIO")


def test_c9_2_reset_cycle_success_persists_baseline_marker_and_recovery_atomically(tmp_path):
    rc = _c9_reset_controller(tmp_path, fail_on=None)
    writes = []
    real_save = rc.db.save_settings
    rc.db.save_settings = lambda payload: (writes.append(sorted(payload)), real_save(payload))[1]
    out = rc.reset_cycle()
    assert out["persisted"] is True and rc._expansion_baseline == pytest.approx(130.0)
    assert writes == [sorted([rc._EXPANSION_KEY, rc._RECOVERY_KEY, rc._CYCLE_RESET_KEY])], writes
    again = _c9_restart(tmp_path)
    assert again._expansion_baseline == pytest.approx(130.0) and again.table_manager.recovery_state() == {}
    settings = again.db.get_settings()
    assert json.loads(settings[rc._EXPANSION_KEY])["set_at_ts"] == json.loads(settings[rc._CYCLE_RESET_KEY])["at_ts"]


@pytest.mark.parametrize("fail_on", [("roserpina.recovery_tables_state",), ("roserpina.expansion_baseline_state",)])
def test_c9_2_auto_reset_baseline_and_recovery_state_are_one_transaction(tmp_path, fail_on):
    rc = _c9_reset_controller(tmp_path, fail_on=fail_on)
    rc._account_snapshot = {"ok": True, "cash_equity": 80.0, "available": 80.0, "committed": 0.0}
    rc._cash_equity_peak = 100.0
    rc.config.auto_reset_drawdown_pct = 15.0
    assert rc._maybe_auto_reset(20.0) is True
    assert rc._recovery_persist_failed is True and rc._expansion_baseline is None
    again = _c9_restart(tmp_path)
    assert again.table_manager.recovery_state() == {1: pytest.approx(3.0)}
    assert again._expansion_baseline == pytest.approx(100.0), "baseline del nuovo ciclo accettata con reset non durevole"


def test_c9_2_auto_reset_success_is_one_write(tmp_path):
    rc = _c9_reset_controller(tmp_path, fail_on=None)
    rc._account_snapshot = {"ok": True, "cash_equity": 80.0, "available": 80.0, "committed": 0.0}
    rc._cash_equity_peak = 100.0
    rc.config.auto_reset_drawdown_pct = 15.0
    writes = []
    real_save = rc.db.save_settings
    rc.db.save_settings = lambda payload: (writes.append(sorted(payload)), real_save(payload))[1]
    assert rc._maybe_auto_reset(20.0) is True
    assert writes == [sorted([rc._EXPANSION_KEY, rc._RECOVERY_KEY, rc._CYCLE_RESET_KEY])], writes
    again = _c9_restart(tmp_path)
    assert again._expansion_baseline == pytest.approx(80.0) and again.table_manager.recovery_state() == {}


@pytest.mark.parametrize("trigger", ["reset_cycle", "auto_reset"])
def test_c9_review_unpersisted_reset_travels_with_the_next_recovery_write(tmp_path, trigger):
    """Riesame indipendente c9 (P2): reset non persistito, poi il settlement successivo riscrive lo stato
    tavoli => baseline e marker del reset vanno nella STESSA scrittura, mai le perdite perdonate accanto alla
    baseline vecchia."""
    rc = _c9_reset_controller(tmp_path, fail_on=None)
    real_save = rc.db.save_settings
    calls = {"n": 0}

    def fail_first(payload):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("disk full")
        return real_save(payload)
    rc.db.save_settings = fail_first
    if trigger == "reset_cycle":
        assert rc.reset_cycle()["persisted"] is False
        expected = 130.0
    else:
        rc._account_snapshot = {"ok": True, "cash_equity": 80.0, "available": 80.0, "committed": 0.0}
        rc._cash_equity_peak = 100.0
        rc.config.auto_reset_drawdown_pct = 15.0
        assert rc._maybe_auto_reset(20.0) is True
        expected = 80.0
    assert rc._expansion_baseline is None
    writes = []
    rc.db.save_settings = lambda payload: (writes.append(sorted(payload)), real_save(payload))[1]
    assert rc._persist_recovery_tables() is True                    # p.es. il settlement successivo
    assert writes == [sorted([rc._EXPANSION_KEY, rc._RECOVERY_KEY, rc._CYCLE_RESET_KEY])], writes
    assert rc._expansion_baseline == pytest.approx(expected) and rc._recovery_persist_failed is False
    again = _c9_restart(tmp_path)
    assert again.table_manager.recovery_state() == {}
    assert again._expansion_baseline == pytest.approx(expected), "perdite perdonate accanto alla baseline vecchia"
    writes.clear()
    assert rc._persist_recovery_tables() is True                    # BLOCK: il reset non si ripete
    assert writes == [[rc._RECOVERY_KEY]], writes


def test_c9_review2_unpersisted_reset_is_abandoned_when_state_is_reloaded_in_process(tmp_path):
    """Secondo riesame c9 (P2): reset non persistito, poi STOP/START nello stesso processo (lo stato si
    ricarica dal disco: perdite e baseline del ciclo vecchio) => il reset in sospeso e' abbandonato e
    dichiarato; la scrittura successiva NON mette la baseline nuova accanto alle perdite vecchie."""
    rc = _c9_reset_controller(tmp_path, fail_on=None)
    real_save = rc.db.save_settings
    calls = {"n": 0}

    def fail_first(payload):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("disk full")
        return real_save(payload)
    rc.db.save_settings = fail_first
    assert rc.reset_cycle()["persisted"] is False and rc._pending_cycle_reset is not None
    rc._load_expansion_baseline()                    # come start() nello stesso processo
    rc._restore_recovery_tables()
    assert rc._pending_cycle_reset is None
    assert _events(rc, "CYCLE_RESET_ABANDONED") == [{"reason": "state_reloaded_before_reset_was_persisted"}]
    assert rc.table_manager.recovery_state() == {1: pytest.approx(3.0)}
    writes = []
    rc.db.save_settings = lambda payload: (writes.append(sorted(payload)), real_save(payload))[1]
    assert rc._persist_recovery_tables() is True
    assert writes == [[rc._RECOVERY_KEY]], writes
    again = _c9_restart(tmp_path)
    assert again.table_manager.recovery_state() == {1: pytest.approx(3.0)}
    assert again._expansion_baseline == pytest.approx(100.0), "baseline nuova accanto alle perdite vecchie"


def test_c9_2_first_start_stays_a_single_baseline_write_without_marker(tmp_path):
    from test_f032_rsr12_units import _rc
    rc = _rc(tmp_path / "s.db")
    assert rc._start_expansion_cycle("first_start", funds={"available": 100.0, "exposure": 0.0}) is True
    settings = rc.db.get_settings()
    assert rc._EXPANSION_KEY in settings and rc._CYCLE_RESET_KEY not in settings and rc._RECOVERY_KEY not in settings


# ------------------------------------------------------------------ c9_3 (Codex r4307328204): ROLLED_BACK riconciliato
def _engine_with_saga(ref, phase):
    sys.path.insert(0, os.path.join(ROOT, "tests"))
    from test_microstake_engine_native import _engine
    from core.microstake_sim_port import DbSagaStore
    engine, broker, db = _engine()
    store = DbSagaStore(db)
    store.save(ref, phase=phase, request={"market_id": "1.100", "selection_id": 7, "side": "BACK"})
    assert store.pending_refs() == [ref]
    return engine, store


def test_c9_3_completed_rollback_is_reconciled_not_pending_at_startup():
    engine, store = _engine_with_saga("rb", "ROLLBACK_PENDING")

    class _Exec:
        def recover_pending(self):
            store.save("rb", phase="ROLLED_BACK", reservation_held=False)   # come il coordinatore reale
            return {"rb": "ROLLED_BACK"}
    engine.native_microstake_executor = lambda: _Exec()
    out = engine.recover_native_microstake_sagas()
    assert out["ok"] is True and out["pending"] == [] and out["status"] == "RECOVERED", out


@pytest.mark.parametrize("phase", ["AMBIGUOUS", "FAILED", "BLOCKED", "ERROR:boom", "ROLLBACK_PENDING"])
def test_c9_3_non_reconciled_results_stay_pending(phase):
    engine, store = _engine_with_saga("x", "ROLLBACK_PENDING")

    class _Exec:
        def recover_pending(self):
            return {"x": phase}
    engine.native_microstake_executor = lambda: _Exec()
    out = engine.recover_native_microstake_sagas()
    assert out["ok"] is False and out["pending"] == ["x"], out


def test_c9_3_a_terminal_result_that_the_store_still_lists_as_pending_stays_pending():
    """Doppia condizione: un esito terminale riportato dall'esecutore non basta se lo store (fonte
    durevole) elenca ancora il ref come pendente."""
    engine, store = _engine_with_saga("y", "ROLLBACK_PENDING")

    class _Exec:
        def recover_pending(self):
            return {"y": "ROLLED_BACK"}                                      # fase NON persistita
    engine.native_microstake_executor = lambda: _Exec()
    out = engine.recover_native_microstake_sagas()
    assert out["ok"] is False and out["pending"] == ["y"], out


def test_c9_review_saga_pending_in_store_but_missing_from_results_stays_pending():
    """Riesame indipendente c9 (minore): una saga che lo store elenca come pendente ma che l'esecutore non
    ha ripreso (assente dai risultati) resta pendente; un esito vuoto non vale come riconciliazione."""
    engine, store = _engine_with_saga("w", "ROLLBACK_PENDING")

    class _Exec:
        def recover_pending(self):
            return {}
    engine.native_microstake_executor = lambda: _Exec()
    out = engine.recover_native_microstake_sagas()
    assert out["ok"] is False and out["pending"] == ["w"], out


def test_c9_review2_rollback_on_the_real_sim_port_terminates_after_a_stale_snapshot():
    """Secondo riesame c9 (P2): sul broker SIM reale un ordine interamente cancellato sparisce dagli ordini
    correnti; il rollback deciso si chiude comunque ROLLED_BACK leggendo la riga terminale per bet id."""
    from core.microstake_safe import MicrostakeCoordinator, MicrostakePolicy, MicrostakeRequest, Phase, Side
    from core.microstake_sim_port import DbSagaStore, SimulationExchangePort
    from simulation_broker import SimulationBroker
    sys.path.insert(0, os.path.join(ROOT, "tests"))
    from test_microstake_sim_port import _Db
    broker = SimulationBroker(starting_balance=100.0, db=_Db())
    port = SimulationExchangePort(broker)
    store = DbSagaStore(_Db())
    policy = MicrostakePolicy(simulation_only=True, minimum_economic_stake=D("0.10"), exchange_stub_size=D("2.00"),
                              max_transient_exposure=D("10.00"), commission_rate=D("0.045"))
    coord = MicrostakeCoordinator(port, store, policy)
    req = MicrostakeRequest(market_id="1.1", selection_id=7, side=Side.BACK, economic_stake=D("0.10"),
                            target_price=D("1.00"), customer_ref="rb-real")      # 1.00 => replace rifiutato
    real_cancel_all = port.cancel_all_remaining

    def cancel_then_stale(bet_id):
        out = real_cancel_all(bet_id)
        port.last_snapshot_persisted = False
        return out
    port.cancel_all_remaining = cancel_then_stale
    r0 = coord.execute(req, stub_price=D("3.00"))
    assert r0.phase is Phase.AMBIGUOUS and r0.reason == "snapshot_not_persisted", (r0.phase, r0.reason)
    assert port.find_by_ref("rb-real") is None                  # la riga cancellata non e' piu' corrente
    port.cancel_all_remaining = real_cancel_all
    assert port._snapshot() is True
    before = len(port.operation_log)
    res = coord.recover("rb-real")
    assert res.phase is Phase.ROLLED_BACK and res.reservation_held is False, (res.phase, res.reason)
    assert [op[0] for op in port.operation_log[before:]] == [], "nessuna seconda cancellazione, nessun replace"
    assert store.load("rb-real")["phase"] == "ROLLED_BACK"


def test_c9_review2_a_live_order_found_only_by_bet_id_is_not_taken_as_rolled_back(tmp_path):
    """BLOCK: la lettura per bet id accetta solo una riga terminale senza residuo; un ordine ancora vivo
    che find_by_ref non vede lascia la saga pendente, mai chiusa."""
    from core.microstake_safe import Phase
    ex, path, policy, Coordinator, Store = _rollback_world(tmp_path)
    ex.find_by_ref = lambda ref: None
    ex.cancel_all_mode = "ok"
    before = len(ex.operation_log)
    res = Coordinator(ex, Store(path), policy).recover("rb-1")
    assert res.phase is Phase.AMBIGUOUS and res.reason == "reconciliation_incomplete"
    assert Store(path).load("rb-1")["phase"] == "ROLLBACK_PENDING" and ex.has_reservation("rb-1")
    assert ex.operation_log[before:] == []


def test_c9_review2_store_unreadable_after_recovery_reports_every_pending_ref():
    from core.microstake_sim_port import DbSagaStore, SagaStoreError
    engine, store = _engine_with_saga("v", "ROLLBACK_PENDING")
    calls = {"n": 0}
    real = DbSagaStore.pending_refs

    def flaky(self):
        calls["n"] += 1
        if calls["n"] > 1:
            raise SagaStoreError("illeggibile")
        return real(self)

    class _Exec:
        def recover_pending(self):
            return {}
    engine.native_microstake_executor = lambda: _Exec()
    DbSagaStore.pending_refs = flaky
    try:
        out = engine.recover_native_microstake_sagas()
    finally:
        DbSagaStore.pending_refs = real
    assert out["ok"] is False and out["status"] == "STORE_UNREADABLE" and out["pending"] == ["v"], out


def test_c9_3_store_unreadable_after_recovery_is_fail_closed():
    from core.microstake_sim_port import DbSagaStore, SagaStoreError
    engine, store = _engine_with_saga("z", "ROLLBACK_PENDING")
    calls = {"n": 0}
    real = DbSagaStore.pending_refs

    def flaky(self):
        calls["n"] += 1
        if calls["n"] > 1:
            raise SagaStoreError("disco")
        return real(self)

    class _Exec:
        def recover_pending(self):
            store.save("z", phase="ROLLED_BACK", reservation_held=False)
            return {"z": "ROLLED_BACK"}
    engine.native_microstake_executor = lambda: _Exec()
    import core.microstake_sim_port as port_mod
    old = port_mod.DbSagaStore.pending_refs
    port_mod.DbSagaStore.pending_refs = flaky
    try:
        out = engine.recover_native_microstake_sagas()
    finally:
        port_mod.DbSagaStore.pending_refs = old
    assert out["ok"] is False and out["status"] == "STORE_UNREADABLE", out


# --- Ciclo 9 #512, riesame indipendente: intento di rollback durevole ---------------------------

def test_c9_review_rollback_intent_survives_a_recover_that_cannot_find_the_order(tmp_path):
    """P1: un recover che non trova l'ordine non declassa ROLLBACK_PENDING ad AMBIGUOUS; il recover
    successivo cancella soltanto il residuo, mai riduzione/replace al prezzo target."""
    from core.microstake_safe import Phase
    ex, path, policy, Coordinator, Store = _rollback_world(tmp_path)
    real = ex.find_by_ref
    ex.find_by_ref = lambda ref: None
    r1 = Coordinator(ex, Store(path), policy).recover("rb-1")
    assert r1.phase is Phase.AMBIGUOUS and r1.reservation_held is True
    assert Store(path).load("rb-1")["phase"] == "ROLLBACK_PENDING"
    assert Store(path).load("rb-1")["rollback_intent"] is True
    ex.find_by_ref = real
    ex.cancel_all_mode = "ok"
    before = len(ex.operation_log)
    r2 = Coordinator(ex, Store(path), policy).recover("rb-1")
    ops = [op[0] for op in ex.operation_log[before:]]
    assert "REPLACE" not in ops, ops
    assert ops == ["CANCEL"], ops
    assert r2.phase is Phase.ROLLED_BACK and r2.reservation_held is False


def test_c9_review_rollback_with_stale_snapshot_terminates_once_the_source_is_aligned(tmp_path):
    """P2: cancellazione riuscita ma snapshot non persistito => AMBIGUOUS; al recover successivo, con
    fonte allineata, la saga si chiude ROLLED_BACK senza una seconda cancellazione e senza replace."""
    from core.microstake_safe import Phase
    ex, path, policy, Coordinator, Store = _rollback_world(tmp_path)
    ex.cancel_all_mode = "ok"
    ex.last_snapshot_persisted = False
    r1 = Coordinator(ex, Store(path), policy).recover("rb-1")
    assert r1.phase is Phase.AMBIGUOUS and r1.reason == "snapshot_not_persisted"
    assert ex.has_reservation("rb-1")
    ex.last_snapshot_persisted = True
    before = len(ex.operation_log)
    r2 = Coordinator(ex, Store(path), policy).recover("rb-1")
    ops = [op[0] for op in ex.operation_log[before:]]
    assert r2.phase is Phase.ROLLED_BACK and r2.reservation_held is False, (r2.phase, r2.reason)
    assert ops == [], ops
    assert not ex.has_reservation("rb-1")
    assert Store(path).load("rb-1")["phase"] == "ROLLED_BACK"


def test_c9_review_rollback_intent_blocks_replace_even_when_a_matched_check_intervenes(tmp_path):
    """BLOCK: il flag e' scritto con la prima decisione di rollback e sopravvive a ogni salvataggio
    intermedio (merge dello store)."""
    ex, path, policy, Coordinator, Store = _rollback_world(tmp_path)
    Store(path).save("rb-1", phase="AMBIGUOUS", reason="altro")
    ex.cancel_all_mode = "ok"
    before = len(ex.operation_log)
    Coordinator(ex, Store(path), policy).recover("rb-1")
    assert "REPLACE" not in [op[0] for op in ex.operation_log[before:]]
