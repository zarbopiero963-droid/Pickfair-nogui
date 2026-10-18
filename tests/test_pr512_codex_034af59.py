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
