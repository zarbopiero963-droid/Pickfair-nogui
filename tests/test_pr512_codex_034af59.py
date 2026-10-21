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


def test_c9_1_occupied_table_guard_follows_the_anti_duplication_setting(tmp_path):
    """Il rifiuto `evento_gia_in_posizione` estende il dedupe oltre il TTL: con l'anti-duplicazione
    disattivata dall'utente non cambia in silenzio il comportamento precedente (nessun rifiuto runtime;
    la riconsegna dello stesso messaggio la ferma l'engine col customer_ref stabile)."""
    c = _c9_chain(tmp_path)
    c.rc.config.anti_duplication_enabled = False
    c.rc._on_signal_received(_c9_signal())
    assert len(c.broker.state.orders) == 1, _c9_rejects(c)
    c.rc._on_signal_received(_c9_signal())                         # stesso messaggio riconsegnato
    assert "evento_gia_in_posizione" not in _c9_rejects(c), _c9_rejects(c)
    assert len(c.broker.state.orders) == 1, "la riconsegna dello stesso messaggio ha piazzato un secondo ordine"


@pytest.mark.parametrize("state", ["uncertain", "recovered"])
def test_c9_1_disabled_anti_duplication_never_disables_the_mandatory_position_guards(tmp_path, state):
    """Mandato owner (chiusura #512): disattivare l'anti-duplicazione funzionale non disattiva mai le
    protezioni obbligatorie. Su un evento con posizione AMBIGUOUS (esito incerto) o ricostruita al riavvio
    (F018, chiave fissata), un nuovo segnale e' rifiutato anche con anti_duplication_enabled=False."""
    c = _c9_chain(tmp_path)
    c.rc.config.anti_duplication_enabled = False
    c.rc._on_signal_received(_c9_signal())
    assert len(c.broker.state.orders) == 1, _c9_rejects(c)
    key = c.rc.duplication_guard.build_event_key(_c9_signal())
    table = c.rc.table_manager.find_by_event_key(key)
    assert table is not None
    if state == "uncertain":
        c.rc._uncertain_reservations_map()[key] = int(table.table_id)      # esito AMBIGUOUS (PF-ALL-F024)
    else:
        c.rc.duplication_guard.acquire(key, pin=True)                     # come la ricostruzione F018
    c.rc._on_signal_received(_c9_signal(price=2.1, received_at="2026-10-08T10:05:00.000000+00:00"))
    assert _c9_rejects(c) == ["evento_gia_in_posizione"], _c9_rejects(c)
    assert len(c.broker.state.orders) == 1, "secondo ordine su una posizione incerta/ricostruita"
    assert c.rc.table_manager.find_by_event_key(key) is not None, "tavolo della posizione liberato dal rifiuto"


@pytest.mark.parametrize("terminal", ["failed", "success"])
def test_c9_1_ambiguous_order_stays_protected_when_a_sibling_order_on_the_same_event_ends(tmp_path, terminal):
    """Quarto riesame c9 (BLOCKER P1): anti-duplicazione disattivata, due ordini sullo stesso evento su due
    tavoli; B diventa AMBIGUOUS, A ha un esito certo. L'esito di A non cancella l'incertezza di B: nessun
    nuovo ordine sull'evento finche' B non ha un esito certo (handler reali del ciclo di vita)."""
    c = _c9_chain(tmp_path)
    c.rc.config.anti_duplication_enabled = False
    c.rc._on_signal_received(_c9_signal())
    c.rc._on_signal_received(_c9_signal(price=2.1, received_at="2026-10-08T10:05:00.000000+00:00"))
    occ = _c9_occupied(c)
    assert len(occ) == 2, occ
    (t1, key), (t2, _) = occ
    c.rc._on_quick_bet_ambiguous({"event_key": key, "table_id": t2, "customer_ref": "B"})
    handler = c.rc._on_quick_bet_failed if terminal == "failed" else c.rc._on_quick_bet_success
    handler({"event_key": key, "table_id": t1, "customer_ref": "A"})
    assert c.rc._uncertain_reservations_map().get(key) == t2, "incertezza di B cancellata dall'esito di A"
    n = len(c.broker.state.orders)
    c.rc._on_signal_received(_c9_signal(price=2.3, received_at="2026-10-08T10:07:00.000000+00:00"))
    assert _c9_rejects(c)[-1] == "evento_gia_in_posizione", _c9_rejects(c)
    assert len(c.broker.state.orders) == n, "nuovo ordine su un evento con un ordine AMBIGUOUS aperto"
    # BLOCK inverso: quando anche B ha un esito certo, l'evento torna libero
    c.rc._on_quick_bet_failed({"event_key": key, "table_id": t2, "customer_ref": "B"})
    assert key not in c.rc._uncertain_reservations_map()


def test_c9_1_certain_outcome_without_table_id_never_clears_ambiguity_on_two_tables(tmp_path):
    from core.runtime_controller import RuntimeController
    rc = RuntimeController.__new__(RuntimeController)
    rc._mark_uncertain("E", 1)
    rc._mark_uncertain("E", 2)
    assert rc._clear_uncertain("E", None) is True and "E" in rc._uncertain_reservations_map()
    assert rc._clear_uncertain("E", 1) is True and rc._uncertain_reservations_map()["E"] == 2
    assert rc._clear_uncertain("E", 2) is False and "E" not in rc._uncertain_reservations_map()
    rc._uncertain_reservations_map()["F"] = 3                     # marcatura senza registro per tavolo
    assert rc._clear_uncertain("F", 3) is False and "F" not in rc._uncertain_reservations_map()


def test_c9_1_is_pinned_tracks_only_live_pinned_keys():
    from core.duplication_guard import DuplicationGuard
    g = DuplicationGuard(ttl_seconds=60)
    assert g.acquire("N") and not g.is_pinned("N") and not g.is_pinned("") and not g.is_pinned("assente")
    assert g.acquire("P", pin=True) and g.is_pinned("P")
    g.unpin("P")
    assert not g.is_pinned("P")
    g.acquire("Q", pin=True)
    g.release("Q")
    assert not g.is_pinned("Q")


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


@pytest.mark.parametrize("status,matched", [("SETTLED", 2.0), ("LAPSED", 0.5), ("VOIDED", 1.0),
                                            ("CANCELLED", 0.5), ("EXECUTION_COMPLETE", 2.0)])
def test_c9_review3_terminal_row_with_any_match_found_by_bet_id_stays_ambiguous(status, matched):
    """Terzo riesame c9: una riga terminale trovata per bet id con QUALSIASI abbinato non chiude mai la saga
    ROLLED_BACK: AMBIGUOUS, riserva tenuta, riconciliazione."""
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
                            target_price=D("1.00"), customer_ref="rb")
    real = port.cancel_all_remaining

    def stale(bet_id):
        out = real(bet_id)
        port.last_snapshot_persisted = False
        return out
    port.cancel_all_remaining = stale
    assert coord.execute(req, stub_price=D("3.00")).reason == "snapshot_not_persisted"
    port.cancel_all_remaining = real
    o = broker.state.orders[store.load("rb")["bet_id"]]
    o.matched_size = matched
    o.status = status
    assert port._snapshot() is True
    res = coord.recover("rb")
    assert res.phase is Phase.AMBIGUOUS and res.reservation_held is True, (status, res.phase, res.reason)
    assert store.load("rb")["phase"] != "ROLLED_BACK"


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


def test_c9_review2_store_unreadable_after_recovery_reports_every_pending_ref(monkeypatch):
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
    monkeypatch.setattr(DbSagaStore, "pending_refs", flaky)
    out = engine.recover_native_microstake_sagas()
    assert out["ok"] is False and out["status"] == "STORE_UNREADABLE" and out["pending"] == ["v"], out


def test_c9_3_store_unreadable_after_recovery_is_fail_closed(monkeypatch):
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
    monkeypatch.setattr(DbSagaStore, "pending_refs", flaky)
    out = engine.recover_native_microstake_sagas()
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



# ------------------------------------------------------------------ ciclo 10 (rilievi Codex su b918a6d)
def test_c10_reset_keeps_every_uncertain_table_of_an_event_even_on_a_settled_market(tmp_path):
    """Codex r4308704060 (P1): con due ordini AMBIGUOUS sullo stesso evento su due tavoli, il reset li
    trattiene ENTRAMBI anche se il mercato e' marcato regolato: un esito ignoto non libera mai il tavolo."""
    c = _c9_chain(tmp_path)
    c.rc.config.anti_duplication_enabled = False
    c.rc._on_signal_received(_c9_signal())
    c.rc._on_signal_received(_c9_signal(price=2.1, received_at="2026-10-08T10:05:00.000000+00:00"))
    occ = _c9_occupied(c)
    assert len(occ) == 2, occ
    (t1, key), (t2, _) = occ
    c.rc._on_quick_bet_ambiguous({"event_key": key, "table_id": t1, "customer_ref": "A"})
    c.rc._on_quick_bet_ambiguous({"event_key": key, "table_id": t2, "customer_ref": "B"})
    market_id = str(c.rc.table_manager.get_table(int(t1)).market_id or "1.234")
    c.rc._mark_market_settled(market_id)
    c.rc.reset_cycle()
    still = {tid for tid, _ in _c9_occupied(c)}
    assert {t1, t2} <= still, ("tavolo con esito ignoto liberato dal reset", still)


def test_c10_recovery_persist_failure_blocks_every_new_entry_until_a_write_succeeds(tmp_path):
    """Codex r4308704062 (P1): stato di recovery non persistito => nessun nuovo ingresso (anche senza
    recovery attiva e con tavoli liberi), finche' una scrittura riesce; il nuovo tentativo e' automatico."""
    c = _c9_chain(tmp_path)
    real_save = c.rc.db.save_settings
    c.rc._recovery_persist_failed = True
    c.rc.db.save_settings = lambda payload: (_ for _ in ()).throw(OSError("disk full"))
    c.rc._on_signal_received(_c9_signal())
    assert c.broker.state.orders == {}, "ordine piazzato con stato di recovery non durevole"
    assert _c9_rejects(c) and _c9_rejects(c)[-1] == "recovery_state_not_persisted", _c9_rejects(c)
    assert _c9_occupied(c) == [], "tavolo lasciato occupato dal rifiuto"
    assert c.rc._risk_allows_auto_trade() == (False, "recovery_state_not_persisted")
    c.rc.db.save_settings = real_save                                  # il disco torna disponibile
    c.rc._on_signal_received(_c9_signal(received_at="2026-10-08T10:09:00.000000+00:00"))
    assert c.rc._recovery_persist_failed is False and len(c.broker.state.orders) == 1, _c9_rejects(c)


def test_c10_pending_saga_reservations_are_rehydrated_before_recovery(monkeypatch):
    """Codex r4308704071 (P1): dopo un riavvio le riserve transitorie delle saghe pendenti
    (reservation_held) vengono ripristinate PRIMA dello snapshot/recovery: anche se la recovery resta
    pendente, lo stub da 2 EUR occupa il cap e un altro microstake non lo supera."""
    from decimal import Decimal
    from core.microstake_sim_port import DbSagaStore, build_native_microstake_executor
    from simulation_broker import SimulationBroker
    sys.path.insert(0, os.path.join(ROOT, "tests"))
    from test_microstake_sim_port import _Db
    db = _Db()
    store = DbSagaStore(db)
    req = {"market_id": "1.1", "selection_id": 7, "side": "BACK", "economic_stake": "0.10", "target_price": "2.00",
           "customer_ref": "held"}
    store.save("held", phase="AMBIGUOUS", reservation_held=True, transient_exposure="2.00", request=req)
    store.save("released", phase="AMBIGUOUS", reservation_held=False, transient_exposure="2.00",
               request=dict(req, customer_ref="released"))
    ex = build_native_microstake_executor(SimulationBroker(starting_balance=100.0, db=_Db()), db,
                                          lambda payload: Decimal("10"))
    monkeypatch.setattr(ex.port, "_snapshot", lambda: False)           # fonte di reload non allineabile
    out = ex.recover_pending()
    assert out == {"held": "AMBIGUOUS", "released": "AMBIGUOUS"}, out
    assert ex.port.has_reservation("held"), "riserva della saga pendente non ripristinata"
    assert not ex.port.has_reservation("released")
    assert ex.port._reservations["held"] == Decimal("2.00")
    assert ex.port.reserve("nuovo", Decimal("9.00"), Decimal("10")) is False, "cap superato oltre lo stub pendente"



@pytest.mark.parametrize("path", ["execute", "recover"])
def test_c10_rollback_with_a_partial_match_is_ambiguous_never_rolled_back(tmp_path, path):
    """Riesame indipendente ciclo 10: se lo stub si abbina in parte appena prima della cancellazione di rollback,
    la saga NON e' ROLLED_BACK (l'engine la registrerebbe FAILED liberando tavolo e chiave su uno stake reale):
    AMBIGUOUS con riserva tenuta, riconciliazione."""
    from core.microstake_safe import (JsonSagaStore, MicrostakeCoordinator, MicrostakePolicy, MicrostakeRequest,
                                      Phase, Side, StatefulExchangeSimulator)

    class _Ex(StatefulExchangeSimulator):
        def __init__(self):
            super().__init__(balance=D("100"))
            self.crash_cancel = path == "recover"

        def replace(self, bet_id, new_price):
            raise RuntimeError("REPLACE_REJECTED")

        def cancel_all_remaining(self, bet_id):
            if self.crash_cancel:
                self.crash_cancel = False
                raise RuntimeError("CANCEL_UNKNOWN")
            o = self.order(bet_id)
            o.matched = D("0.05")                       # abbinato in parte un istante prima del cancel
            o.remaining = o.remaining - D("0.05")
            return super().cancel_all_remaining(bet_id)

    policy = MicrostakePolicy(simulation_only=True, minimum_economic_stake=D("0.10"), exchange_stub_size=D("2.00"),
                              max_transient_exposure=D("10.00"), commission_rate=D("0.045"))
    ex = _Ex()
    store = JsonSagaStore(tmp_path / "s.json")
    req = MicrostakeRequest(market_id="1.100", selection_id=7, side=Side.BACK, economic_stake=D("0.10"),
                            target_price=D("2.00"), customer_ref="pm-1")
    res = MicrostakeCoordinator(ex, store, policy).execute(req, stub_price=D("3.00"))
    if path == "recover":
        assert store.load("pm-1")["phase"] == "ROLLBACK_PENDING"
        res = MicrostakeCoordinator(ex, JsonSagaStore(tmp_path / "s.json"), policy).recover("pm-1")
    assert res.phase is Phase.AMBIGUOUS and res.reason == "rollback_matched", (res.phase, res.reason)
    assert res.reservation_held is True and ex.has_reservation("pm-1")
    assert JsonSagaStore(tmp_path / "s.json").load("pm-1")["phase"] == "AMBIGUOUS"



@pytest.mark.parametrize("bad", [None, "", "nan", "-1", "0", "abc"])
@pytest.mark.parametrize("stub_price,expected", [("6.00", "10.00"), (None, "Infinity"), ("abc", "Infinity")])
def test_c10_unknown_transient_exposure_never_holds_less_than_the_stub_liability(monkeypatch, bad, stub_price, expected):
    """Grok su 9ab753a: importo transitorio della saga ignoto => mai il solo stub da 2 EUR. Si ricalcola la
    liability dello stub (LAY 2 EUR a 6.00 = 10 EUR); senza prezzo, riserva illimitata (fail-closed)."""
    from decimal import Decimal
    from core.microstake_sim_port import DbSagaStore, build_native_microstake_executor
    from simulation_broker import SimulationBroker
    sys.path.insert(0, os.path.join(ROOT, "tests"))
    from test_microstake_sim_port import _Db
    db = _Db()
    store = DbSagaStore(db)
    req = {"market_id": "1.1", "selection_id": 7, "side": "LAY", "economic_stake": "0.10", "target_price": "6.00",
           "customer_ref": "lay"}
    values = {"phase": "AMBIGUOUS", "reservation_held": True, "request": req}
    if bad is not None:
        values["transient_exposure"] = bad
    if stub_price is not None:
        values["stub_price"] = stub_price
    store.save("lay", **values)
    ex = build_native_microstake_executor(SimulationBroker(starting_balance=100.0, db=_Db()), db,
                                          lambda payload: Decimal("10"))
    monkeypatch.setattr(ex.port, "_snapshot", lambda: False)
    ex.recover_pending()
    assert ex.port._reservations["lay"] == Decimal(expected), ex.port._reservations
    assert ex.port.reserve("altro", Decimal("0.50"), Decimal("10")) is False, "cap superato oltre lo stub ignoto"



# ------------------------------------------------------------------ ciclo 11 (preparato): rilievi Codex su 9ab753a
def test_c11_missing_dutching_book_is_unknown_liquidity_when_the_guard_blocks():
    """Codex r4309039358: book Dutching assente con guard armato in blocco => LIQUIDITY_UNKNOWN per ogni gamba
    (mai "liquidita' sufficiente"); in sola osservazione nessuna osservazione, come prima."""
    from types import SimpleNamespace
    from controllers.dutching_controller import DutchingController
    ctl = DutchingController.__new__(DutchingController)
    ctl._market_book = lambda market_id: None
    results = [{"selectionId": 11, "stake": 10.0, "side": "BACK", "price": 2.0},
               {"selectionId": 12, "stake": 5.0, "side": "BACK", "price": 3.0}]
    armed = ctl._evaluate_liquidity_guard(SimpleNamespace(liquidity_warning_only=False), {"market_id": "1.1"}, results)
    assert armed["warning"] is True and [l["reason"] for l in armed["shortfall"]] == ["LIQUIDITY_UNKNOWN"] * 2, armed
    observe = ctl._evaluate_liquidity_guard(SimpleNamespace(liquidity_warning_only=True), {"market_id": "1.1"}, results)
    assert observe == {"warning": False, "shortfall": []}
    off = ctl._evaluate_liquidity_guard(SimpleNamespace(liquidity_guard_enabled=False, liquidity_warning_only=False),
                                        {"market_id": "1.1"}, results)
    assert off == {"warning": False, "shortfall": []}


def test_c11_rollback_intent_cancels_the_remainder_even_after_a_partial_match(tmp_path):
    """Codex r4309039350: rollback deciso e stub abbinato in parte PRIMA del recover => il recover cancella comunque
    il residuo (mai lasciato eseguibile) e poi resta AMBIGUOUS `rollback_matched` con riserva tenuta."""
    from core.microstake_safe import Phase
    ex, path, policy, Coordinator, Store = _rollback_world(tmp_path)
    bet = Store(path).load("rb-1")["bet_id"]
    o = ex.order(bet)
    o.matched = D("0.05")
    o.remaining = o.remaining - D("0.05")
    ex.cancel_all_mode = "ok"
    before = len(ex.operation_log)
    res = Coordinator(ex, Store(path), policy).recover("rb-1")
    ops = [op[0] for op in ex.operation_log[before:]]
    assert ops == ["CANCEL"], ops
    assert ex.order(bet).remaining == D("0")
    assert res.phase is Phase.AMBIGUOUS and res.reason == "rollback_matched" and ex.has_reservation("rb-1")
    again = Coordinator(ex, Store(path), policy).recover("rb-1")                  # idempotente, nessun replace
    assert again.phase is Phase.AMBIGUOUS and "REPLACE" not in [op[0] for op in ex.operation_log[before:]]


def test_c11_f028_releases_the_finalized_orders_own_table_never_the_live_one(tmp_path):
    """Codex r4309039372: anti-duplicazione disattivata, due ordini certi sullo stesso evento su due tavoli; solo A
    e' confermato annullato/decaduto => si libera il tavolo di A, mai quello di B ancora vivo."""
    c = _c9_chain(tmp_path)
    c.rc.config.anti_duplication_enabled = False
    c.rc._on_signal_received(_c9_signal())
    c.rc._on_signal_received(_c9_signal(price=2.1, received_at="2026-10-08T10:05:00.000000+00:00"))
    occ = dict(_c9_occupied(c))
    assert len(occ) == 2, occ
    t_a, t_b = sorted(occ)
    key = occ[t_a]
    cand = [{"customer_ref": "A", "status": "SUBMITTED", "bet_id": "BA", "market_id": "1.234", "event_key": key,
             "table_id": t_a},
            {"customer_ref": "B", "status": "SUBMITTED", "bet_id": "BB", "market_id": "1.234", "event_key": key,
             "table_id": t_b}]
    c.rc._open_order_candidates = lambda: (cand, [])
    c.rc._final_unmatched_bets = lambda markets, bets: {"BA"}
    c.rc._settlement_reconciliation_allowed = lambda: True
    assert c.rc._release_final_unmatched_orders() == ["A"]
    still = {tid for tid, _ in _c9_occupied(c)}
    assert t_b in still and t_a not in still, ("tavolo dell'ordine vivo liberato", still)


def test_c11_f028_without_table_id_and_two_tables_releases_nothing(tmp_path):
    c = _c9_chain(tmp_path)
    c.rc.config.anti_duplication_enabled = False
    c.rc._on_signal_received(_c9_signal())
    c.rc._on_signal_received(_c9_signal(price=2.1, received_at="2026-10-08T10:05:00.000000+00:00"))
    occ = dict(_c9_occupied(c))
    key = next(iter(occ.values()))
    cand = [{"customer_ref": "A", "status": "SUBMITTED", "bet_id": "BA", "market_id": "1.234", "event_key": key,
             "table_id": None}]
    c.rc._open_order_candidates = lambda: (cand, [])
    c.rc._final_unmatched_bets = lambda markets, bets: {"BA"}
    c.rc._settlement_reconciliation_allowed = lambda: True
    assert c.rc._release_final_unmatched_orders() == []
    assert len(_c9_occupied(c)) == 2


# ------------------------------------------------------------------ ciclo 12 (decisione owner 19/10/2026)
def _c12_failing_saga_db(db, phase, exc):
    """`save_settings` del DB dell'app fallisce con `exc` quando la saga passa a `phase`; il resto e' invariato."""
    real = db.save_settings

    def flaky(data):
        raw = next((v for k, v in data.items() if "saga" in str(k).lower()), None)
        if raw and f'"{phase}"' in raw:
            raise exc
        return real(data)

    db.save_settings = flaky


@pytest.mark.parametrize("phase", ["STUB_ACCEPTED", "REDUCED"])
def test_c12_db_error_after_stub_send_is_ambiguous_never_failed(phase):
    """Codex r4311806874 (P1): un errore ordinario del DB (non `SagaStoreError`) DOPO l'invio dello stub era
    classificato PERMANENT => FAILED, con lo stub vivo sul broker e il tavolo libero. Deve restare AMBIGUOUS:
    riserva trattenuta, intento dedupe consumato, ordine contato come posizione fino alla riconciliazione."""
    import sqlite3
    sys.path.insert(0, os.path.join(ROOT, "tests"))
    from test_microstake_engine_native import _engine, _payload, _orders
    engine, broker, db = _engine()
    _c12_failing_saga_db(db, phase, sqlite3.OperationalError("database is locked"))
    res = engine.submit_quick_bet(_payload("c12", size=0.10, price=3.0))
    live = [o for o in _orders(broker) if o.get("customerOrderRef") == "c12"]
    assert live, "probe non significativo: lo stub non e' partito"
    assert str(res.get("status")).upper() == "AMBIGUOUS", res.get("status")
    assert res.get("ok") is not True
    assert engine.microstake_executor.port.has_reservation("c12"), "riserva dello stub vivo rilasciata"
    # anti-duplicazione: lo stesso intento non riparte
    again = engine.submit_quick_bet(_payload("c12", size=0.10, price=3.0))
    assert str(again.get("status")).upper() == "DUPLICATE_BLOCKED", again.get("status")
    assert len([o for o in _orders(broker) if o.get("customerOrderRef") == "c12"]) == len(live), "secondo invio"


def test_c12_db_error_before_any_send_stays_a_plain_failure():
    """Senza operazioni exchange l'errore resta un fallimento: nessun ordine sul broker, nessuna riserva appesa."""
    import sqlite3
    sys.path.insert(0, os.path.join(ROOT, "tests"))
    from test_microstake_engine_native import _engine, _payload, _orders
    engine, broker, db = _engine()
    _c12_failing_saga_db(db, "PREPARED", sqlite3.OperationalError("disk I/O error"))
    res = engine.submit_quick_bet(_payload("c12-pre", size=0.10, price=3.0))
    assert not [o for o in _orders(broker) if o.get("customerOrderRef") == "c12-pre"]
    assert str(res.get("status")).upper() != "AMBIGUOUS", res.get("status")
    assert not engine.microstake_executor.port.has_reservation("c12-pre")


def _c12_rc(db):
    sys.path.insert(0, os.path.join(ROOT, "tests", "safety"))
    from test_emergency_stop import _make_rc
    rc, bus = _make_rc(db=db)
    return rc, bus


class _C12Broker:
    def __init__(self):
        self.reads = 0

    def list_settlements(self):
        self.reads += 1
        return []


def _c12_poll(rc):
    broker = _C12Broker()
    rc.betfair_service.get_simulation_broker = lambda: broker
    rc._poll_simulated_settlements_locked()
    return broker.reads


def _c12_db(settings=None):
    sys.path.insert(0, os.path.join(ROOT, "tests", "safety"))
    from test_emergency_stop import _PersistentDb
    return _PersistentDb(settings=settings)


def test_c12_live_emergency_source_survives_restart():
    """Codex r4311806886 (P1): la provenienza LIVE dei settlement era solo in memoria. Dopo il riavvio in
    emergenza il registro SIM non deve valere per le posizioni LIVE aperte prima dello stop."""
    db = _c12_db()
    rc1, _ = _c12_rc(db)
    rc1.set_simulation_mode(False)
    rc1.emergency_stop(reason="c12_live")
    assert db.settings.get("emergency_settlement_source") == "LIVE", db.settings
    rc2, bus2 = _c12_rc(db)
    assert rc2.is_emergency_stopped is True
    assert rc2._settlement_source_before_emergency == "LIVE"
    assert _c12_poll(rc2) == 0, "registro SIM letto per posizioni LIVE dopo il riavvio"
    assert any(n == "SETTLEMENT_RECONCILIATION_UNCERTAIN" for n, _ in bus2.published)


def test_c12_source_is_persisted_before_the_sim_flip():
    """Il marker entra nello STESSO salvataggio del flag di emergenza: un crash subito dopo non lo perde."""
    db = _c12_db()
    rc, _ = _c12_rc(db)
    rc.set_simulation_mode(False)
    writes = []
    real = db.save_settings
    db.save_settings = lambda data: (writes.append(dict(data)), real(data))[1]
    rc.emergency_stop(reason="c12_order")
    first = writes[0]
    assert first.get("emergency_stopped") == "1" and first.get("emergency_settlement_source") == "LIVE", first


def test_c12_sim_emergency_restart_keeps_reading_sim_ledger():
    db = _c12_db()
    rc1, _ = _c12_rc(db)
    rc1.set_simulation_mode(True)
    rc1.emergency_stop(reason="c12_sim")
    rc2, _ = _c12_rc(db)
    assert rc2._settlement_source_before_emergency == "SIM"
    assert _c12_poll(rc2) == 1


@pytest.mark.parametrize("settings", [
    {"emergency_stopped": "1", "emergency_reason": "legacy"},                                  # senza marker
    {"emergency_stopped": "1", "emergency_reason": "x", "emergency_settlement_source": "??"},  # marker invalido
])
def test_c12_unknown_source_after_restart_is_fail_closed(settings):
    rc, _ = _c12_rc(_c12_db(settings))
    assert rc.is_emergency_stopped is True
    assert rc._settlement_source_before_emergency == "UNKNOWN"
    assert _c12_poll(rc) == 0, "provenienza ignota trattata come SIM"


def test_c12_daily_loss_durable_marker_records_the_source():
    db = _c12_db()
    rc, _ = _c12_rc(db)
    rc.set_simulation_mode(False)
    assert rc._persist_db_emergency_marker(active=True, reason="daily_loss") == ""
    rc2, _ = _c12_rc(db)
    assert rc2._settlement_source_before_emergency == "LIVE"


def test_c12_reset_clears_the_persisted_source():
    db = _c12_db()
    rc, _ = _c12_rc(db)
    rc.set_simulation_mode(False)
    rc.emergency_stop(reason="c12_reset")
    rc.reset_emergency()
    assert db.settings.get("emergency_settlement_source") == ""
    rc2, _ = _c12_rc(db)
    assert rc2.is_emergency_stopped is False and rc2._settlement_source_before_emergency == ""


def test_c12_db_error_after_stub_send_keeps_the_durable_intent_consumed():
    """Riesame indipendente ciclo 12 (lacuna P2): con la dedupe DUREVOLE, l'intento dello stub vivo resta consumato
    anche per un engine nuovo sullo stesso DB (memoria persa, come dopo un riavvio): nessun secondo invio."""
    import sqlite3
    sys.path.insert(0, os.path.join(ROOT, "tests"))
    from test_microstake_engine_native import _DB, _engine, _payload, _orders
    from helpers.fake_consumed_intents import FakeConsumedIntentsMixin

    class _DurableDB(FakeConsumedIntentsMixin, _DB):
        pass

    db = _DurableDB()
    engine, broker, _ = _engine(db)
    _c12_failing_saga_db(db, "STUB_ACCEPTED", sqlite3.OperationalError("database is locked"))
    res = engine.submit_quick_bet(_payload("c12-dur", size=0.10, price=3.0))
    assert str(res.get("status")).upper() == "AMBIGUOUS", res.get("status")
    assert db.is_order_intent_consumed("c12-dur"), "intento durevole rilasciato con lo stub vivo"
    sent = len([o for o in _orders(broker) if o.get("customerOrderRef") == "c12-dur"])
    engine2, _, _ = _engine(db)
    engine2.simulation_broker = broker
    engine2.runtime_controller = engine.runtime_controller
    again = engine2.submit_quick_bet(_payload("c12-dur", size=0.10, price=3.0))
    assert len([o for o in _orders(broker) if o.get("customerOrderRef") == "c12-dur"]) == sent, "secondo invio"
    assert str(again.get("status")).upper() == "DUPLICATE_BLOCKED", again.get("status")


@pytest.mark.parametrize("fail_lookup", [False, True])
def test_c12_post_accept_read_failure_never_releases_a_live_stub(fail_lookup):
    """Riesame indipendente ciclo 12: ordine accettato dal broker ma lettura successiva fallita (`order(bet_id)`).
    Prima: eccezione generica => riserva rilasciata e saga FAILED con lo stub vivo. Ora e' una risposta persa:
    riconciliazione per customer_ref; se anche quella fallisce, AMBIGUOUS con riserva tenuta. Mai FAILED."""
    sys.path.insert(0, os.path.join(ROOT, "tests"))
    from test_microstake_engine_native import _engine, _payload, _orders
    engine, broker, db = _engine()
    engine._default_native_microstake_executor("SIMULATION")
    port = engine.microstake_executor.port
    real_order, real_find = port.order, port.find_by_ref
    calls = {"n": 0}

    def flaky_order(bet_id):
        calls["n"] += 1
        if calls["n"] == 1:
            raise KeyError(bet_id)
        return real_order(bet_id)

    port.order = flaky_order
    if fail_lookup:
        placed = {"done": False}
        orig_place = port.broker.place_bet

        def place_and_mark(**kw):
            out = orig_place(**kw)
            placed["done"] = True
            return out

        port.broker.place_bet = place_and_mark

        def flaky_find(ref):
            if placed["done"]:
                raise RuntimeError("lookup unavailable")
            return real_find(ref)

        port.find_by_ref = flaky_find
    res = engine.submit_quick_bet(_payload("c12-pa", size=0.10, price=3.0))
    live = [o for o in _orders(broker) if o.get("customerOrderRef") == "c12-pa"]
    assert live, "probe non significativo"
    status = str(res.get("status")).upper()
    assert status != "FAILED", res.get("error")
    if fail_lookup:
        assert status == "AMBIGUOUS", status
        assert port.has_reservation("c12-pa"), "riserva rilasciata con lo stub vivo"
    else:
        # riconciliato per customer_ref: la saga prosegue e si attiva normalmente (riserva sciolta solo ad ACTIVE)
        assert res.get("ok") is True and status in {"SUBMITTED", "ACCEPTED_FOR_PROCESSING"}, status


# ------------------------------------------------------------------ ciclo 13 (PF-QA-AUTONOMOUS-CONTINUATION-01)
@pytest.mark.parametrize("bad", [-0.01, -5.0])
def test_c13_negative_persisted_recovery_loss_is_unreadable_state(tmp_path, bad):
    """Codex r4312360615: una perdita di recovery persistita NEGATIVA era saltata in silenzio e il restore
    riportava successo, cancellando il debito. Ora e' stato illeggibile (fail-closed); lo zero resta ammesso."""
    sys.path.insert(0, os.path.join(ROOT, "tests", "f026"))
    from test_f032_rsr12_units import _rc
    rc = _rc(tmp_path / "neg.db")
    rc.db.save_settings({"roserpina.recovery_tables_state": json.dumps({"1": 3.0, "2": bad})})
    assert rc._restore_recovery_tables() is False
    assert rc._recovery_state_unreadable is True
    assert rc._active_recovery_count() >= rc.config.max_recovery_tables, "debito negativo trattato come zero"


def test_c13_zero_persisted_recovery_loss_is_still_accepted(tmp_path):
    sys.path.insert(0, os.path.join(ROOT, "tests", "f026"))
    from test_f032_rsr12_units import _rc
    rc = _rc(tmp_path / "zero.db")
    rc.db.save_settings({"roserpina.recovery_tables_state": json.dumps({"1": 3.0, "2": 0.0})})
    assert rc._restore_recovery_tables() is True
    assert rc._active_recovery_count() == 1


def test_c13_cancelled_error_raised_by_the_task_is_ambiguous_not_pre_send():
    """Codex r4312360608: un `CancelledError` sollevato DALLA callable (trasporto partito) non e' un Future
    cancellato prima dell'avvio: AMBIGUOUS, mai `_ErrorePrimaDellInvio` (che rilascerebbe l'intento)."""
    import concurrent.futures
    import types as _t
    from core.trading_engine import TradingEngine, _ErrorePrimaDellInvio, ExecutionError, ERROR_AMBIGUOUS
    eng = TradingEngine.__new__(TradingEngine)
    eng.executor = _t.SimpleNamespace(default_timeout=1.0)
    f = concurrent.futures.Future()
    f.set_running_or_notify_cancel()
    f.set_exception(concurrent.futures.CancelledError("transport cancelled after send"))
    assert not f.cancelled()
    with pytest.raises(ExecutionError) as ei:
        eng._resolve_executor_future(f)
    assert not isinstance(ei.value, _ErrorePrimaDellInvio)
    assert ei.value.error_type == ERROR_AMBIGUOUS


def test_c13_really_cancelled_future_stays_pre_send():
    import concurrent.futures
    import types as _t
    from core.trading_engine import TradingEngine, _ErrorePrimaDellInvio
    eng = TradingEngine.__new__(TradingEngine)
    eng.executor = _t.SimpleNamespace(default_timeout=1.0)
    f = concurrent.futures.Future()
    assert f.cancel()
    with pytest.raises(_ErrorePrimaDellInvio):
        eng._resolve_executor_future(f)


@pytest.mark.parametrize("result", [{"moreAvailable": False}, {"currentOrders": None, "moreAvailable": False}])
def test_c13_missing_current_orders_collection_is_malformed(result):
    """Codex r4312360620: `currentOrders` assente o null non e' una lista vuota autorevole: la ricostruzione F018
    chiuderebbe ogni SUBMITTED persistito. Errore esplicito; la lista vuota esplicita resta il caso legittimo."""
    from betfair_client import BetfairClient
    client = BetfairClient.__new__(BetfairClient)
    client._post_jsonrpc = lambda *a, **k: dict(result)
    with pytest.raises(RuntimeError, match="CURRENT_ORDERS_MALFORMED"):
        client.get_current_orders()


def test_c13_explicit_empty_current_orders_is_no_orders():
    from betfair_client import BetfairClient
    client = BetfairClient.__new__(BetfairClient)
    client._post_jsonrpc = lambda *a, **k: {"currentOrders": [], "moreAvailable": False}
    assert client.get_current_orders() == []


def test_c13_service_treats_a_missing_collection_as_malformed():
    """Stesso difetto a livello di servizio (input reale di F018): `None` dal client non e' "nessun ordine"."""
    with pytest.raises(RuntimeError, match="CURRENT_ORDERS_MALFORMED"):
        BetfairService._well_formed_current_orders(None)
    assert BetfairService._well_formed_current_orders([]) == []


# ------------------------------------------------------------------ ciclo 14 (verifica indipendente dei rinvii)
class _C14Db:
    """Ordini del bot + settings persistiti, per il registro dei mercati con hedge di cashout."""

    def __init__(self, payloads=(), settings=None, fail_save=False, fail_load=False, bot_bets=("111",)):
        self.payloads = [dict(p) for p in payloads]
        self.bot_bets = list(bot_bets)
        self.settings = dict(settings or {})
        self.fail_save = fail_save
        self.fail_load = fail_load

    def _execute(self, sql, *args, **kwargs):
        if "FROM orders" in sql:
            # PR29-c: ogni ordine del bot porta il customer_ref dell'intento (identita' della riserva)
            return [{"payload_json": json.dumps(p), "status": "SUBMITTED", "customer_ref": "REF-A"} for p in self.payloads]
        return []

    def get_settings(self):
        if self.fail_load:
            raise RuntimeError("settings down")
        return dict(self.settings)

    def save_settings(self, data):
        if self.fail_save:
            raise RuntimeError("disk full")
        self.settings.update({str(k): v for k, v in data.items()})

    def get_bot_active_orders(self):
        return [{"bet_id": b, "market_id": "1.10", "event_name": "E"} for b in self.bot_bets]

    def find_orders_by_bet_id(self, bet_id):
        # PR29-c: la bet si lega all'intento tramite la sua riga `orders`
        if str(bet_id) not in self.bot_bets:
            return []
        return [{"customer_ref": "REF-A", "status": "SUBMITTED", "payload": dict(p)} for p in self.payloads]


# PR29-c: settlement cleared per bet della sola bet del bot (identita' completa). Senza hedge rilascerebbe: i test
# che lo trattengono misurano quindi il controllo hedge, non la mancanza di identita'.
_C14_CLEARED = {"market_id": "1.10", "event_key": "cleared:1.10:111", "settlement_ref": "111"}


def _c14_settle(rc, pnl=-10.0):
    rc._release_settled_tables(dict(_C14_CLEARED), table_id=None, pnl=pnl, settlement_key="cleared:1.10:111")


def _c14_rc(db):
    sys.path.insert(0, os.path.join(ROOT, "tests", "f026"))
    from test_f026_units import _rc, _activate
    rc = _rc()
    rc.db = db
    rc._load_hedged_markets()
    _activate(rc, 1, "EV-A", "1.10", ref="REF-A")
    rc.betfair_service = _C16Service(rows=[{"betId": b, "marketId": "1.10"} for b in db.bot_bets])
    return rc


def _c14_hedge(rc, market="1.10"):
    rc._on_cashout_command({"market_id": market, "selection_id": 7, "side": "LAY", "price": 2.0, "stake": 5.0,
                            "green_up": 0.5})


def test_c14_cashout_hedge_blocks_release_by_market_with_one_leg_pnl():
    """Verifica indipendente (r4303297587, residuo BLOCKING): posizione singola + hedge di cashout. Il settlement
    per bet senza table_id di UNA gamba liberava il tavolo con il P&L di quella gamba sola => perdita di recovery
    (e stake successivo) sbagliata. Con un hedge registrato sul mercato: nessun rilascio, perdita invariata."""
    rc = _c14_rc(_C14Db([{"market_id": "1.10", "event_key": "EV-A", "selection_id": 7}]))
    _c14_hedge(rc)
    _c14_settle(rc)
    t = {x["table_id"]: x for x in rc.table_manager.snapshot()}[1]
    assert t["current_event_key"] == "EV-A", "tavolo liberato con il P&L di una sola gamba"
    assert t["loss_amount"] == 0.0


def test_c14_hedge_registry_survives_restart():
    db = _C14Db([{"market_id": "1.10", "event_key": "EV-A", "selection_id": 7}])
    rc = _c14_rc(db)
    _c14_hedge(rc)
    again = _c14_rc(db)                                      # nuovo processo, stesso DB
    _c14_settle(again)
    t = {x["table_id"]: x for x in again.table_manager.snapshot()}[1]
    assert t["current_event_key"] == "EV-A", "registro degli hedge perso al riavvio"


@pytest.mark.parametrize("kind", ["save", "load"])
def test_c14_unreliable_hedge_registry_is_fail_closed(kind):
    db = _C14Db([{"market_id": "1.10", "event_key": "EV-A", "selection_id": 7}],
                fail_save=(kind == "save"), fail_load=(kind == "load"))
    rc = _c14_rc(db)
    if kind == "save":
        _c14_hedge(rc, market="9.99")                        # hedge su ALTRO mercato, ma non persistito
    _c14_settle(rc)
    t = {x["table_id"]: x for x in rc.table_manager.snapshot()}[1]
    assert t["current_event_key"] == "EV-A", "registro inaffidabile trattato come vuoto"


def test_c14_without_hedge_the_unique_single_bet_table_is_still_released():
    rc = _c14_rc(_C14Db([{"market_id": "1.10", "event_key": "EV-A", "selection_id": 7}]))
    _c14_hedge(rc, market="2.20")                            # hedge su un altro mercato: irrilevante
    rc._release_settled_tables({"market_id": "1.10", "event_key": "sim:1.10:S1"}, table_id=None, pnl=-10.0,
                               settlement_key="sim:1.10:S1")
    t = {x["table_id"]: x for x in rc.table_manager.snapshot()}[1]
    assert t["current_event_key"] == "" and t["loss_amount"] == 10.0


def test_c14_runtime_marks_every_cashout_outcome_and_leaves_the_command_single_consumer():
    import inspect
    from core.runtime_controller import RuntimeController
    src = inspect.getsource(RuntimeController)
    assert "subscribe(CASHOUT_SUCCESS, self._on_cashout_command)" in src
    assert "subscribe(CASHOUT_FAILED, self._on_cashout_command)" in src
    assert "subscribe(CMD_EXECUTE_CASHOUT" not in src, "CMD_EXECUTE_CASHOUT deve restare a consumatore unico"


def test_c14_sim_market_level_settlement_still_releases_after_a_hedge():
    """Il settlement SIM e' PER MERCATO (`sim:<mercato>:<id>`): il broker regola il netto di tutte le gambe,
    hedge compreso, quindi il P&L e' il netto del tavolo e il rilascio resta corretto."""
    rc = _c14_rc(_C14Db([{"market_id": "1.10", "event_key": "EV-A", "selection_id": 7}]))
    _c14_hedge(rc)
    rc._release_settled_tables({"market_id": "1.10", "event_key": "sim:1.10:S1"}, table_id=None, pnl=-0.5,
                               settlement_key="sim:1.10:S1")
    t = {x["table_id"]: x for x in rc.table_manager.snapshot()}[1]
    assert t["current_event_key"] == "" and t["loss_amount"] == 0.5


def test_c14_cleared_per_bet_settlement_with_hedge_is_held():
    rc = _c14_rc(_C14Db([{"market_id": "1.10", "event_key": "EV-A", "selection_id": 7}]))
    _c14_hedge(rc)
    _c14_settle(rc)
    t = {x["table_id"]: x for x in rc.table_manager.snapshot()}[1]
    assert t["current_event_key"] == "EV-A" and t["loss_amount"] == 0.0


def test_c14_outcome_without_market_is_ignored_not_unreliable():
    rc = _c14_rc(_C14Db([{"market_id": "1.10", "event_key": "EV-A", "selection_id": 7}]))
    rc._on_cashout_command({"reason": "bridge_rejected"})
    assert rc._hedged_markets_unreliable is False
    rc._release_settled_tables({"market_id": "1.10", "event_key": "sim:1.10:S1"}, table_id=None, pnl=-10.0,
                               settlement_key="sim:1.10:S1")
    t = {x["table_id"]: x for x in rc.table_manager.snapshot()}[1]
    assert t["current_event_key"] == "" and t["loss_amount"] == 10.0


@pytest.mark.parametrize("book", [
    {"runners": [{"selectionId": 7, "status": "ACTIVE",
                  "ex": {"availableToBack": [{"price": 3.0, "size": 50}], "availableToLay": []}}]},          # no status
    {"status": "", "runners": [{"selectionId": 7, "status": "ACTIVE",
                                "ex": {"availableToBack": [{"price": 3.0, "size": 50}], "availableToLay": []}}]},
    {"status": "OPEN", "runners": [{"selectionId": 7,
                                    "ex": {"availableToBack": [{"price": 3.0, "size": 50}], "availableToLay": []}}]},
])
def test_c14_missing_market_or_runner_status_is_not_observed_liquidity(book):
    """Codex r4312719713: uno stato di mercato/selezione assente era letto come OPEN/ACTIVE e la liquidita'
    risultava osservata (ingresso ammesso da una risposta incompleta). Ora stato esplicito obbligatorio."""
    from core.liquidity import single_entry_liquidity
    value, reason = single_entry_liquidity(book, 7, "BACK", 3.0)
    assert value is None and reason in {"LIQUIDITY_MARKET_NOT_OPEN", "LIQUIDITY_RUNNER_NOT_ACTIVE"}, (value, reason)


def test_c14_explicit_open_active_book_is_still_observed():
    from core.liquidity import single_entry_liquidity
    book = {"status": "OPEN", "runners": [{"selectionId": 7, "status": "ACTIVE",
                                           "ex": {"availableToBack": [{"price": 3.0, "size": 50}], "availableToLay": []}}]}
    value, reason = single_entry_liquidity(book, 7, "BACK", 3.0)
    assert value is not None and reason == ""


def test_c14_existing_customer_ref_on_the_broker_is_ambiguous_never_failed():
    """Codex r4312719704: un ordine gia' presente sul broker con lo stesso customer_ref (stato SIM ripristinato senza
    la saga) dava DUPLICATE_CUSTOMER_REF prima dell'operation log => FAILED, riserva rilasciata e pre_send=True:
    l'engine rilasciava intento e tavolo con l'ordine vivo. Ora AMBIGUOUS con riserva trattenuta."""
    sys.path.insert(0, os.path.join(ROOT, "tests"))
    from test_microstake_engine_native import _engine, _payload, _orders
    engine, broker, db = _engine()
    broker.place_bet(market_id="1.100", selection_id=7, side="BACK", price=3.0, size=2.0, customer_ref="c14-dup")
    engine._default_native_microstake_executor("SIMULATION")
    ex = engine.microstake_executor
    res = ex.execute(_payload("c14-dup", size=0.10, price=3.0))
    assert res.get("status") == "AMBIGUOUS", res
    assert res.get("pre_send") is not True
    assert ex.port.has_reservation("c14-dup"), "riserva rilasciata con un ordine vivo sullo stesso customer_ref"


def test_c14_recover_never_adopts_a_pre_existing_order_with_the_same_ref():
    """Riesame indipendente ciclo 14: dopo AMBIGUOUS `existing_customer_ref`, recover() riduceva e riprezzava l'ordine
    preesistente come se fosse il proprio stub (fino ad ACTIVE). Ora resta AMBIGUOUS, nessuna operazione exchange."""
    sys.path.insert(0, os.path.join(ROOT, "tests"))
    from test_microstake_engine_native import _engine, _payload, _orders
    engine, broker, db = _engine()
    broker.place_bet(market_id="1.100", selection_id=7, side="BACK", price=2.5, size=2.0, customer_ref="c14-adopt")
    engine._default_native_microstake_executor("SIMULATION")
    ex = engine.microstake_executor
    assert ex.execute(_payload("c14-adopt", size=0.10, price=3.0)).get("status") == "AMBIGUOUS"
    ops_before = list(ex.port.operation_log)
    before = [(o["sizeRemaining"], o["priceSize"]["price"]) for o in _orders(broker) if o.get("customerOrderRef") == "c14-adopt"]
    out = ex.recover_pending()
    assert out.get("c14-adopt") == "AMBIGUOUS", out
    after = [(o["sizeRemaining"], o["priceSize"]["price"]) for o in _orders(broker) if o.get("customerOrderRef") == "c14-adopt"]
    assert after == before == [(2.0, 2.5)], (before, after)
    assert ex.port.operation_log == ops_before, "operazioni exchange sull'ordine preesistente"


def test_c14_hedge_registry_concurrent_updates_are_not_lost():
    import threading
    import time
    db = _C14Db([{"market_id": "1.10", "event_key": "EV-A", "selection_id": 7}])
    rc = _c14_rc(db)
    from collections.abc import Mapping

    class _SlowRegistry(Mapping):            # copia lenta: finestra larga fra lettura e riscrittura del registro
        def __init__(self):
            from datetime import datetime, timezone
            self._d = {"0.0": datetime.now(timezone.utc).isoformat()}   # non vuoto: niente scorciatoia `or {}`

        def __getitem__(self, k):
            return self._d[k]

        def __iter__(self):
            time.sleep(0.05)
            return iter(self._d)

        def __len__(self):
            return len(self._d)

    rc._hedged_markets = _SlowRegistry()
    threads = [threading.Thread(target=_c14_hedge, args=(rc, f"5.{i}")) for i in range(16)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    persisted = json.loads(db.settings["roserpina.hedged_markets"])
    assert {f"5.{i}" for i in range(16)} <= set(persisted), sorted(persisted)


# ------------------------------------------------------------------ ciclo 15 (bloccante Sol su 180ffa3)
def test_c15_failed_registry_save_survives_restart_as_unreliable(tmp_path):
    """Sol su 180ffa3: se save_settings fallisce dopo un hedge, il fail-closed restava in RAM; al riavvio il registro
    leggibile (senza quel mercato) risultava affidabile e il settlement di UNA gamba liberava il tavolo. Ora una
    sentinella durevole accanto al DB rende il registro inaffidabile anche dopo il riavvio."""
    db = _C14Db([{"market_id": "1.10", "event_key": "EV-A", "selection_id": 7}], fail_save=True)
    db.db_path = str(tmp_path / "pickfair.db")
    rc = _c14_rc(db)
    _c14_hedge(rc)                                           # hedge sul mercato 1.10, registro NON persistito
    sentinel = tmp_path / "pickfair.db.hedged_markets.unreliable"
    assert sentinel.exists(), "nessuna traccia durevole del registro non persistito"
    db.fail_save = False                                     # il DB torna a funzionare, processo riavviato
    again = _c14_rc(db)
    assert again._hedged_markets_unreliable is True
    _c14_settle(again)
    t = {x["table_id"]: x for x in again.table_manager.snapshot()}[1]
    assert t["current_event_key"] == "EV-A" and t["loss_amount"] == 0.0, "rilascio con P&L di una sola gamba"


def test_c15_no_sentinel_without_a_failure(tmp_path):
    db = _C14Db([{"market_id": "1.10", "event_key": "EV-A", "selection_id": 7}])
    db.db_path = str(tmp_path / "pickfair.db")
    rc = _c14_rc(db)
    _c14_hedge(rc, market="2.20")
    assert not (tmp_path / "pickfair.db.hedged_markets.unreliable").exists()
    assert _c14_rc(db)._hedged_markets_unreliable is False


def test_c15_unreliable_registry_is_visible_and_notified(tmp_path):
    db = _C14Db([{"market_id": "1.10", "event_key": "EV-A", "selection_id": 7}], fail_save=True)
    db.db_path = str(tmp_path / "pickfair.db")
    rc = _c14_rc(db)
    sys.path.insert(0, os.path.join(ROOT, "tests", "f026"))
    from test_f026_units import _Bus
    rc.bus = _Bus()
    _c14_hedge(rc)
    events = [n for n, _p in rc.bus.events]
    assert "HEDGE_REGISTRY_UNRELIABLE" in events, events
    import inspect
    from core.runtime_controller import RuntimeController
    assert 'data["hedged_markets_unreliable"]' in inspect.getsource(RuntimeController.get_status)


# ------------------------------------------------------------------ ciclo 16 (bloccante Sol su 23bdbfe)
class _C16Service:
    def __init__(self, rows=None, fail=False):
        self.rows = rows
        self.fail = fail
        self.calls = []

    def list_cleared_orders(self, **kw):
        self.calls.append(kw)
        if self.fail:
            raise RuntimeError("betfair down")
        return list(self.rows or [])


class _C16Db(_C14Db):
    pass


def _c16_rc(service, *, bot_bets=("111",), fail_save=False, tmp_path=None):
    db = _C16Db([{"market_id": "1.10", "event_key": "EV-A", "selection_id": 7}], bot_bets=bot_bets, fail_save=fail_save)
    if tmp_path is not None:
        db.db_path = str(tmp_path / "ro" / "pickfair.db")          # directory inesistente: sentinella non scrivibile
    rc = _c14_rc(db)
    rc.betfair_service = service
    return rc


def _c16_table(rc):
    return {x["table_id"]: x for x in rc.table_manager.snapshot()}[1]


def test_c16_foreign_settled_leg_on_the_exchange_blocks_release_without_any_local_state(tmp_path):
    """Sol su 23bdbfe: con filesystem in sola lettura falliscono registro E sentinella; dopo il riavvio il registro
    vecchio e' 'affidabile'. La riconciliazione sull'exchange non dipende da scritture locali: una bet regolata sul
    mercato che non e' del bot (l'hedge) => nessun rilascio per mercato."""
    service = _C16Service(rows=[{"betId": "111", "marketId": "1.10"}, {"betId": "999", "marketId": "1.10"}])
    rc = _c16_rc(service, fail_save=True, tmp_path=tmp_path)
    _c14_hedge(rc)                                           # registro E sentinella falliscono
    again = _c16_rc(service, tmp_path=tmp_path)              # riavvio: nessuna traccia locale dell'hedge
    assert again._hedged_markets_unreliable is False
    again._release_settled_tables({"market_id": "1.10", "event_key": "cleared:1.10:111", "settlement_ref": "111"},
                                  table_id=None, pnl=-10.0, settlement_key="cleared:1.10:111")
    t = _c16_table(again)
    assert t["current_event_key"] == "EV-A" and t["loss_amount"] == 0.0, "rilascio con il P&L di una sola gamba"
    assert service.calls and service.calls[-1].get("market_ids") == ["1.10"]


@pytest.mark.parametrize("service", [_C16Service(fail=True), _C16Service(rows=[]),
                                     _C16Service(rows=[{"marketId": "1.10"}]), _C16Service(rows=["x"])])
def test_c16_unreadable_exchange_reconciliation_is_fail_closed(service):
    rc = _c16_rc(service)
    rc._release_settled_tables({"market_id": "1.10", "event_key": "cleared:1.10:111", "settlement_ref": "111"},
                               table_id=None, pnl=-10.0, settlement_key="cleared:1.10:111")
    assert _c16_table(rc)["current_event_key"] == "EV-A"


def test_c16_only_bot_legs_on_the_exchange_still_release():
    rc = _c16_rc(_C16Service(rows=[{"betId": "111", "marketId": "1.10"}]))
    rc._release_settled_tables({"market_id": "1.10", "event_key": "cleared:1.10:111", "settlement_ref": "111"},
                               table_id=None, pnl=-10.0, settlement_key="cleared:1.10:111")
    t = _c16_table(rc)
    assert t["current_event_key"] == "" and t["loss_amount"] == 10.0


# ------------------------------------------------------------------ ciclo 17 (bloccante Sol su 9f0d6ec)
@pytest.mark.parametrize("payload", [{"market_id": "1.10"}, {"market_id": "1.10", "event_key": "EV-A"}])
def test_c17_keyless_or_position_settlement_also_reconciles_on_the_exchange(tmp_path, payload):
    """Sol su 9f0d6ec: la riconciliazione scattava solo per `cleared:`; un settlement senza chiave (o con la chiave
    della posizione) dopo un riavvio senza tracce dell'hedge liberava ancora il tavolo con il P&L di una sola gamba."""
    service = _C16Service(rows=[{"betId": "111", "marketId": "1.10"}, {"betId": "999", "marketId": "1.10"}])
    rc = _c16_rc(service, fail_save=True, tmp_path=tmp_path)
    _c14_hedge(rc)
    again = _c16_rc(service, tmp_path=tmp_path)
    again._release_settled_tables(dict(payload), table_id=None, pnl=-10.0,
                                  settlement_key=again._build_bankroll_sync_key(dict(payload)))
    t = _c16_table(again)
    assert t["current_event_key"] == "EV-A" and t["loss_amount"] == 0.0, "settlement per bet non verificato"


def test_c17_keyless_settlement_without_identity_is_held_even_with_only_bot_legs():
    """#461 PR29-c: un settlement senza chiave e senza bet non e' legabile a una riserva (nessuna identita'
    durevole): resta trattenuto, anche se sull'exchange ci sono solo gambe del bot. Prima di PR29-c liberava il
    tavolo; la stessa bet con identita' (`settlement_ref`) rilascia ancora (test c16 qui sopra)."""
    rc = _c16_rc(_C16Service(rows=[{"betId": "111", "marketId": "1.10"}]))
    payload = {"market_id": "1.10"}
    rc._release_settled_tables(payload, table_id=None, pnl=-10.0, settlement_key=rc._build_bankroll_sync_key(payload))
    t = _c16_table(rc)
    assert t["current_event_key"] == "EV-A" and t["loss_amount"] == 0.0
