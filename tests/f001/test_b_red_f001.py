"""PF-ALL-F001 — suite B-RED (fase B, NON committata nel repository).

Componenti REALI: TradingEngine, ExecutorManager (pool di thread vero, Future vero), Database SQLite,
EventBus, SettingsService, BetfairService, RiskGate/RoserpinaRiskLimits, HeadlessApp (scenari applicativi).
Simulato solo il trasporto esterno: SimulationBroker con esito pilotato (FaultBroker).
Nessun FakeExecutor sincrono sostituisce il contratto dell'ExecutorManager; gli unici executor
non-ExecutorManager (B-RED-08, B-RED-12b) riproducono stati del Future che il pool reale non permette
di forzare in modo deterministico, e restituiscono un concurrent.futures.Future autentico.

Contratto atteso: vedi PF-F001-ASYNC-CONTRACT.md (C1..C12).
Esecuzione: PF_TREE=<albero> PF_PY=<python> PF_NETGUARD=<dir sitecustomize> python -m pytest -q phaseB/test_b_red_f001.py
"""
import concurrent.futures
import json
import os
import subprocess
import sys
import tempfile
import threading
import time

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
# Default: radice del repository (il file vive in tests/f001/). PF_TREE resta per
# eseguire la stessa suite su un albero esterno (baseline RED della fase B).
TREE = os.environ.get("PF_TREE") or os.path.dirname(os.path.dirname(HERE))
PY = os.environ.get("PF_PY", sys.executable)
NETGUARD = os.environ.get("PF_NETGUARD", "")
if TREE not in sys.path:
    sys.path.insert(0, TREE)

from database import Database  # noqa: E402
from core.event_bus import EventBus  # noqa: E402
from executor_manager import ExecutorManager  # noqa: E402
from services.setting_service import SettingsService  # noqa: E402
from services.betfair_service import BetfairService  # noqa: E402
from simulation_broker import SimulationBroker  # noqa: E402
from core.trading_engine import TradingEngine  # noqa: E402
from core.risk_gate import RiskGate, RoserpinaRiskLimits  # noqa: E402
from safe_mode import get_safe_mode_manager  # noqa: E402


def live_ok(mid, bid="B-LIVE-1", matched=0.5, price=3.0):
    return {"status": "SUCCESS", "marketId": mid, "instructionReports": [
        {"status": "SUCCESS", "betId": bid, "sizeMatched": matched, "averagePriceMatched": price if matched else 0.0}]}


class FaultBroker(SimulationBroker):
    """Trasporto SIM con esito pilotato per (mercato, selezione) o per mercato."""

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.modes, self.calls, self.placed, self.done = {}, [], [], []
        self.gate = threading.Event()
        self._lk = threading.Lock()

    def _mode(self, mid, sel):
        return self.modes.get((mid, sel), self.modes.get(mid, "ok"))

    def place_bet(self, **kw):
        mid = str(kw.get("market_id") or kw.get("marketId") or "")
        sel = int(kw.get("selection_id") or kw.get("selectionId") or 0)
        mode = self._mode(mid, sel)
        with self._lk:
            self.calls.append((mid, sel))
        try:
            if mode == "reject":
                return {"status": "FAILURE", "error": "INSUFFICIENT_FUNDS",
                        "instructionReports": [{"status": "FAILURE", "errorCode": "INSUFFICIENT_FUNDS"}]}
            if mode == "pre_send_timeout":
                raise TimeoutError("connect timeout")  # usata solo dove il task non parte comunque
            if mode == "lost":
                with self._lk:
                    self.placed.append((mid, sel))
                raise TimeoutError("response lost after send")
            if mode == "unknown":
                with self._lk:
                    self.placed.append((mid, sel))
                return {"order_unknown": True, "status": "TIMEOUT"}
            if mode == "slow":
                with self._lk:
                    self.placed.append((mid, sel))
                time.sleep(2.0)
                return live_ok(mid)
            if mode == "gate":
                self.gate.wait(3.0)
                with self._lk:
                    self.placed.append((mid, sel))
                return live_ok(mid)
            if mode == "boom":
                raise RuntimeError("client failure")
            with self._lk:
                self.placed.append((mid, sel))
            if mode == "live":
                return live_ok(mid)
            if mode == "partial":
                return live_ok(mid, "B-PART-1", matched=0.2)
            if mode == "unmatched":
                return live_ok(mid, "B-UNM-1", matched=0.0)
            if mode == "int_betid":
                r = live_ok(mid); r["instructionReports"][0]["betId"] = 424242; return r
            if mode == "nested_betid":
                return {"ok": True, "result": live_ok(mid, "B-NEST-1")}
            if mode == "no_betid":
                r = live_ok(mid); del r["instructionReports"][0]["betId"]; return r
            return super().place_bet(**kw)
        finally:
            with self._lk:
                self.done.append((mid, sel))


def _book(br, mid):
    br.update_market_book({"marketId": mid, "status": "OPEN", "runners": [
        {"selectionId": s, "status": "ACTIVE",
         "ex": {"availableToBack": [{"price": 3.0, "size": 500}], "availableToLay": [{"price": 3.1, "size": 500}]}}
        for s in (7, 8)]})


class Rig:
    def __init__(self, executor, db_path=None, markets=("1.9",), risk=True):
        self.db_path = db_path or os.path.join(tempfile.mkdtemp(prefix="bred_"), "p.db")
        self.db = Database(db_path=self.db_path)
        self.bus = EventBus()
        self.events, self.received = [], []
        orig = self.bus.publish

        def rec(ev, data=None):
            self.events.append((ev, data if isinstance(data, dict) else {}))
            return orig(ev, data)
        self.bus.publish = rec
        for ev in ("QUICK_BET_FAILED", "QUICK_BET_AMBIGUOUS", "QUICK_BET_SUCCESS"):
            self.bus.subscribe(ev, lambda d, ev=ev: self.received.append((ev, dict(d) if isinstance(d, dict) else {})))
        settings = SettingsService(self.db)
        self.bf = BetfairService(settings)
        self.broker = FaultBroker(starting_balance=1000.0)
        self.bf.simulation_broker = self.broker
        self.bf.set_simulation_mode(True)
        for m in markets:
            _book(self.broker, m)
        self.executor = executor
        self.engine = TradingEngine(bus=self.bus, db=self.db, client_getter=self.bf.get_client, executor=executor,
                                    safe_mode=get_safe_mode_manager(),
                                    risk_middleware=RiskGate(config=RoserpinaRiskLimits(settings)) if risk else None)

    # PF-MICROSTAKE-NATIVE-INTEGRATION-02: sotto 2 EUR l'engine instrada SEMPRE nel
    # Microstake nativo (saga sul broker reale), mai nel trasporto diretto che questa
    # suite pilota con risposte sintetiche (FaultBroker). Il contratto F001 C1..C12
    # riguarda il trasporto diretto: lo stake di default resta sopra il minimo exchange.
    def bet(self, mid="1.9", ref="pf-f001", stake=2.5, sel=7):
        p = {"market_id": mid, "selection_id": sel, "side": "BACK", "bet_type": "BACK", "stake": stake, "price": 3.0,
             "event_key": f"A{mid} v B:{sel}", "event_name": f"A{mid} v B", "customer_ref": ref, "simulation_mode": True}
        try:
            return self.engine.submit_quick_bet(p)
        except Exception as e:
            return {"status": "EXCEPTION", "error": repr(e)}

    def rows(self, ref):
        return [dict(r) for r in (self.db._execute("SELECT status, response_json FROM orders WHERE customer_ref = ?", (ref,),
                                                   fetch=True, commit=False) or [])]

    def row(self, ref):
        rows = self.rows(ref)
        assert len(rows) == 1, f"attesa una riga per {ref}, trovate {len(rows)}"
        return rows[0]["status"], rows[0]["response_json"] or ""

    def kind(self):
        ev = [e for e, _ in self.events]
        return "FAILED" if "QUICK_BET_FAILED" in ev else "AMBIGUOUS" if "QUICK_BET_AMBIGUOUS" in ev else "ACK"


def em(timeout=10.0, workers=2):
    return ExecutorManager(max_workers=workers, default_timeout=timeout)


def settle(sec=0.5):
    time.sleep(sec)


def outcome(rig, ref):
    s, resp = rig.row(ref)
    return s, rig.kind(), resp


def both(mode, ref, **kw):
    """Stesso scenario su percorso di produzione (ExecutorManager) e controllo diretto (executor=None)."""
    a = Rig(em()); a.broker.modes["1.9"] = mode; a.bet(ref=ref + "-ex", **kw); settle()
    b = Rig(None); b.broker.modes["1.9"] = mode; b.bet(ref=ref + "-dir", **kw); settle()
    return a, outcome(a, ref + "-ex"), b, outcome(b, ref + "-dir")


# =================================================================== B-RED-01
def test_B_RED_01_accepted_persists_real_response_parity():
    a, ex, b, dr = both("live", "r01")
    assert dr[:2] == ("SUBMITTED", "ACK") and json.loads(dr[2])["instructionReports"][0]["betId"] == "B-LIVE-1"
    assert ex[:2] == dr[:2]
    assert ex[2] not in ("", "{}"), "C1/C8: risposta reale persa: response_json='{}' (Future al posto del risultato)"


# =================================================================== B-RED-02
def test_B_RED_02_rejected_is_failed():
    a, ex, b, dr = both("reject", "r02")
    assert dr[:2] == ("FAILED", "FAILED")
    assert ex[:2] == ("FAILED", "FAILED"), f"C3: rifiuto del broker registrato {ex[:2]}"


# =================================================================== B-RED-03
def test_B_RED_03_wait_timeout_before_send_cancels_and_fails_without_client_call():
    ex = em(timeout=0.5, workers=1)
    blocker = ex.submit("blocker", time.sleep, 2.5)  # unico worker occupato: il task quick_bet resta PENDING
    rig = Rig(ex); rig.bet(ref="r03")
    status, kind, _ = outcome(rig, "r03")
    blocker.result(timeout=5); settle(1.0)
    assert rig.broker.calls == [], "C5: task non partito eseguito DOPO il timeout => ordine tardivo non governato"
    assert (status, kind) == ("FAILED", "FAILED"), f"C5: atteso FAILED pre-invio, ottenuto {status}/{kind}"


# =================================================================== B-RED-04
def test_B_RED_04_wait_timeout_after_send_is_ambiguous():
    rig = Rig(em(timeout=0.5)); rig.broker.modes["1.9"] = "slow"; rig.bet(ref="r04")
    status, kind, _ = outcome(rig, "r04"); settle(2.5)
    assert rig.broker.placed == [("1.9", 7)]
    assert (status, kind) == ("AMBIGUOUS", "AMBIGUOUS"), f"C4: ordine inviato, esito {status}/{kind}"


# =================================================================== B-RED-05
def test_B_RED_05_response_lost_after_acceptance_is_ambiguous_parity():
    a, ex, b, dr = both("lost", "r05")
    assert dr[:2] == ("AMBIGUOUS", "AMBIGUOUS")
    assert ex[:2] == ("AMBIGUOUS", "AMBIGUOUS"), f"C2/C4: risposta persa registrata {ex[:2]}"


# =================================================================== B-RED-06
def test_B_RED_06_unresolved_future_is_never_reported_as_submitted():
    rig = Rig(em()); rig.broker.modes["1.9"] = "gate"
    res = rig.bet(ref="r06")
    status_at_return, _ = rig.row("r06")
    broker_done_at_return = list(rig.broker.done)
    rig.broker.gate.set(); settle(1.0)
    assert not (status_at_return == "SUBMITTED" and not broker_done_at_return), (
        f"C1: l'engine ha registrato SUBMITTED ({res.get('status')}) mentre la chiamata al broker non era conclusa")


# =================================================================== B-RED-07
def test_B_RED_07_future_with_exception_matches_direct_path():
    a, ex, b, dr = both("boom", "r07")
    assert dr[0] != "SUBMITTED"
    assert ex[:2] == dr[:2], f"C2: eccezione del client => executor {ex[:2]}, diretto {dr[:2]}"


class _CrashAfterStartExecutor:
    """Il task gira davvero (l'ordine parte), poi l'infrastruttura dell'executor si guasta:
    Future autentico con BrokenExecutor, non l'eccezione del callable."""
    default_timeout = 2.0

    def submit(self, name, fn, *a, **k):
        f = concurrent.futures.Future(); f.set_running_or_notify_cancel()
        try:
            fn(*a, **k)
        finally:
            f.set_exception(concurrent.futures.BrokenExecutor("worker crashed after start"))
        return f


def test_B_RED_07b_executor_infrastructure_failure_after_start_is_ambiguous():
    rig = Rig(_CrashAfterStartExecutor()); rig.broker.modes["1.9"] = "live"; rig.bet(ref="r07b"); settle()
    status, kind, _ = outcome(rig, "r07b")
    assert rig.broker.placed == [("1.9", 7)]
    assert (status, kind) == ("AMBIGUOUS", "AMBIGUOUS"), f"C7: ordine partito, executor guasto, esito {status}/{kind}"


# =================================================================== B-RED-08
class _CancelledFutureExecutor:
    """submit restituisce un concurrent.futures.Future autentico gia' CANCELLATO (task mai partito)."""
    default_timeout = 1.0

    def submit(self, name, fn, *a, **k):
        f = concurrent.futures.Future(); f.cancel(); return f


def test_B_RED_08_cancelled_future_is_failed_and_client_never_called():
    rig = Rig(_CancelledFutureExecutor()); rig.bet(ref="r08"); settle()
    status, kind, _ = outcome(rig, "r08")
    assert rig.broker.calls == []
    assert (status, kind) == ("FAILED", "FAILED"), f"C6: Future cancellato registrato {status}/{kind}"


def test_B_RED_08b_closed_executor_refuses_and_fails_without_order_guard():
    """Guardia (atteso PASS gia' oggi): submit su ExecutorManager chiuso solleva => FAILED, nessun ordine."""
    ex = em(); ex.shutdown(wait=True)
    rig = Rig(ex); rig.bet(ref="r08b"); settle()
    status, kind, _ = outcome(rig, "r08b")
    assert rig.broker.calls == []
    assert (status, kind) == ("FAILED", "FAILED")


# =================================================================== B-RED-09
def test_B_RED_09_unknown_result_is_ambiguous_and_never_resent():
    a, ex, b, dr = both("unknown", "r09")
    assert dr[:2] == ("AMBIGUOUS", "AMBIGUOUS")
    assert ex[:2] == ("AMBIGUOUS", "AMBIGUOUS"), f"C4: UNKNOWN registrato {ex[:2]}"
    a.bet(ref="r09-ex"); settle()
    assert len(a.broker.calls) == 1, "C9: un esito sconosciuto non autorizza un secondo invio"


# =================================================================== B-RED-10
def test_B_RED_10_partial_match_is_persisted_with_matched_size():
    a, ex, b, dr = both("partial", "r10")
    assert json.loads(dr[2])["instructionReports"][0]["sizeMatched"] == 0.2
    assert ex[2] not in ("", "{}") and json.loads(ex[2])["instructionReports"][0]["sizeMatched"] == 0.2, (
        "C8: matching parziale non ricostruibile dal ledger (response_json vuota)")


# =================================================================== B-RED-11
def test_B_RED_11_unmatched_order_keeps_bet_identity():
    a, ex, b, dr = both("unmatched", "r11")
    assert ex[0] == "SUBMITTED"
    assert ex[2] not in ("", "{}") and json.loads(ex[2])["instructionReports"][0]["betId"] == "B-UNM-1", (
        "C8: ordine unmatched senza betId persistito => TTL/cancel/cashout non possono indirizzarlo")


# =================================================================== B-RED-12
def _crash(path):
    d = tempfile.mkdtemp(prefix="bred12_"); db, marker = os.path.join(d, "c.db"), os.path.join(d, "sent")
    env = dict(os.environ, PYTHONPATH=NETGUARD)
    p = subprocess.run([PY, os.path.join(HERE, "child_crash.py"), TREE, db, marker, path], env=env,
                       capture_output=True, text=True, timeout=60)
    rows = [dict(r) for r in (Database(db_path=db)._execute("SELECT customer_ref, status, response_json FROM orders",
                                                            fetch=True, commit=False) or [])]
    return p.returncode, os.path.exists(marker), rows


def test_B_RED_12_crash_between_send_and_response_persistence_keeps_uncertainty():
    rc_d, sent_d, rows_d = _crash("direct")
    rc_e, sent_e, rows_e = _crash("exec")
    assert rc_d == 1 and sent_d and [r["status"] for r in rows_d] == ["INFLIGHT"]
    assert rc_e == 1 and sent_e, "il processo figlio deve morire DOPO l'invio"
    assert [r["status"] for r in rows_e] == ["INFLIGHT"], (
        f"C10: dopo il crash il ledger dice {[(r['status'], r['response_json']) for r in rows_e]}: "
        "l'incertezza (ordine inviato, esito ignoto) e' stata cancellata")


# =================================================================== B-RED-13
def test_B_RED_13_restart_reconciliation_identity():
    # shape {"ok": True, "result": {...}} = BetfairClient.place_bet LIVE: l'unica che _order_bet_id
    # riconosce gia' sul percorso diretto, cosi' il test isola F001 da F011.
    rig = Rig(em()); rig.broker.modes["1.9"] = "nested_betid"; rig.bet(ref="r13"); settle()
    getattr(rig.db, "close", lambda: None)()
    restarted = Database(db_path=rig.db_path)
    ids = [o["bet_id"] for o in restarted._live_bot_orders()]
    assert "B-NEST-1" in ids, f"C8: dopo il riavvio l'ordine piazzato non e' riconciliabile (bet_id {ids})"


# =================================================================== B-RED-14
def test_B_RED_14_two_concurrent_orders_same_market_get_own_outcome():
    rig = Rig(em(workers=4), risk=False)
    rig.broker.modes[("1.9", 7)] = "live"; rig.broker.modes[("1.9", 8)] = "reject"
    bar = threading.Barrier(2)
    ths = [threading.Thread(target=lambda s=s: (bar.wait(), rig.bet(ref=f"r14-{s}", sel=s))) for s in (7, 8)]
    [t.start() for t in ths]; [t.join() for t in ths]; settle(1.0)
    got = {s: rig.row(f"r14-{s}")[0] for s in (7, 8)}
    assert got == {7: "SUBMITTED", 8: "FAILED"}, f"C9: esiti sullo stesso mercato {got}"


# =================================================================== B-RED-15
def _app(scen, path):
    out = os.path.join(tempfile.mkdtemp(prefix="bredapp_"), "o.json")
    env = dict(os.environ, PYTHONPATH=NETGUARD)
    subprocess.run([PY, os.path.join(HERE, "child_runtime.py"), TREE, out, scen, path], env=env,
                   capture_output=True, text=True, timeout=180)
    r = json.load(open(out))
    assert "error" not in r, r.get("error")
    return r["steps"]


def test_B_RED_15_multi_table_mixed_outcomes_reserve_matches_truth():
    s = _app("multi", "exec")["after_orders"]
    by_mkt = {o["market_id"]: o["status"] for o in s["orders"]}
    assert by_mkt == {"1.711": "SUBMITTED", "1.712": "FAILED", "1.713": "AMBIGUOUS"}, f"C9: {by_mkt}"
    # oracolo: ok 30 (vivo) + ambiguo 30 (trattenuto finche' non riconciliato) + rifiutato 0
    assert s["total_exposure"] == 60.0, f"C11: esposizione dei tavoli {s['total_exposure']} != 60"


# =================================================================== B-RED-16
def test_B_RED_16_duplicate_same_request_single_broker_call():
    rig = Rig(em(workers=4)); rig.broker.modes["1.9"] = "live"
    bar = threading.Barrier(2)
    ths = [threading.Thread(target=lambda: (bar.wait(), rig.bet(ref="r16"))) for _ in range(2)]
    [t.start() for t in ths]; [t.join() for t in ths]; settle(1.0)
    assert len(rig.broker.calls) == 1, f"idempotenza: {len(rig.broker.calls)} chiamate al broker"
    vivi = [r for r in rig.rows("r16") if r["status"] != "DUPLICATE_BLOCKED"]
    assert len(vivi) == 1, f"idempotenza: righe d'ordine {rig.rows('r16')}"
    assert vivi[0]["response_json"] not in ("", "{}"), "C8: l'unico ordine piazzato non ha la risposta reale"


# =================================================================== B-RED-17
@pytest.mark.parametrize("mode,expected", [
    ("nested_betid", "B-NEST-1"),   # F001 puro (diretto: PASS)
    ("live", "B-LIVE-1"),           # F011: instructionReports top-level
    ("int_betid", "424242"),        # F011: identita' Betfair numerica canonizzata
])
def test_B_RED_17_betid_other_formats_recoverable(mode, expected):
    rig = Rig(em()); rig.broker.modes["1.9"] = mode; rig.bet(ref="r17"); settle()
    ids = [o["bet_id"] for o in rig.db._live_bot_orders()]
    assert expected in ids, f"C8: betId {mode} non recuperabile dal ledger ({ids})"


@pytest.mark.parametrize("path", ["exec", "direct"])
def test_B_RED_17d_success_without_betid_is_ambiguous(path):
    rig = Rig(em() if path == "exec" else None)
    rig.broker.modes["1.9"] = "no_betid"
    rig.bet(ref=f"r17d-{path}")
    settle()
    rows = rig.rows(f"r17d-{path}")
    assert [r["status"] for r in rows] == ["AMBIGUOUS"], rows
    assert rig.broker.calls == [("1.9", 7)], "il client deve essere stato chiamato una sola volta"
    assert any(e == "QUICK_BET_AMBIGUOUS" for e, _ in rig.received)


# =================================================================== B-RED-18
def test_B_RED_18_terminal_events_emitted_and_received_with_correlation():
    rig = Rig(em()); rig.broker.modes["1.9"] = "reject"; rig.bet(ref="r18"); settle(1.0)
    got = [d for e, d in rig.received if e == "QUICK_BET_FAILED"]
    assert got, "C7: nessun evento terminale ricevuto dai subscriber per un ordine rifiutato"
    assert got[0].get("customer_ref") == "r18"


def test_B_RED_18b_terminal_event_carries_table_id_for_release():
    s = _app("reject", "direct")["after_orders"]
    failed = [e for e in s["events"] if e["event"] == "QUICK_BET_FAILED"]
    assert failed, "controllo diretto: QUICK_BET_FAILED atteso"
    assert failed[0]["table_id"] is not None, "C7/C11: evento terminale senza table_id => il tavolo non puo' essere rilasciato (indipendente da F001)"


# =================================================================== B-RED-19
@pytest.mark.parametrize("path", ["exec", "direct"])
def test_B_RED_19_rejected_order_releases_reservation(path):
    s = _app("reject", path)["after_orders"]
    assert s["orders"][0]["status"] == "FAILED", f"{path}: ordine {s['orders'][0]['status']}"
    assert s["total_exposure"] == 0.0, f"{path}: riserva {s['total_exposure']} su ordine mai piazzato"


def test_B_RED_19b_accepted_order_reserves_stake_control():
    s = _app("ok", "exec")["after_orders"]
    assert s["total_exposure"] == 30.0 and len(s["broker_orders"]) == 1


# =================================================================== B-RED-20
def test_B_RED_20_cashout_and_settlement_never_act_on_rejected_order():
    c = _app("cashout", "exec")
    st = _app("settle", "exec")
    tables_after = c["after_cashout"]["tables"] + st["after_settle"]["tables"]
    assert not tables_after, f"C12: posizione inventata sopravvive a RISK_STOP/cashout e al settlement: {tables_after}"
    assert c["after_orders"]["orders"][0]["status"] == "FAILED"
