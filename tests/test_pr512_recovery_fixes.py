"""PF-PR-RECOVERY-DECISIONS-02 (owner 17/10) — lotto unico dei sei finding triagiati sul head 339cd24 di PR #512.
Ogni test era RED prima della patch.

- Codex 4303374299: resume() ritenta anche la recovery delle saghe Microstake, non solo F018;
- Codex 4303374312: stake con piu' di due decimali rifiutato PRIMA dell'invio; residuo del broker verificato
  uguale allo stake richiesto (importo eseguito = importo richiesto, rettifica owner 17/10);
- Codex 4303374328: ladder del book malformata => liquidita' ignota (RISK_LIQUIDITY_UNKNOWN), mai un'eccezione
  dopo l'acquisizione di tavolo e chiave dedupe;
- Codex 4303374317: lo scan segreti del checkpoint copre anche i sorgenti Python catturati;
- Codex 4303374326: uno SHA di evidenza deve risolversi a un commit del repository;
- Grok (c.6236399723): snapshot SIM salvato sotto lock e fallimento dichiarato nel risultato; `pre_send`
  calcolato per customer_ref e non dalla lunghezza globale del log operazioni.
"""
import json
import os
import subprocess
import sys
import threading
from decimal import Decimal as D
from types import SimpleNamespace

import pytest

from core import liquidity
from core.runtime_controller import RuntimeController, RuntimeMode
from simulation_broker import SimulationBroker
from test_microstake_engine_native import _book, _engine, _orders, _payload
from test_microstake_sim_port import _Db as _SagaDb

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# ---------------------------------------------------------------- 4303374299: resume() e saghe pendenti
class _Engine:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = 0

    def recover_native_microstake_sagas(self):
        self.calls += 1
        return self.outcomes.pop(0) if self.outcomes else {"ok": True, "status": "RECONCILED", "pending": []}


def _paused_controller(engine):
    rc = RuntimeController.__new__(RuntimeController)
    rc.mode = RuntimeMode.PAUSED
    rc.trading_engine = engine
    rc.bus = SimpleNamespace(publish=lambda *a, **k: None)
    rc._reconciliation_required = True
    rc._monitor_daily_loss_breach = lambda **k: {"breached": False}
    rc._loss_block_reason = lambda: ""
    rc._rebuild_open_positions = lambda: {"ok": True, "reason": "", "rebuilt": [], "uncertain": [], "held": []}
    rc.get_status = lambda: {"mode": rc.mode.value}
    rc._safe_status_snapshot = lambda: {"mode": rc.mode.value}
    return rc


def test_resume_keeps_the_runtime_paused_while_a_microstake_saga_is_still_pending():
    engine = _Engine([{"ok": False, "status": "PENDING", "pending": ["ms-1"]}])
    rc = _paused_controller(engine)
    out = rc.resume()
    assert out["resumed"] is False and "MICROSTAKE_SAGA_PENDING" in out["reason"], out
    assert rc.mode is RuntimeMode.PAUSED and rc._reconciliation_required is True
    assert engine.calls == 1, "la recovery Microstake non e' stata ritentata dal resume"


def test_resume_becomes_active_only_when_both_recovery_paths_succeed():
    engine = _Engine([{"ok": False, "status": "PENDING", "pending": ["ms-1"]}, {"ok": True, "status": "RECONCILED", "pending": []}])
    rc = _paused_controller(engine)
    assert rc.resume()["resumed"] is False
    out = rc.resume()
    assert out["resumed"] is True and rc.mode is RuntimeMode.ACTIVE and rc._reconciliation_required is False


# ---------------------------------------------------------------- 4303374312: precisione e residuo
# PF-STAKE-PRECISION-MICROSTAKE-FINAL-01 (owner 24/10): l'engine non rifiuta piu' uno stake con piu' di due
# decimali, lo tronca al centesimo (mai verso l'alto) PRIMA della scelta del percorso. L'importo non rappresentabile
# continua a non raggiungere mai l'exchange; il blocco del coordinatore resta come difesa (test sotto).
@pytest.mark.parametrize("size,floored", [(0.105, 0.10), (0.111, 0.11), (1.999, 1.99)])
def test_stake_with_more_than_two_decimals_is_floored_never_sent_raw(size, floored):
    engine, broker, _ = _engine()
    res = engine.submit_quick_bet(_payload("prec", size=size, price=3.0))
    assert res["ok"] is True, res
    assert [o["sizeRemaining"] for o in _orders(broker)] == [pytest.approx(floored, abs=1e-12)], \
        "un importo non rappresentabile in centesimi e' stato inviato"


@pytest.mark.parametrize("size", [0.105, 0.111, 0.999])                 # sotto il minimo exchange (1 EUR)
def test_coordinator_still_rejects_more_than_two_decimals_before_any_exchange_operation(size):
    engine, broker, _ = _engine()
    engine._default_native_microstake_executor("SIMULATION")
    res = engine.microstake_executor.execute(_payload("prec-direct", size=size, price=3.0))
    assert "invalid_size_precision" in str(res), res
    assert _orders(broker) == [], "difesa del coordinatore aggirata"


def test_two_decimal_stake_still_executes_exactly():
    engine, broker, _ = _engine()
    assert engine.submit_quick_bet(_payload("ok", size=0.15, price=3.0))["ok"] is True
    assert _orders(broker)[0]["sizeRemaining"] == pytest.approx(0.15)


def test_residual_different_from_the_requested_stake_is_ambiguous_never_active():
    engine, broker, _ = _engine()
    engine._default_native_microstake_executor("SIMULATION")
    port = engine.microstake_executor.port
    original = port.cancel_reduction

    def wrong_reduction(bet_id, reduction):
        return original(bet_id, reduction - D("0.01"))          # il broker lascia 0.11 invece di 0.10

    port.cancel_reduction = wrong_reduction
    res = engine.microstake_executor.execute(_payload("res", size=0.10, price=3.0))
    assert res["status"] == "AMBIGUOUS" and res["error"] == "unexpected_residual_size", res


# ---------------------------------------------------------------- 4303374328: ladder malformata
@pytest.mark.parametrize("ladder", [1, "x", {"price": 2.0}, 3.5, True])
def test_malformed_ladder_means_unknown_liquidity_not_an_exception(ladder):
    book = {"status": "OPEN", "runners": [{"selectionId": 7, "status": "ACTIVE", "ex": {"availableToBack": ladder}}]}
    assert liquidity.executable_liquidity(book, 7, "BACK", 2.0) is None
    assert liquidity.single_entry_liquidity(book, 7, "BACK", 2.0) == (None, "LIQUIDITY_LADDER_UNAVAILABLE")
    assert liquidity.sum_executable_liquidity(ladder, "BACK", 2.0) == 0.0


def test_well_formed_ladder_still_sums_executable_levels():
    ladder = [{"price": 2.0, "size": 5.0}, {"price": 2.2, "size": 3.0}, {"price": 1.9, "size": 100.0}]
    assert liquidity.sum_executable_liquidity(ladder, "BACK", 2.0) == pytest.approx(8.0)


# ---------------------------------------------------------------- 4303374326: SHA di evidenza risolto
sys.path.insert(0, os.path.join(ROOT, "tools", "traceability"))
import pf_trace  # noqa: E402


def _git_repo(tmp_path):
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test_x.py").write_text("def test_ok():\n    assert True\n")
    env = dict(os.environ, GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@t", GIT_COMMITTER_NAME="t", GIT_COMMITTER_EMAIL="t@t")
    subprocess.run(["git", "-C", str(tmp_path), "add", "-A"], check=True, env=env)
    subprocess.run(["git", "-C", str(tmp_path), "commit", "-qm", "c"], check=True, env=env)
    return subprocess.run(["git", "-C", str(tmp_path), "rev-parse", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()


def _req(sha):
    return {"schema": "pf-traceability/1", "requirements": [{
        "id": "R-1", "text": "t", "source": "s", "owner_decision": "D", "activity": "ACTIVE", "status": "TESTED",
        "issue": ["#1"], "component": ["x.py"], "code": ["x.py::f"], "oracle": "o", "tests": ["tests/test_x.py::test_ok"],
        "evidence": [{"sha": sha, "command": "pytest", "environment": "venv", "timestamp": "2026-10-17T00:00:00Z", "result": "PASS"}],
        "findings": [], "reviewer": None, "gate": "A", "limitations": []}]}


def test_fabricated_evidence_sha_is_rejected_and_a_real_one_is_accepted(tmp_path):
    sha = _git_repo(tmp_path)
    assert pf_trace.validate(_req(sha[:7]), str(tmp_path))[0] == []
    errors, _ = pf_trace.validate(_req("abcdef1"), str(tmp_path))
    assert any("non risolto" in e for e in errors), errors


def test_evidence_sha_outside_a_git_repository_is_not_verifiable(tmp_path):
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test_x.py").write_text("def test_ok():\n    assert True\n")
    errors, _ = pf_trace.validate(_req("8ffb4f3"), str(tmp_path))
    assert any("non verificabile" in e for e in errors), errors


# ---------------------------------------------------------------- Grok: snapshot sotto lock e dichiarato
class _LockProbeDb(_SagaDb):
    def __init__(self, broker_ref):
        super().__init__()
        self.broker_ref = broker_ref
        self.lock_held = []

    def save_simulation_state(self, key, state):
        held = {}

        def probe():
            held["free"] = self.broker_ref["b"]._lock.acquire(blocking=False)
            if held["free"]:
                self.broker_ref["b"]._lock.release()
        t = threading.Thread(target=probe)
        t.start(); t.join()
        self.lock_held.append(not held["free"])
        super().save_simulation_state(key, state)


def test_sim_snapshot_is_written_while_the_broker_lock_is_held():
    ref = {}
    db = _LockProbeDb(ref)
    broker = SimulationBroker(starting_balance=100.0, db=db)
    ref["b"] = broker
    assert broker.persist_snapshot() is True
    assert db.lock_held == [True], "save_simulation_state eseguita fuori dal lock: lost update fra saghe concorrenti"


def test_snapshot_failure_is_declared_in_the_executor_result():
    engine, broker, _ = _engine()
    engine._default_native_microstake_executor("SIMULATION")
    port = engine.microstake_executor.port
    broker.persist_snapshot = lambda: False
    res = engine.microstake_executor.execute(_payload("snap", size=0.10, price=3.0))
    # ciclo 8 (Codex su 034af59): snapshot FINALE non persistito => mai SUCCESS/ACTIVE, saga AMBIGUOUS
    assert res["status"] == "AMBIGUOUS" and res["phase"] == "AMBIGUOUS" and res["snapshot_persisted"] is False, res
    assert port.snapshot_failures >= 1


def test_snapshot_success_is_declared_too():
    engine, broker, _ = _engine()
    engine._default_native_microstake_executor("SIMULATION")
    broker.persist_snapshot = lambda: True
    res = engine.microstake_executor.execute(_payload("snap2", size=0.10, price=3.0))
    assert res["status"] == "SUCCESS" and res["phase"] == "ACTIVE" and res["snapshot_persisted"] is True


# ---------------------------------------------------------------- Grok: pre_send per customer_ref
def test_pre_send_is_not_masked_by_a_concurrent_place_of_another_saga():
    engine, broker, _ = _engine()
    engine._default_native_microstake_executor("SIMULATION")
    ex = engine.microstake_executor
    port = ex.port
    original_quote = ex.quote_provider

    def quote_with_concurrent_place(payload):
        # un'altra saga invia il suo stub mentre questa e' ancora nel preflight
        port.operation_log.append(("PLACE", "B-other", "2.00", "3.00", "other-ref"))
        return {"error": "book_unreadable"}

    ex.quote_provider = quote_with_concurrent_place
    res = ex.execute(_payload("mine", size=0.10, price=3.0))
    ex.quote_provider = original_quote
    assert res["status"] == "FAILURE" and res["pre_send"] is True, res


def test_pre_send_is_false_once_this_saga_has_placed_its_stub():
    engine, broker, _ = _engine()
    engine._default_native_microstake_executor("SIMULATION")
    port = engine.microstake_executor.port
    original = port.broker.replace_orders
    port.broker.replace_orders = lambda **kw: {"status": "SUCCESS", "instructionReports": [{"status": "FAILURE", "betId": kw["bet_id"]}]}
    _book(broker, "1.100", 7, back=(3.0, 50.0))
    res = engine.microstake_executor.execute(_payload("placed", size=0.10, price=3.0))
    port.broker.replace_orders = original
    assert res["status"] == "FAILURE" and res["pre_send"] is False
