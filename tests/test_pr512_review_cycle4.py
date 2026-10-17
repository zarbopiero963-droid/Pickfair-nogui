"""PR #512, ciclo 4 del fix loop — rilievi Codex su cad07e1/21aab95 confermati sul head e corretti (RED prima della patch).

- 4304693618 (P1): esecutore Microstake in cache legato al broker/modalita' correnti; mai l'adapter SIM in LIVE;
- 4304759974: fondi non leggibili o risposta incompleta => cap transitorio non disponibile (nessuno stub);
- 4304693616: riserva transitoria rilasciata se lo store della saga fallisce prima dell'invio;
- 4304759970: cashout AMBIGUOUS conserva il matched noto;
- 4304759964: valori non finiti nella ladder => liquidita' ignota;
- 4304759979: perdita di recovery persistita non finita => stato illeggibile (fail-closed);
- 4304759984: percorsi non tracciati con spazi catturati correttamente nel code_patch;
- 4304693614 (mitigazione fail-closed): nessun nuovo hedge nativo sulla stessa posizione mentre una saga e' pendente.
"""
import json
import os
import subprocess
import sys
from decimal import Decimal as D
from types import SimpleNamespace

import pytest

from core import liquidity
from core.microstake_sim_port import SagaStoreError
from core.order_router import OrderRouter
from core.runtime_controller import RuntimeController
from core.table_manager import TableManager
from simulation_broker import SimulationBroker
from test_microstake_cashout_and_recovery import _Service, _router_payload
from test_microstake_engine_native import _engine, _orders, _payload, _runtime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools", "agent_checkpoint"))
import pfckpt  # noqa: E402


# ---------------------------------------------------------------- 4304693618: esecutore legato a broker e modalita'
def test_cached_sim_executor_is_rebuilt_when_the_simulation_broker_changes():
    engine, old_broker, _ = _engine()
    assert engine.submit_quick_bet(_payload("a", size=0.10, price=3.0))["ok"] is True
    stale = engine.microstake_executor
    new_broker = SimulationBroker(starting_balance=100.0)                     # stop/start: broker nuovo
    engine.simulation_broker = new_broker
    engine.client_getter = lambda: new_broker
    engine.runtime_controller = _runtime(new_broker)
    assert engine.submit_quick_bet(_payload("b", size=0.10, price=3.0))["ok"] is True
    assert engine.microstake_executor is not stale and engine.microstake_executor.port.broker is new_broker
    assert len(_orders(new_broker)) == 1 and len(_orders(old_broker)) == 1, "ordine finito sul broker stantio"


def test_cached_sim_executor_is_never_reused_in_live_mode():
    engine, broker, _ = _engine()
    assert engine.submit_quick_bet(_payload("sim", size=0.10, price=3.0))["ok"] is True
    engine.runtime_controller = _runtime(broker, mode="LIVE")
    res = engine.submit_quick_bet(_payload("live", size=0.10, price=3.0))
    assert res["ok"] is False, res                                           # LIVE_EXECUTION_BLOCKED a monte
    assert len(_orders(broker)) == 1, "in LIVE un ordine e' stato inviato al broker di simulazione"
    assert engine._bound_native_microstake_executor("LIVE") is None, "adapter SIM riusato in LIVE"
    assert engine.native_microstake_executor() is None
    assert engine.microstake_executor is not None and engine.microstake_executor.port.broker is broker


# ---------------------------------------------------------------- 4304759974: fondi non leggibili => nessuno stub
@pytest.mark.parametrize("funds", [{"ok": False, "available": 100.0}, {"ok": True}, None, "x", {"ok": True, "available": None}])
def test_unreadable_account_funds_make_the_transient_cap_unavailable(funds):
    engine, broker, _ = _engine(bankroll=100.0, open_exposure=0.0)        # headroom ampio: non deve bastare
    engine.runtime_controller.betfair_service.get_account_funds = lambda: funds
    res = engine.submit_quick_bet(_payload("nf", size=0.10, price=3.0))
    assert res["ok"] is False and "transient_cap_unavailable" in str(res["error"]), res
    assert _orders(broker) == []


# ---------------------------------------------------------------- 4304693616: riserva rilasciata su store fallito
def test_reservation_is_released_when_the_saga_store_fails_before_the_place():
    engine, broker, _ = _engine()
    engine._default_native_microstake_executor("SIMULATION")
    ex = engine.microstake_executor
    original_save = ex.store.save
    calls = {"n": 0}

    def failing_save(ref, **values):
        calls["n"] += 1
        if calls["n"] == 1:
            raise SagaStoreError("SAGA_STORE_CORRUPT:disk")
        return original_save(ref, **values)

    ex.store.save = failing_save
    res = ex.execute(_payload("st", size=0.10, price=3.0))
    assert res["status"] == "FAILURE" and res["pre_send"] is True, res
    assert ex.port._reservations == {}, "riserva transitoria rimasta dopo un fallimento pre-invio dello store"
    assert ex.execute(_payload("st2", size=0.10, price=3.0))["status"] == "SUCCESS"


# ---------------------------------------------------------------- 4304759970: matched conservato in AMBIGUOUS
def test_ambiguous_native_cashout_keeps_the_known_matched_amount():
    out = OrderRouter._normalize_native({"status": "AMBIGUOUS", "order_unknown": True, "matched": "0.40", "error": "stub_matched"}, "r")
    assert out["order_unknown"] is True and out["placed"] is False and out["matched"] == pytest.approx(0.40)
    out2 = OrderRouter._normalize_native({"status": "AMBIGUOUS", "order_unknown": True, "matched": "nan"}, "r")
    assert out2["order_unknown"] is True and out2["matched"] == 0.0


# ---------------------------------------------------------------- 4304759964: valori non finiti nella ladder
@pytest.mark.parametrize("level", [{"price": 3.0, "size": "Infinity"}, {"price": "nan", "size": 5.0}, {"price": float("inf"), "size": 1.0}])
def test_non_finite_ladder_values_mean_unknown_liquidity(level):
    book = {"status": "OPEN", "runners": [{"selectionId": 7, "status": "ACTIVE", "ex": {"availableToBack": [{"price": 2.0, "size": 5.0}, level]}}]}
    assert liquidity.executable_liquidity(book, 7, "BACK", 2.0) is None
    assert liquidity.single_entry_liquidity(book, 7, "BACK", 2.0) == (None, "LIQUIDITY_LADDER_UNAVAILABLE")


# ---------------------------------------------------------------- 4304759979: perdita di recovery non finita
@pytest.mark.parametrize("bad", [float("inf"), float("nan"), float("-inf")])
def test_non_finite_persisted_recovery_loss_is_rejected(bad):
    tm = TableManager(table_count=2)
    with pytest.raises(ValueError):
        tm.restore_recovery_state({1: bad})


def test_runtime_treats_non_finite_recovery_state_as_unreadable():
    rc = RuntimeController.__new__(RuntimeController)
    rc.table_manager = TableManager(table_count=2)
    rc.db = SimpleNamespace(get_settings=lambda: {rc._RECOVERY_KEY: '{"1": Infinity}'})
    assert rc._restore_recovery_tables() is False and rc._recovery_state_unreadable is True
    assert rc.table_manager.recovery_state() == {}


# ---------------------------------------------------------------- 4304759984: percorsi con spazi nel code_patch
def _git(repo, *args):
    env = dict(os.environ, GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@t", GIT_COMMITTER_NAME="t", GIT_COMMITTER_EMAIL="t@t")
    return subprocess.run(["git", "-C", repo, *args], check=True, capture_output=True, text=True, env=env).stdout


def test_untracked_paths_with_whitespace_are_captured_in_the_code_patch(tmp_path):
    code = tmp_path / "code"; code.mkdir()
    _git(str(code), "init", "-q"); (code / "a.py").write_text("x = 1\n")
    _git(str(code), "add", "-A"); _git(str(code), "commit", "-qm", "c")
    (code / "space name.py").write_text("y = 2\n")
    state = tmp_path / "state"; (state / "ck").mkdir(parents=True)
    meta = pfckpt.capture_code_patch(str(code), str(state), "ck")
    assert "space name.py" in meta["untracked"], meta
    assert os.path.isfile(state / "ck" / pfckpt.CODE_PATCH_DIR / "untracked" / "space name.py")


# ---------------------------------------------------------------- 4304693614: nessun nuovo hedge sulla stessa posizione pendente
def test_router_refuses_a_second_native_hedge_while_a_saga_on_the_same_position_is_pending():
    engine, broker, _ = _engine()
    engine._default_native_microstake_executor("SIMULATION")
    engine.microstake_executor.port.inject_fault("LOST_CANCEL_RESPONSE")
    router = OrderRouter(_Service(broker), microstake_executor_provider=engine.native_microstake_executor)
    first = router.place(_router_payload(0.50))
    assert first["order_unknown"] is True and first["status"] == "AMBIGUOUS"
    places_before = [op for op in engine.microstake_executor.port.operation_log if op[0] == "PLACE"]
    second = router.place(_router_payload(0.50))                               # stessa posizione, nuovo ref
    assert second["placed"] is False and second["order_unknown"] is False
    assert second["error"] == "MICROSTAKE_PENDING_SAME_POSITION", second
    assert [op for op in engine.microstake_executor.port.operation_log if op[0] == "PLACE"] == places_before
    other = router.place(_router_payload(0.50, side="BACK"))                    # lato diverso: posizione diversa
    assert other["error"] != "MICROSTAKE_PENDING_SAME_POSITION"
