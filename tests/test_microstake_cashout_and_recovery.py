"""PF-MICROSTAKE-NATIVE-INTEGRATION-02, lotti M4 (cashout) e M5 (recovery all'avvio).

- `OrderRouter.place` sotto il minimo exchange non chiama MAI il broker diretto: Microstake nativo o fail-closed;
- la catena cashout (stesso wiring di headless e GUI) esegue un hedge da 0.50 EUR come green-up reale sul
  SimulationBroker, con esito AMBIGUOUS mai mascherato;
- `TradingEngine.recover_native_microstake_sagas` e `RuntimeController._recover_native_microstake`: saga pendente
  completata dalla sua identita' persistita, store illeggibile / esecutore assente => non ok => avvio in PAUSED.
"""
from decimal import Decimal
from types import SimpleNamespace

import pytest

import cashout_wiring
from core.order_router import OrderRouter
from core.runtime_controller import RuntimeController
from core.system_state import RuntimeMode
from simulation_broker import SimulationBroker
from test_microstake_engine_native import _DB, _book, _engine, _orders, _payload

D = Decimal
CMD_EXECUTE_CASHOUT = "CMD_EXECUTE_CASHOUT"


class _Bus:
    def __init__(self):
        self._subscribers = {}
        self.events = []

    def subscribe(self, topic, handler):
        self._subscribers.setdefault(topic, []).append(handler)

    def publish(self, topic, payload=None):
        self.events.append((topic, dict(payload) if isinstance(payload, dict) else payload))
        for h in list(self._subscribers.get(topic, [])):
            h(payload)

    def payloads(self, topic):
        return [p for t, p in self.events if t == topic]


class _Service:
    def __init__(self, broker):
        self.simulation_broker = broker

    def get_client(self):
        return self.simulation_broker

    def is_simulation_mode(self):
        return True

    def get_simulation_broker(self):
        return self.simulation_broker

    def get_live_client(self):
        return None


def _router_payload(stake, *, price=3.0, side="LAY", ref="telegram"):
    return {"market_id": "1.100", "selection_id": 7, "bet_type": side, "price": price, "stake": stake,
            "customer_ref": ref, "event_key": "", "event_name": "", "market_name": "", "runner_name": ""}


# ------------------------------------------------------------------ OrderRouter
def test_router_below_minimum_without_executor_fails_closed_and_never_calls_the_broker():
    engine, broker, _ = _engine()
    router = OrderRouter(_Service(broker))
    res = router.place(_router_payload(0.50))
    assert res["placed"] is False and res["ok"] is False and res["error"] == "MICROSTAKE_EXECUTOR_UNAVAILABLE"
    assert _orders(broker) == []


def test_router_below_minimum_uses_the_engine_native_executor_and_reports_the_new_identity():
    engine, broker, db = _engine()
    router = OrderRouter(_Service(broker), microstake_executor_provider=engine.native_microstake_executor)
    _book(broker, "1.100", 7, lay=(3.0, 50.0))                                 # il LAY a 3.0 si abbina subito
    res = router.place(_router_payload(0.50, side="LAY", price=3.0))
    assert res["placed"] is True and res["ok"] is True and res["matched"] == pytest.approx(0.50), res
    assert res["bet_id"] and res["raw"]["microstake_ref"].startswith("telegram-ms-")
    live = [o for o in _orders(broker) if o["status"] != "CANCELLED"]
    assert len(live) == 1 and live[0]["betId"] == res["bet_id"] and live[0]["sizeMatched"] == pytest.approx(0.50)
    assert all(o["sizeMatched"] == 0.0 for o in _orders(broker) if o["status"] == "CANCELLED"), "stub mai abbinato"


def test_router_at_or_above_minimum_stays_direct():
    engine, broker, _ = _engine()
    router = OrderRouter(_Service(broker), microstake_executor_provider=engine.native_microstake_executor)
    res = router.place(_router_payload(2.0))
    assert res["placed"] is True and engine.microstake_executor is None


@pytest.mark.parametrize("stake", [0, -1, float("nan"), float("inf"), "abc", None])
# NB: uno stake booleano (True) NON e' qui: il SimulationBroker preesistente lo accetta come 1.0 (finding MS-NI-007).
def test_router_non_routable_stakes_are_left_to_the_broker_validation(stake):
    engine, broker, _ = _engine()
    router = OrderRouter(_Service(broker), microstake_executor_provider=engine.native_microstake_executor)
    try:
        res = router.place(_router_payload(stake))
    except RuntimeError:
        res = {"placed": False}
    assert res["placed"] is False and _orders(broker) == [] and engine.microstake_executor is None


def test_router_native_ambiguous_is_order_unknown_never_placed():
    engine, broker, _ = _engine()
    engine._default_native_microstake_executor("SIMULATION")
    engine.microstake_executor.port.inject_fault("LOST_CANCEL_RESPONSE")
    router = OrderRouter(_Service(broker), microstake_executor_provider=engine.native_microstake_executor)
    res = router.place(_router_payload(0.50))
    assert res["order_unknown"] is True and res["status"] == "AMBIGUOUS" and res["placed"] is False


def test_router_native_cap_block_is_a_plain_failure_with_reason():
    engine, broker, _ = _engine(available=0.99, bankroll=0.0)                # fondi insufficienti per lo stub BACK da 1
    router = OrderRouter(_Service(broker), microstake_executor_provider=engine.native_microstake_executor)
    res = router.place(_router_payload(0.50, side="BACK"))
    assert res["placed"] is False and res["order_unknown"] is False and res["error"] == "transient_exposure_cap"
    assert _orders(broker) == []


# ------------------------------------------------------------------ catena cashout reale (stesso wiring di headless/GUI)
def _cashout_cmd(stake, side="LAY", price=3.0):
    return {"market_id": "1.100", "selection_id": 7, "side": side, "price": price, "stake": stake, "green_up": 0.1,
            "source": "telegram", "position_id": "p1"}


def test_cashout_chain_executes_a_sub_minimum_hedge_through_native_microstake():
    engine, broker, _ = _engine()
    bus = _Bus()
    catena = cashout_wiring.cabla_catena_cashout(
        bus=bus, betfair_service=_Service(broker), persist=lambda r: None,
        notify=lambda text, *, severity="HIGH": None,
        microstake_executor_provider=engine.native_microstake_executor)
    _book(broker, "1.100", 7, lay=(3.0, 50.0))
    bus.publish(CMD_EXECUTE_CASHOUT, _cashout_cmd(0.50))
    assert bus.payloads("CASHOUT_SUCCESS"), bus.events
    assert not bus.payloads("CASHOUT_FAILED")
    assert catena.order_router._microstake_executor_provider == engine.native_microstake_executor


def test_cashout_chain_without_native_executor_fails_closed_instead_of_sending_a_sub_minimum_hedge():
    engine, broker, _ = _engine()
    bus = _Bus()
    cashout_wiring.cabla_catena_cashout(bus=bus, betfair_service=_Service(broker), persist=lambda r: None,
                                        notify=lambda text, *, severity="HIGH": None)
    bus.publish(CMD_EXECUTE_CASHOUT, _cashout_cmd(0.50))
    failed = bus.payloads("CASHOUT_FAILED")
    assert failed and "MICROSTAKE_EXECUTOR_UNAVAILABLE" in str(failed[0])
    assert _orders(broker) == []


# ------------------------------------------------------------------ recovery all'avvio (engine + runtime)
def _pending_after_crash(engine, broker):
    engine._default_native_microstake_executor("SIMULATION")
    port = engine.microstake_executor.port
    port.inject_fault("LOST_PLACE_RESPONSE")
    port.hide_reconciliation = True
    assert engine.submit_quick_bet(_payload("crash", size=0.10, price=3.0))["status"] == "AMBIGUOUS"
    assert _orders(broker)[0]["sizeRemaining"] == pytest.approx(1.0)                 # stub intero (minimo exchange)


def test_engine_recovery_completes_pending_saga_without_new_place():
    engine, broker, db = _engine()
    _pending_after_crash(engine, broker)
    restarted, _, _ = _engine(db=db)
    restarted.simulation_broker = broker; restarted.client_getter = lambda: broker
    restarted.runtime_controller.betfair_service.simulation_broker = broker
    out = restarted.recover_native_microstake_sagas()
    assert out["ok"] is True and out["results"] == {"crash": "ACTIVE"}, out
    assert _orders(broker)[0]["sizeRemaining"] == pytest.approx(0.10)
    assert [op[0] for op in restarted.microstake_executor.port.operation_log] == ["CANCEL"]


def test_engine_recovery_nothing_pending_is_ok_and_builds_no_executor():
    engine, _, _ = _engine()
    out = engine.recover_native_microstake_sagas()
    assert out == {"ok": True, "status": "NOTHING_PENDING", "pending": [], "results": {}}
    assert engine.microstake_executor is None


def test_engine_recovery_store_unreadable_is_not_ok():
    engine, _, db = _engine()
    db.settings["microstake.sagas"] = "{rotto"
    out = engine.recover_native_microstake_sagas()
    assert out["ok"] is False and out["status"] == "STORE_UNREADABLE"


def test_engine_recovery_in_live_mode_keeps_a_pending_sim_saga_not_ok():
    # RECTIFICATION-01: store separati per modalita'. In LIVE una saga SIM pendente non si riprende (porta diversa)
    # ma blocca comunque: nessuna saga resta dimenticata, ok False finche' una sessione SIM non la riconcilia.
    engine, broker, db = _engine()
    _pending_after_crash(engine, broker)
    live, _, _ = _engine(db=db, mode="LIVE")
    out = live.recover_native_microstake_sagas()
    assert out["ok"] is False and out["status"] == "SIM_SAGAS_PENDING" and out["pending"] == ["crash"]


def _rc(engine):
    rc = RuntimeController.__new__(RuntimeController)
    rc.trading_engine = engine
    return rc


def test_runtime_recovery_helper_is_fail_closed_on_error_or_malformed_result():
    assert _rc(SimpleNamespace())._recover_native_microstake()["ok"] is True          # engine senza Microstake: nulla da fare
    boom = SimpleNamespace(recover_native_microstake_sagas=lambda: (_ for _ in ()).throw(RuntimeError("x")))
    assert _rc(boom)._recover_native_microstake()["ok"] is False
    bad = SimpleNamespace(recover_native_microstake_sagas=lambda: "ok")
    assert _rc(bad)._recover_native_microstake()["ok"] is False
    good = SimpleNamespace(recover_native_microstake_sagas=lambda: {"ok": True, "status": "RECOVERED", "pending": []})
    assert _rc(good)._recover_native_microstake()["ok"] is True


def test_runtime_start_sequence_pauses_when_microstake_saga_stays_pending(monkeypatch):
    """Il blocco di start (F018) con recovery non ok deve portare a PAUSED e RECONCILIATION_REQUIRED."""
    import inspect
    import textwrap
    src = inspect.getsource(RuntimeController)
    i = src.rfind("\n", 0, src.index("self._restore_recovery_tables()")) + 1
    block = textwrap.dedent(src[i: src.index("# PR3: ciclo chiusura reale", i)])
    assert "self._recover_native_microstake()" in block and "MICROSTAKE_SAGA_PENDING" in block
    assert block.index("self._recover_native_microstake()") < block.index("self._rebuild_open_positions()")

    rc = RuntimeController.__new__(RuntimeController)
    rc.trading_engine = SimpleNamespace(recover_native_microstake_sagas=lambda: {"ok": False, "status": "PENDING", "pending": ["crash"]})
    rc.bus = _Bus()
    rc._restore_recovery_tables = lambda: None
    rc._rebuild_open_positions = lambda: {"ok": True, "reason": ""}
    rc.mode = RuntimeMode.STOPPED if hasattr(RuntimeMode, "STOPPED") else RuntimeMode.PAUSED
    rc.last_error = ""
    ns = {"self": rc, "RuntimeMode": RuntimeMode, "logger": __import__("logging").getLogger("t")}
    exec(compile(block, "<start-block>", "exec"), ns)
    assert rc.mode is RuntimeMode.PAUSED and rc._reconciliation_required is True
    assert rc.last_error.startswith("RECONCILIATION_REQUIRED:MICROSTAKE_SAGA_PENDING:PENDING:crash")
