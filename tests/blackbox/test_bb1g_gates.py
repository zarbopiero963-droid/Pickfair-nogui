"""BB-1G (decisione owner PF-OWNER-BB1G-FULL-GO-20261110): doppio gate fail-closed con la scatola nera REALE.

Gate A: nessun runtime operativo parte (ne' riprende) senza una scatola nera installata, runtime (lock D3) e
scrivibile. Gate B: nessun invio finanziario dai punti d'invio reali (`BetfairClient`, `SimulationBroker`) senza la
registrazione prevista dal contratto BB-1 (ingresso durevole; uscita protettiva bufferizzabile; emergency stop CLI D3).

Il modulo sta in `tests/blackbox`: la fixture del runtime di test (`tests/conftest.py`) NON vale qui, ogni test parte
senza scatola nera e installa quella vera dove serve. Offline: SQLite reale su disco, nessuna rete, nessun ordine.
"""
import json
import sqlite3
import threading

import pytest

import headless_main
from core import blackbox as bb
from core import blackbox_runtime as rt
from database import Database
from simulation_broker import SimulationBroker

pytestmark = pytest.mark.real_blackbox


@pytest.fixture
def bbpath(tmp_path, monkeypatch):
    path = tmp_path / "bb" / "blackbox.sqlite"
    monkeypatch.setenv(bb.ENV_PATH, str(path))
    rt.shutdown("TEST_SETUP")
    yield path
    rt.shutdown("TEST_TEARDOWN")


def _events(path, type_=None):
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    rows = [dict(r) for r in conn.execute("SELECT * FROM events ORDER BY seq")]
    conn.close()
    return [r for r in rows if type_ is None or r["type"] == type_]


def _broker():
    b = SimulationBroker()
    b.connect() if hasattr(b, "connect") else None
    return b


def _place(b, **kw):
    args = dict(market_id="1.100", selection_id=7, side="BACK", price=2.0, size=5.0)
    args.update(kw)
    return b.place_bet(**args)


def _break_writes(monkeypatch, box):
    """Guasto del registro: ogni scrittura fallisce come un disco pieno."""
    def fail(rec):
        raise sqlite3.OperationalError("disk I/O error")
    monkeypatch.setattr(box, "_insert", fail)


# ------------------------------------------------------------------------------------------------- Gate B, SIM

def test_gate_b_refuses_a_new_order_without_a_blackbox_and_touches_nothing():
    rt.shutdown("TEST_SETUP")
    b = _broker()
    with pytest.raises(rt.GateRefused) as exc:
        _place(b)
    assert str(exc.value) == "BLACKBOX_UNAVAILABLE"
    assert b.get_current_orders(["1.100"]) == [] and not b.state.orders, "nessun ordine creato prima del gate"


def test_gate_b_lets_an_order_through_with_a_blackbox_and_records_it_first(bbpath):
    rt.install()
    b = _broker()
    res = _place(b)
    assert res and b.state.orders, res
    req = _events(bbpath, "ORDER_PLACE_REQUEST")
    assert len(req) == 1 and req[0]["phase"] == "ATTEMPT" and req[0]["market_id"] == "1.100"
    data = json.loads(req[0]["data"])
    assert data["protective"] is False and data["intent"] == "ENTRY"


def test_gate_b_refuses_an_entry_when_the_blackbox_cannot_write(bbpath, monkeypatch):
    box = rt.install()
    _break_writes(monkeypatch, box)
    b = _broker()
    with pytest.raises(rt.GateRefused):
        _place(b)
    assert not b.state.orders, "un ingresso non si bufferizza mai"


def test_gate_b_never_blocks_a_cancel_during_a_blackbox_outage(bbpath, monkeypatch):
    box = rt.install()
    b = _broker()
    _place(b, price=50.0)                       # ordine a riposo
    _break_writes(monkeypatch, box)
    out = b.cancel_orders(market_id="1.100")
    assert out is not None and box._buffer, "cancel eseguito, evento nel buffer critico"
    assert box._buffer[-1]["type"] == "ORDER_CANCEL_REQUEST"


def test_protective_exit_place_passes_during_an_outage_but_an_entry_does_not(bbpath, monkeypatch):
    box = rt.install()
    _break_writes(monkeypatch, box)
    b = _broker()
    with rt.protective_exit("CASHOUT_HEDGE"):
        assert _place(b)
    assert box._buffer[-1]["data"]["intent"] == "CASHOUT_HEDGE"
    with pytest.raises(rt.GateRefused):
        _place(b)


def test_protective_context_is_not_inherited_by_another_thread(bbpath, monkeypatch):
    box = rt.install()
    _break_writes(monkeypatch, box)
    b = _broker()
    errors = []

    def worker():
        try:
            _place(b)
        except rt.GateRefused as exc:
            errors.append(exc)

    with rt.protective_exit("CASHOUT_HEDGE"):
        t = threading.Thread(target=worker)
        t.start()
        t.join(5)
    assert len(errors) == 1, "un invio spostato in un altro thread vale come ingresso (fail-closed)"


def test_emergency_exit_allows_only_cancellations_without_a_blackbox():
    rt.shutdown("TEST_SETUP")
    b = _broker()
    with rt.emergency_exit():
        assert b.cancel_orders(market_id="1.100") is not None
        with pytest.raises(rt.GateRefused):
            _place(b)
    with pytest.raises(rt.GateRefused):
        b.cancel_orders(market_id="1.100")       # fuori dall'emergency stop CLI: nessuna cancellazione senza registro


def test_late_operations_after_shutdown_are_refused(bbpath):
    rt.install()
    b = _broker()
    _place(b, price=50.0)
    rt.shutdown("STOP")
    with pytest.raises(rt.GateRefused):
        _place(b)
    with pytest.raises(rt.GateRefused):
        b.cancel_orders(market_id="1.100")
    with pytest.raises(rt.GateRefused):
        b.replace_orders(market_id="1.100", bet_id="x", new_price=3.0)


def test_batch_is_gated_once_before_any_leg(bbpath, monkeypatch):
    box = rt.install()
    b = _broker()
    legs = [{"selection_id": 7, "side": "BACK", "price": 2.0, "size": 2.0},
            {"selection_id": 8, "side": "BACK", "price": 3.0, "size": 2.0}]
    _break_writes(monkeypatch, box)
    with pytest.raises(rt.GateRefused):
        b.place_orders(market_id="1.200", instructions=legs)
    assert not b.state.orders, "nessuna gamba prima del gate: mai un batch a meta'"
    monkeypatch.undo()
    monkeypatch.setenv(bb.ENV_PATH, str(bbpath))
    assert b.place_orders(market_id="1.200", instructions=legs)
    assert len(_events(bbpath, "ORDER_PLACE_REQUEST")) == 1


def test_a_non_runtime_blackbox_does_not_satisfy_the_gate(bbpath, monkeypatch):
    box = rt.install()
    monkeypatch.setattr(box, "is_runtime", lambda: False)
    with pytest.raises(rt.GateRefused):
        _place(_broker())


# ------------------------------------------------------------------------------------------------- Gate B, LIVE

@pytest.fixture
def live_client(monkeypatch):
    from betfair_client import BetfairClient
    c = BetfairClient(username="u", app_key="a", cert_pem="c.pem", key_pem="k.pem", session=object())
    sent = []
    monkeypatch.setattr(c, "_post_jsonrpc", lambda url, method, params, **kw: sent.append(method) or
                        {"status": "SUCCESS", "instructionReports": [{"status": "SUCCESS", "betId": "1",
                                                                       "sizeMatched": 0.0}]})
    return c, sent


@pytest.mark.parametrize("call", [
    lambda c: c.place_bet(market_id="1.1", selection_id=2, side="BACK", price=2.0, size=2.0),
    lambda c: c.cancel_orders(market_id="1.1", bet_ids=["9"]),
    lambda c: c.reduce_order(market_id="1.1", bet_id="9", size_reduction=1.0),
    lambda c: c.replace_orders(market_id="1.1", bet_id="9", new_price=3.0),
])
def test_live_client_sends_nothing_without_a_blackbox(live_client, call):
    rt.shutdown("TEST_SETUP")
    c, sent = live_client
    with pytest.raises(rt.GateRefused):
        call(c)
    assert sent == [], "nessuna richiesta Betfair senza scatola nera"


def test_live_client_records_before_sending(bbpath, live_client):
    rt.install()
    c, sent = live_client
    c.place_bet(market_id="1.1", selection_id=2, side="BACK", price=2.0, size=2.0)
    assert sent == ["SportsAPING/v1.0/placeOrders"]
    assert len(_events(bbpath, "ORDER_PLACE_REQUEST")) == 1


def test_live_client_still_validates_before_the_gate(live_client):
    rt.shutdown("TEST_SETUP")
    c, sent = live_client
    with pytest.raises(RuntimeError, match="INVALID_PRICE"):
        c.place_bet(market_id="1.1", selection_id=2, side="BACK", price=float("nan"), size=2.0)


def test_gate_refusal_is_a_certain_pre_send_rejection_never_ambiguous():
    from order_manager import ErrorClass, classify_error
    from core import microstake_live_port, microstake_sim_port
    import simulation_broker
    assert classify_error("BLACKBOX_UNAVAILABLE") == ErrorClass.PERMANENT
    assert "BLACKBOX_UNAVAILABLE" in microstake_live_port.PRE_SEND_REJECTIONS
    assert "BLACKBOX_UNAVAILABLE" in microstake_sim_port.PRE_SEND_REJECTIONS
    assert "BLACKBOX_UNAVAILABLE" in simulation_broker._PRE_SEND_REJECTIONS


def test_cashout_hedge_router_sends_inside_a_protective_exit(monkeypatch):
    from core.order_router import OrderRouter
    seen = []

    class Client:
        def place_bet(self, **kw):
            seen.append(getattr(rt._gate_ctx, "protective", None))
            return {"status": "SUCCESS", "instructionReports": [{"status": "SUCCESS", "betId": "h1",
                                                                  "sizeMatched": 2.0}]}

    class Service:
        def get_client(self):
            return Client()

    OrderRouter(Service()).place({"market_id": "1.1", "selection_id": 2, "bet_type": "LAY", "price": 2.0,
                                  "stake": 5.0})
    assert seen == ["CASHOUT_HEDGE"]
    assert getattr(rt._gate_ctx, "protective", None) is None, "il contesto non sopravvive all'invio"


# ------------------------------------------------------------------------------------------------- Gate A

def _app(tmp_path, monkeypatch):
    p = str(tmp_path / "pf.db")
    monkeypatch.setattr(headless_main, "Database", lambda: Database(p))
    app = headless_main.HeadlessApp()
    app.build(start_services=False)
    app.telegram_service.start = lambda *a, **k: {"started": False, "stub": True}
    app.telegram_service.stop = lambda *a, **k: None
    return app


def test_gate_a_refuses_runtime_start_without_a_blackbox_and_takes_no_lock(tmp_path, monkeypatch):
    rt.shutdown("TEST_SETUP")
    monkeypatch.chdir(tmp_path)
    app = _app(tmp_path, monkeypatch)
    res = app.runtime.start(execution_mode="SIMULATION")
    assert res["started"] is False and res["reason_code"] == "BLACKBOX_UNAVAILABLE", res
    assert app.runtime._acquire_runtime_instance_lock() == "", "nessun lock del runtime preso dal rifiuto"
    app.runtime._release_runtime_instance_lock_if_stopped()
    assert app.runtime.resume()["resumed"] is False


def test_gate_a_lets_the_runtime_start_with_a_blackbox(bbpath, tmp_path, monkeypatch):
    rt.install()
    monkeypatch.chdir(tmp_path)
    app = _app(tmp_path, monkeypatch)
    try:
        assert app.runtime.start(execution_mode="SIMULATION").get("started") is True
        starts = [e for e in _events(bbpath, "START") if e["reason_code"] == "BLACKBOX_GATE_A"]
        assert starts and starts[0]["component"] == "runtime_controller.start"
    finally:
        app.stop()


def test_gate_a_refuses_when_the_blackbox_cannot_write(bbpath, tmp_path, monkeypatch):
    box = rt.install()
    monkeypatch.chdir(tmp_path)
    app = _app(tmp_path, monkeypatch)
    _break_writes(monkeypatch, box)
    assert app.runtime.start(execution_mode="SIMULATION")["reason_code"] == "BLACKBOX_UNAVAILABLE"


def test_gui_entrypoint_refuses_to_start_and_builds_nothing_without_a_blackbox(tmp_path, monkeypatch):
    import mini_gui
    rt.shutdown("TEST_SETUP")
    blocker = tmp_path / "file"
    blocker.write_text("x")
    monkeypatch.setenv(bb.ENV_PATH, str(blocker / "sub" / "blackbox.sqlite"))       # registro impossibile
    built = []
    monkeypatch.setattr(mini_gui, "MiniPickfairGUI", lambda *a, **k: built.append(1))
    assert mini_gui.main() == 1 and built == []


def test_gui_entrypoint_installs_the_blackbox_first_and_closes_it(bbpath, monkeypatch):
    import mini_gui
    seen = []

    class FakeGui:
        def __init__(self, **kw):
            seen.append(rt.get() is not None)

        def mainloop(self):
            pass

        def destroy(self):
            pass

    monkeypatch.setattr(mini_gui, "MiniPickfairGUI", FakeGui)
    assert mini_gui.main() == 0
    assert seen == [True], "la scatola nera e' installata prima della GUI"
    assert rt.get() is None, "chiusa all'uscita: ogni invio tardivo trova il gate chiuso"
    types = [e["type"] for e in _events(bbpath)]
    assert types[0] == "PROCESS_START" and types[-1] == "SHUTDOWN"
