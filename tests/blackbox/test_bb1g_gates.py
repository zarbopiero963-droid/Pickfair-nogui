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
    box.flush_protective()                      # il writer dedicato tenta la scrittura durante il guasto
    assert out is not None and box._buffer, "cancel eseguito, evento nel buffer critico"
    assert box._buffer[-1]["type"] == "ORDER_CANCEL_REQUEST"


def test_protective_exit_place_passes_during_an_outage_but_an_entry_does_not(bbpath, monkeypatch):
    box = rt.install()
    _break_writes(monkeypatch, box)
    b = _broker()
    with rt.protective_exit("CASHOUT_HEDGE"):
        assert _place(b)
    box.flush_protective()
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
    # Credenziali finte (nessun segreto). Costruite con zip, non come `chiave=valore`: la redazione dei reviewer AI
    # oscura ogni assegnazione a un nome simile a una chiave, anche fittizio, e Sol non poteva leggere la riga (#630).
    fake = dict(zip(("username", "app_key", "cert_pem", "key_pem"), ("u", "a", "c.pem", "k.pem")))
    c = BetfairClient(**fake, session=object())
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


# ------------------------------------------------------------------------------- uscite protettive mai in attesa
# Codex su #630 (6be44640): un'uscita protettiva non deve attendere il registro (lock del processo, BEGIN IMMEDIATE con
# busy timeout di 30 s, scarico del backlog). PF-EXEC-03: la riduzione del rischio non si ritarda mai.

def _install_runtime_box():
    rt.install(entrypoint="test_bb1g")
    box = rt._active_runtime_box()
    assert box is not None
    return box


def test_protective_cancel_does_not_wait_for_a_journal_locked_by_another_writer(bbpath):
    import time as _time
    box = _install_runtime_box()
    other = sqlite3.connect(str(bbpath), timeout=0, isolation_level=None)
    other.execute("BEGIN IMMEDIATE")                 # un altro processo (lettore Control API/MCP) sta scrivendo
    try:
        t0 = _time.monotonic()
        event_id = rt.financial_gate("ORDER_CANCEL_REQUEST", component="betfair_client", cancel=True,
                                     market_id="1.5", bet_ids=["B1"])
        elapsed = _time.monotonic() - t0
    finally:
        other.execute("ROLLBACK")
        other.close()
    assert event_id and elapsed < 1.0, f"il cancel protettivo ha atteso il registro per {elapsed:.2f} s"
    rt.record("SESSION_CHECKPOINT", source="SYSTEM", component="test", phase="OBSERVATION", outcome="VERIFIED")
    assert [r for r in _events(bbpath, "ORDER_CANCEL_REQUEST") if r["event_id"] == event_id], \
        "l'evento protettivo e' scritto dal writer dedicato, dopo l'invio"


def test_protective_hedge_does_not_wait_for_the_journal_lock_held_by_another_thread(bbpath):
    import time as _time
    box = _install_runtime_box()
    held, release = threading.Event(), threading.Event()

    def hold():
        with box._lock:
            held.set()
            release.wait(10)

    t = threading.Thread(target=hold, daemon=True)
    t.start()
    held.wait(5)
    try:
        t0 = _time.monotonic()
        with rt.protective_exit("CASHOUT_HEDGE"):
            event_id = rt.financial_gate("ORDER_PLACE_REQUEST", component="simulation_broker", market_id="1.5",
                                         selection_id=7, side="LAY", price=2.0, size=5.0)
        elapsed = _time.monotonic() - t0
    finally:
        release.set()
        t.join(5)
    assert event_id and elapsed < 1.0, f"l'hedge protettivo ha atteso il lock per {elapsed:.2f} s"
    rt.record("SESSION_CHECKPOINT", source="SYSTEM", component="test", phase="OBSERVATION", outcome="VERIFIED")
    assert [r for r in _events(bbpath, "ORDER_PLACE_REQUEST") if r["event_id"] == event_id]


def test_an_entry_still_requires_a_durable_write_while_the_journal_is_busy(bbpath, monkeypatch):
    box = _install_runtime_box()
    _break_writes(monkeypatch, box)
    with pytest.raises(rt.GateRefused):
        rt.financial_gate("ORDER_PLACE_REQUEST", component="betfair_client", market_id="1.5", selection_id=7,
                          side="BACK", price=2.0, size=5.0)


# --------------------------------------------------------------------- rifiuto del gate sull'hedge del cashout

def test_cashout_hedge_refused_by_the_gate_is_a_failed_hedge_never_ambiguous():
    """Codex su #630: un `GateRefused` prova che nulla e' partito. Il router lo restituisce come hedge non piazzato
    (placed=False, order_unknown=False, BLACKBOX_UNAVAILABLE), mai come eccezione che il cashout registrerebbe
    HEDGE_AMBIGUOUS bloccando i cashout successivi."""
    from cashout_executor import CashoutExecutor
    from core import hedge_ref as _hedge
    from core.order_router import OrderRouter

    class Client:
        def place_bet(self, **kw):
            raise rt.GateRefused("simulation_broker: ORDER_PLACE_REQUEST senza scatola nera")

    class Service:
        def get_client(self):
            return Client()

    res = OrderRouter(Service()).place({"market_id": "1.1", "selection_id": 2, "bet_type": "LAY", "price": 2.0,
                                        "stake": 5.0})
    assert res["placed"] is False and res["order_unknown"] is False, res
    assert res["error"] == rt.GATE_REFUSED_CODE
    assert CashoutExecutor._ledger_status(None, res) == _hedge.HEDGE_FAILED


def test_protective_exit_does_not_wait_for_a_stalled_disk(bbpath, monkeypatch):
    """Sol su #630 (7cda1f63): un busy timeout breve limita la contesa SQLite, non un disco in stallo su COMMIT o
    fsync. Il thread d'invio non deve fare I/O sul registro: l'evento lo scrive il writer dedicato."""
    import time as _time
    box = _install_runtime_box()
    real_insert = box._insert

    def stalled(rec):
        _time.sleep(2.0)                             # disco in stallo: ogni scrittura impiega 2 s
        return real_insert(rec)

    monkeypatch.setattr(box, "_insert", stalled)
    t0 = _time.monotonic()
    event_id = rt.financial_gate("ORDER_CANCEL_REQUEST", component="betfair_client", cancel=True,
                                 market_id="1.5", bet_ids=["B1"])
    elapsed = _time.monotonic() - t0
    assert event_id and elapsed < 0.5, f"il cancel protettivo ha atteso il disco per {elapsed:.2f} s"
    monkeypatch.setattr(box, "_insert", real_insert)
    rt.record("SESSION_CHECKPOINT", source="SYSTEM", component="test", phase="OBSERVATION", outcome="VERIFIED")
    assert [r for r in _events(bbpath, "ORDER_CANCEL_REQUEST") if r["event_id"] == event_id]


def test_protective_events_are_never_lost_with_concurrent_senders(bbpath):
    """Il writer dedicato si ferma quando la coda e' vuota e riparte al primo evento: nessun evento resta in coda."""
    box = _install_runtime_box()
    ids, errors = [], []

    def send(i):
        try:
            ids.append(rt.financial_gate("ORDER_CANCEL_REQUEST", component="betfair_client", cancel=True,
                                         market_id="1.5", bet_ids=[f"B{i}"]))
        except Exception as exc:                     # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=send, args=(i,)) for i in range(40)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(5)
    import time as _time
    deadline = _time.monotonic() + 5                 # nessun altro accesso al registro: scrive solo il writer
    written = set()
    while _time.monotonic() < deadline:
        written = {r["event_id"] for r in _events(bbpath, "ORDER_CANCEL_REQUEST")}
        if set(ids) <= written:
            break
        _time.sleep(0.05)
    assert not errors and len(ids) == 40 and set(ids) <= written, "eventi protettivi rimasti in coda"
    assert not box._protective_queue


# ------------------------------------------- passata unica sui chiamanti dei punti d'invio (Codex/D su #630)
# Un `GateRefused` prova che nulla e' partito: nessun chiamante lo deve trattare come esito ignoto.

def _stub_order(port, monkeypatch):
    from decimal import Decimal
    from core.microstake_safe import Side, SimOrder
    stub = SimOrder(bet_id="S1", market_id="1.5", selection_id=7, side=Side.BACK, price=Decimal("1000"),
                    size=Decimal("1"), matched=Decimal("0"), remaining=Decimal("0.10"), customer_ref="ms-1")
    monkeypatch.setattr(port, "order", lambda bet_id: stub)


@pytest.mark.parametrize("venue", ["sim", "live"])
def test_microstake_replace_refused_by_the_gate_is_a_definite_refusal_not_lost(venue, monkeypatch):
    """Codex su #630 (D-630-05): il replace rifiutato dal Gate B non e' mai `LostResponse` (che porta la saga in
    AMBIGUOUS con lo stub abbandonato): e' un rifiuto certo, e il coordinatore fa il rollback dello stub."""
    from decimal import Decimal
    from core.microstake_safe import LostResponse

    class Venue:
        def replace_orders(self, **kw):
            raise rt.GateRefused("ORDER_REPLACE_REQUEST senza scatola nera")

    for name in ("place_bet", "reduce_order", "cancel_orders", "list_current_orders", "get_current_orders",
                 "list_cleared_orders", "get_account_funds"):
        setattr(Venue, name, lambda self, *a, **k: None)

    if venue == "sim":
        from core.microstake_sim_port import SimulationExchangePort as Port
    else:
        from core.microstake_live_port import LiveExchangePort as Port
    port = Port(Venue())
    _stub_order(port, monkeypatch)
    with pytest.raises(RuntimeError) as info:
        port.replace("S1", Decimal("2.0"))
    assert not isinstance(info.value, LostResponse)
    assert str(info.value) == rt.GATE_REFUSED_CODE


def test_trading_engine_treats_a_gate_refusal_as_nothing_sent():
    """Codex su #630 (D-630-04): sul percorso LIVE diretto del motore il rifiuto del gate deve rilasciare l'intento
    e non diventare LIVE_SUBMIT_ESITO_IGNOTO."""
    from core.trading_engine import TradingEngine
    assert rt.GATE_REFUSED_CODE in TradingEngine._ERRORI_PRIMA_DELL_INVIO
    from betfair_client import BetfairClient
    fake = dict(zip(("username", "app_key", "cert_pem", "key_pem"), ("u", "a", "c.pem", "k.pem")))
    client = BetfairClient(**fake, session=object())
    engine = TradingEngine.__new__(TradingEngine)
    engine.simulation_broker = None
    assert engine._nulla_e_partito(rt.GateRefused("betfair_client: ORDER_PLACE_REQUEST"), client) is True


def test_a_protective_event_after_close_is_refused_never_silently_dropped(bbpath):
    """Codex su #630 (D-630-06): chiusa la scatola nera, un'uscita protettiva non viene accettata in una coda che
    nessuno scrivera' piu': rifiuto esplicito. Quelle accettate prima della chiusura sono scritte."""
    box = _install_runtime_box()
    before = rt.financial_gate("ORDER_CANCEL_REQUEST", component="betfair_client", cancel=True, market_id="1.5",
                               bet_ids=["B1"])
    box.end_session("STOP")
    box.close()
    with pytest.raises(bb.BlackBoxUnavailable):
        box.record("ORDER_CANCEL_REQUEST", critical=True, buffer_on_failure=True, source="SYSTEM",
                   component="betfair_client", phase="ATTEMPT", outcome="PENDING", market_id="1.5")
    assert [r for r in _events(bbpath, "ORDER_CANCEL_REQUEST") if r["event_id"] == before]


def test_concurrent_protective_events_during_shutdown_are_written_or_refused(bbpath):
    """D-630-06 con thread: durante lo shutdown ogni uscita protettiva e' scritta oppure rifiutata, mai accettata e
    poi persa."""
    box = _install_runtime_box()
    accepted, refused, stop = [], [], threading.Event()

    def sender():
        import time as _t
        i = 0
        while not stop.is_set():                 # ritmo fisso: attraversa la chiusura senza saturare il buffer
            _t.sleep(0.005)
            try:
                accepted.append(box.record("ORDER_CANCEL_REQUEST", critical=True, buffer_on_failure=True,
                                           source="SYSTEM", component="betfair_client", phase="ATTEMPT",
                                           outcome="PENDING", market_id="1.5", bet_id=f"B{i}"))
            except bb.BlackBoxUnavailable:
                refused.append(i)
            i += 1

    threads = [threading.Thread(target=sender) for _ in range(4)]
    for t in threads:
        t.start()
    import time as _time
    _time.sleep(0.2)
    box.end_session("STOP")
    box.close()
    _time.sleep(0.1)
    stop.set()
    for t in threads:
        t.join(5)
    written = {r["event_id"] for r in _events(bbpath, "ORDER_CANCEL_REQUEST")}
    lost = [e for e in accepted if e not in written]
    assert accepted and refused and not lost, f"{len(lost)} uscite protettive accettate e mai scritte"


def test_a_producer_paused_after_build_is_refused_once_shutdown_completes(bbpath, monkeypatch):
    """Sol su #630 (96f80476, D-630-07): il mittente si ferma DOPO `_build()` e prima dell'accodamento; nel frattempo
    `shutdown()` completa lo scarico finale e chiude. Quando riprende, l'uscita protettiva e' RIFIUTATA
    (BlackBoxUnavailable): il gate non autorizza mai un invio il cui evento nessuno scrivera'."""
    box = _install_runtime_box()
    real_build = box._build
    built, resume = threading.Event(), threading.Event()

    def paused_build(event_type, fields):
        rec = real_build(event_type, fields)
        if event_type == "ORDER_CANCEL_REQUEST":
            built.set()
            resume.wait(5)
        return rec

    monkeypatch.setattr(box, "_build", paused_build)
    outcome = {}

    def producer():
        try:
            outcome["id"] = rt.financial_gate("ORDER_CANCEL_REQUEST", component="betfair_client", cancel=True,
                                              market_id="1.5", bet_ids=["B1"])
        except rt.GateRefused as exc:
            outcome["refused"] = exc

    t = threading.Thread(target=producer)
    t.start()
    assert built.wait(5)
    rt.shutdown("STOP")                              # end_session + close: scarico finale gia' avvenuto
    resume.set()
    t.join(5)
    assert "refused" in outcome and "id" not in outcome, outcome
    assert not _events(bbpath, "ORDER_CANCEL_REQUEST"), "nessun evento: l'invio non e' stato autorizzato"


def test_close_drains_a_protective_event_accepted_after_the_session_end(bbpath, monkeypatch):
    """Claude D (mutante MA3, D-630-06): un cancel protettivo accettato tra `end_session()` e `close()`, con il writer
    dedicato disattivato, e' scritto dallo scarico finale di `close()`."""
    box = _install_runtime_box()
    monkeypatch.setattr(box, "_ensure_protective_writer", lambda: None)   # solo close() puo' scriverlo
    real_end = box.end_session
    late = {}

    def end_then_late_cancel(reason="STOP", **data):
        out = real_end(reason, **data)
        late["id"] = box.record("ORDER_CANCEL_REQUEST", critical=True, buffer_on_failure=True, source="SYSTEM",
                                component="betfair_client", phase="ATTEMPT", outcome="PENDING", market_id="1.5",
                                bet_id="B9")
        return out

    monkeypatch.setattr(box, "end_session", end_then_late_cancel)
    rt.shutdown("STOP")
    assert late.get("id")
    assert [r for r in _events(bbpath, "ORDER_CANCEL_REQUEST") if r["event_id"] == late["id"]], \
        "accettato prima della chiusura e mai scritto"


def test_a_protective_only_session_recovers_after_a_transient_journal_failure(bbpath, monkeypatch):
    """Codex su #630 (b78c4b6e): dopo un guasto temporaneo del registro, una sessione con sole uscite protettive deve
    riprendere a scrivere da sola: il writer dedicato riprova e scarica il backlog (eventi + GAP), senza bisogno di un
    `record()` sincrono estraneo. Altrimenti il buffer si riempie e i cancel vengono rifiutati a registro guarito."""
    import time as _time
    box = _install_runtime_box()
    real_insert = box._insert
    broken = {"on": True}

    def flaky(rec):
        if broken["on"]:
            raise sqlite3.OperationalError("disk I/O error")
        return real_insert(rec)

    monkeypatch.setattr(box, "_insert", flaky)
    first = rt.financial_gate("ORDER_CANCEL_REQUEST", component="betfair_client", cancel=True, market_id="1.5",
                              bet_ids=["B1"])
    deadline = _time.monotonic() + 5
    while not box._buffer and _time.monotonic() < deadline:   # il writer ha tentato durante il guasto
        _time.sleep(0.02)
    assert box._buffer
    broken["on"] = False                                       # il registro torna scrivibile
    second = rt.financial_gate("ORDER_CANCEL_REQUEST", component="betfair_client", cancel=True, market_id="1.5",
                               bet_ids=["B2"])
    deadline = _time.monotonic() + 5
    written = set()
    while _time.monotonic() < deadline:                        # nessun record() sincrono: scrive solo il writer
        written = {r["event_id"] for r in _events(bbpath, "ORDER_CANCEL_REQUEST")}
        if {first, second} <= written:
            break
        _time.sleep(0.05)
    assert {first, second} <= written, "backlog protettivo mai scaricato a registro guarito"
    assert _events(bbpath, "BLACKBOX_GAP") and not box._buffer
