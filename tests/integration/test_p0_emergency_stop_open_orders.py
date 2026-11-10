"""P0 AUD-BF-016 (decisione owner PF-OWNER-P0-EMERGENCY-STOP-20261110): l'arresto d'emergenza cancella davvero gli
ordini aperti e non dichiara successo senza averlo verificato sull'exchange.

Riproduzione (Claude D, #627 c.6775355046): il motore registra gli ordini nella tabella `orders`, l'arresto leggeva solo
`order_saga` (scritta da `OrderManager`, mai costruito in produzione) e non interrogava l'exchange: zero cancellazioni,
esito «riuscito».

Ambiente: `HeadlessApp` reale (SIM, SQLite reale in una cartella temporanea). L'exchange LIVE e' un `SimulationBroker`
separato collegato come client LIVE: espone gli stessi metodi del client Betfair (`get_current_orders`,
`cancel_orders`) e tiene lo stato degli ordini, quindi la verifica avviene sull'exchange, non sul DB. Nessuna rete,
nessun ordine reale.
"""
import time
import uuid

import pytest

import headless_main
from database import Database
from simulation_broker import SimulationBroker


def _app(tmp_path, monkeypatch):
    p = str(tmp_path / "pf.db")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(headless_main, "Database", lambda: Database(p))
    app = headless_main.HeadlessApp()
    app.build(start_services=False)
    app.telegram_service.start = lambda *a, **k: {"started": False, "stub": True}
    app.telegram_service.stop = lambda *a, **k: None
    return app


def _exchange_with_book(market_id="1.999", selection_id=11):
    ex = SimulationBroker(starting_balance=1000.0)
    return ex


def _live(app, exchange, monkeypatch):
    """Runtime in LIVE (stato al momento dell'arresto) con l'exchange simulato come client LIVE."""
    app.runtime.simulation_mode = False
    app.runtime.execution_mode = "LIVE"
    svc = app.runtime.betfair_service
    monkeypatch.setattr(svc, "client", exchange, raising=False)
    monkeypatch.setattr(svc, "get_live_client", lambda: exchange)


def _engine_order(db, *, market_id, bet_id, status="SUBMITTED", selection_id=11):
    """Riga `orders` come la scrive il motore (TradingEngine): payload della richiesta, risposta con betId."""
    ref = "pf-" + uuid.uuid4().hex[:12]
    return db.insert_order({
        "customer_ref": ref, "correlation_id": uuid.uuid4().hex, "status": status,
        "payload": {"market_id": market_id, "selection_id": selection_id, "bet_type": "BACK", "price": 50.0,
                    "stake": 5.0, "customer_ref": ref},
        "response": ({"bet_id": bet_id, "status": "SUCCESS"} if bet_id else None),
    })


def _open_on_exchange(exchange, market_id="1.999", price=50.0, size=5.0):
    res = exchange.place_bet(market_id=market_id, selection_id=11, side="BACK", price=price, size=size)
    bet_id = (res.get("instructionReports") or [{}])[0].get("betId") or res.get("bet_id")
    assert bet_id, res
    return bet_id


def _executable(exchange, market_id="1.999"):
    return [o for o in exchange.get_current_orders([market_id])
            if str(o.get("status")).upper() == "EXECUTABLE" and float(o.get("sizeRemaining") or 0) > 0]


def test_emergency_stop_cancels_an_open_engine_order_and_verifies_it_on_the_exchange(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch)
    exchange = _exchange_with_book()
    bet_id = _open_on_exchange(exchange)
    _engine_order(app.runtime.db, market_id="1.999", bet_id=bet_id)
    _live(app, exchange, monkeypatch)
    assert _executable(exchange), "precondizione: ordine aperto sull'exchange"

    res = app.runtime.emergency_stop(reason="test")

    assert _executable(exchange) == [], f"ordine ancora aperto dopo l'arresto d'emergenza: {res}"
    assert res["verification"] == "VERIFIED", res
    assert res["markets_attempted"] == 1
    outcomes = {o["bet_id"]: o["outcome"] for o in res["order_outcomes"]}
    assert outcomes.get(bet_id) == "CANCELLED", res["order_outcomes"]


# ------------------------------------------------------------------------------------------------- exchange controllato

class FakeExchange:
    """Exchange LIVE con comportamento controllato (forma `listCurrentOrders` di Betfair). Ogni ordine:
    betId, marketId, status EXECUTABLE/EXECUTION_COMPLETE, sizeMatched, sizeRemaining, customerOrderRef."""

    def __init__(self, orders, *, cancel_mode="ok", read_fail=(), emergency_seen=None):
        self.orders = [dict(o) for o in orders]
        self.cancel_mode = cancel_mode            # ok | fail | raise | noop
        self.read_fail = set(read_fail)           # "before" | "after"
        self.reads = 0
        self.cancel_calls = []
        self.emergency_seen = emergency_seen if emergency_seen is not None else []

    def get_current_orders(self, market_ids=None, emergency=False):
        self.emergency_seen.append(("read", emergency))
        self.reads += 1
        phase = "before" if self.reads == 1 else "after"
        if phase in self.read_fail:
            raise RuntimeError("TIMEOUT")
        return [dict(o) for o in self.orders if not market_ids or o["marketId"] in market_ids]

    def cancel_orders(self, *, market_id, bet_ids=None, emergency=False):
        self.emergency_seen.append(("cancel", emergency))
        self.cancel_calls.append(market_id)
        if self.cancel_mode == "raise":
            raise RuntimeError("REQUEST_FAILED: TIMEOUT")
        if self.cancel_mode == "fail":
            return {"ok": False, "market_id": market_id, "error": "API_ERROR"}
        if self.cancel_mode == "ok":
            for o in self.orders:
                if o["marketId"] == market_id and o["status"] == "EXECUTABLE":
                    o["status"], o["sizeRemaining"] = "EXECUTION_COMPLETE", 0.0
                    if not o["sizeMatched"]:
                        o["gone"] = True
            self.orders = [o for o in self.orders if not o.get("gone")]
        return {"ok": True, "market_id": market_id, "cancelled_count": 1}


def _o(bet_id, market="1.5", status="EXECUTABLE", matched=0.0, remaining=5.0, ref=""):
    return {"betId": bet_id, "marketId": market, "selectionId": 3, "side": "BACK", "status": status,
            "sizeMatched": matched, "sizeRemaining": remaining, "averagePriceMatched": 2.0 if matched else 0.0,
            "customerOrderRef": ref}


def test_partially_matched_order_is_cancelled_and_its_position_stays_visible(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch)
    ex = FakeExchange([_o("B1", matched=2.0, remaining=3.0)])
    _engine_order(app.runtime.db, market_id="1.5", bet_id="B1", status="PARTIAL")
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert res["verification"] == "VERIFIED", res
    assert {o["bet_id"]: o["outcome"] for o in res["order_outcomes"]}["B1"] == "PARTIALLY_MATCHED"
    assert res["matched_positions"] == [{"bet_id": "B1", "market_id": "1.5", "selection_id": 3, "side": "BACK",
                                         "size_matched": 2.0, "average_price_matched": 2.0}]


def test_already_matched_and_not_found_orders_are_distinguished(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch)
    ex = FakeExchange([_o("M1", status="EXECUTION_COMPLETE", matched=5.0, remaining=0.0)])
    _engine_order(app.runtime.db, market_id="1.5", bet_id="M1", status="PLACED")
    _engine_order(app.runtime.db, market_id="1.6", bet_id="GONE", status="SUBMITTED")
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    outcomes = {o["bet_id"]: o["outcome"] for o in res["order_outcomes"] if "bet_id" in o}
    assert outcomes == {"M1": "ALREADY_MATCHED", "GONE": "NOT_FOUND"}, res["order_outcomes"]
    assert res["verification"] == "VERIFIED" and res["matched_positions"][0]["bet_id"] == "M1"


@pytest.mark.parametrize("mode", ["fail", "raise", "noop"])
def test_cancel_not_done_is_never_reported_as_verified(tmp_path, monkeypatch, mode):
    app = _app(tmp_path, monkeypatch)
    ex = FakeExchange([_o("B1")], cancel_mode=mode)
    _engine_order(app.runtime.db, market_id="1.5", bet_id="B1")
    _live(app, ex, monkeypatch)
    published = []
    app.runtime.bus.subscribe("EMERGENCY_STOP_MANUAL_ACTION_REQUIRED", lambda p: published.append(p))
    res = app.runtime.emergency_stop(reason="t")
    assert res["verification"] in ("OPEN_ORDERS_REMAIN", "CANCEL_INCOMPLETE"), res
    deadline = time.monotonic() + 5                      # il bus consegna in modo asincrono
    while not published and time.monotonic() < deadline:
        time.sleep(0.02)
    assert res["manual_action_required"] is True and published, "notifica di intervento manuale"
    assert {o["bet_id"]: o["outcome"] for o in res["order_outcomes"]}["B1"] == "STILL_OPEN"
    assert headless_main._emergency_stop_outcome(res)[0] == "FAILED"


@pytest.mark.parametrize("phase", ["before", "after"])
def test_exchange_unreadable_means_unverified_never_success(tmp_path, monkeypatch, phase):
    app = _app(tmp_path, monkeypatch)
    ex = FakeExchange([_o("B1")], read_fail={phase})
    _engine_order(app.runtime.db, market_id="1.5", bet_id="B1")
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert ex.cancel_calls == ["1.5"], "il cancel parte comunque sui mercati noti"
    assert res["verification"] == "UNVERIFIED" and res["manual_action_required"] is True, res
    assert {o["bet_id"]: o["outcome"] for o in res["order_outcomes"]}["B1"] == "UNKNOWN"
    assert headless_main._emergency_stop_outcome(res)[0] == "UNKNOWN"


def test_ambiguous_order_without_bet_id_is_covered_by_the_market_cancel(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch)
    ex = FakeExchange([_o("X9", market="1.7")])                 # risposta persa: il bot non conosce il betId
    _engine_order(app.runtime.db, market_id="1.7", bet_id=None, status="AMBIGUOUS")
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert ex.cancel_calls == ["1.7"] and res["verification"] == "VERIFIED", res
    amb = [o for o in res["order_outcomes"] if o.get("status") == "AMBIGUOUS"]
    assert amb and amb[0]["outcome"] == "NO_OPEN_ORDER_ON_MARKET"


def test_open_order_known_only_on_the_exchange_by_customer_order_ref_is_cancelled(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch)
    oid = _engine_order(app.runtime.db, market_id="", bet_id=None, status="INFLIGHT")
    ref = app.runtime.db.get_order(oid)["customer_ref"]
    ex = FakeExchange([_o("R1", market="1.8", ref=ref)])
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert ex.cancel_calls == ["1.8"] and res["verification"] == "VERIFIED", res


def test_unattributed_open_orders_are_not_cancelled_blindly_and_require_the_operator(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch)
    ex = FakeExchange([_o("MANUAL", market="2.1")])
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert ex.cancel_calls == [] and res["unattributed_open_orders"][0]["bet_id"] == "MANUAL"
    assert res["manual_action_required"] is True
    assert headless_main._emergency_stop_outcome(res) == ("UNKNOWN", "UNATTRIBUTED_OPEN_ORDERS")


def test_emergency_stop_uses_the_breaker_bypass_and_is_idempotent(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch)
    seen = []
    ex = FakeExchange([_o("B1")], emergency_seen=seen)
    _engine_order(app.runtime.db, market_id="1.5", bet_id="B1")
    _live(app, ex, monkeypatch)
    first = app.runtime.emergency_stop(reason="t")
    second = app.runtime.emergency_stop(reason="t")             # riavvio/ripetizione: nessun effetto doppio
    assert first["verification"] == second["verification"] == "VERIFIED"
    assert seen and all(flag is True for _, flag in seen), "lettura e cancel dell'emergenza oltre il breaker"
    assert {o["bet_id"]: o["outcome"] for o in second["order_outcomes"]}["B1"] == "NOT_FOUND"


def test_sim_runtime_emergency_cancels_on_the_simulation_broker(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch)
    broker = SimulationBroker(starting_balance=1000.0)
    monkeypatch.setattr(app.runtime.betfair_service, "get_simulation_broker", lambda: broker)
    bet_id = _open_on_exchange(broker, market_id="1.42")
    _engine_order(app.runtime.db, market_id="1.42", bet_id=bet_id)
    app.runtime.simulation_mode = True
    res = app.runtime.emergency_stop(reason="t")
    assert res["venue"] == "SIM" and res["verification"] == "VERIFIED", res
    assert _executable(broker, "1.42") == []


def test_no_open_orders_anywhere_is_verified(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch)
    _live(app, FakeExchange([]), monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert res["verification"] == "VERIFIED" and res["manual_action_required"] is False, res
    assert headless_main._emergency_stop_outcome(res)[0] == "VERIFIED"


def test_live_client_breaker_is_bypassed_only_for_the_emergency(monkeypatch):
    from betfair_client import BetfairClient

    class Resp:
        def raise_for_status(self):
            pass

        def json(self):
            return [{"result": {"status": "SUCCESS", "instructionReports": [], "currentOrders": [],
                                "moreAvailable": False}}]

    class Sess:
        def post(self, *a, **k):
            return Resp()

    c = BetfairClient(username="u", app_key="a", cert_pem="c.pem", key_pem="k.pem", session=Sess())
    monkeypatch.setattr(c, "_session_token_value", lambda: "tok")
    monkeypatch.setattr(c, "_headers", lambda: {"X-Authentication": "tok"})
    monkeypatch.setattr(c, "_parse_json", lambda r, code: r.json())
    monkeypatch.setattr(c._api_breaker, "is_open", lambda: True)
    assert c.cancel_orders(market_id="1.1")["ok"] is False, "fuori dall'emergenza il breaker resta rispettato"
    assert c.cancel_orders(market_id="1.1", emergency=True)["ok"] is True
    with pytest.raises(RuntimeError, match="CIRCUIT_BREAKER_OPEN"):
        c.get_current_orders()
    assert c.get_current_orders(emergency=True) == []


def test_gui_emergency_button_alerts_the_operator_when_the_stop_is_not_verified():
    import mini_gui
    gui = object.__new__(mini_gui.MiniPickfairGUI)
    shown, workers = [], []

    class Runtime:
        def __init__(self, result):
            self.result = result

        def emergency_stop(self, reason):
            return self.result

    gui.uiq = type("Q", (), {"post": staticmethod(lambda fn, *a: fn(*a))})()
    gui._safe_show_error = lambda title, text: shown.append((title, text))
    gui._run_runtime_command_async = lambda name, fn: workers.append(fn)
    gui.runtime = Runtime({"manual_action_required": True, "verification": "UNVERIFIED",
                           "open_after_cancel": [{"bet_id": "1"}], "unattributed_open_orders": [],
                           "cancel_error_count": 0})
    gui._runtime_emergency_stop()
    workers[0]()
    assert shown and shown[0][0] == "EMERGENCY STOP: intervento manuale richiesto"
    assert "UNVERIFIED" in shown[0][1] and "Ordini ancora aperti: 1" in shown[0][1]
    shown.clear(), workers.clear()
    gui.runtime = Runtime({"manual_action_required": False, "verification": "VERIFIED"})
    gui._runtime_emergency_stop()
    workers[0]()
    assert shown == [], "nessun avviso per un arresto verificato"


def test_unrecognised_order_on_a_market_being_cancelled_is_covered_not_flagged(tmp_path, monkeypatch):
    """Uno stub noto solo a un altro registro (es. Microstake) sullo stesso mercato di un ordine del bot: il cancel di
    mercato lo copre e la verifica dopo lo controlla; non e' un ordine «non attribuibile» da lasciare all'operatore."""
    app = _app(tmp_path, monkeypatch)
    ex = FakeExchange([_o("B1"), _o("STUB")])
    _engine_order(app.runtime.db, market_id="1.5", bet_id="B1")
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert res["unattributed_open_orders"] == [] and res["verification"] == "VERIFIED", res
    assert ex.get_current_orders(["1.5"]) == []
