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

    def __init__(self, orders, *, cancel_mode="ok", read_fail=(), emergency_seen=None, appear_after=()):
        self.orders = [dict(o) for o in orders]
        self.appear_after = [dict(o) for o in appear_after]   # accettati dall'exchange dopo la prima lettura
        self.cancel_mode = cancel_mode            # ok | fail | raise | noop
        self.read_fail = set(read_fail)           # "before" | "after"
        self.reads = 0
        self.cancel_calls = []
        self.cancel_bet_ids = []
        self.emergency_seen = emergency_seen if emergency_seen is not None else []

    def get_current_orders(self, market_ids=None, emergency=False):
        self.emergency_seen.append(("read", emergency))
        self.reads += 1
        phase = "before" if self.reads == 1 else "after"
        if phase in self.read_fail:
            raise RuntimeError("TIMEOUT")
        if phase == "after" and self.appear_after:
            self.orders.extend(self.appear_after)
            self.appear_after = []
        return [dict(o) for o in self.orders if not market_ids or o["marketId"] in market_ids]

    def cancel_orders(self, *, market_id, bet_ids=None, emergency=False):
        self.emergency_seen.append(("cancel", emergency))
        self.cancel_calls.append(market_id)
        self.cancel_bet_ids.append(None if bet_ids is None else list(bet_ids))
        if bet_ids is not None and len(bet_ids) > 60:      # limite Betfair: max 60 istruzioni per cancelOrders
            return {"ok": False, "market_id": market_id, "error": "TOO_MANY_INSTRUCTIONS"}
        if self.cancel_mode == "raise":
            raise RuntimeError("REQUEST_FAILED: TIMEOUT")
        if self.cancel_mode == "fail":
            return {"ok": False, "market_id": market_id, "error": "API_ERROR"}
        if self.cancel_mode == "match_during":       # abbinamento di 2.0 un istante prima del cancel
            for o in self.orders:
                if o["marketId"] == market_id and (bet_ids is None or o["betId"] in bet_ids):
                    o.update(status="EXECUTION_COMPLETE", sizeMatched=2.0, sizeRemaining=0.0, averagePriceMatched=2.0)
            return {"ok": True, "market_id": market_id, "cancelled_count": 1}
        done = 0
        if self.cancel_mode == "ok":
            for o in self.orders:
                if o["marketId"] == market_id and o["status"] == "EXECUTABLE" and \
                        (bet_ids is None or o["betId"] in bet_ids):
                    o["status"], o["sizeRemaining"] = "EXECUTION_COMPLETE", 0.0
                    done += 1
                    if not o["sizeMatched"]:
                        o["gone"] = True
            self.orders = [o for o in self.orders if not o.get("gone")]
        return {"ok": True, "market_id": market_id, "cancelled_count": done or 1}


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


def test_ambiguous_order_without_bet_id_is_found_by_its_customer_order_ref_and_cancelled(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch)
    oid = _engine_order(app.runtime.db, market_id="1.7", bet_id=None, status="AMBIGUOUS")
    ref = app.runtime.db.get_order(oid)["customer_ref"]
    ex = FakeExchange([_o("X9", market="1.7", ref=ref)])        # risposta persa: betId ignoto, ref inviato (F-13)
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert ex.cancel_bet_ids == [["X9"]] and res["verification"] == "VERIFIED", res
    amb = [o for o in res["order_outcomes"] if o.get("status") == "AMBIGUOUS"]
    assert amb[0]["outcome"] == "SEE_BET_ID" and amb[0]["bet_id_by_ref"] == "X9"
    assert {o.get("bet_id"): o["outcome"] for o in res["order_outcomes"]}["X9"] == "CANCELLED"


def test_ambiguous_order_without_any_identity_is_never_guessed(tmp_path, monkeypatch):
    """Ordine AMBIGUOUS senza betId ne' customerOrderRef sull'exchange: non si distingue da un ordine manuale sullo
    stesso mercato, quindi nessuna cancellazione alla cieca, esito UNKNOWN e intervento manuale."""
    app = _app(tmp_path, monkeypatch)
    ex = FakeExchange([_o("X9", market="1.7")])
    _engine_order(app.runtime.db, market_id="1.7", bet_id=None, status="AMBIGUOUS")
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert ex.cancel_calls == [] and res["unattributed_open_orders"][0]["bet_id"] == "X9", res
    assert [o["outcome"] for o in res["order_outcomes"] if o.get("status") == "AMBIGUOUS"] == ["UNKNOWN"]
    assert res["manual_action_required"] is True


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


def test_foreign_order_on_the_same_market_is_never_cancelled(tmp_path, monkeypatch):
    """Rilievo Sol su c8338ef3: il cancel di mercato annullava anche ordini manuali o di altri programmi sullo stesso
    mercato. Ora si cancellano solo i betId del bot; l'ordine estraneo resta, elencato all'operatore."""
    app = _app(tmp_path, monkeypatch)
    ex = FakeExchange([_o("B1"), _o("MANUALE")])
    _engine_order(app.runtime.db, market_id="1.5", bet_id="B1")
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert ex.cancel_bet_ids == [["B1"]], "solo il betId del bot, mai il mercato intero"
    assert [o["betId"] for o in ex.orders] == ["MANUALE"]
    assert res["verification"] == "VERIFIED" and res["unattributed_open_orders"][0]["bet_id"] == "MANUALE"
    assert res["manual_action_required"] is True


def test_fill_during_the_cancel_is_reported_as_exposure(tmp_path, monkeypatch):
    """Rilievo Sol su c8338ef3: un abbinamento fra la fotografia e il cancel deve risultare dallo stato finale."""
    app = _app(tmp_path, monkeypatch)
    ex = FakeExchange([_o("B1")], cancel_mode="match_during")
    _engine_order(app.runtime.db, market_id="1.5", bet_id="B1")
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert {o["bet_id"]: o["outcome"] for o in res["order_outcomes"]}["B1"] == "PARTIALLY_MATCHED", res
    assert res["matched_positions"][0]["size_matched"] == 2.0


def test_unreadable_exchange_cancels_only_known_bot_bet_ids_never_the_whole_market(tmp_path, monkeypatch):
    """Sol su 7094b1a6: lettura iniziale fallita. Mai il cancel dell'intero mercato (toccherebbe ordini manuali o di
    altri programmi): si cancellano i betId del bot noti nel ledger, l'ordine estraneo resta e va all'operatore."""
    app = _app(tmp_path, monkeypatch)
    ex = FakeExchange([_o("B1"), _o("FOREIGN")], read_fail={"before"})
    _engine_order(app.runtime.db, market_id="1.5", bet_id="B1")
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert ex.cancel_bet_ids == [["B1"]], ex.cancel_bet_ids
    assert res["cancel_results"][0]["scope"] == "BET_IDS"
    assert [o["bet_id"] for o in res["unattributed_open_orders"]] == ["FOREIGN"]
    assert res["verification"] == "UNVERIFIED" and res["manual_action_required"] is True


def test_unreadable_exchange_without_known_bet_ids_sends_no_cancel(tmp_path, monkeypatch):
    """Lettura iniziale fallita e solo un invio senza betId sul mercato: nessun cancel alla cieca; l'invio resta
    irrisolto e l'esito non e' mai VERIFIED."""
    app = _app(tmp_path, monkeypatch)
    ex = FakeExchange([_o("FOREIGN", market="1.6")], read_fail={"before"})
    _engine_order(app.runtime.db, market_id="1.6", bet_id=None, status="INFLIGHT")
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert ex.cancel_calls == [], ex.cancel_calls
    assert len(res["unresolved_submissions"]) == 1 and res["verification"] == "UNVERIFIED", res
    assert res["manual_action_required"] is True


def test_sim_ledger_rows_are_not_live_candidates(tmp_path, monkeypatch):
    """Codex su c8338ef3: una riga `orders` di una sessione SIM (payload simulation_mode=True) non decide cosa si
    cancella in LIVE; l'ordine reale sullo stesso mercato, non del bot, resta e va all'operatore."""
    app = _app(tmp_path, monkeypatch)
    ref = "pf-sim-" + uuid.uuid4().hex[:8]
    app.runtime.db.insert_order({
        "customer_ref": ref, "correlation_id": uuid.uuid4().hex, "status": "SUBMITTED",
        "payload": {"market_id": "1.5", "selection_id": 3, "simulation_mode": True, "customer_ref": ref},
        "response": {"bet_id": "SIMBET-1", "status": "SUCCESS"}})
    ex = FakeExchange([_o("REALE")], read_fail={"before"})       # anche nel caso cieco: nessun cancel di mercato
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert ex.cancel_calls == [] and res["pending_count"] == 0, res


def test_unknown_provenance_is_handled_as_live(tmp_path, monkeypatch):
    """Codex su c8338ef3: emergenza ripristinata con provenienza UNKNOWN => si cancella e verifica in LIVE."""
    app = _app(tmp_path, monkeypatch)
    ex = FakeExchange([_o("B1")])
    _engine_order(app.runtime.db, market_id="1.5", bet_id="B1")
    _live(app, ex, monkeypatch)
    app.runtime.simulation_mode = True
    app.runtime._settlement_source_before_emergency = "UNKNOWN"
    res = app.runtime.emergency_stop(reason="t")
    assert res["venue"] == "LIVE" and ex.cancel_bet_ids == [["B1"]] and res["verification"] == "VERIFIED", res


@pytest.mark.parametrize("mode,read_fail", [("fail", ()), ("ok", ("after",))])
def test_matched_exposure_is_kept_when_cancel_or_verification_fails(tmp_path, monkeypatch, mode, read_fail):
    """Codex su d85a62ff: la parte abbinata di un ordine resta nel risultato anche se il cancel fallisce o la
    verifica non si legge."""
    app = _app(tmp_path, monkeypatch)
    ex = FakeExchange([_o("B1", matched=2.0, remaining=3.0)], cancel_mode=mode, read_fail=set(read_fail))
    _engine_order(app.runtime.db, market_id="1.5", bet_id="B1", status="PARTIAL")
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert res["verification"] != "VERIFIED" and res["manual_action_required"] is True
    assert [p["bet_id"] for p in res["matched_positions"]] == ["B1"], res
    assert res["matched_positions"][0]["size_matched"] == 2.0


def test_unreadable_local_ledger_is_never_verified(tmp_path, monkeypatch):
    """Codex su d85a62ff: `orders` illeggibile (es. database is locked) con exchange vuoto non e' VERIFIED."""
    app = _app(tmp_path, monkeypatch)
    _live(app, FakeExchange([]), monkeypatch)
    real = app.runtime.db._execute

    def locked(sql, *a, **k):
        if "FROM orders" in sql:
            import sqlite3
            raise sqlite3.OperationalError("database is locked")
        return real(sql, *a, **k)

    monkeypatch.setattr(app.runtime.db, "_execute", locked)
    res = app.runtime.emergency_stop(reason="t")
    assert res["verification"] == "SOURCES_INCOMPLETE" and res["manual_action_required"] is True, res
    assert headless_main._emergency_stop_outcome(res)[0] != "VERIFIED"


def test_bot_order_accepted_after_the_first_read_is_never_verified_away(tmp_path, monkeypatch):
    """Sol su d85a62ff: INFLIGHT senza mercato, accettato dall'exchange dopo la prima fotografia. Nessun mercato
    candidato, ma la lettura finale lo vede: mai VERIFIED, intervento manuale."""
    app = _app(tmp_path, monkeypatch)
    oid = _engine_order(app.runtime.db, market_id="", bet_id=None, status="INFLIGHT")
    ref = app.runtime.db.get_order(oid)["customer_ref"]
    ex = FakeExchange([], appear_after=[_o("LATE", market="3.3", ref=ref)])
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert res["verification"] == "OPEN_ORDERS_REMAIN" and res["manual_action_required"] is True, res
    assert res["open_after_cancel"][0]["bet_id"] == "LATE"


def test_unattributed_order_seen_only_in_the_final_read_is_reported(tmp_path, monkeypatch):
    """Sol su d85a62ff: un ordine non attribuibile comparso solo nella seconda lettura va all'operatore."""
    app = _app(tmp_path, monkeypatch)
    _engine_order(app.runtime.db, market_id="1.7", bet_id=None, status="AMBIGUOUS")
    ex = FakeExchange([], appear_after=[_o("X9", market="1.7")])
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert res["unattributed_open_orders"][0]["bet_id"] == "X9" and res["manual_action_required"] is True, res
    assert [o["outcome"] for o in res["order_outcomes"] if o.get("status") == "AMBIGUOUS"] == ["UNKNOWN"]


def test_matched_bot_order_seen_only_in_the_final_read_is_reported_as_exposure(tmp_path, monkeypatch):
    """Sol su 82bbd010: INFLIGHT senza betId, comparso solo nella lettura finale gia' abbinato e ritrovato per
    customerOrderRef: la sua esposizione va riportata."""
    app = _app(tmp_path, monkeypatch)
    oid = _engine_order(app.runtime.db, market_id="", bet_id=None, status="INFLIGHT")
    ref = app.runtime.db.get_order(oid)["customer_ref"]
    ex = FakeExchange([], appear_after=[_o("LATE", market="3.3", ref=ref, status="EXECUTION_COMPLETE",
                                           matched=5.0, remaining=0.0)])
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert [p["bet_id"] for p in res["matched_positions"]] == ["LATE"], res
    assert {o.get("bet_id"): o["outcome"] for o in res["order_outcomes"] if o.get("bet_id")}["LATE"] == \
        "ALREADY_MATCHED"
    assert [o["outcome"] for o in res["order_outcomes"] if o.get("status") == "INFLIGHT"] == ["SEE_BET_ID"]


# ------------------------------------------------------------------------- Microstake nativo (Codex su 82bbd010)

def test_native_cashout_hedge_is_recognised_by_its_microstake_ref(tmp_path, monkeypatch):
    """Hedge nativo del cashout con esito ignoto: la riga `orders` ha la `customer_ref` della sorgente, l'ordine
    sull'exchange porta il ref della saga (`response.raw.microstake_ref`). Va riconosciuto e cancellato."""
    app = _app(tmp_path, monkeypatch)
    ms_ref = "hedge:src-ms-0123456789ab"
    app.runtime.db.insert_order({
        "customer_ref": "hedge:src", "correlation_id": uuid.uuid4().hex, "status": "HEDGE_AMBIGUOUS",
        "payload": {"market_id": "1.8", "selection_id": 3, "bet_type": "LAY", "price": 1.01, "stake": 0.5},
        "response": {"ok": False, "status": "AMBIGUOUS", "order_unknown": True, "bet_id": None,
                     "raw": {"status": "AMBIGUOUS", "microstake_ref": ms_ref}},
    })
    ex = FakeExchange([_o("MS1", market="1.8", ref=ms_ref)])
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert ex.cancel_bet_ids == [["MS1"]], res
    assert res["unattributed_open_orders"] == [] and res["verification"] == "VERIFIED", res


def _save_live_saga(db, ref, **record):
    import json
    from core.microstake_live_port import LIVE_SAGA_SETTINGS_KEY
    db.save_settings({LIVE_SAGA_SETTINGS_KEY: json.dumps({ref: record})})


def test_microstake_saga_stub_known_only_to_the_saga_store_is_cancelled(tmp_path, monkeypatch):
    """Stub della saga (minimo exchange, prezzo non abbinabile) noto solo allo store `microstake.sagas.live`: si
    cancella. La saga NON e' conclusa (STUB_ACCEPTED): il sostituto puo' ancora arrivare dopo l'ultima lettura, quindi
    l'invio resta irrisolto e l'esito non e' VERIFIED (Sol su d892396d)."""
    app = _app(tmp_path, monkeypatch)
    _save_live_saga(app.runtime.db, "abc-ms-1", phase="STUB_ACCEPTED", bet_id="ST1", stub_bet_id="ST1",
                    request={"market_id": "1.9", "selection_id": 3, "side": "BACK"})
    ex = FakeExchange([_o("ST1", market="1.9", ref="abc-ms-1")])
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert ex.cancel_bet_ids == [["ST1"]], res
    assert {o.get("bet_id"): o["outcome"] for o in res["order_outcomes"] if o.get("bet_id")}["ST1"] == "CANCELLED"
    assert res["unattributed_open_orders"] == [], res
    assert [u["customer_ref"] for u in res["unresolved_submissions"]] == ["abc-ms-1"], res["unresolved_submissions"]
    assert res["verification"] == "SUBMISSIONS_UNRESOLVED" and res["manual_action_required"] is True


def test_saga_replacement_in_flight_after_stub_cancelled_is_unresolved(tmp_path, monkeypatch):
    """Sol su d892396d: saga REDUCED con sostituto in volo (replace_sent, nessun replacement_bet_id); lo stub e' gia'
    cancellato e non compare piu'. Il sostituto puo' arrivare dopo l'ultima lettura: mai VERIFIED."""
    app = _app(tmp_path, monkeypatch)
    _save_live_saga(app.runtime.db, "abc-ms-3", phase="REDUCED", bet_id="ST3", stub_bet_id="ST3", replace_sent=True,
                    request={"market_id": "1.33", "selection_id": 3, "side": "BACK"})
    ex = FakeExchange([])
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert [u["customer_ref"] for u in res["unresolved_submissions"]] == ["abc-ms-3"], res
    assert res["verification"] != "VERIFIED" and res["manual_action_required"] is True


def test_saga_replacement_found_on_the_exchange_is_cancelled_and_resolves_the_saga(tmp_path, monkeypatch):
    """Sostituto ritrovato sull'exchange per ref della saga con un betId NUOVO: e' del bot, si cancella, e chiude
    l'invio irrisolto della saga."""
    app = _app(tmp_path, monkeypatch)
    _save_live_saga(app.runtime.db, "abc-ms-4", phase="REDUCED", bet_id="ST4", stub_bet_id="ST4", replace_sent=True,
                    request={"market_id": "1.34", "selection_id": 3, "side": "BACK"})
    ex = FakeExchange([_o("RP4", market="1.34", ref="abc-ms-4")])
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert ex.cancel_bet_ids == [["RP4"]], ex.cancel_bet_ids
    assert res["unresolved_submissions"] == [] and res["verification"] == "VERIFIED", res


def test_concluded_active_saga_with_known_replacement_is_verified(tmp_path, monkeypatch):
    """Saga ACTIVE (sostituto piazzato, betId noto): nessun invio in volo; cancellato il sostituto, VERIFIED."""
    app = _app(tmp_path, monkeypatch)
    _save_live_saga(app.runtime.db, "abc-ms-5", phase="ACTIVE", bet_id="RP5", stub_bet_id="ST5",
                    replacement_bet_id="RP5", request={"market_id": "1.35", "selection_id": 3, "side": "BACK"})
    ex = FakeExchange([_o("RP5", market="1.35", ref="abc-ms-5")])
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert ex.cancel_bet_ids == [["RP5"]], ex.cancel_bet_ids
    assert res["unresolved_submissions"] == [] and res["verification"] == "VERIFIED", res


def test_concluded_microstake_sagas_are_not_candidates(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch)
    _save_live_saga(app.runtime.db, "abc-ms-2", phase="ROLLED_BACK", bet_id="", stub_bet_id="",
                    request={"market_id": "1.9"})
    ex = FakeExchange([])
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert res["order_outcomes"] == [] and res["verification"] == "VERIFIED", res


def test_unreadable_microstake_saga_store_is_never_verified(tmp_path, monkeypatch):
    from core.microstake_live_port import LIVE_SAGA_SETTINGS_KEY
    app = _app(tmp_path, monkeypatch)
    app.runtime.db.save_settings({LIVE_SAGA_SETTINGS_KEY: "{corrotto"})
    ex = FakeExchange([])
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert res["verification"] == "SOURCES_INCOMPLETE" and res["manual_action_required"] is True, res


@pytest.mark.parametrize("durable_on_retry, expected", [(False, "CANCEL_INCOMPLETE"), (True, "VERIFIED")])
def test_sim_cancel_not_persisted_is_never_verified_unless_the_retry_persists_it(tmp_path, monkeypatch,
                                                                                 durable_on_retry, expected):
    """Codex su 82bbd010: cancel SIM eseguito in memoria con lo snapshot di reload non scritto (`snapshot_persisted`
    False): al riavvio l'ordine tornerebbe aperto. VERIFIED solo se `ensure_durable` lo rende persistente."""
    app = _app(tmp_path, monkeypatch)
    broker = SimulationBroker(starting_balance=1000.0)
    monkeypatch.setattr(app.runtime.betfair_service, "get_simulation_broker", lambda: broker)
    bet_id = _open_on_exchange(broker, market_id="1.43")
    _engine_order(app.runtime.db, market_id="1.43", bet_id=bet_id)
    real_cancel = broker.cancel_orders
    retries = []

    def cancel_not_persisted(*, market_id=None, instructions=None, customer_ref=""):   # firma del broker reale
        return dict(real_cancel(market_id=market_id, instructions=instructions, customer_ref=customer_ref),
                    snapshot_persisted=False)

    monkeypatch.setattr(broker, "cancel_orders", cancel_not_persisted)
    monkeypatch.setattr(broker, "ensure_durable", lambda: retries.append(1) or durable_on_retry)
    app.runtime.simulation_mode = True
    res = app.runtime.emergency_stop(reason="t")
    assert retries == [1], res
    assert res["verification"] == expected, res
    assert res["manual_action_required"] is (expected != "VERIFIED"), res


def test_foreign_ref_saga_order_is_never_cancelled_and_goes_to_the_operator(tmp_path, monkeypatch):
    """Codex su c1db40c3: saga con `foreign_ref` (DUPLICATE_CUSTOMER_REF: un ordine con quel ref esisteva gia', origine
    non dimostrabile). Il recovery non lo adotta ne' lo cancella; l'arresto nemmeno: resta all'operatore. Vale anche
    se la riga `orders` dell'hedge porta lo stesso ref in `raw.microstake_ref`."""
    app = _app(tmp_path, monkeypatch)
    ref = "hedge:src-ms-feedfacecafe"
    _save_live_saga(app.runtime.db, ref, phase="AMBIGUOUS", reason="existing_customer_ref", foreign_ref=True,
                    bet_id="", stub_bet_id="", request={"market_id": "1.11", "selection_id": 3, "side": "LAY"})
    app.runtime.db.insert_order({
        "customer_ref": "hedge:src", "correlation_id": uuid.uuid4().hex, "status": "HEDGE_AMBIGUOUS",
        "payload": {"market_id": "1.11", "selection_id": 3, "bet_type": "LAY", "price": 1.01, "stake": 0.5},
        "response": {"ok": False, "status": "AMBIGUOUS", "order_unknown": True, "bet_id": None,
                     "raw": {"status": "AMBIGUOUS", "microstake_ref": ref}},
    })
    ex = FakeExchange([_o("FOREIGN1", market="1.11", ref=ref)])
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert ex.cancel_calls == [], res
    assert [o["bet_id"] for o in res["unattributed_open_orders"]] == ["FOREIGN1"], res
    assert res["manual_action_required"] is True, res


# ------------------------------------------------------------------------------------------- Sol su c1db40c3

def test_inflight_submission_not_found_on_the_exchange_is_never_verified(tmp_path, monkeypatch):
    """Invio INFLIGHT senza betId e nessun ordine sull'exchange: il placeOrders puo' essere ancora in volo e venire
    accettato dopo l'ultima lettura. «Nessun ordine» non e' dimostrato: mai VERIFIED, all'operatore."""
    app = _app(tmp_path, monkeypatch)
    oid = _engine_order(app.runtime.db, market_id="1.12", bet_id=None, status="INFLIGHT")
    ex = FakeExchange([])
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert res["verification"] == "SUBMISSIONS_UNRESOLVED" and res["manual_action_required"] is True, res
    assert [o["order_id"] for o in res["unresolved_submissions"]] == [str(oid)], res


def test_cancelled_row_with_matched_part_keeps_its_exposure_visible(tmp_path, monkeypatch):
    """Riga CANCELLED (residuo annullato) di un ordine parzialmente abbinato: nessun cancel, ma la parte abbinata e'
    esposizione reale sull'exchange e va riportata in matched_positions."""
    app = _app(tmp_path, monkeypatch)
    _engine_order(app.runtime.db, market_id="1.13", bet_id="PM1", status="CANCELLED")
    ex = FakeExchange([_o("PM1", market="1.13", status="EXECUTION_COMPLETE", matched=2.0, remaining=0.0)])
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert ex.cancel_calls == [], res
    assert [p["bet_id"] for p in res["matched_positions"]] == ["PM1"], res
    assert res["unattributed_open_orders"] == [] and res["verification"] == "VERIFIED", res


def test_emergency_state_not_persisted_requires_manual_action(tmp_path, monkeypatch):
    """Cancellazione verificata ma stato d'emergenza (LOCKDOWN) non salvato: il blocco potrebbe perdersi al riavvio,
    quindi intervento manuale, evento sul bus e avviso nella GUI."""
    import mini_gui
    app = _app(tmp_path, monkeypatch)
    monkeypatch.setattr(app.runtime, "_persist_emergency_state", lambda: "disk I/O error")
    events, publish = [], app.runtime.bus.publish
    monkeypatch.setattr(app.runtime.bus, "publish", lambda name, payload=None, *a, **k: (
        events.append(payload) if name == "EMERGENCY_STOP_MANUAL_ACTION_REQUIRED" else None,
        publish(name, payload, *a, **k))[1])
    ex = FakeExchange([])
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert res["verification"] == "VERIFIED" and res["manual_action_required"] is True, res
    assert events and events[-1]["persist_error"] == "disk I/O error"
    assert "NON salvato" in mini_gui._emergency_manual_action_text(res)


def test_inflight_without_market_is_unresolved_even_when_the_first_read_fails(tmp_path, monkeypatch):
    """Sol su ff0f9ea1: INFLIGHT senza mercato ne' betId, prima lettura fallita e lettura finale vuota. L'esito e'
    UNKNOWN, ma l'invio puo' ancora essere accettato: mai VERIFIED, all'operatore."""
    app = _app(tmp_path, monkeypatch)
    _engine_order(app.runtime.db, market_id="", bet_id=None, status="INFLIGHT")
    ex = FakeExchange([], read_fail=("before",))
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert res["verification"] != "VERIFIED" and res["manual_action_required"] is True, res
    assert len(res["unresolved_submissions"]) == 1, res


def test_denied_rows_are_not_unresolved_submissions(tmp_path, monkeypatch):
    """D-629-05 (Claude D su 44164b44): una riga DENIED e' un rifiuto PRE-invio (risk gate, safe mode, durabilita',
    autorita'), mai un ordine sull'exchange. Non e' un invio irrisolto: senza altri ordini l'arresto e' VERIFIED."""
    app = _app(tmp_path, monkeypatch)
    for _ in range(3):
        _engine_order(app.runtime.db, market_id="1.14", bet_id=None, status="DENIED")
    ex = FakeExchange([])
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert res["unresolved_submissions"] == [] and res["verification"] == "VERIFIED", res
    assert res["manual_action_required"] is False, res


def test_more_than_60_bot_orders_on_a_market_are_cancelled_in_batches(tmp_path, monkeypatch):
    """Sol su 44164b44: Betfair accetta al massimo 60 istruzioni per cancelOrders. Con 61 ordini aperti del bot sullo
    stesso mercato il cancel va diviso in lotti, mai un'unica richiesta respinta."""
    app = _app(tmp_path, monkeypatch)
    bets = [f"B{i:03d}" for i in range(61)]
    for b in bets:
        _engine_order(app.runtime.db, market_id="1.15", bet_id=b)
    ex = FakeExchange([_o(b, market="1.15") for b in bets])
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert all(len(batch) <= 60 for batch in ex.cancel_bet_ids), [len(b) for b in ex.cancel_bet_ids]
    assert sorted(b for batch in ex.cancel_bet_ids for b in batch) == bets
    assert res["verification"] == "VERIFIED" and res["open_after_cancel"] == [], res["verification"]
    assert res["cancelled_count"] == 61, res["cancelled_count"]


def test_a_rejected_batch_does_not_stop_the_other_batches(tmp_path, monkeypatch):
    """Primo lotto respinto: il secondo si invia comunque; l'esito e' CANCEL_INCOMPLETE, mai VERIFIED."""
    app = _app(tmp_path, monkeypatch)
    bets = [f"C{i:03d}" for i in range(61)]
    for b in bets:
        _engine_order(app.runtime.db, market_id="1.16", bet_id=b)
    ex = FakeExchange([_o(b, market="1.16") for b in bets])
    real_cancel, calls = ex.cancel_orders, []

    def first_batch_rejected(*, market_id, bet_ids=None, emergency=False):
        calls.append(list(bet_ids or []))
        if len(calls) == 1:
            return {"ok": False, "market_id": market_id, "error": "SERVICE_UNAVAILABLE"}
        return real_cancel(market_id=market_id, bet_ids=bet_ids, emergency=emergency)

    ex.cancel_orders = first_batch_rejected
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert [len(c) for c in calls] == [60, 1], [len(c) for c in calls]
    assert res["verification"] in ("OPEN_ORDERS_REMAIN", "CANCEL_INCOMPLETE") and res["manual_action_required"], res
    assert len(res["open_after_cancel"]) == 60 and res["cancelled_count"] == 1, res["cancelled_count"]


# ------------------------------------------------- direttiva owner D-629-05: DENIED solo se pre-invio dimostrato

def test_denied_is_reachable_only_from_inflight_never_after_a_send():
    """Invariante su cui poggia la chiusura di DENIED: la macchina a stati del motore ammette DENIED solo da INFLIGHT
    (risk gate prima di `_atomic_submit`), mai da SUBMITTED o AMBIGUOUS (invio avvenuto o incerto)."""
    from core.trading_constants import ALLOWED_TRANSITIONS
    sources = sorted(src for src, targets in ALLOWED_TRANSITIONS.items() if "DENIED" in targets)
    assert sources == ["INFLIGHT"], sources
    assert ALLOWED_TRANSITIONS["DENIED"] == set()


def test_ambiguous_after_a_send_attempt_is_unresolved(tmp_path, monkeypatch):
    """Direttiva owner, test 3: AMBIGUOUS senza betId dopo un tentativo d'invio, nulla sull'exchange => irrisolto."""
    app = _app(tmp_path, monkeypatch)
    _engine_order(app.runtime.db, market_id="1.17", bet_id=None, status="AMBIGUOUS")
    ex = FakeExchange([])
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert res["verification"] == "SUBMISSIONS_UNRESOLVED" and res["manual_action_required"] is True, res


def test_denied_row_with_transmission_evidence_stays_a_candidate(tmp_path, monkeypatch):
    """Una riga DENIED che porta un betId e' contraddittoria: non e' dimostrato che non sia partita. Resta del bot:
    il suo ordine aperto si attribuisce e si cancella, mai «non attribuibile» ne' ignorato."""
    app = _app(tmp_path, monkeypatch)
    _engine_order(app.runtime.db, market_id="1.18", bet_id="DX1", status="DENIED")
    ex = FakeExchange([_o("DX1", market="1.18")])
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert ex.cancel_bet_ids == [["DX1"]], res
    assert res["unattributed_open_orders"] == [] and res["verification"] == "VERIFIED", res


def test_denied_row_with_unknown_outcome_is_unresolved(tmp_path, monkeypatch):
    """DENIED con esito ignoto registrato (order_unknown) e nessun ordine visibile: mai VERIFIED."""
    app = _app(tmp_path, monkeypatch)
    app.runtime.db.insert_order({
        "customer_ref": "pf-x", "correlation_id": uuid.uuid4().hex, "status": "DENIED",
        "payload": {"market_id": "1.19", "selection_id": 3, "bet_type": "BACK", "price": 2.0, "stake": 2.0},
        "response": {"status": "AMBIGUOUS", "order_unknown": True, "bet_id": None},
    })
    ex = FakeExchange([])
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert res["verification"] == "SUBMISSIONS_UNRESOLVED" and res["manual_action_required"] is True, res


def test_mixed_history_denied_ambiguous_and_open_orders_never_falsely_verified(tmp_path, monkeypatch):
    """Direttiva owner, test 5: storico con DENIED pre-invio, un AMBIGUOUS senza betId e ordini aperti del bot.
    I DENIED non contano come irrisolti, l'AMBIGUOUS si', gli aperti si cancellano: l'esito non e' VERIFIED."""
    app = _app(tmp_path, monkeypatch)
    for _ in range(2):
        _engine_order(app.runtime.db, market_id="1.20", bet_id=None, status="DENIED")
    amb = _engine_order(app.runtime.db, market_id="1.20", bet_id=None, status="AMBIGUOUS")
    _engine_order(app.runtime.db, market_id="1.20", bet_id="OP1")
    _engine_order(app.runtime.db, market_id="1.21", bet_id="OP2")
    ex = FakeExchange([_o("OP1", market="1.20"), _o("OP2", market="1.21")])
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    assert sorted(b for batch in ex.cancel_bet_ids for b in batch) == ["OP1", "OP2"], res
    assert [o["order_id"] for o in res["unresolved_submissions"]] == [str(amb)], res["unresolved_submissions"]
    assert res["verification"] == "SUBMISSIONS_UNRESOLVED" and res["manual_action_required"] is True, res


# ------------------------------------- direttiva owner: lotti da 60 con errore parziale (121 ordini = 60 + 60 + 1)

def _batched_stop(tmp_path, monkeypatch, second_batch):
    """121 ordini del bot aperti sullo stesso mercato; il secondo lotto si comporta secondo `second_batch`."""
    app = _app(tmp_path, monkeypatch)
    bets = [f"L{i:03d}" for i in range(121)]
    for b in bets:
        _engine_order(app.runtime.db, market_id="1.30", bet_id=b)
    ex = FakeExchange([_o(b, market="1.30") for b in bets])
    real_cancel, calls = ex.cancel_orders, []

    def cancel(*, market_id, bet_ids=None, emergency=False):
        calls.append(list(bet_ids or []))
        if len(calls) == 2:
            if second_batch == "fail":
                return {"ok": False, "market_id": market_id, "error": "SERVICE_UNAVAILABLE"}
            if second_batch == "timeout":
                raise RuntimeError("REQUEST_FAILED: TIMEOUT")
            if second_batch == "match":       # il lotto si abbina per intero un istante prima del cancel
                for o in ex.orders:
                    if o["betId"] in bet_ids:
                        o.update(status="EXECUTION_COMPLETE", sizeMatched=5.0, sizeRemaining=0.0,
                                 averagePriceMatched=2.0)
                return {"ok": True, "market_id": market_id, "cancelled_count": 0}
        return real_cancel(market_id=market_id, bet_ids=bet_ids, emergency=emergency)

    ex.cancel_orders = cancel
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    by_bet = {o["bet_id"]: o["outcome"] for o in res["order_outcomes"] if o.get("bet_id")}
    return res, calls, bets, by_bet


@pytest.mark.parametrize("second_batch", ["fail", "timeout"])
def test_failed_middle_batch_keeps_per_order_traceability(tmp_path, monkeypatch, second_batch):
    res, calls, bets, by_bet = _batched_stop(tmp_path, monkeypatch, second_batch)
    assert [len(c) for c in calls] == [60, 60, 1]
    first, middle, last = bets[:60], bets[60:120], bets[120:]
    assert all(by_bet[b] == "CANCELLED" for b in first + last), "primo e ultimo lotto tracciati come cancellati"
    assert all(by_bet[b] == "STILL_OPEN" for b in middle), "il lotto fallito non risulta mai cancellato"
    assert sorted(o["bet_id"] for o in res["open_after_cancel"]) == middle
    assert res["cancelled_count"] == 61 and res["verification"] == "OPEN_ORDERS_REMAIN", res["verification"]
    assert res["manual_action_required"] is True
    (market_result,) = res["cancel_results"]
    assert [b["ok"] for b in market_result["batches"]] == [True, False, True]
    assert market_result["batches"][1]["bet_ids"] == middle


def test_batch_matched_during_cancel_is_exposure_not_cancelled(tmp_path, monkeypatch):
    res, calls, bets, by_bet = _batched_stop(tmp_path, monkeypatch, "match")
    assert [len(c) for c in calls] == [60, 60, 1]
    middle = bets[60:120]
    # Aperti alla prima lettura e abbinati durante il cancel: mai CANCELLED, esposizione riportata (contratto:
    # PARTIALLY_MATCHED = parte abbinata che resta).
    assert all(by_bet[b] in ("PARTIALLY_MATCHED", "ALREADY_MATCHED") for b in middle), {b: by_bet[b] for b in middle[:3]}
    assert sorted(p["bet_id"] for p in res["matched_positions"]) == middle
    assert all(by_bet[b] == "CANCELLED" for b in bets[:60] + bets[120:])


@pytest.mark.parametrize("in_flight", ["ok", "timeout"])
def test_authority_lost_while_a_batch_is_in_flight(tmp_path, monkeypatch, in_flight):
    """Sol su 7094b1a6, sonde owner B4/B5: l'autorita' del runtime si perde mentre il primo lotto e' in volo. Nessun
    altro lotto parte; il lotto in volo conserva i suoi betId e il proprio stato di risposta (successo confermato con
    [ok], UNKNOWN con [timeout]) ed e' marcato authority_lost_during; un record per lotto (NOT_SENT per i successivi);
    mai VERIFIED, intervento manuale."""
    app = _app(tmp_path, monkeypatch)
    bets = [f"A{i:03d}" for i in range(121)]
    for b in bets:
        _engine_order(app.runtime.db, market_id="1.31", bet_id=b)
    ex = FakeExchange([_o(b, market="1.31") for b in bets])
    real_cancel = ex.cancel_orders

    def cancel(*, market_id, bet_ids=None, emergency=False):
        result = real_cancel(market_id=market_id, bet_ids=bet_ids, emergency=emergency)
        if in_flight == "timeout":
            raise RuntimeError("REQUEST_FAILED: TIMEOUT")
        return result

    ex.cancel_orders = cancel
    _live(app, ex, monkeypatch)
    monkeypatch.setattr(app.runtime, "_authority_definitively_lost",
                        lambda: "AUTHORITY_SUPERSEDED" if ex.cancel_calls else None)
    res = app.runtime.emergency_stop(reason="t")
    assert [len(c) for c in ex.cancel_bet_ids] == [60], "dopo la perdita di autorita' nessun altro lotto"
    assert res["verification"] != "VERIFIED" and res["manual_action_required"] is True, res["verification"]
    assert any(e.get("stage") == "runtime_authority" for e in res["cancel_errors"]), res["cancel_errors"]
    (market_result,) = res["cancel_results"]
    batches = market_result["batches"]
    in_flight_status = "OK" if in_flight == "ok" else "UNKNOWN"     # requisito owner 3: stato di risposta vero
    assert [b["status"] for b in batches] == [in_flight_status, "NOT_SENT", "NOT_SENT"], batches
    assert batches[0]["authority_lost_during"] == "AUTHORITY_SUPERSEDED", batches[0]
    assert [len(b["bet_ids"]) for b in batches] == [60, 60, 1]
    assert batches[0]["bet_ids"] == bets[:60], "betId del lotto in volo conservati per la riconciliazione"
    assert res["cancelled_count"] == (60 if in_flight == "ok" else 0), "solo un successo confermato conta"
    # Incidente distinto, registrato anche se l'exchange conferma le cancellazioni.
    assert res["authority_lost"] and res["authority_lost"][0]["error"] == "AUTHORITY_SUPERSEDED", res["authority_lost"]
    # Esito della RICHIESTA (lotto UNKNOWN) separato dallo stato EFFETTIVO degli ordini, deciso dalla lettura finale:
    # l'exchange ha cancellato i primi 60 (conferma successiva), gli altri 61 restano aperti e non sono mai CANCELLED.
    by_bet = {o["bet_id"]: o["outcome"] for o in res["order_outcomes"] if o.get("bet_id")}
    assert all(by_bet[b] == "CANCELLED" for b in bets[:60]), {b: by_bet[b] for b in bets[:2]}
    assert all(by_bet[b] == "STILL_OPEN" for b in bets[60:])
    assert sorted(o["bet_id"] for o in res["open_after_cancel"]) == bets[60:]


def test_cancel_timeout_is_unknown_not_failed(tmp_path, monkeypatch):
    """Un timeout sul cancel non prova che il cancel non sia avvenuto: il lotto e' UNKNOWN, non FAILED."""
    app = _app(tmp_path, monkeypatch)
    _engine_order(app.runtime.db, market_id="1.32", bet_id="T1")
    ex = FakeExchange([_o("T1", market="1.32")], cancel_mode="raise")
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    (market_result,) = res["cancel_results"]
    assert [b["status"] for b in market_result["batches"]] == ["UNKNOWN"], market_result
    assert res["verification"] != "VERIFIED" and res["manual_action_required"] is True


@pytest.mark.parametrize("durable_on_retry, expected", [(False, "CANCEL_INCOMPLETE"), (True, "VERIFIED")])
def test_second_sim_stop_checks_durability_even_without_new_cancels(tmp_path, monkeypatch, durable_on_retry,
                                                                     expected):
    """Sol su d892396d: un arresto precedente ha lasciato lo snapshot SIM non durevole; la memoria e' gia' pulita,
    quindi il secondo arresto non cancella nulla. La durabilita' si verifica comunque: al riavvio gli ordini
    tornerebbero aperti."""
    app = _app(tmp_path, monkeypatch)
    broker = SimulationBroker(starting_balance=1000.0)
    monkeypatch.setattr(app.runtime.betfair_service, "get_simulation_broker", lambda: broker)
    retries = []
    monkeypatch.setattr(broker, "is_durable", lambda: False)
    monkeypatch.setattr(broker, "ensure_durable", lambda: retries.append(1) or durable_on_retry)
    app.runtime.simulation_mode = True
    res = app.runtime.emergency_stop(reason="t")
    assert retries == [1], res
    assert res["verification"] == expected, res
    assert res["manual_action_required"] is (expected != "VERIFIED"), res


@pytest.mark.parametrize("bet_ids", [None, []])
def test_emergency_cancel_never_sends_a_market_wide_cancel(tmp_path, monkeypatch, bet_ids):
    """GUARDRAIL D (P3 su d892396d): `_emergency_cancel` senza betId non chiama mai l'exchange (per Betfair una lista
    vuota o assente vuol dire «tutti gli ordini del mercato»); esito fail-closed."""
    app = _app(tmp_path, monkeypatch)
    ex = FakeExchange([_o("FOREIGN", market="1.40")])
    res = app.runtime._emergency_cancel(ex, "1.40", bet_ids)
    assert ex.cancel_calls == [] and res["ok"] is False and res["error"] == "MARKET_WIDE_CANCEL_FORBIDDEN"


def test_pending_saga_unresolved_until_protective_recovery_rolls_it_back(tmp_path, monkeypatch):
    """Via d'uscita (D-629-06, revisione concordata con Claude D): (a) saga STUB_ACCEPTED => lo stop resta
    SUBMISSIONS_UNRESOLVED; (b) il recovery protettivo (avvio/resume con STOP attivo: cancellazione dello stub, mai
    replace; provato da tests/core/test_microstake_live_adapter.py::
    test_recovery_with_live_blocked_only_cancels_the_stub_never_reprices_it) porta la saga a ROLLED_BACK => lo stop
    successivo e' VERIFIED. UNRESOLVED non resta permanente."""
    import json
    from core.microstake_live_port import LIVE_SAGA_SETTINGS_KEY
    from core.microstake_sim_port import DbSagaStore
    app = _app(tmp_path, monkeypatch)
    _save_live_saga(app.runtime.db, "abc-ms-6", phase="STUB_ACCEPTED", bet_id="ST6", stub_bet_id="ST6",
                    request={"market_id": "1.36", "selection_id": 3, "side": "BACK"})
    ex = FakeExchange([_o("ST6", market="1.36", ref="abc-ms-6")])
    _live(app, ex, monkeypatch)
    first = app.runtime.emergency_stop(reason="t1")
    assert first["verification"] == "SUBMISSIONS_UNRESOLVED", first["verification"]
    # Esito del recovery protettivo: stub cancellato (gia' fatto dal primo stop), saga ROLLED_BACK nello store.
    DbSagaStore(app.runtime.db, LIVE_SAGA_SETTINGS_KEY).save("abc-ms-6", phase="ROLLED_BACK")
    assert json.loads(app.runtime.db.get_settings()[LIVE_SAGA_SETTINGS_KEY])["abc-ms-6"]["phase"] == "ROLLED_BACK"
    second = app.runtime.emergency_stop(reason="t2")
    assert second["unresolved_submissions"] == [] and second["verification"] == "VERIFIED", second


def test_cancel_refused_by_the_blackbox_gate_is_not_sent_never_unknown(tmp_path, monkeypatch):
    """BB-1G x #629: il Gate B rifiuta PRIMA di qualunque invio (`GateRefused`, BLACKBOX_UNAVAILABLE). Il lotto e'
    NOT_SENT con quel motivo, mai UNKNOWN (che vorrebbe dire «forse cancellato»), e l'arresto non e' mai VERIFIED."""
    from core import blackbox_runtime
    app = _app(tmp_path, monkeypatch)
    ex = FakeExchange([_o("B1")])

    def refused(**_kw):
        ex.cancel_calls.append("refused")
        raise blackbox_runtime.GateRefused("betfair_client: ORDER_CANCEL_REQUEST senza scatola nera")

    ex.cancel_orders = lambda *, market_id, bet_ids=None, emergency=False: refused()
    _engine_order(app.runtime.db, market_id="1.5", bet_id="B1")
    _live(app, ex, monkeypatch)
    res = app.runtime.emergency_stop(reason="t")
    (market_result,) = res["cancel_results"]
    (batch,) = market_result["batches"]
    assert batch["status"] == "NOT_SENT" and batch["error"] == blackbox_runtime.GATE_REFUSED_CODE, batch
    assert res["verification"] != "VERIFIED" and res["manual_action_required"] is True, res["verification"]
    assert {o["bet_id"]: o["outcome"] for o in res["order_outcomes"]}["B1"] == "STILL_OPEN"
