"""F-04 (#586) — chiusura del SOLO tavolo che raggiunge SL/TP, consumando gli eventi di F-02.

Catena reale: RuntimeController -> TradingEngine -> SimulationBroker (SQLite su tmp_path) per gli ingressi; cashout
cablato come headless (`cabla_catena_cashout`: bridge, executor, OrderRouter, ledger `orders`). Gli eventi
SL_TRIGGERED/TP_TRIGGERED sono costruiti con la forma v1 pubblicata da `_trigger_auto_close_table` sul branch F-02
(keep/f02-sl-tp-path-20261103 @ 80b0090), che non e' su main: payload SINTETICO, non registrato.
"""
from __future__ import annotations

import json
import threading
from types import SimpleNamespace

import pytest

import cashout_router
import cashout_wiring
from core import sl_tp_close
from core.runtime_controller import RuntimeController
from core.system_state import RuntimeMode
from core.trading_engine import TradingEngine
from database import Database
from simulation_broker import SimulationBroker
from tests.integration.test_issue461_pr26a_customer_ref_provenance import (
    _decision, _Executor, _Settings, _Telegram)

MKT, SEL_A, SEL_B, SEL_C = "1.234", 11, 12, 13


class _Bus:
    """Bus sincrono con lo stesso indice dei subscriber dell'EventBus (``_subscribers``): il runtime lo ispeziona per
    sapere se la catena cashout e' cablata."""

    def __init__(self):
        self._subscribers = {}
        self.events = []

    def subscribe(self, topic, handler):
        self._subscribers.setdefault(topic, []).append(handler)

    def publish(self, topic, payload=None):
        self.events.append((topic, dict(payload) if isinstance(payload, dict) else payload))
        for handler in list(self._subscribers.get(topic, [])):
            handler(payload)

    def payloads(self, topic):
        return [p for t, p in self.events if t == topic]


class _Service:
    """BetfairService in SIMULATION, con il book reale del broker (anche con prezzi)."""

    def __init__(self, broker, funds=100.0):
        self.simulation_broker = broker
        self.funds = funds

    def get_client(self):
        return self.simulation_broker

    def set_simulation_mode(self, _enabled):
        return None

    def is_simulation_mode(self):
        return True

    def get_simulation_broker(self):
        return self.simulation_broker

    def get_live_client(self):
        return None

    def get_account_funds(self):
        return {"available": self.funds, "exposure": 0.0, "ok": True}

    def list_current_orders(self, market_ids=None):
        return list(self.simulation_broker.get_current_orders(market_ids) or [])

    def get_market_book_snapshot(self, market_id, include_prices=False):
        return self.simulation_broker.get_market_book(market_id)

    def status(self):
        return {"connected": True}

    def ensure_stream_session_ready(self):
        return True


def _book(broker, status="OPEN", *, back=(2.0, 500.0), lay=(2.02, 500.0)):
    runners = [{"selectionId": sel, "status": "ACTIVE", "ex": {
        "availableToBack": [{"price": back[0], "size": back[1]}],
        "availableToLay": [{"price": lay[0], "size": lay[1]}]}} for sel in (SEL_A, SEL_B, SEL_C)]
    broker.update_market_book(MKT, {"marketId": MKT, "status": status, "inplay": False, "runners": runners})


def _chain(tmp_path, *, broker=None, db_name="f04.db", stake=4.0):
    bus = _Bus()
    broker = broker or SimulationBroker(starting_balance=100.0)       # Roserpina con 100 EUR
    svc = _Service(broker)
    db = Database(str(tmp_path / db_name))
    engine = TradingEngine(bus=bus, db=db, client_getter=svc.get_client, executor=_Executor())
    rc = RuntimeController(bus=bus, db=db, settings_service=_Settings(False), betfair_service=svc,
                           telegram_service=_Telegram(), trading_engine=engine)
    engine.runtime_controller = rc
    rc.mode = RuntimeMode.ACTIVE
    rc.mm.calculate = _decision(stake)
    cashout_wiring.cabla_catena_cashout(bus=bus, betfair_service=svc, persist=lambda _r: None,
                                        notify=lambda *_a, **_k: None, ledger=db)
    return SimpleNamespace(bus=bus, broker=broker, svc=svc, db=db, engine=engine, rc=rc)


def _enter(c, selection, price, received_at):
    c.rc._on_signal_received({"market_id": MKT, "selection_id": selection, "price": price, "bet_type": "BACK",
                              "simulation_mode": True, "chat_id": -100123, "received_at": received_at,
                              "event": "Alpha v Beta", "market": "Match Odds", "selection": f"S{selection}"})


def _three_tables(c):
    """Stessa partita e stesso mercato: A BACK abbinato su 11, B BACK abbinato su 12, C BACK non abbinato su 13
    (un tavolo per selezione: la chiave evento di Roserpina e' mercato:selezione:lato)."""
    _book(c.broker)
    _enter(c, SEL_A, 2.0, "2026-11-04T10:00:00.000001+00:00")
    _enter(c, SEL_B, 2.0, "2026-11-04T10:00:01.000001+00:00")
    _enter(c, SEL_C, 50.0, "2026-11-04T10:00:02.000001+00:00")
    tables = sorted(c.rc.table_manager.active_tables(), key=lambda t: t.table_id)
    assert len(tables) == 3, [t.to_dict() for t in tables]
    return tables


def _trigger(table, topic=sl_tp_close.SL_TRIGGERED, **over):
    """Forma v1 di F-02 (`_trigger_auto_close_table`, branch F-02 @ 80b0090)."""
    meta = table.meta
    payload = {"table_id": table.table_id, "market_id": table.market_id, "selection_id": table.selection_id,
               "side": str(meta.get("bet_type") or "").upper(), "stake": meta.get("stake"),
               "price": meta.get("price"), "threshold": -1.0, "pnl": -1.5,
               "reason": sl_tp_close.TRIGGER_REASONS[topic], "customer_ref": meta.get("customer_ref") or "",
               "source": "AUTO_MONITOR"}
    payload.update(over)
    return payload


def _orders_by_ref(c):
    out = {}
    for o in c.broker.get_current_orders(None):
        out.setdefault(o.get("customerOrderRef") or "", []).append(o)
    return out


def _state(c, table):
    raw = c.db.get_settings().get(sl_tp_close.CLOSE_KEY_PREFIX + table.meta["customer_ref"])
    return json.loads(raw) if raw else None


def _outcomes(c):
    topics = (sl_tp_close.SL_TP_CLOSE_ROUTED, sl_tp_close.SL_TP_CLOSE_NOT_EXECUTED, sl_tp_close.SL_TP_CLOSE_REJECTED,
              sl_tp_close.SL_TP_CLOSE_SKIPPED, sl_tp_close.SL_TP_CLOSE_UNCERTAIN)
    return [(t, p) for t, p in c.bus.events if t in topics]


# ------------------------------------------------------------------ scenario multi-tavolo Roserpina, 100 EUR
def test_sl_del_tavolo_a_chiude_solo_a_b_e_c_restano_intatti(tmp_path) -> None:
    c = _chain(tmp_path)
    a, b, tc = _three_tables(c)
    before = _orders_by_ref(c)
    assert before[a.meta["customer_ref"]][0]["sizeMatched"] == 4.0
    assert before[b.meta["customer_ref"]][0]["sizeMatched"] == 4.0
    assert before[tc.meta["customer_ref"]][0]["sizeMatched"] == 0.0

    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(a))

    reqs = c.bus.payloads("REQ_EXECUTE_CASHOUT")
    assert len(reqs) == 1, reqs
    assert reqs[0]["side"] == "LAY" and str(reqs[0]["selection_id"]) == str(SEL_A)
    assert reqs[0]["stake"] == pytest.approx(4.0 * 2.0 / 2.02, abs=0.01), reqs[0]   # hedge della SOLA posizione A
    after = _orders_by_ref(c)
    assert after[b.meta["customer_ref"]] == before[b.meta["customer_ref"]], "tavolo B toccato"
    assert after[tc.meta["customer_ref"]] == before[tc.meta["customer_ref"]], "tavolo C (non abbinato) cancellato"
    lays = [o for o in c.broker.get_current_orders(None) if o["side"] == "LAY"]
    assert len(lays) == 1 and lays[0]["priceSize"]["size"] == pytest.approx(reqs[0]["stake"], abs=0.01)
    assert [t for t, _ in _outcomes(c)] == [sl_tp_close.SL_TP_CLOSE_ROUTED]
    assert _state(c, a)["state"] == sl_tp_close.STATE_ROUTED
    assert _state(c, b) is None and _state(c, tc) is None


def test_scatto_ripetuto_e_tp_dopo_sl_non_producono_un_secondo_cashout(tmp_path) -> None:
    c = _chain(tmp_path)
    a, _b, _c = _three_tables(c)
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(a))
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(a))
    c.bus.publish(sl_tp_close.TP_TRIGGERED, _trigger(a, sl_tp_close.TP_TRIGGERED, pnl=2.0))

    assert len(c.bus.payloads("REQ_EXECUTE_CASHOUT")) == 1
    outs = _outcomes(c)
    assert [t for t, _ in outs] == [sl_tp_close.SL_TP_CLOSE_ROUTED, sl_tp_close.SL_TP_CLOSE_SKIPPED,
                                    sl_tp_close.SL_TP_CLOSE_SKIPPED]
    assert outs[1][1]["detail"] == "ALREADY_ROUTED"


def test_sl_e_tp_concorrenti_sulla_stessa_posizione_un_solo_cashout(tmp_path) -> None:
    c = _chain(tmp_path)
    a, _b, _c = _three_tables(c)
    start = threading.Barrier(2)

    def fire(topic):
        start.wait()
        c.rc._on_sl_tp_triggered(topic, _trigger(a, topic))

    threads = [threading.Thread(target=fire, args=(t,)) for t in (sl_tp_close.SL_TRIGGERED, sl_tp_close.TP_TRIGGERED)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)

    assert len(c.bus.payloads("REQ_EXECUTE_CASHOUT")) == 1
    assert sorted(t for t, _ in _outcomes(c)) == sorted([sl_tp_close.SL_TP_CLOSE_ROUTED,
                                                         sl_tp_close.SL_TP_CLOSE_SKIPPED])


def test_sl_su_tavolo_solo_non_abbinato_cancella_solo_il_suo_ordine(tmp_path) -> None:
    c = _chain(tmp_path)
    a, b, tc = _three_tables(c)
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(tc))

    assert c.bus.payloads("REQ_EXECUTE_CASHOUT") == []
    left = _orders_by_ref(c)
    assert tc.meta["customer_ref"] not in left, "resting del tavolo C non cancellato"
    assert left[a.meta["customer_ref"]][0]["sizeMatched"] == 4.0 and left[b.meta["customer_ref"]]
    assert _outcomes(c)[-1][0] == sl_tp_close.SL_TP_CLOSE_NOT_EXECUTED


# ------------------------------------------------------------------ riavvio / crash in ogni fase
def _restart(c, tmp_path):
    """Nuovo processo sullo STESSO file SQLite e sullo stesso exchange; il tavolo e' ripristinato come da F018."""
    c2 = _chain(tmp_path, broker=c.broker)
    for t in c.rc.table_manager.active_tables():
        c2.rc.table_manager.restore_table(c.rc.table_manager.snapshot_table(t.table_id))
    return c2


def test_dopo_un_riavvio_lo_stesso_scatto_non_rifa_il_cashout(tmp_path) -> None:
    c = _chain(tmp_path)
    a, _b, _c = _three_tables(c)
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(a))
    c2 = _restart(c, tmp_path)

    c2.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(c2.rc.table_manager.get_table(a.table_id)))

    assert c2.bus.payloads("REQ_EXECUTE_CASHOUT") == []
    assert _outcomes(c2)[-1][1]["detail"] == "ALREADY_ROUTED"
    assert len([o for o in c.broker.get_current_orders(None) if o["side"] == "LAY"]) == 1


@pytest.mark.parametrize("stato", [sl_tp_close.STATE_CLAIMED, sl_tp_close.STATE_UNCERTAIN, "???"])
def test_block_crash_dopo_la_rivendicazione_nessun_reinvio(tmp_path, stato: str) -> None:
    """Crash fra rivendicazione e instradamento, o durante l'instradamento: lo stato resta CLAIMED/UNCERTAIN (o
    illeggibile). Al riavvio nessun cashout: l'esito va riconciliato, mai reinviato alla cieca."""
    c = _chain(tmp_path)
    a, _b, _c = _three_tables(c)
    trig, _ = sl_tp_close.parse_trigger(sl_tp_close.SL_TRIGGERED, _trigger(a))
    raw = sl_tp_close.state_record(trig, stato, bet_ids=["x"]) if stato != "???" else "{rotto"
    c.db.save_settings({trig.close_key: raw})
    c2 = _restart(c, tmp_path)

    c2.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(c2.rc.table_manager.get_table(a.table_id)))

    assert c2.bus.payloads("REQ_EXECUTE_CASHOUT") == []
    assert _outcomes(c2)[-1][0] == sl_tp_close.SL_TP_CLOSE_SKIPPED


def test_errore_dopo_la_pubblicazione_dell_hedge_e_incerto_e_non_si_reinvia(tmp_path, monkeypatch) -> None:
    c = _chain(tmp_path)
    a, _b, _c = _three_tables(c)

    def boom(*_a, **_k):
        raise RuntimeError("crash dopo l'hedge")

    monkeypatch.setattr(cashout_router.CashoutRouter, "_flatten_pure_resting", boom)
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(a))
    monkeypatch.undo()
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(a))

    assert len(c.bus.payloads("REQ_EXECUTE_CASHOUT")) == 1
    outs = _outcomes(c)
    assert outs[0][0] == sl_tp_close.SL_TP_CLOSE_UNCERTAIN and outs[0][1]["detail"].startswith("ROUTE_ERROR")
    assert outs[1][0] == sl_tp_close.SL_TP_CLOSE_SKIPPED and outs[1][1]["detail"] == "ALREADY_UNCERTAIN"


def test_block_rivendicazione_non_persistita_nessun_ordine(tmp_path, monkeypatch) -> None:
    c = _chain(tmp_path)
    a, _b, _c = _three_tables(c)

    def fail(*_a, **_k):
        raise OSError("disco pieno")

    monkeypatch.setattr(c.db, "claim_setting_once", fail)
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(a))

    assert c.bus.payloads("REQ_EXECUTE_CASHOUT") == []
    assert _outcomes(c)[-1][1]["detail"].startswith("CLAIM_NOT_PERSISTED")


def test_stato_finale_non_persistito_resta_rivendicato_e_non_si_reinvia(tmp_path, monkeypatch) -> None:
    c = _chain(tmp_path)
    a, _b, _c = _three_tables(c)
    real = c.db.save_settings

    def fail_on_close(data):
        if any(k.startswith(sl_tp_close.CLOSE_KEY_PREFIX) for k in data):
            raise OSError("disco pieno")
        return real(data)

    monkeypatch.setattr(c.db, "save_settings", fail_on_close)
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(a))
    monkeypatch.undo()
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(a))

    assert len(c.bus.payloads("REQ_EXECUTE_CASHOUT")) == 1
    assert _state(c, a)["state"] == sl_tp_close.STATE_CLAIMED
    assert _outcomes(c)[-1][1]["detail"] == "ALREADY_CLAIMED"


# ------------------------------------------------------------------ mercato, gate, esiti incerti
@pytest.mark.parametrize("status", ["SUSPENDED", "CLOSED"])
def test_mercato_sospeso_o_chiuso_nessun_hedge_e_nessun_retry_automatico(tmp_path, status: str) -> None:
    """PF-EXEC-05: mercato sospeso / settlement durante la chiusura => nessun hedge, NOT_EXECUTED, nessun retry da
    solo. Un NUOVO scatto a mercato riaperto chiude (una sola volta)."""
    c = _chain(tmp_path)
    a, b, _c = _three_tables(c)
    _book(c.broker, status)
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(a))

    assert c.bus.payloads("REQ_EXECUTE_CASHOUT") == []
    assert _outcomes(c)[-1][0] == sl_tp_close.SL_TP_CLOSE_NOT_EXECUTED
    assert _state(c, a)["state"] == sl_tp_close.STATE_NOT_EXECUTED
    if status == "SUSPENDED":
        _book(c.broker)
        c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(a))
        c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(a))
        assert len(c.bus.payloads("REQ_EXECUTE_CASHOUT")) == 1
        assert _orders_by_ref(c)[b.meta["customer_ref"]][0]["sizeMatched"] == 4.0


def test_profondita_insufficiente_nessun_hedge_parziale(tmp_path) -> None:
    c = _chain(tmp_path)
    a, _b, _c = _three_tables(c)
    _book(c.broker, lay=(2.02, 1.0))                     # 1 EUR al prezzo di chiusura, hedge ~3,96
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(a))

    assert c.bus.payloads("REQ_EXECUTE_CASHOUT") == []
    assert _outcomes(c)[-1][0] == sl_tp_close.SL_TP_CLOSE_NOT_EXECUTED


def test_emergency_stop_durante_la_chiusura_nessun_ordine(tmp_path) -> None:
    c = _chain(tmp_path)
    a, _b, _c = _three_tables(c)
    c.rc._emergency_stopped = True
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(a))

    assert c.bus.payloads("REQ_EXECUTE_CASHOUT") == []
    out = _outcomes(c)[-1]
    assert out[0] == sl_tp_close.SL_TP_CLOSE_NOT_EXECUTED and out[1]["detail"].startswith(
        "GATE_REJECTED:emergency_stop_active")


@pytest.mark.parametrize("status,bet,motivo", [
    ("AMBIGUOUS", "B1", "ORDER_STATE_UNCERTAIN:AMBIGUOUS"),
    ("INFLIGHT", "", "ORDER_STATE_UNCERTAIN:INFLIGHT"),
    ("SUBMITTED", "", "ORDER_STATE_UNCERTAIN:SUBMITTED:NO_BET_ID"),       # betId mancante
])
def test_block_ordine_del_tavolo_incerto_prima_la_riconciliazione(tmp_path, status, bet, motivo) -> None:
    c = _chain(tmp_path)
    a, _b, _c = _three_tables(c)
    resp = {"status": "SUCCESS", "instructionReports": [{"betId": bet}]} if bet else None
    c.db.insert_order({"customer_ref": a.meta["customer_ref"], "correlation_id": "x", "status": status,
                       "payload": {"market_id": MKT, "selection_id": SEL_A, "bet_type": "BACK"}, "response": resp})
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(a))

    assert c.bus.payloads("REQ_EXECUTE_CASHOUT") == []
    assert _outcomes(c)[-1][1]["detail"] == motivo
    assert _state(c, a) is None, "rivendicata una chiusura non verificabile"


# ------------------------------------------------------------------ contratto dell'evento e identita' del tavolo
@pytest.mark.parametrize("over,motivo", [
    ({"reason": "TAKE_PROFIT_REACHED"}, "REASON_MISMATCH"),
    ({"source": "TELEGRAM"}, "SOURCE_INVALID"),
    ({"table_id": True}, "TABLE_ID_INVALID"),
    ({"table_id": "1"}, "TABLE_ID_INVALID"),
    ({"market_id": ""}, "MARKET_SELECTION_INVALID"),
    ({"selection_id": None}, "MARKET_SELECTION_INVALID"),
    ({"side": "back"}, "SIDE_INVALID"),
    ({"customer_ref": ""}, "CUSTOMER_REF_MISSING"),
    ({"pnl": float("nan")}, "PNL_INVALID"),
    ({"pnl": None}, "PNL_INVALID"),
])
def test_block_evento_non_conforme_rifiutato(tmp_path, over, motivo: str) -> None:
    c = _chain(tmp_path)
    a, _b, _c = _three_tables(c)
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(a, **over))

    assert c.bus.payloads("REQ_EXECUTE_CASHOUT") == []
    out = _outcomes(c)[-1]
    assert out[0] == sl_tp_close.SL_TP_CLOSE_REJECTED and out[1]["detail"] == f"PAYLOAD_INVALID:{motivo}"


@pytest.mark.parametrize("mutate,motivo", [
    (lambda a, b: {"table_id": b.table_id}, "TABLE_REF_MISMATCH"),                 # mai per tavolo sbagliato
    (lambda a, b: {"table_id": 99}, "TABLE_NOT_FOUND"),
    (lambda a, b: {"selection_id": SEL_B}, "TABLE_SELECTION_MISMATCH"),
    (lambda a, b: {"market_id": "1.999"}, "TABLE_MARKET_MISMATCH"),
    (lambda a, b: {"side": "LAY"}, "TABLE_SIDE_MISMATCH"),
])
def test_block_identita_del_tavolo_non_verificabile(tmp_path, mutate, motivo: str) -> None:
    c = _chain(tmp_path)
    a, b, _c = _three_tables(c)
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(a, **mutate(a, b)))

    assert c.bus.payloads("REQ_EXECUTE_CASHOUT") == []
    assert _outcomes(c)[-1][1]["detail"] == motivo


def test_block_cashout_table_esterno_rifiutato(tmp_path) -> None:
    c = _chain(tmp_path)
    a, _b, _c = _three_tables(c)
    bet = c.db.find_orders_by_customer_ref(a.meta["customer_ref"])[0]["bet_id"]
    c.bus.publish("SIGNAL_RECEIVED", {"signal_type": "CASHOUT_TABLE", "market_id": MKT, "selection_id": SEL_A,
                                      "side": "BACK", "bet_ids": [bet], "sl_tp_route_token": "sltp-forgiato"})

    assert c.bus.payloads("REQ_EXECUTE_CASHOUT") == []
    assert c.bus.payloads("SIGNAL_REJECTED")[-1]["reason"] == "cashout_table_internal_only"


def test_cashout_manuale_della_partita_resta_per_evento(tmp_path) -> None:
    """Il CASHOUT manuale (per partita) non cambia: chiude A e B e cancella il resting di C."""
    c = _chain(tmp_path)
    _three_tables(c)
    c.bus.publish("SIGNAL_RECEIVED", {"signal_type": "CASHOUT", "event_name": "Alpha v Beta"})

    reqs = c.bus.payloads("REQ_EXECUTE_CASHOUT")
    assert sorted(str(r["selection_id"]) for r in reqs) == [str(SEL_A), str(SEL_B)]
    assert c.broker.get_current_orders(None) and not [o for o in c.broker.get_current_orders(None)
                                                      if o["selectionId"] == SEL_C]


# ------------------------------------------------------------------ rivendicazione atomica fra processi
def test_claim_setting_once_vale_una_volta_anche_fra_due_connessioni(tmp_path) -> None:
    db1, db2 = Database(str(tmp_path / "x.db")), Database(str(tmp_path / "x.db"))
    assert db1.claim_setting_once("risk.sl_tp_close.r", "uno") is True
    assert db2.claim_setting_once("risk.sl_tp_close.r", "due") is False
    assert db2.get_settings()["risk.sl_tp_close.r"] == "uno"


# ------------------------------------------------------------------ router: ambito del tavolo
def _router(bot, current, published):
    return cashout_router.CashoutRouter(
        fetch_current_orders=lambda: current, fetch_bot_orders=lambda: bot,
        fetch_market_book=lambda _m: None, cancel_orders=lambda *_a, **_k: True,
        publish=lambda t, p=None: published.append((t, p)), commission_pct=4.5, source="AUTO_MONITOR")


@pytest.mark.parametrize("over,motivo", [
    ({"market_id": ""}, "MARKET_MISSING"),
    ({"selection_id": True}, "SELECTION_MISSING"),
    ({"selection_id": 0}, "SELECTION_MISSING"),
    ({"side": "X"}, "SIDE_INVALID"),
    ({"bet_ids": []}, "BET_IDS_MISSING"),
    ({"bet_ids": "B1"}, "BET_IDS_MISSING"),
    ({"bet_ids": ["B1", "B1"]}, "BET_IDS_INVALID"),
    ({"bet_ids": [True]}, "BET_IDS_INVALID"),
    ({"bet_ids": [""]}, "BET_IDS_INVALID"),
    ({"bet_ids": ["B9"]}, "BET_IDS_NOT_BOT"),
])
def test_block_router_ambito_del_tavolo_non_verificabile(over, motivo: str) -> None:
    published = []
    sig = {"signal_type": "CASHOUT_TABLE", "market_id": MKT, "selection_id": SEL_A, "side": "BACK",
           "bet_ids": ["B1"], **over}
    out = _router([{"bet_id": "B1", "market_id": MKT, "event_name": "x"}], [], published).route(sig)

    assert out["published"] == 0 and out["reason"] == f"table_scope_invalid:{motivo}"
    assert published[0][0] == "CASHOUT_FAILED" and published[0][1]["status"] == "REJECTED"


def test_block_router_lato_della_posizione_diverso_dal_tavolo() -> None:
    published = []
    current = [{"betId": "B1", "marketId": MKT, "selectionId": SEL_A, "side": "LAY", "sizeMatched": 4.0,
                "averagePriceMatched": 2.0, "sizeRemaining": 0.0, "status": "EXECUTION_COMPLETE",
                "priceSize": {"price": 2.0, "size": 4.0}}]
    sig = {"signal_type": "CASHOUT_TABLE", "market_id": MKT, "selection_id": SEL_A, "side": "BACK",
           "bet_ids": ["B1"]}
    out = _router([{"bet_id": "B1", "market_id": MKT, "event_name": "x"}], current, published).route(sig)

    assert out["reason"] == "table_scope_invalid:SIDE_MISMATCH"
    assert [t for t, _ in published] == ["CASHOUT_FAILED"]


# ------------------------------------------------------------------ chiusure di casi limite (mutazioni sopravvissute)
def test_block_router_chiude_solo_la_selezione_del_tavolo() -> None:
    """Anche se l'elenco dei bet_id contenesse un ordine su un'altra selezione, si chiude solo quella del tavolo."""
    published = []
    row = {"marketId": MKT, "side": "BACK", "sizeMatched": 4.0, "averagePriceMatched": 2.0, "sizeRemaining": 0.0,
           "status": "EXECUTION_COMPLETE", "priceSize": {"price": 2.0, "size": 4.0}}
    current = [{**row, "betId": "B1", "selectionId": SEL_A}, {**row, "betId": "B2", "selectionId": SEL_B}]
    book = {"marketId": MKT, "status": "OPEN", "runners": [
        {"selectionId": s, "status": "ACTIVE", "ex": {"availableToBack": [{"price": 2.0, "size": 500.0}],
                                                       "availableToLay": [{"price": 2.02, "size": 500.0}]}}
        for s in (SEL_A, SEL_B)]}
    bot = [{"bet_id": b, "market_id": MKT, "event_name": "x"} for b in ("B1", "B2")]
    router = cashout_router.CashoutRouter(
        fetch_current_orders=lambda: current, fetch_bot_orders=lambda: bot, fetch_market_book=lambda _m: book,
        cancel_orders=lambda *_a, **_k: True, publish=lambda t, p=None: published.append((t, p)),
        commission_pct=4.5, source="AUTO_MONITOR")

    out = router.route({"signal_type": "CASHOUT_TABLE", "market_id": MKT, "selection_id": SEL_A, "side": "BACK",
                        "bet_ids": ["B1", "B2"]})

    assert out["published"] == 1
    assert [str(p["selection_id"]) for t, p in published if t == "REQ_EXECUTE_CASHOUT"] == [str(SEL_A)]


def test_esito_d_instradamento_assente_e_incerto(tmp_path, monkeypatch) -> None:
    """Instradamento terminato senza alcun esito registrato (ne' rifiuto ne' risultato del router): non si sa se un
    hedge e' partito => UNCERTAIN, mai ritentabile."""
    c = _chain(tmp_path)
    a, _b, _c = _three_tables(c)
    monkeypatch.setattr(RuntimeController, "_route_cashout_signal", lambda self, signal: None)
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(a))

    assert _outcomes(c)[-1][0] == sl_tp_close.SL_TP_CLOSE_UNCERTAIN
    assert _state(c, a)["state"] == sl_tp_close.STATE_UNCERTAIN


def test_crash_durante_il_secondo_tentativo_resta_rivendicato(tmp_path, monkeypatch) -> None:
    """NOT_EXECUTED (mercato sospeso) -> mercato riaperto -> nuovo scatto: la ri-rivendicazione e' persistita PRIMA
    dell'instradamento. Se il processo cade prima dello stato finale, la chiusura resta CLAIMED: nessun secondo
    hedge al riavvio."""
    c = _chain(tmp_path)
    a, _b, _c = _three_tables(c)
    _book(c.broker, "SUSPENDED")
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(a))
    _book(c.broker)
    real = c.db.save_settings

    def crash_on_final(data):
        if any(k.startswith(sl_tp_close.CLOSE_KEY_PREFIX) for k in data):
            raise OSError("processo caduto")
        return real(data)

    monkeypatch.setattr(c.db, "save_settings", crash_on_final)
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(a))
    monkeypatch.undo()
    c2 = _restart(c, tmp_path)
    c2.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(c2.rc.table_manager.get_table(a.table_id)))

    assert len(c.bus.payloads("REQ_EXECUTE_CASHOUT")) == 1 and c2.bus.payloads("REQ_EXECUTE_CASHOUT") == []
    assert _outcomes(c2)[-1][1]["detail"] == "ALREADY_CLAIMED"


def test_ri_rivendicazione_concorrente_un_solo_vincitore(tmp_path, monkeypatch) -> None:
    """Due processi leggono entrambi NOT_EXECUTED: il compare-and-set fa vincere solo il primo."""
    c = _chain(tmp_path)
    a, _b, _c = _three_tables(c)
    _book(c.broker, "SUSPENDED")
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(a))
    _book(c.broker)
    stale = c.db.get_settings()                                  # lettura fatta PRIMA che l'altro processo vinca
    trig, _ = sl_tp_close.parse_trigger(sl_tp_close.SL_TRIGGERED, _trigger(a))
    assert c.db.claim_setting_once(trig.close_key, sl_tp_close.state_record(trig, "CLAIMED", bet_ids=[]),
                                   replace_if=stale[trig.close_key]) is True        # l'altro processo
    monkeypatch.setattr(c.db, "get_settings", lambda: stale)

    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(a))

    assert c.bus.payloads("REQ_EXECUTE_CASHOUT") == []
    assert _outcomes(c)[-1][1]["detail"] == "ALREADY_CLAIMED_CONCURRENTLY"


def test_claim_compare_and_set(tmp_path) -> None:
    db = Database(str(tmp_path / "y.db"))
    assert db.claim_setting_once("k", "a", replace_if="a") is False                 # assente: niente da sostituire
    assert db.claim_setting_once("k", "a") is True
    assert db.claim_setting_once("k", "b", replace_if="z") is False
    assert db.get_settings()["k"] == "a"
    assert db.claim_setting_once("k", "b", replace_if="a") is True
    assert db.get_settings()["k"] == "b"


def test_record_di_chiusura_lega_il_tavolo_alla_riga_dell_hedge(tmp_path) -> None:
    """Richiesta Claude C (MCP-01k): il record `risk.sl_tp_close.*` porta i `cashout_request_id` delle richieste d'hedge,
    gli stessi scritti dal CashoutExecutor nella riga `orders` dell'hedge: la Control API attribuisce l'hedge al tavolo."""
    c = _chain(tmp_path)
    a, _b, _c = _three_tables(c)
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(a))

    rec = _state(c, a)
    req = c.bus.payloads("REQ_EXECUTE_CASHOUT")[0]
    assert rec["cashout_request_ids"] == [req["cashout_request_id"]]
    assert _outcomes(c)[-1][1]["cashout_request_ids"] == [req["cashout_request_id"]]
    rows = c.db._execute("SELECT payload_json FROM orders", fetch=True, commit=False)
    hedge = [json.loads(r["payload_json"]) for r in rows
             if json.loads(r["payload_json"]).get("cashout_request_id") == req["cashout_request_id"]]
    assert len(hedge) == 1, rows


def test_chiusura_non_eseguita_non_ha_richieste(tmp_path) -> None:
    c = _chain(tmp_path)
    a, _b, _c = _three_tables(c)
    _book(c.broker, "SUSPENDED")
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(a))
    assert _state(c, a)["cashout_request_ids"] == []
