"""Prova cumulativa F-02 + F-04 + F-03 (#586, mandato owner 05/11/2026), catena reale end-to-end.

Il poller SL/TP di F-02 (`_sl_tp_poll_once`) legge il book dal servizio (broker SIM), il monitor pubblica gli scatti
(F-02 SL/TP, F-03 trailing), F-04 li consuma, il CashoutRouter costruisce l'hedge e la catena cashout cablata come
headless lo esegue sul SimulationBroker. Quattro tavoli sulla stessa partita e sullo stesso mercato:
A SL 10 %, B TP 10 %, C trailing 20 %, D nessuna protezione. Quote SINTETICHE.

Verifica: lo SL scatta, il TP scatta, il trailing aggiorna la propria soglia e scatta, il router chiude la posizione
giusta (una per tavolo), D resta aperto e intatto, un crash con riavvio non duplica nessun hedge.
"""
from __future__ import annotations

from decimal import Decimal as D

from core import sl_tp_close, trailing_stop
from tests.acceptance.test_f04_sl_tp_table_close import MKT, _chain

SEL = {"A": 11, "B": 12, "C": 13, "D": 14}


def _book(broker, lay: dict, status="OPEN"):
    runners = [{"selectionId": s, "status": "ACTIVE", "ex": {
        "availableToBack": [{"price": 2.0, "size": 500.0}],
        "availableToLay": [{"price": lay.get(s, 2.02), "size": 500.0}]}} for s in SEL.values()]
    broker.update_market_book(MKT, {"marketId": MKT, "status": status, "inplay": True, "runners": runners})


def _enter(c, name, i, **protezioni):
    c.rc._on_signal_received({"market_id": MKT, "selection_id": SEL[name], "price": 2.0, "bet_type": "BACK",
                              "simulation_mode": True, "chat_id": -100123,
                              "received_at": f"2026-11-05T12:00:0{i}.000001+00:00",
                              "event": "Alpha v Beta", "market": "Match Odds", "selection": name, **protezioni})


def _live(c, sel):
    return next(t for t in c.rc.table_manager._tables.values() if str(t.selection_id) == str(sel) and t.status == "ACTIVE")


def _setup(tmp_path):
    c = _chain(tmp_path)
    _book(c.broker, {})
    _enter(c, "A", 0, stop_loss_pct=10)
    _enter(c, "B", 1, take_profit_pct=10)
    _enter(c, "C", 2, trailing_stop_pct=20)
    _enter(c, "D", 3)
    assert len(c.rc.table_manager.active_tables()) == 4
    return c


def _tick(c, lay):
    _book(c.broker, lay)
    return c.rc._sl_tp_poll_once()                       # il poller reale di F-02 valuta il mercato


def _hedges(c):
    return sorted((o["selectionId"], round(o["priceSize"]["size"], 2)) for o in c.broker.get_current_orders(None)
                  if o["side"] == "LAY")


def _triggers(c):
    return [(t, p["table_id"]) for t, p in c.bus.events
            if t in ("SL_TRIGGERED", "TP_TRIGGERED", trailing_stop.TRAILING_TRIGGERED)]


def test_sl_tp_trailing_chiudono_solo_il_proprio_tavolo_e_un_crash_non_duplica(tmp_path) -> None:
    c = _setup(tmp_path)
    a, b, cc, d = (_live(c, SEL[n]) for n in "ABCD")
    d_order = [o for o in c.broker.get_current_orders(None) if o["selectionId"] == SEL["D"]]

    assert _tick(c, {11: 2.02, 12: 1.6, 13: 1.4, 14: 1.4}) == 1   # B: TP (0,96 >= 0,40); C: picco 1,63
    assert _triggers(c) == [("TP_TRIGGERED", b.table_id)]
    soglia_1 = D(str(_live(c, 13).meta["trailing_threshold"]))

    _tick(c, {11: 2.02, 12: 1.6, 13: 1.3, 14: 1.3})       # C: nuovo picco 2,05 => la soglia sale
    soglia_2 = D(str(_live(c, 13).meta["trailing_threshold"]))
    assert soglia_2 > soglia_1, (soglia_1, soglia_2)

    _tick(c, {11: 3.0, 12: 1.6, 13: 1.35, 14: 3.0})       # A: SL (-1,34 <= -0,40); C ~1,84 sopra soglia 1,64
    assert _triggers(c)[-1] == ("SL_TRIGGERED", a.table_id)
    _tick(c, {11: 3.0, 12: 1.6, 13: 1.6, 14: 3.0})        # C: 0,96 <= soglia => trailing
    assert _triggers(c) == [("TP_TRIGGERED", b.table_id), ("SL_TRIGGERED", a.table_id),
                            (trailing_stop.TRAILING_TRIGGERED, cc.table_id)]

    routed = [p for t, p in c.bus.events if t == sl_tp_close.SL_TP_CLOSE_ROUTED]
    assert sorted(p["table_id"] for p in routed) == sorted([a.table_id, b.table_id, cc.table_id])
    hedges = _hedges(c)
    assert [s for s, _ in hedges] == [11, 12, 13], hedges           # una chiusura per tavolo, nessuna su D
    assert [o for o in c.broker.get_current_orders(None) if o["selectionId"] == SEL["D"]] == d_order
    assert _live(c, SEL["D"]).meta.get("sl_tp_triggered") is None

    # crash + riavvio sullo stesso DB e lo stesso exchange: latch in memoria perso, gli scatti si ripetono
    c2 = _chain(tmp_path, broker=c.broker)
    for t in c.rc.table_manager.active_tables():
        snap = c.rc.table_manager.snapshot_table(t.table_id)
        table = dict(snap["table"])
        table["meta"] = {k: v for k, v in (table.get("meta") or {}).items()
                         if k not in ("sl_tp_triggered", "trailing_peak", "trailing_threshold")}
        c2.rc.table_manager.restore_table(dict(snap, table=table))
    _book(c2.broker, {11: 3.0, 12: 1.6, 13: 1.6, 14: 3.0})
    c2.rc._sl_tp_poll_once()

    assert c2.bus.payloads("REQ_EXECUTE_CASHOUT") == [], "hedge duplicato dopo il riavvio"
    skipped = sorted(p["table_id"] for t, p in c2.bus.events if t == sl_tp_close.SL_TP_CLOSE_SKIPPED)
    assert skipped == sorted([a.table_id, b.table_id, cc.table_id])
    assert _hedges(c2) == hedges
