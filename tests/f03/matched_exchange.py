"""D-C4-02: exchange finto per i test unitari del monitor SL/TP che costruiscono un RuntimeController senza DB ne'
exchange. Il monitor ora decide solo sulla posizione ABBINATA confermata (righe `orders` del tavolo + listCurrentOrders):
questo helper dichiara la posizione del tavolo interamente abbinata allo stake e al prezzo d'ingresso del meta, cioe'
l'ipotesi che quei test facevano implicitamente. Nessuna asserzione dei test cambia."""
from __future__ import annotations

from types import SimpleNamespace

from core import sl_tp_pnl


def fully_matched(rc, tables, service=None):
    db_rows, exch_rows = {}, []
    for t in tables:
        meta = t.meta
        ref = meta.setdefault("customer_ref", f"r{t.table_id}")
        side = str(meta.get("bet_type") or "BACK").upper()
        stake = sl_tp_pnl.position_stake(side, meta, t.current_exposure)
        bet = f"B{t.table_id}"
        db_rows.setdefault(ref, []).append({"status": "SUBMITTED", "bet_id": bet, "payload": {
            "market_id": str(t.market_id), "selection_id": str(t.selection_id), "bet_type": side}})
        if stake is not None:
            exch_rows.append({"betId": bet, "marketId": str(t.market_id), "sizeMatched": float(stake),
                              "averagePriceMatched": meta.get("price"), "sizeRemaining": 0.0})
    rc.db = SimpleNamespace(find_orders_by_customer_ref=lambda ref: list(db_rows.get(ref, [])))
    svc = service if service is not None else SimpleNamespace()
    svc.list_current_orders = lambda market_ids=None: [r for r in exch_rows
                                                       if not market_ids or r["marketId"] in market_ids]
    rc.betfair_service = svc
    return rc
