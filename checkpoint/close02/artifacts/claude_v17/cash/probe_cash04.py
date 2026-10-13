"""close02 (Claude) — sonda PF-CASH-04 «Ordini correnti e residui» + V3 (unmatched/ambiguous: tavolo non chiuso,
rischio non azzerato) sul CashoutRouter reale dell'albero PROBE_ROOT, con dipendenze iniettate (nessuna rete).
Fonte: #461/6085516609 (sha256 56fc7836...). Fixture del caso Under 1.5 HT: BACK 10.00@2.00 matched, mercato
1.230000777, selezione 1222347; book con LAY eseguibile a 7.00."""
import json
import os
import sys

ROOT = os.environ["PROBE_ROOT"]
sys.path.insert(0, ROOT)
os.chdir(ROOT)
from cashout_router import CashoutRouter  # noqa: E402

M, SEL, EV = "1.230000777", 1222347, "Squadra A v Squadra B"
BOOK = {"marketId": M, "status": "OPEN", "inplay": True,
        "runners": [{"selectionId": SEL, "ex": {"availableToLay": [{"price": 7.0, "size": 100.0}],
                                                "availableToBack": [{"price": 6.8, "size": 100.0}]}}]}
MATCHED = {"betId": "B1", "marketId": M, "selectionId": SEL, "side": "BACK", "sizeMatched": 10.0,
           "sizeRemaining": 0.0, "averagePriceMatched": 2.0, "priceSize": {"price": 2.0, "size": 10.0}}
RESTING = {"betId": "B2", "marketId": M, "selectionId": SEL, "side": "BACK", "sizeMatched": 0.0,
           "sizeRemaining": 2.0, "averagePriceMatched": 0.0, "priceSize": {"price": 2.1, "size": 2.0}}
BOT = [{"bet_id": "B1", "market_id": M, "event_name": EV}, {"bet_id": "B2", "market_id": M, "event_name": EV}]
SIGNAL = {"signal_type": "CASHOUT", "event_name": EV, "market_id": M}


def run(current, bot=BOT, cancel_result=None, cancel_raises=False):
    events, cancels = [], []

    def cancel(market_id, ids):
        cancels.append((market_id, list(ids)))
        if cancel_raises:
            raise RuntimeError("cancel non disponibile")
        return cancel_result if cancel_result is not None else {"status": "SUCCESS",
                                                                "instructionReports": [{"status": "SUCCESS"}]}

    r = CashoutRouter(fetch_current_orders=current, fetch_bot_orders=lambda: list(bot),
                      fetch_market_book=lambda m: dict(BOOK), cancel_orders=cancel,
                      publish=lambda n, p: events.append((n, p)))
    summary = r.route(dict(SIGNAL))
    hedges = [p for n, p in events if n == "REQ_EXECUTE_CASHOUT"]
    fails = [p for n, p in events if n == "CASHOUT_FAILED"]
    return {"summary": summary, "cancels": cancels, "hedges": hedges, "fails": fails,
            "eventi": [n for n, _ in events]}


out = {}

# R1: matched 10 + residuo unmatched 2 sulla stessa selezione => il residuo e' considerato (cancel confermato
# PRIMA dell'hedge), l'hedge copre solo il matched 10 (20/7), il residuo non e' trattato come chiuso.
o = run(lambda: [dict(MATCHED), dict(RESTING)])
stake = (o["hedges"] or [{}])[0].get("stake")
ok = (o["cancels"] == [(M, ["B2"])] and len(o["hedges"]) == 1 and stake is not None
      and abs(float(stake) - 20.0 / 7.0) < 0.01)
out["CASH-04-R1"] = {"atteso": "cancel del residuo B2 confermato prima dell'hedge; hedge solo sul matched (20/7)",
                     "osservato": o, "verdetto": "CONFORME" if ok else "NON_CONFORME"}

# R1-neg: residuo il cui cancel NON e' confermato => nessun hedge (residuo non trattato come chiuso)
o = run(lambda: [dict(MATCHED), dict(RESTING)], cancel_result={"ok": False, "error": "x"})
out["CASH-04-R1-cancel-ko"] = {"atteso": "cancel del residuo non confermato => nessun hedge pubblicato",
                               "osservato": o, "verdetto": "CONFORME" if not o["hedges"] else "NON_CONFORME"}

# R2: listCurrentOrders non disponibile => nessun hedge, CASHOUT_FAILED
def boom():
    raise RuntimeError("listCurrentOrders: errore")
o = run(boom)
out["CASH-04-R2"] = {"atteso": "nessun hedge; CASHOUT_FAILED (fail-closed)", "osservato": o,
                     "verdetto": "CONFORME" if (not o["hedges"] and o["fails"]) else "NON_CONFORME"}

# R3: ordine ambiguo BACK 1.00@2.20 (timeout dopo invio): nessun betId, assente da listCurrentOrders, presente solo
# come intento locale AMBIGUOUS. Oracolo: l'ambiguo e' conteggiato fra gli ordini dello scope; tavolo non chiuso.
# Il router riceve solo ordini del bot con betId (fetch_bot_orders) e ordini correnti exchange: l'ambiguo senza
# betId non e' rappresentabile. Si osserva se il router procede ugualmente all'hedge del solo matched.
bot_with_ambiguous = BOT + [{"bet_id": "", "customer_ref": "sig-ambiguo", "market_id": M, "event_name": EV,
                             "status": "AMBIGUOUS", "side": "BACK", "size": 1.0, "price": 2.2}]
o = run(lambda: [dict(MATCHED)], bot=bot_with_ambiguous)
out["CASH-04-R3"] = {"atteso": "ordine ambiguo nello scope => nessuna chiusura dichiarata sul solo matched (hedge "
                                "sospeso o ambiguo conteggiato)",
                     "osservato": o, "verdetto": "CONFORME" if not o["hedges"] else "NON_CONFORME",
                     "nota": "l'ambiguo senza betId non entra nell'identita' del router (cfr. L06-F1)"}

json.dump(out, open(os.environ["PROBE_OUT"], "w"), indent=1, ensure_ascii=False, default=str)
for k, v in out.items():
    print(k, v["verdetto"], "hedges", len(v["osservato"]["hedges"]), "cancels", v["osservato"]["cancels"],
          "fails", len(v["osservato"]["fails"]))
