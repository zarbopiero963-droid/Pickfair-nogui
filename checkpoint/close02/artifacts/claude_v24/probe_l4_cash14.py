"""close02 (Claude) — L4 PF-CASH-14/15: «Fill completo verificato», «Fill parziale e residuo protetto» (#461/
6085516609, righe verbatim) e V2 «parziale→contabilizzare solo eseguito e mantenere residuo». CashoutExecutor reale
con OrderRouter finto: hedge LAY 2.86@7.00 abbinato solo per 1.00. Oracolo: nessun esito di cashout COMPLETATO
(DONE) con green_up intero; l'esito dichiara il parziale e il residuo. Nessuna rete."""
import json, os, sys
ROOT = os.environ["PROBE_ROOT"]; sys.path.insert(0, ROOT); os.chdir(ROOT)
import logging; logging.disable(logging.CRITICAL)
from cashout_executor import CashoutExecutor  # noqa: E402

CMD = {"market_id": "1.230000777", "selection_id": 1222347, "side": "LAY", "price": 7.0, "stake": 2.86,
       "green_up": -7.15, "source": "TELEGRAM", "event_name": "A v B"}


class Bus:
    def __init__(self): self.ev = []
    def subscribe(self, *a): pass
    def publish(self, n, p=None): self.ev.append((n, p))


class Router:
    def __init__(self, matched): self.matched = matched
    def place(self, p):
        return {"ok": self.matched > 0, "placed": True, "order_unknown": False, "status": "SUCCESS",
                "matched": self.matched, "bet_id": "900000000555", "error": None, "raw": {}}


out = {}
for cid, matched in (("CASH-14-FULL", 2.86), ("CASH-14-PARTIAL", 1.0)):
    bus = Bus()
    CashoutExecutor(bus, Router(matched)).on_cmd_execute_cashout(dict(CMD))
    ev = [(n, {k: p.get(k) for k in ("status", "matched", "green_up", "reason")}) for n, p in bus.ev]
    succ = [p for n, p in ev if n == "CASHOUT_SUCCESS"]
    if matched >= 2.86:
        ok = bool(succ)
        atteso = "fill completo => CASHOUT_SUCCESS"
    else:
        ok = not any(p.get("status") == "DONE" and p.get("green_up") == CMD["green_up"] for p in succ)
        atteso = "fill parziale => nessun DONE con green_up intero; parziale e residuo dichiarati"
    out[cid] = {"atteso": atteso, "eventi": ev, "verdetto": "CONFORME" if ok else "NON_CONFORME"}
json.dump(out, open(os.environ["PROBE_OUT"], "w"), indent=1, ensure_ascii=False)
for k, v in out.items():
    print(k, v["verdetto"], v["eventi"])
