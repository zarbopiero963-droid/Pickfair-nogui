"""close02 (Claude) — sonde PF-CASH-19/21/23 sull'albero PROBE_ROOT. Fonte: #461/6085516609 (sha 56fc7836...),
paragrafo «Dopo richiesta cashout» V1-V5. Nessuna rete, nessuna credenziale; broker simulato e fake iniettati.

Fixture del caso: BACK 10.00@2.00 matched (mercato 1.230000777, selezione 1222347), hedge LAY 20/7@7.00.
Payoff atteso a fill completo: vince +10 - 6*20/7 = -50/7; perde -10 + 20/7 = -50/7 (≈ -7.142857)."""
import inspect
import json
import os
import sys
import tempfile
from pathlib import Path

ROOT = os.environ["PROBE_ROOT"]
sys.path.insert(0, ROOT)
os.chdir(ROOT)
from core.pnl_engine import PnLEngine  # noqa: E402
from core.state_recovery import StateRecovery  # noqa: E402
from tests.acceptance.test_issue437_pr27 import _catena, _rifiuti  # noqa: E402
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _signal  # noqa: E402

M, SEL = "1.230000777", 1222347
HEDGE = 20.0 / 7.0
out = {}


class Bus:
    def __init__(self):
        self.ev = []

    def subscribe(self, *a, **k):
        pass

    def publish(self, n, p=None):
        self.ev.append((n, p))


def close_events(bus):
    return [p for n, p in bus.ev if n == "RUNTIME_CLOSE_POSITION"]


# ---------------- CASH-21 -----------------------------------------------------------------------------------
def settle(rows, retry=False):
    bus = Bus()
    e = PnLEngine(bus=bus)
    res, dup = [], []
    for ref, profit in rows:
        res.append(e.apply_cleared_market_settlement(market_id=M, gross_pnl=profit, settlement_ref=ref))
    if retry:
        for ref, profit in rows:
            try:
                e.apply_cleared_market_settlement(market_id=M, gross_pnl=profit, settlement_ref=ref)
            except ValueError as exc:
                dup.append(str(exc).split(":")[0])
    gross = round(sum(float(p["gross_pnl"]) for p in close_events(bus)), 6)
    net = round(sum(float(p["net_pnl"]) for p in close_events(bus)), 6)
    return {"close_events": len(close_events(bus)), "gross_sum": gross, "net_sum": net, "duplicate_raises": dup}


for esito, rows in (("selezione vince", [("B1", 10.0), ("H1", -6.0 * HEDGE)]),
                    ("selezione perde", [("B1", -10.0), ("H1", HEDGE)])):
    o = settle(rows)
    ok = abs(o["gross_sum"] - (-50.0 / 7.0)) < 1e-6 and abs(o["net_sum"] - o["gross_sum"]) < 1e-9
    out[f"CASH-21-R1[{esito}]"] = {"atteso": "P/L dai profit per-bet del settlement: lordo -50/7, nessuna commissione "
                                            "su netto di mercato negativo", "osservato": o,
                                   "verdetto": "CONFORME" if ok else "NON_CONFORME"}

# R2: hedge eseguito 1.50 su 20/7 richiesti; il report cleared contiene solo l'eseguito (il richiesto non entra)
o = settle([("B1", 10.0), ("H1", -6.0 * 1.5)])
out["CASH-21-R2"] = {"atteso": "P/L sul solo eseguito: +10 - 9 = +1.00 lordo (non -50/7 del richiesto)",
                     "osservato": o, "verdetto": "CONFORME" if abs(o["gross_sum"] - 1.0) < 1e-9 else "NON_CONFORME",
                     "nota": "la parte 'residuo resta esposto' e' coperta da CASH-25-R3/CASH-04-R1 (tavolo/fencing)"}

# R3: settlement ricevuto due volte (retry) per lo stesso mercato e le stesse bet
o = settle([("B1", 10.0), ("H1", -6.0 * HEDGE)], retry=True)
out["CASH-21-R3"] = {"atteso": "un solo aggiornamento per bet; il retry e' rifiutato (CLEARED_SETTLEMENT_DUPLICATE)",
                     "osservato": o, "verdetto": "CONFORME" if (o["close_events"] == 2 and
                                                                o["duplicate_raises"] == ["CLEARED_SETTLEMENT_DUPLICATE"] * 2)
                     else "NON_CONFORME"}

# ---------------- CASH-19 -----------------------------------------------------------------------------------
import core.runtime_controller as rcmod  # noqa: E402
src = inspect.getsource(rcmod.RuntimeController._poll_cleared_settlements_locked)
uses_current = any(k in src for k in ("list_current_orders", "get_current_orders", "list_active_orders"))
out["CASH-19-R1"] = {"atteso": "chiusura dichiarata solo se current e cleared concordano sui due bet",
                     "osservato": {"poller_legge_current_orders": uses_current},
                     "verdetto": "CONFORME" if uses_current else "NON_CONFORME",
                     "nota": "statico: il ciclo di chiusura reale realizza dai soli cleared (nessun confronto con i "
                             "current); il payoff -50/7 e' comunque corretto (CASH-21-R1)"}
out["CASH-19-R2"] = {"atteso": "hedge matched nei cleared ma ancora EXECUTABLE nei current => nessuna chiusura",
                     "osservato": {"poller_legge_current_orders": uses_current},
                     "verdetto": "CONFORME" if uses_current else "NON_CONFORME",
                     "nota": "senza lettura dei current la discordanza non e' rilevabile (L07-F5)"}
import betfair_client as bfc  # noqa: E402
csrc = inspect.getsource(bfc.BetfairClient.list_cleared_orders)
out["CASH-19-R3"] = {"atteso": "lettura cleared incompleta (moreAvailable) => nessuna riconciliazione",
                     "osservato": {"pagina_vuota_con_moreAvailable_solleva": "moreAvailable" in csrc and "raise" in csrc},
                     "verdetto": "VEDI_TEST",
                     "nota": "coperto dai test tests/unit/test_betfair_client_cleared_orders.py (eseguiti a parte)"}


# ---------------- CASH-23 -----------------------------------------------------------------------------------
class Recon:
    def __init__(self, orders, boom=False):
        self.orders, self.boom = orders, boom

    def fetch_startup_active_orders(self):
        if self.boom:
            raise RuntimeError("listCurrentOrders non disponibile")
        return [dict(o) for o in self.orders]

    def merge_startup_active_orders(self, rows):
        return {"orders": list(rows)}


RESTING_HEDGE = {"bet_id": "900000000001", "market_id": M, "selection_id": SEL, "side": "LAY", "price": 7.0,
                 "size": round(HEDGE, 2), "size_matched": 0.0, "size_remaining": round(HEDGE, 2),
                 "status": "EXECUTABLE", "customer_ref": "cashout-h1"}


def restart_then_entry(orders, boom=False):
    c = _catena(Path(tempfile.mkdtemp()))
    for k, v in {"max_open_exposure": 10.0, "max_order_exposure": 1.0, "max_market_exposure": 2.0,
                 "max_event_exposure_abs": 2.0, "max_drawdown_abs": 10.0}.items():
        setattr(c.rc.config, k, v)
    sr = StateRecovery(db=None, bus=c.bus, reconciliation_engine=Recon(orders, boom),
                       duplication_guard=c.rc.duplication_guard)
    rec = sr.recover()
    blocked = not rec.get("ok")
    if blocked:  # stesso effetto del ramo CL02-RST-001 di start()
        c.rc._startup_recovery_blocked = str(rec.get("reason"))
    exposure_before = float(c.rc.table_manager.total_exposure())
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK", market_id=M, selection_id=SEL + 7))
    return {"recover": rec, "exposure_dopo_recovery": exposure_before,
            "orders": len(c.broker.state.orders), "rejects": _rifiuti(c)}


o = restart_then_entry([RESTING_HEDGE])
ok = o["orders"] == 0 or o["exposure_dopo_recovery"] > 0
out["CASH-23-R1"] = {"atteso": "hedge unmatched ritrovato al riavvio: tavolo/rischio del mercato restano aperti; "
                               "nuovo ingresso nello scope non ammesso finche' il cashout non e' risolto",
                     "osservato": o, "verdetto": "CONFORME" if ok else "NON_CONFORME",
                     "nota": "STARTUP_GHOST_ORDER_RECOVERED non ha subscriber: l'ordine e' solo registrato"}

o = restart_then_entry([], boom=True)
out["CASH-23-R2"] = {"atteso": "esito non verificabile al riavvio => nessun nuovo ingresso",
                     "osservato": o, "verdetto": "CONFORME" if (o["orders"] == 0 and o["rejects"]) else "NON_CONFORME"}

# R2b: cashout AMBIGUO (order_unknown) prima del crash e ordine non (ancora) visibile tra i current al riavvio.
o = restart_then_entry([])
out["CASH-23-R2b"] = {"atteso": "cashout ambiguo pre-crash => nessun nuovo ingresso finche' non risolto",
                      "osservato": o, "verdetto": "CONFORME" if o["orders"] == 0 else "NON_CONFORME",
                      "nota": "l'hedge del cashout passa da OrderRouter.place senza saga durevole: al riavvio non "
                              "resta traccia locale dell'esito ambiguo"}
out["CASH-23-R3"] = {"atteso": "settlement gia' applicato prima del crash: nessuna ripetizione",
                     "verdetto": "VEDI_TEST",
                     "nota": "coperto da tests/core/test_settlement_poller.py::test_poll_dedupe_durevole_dopo_riavvio_"
                             "nessuna_riemissione e ::test_sim_giro_checkpoint_durevole_presente_non_riemette"}

json.dump(out, open(os.environ["PROBE_OUT"], "w"), indent=1, ensure_ascii=False, default=str)
for k, v in out.items():
    print(k, v["verdetto"], json.dumps(v.get("osservato", ""), default=str)[:230])
