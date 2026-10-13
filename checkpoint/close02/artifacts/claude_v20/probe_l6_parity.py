"""close02 (Claude) — L6 PARITY tra corsie d'ingresso: segnale runtime (Telegram/headless) vs bet manuale GUI
(DutchingController.manual_bet), stessa catena reale (_catena: RuntimeController + MM vero + SimulationBroker).
Fonte (phase_row_verbatim L6 PARITY, #461/6085510337): «Applicare la stessa regola pertinente a BACK/LAY e
GUI/headless/MCP, separando SIM da LIVE; non confondere stake e liability». Oracolo: in ogni condizione di rischio
le due corsie danno lo STESSO esito (ammesso/bloccato), e l'esito e' quello della regola (owner D1/D2/Q3/Q4,
OWNER-EXPOSURE-01). MCP: corsia inesistente nel codice => NOT VERIFIED. Nessuna rete."""
import json, os, sys, tempfile
from pathlib import Path
ROOT = os.environ["PROBE_ROOT"]; sys.path.insert(0, ROOT); os.chdir(ROOT)
import logging; logging.disable(logging.CRITICAL)
from controllers.dutching_controller import DutchingController  # noqa: E402
from tests.acceptance.test_issue437_pr27 import _catena, _rifiuti  # noqa: E402
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _signal  # noqa: E402

CAPS = {"max_open_exposure": 10.0, "max_order_exposure": 1.0, "max_market_exposure": 2.0,
        "max_event_exposure_abs": 2.0, "max_drawdown_abs": 10.0}
H1 = {"betId": "100000000002", "marketId": "1.230000777", "selectionId": 1222347, "side": "LAY", "handicap": 0.0,
      "priceSize": {"price": 7.0, "size": 2.86}, "sizeMatched": 0.0, "sizeRemaining": 2.86, "status": "EXECUTABLE"}


def chain(setup):
    c = _catena(Path(tempfile.mkdtemp()))
    for k, v in CAPS.items():
        setattr(c.rc.config, k, v)
    if setup:
        setup(c)
    return c


def lane_signal(setup, side, stake, price):
    c = chain(setup)
    c.rc._on_signal_received(_signal(stake=stake, bet_type=side, price=price))
    return {"orders": len(c.broker.state.orders), "reason": (_rifiuti(c) or [""])[0]}


def lane_manual(setup, side, stake, price):
    c = chain(setup)
    r = DutchingController(c.bus, c.rc).manual_bet({"market_id": "1.234", "selection_id": 11, "price": price,
                                                     "stake": stake, "bet_type": side, "event_key": "man-1"})
    return {"orders": len(c.broker.state.orders), "reason": "" if r.get("ok", r.get("success")) else str(r.get("error") or r.get("reason") or r)[:90]}


def occupy(exp):
    def s(c):
        t = c.rc.table_manager._tables[2]
        t.status = "ACTIVE"; t.current_event_key = "altro"; t.market_id = "1.999"; t.current_exposure = exp
    return s


def dd(c):
    c.rc.risk_desk.equity_peak = 1000.0; c.rc.risk_desk.bankroll_current = 990.0


def corrupt(c):
    c.rc.config.corrupt_keys = ("roserpina.book_block",)


def riskstop(c):
    c.rc._risk_stop_reason = "risk_stop_manuale"


def peak_blocked(c):
    c.rc._equity_peak_blocked = "equity_peak_non_persistito"


def remote(c):
    reg = getattr(c.rc, "remote_exposure", None)
    if reg is None:
        raise RuntimeError("registro esposizione remota assente nell'albero")
    reg.seed([dict(H1)])
    c.rc.betfair_service.list_current_orders = lambda market_ids=None: [dict(H1)]


CASES = [  # id, descrizione, setup, side, stake, price, atteso
    ("PAR-CTRL", "nessun vincolo attivo", None, "BACK", 0.5, 2.0, "AMMESSO"),
    ("PAR-LAY-LIAB", "esposizione 9.50 + LAY 0.50@3.00 (liability 1.00) > 10", occupy(9.5), "LAY", 0.5, 3.0, "BLOCCATO"),
    ("PAR-BACK-EQ", "esposizione 9.50 + BACK 0.50 = 10.00 (uguale al cap)", occupy(9.5), "BACK", 0.5, 2.0, "AMMESSO"),
    ("PAR-DD", "perdita 10.00 su picco 1000 (D1: stop a >= 10)", dd, "BACK", 0.5, 2.0, "BLOCCATO"),
    ("PAR-CORRUPT", "config con chiave corrotta (FAILSAFE)", corrupt, "BACK", 0.5, 2.0, "BLOCCATO"),
    ("PAR-RISKSTOP", "risk stop attivo (P38)", riskstop, "BACK", 0.5, 2.0, "BLOCCATO"),
    ("PAR-PEAK", "picco equity non persistito (CL02-RST-003)", peak_blocked, "BACK", 0.5, 2.0, "BLOCCATO"),
    ("PAR-REMOTE", "ordine remoto al riavvio, liability 17.16 > cap 10 (CL17)", remote, "BACK", 0.5, 2.0, "BLOCCATO"),
]
out = {}
for cid, desc, setup, side, stake, price, exp in CASES:
    row = {"descrizione": desc, "atteso": exp}
    for lane, fn in (("segnale", lane_signal), ("manuale_gui", lane_manual)):
        try:
            o = fn(setup, side, stake, price)
        except Exception as exc:
            o = {"orders": None, "reason": f"NON_APPLICABILE: {exc}"}
        o["esito"] = "AMMESSO" if o["orders"] == 1 else ("BLOCCATO" if o["orders"] == 0 else "N/A")
        row[lane] = o
    ok = [row[l]["esito"] == exp for l in ("segnale", "manuale_gui")]
    row["verdetto"] = "CONFORME" if all(ok) else ("N/A" if any(row[l]["esito"] == "N/A" for l in ("segnale", "manuale_gui")) else "NON_CONFORME")
    out[cid] = row
out["PAR-MCP"] = {"descrizione": "corsia MCP", "verdetto": "NOT_VERIFIED", "nota": "nessuna corsia MCP nel codice"}
json.dump(out, open(os.environ["PROBE_OUT"], "w"), indent=1, ensure_ascii=False)
for k, v in out.items():
    if "segnale" in v:
        print(f'{k:13} {v["verdetto"]:13} segnale={v["segnale"]["esito"]:9} manuale={v["manuale_gui"]["esito"]:9} | {v["manuale_gui"]["reason"][:60]}')
    else:
        print(k, v["verdetto"])
