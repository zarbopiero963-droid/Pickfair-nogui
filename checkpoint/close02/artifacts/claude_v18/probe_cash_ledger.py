"""close02 (Claude) — CL18-CASH-LEDGER-001: l'hedge del cashout non entra nel ledger ordini del bot, quindi il
poller dei settlement scarta la sua riga cleared e il P/L realizzato conta solo l'ingresso.
Fonte: #461/6085516609 V1 'fill completo->riconciliare payoff netto', PF-CASH-21 'P/L realizzato da
fills/settlement'; #506/6124429007 'P&L realizzato (solo da listClearedOrders)'. Nessuna rete."""
import inspect, json, os, sys
ROOT = os.environ["PROBE_ROOT"]; sys.path.insert(0, ROOT); os.chdir(ROOT)
import cashout_executor, core.order_router as orr  # noqa: E402
from tests.core.test_settlement_poller import _controller, _closes  # noqa: E402

out = {}
src = inspect.getsource(cashout_executor) + inspect.getsource(orr)
out["statico"] = {"cashout_scrive_ledger_bot": any(k in src for k in ("insert_order", "orders_ledger", "record_order"))}
# Ingresso B1 (101) nel ledger; hedge H1 (102) piazzato da OrderRouter: assente dal ledger.
rows = [{"betId": "101", "marketId": "1.230000777", "profit": 10.0, "commission": 0.0,
         "settledDate": "2026-10-13T10:00:00Z"},
        {"betId": "102", "marketId": "1.230000777", "profit": round(-6 * 20 / 7, 2), "commission": 0.0,
         "settledDate": "2026-10-13T10:00:00Z"}]
rc = _controller(results=[rows], bot_orders=[{"bet_id": "101", "market_id": "1.230000777", "event_name": "A v B"}])
rc._poll_cleared_settlements()
closes = _closes(rc)
gross = round(sum(float(p["gross_pnl"]) for p in closes), 2)
out["CASH-21-R1-ledger"] = {"atteso": "P/L realizzato del mercato = -7.14 (ingresso +10, hedge -17.14)",
                            "osservato": {"close": len(closes), "gross": gross, "bet_applicate": [p["event_key"] for p in closes]},
                            "verdetto": "CONFORME" if abs(gross - (-7.14)) < 0.005 else "NON_CONFORME"}
json.dump(out, open(os.environ["PROBE_OUT"], "w"), indent=1, ensure_ascii=False)
print(json.dumps(out, ensure_ascii=False))
