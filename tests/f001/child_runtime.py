"""Processo figlio per gli scenari B-RED a livello applicazione: HeadlessApp reale (cablaggio di produzione,
ExecutorManager reale), broker SIM con esito pilotato, HOME/DB temporanei, rete negata dal sitecustomize.

argv: TREE OUT SCENARIO PATH(exec|direct)
  exec   = cablaggio di produzione (TradingEngine.executor = ExecutorManager)
  direct = controllo di parita' (TradingEngine.executor = None, stesso runtime)
"""
import json, os, sys, tempfile, time, threading, traceback
TREE, OUT, SCEN, PATH = sys.argv[1:5]
work = tempfile.mkdtemp(prefix="pfB_"); os.chdir(work); os.environ["HOME"] = work
sys.path.insert(0, TREE)
R = {"tree": TREE, "scenario": SCEN, "path": PATH, "steps": {}}
EV = []
try:
    import headless_main
    app = headless_main.HeadlessApp(); app.build(start_services=True)
    orig = app.bus.publish
    def rec(e, d=None):
        EV.append((round(time.time(), 3), e, d if isinstance(d, dict) else {})); return orig(e, d)
    app.bus.publish = rec
    if PATH == "direct":
        app.trading_engine.executor = None
    # Harness (dichiarato): su #505/v18+ il cap owner di 1 EUR per ordine respinge lo stake SIM di 30 EUR
    # prima dell'engine. Per esercitare il percorso ordine si alzano i cap assoluti SOLO in questo DB SIM
    # temporaneo; il cap non e' oggetto di questi scenari. Su main le chiavi non esistono e sono ignorate.
    caps = {"roserpina.max_order_exposure": 1000.0, "roserpina.max_market_exposure": 1000.0,
            "roserpina.max_event_exposure_abs": 1000.0, "roserpina.max_drawdown_abs": 1000.0,
            # v18+ (gate T80U): parametri di rischio confermati esplicitamente (valori SIM dichiarati)
            "roserpina.commission_pct": 4.5, "roserpina.max_event_exposure_pct": 10.0, "roserpina.max_single_bet_pct": 5.0,
            "roserpina.max_stake_abs": 1000.0, "roserpina.max_total_exposure_pct": 50.0,
            "roserpina.max_daily_loss": 500.0, "roserpina.max_drawdown_hard_stop_pct": 50.0, "roserpina.max_open_exposure": 500.0}
    try:
        app.settings_service.save_settings(caps); R["harness_caps"] = caps
    except Exception as e:
        R["harness_caps_error"] = repr(e)[:200]
    if SCEN.startswith("settle"):
        app.settings_service.save_settings({"settlement.poll_enabled": True, "settlement.poll_sec": 2})
    rt = app.runtime
    R["steps"]["start"] = bool(rt.start(simulation_mode=True, execution_mode="SIM"))
    brk = app.betfair_service.get_simulation_broker()
    MODES = {}
    LIVE = lambda mid, bid, matched: {"status": "SUCCESS", "marketId": mid, "simulated": True, "instructionReports": [
        {"status": "SUCCESS", "betId": bid, "sizeMatched": matched, "averagePriceMatched": 2.0 if matched else 0.0}]}
    real_place_bet = brk.place_bet
    def place_bet(*a, **k):
        mid = str(k.get("market_id") or k.get("marketId") or (a[0] if a else ""))
        mode = MODES.get(mid, "ok")
        if mode == "reject":
            return {"status": "FAILURE", "errorCode": "INSUFFICIENT_FUNDS", "simulated": True,
                    "instructionReports": [{"status": "FAILURE", "errorCode": "INSUFFICIENT_FUNDS"}]}
        if mode == "lost":
            raise TimeoutError("response lost after send")
        return real_place_bet(*a, **k)
    brk.place_bet = place_bet

    def seed(mid, status="OPEN", winner=None):
        runners = [{"selectionId": s, "status": ("ACTIVE" if status == "OPEN" else "WINNER" if s == winner else "LOSER"),
                    "ex": {"availableToBack": [{"price": 2.0, "size": 100}], "availableToLay": [{"price": 2.02, "size": 100}]}} for s in (11, 12)]
        brk.update_market_book({"marketId": mid, "status": status, "inplay": False, "runners": runners})

    def sig(i, mid):
        app.bus.publish("SIGNAL_RECEIVED", {"market_id": mid, "selection_id": 11, "price": 2.0, "side": "BACK", "bet_type": "BACK",
            "event_name": f"Casa{i} v Ospite{i}", "market_name": "Match Odds", "runner_name": f"Casa{i}",
            "simulation_mode": True, "source": "AUDIT_PHASE_B"})

    def tables():
        return [{"table_id": t.get("table_id"), "status": str(t.get("status")), "exposure": t.get("current_exposure"),
                 "event_key": t.get("event_key")} for t in rt.table_manager.snapshot() if "FREE" not in str(t.get("status"))]

    def rows():
        out = []
        for r in app.db._execute("SELECT customer_ref, status, payload_json, response_json FROM orders", fetch=True, commit=False) or []:
            r = dict(r); p = json.loads(r["payload_json"] or "{}")
            out.append({"ref": r["customer_ref"], "status": r["status"], "market_id": p.get("market_id"),
                        "response": (r["response_json"] or "")[:160]})
        return out

    def qb():
        return [{"event": e, "market_id": d.get("market_id"), "table_id": d.get("table_id"), "event_key": d.get("event_key"),
                 "status": d.get("status"), "reason": d.get("reason")} for _, e, d in EV if e.startswith("QUICK_BET") or e.startswith("CASHOUT")
                or e in ("SIGNAL_REJECTED", "REQ_EXECUTE_CASHOUT", "CMD_EXECUTE_CASHOUT", "RUNTIME_CLOSE_POSITION", "RISK_STOP_TRIGGERED")]

    plan = {"reject": {"1.701": "reject"}, "ok": {"1.701": "ok"}, "lost": {"1.701": "lost"},
            "multi": {"1.711": "ok", "1.712": "reject", "1.713": "lost"},
            "cashout": {"1.721": "reject"}, "settle": {"1.731": "reject"}}[SCEN]
    MODES.update(plan)
    for m in plan:
        seed(m)
    for i, m in enumerate(plan, 1):
        sig(i, m)
    time.sleep(3)
    R["steps"]["after_orders"] = {"tables": tables(), "total_exposure": rt.table_manager.total_exposure(), "orders": rows(),
                                  "broker_orders": [(o.get("marketId"), o.get("status"), o.get("sizeMatched")) for o in brk.get_current_orders()],
                                  "events": qb()}
    if SCEN == "cashout":
        R["steps"]["risk_stop"] = str(rt.risk_stop("phaseB_cashout_probe"))[:200]
        time.sleep(3)
        R["steps"]["after_cashout"] = {"events": qb(), "tables": tables()}
    if SCEN == "settle":
        seed(list(plan)[0], status="CLOSED", winner=11)
        time.sleep(8)
        R["steps"]["after_settle"] = {"events": qb(), "tables": tables(),
                                      "realized_pnl": getattr(rt.risk_desk, "realized_pnl", None)}
    R["steps"]["stop"] = bool(app.stop() or True)
except Exception:
    R["error"] = traceback.format_exc()[-1500:]
json.dump(R, open(OUT, "w"), indent=1, default=str)
sys.stdout.flush(); os._exit(0)
