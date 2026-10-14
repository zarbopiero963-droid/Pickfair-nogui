"""PF-ALL-F024 / F026: auto-reset Roserpina da drawdown con un ordine AMBIGUOUS aperto.

argv: TREE OUT
HeadlessApp reale in SIM (ExecutorManager di produzione). Mercato 1.811: risposta persa DOPO
l'invio (ordine vivo sull'exchange, esito AMBIGUOUS). Mercato 1.812: ordine normale, poi
settlement vincente. Soglia auto-reset abbassata a 1% (parametro SIM dichiarato): il bankroll
sincronizzato dal saldo disponibile dell'exchange fa scattare ROSERPINA_AUTO_RESET.
"""
import json, os, sys, tempfile, time, traceback
TREE, OUT = sys.argv[1:3]
work = tempfile.mkdtemp(prefix="pfF024_"); os.chdir(work); os.environ["HOME"] = work
sys.path.insert(0, TREE)
R = {"tree": TREE}
EV = []
try:
    import headless_main
    app = headless_main.HeadlessApp(); app.build(start_services=True)
    orig = app.bus.publish
    app.bus.publish = lambda e, d=None: (EV.append((e, d if isinstance(d, dict) else {})), orig(e, d))[1]
    app.settings_service.save_settings({"simulation.starting_balance": 100.0, "settlement.poll_enabled": True,
                                        "settlement.poll_sec": 1, "roserpina.auto_reset_drawdown_pct": 1.0})
    rt = app.runtime
    rt.start(simulation_mode=True, execution_mode="SIM")
    brk = app.betfair_service.get_simulation_broker()
    real = brk.place_bet

    def place_bet(*a, **k):
        if str(k.get("market_id") or "") == "1.811":
            real(*a, **k)
            raise TimeoutError("response lost after send")
        return real(*a, **k)
    brk.place_bet = place_bet

    def book(m, status="OPEN", winner=None):
        brk.update_market_book({"marketId": m, "status": status, "inplay": False, "runners": [
            {"selectionId": s, "status": "ACTIVE" if status == "OPEN" else ("WINNER" if s == winner else "LOSER"),
             "ex": {"availableToBack": [{"price": 2.0, "size": 100}], "availableToLay": [{"price": 2.02, "size": 100}]}}
            for s in (11, 12)]})
    for m in ("1.811", "1.812"):
        book(m)
    for i, m in enumerate(("1.811", "1.812")):
        app.bus.publish("SIGNAL_RECEIVED", {"market_id": m, "selection_id": 11, "price": 2.0, "side": "BACK", "bet_type": "BACK",
                                            "event_name": f"Casa{i} v Ospite{i}", "market_name": "Match Odds", "runner_name": f"Casa{i}",
                                            "simulation_mode": True, "source": "F024_AUTORESET"})
    time.sleep(3)
    R["before"] = {"tables": [(t.get("table_id"), t.get("market_id"), t.get("current_exposure")) for t in rt.table_manager.snapshot()
                              if "FREE" not in str(t.get("status"))],
                   "orders": {json.loads(dict(r)["payload_json"])["market_id"]: dict(r)["status"] for r in
                              app.db._execute("SELECT status, payload_json FROM orders", fetch=True, commit=False) or []}}
    amb_key = next((d.get("event_key") for e, d in EV if e == "QUICK_BET_AMBIGUOUS"), None)
    book("1.812", status="CLOSED", winner=11)
    time.sleep(5)
    R["auto_reset"] = [d.get("reason") for e, d in EV if e == "ROSERPINA_AUTO_RESET"]
    R["auto_reset_events"] = sum(1 for e, _ in EV if e == "ROSERPINA_AUTO_RESET")
    R["after"] = {"tables": [(t.get("table_id"), t.get("market_id"), t.get("current_exposure")) for t in rt.table_manager.snapshot()
                             if "FREE" not in str(t.get("status"))]}
    R["ambiguous_key"] = amb_key
    R["ambiguous_key_still_held"] = (not rt.duplication_guard.acquire(amb_key)) if amb_key else None
    app.stop()
except Exception:
    R["error"] = traceback.format_exc()[-1500:]
json.dump(R, open(OUT, "w"), indent=1, default=str)
sys.stdout.flush(); os._exit(0)
