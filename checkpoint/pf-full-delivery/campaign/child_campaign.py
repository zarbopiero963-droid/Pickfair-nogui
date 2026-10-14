"""PF-ROSERPINA-41-FULL-SIMULATION-02 — esecutore di UNO scenario sul runtime reale (HeadlessApp, SIM, SQLite su disco).

argv: TREE OUT SPEC_JSON_PATH HOME PHASE
Lo scenario (generato da generate.py) dichiara capitale, impostazioni Roserpina, book e passi. Qui non si calcola
nessun atteso: si registrano gli eventi del bus, il registro del broker SIM (l'«exchange»), il ledger ordini del
bot e lo stato del runtime. L'oracolo indipendente (oracle.py) giudica.
Passi: signal, price, mode (ok|reject|partial|lost|unknown), settle, cashout, risk_stop, withdraw, status, crash.
"""
import json, os, sys, time, traceback

TREE, OUT, SPEC_PATH, HOME, PHASE = sys.argv[1:6]
PHASE = int(PHASE)
os.makedirs(HOME, exist_ok=True); os.chdir(HOME); os.environ["HOME"] = HOME
sys.path.insert(0, TREE)
SPEC = json.load(open(SPEC_PATH))
R = {"scenario": SPEC["id"], "phase": PHASE, "steps": [], "events": [], "mm_calls": [], "bankroll_syncs": []}
EV = []
T0 = time.time()
TERMINAL = ("SIGNAL_REJECTED", "QUICK_BET_SUCCESS", "QUICK_BET_FAILED", "QUICK_BET_AMBIGUOUS", "QUICK_BET_ROUTED",
            "QUICK_BET_DUPLICATE", "QUICK_BET_ACCEPTED", "QUICK_BET_PARTIAL", "QUICK_BET_FILLED")


def dump(app=None, rt=None, brk=None):
    try:
        if brk is not None:
            R["broker_orders"] = [o.to_dict() for o in brk.state.orders.values()]
            R["broker_funds"] = dict(brk.get_account_funds())
            R["broker_settlements"] = list(brk.list_settlements() or [])
            st = brk.state.to_dict()
            R["broker_state"] = {k: st.get(k) for k in ("starting_balance", "balance", "exposure", "commission_pct",
                                                        "realized_pnl", "realized_commission", "market_commission_ledger")}
        if rt is not None:
            d = rt.risk_desk
            R["runtime"] = {
                "mode": getattr(rt.mode, "value", str(rt.mode)), "bankroll": d.bankroll_current, "peak": d.equity_peak,
                "realized": d.realized_pnl, "daily": {k: (v if isinstance(v, (int, float, bool, str)) or v is None else str(v))
                                                      for k, v in (getattr(rt, "_daily_loss_monitor_state", {}) or {}).items()},
                "account": dict(getattr(rt, "_account_snapshot", None) or {}),
                "tables": [{"table_id": t.get("table_id"), "status": str(t.get("status")), "market_id": t.get("market_id"),
                            "exposure": t.get("current_exposure"), "event_key": t.get("current_event_key"),
                            "loss": t.get("loss_amount")} for t in rt.table_manager.snapshot()],
                "rebuild": dict(getattr(rt, "_last_position_rebuild", None) or {}),
            }
        if app is not None:
            rows = app.db._execute("SELECT * FROM orders",
                                   fetch=True, commit=False) or []
            out = []
            for r in rows:
                r = dict(r); p = json.loads(r["payload_json"] or "{}")
                out.append({"ref": r["customer_ref"], "status": r["status"], "market_id": p.get("market_id"),
                            "side": p.get("bet_type"), "stake": p.get("stake"), "price": p.get("price"),
                            "event_key": p.get("event_key"), "table_id": p.get("table_id"),
                            "extra": {k: (str(v)[:300] if v is not None else None) for k, v in r.items()
                                      if k not in ("payload_json", "customer_ref", "status")}})
            R["db_orders"] = out
    except Exception:
        R["dump_error"] = traceback.format_exc()[-800:]
    R["events"] = [{"t": round(t - T0, 3), "e": e, "m": d.get("market_id"), "reason": d.get("reason"),
                    "pnl": d.get("net_pnl", d.get("pnl")), "table_id": d.get("table_id"),
                    "stake": d.get("stake"), "status": d.get("status"),
                    "ref": d.get("customer_ref"), "price": d.get("price"), "side": d.get("side") or d.get("bet_type"),
                    **({"detail": json.dumps(d, default=str)[:700]} if e in ("QUICK_BET_FAILED", "QUICK_BET_AMBIGUOUS",
                       "SIGNAL_REJECTED", "ROSERPINA_AUTO_RESET", "RUNTIME_CLOSE_POSITION", "BANKROLL_SYNC_RESULT",
                       "STARTUP_POSITIONS_REBUILT", "QUICK_BET_DUPLICATE") or e.startswith("CASHOUT") else {})}
                   for t, e, d in EV]
    json.dump(R, open(OUT, "w"), default=str)


def wait_for(pred, timeout):
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return True
        time.sleep(0.02)
    return False


app = rt = brk = None
try:
    import headless_main
    app = headless_main.HeadlessApp(); app.build(start_services=True)
    orig = app.bus.publish

    def rec(e, d=None):
        EV.append((time.time(), e, d if isinstance(d, dict) else {}))
        return orig(e, d)
    app.bus.publish = rec
    if PHASE == 0:
        base = {"simulation.starting_balance": float(SPEC["capital"]), "settlement.poll_enabled": False}
        base.update(SPEC.get("settings") or {})
        app.settings_service.save_settings(base)
    rt = app.runtime
    _sync = rt.risk_desk.sync_bankroll

    def sync_rec(value):
        b = app.betfair_service.get_simulation_broker()
        f = dict(b.get_account_funds()) if b is not None else {}
        R["bankroll_syncs"].append({"value": value, "available": f.get("available"),
                                    "caller": sys._getframe(1).f_code.co_name})
        return _sync(value)
    rt.risk_desk.sync_bankroll = sync_rec
    R["start"] = str(rt.start(simulation_mode=True, execution_mode="SIM"))[:120]
    R["mode_after_start"] = getattr(rt.mode, "value", str(rt.mode))
    if getattr(app, "executor", None) is not None:
        app.executor.default_timeout = 1.0
    _calc = rt.mm.calculate

    def calc_rec(*a, **k):
        res = _calc(*a, **k)
        R["mm_calls"].append({"bankroll_current": k.get("bankroll_current"), "t": round(time.time() - T0, 3),
                              "stake": getattr(res, "recommended_stake", None),
                              "approved": getattr(res, "approved", None), "reason": getattr(res, "reason", None)})
        return res
    rt.mm.calculate = calc_rec
    brk = app.betfair_service.get_simulation_broker()
    MODE = {}
    real = brk.place_bet

    def place_bet(*a, **k):
        m = str(k.get("market_id") or "")
        mode = MODE.get(m, "ok") if str(k.get("side") or "").upper() in ("BACK", "LAY") and not str(
            k.get("customer_ref") or "").startswith("sig-") is False else "ok"
        if mode == "reject":
            return {"status": "FAILURE", "errorCode": "INSUFFICIENT_FUNDS", "simulated": True,
                    "instructionReports": [{"status": "FAILURE", "errorCode": "INSUFFICIENT_FUNDS"}]}
        if mode == "lost":
            real(*a, **k); raise TimeoutError("response lost after send")
        if mode == "unknown":
            real(*a, **k); return {"order_unknown": True, "status": "TIMEOUT"}
        return real(*a, **k)
    brk.place_bet = place_bet
    BOOK = {m: dict(b) for m, b in (SPEC.get("books") or {}).items()}

    def book(m, status="OPEN", winner=None):
        b = BOOK[m]
        px = float(b["price"])
        brk.update_market_book({"marketId": m, "status": status, "inplay": bool(b.get("inplay")), "runners": [
            {"selectionId": s, "status": "ACTIVE" if status == "OPEN" else ("WINNER" if s == winner else "LOSER"),
             "ex": {"availableToBack": [{"price": px, "size": float(b.get("back_size", 100.0))}],
                    "availableToLay": [{"price": round(px + 0.02, 2), "size": float(b.get("lay_size", 100.0))}]}}
            for s in (11, 12)]})

    for m in BOOK:
        book(m)
    steps = SPEC["phases"][PHASE]
    for i, st in enumerate(steps):
        op = st["op"]; m = st.get("m")
        n0 = len(EV)
        res = {"i": i, "op": op, "m": m, "t": round(time.time() - T0, 3)}
        if op == "mode":
            MODE[m] = st["mode"]
            if st["mode"] == "partial":
                BOOK[m]["back_size"] = BOOK[m]["lay_size"] = float(st.get("size", 1.0)); book(m)
        elif op == "price":
            BOOK[m]["price"] = float(st["price"]); book(m)
        elif op == "signal":
            side = st.get("side", "BACK")
            app.bus.publish("SIGNAL_RECEIVED", {"market_id": m, "selection_id": 11, "price": float(BOOK[m]["price"]),
                                                "side": side, "bet_type": side, "event_name": f"E{m} v X",
                                                "market_name": "Match Odds", "runner_name": f"E{m}",
                                                "simulation_mode": True, "source": "CAMPAIGN"})
            res["resolved"] = wait_for(lambda: any(e in TERMINAL and d.get("market_id") == m for _, e, d in EV[n0:])
                                       or any(e == "SIGNAL_REJECTED" for _, e, d in EV[n0:]), 4.0)
            wait_for(lambda: False, 0.05)
        elif op == "settle":
            has_order = any(str(o.market_id) == m for o in brk.state.orders.values())
            book(m, status="CLOSED", winner=11 if st["win"] else 12)
            res["had_order"] = has_order
            res["mode_before"] = getattr(rt.mode, "value", str(rt.mode))
            rt._poll_cleared_settlements()
            res["booked"] = wait_for(lambda: any(e == "BANKROLL_SYNC_RESULT" for _, e, d in EV[n0:]),
                                     3.0 if has_order else 0.3)
        elif op == "cashout":
            app.bus.publish("SIGNAL_RECEIVED", {"signal_type": "CASHOUT", "event_name": f"E{m} v X", "source": "CAMPAIGN"})
            res["resolved"] = wait_for(lambda: any(e.startswith("CASHOUT_") for _, e, d in EV[n0:]), 4.0)
        elif op == "risk_stop":
            res["result"] = str(rt.risk_stop("campaign"))[:120]
            wait_for(lambda: False, 0.5)
        elif op == "withdraw":
            with brk._lock:
                brk.state.balance -= float(st["amount"])
        elif op == "status":
            rt.get_status()
        elif op == "crash":
            app.betfair_service._persist_simulation_state_if_needed(force=True)
            R["crashed"] = True
            R["steps"].append(res); dump(app, rt, brk); os._exit(1)
        res["mode"] = getattr(rt.mode, "value", str(rt.mode))
        R["steps"].append(res)
    app.betfair_service._persist_simulation_state_if_needed(force=True)
    dump(app, rt, brk)
    import threading
    th = threading.Thread(target=app.stop, daemon=True); t_stop = time.time(); th.start(); th.join(20.0)
    R["stop"] = {"done": not th.is_alive(), "sec": round(time.time() - t_stop, 2),
                 "mode": getattr(rt.mode, "value", str(rt.mode))}
except Exception:
    R["error"] = traceback.format_exc()[-1500:]
    dump(app, rt, brk)
json.dump(R, open(OUT, "w"), default=str)
os._exit(0)
