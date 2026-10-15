"""Replay ScoreTrend 13/10/2026 (#510) su HeadlessApp reale in SIM — un profilo per processo.

argv: TREE DATASET OUT PROFILE HOME PHASE MKT
  MKT: pre | live | mixed  (PF-F026 #510: dimensione PRE-MATCH / LIVE / MISTA, sintetica e DICHIARATA)
  PROFILE: normal | adversarial | crash | financial
  PHASE (solo crash): pre (fino al crash delle 21:00, termina con os._exit) | post (riavvio)

Componenti reali: HeadlessApp.build/start (RuntimeController, Roserpina MM, TableManager, RiskGate,
TradingEngine, ExecutorManager, EventBus, Database, settlement poller). Simulato solo il trasporto
(SimulationBroker con comportamento per mercato DICHIARATO). Rete negata dal sitecustomize.
"""
import json, os, sys, time, threading, traceback
TREE, DATASET, OUT, PROFILE, HOME = sys.argv[1:6]
PHASE = sys.argv[6] if len(sys.argv) > 6 and sys.argv[6] != "-" else ""
MKT = sys.argv[7] if len(sys.argv) > 7 else "pre"
os.makedirs(HOME, exist_ok=True); os.chdir(HOME); os.environ["HOME"] = HOME
sys.path.insert(0, TREE)
DS = json.load(open(DATASET))
SIG = DS["signals"]
R = {"tree": TREE, "profile": PROFILE, "phase": PHASE, "mkt": MKT, "timeline": [], "sim_declared": {}}
EV = []
T0 = time.time()

def log(kind, **kw):
    R["timeline"].append({"t": round(time.time() - T0, 3), "kind": kind, **kw})

def mid(i):
    return f"1.95{i:02d}"

SEL = 11          # selezione del segnale (sintetica SIM)
OTHER = 12        # selezione perdente (sintetica SIM)
def is_live(i):
    return MKT == "live" or (MKT == "mixed" and i % 2 == 1)   # MISTA: indici dispari in-play (DICHIARATO)


LIVE_DRIFT = 0.20     # movimento di prezzo in-play dopo l'ingresso (DICHIARATO): solo i mercati LIVE
LIVE_PRE_ENTRY = 0.05 # LIVE: la quota si e' gia' mossa tra segnale e ordine (DICHIARATO): BACK abbinabile a prezzo migliore
PRICE_NOW = {i: round(SIG[i]["price"] + LIVE_PRE_ENTRY, 2) for i in range(len(SIG)) if is_live(i)}
SLOTS = {**{i: "20:45" for i in range(0, 4)}, **{i: "21:00" for i in range(4, 12)}, 12: "00:30"}

try:
    import headless_main
    app = headless_main.HeadlessApp(); app.build(start_services=True)
    orig = app.bus.publish
    def rec(e, d=None):
        EV.append((round(time.time() - T0, 3), e, d if isinstance(d, dict) else {})); return orig(e, d)
    app.bus.publish = rec
    first_start = PHASE != "post"
    if first_start:
        app.settings_service.save_settings({
            "simulation.starting_balance": 100.0,          # bankroll SIM ipotetico (#510)
            "settlement.poll_enabled": True, "settlement.poll_sec": 1,
        })
    rt = app.runtime
    R["start"] = str(rt.start(simulation_mode=True, execution_mode="SIM"))[:300]
    brk = app.betfair_service.get_simulation_broker()
    WAIT = 2.0
    if getattr(app, "executor", None) is not None:
        app.executor.default_timeout = WAIT             # attesa SIM dichiarata (produzione: 30 s)
    R["sim_declared"]["executor_wait_s"] = WAIT

    # comportamento del trasporto per mercato (DICHIARATO)
    MODE = {}
    if PROFILE == "adversarial":
        MODE = {mid(1): "reject", mid(2): "partial", mid(3): "lost", mid(4): "unknown",
                mid(5): "delayed", mid(6): "slow", mid(8): "reject"}
    if PROFILE == "crash" and PHASE == "pre":
        MODE = {mid(4): "slow_crash"}
    if PROFILE == "critical" and PHASE == "pre":
        MODE = {mid(1): "lost", mid(2): "partial", mid(3): "unknown", mid(4): "slow_crash"}   # 5 tavoli: 0-3 + 4
    R["sim_declared"]["transport_modes"] = MODE
    CALLS = []
    BOOKLOG = []
    R["sim_declared"]["live_markets"] = [mid(i) for i in range(len(SIG)) if is_live(i)]
    R["sim_declared"]["live_drift"] = LIVE_DRIFT
    R["sim_declared"]["live_pre_entry_move"] = LIVE_PRE_ENTRY
    real = brk.place_bet
    def place_bet(*a, **k):
        m = str(k.get("market_id") or k.get("marketId") or "")
        mode = MODE.get(m, "ok")
        side = str(k.get("side") or "").upper()
        CALLS.append(m if side == "BACK" else f"{m}:{side}"); log("broker_call", market_id=m, side=side, mode=mode)
        if side != "BACK":                       # copertura LAY del cashout: trasporto normale
            return real(*a, **k)
        if mode == "reject":
            return {"status": "FAILURE", "errorCode": "INSUFFICIENT_FUNDS", "simulated": True,
                    "instructionReports": [{"status": "FAILURE", "errorCode": "INSUFFICIENT_FUNDS"}]}
        if mode == "lost":
            real(*a, **k); raise TimeoutError("response lost after send")
        if mode == "unknown":
            real(*a, **k); return {"order_unknown": True, "status": "TIMEOUT"}
        if mode == "delayed":
            time.sleep(WAIT * 0.4); return real(*a, **k)
        if mode == "slow":
            r = real(*a, **k); time.sleep(WAIT * 2.5); return r
        if mode == "slow_crash":
            real(*a, **k)
            if PROFILE == "critical":       # l'exchange (SIM) ha l'ordine: sopravvive al crash del bot (DICHIARATO)
                app.betfair_service._persist_simulation_state_if_needed(force=True)
            log("CRASH_21_00", market_id=m, note="ordine inviato al broker, risposta mai arrivata, processo terminato")
            dump(); os._exit(1)
        return real(*a, **k)
    brk.place_bet = place_bet

    def book(i, status="OPEN", winner=None):
        s = SIG[i]
        px = PRICE_NOW.get(i, s["price"])
        size_back = 1.0 if MODE.get(mid(i)) == "partial" else 100.0
        runners = []
        for sel in (SEL, OTHER):
            st = "ACTIVE" if status == "OPEN" else ("WINNER" if sel == winner else "LOSER")
            runners.append({"selectionId": sel, "status": st, "ex": {
                "availableToBack": [{"price": px, "size": size_back}],
                "availableToLay": [{"price": round(px + 0.02, 2), "size": 100.0}]}})
        brk.update_market_book({"marketId": mid(i), "status": status, "inplay": is_live(i), "runners": runners})
        BOOKLOG.append({"i": i, "market_id": mid(i), "status": status, "inplay": is_live(i), "back": px,
                        "lay": round(px + 0.02, 2), "t": round(time.time() - T0, 3)})

    def inplay_move():
        for i in range(len(SIG)):
            if is_live(i):
                PRICE_NOW[i] = round(SIG[i]["price"] + LIVE_DRIFT, 2); book(i)
        log("inplay_price_move", drift=LIVE_DRIFT, markets=[mid(i) for i in range(len(SIG)) if is_live(i)])

    def signal(i):
        s = SIG[i]
        app.bus.publish("SIGNAL_RECEIVED", {
            "market_id": mid(i), "selection_id": SEL, "price": s["price"], "side": "BACK", "bet_type": "BACK",
            "event_name": s["match"], "market_name": s["market"], "runner_name": s["selection"],
            "simulation_mode": True, "source": "SCORETREND_REPLAY",
            **({"minute": 20 + i} if is_live(i) else {})})
        log("signal", live=is_live(i), i=i, slot=SLOTS[i], market_id=mid(i), match=s["match"], market=s["market"], price=s["price"])

    def outcome_win(i):
        return i % 2 == 0          # SIM-OUT-A (dichiarato)

    def settle(i):
        book(i, status="CLOSED", winner=SEL if outcome_win(i) else OTHER)
        log("settle", i=i, market_id=mid(i), sim_outcome="WIN" if outcome_win(i) else "LOSE")

    def snap(tag):
        if PROFILE == "critical":
            app.betfair_service._persist_simulation_state_if_needed(force=True)
        tabs = [{"table_id": t.get("table_id"), "status": str(t.get("status")), "exposure": t.get("current_exposure"),
                 "market_id": t.get("market_id"), "event_key": t.get("current_event_key")}
                for t in rt.table_manager.snapshot() if "FREE" not in str(t.get("status"))]
        orders = []
        for r in app.db._execute("SELECT customer_ref, status, payload_json, response_json FROM orders", fetch=True, commit=False) or []:
            r = dict(r); p = json.loads(r["payload_json"] or "{}"); resp = r["response_json"] or ""
            try:
                rj = json.loads(resp) if resp else {}
            except Exception:
                rj = {}
            rep = (rj.get("instructionReports") or [{}])[0] if isinstance(rj, dict) else {}
            orders.append({"ref": r["customer_ref"], "status": r["status"], "market_id": p.get("market_id"),
                           "stake": p.get("stake"), "price": p.get("price"), "bet_id": rep.get("betId"),
                           "size_matched": rep.get("sizeMatched"), "response_empty": resp in ("", "{}")})
        funds = brk.get_account_funds() if brk else {}
        out = {"tag": tag, "tables": tabs, "total_exposure": rt.table_manager.total_exposure(), "orders": orders,
               "broker_orders": [{"market_id": o.get("marketId"), "side": o.get("side"), "status": o.get("status"),
                                  "size": (o.get("priceSize") or {}).get("size"), "matched": o.get("sizeMatched"),
                                  "price": (o.get("priceSize") or {}).get("price"),
                                  "avg_price_matched": o.get("averagePriceMatched")} for o in brk.get_current_orders()],
               "broker_funds": funds, "realized_pnl": getattr(rt.risk_desk, "realized_pnl", None),
               "bankroll_current": getattr(rt.risk_desk, "bankroll_current", None),
               "peak": getattr(rt.risk_desk, "equity_peak", None), "drawdown_pct": rt.risk_desk.drawdown_pct(),
               "auto_resets": sum(1 for _, e, _ in EV if e == "ROSERPINA_AUTO_RESET")}
        R.setdefault("snapshots", []).append(out); return out

    def dump():
        R["events"] = [{"t": t, "event": e, "market_id": d.get("market_id"), "table_id": d.get("table_id"),
                        "status": d.get("status"), "reason": d.get("reason"), "customer_ref": d.get("customer_ref"),
                        "pnl": d.get("pnl", d.get("net_pnl")), "detail": {k: str(v)[:80] for k, v in d.items()
                                                                          if k in ("bankroll_after", "bankroll_before", "drawdown_pct", "bankroll_sync_status", "auto_trade_status", "price")}}
                       for t, e, d in EV if e.startswith(("QUICK_BET", "SIGNAL_", "CMD_QUICK", "RUNTIME_CLOSE", "RISK_STOP", "CASHOUT", "REQ_EXECUTE", "ROSERPINA", "BANKROLL_SYNC", "AUTO_TRADE", "DAILY_LOSS", "SESSION_LOSS"))]
        R["broker_calls"] = CALLS
        R["book_log"] = BOOKLOG
        json.dump(R, open(OUT, "w"), indent=1, default=str)
        sys.stdout.flush()

    for i in range(len(SIG)):
        book(i)

    if PROFILE in ("normal", "financial", "adversarial"):
        for i in range(0, 4): signal(i)
        time.sleep(WAIT * 3 if PROFILE == "adversarial" else 2); snap("after_20_45")
        for i in range(4, 12): signal(i)
        time.sleep(WAIT * 3 if PROFILE == "adversarial" else 3); snap("after_21_00")
        inplay_move(); time.sleep(2); snap("after_inplay_move")
        if PROFILE == "financial":
            # cashout con posizioni APERTE (nel primo giro era NOT VERIFIED: nessuna posizione aperta)
            R["risk_stop"] = str(rt.risk_stop("replay_financial_cashout"))[:300]
            log("risk_stop_cashout"); time.sleep(5); snap("after_risk_stop_cashout")
        for i in (4, 7): settle(i)                       # mercati HT
        time.sleep(4); snap("after_HT_settlement")
        for i in range(0, 12):
            if i not in (4, 7): settle(i)
        time.sleep(5); snap("after_FT_settlement")
        signal(12); time.sleep(3); snap("after_00_30")
        settle(12); time.sleep(4); snap("final")
    elif PROFILE == "crash" and PHASE == "pre":
        for i in range(0, 4): signal(i)
        time.sleep(2); snap("after_20_45")
        dump()                                          # stato prima del crash su disco
        signal(4); time.sleep(10)                       # 21:00: il broker riceve l'ordine e il processo muore
        log("ERROR_no_crash")
    elif PROFILE == "crash" and PHASE == "post":
        snap("after_restart_before_signals")
        R["recovery_events"] = [e for _, e, _ in EV if "RECOVER" in e or "RECONCIL" in e or "GHOST" in e or "STARTUP" in e]
        for i in range(0, 7): signal(i)                 # riconsegna degli stessi segnali (feed che riparte)
        time.sleep(4); snap("after_redelivery")
        for i in range(7, 12): signal(i)
        time.sleep(3); snap("after_remaining_21_00")
    elif PROFILE == "critical" and PHASE == "pre":
        R["sim_declared"]["critical"] = "MISTA + incerti + crash con ordine in volo + exchange SIM persistente"
        for i in range(0, 4): signal(i)
        time.sleep(WAIT * 3); snap("after_20_45")
        settle(0); time.sleep(4); snap("after_settle_0")
        dump()
        signal(4); time.sleep(10)                       # il broker riceve l'ordine 4 (5o tavolo) e il processo muore
        log("ERROR_no_crash")
    elif PROFILE == "critical" and PHASE == "post":
        snap("after_restart_before_signals")
        for i in range(0, 5): signal(i)                 # riconsegna dei segnali gia' visti
        time.sleep(4); snap("after_redelivery")
        for i in range(1, 5): settle(i)
        time.sleep(6); snap("after_settlement")
        snap("final")
    R["stop"] = str(app.stop())[:100]
except Exception:
    R["error"] = traceback.format_exc()[-2000:]
try:
    dump()
except Exception:
    pass
os._exit(0)
