"""PF-ALL-F026 — scenari finanziari Roserpina su HeadlessApp reale (SIM, SQLite reale su disco).

argv: TREE OUT SCENARIO_JSON HOME
SCENARIO_JSON: {"steps": [...]} con passi:
  {"op": "signal", "m": "1.701", "price": 2.0, "side": "BACK"}       segnale reale (Roserpina decide lo stake)
  {"op": "mode", "m": "1.701", "mode": "lost"|"partial"|"ok"}       comportamento del trasporto (SIM)
  {"op": "settle", "m": "1.701", "win": true}                       mercato CLOSED (settlement reale via poller)
  {"op": "resettle", "m": "1.701"}                                  stesso settlement ripubblicato (duplicato)
  {"op": "withdraw", "amount": 20.0}                                variazione esterna del saldo, senza settlement
  {"op": "risk_stop"}                                               RISK_STOP (cancel + cashout)
  {"op": "cashout", "m": "1.701"}                                   cashout PARZIALE: solo la posizione di quel mercato
  {"op": "crash_after_reset"}                                       os._exit(1) appena scatta ROSERPINA_AUTO_RESET
  {"op": "mode", "m": "1.701", "mode": "nomatch"}                  book peggiore del prezzo: l'ordine resta non abbinato
  {"op": "book_down"}                                               snapshot del book assente da qui in poi
  {"op": "crash_now"}                                               os._exit(1) subito (stato SIM persistito)
  {"op": "crash_after_settlement"}                                  os._exit(1) appena il settlement e' contabilizzato
  {"op": "lockdown"} / {"op": "pause"} / {"op": "emergency"}         LOCKDOWN, PAUSED, emergency stop (PF-ALL-F027)
  {"op": "wait", "s": 3}
  {"op": "replay_cmd", "m": "1.701"}                                ripubblica l'ultimo CMD_QUICK_BET del mercato (PR29-a)
Ogni passo produce uno snapshot: fondi exchange, bankroll Roserpina, picco, drawdown, realizzato, tavoli,
ordini (SQLite), eventi. Nessun calcolo atteso qui: l'oracolo vive nei test.
"""
import json, os, sys, time, traceback
TREE, OUT, SCEN, HOME = sys.argv[1:5]
os.makedirs(HOME, exist_ok=True); os.chdir(HOME); os.environ["HOME"] = HOME
sys.path.insert(0, TREE)
steps = json.loads(SCEN)["steps"]
first_run = not os.path.exists(os.path.join(HOME, ".f026_started"))
R = {"tree": TREE, "first_run": first_run, "snaps": [], "events": []}
EV = []
T0 = time.time()
CRASH = {"armed": False}


def dump():
    R["events"] = [{"t": round(t - T0, 3), "e": e, "market_id": d.get("market_id"), "table_id": d.get("table_id"),
                    "reason": d.get("reason"), "pnl": d.get("pnl"),
                    "bankroll_after": d.get("bankroll_after"), "drawdown_pct": d.get("drawdown_pct")}
                   for t, e, d in EV if e.startswith(("QUICK_BET", "SETTLEMENT_", "EMERGENCY", "RUNTIME_LOCK", "SIGNAL_REJ", "RUNTIME_CLOSE", "ROSERPINA", "BANKROLL_SYNC",
                                                      "RISK_STOP", "CASHOUT", "REQ_EXECUTE", "CMD_QUICK"))]
    json.dump(R, open(OUT, "w"), indent=1, default=str)
    sys.stdout.flush()


try:
    import headless_main
    app = headless_main.HeadlessApp(); app.build(start_services=True)
    orig = app.bus.publish

    def rec(e, d=None):
        EV.append((time.time(), e, d if isinstance(d, dict) else {}))
        if e == "BANKROLL_SYNC_RESULT" and CRASH.get("after_settlement"):
            res = orig(e, d)        # il consumer ha gia' contabilizzato e persistito il checkpoint del settlement
            app.betfair_service._persist_simulation_state_if_needed(force=True)
            R["crashed_at"] = "BANKROLL_SYNC_RESULT"; dump(); os._exit(1)
        if e == "ROSERPINA_AUTO_RESET" and CRASH["armed"]:
            # l'exchange (SIM) ha gia' regolato: il suo stato sopravvive al crash del bot
            app.betfair_service._persist_simulation_state_if_needed(force=True)
            R["crashed_at"] = "ROSERPINA_AUTO_RESET"; dump(); os._exit(1)
        return orig(e, d)
    app.bus.publish = rec
    if first_run:
        base = {"simulation.starting_balance": 100.0, "settlement.poll_enabled": True, "settlement.poll_sec": 1}
        base.update(json.loads(SCEN).get("settings") or {})            # parametri SIM dichiarati dal test
        app.settings_service.save_settings(base)
        open(os.path.join(HOME, ".f026_started"), "w").write("1")
    rt = app.runtime
    R["bankroll_syncs"] = []
    _sync = rt.risk_desk.sync_bankroll

    def caller_name():
        # PFC-008: bankroll e cassa si applicano insieme (RiskDesk.sync_funds_reading): il chiamante e' il primo
        # frame fuori dai passaggi interni (Phase 2: anche il contratto unico `_apply_funds_reading`).
        frame = sys._getframe(2)
        while frame.f_code.co_name in ("sync_funds_reading", "_sync_cash_equity_from_funds", "_apply_funds_reading"):
            frame = frame.f_back
        return frame.f_code.co_name

    def sync_rec(value):
        # registra ogni allineamento del bankroll con l'equity dell'exchange in quell'istante (anche dentro start())
        b = app.betfair_service.get_simulation_broker()
        f = dict(b.get_account_funds()) if b is not None else {}
        R["bankroll_syncs"].append({"value": value, "available": f.get("available"), "exposure": f.get("exposure"),
                                    "total": f.get("total"), "in_start": "start" not in R,
                                    "caller": caller_name()})
        return _sync(value)
    rt.risk_desk.sync_bankroll = sync_rec
    R["mm_calls"] = []
    _calc = rt.mm.calculate

    def calc_rec(*a, **k):
        # PF-ALL-F026-R1: base dello stake usata dal money management contro il disponibile dell'exchange
        b = app.betfair_service.get_simulation_broker()
        f = dict(b.get_account_funds()) if b is not None else {}
        R["mm_calls"].append({"bankroll_current": k.get("bankroll_current"), "available": f.get("available"),
                              "exposure": f.get("exposure")})
        return _calc(*a, **k)
    if json.loads(SCEN).get("exchange_down_at_start"):
        # exchange irraggiungibile durante la riconciliazione di avvio (solo per start)
        _lco = app.betfair_service.list_current_orders

        def _down(*a, **k):
            raise RuntimeError("EXCHANGE_DOWN_TEST")
        app.betfair_service.list_current_orders = _down
    R["start"] = str(rt.start(simulation_mode=True, execution_mode="SIM"))[:200]
    R["mode_after_start"] = getattr(rt.mode, "value", str(rt.mode))
    R["rebuild"] = dict(getattr(rt, "_last_position_rebuild", None) or {})
    if json.loads(SCEN).get("exchange_down_at_start"):
        app.betfair_service.list_current_orders = _lco
    _calc = rt.mm.calculate                     # dopo start(): reload_config ricrea self.mm
    rt.mm.calculate = calc_rec
    brk = app.betfair_service.get_simulation_broker()
    MODE = {}
    real = brk.place_bet

    def place_bet(*a, **k):
        m = str(k.get("market_id") or "")
        if MODE.get(m) == "lost":
            real(*a, **k); raise TimeoutError("response lost after send")
        return real(*a, **k)
    brk.place_bet = place_bet
    BOOK = {}

    def book(m, status="OPEN", winner=None, back_size=100.0):
        price = BOOK.get(m, {}).get("price", 2.0)
        if BOOK.get(m, {}).get("nomatch"):
            price = round(price - 0.2, 2)        # BACK al prezzo del segnale non trova contropartita
        brk.update_market_book({"marketId": m, "status": status, "inplay": False, "runners": [
            {"selectionId": s, "status": "ACTIVE" if status == "OPEN" else ("WINNER" if s == winner else "LOSER"),
             "ex": {"availableToBack": [{"price": price, "size": back_size}],
                    "availableToLay": [{"price": round(price + 0.02, 2), "size": 100.0}]}} for s in (11, 12)]})

    def snap(tag):
        # Confine SIM dichiarato: l'exchange e' esterno al bot e sopravvive al suo crash. Il broker SIM salva lo
        # stato solo su stop pulito o via servizio: qui lo si rende durevole dopo ogni passo, come Betfair reale.
        app.betfair_service._persist_simulation_state_if_needed(force=True)
        funds = brk.get_account_funds()
        tabs = [{"table_id": t.get("table_id"), "status": str(t.get("status")), "exposure": t.get("current_exposure"),
                 "loss": t.get("loss_amount"),
                 "market_id": t.get("market_id"), "event_key": t.get("current_event_key")}
                for t in rt.table_manager.snapshot() if "FREE" not in str(t.get("status"))]
        orders = []
        for r in app.db._execute("SELECT customer_ref, status, reason, payload_json, response_json FROM orders", fetch=True, commit=False) or []:
            r = dict(r); p = json.loads(r["payload_json"] or "{}")
            try:
                rep = (json.loads(r["response_json"] or "{}").get("instructionReports") or [{}])[0]
            except Exception:
                rep = {}
            orders.append({"market_id": p.get("market_id"), "status": r["status"], "reason": r.get("reason"), "stake": p.get("stake"), "price": p.get("price"),
                           "side": p.get("bet_type"), "matched": rep.get("sizeMatched")})
        d = rt.risk_desk
        acct = dict(getattr(rt, "_account_snapshot", None) or {})
        R["snaps"].append({"tag": tag, "mode": getattr(rt.mode, "value", str(rt.mode)),
                           "emergency": bool(getattr(rt, "_emergency_stopped", False)), "funds": funds, "bankroll": d.bankroll_current, "peak": d.equity_peak,
                           "account": acct, "cash_peak": getattr(rt, "_cash_equity_peak", None),
                           "drawdown_pct": d.drawdown_pct(), "realized": d.realized_pnl, "tables": tabs, "orders": orders,
                           "auto_resets": sum(1 for _, e, _ in EV if e == "ROSERPINA_AUTO_RESET"),
                           "daily_loss_state": {k: str(v)[:60] for k, v in (getattr(rt, "_daily_loss_monitor_state", {}) or {}).items()}})

    # #528 / PFC-001: niente attese fisse come unico sincronismo. Un passo e' concluso quando l'evento che lo chiude
    # e' stato pubblicato E il bus non ha callback in coda o in esecuzione (unfinished_tasks == 0) per 3 letture
    # consecutive a 0,1 s. Scadenza ampia e registrata in R["waits"]: un passo che non si chiude non fallisce qui,
    # lo giudicano le asserzioni dei test sullo snapshot (nessuna aspettativa cambia).
    OUTCOMES = {"QUICK_BET_SUCCESS", "QUICK_BET_FAILED", "QUICK_BET_AMBIGUOUS", "QUICK_BET_ACCEPTED",
                "QUICK_BET_PARTIAL", "QUICK_BET_FILLED", "QUICK_BET_DUPLICATE", "QUICK_BET_ROLLBACK_DONE"}
    R["waits"] = []
    # Gli esiti QUICK_BET_* non passano dall'involucro di publish qui sopra (il produttore usa un riferimento al bus
    # preso prima): si osservano con una sottoscrizione, che vede la consegna reale sullo stesso bus.
    SUB = []
    for _name in OUTCOMES | {"SIGNAL_REJECTED"}:
        app.bus.subscribe(_name, lambda d, _n=_name: SUB.append((time.time(), _n, d if isinstance(d, dict) else {})))

    def bus_idle():
        return getattr(app.bus._queue, "unfinished_tasks", 0) == 0

    def wait_for(tag, pred, deadline_s=30.0):
        t_start = time.time()
        end = t_start + deadline_s
        stable = 0
        while time.time() < end:
            if pred() and bus_idle():
                stable += 1
                if stable >= 3:
                    R["waits"].append({"tag": tag, "met": True, "s": round(time.time() - t_start, 3)})
                    return True
            else:
                stable = 0
            time.sleep(0.1)
        R["waits"].append({"tag": tag, "met": False, "s": round(time.time() - t_start, 3)})
        return False

    def since(t0, names, market=None, source=None):
        return [d for t, e, d in list(EV if source is None else source) if t >= t0 and e in names
                and (market is None or str(d.get("market_id") or market) == str(market))]

    def order_rows(market):
        rows = []
        for r in app.db._execute("SELECT customer_ref, status, payload_json FROM orders", fetch=True, commit=False) or []:
            r = dict(r)
            if str(json.loads(r["payload_json"] or "{}").get("market_id")) == str(market):
                rows.append((r["customer_ref"], r["status"]))
        return rows

    pending_settle = []
    snap("start")
    for i, st in enumerate(steps):
        op = st["op"]
        if op == "mode":
            MODE[st["m"]] = st["mode"]
            if st["mode"] == "partial":
                BOOK.setdefault(st["m"], {})["partial"] = True
            if st["mode"] == "nomatch":
                BOOK.setdefault(st["m"], {})["nomatch"] = True
            continue
        if op == "signal":
            BOOK[st["m"]] = {"price": st.get("price", 2.0), **BOOK.get(st["m"], {})}
            BOOK[st["m"]]["price"] = st.get("price", 2.0)
            book(st["m"], back_size=1.0 if BOOK[st["m"]].get("partial") else 100.0)
            side = st.get("side", "BACK")
            t_sig = time.time()
            rows_before = order_rows(st["m"])
            app.bus.publish("SIGNAL_RECEIVED", {"market_id": st["m"], "selection_id": 11, "price": st.get("price", 2.0),
                                                "side": side, "bet_type": side, "event_name": st.get("event", f"Ev{st['m']} v X"),
                                                "market_name": "Match Odds", "runner_name": f"Ev{st['m']}",
                                                "simulation_mode": True, "source": "F026"})
            # Il segnale e' concluso quando il suo ordine ha lasciato INFLIGHT (un ordine SIM a mercato resta SUBMITTED e
            # non pubblica un evento terminale), oppure c'e' un esito o un rifiuto esplicito; in ogni caso a bus fermo.
            wait_for(f"{i}:signal:{st['m']}",
                     lambda: any(r not in rows_before and r[1] != "INFLIGHT" for r in order_rows(st["m"]))
                     or since(t_sig, OUTCOMES, st["m"], SUB) or since(t_sig, {"SIGNAL_REJECTED"}, None, SUB)
                     or since(t_sig, {"SIGNAL_REJECTED"}))
        elif op == "settle":
            t_settle = time.time()
            book(st["m"], status="CLOSED", winner=11 if st.get("win") else 12)
            pending_settle.append((st["m"], t_settle))
            if st.get("nowait"):                 # piu' mercati chiusi nello stesso giro del poller
                continue
            # #461 PR29-c e #528: lo snapshot attende, per OGNI mercato chiuso dall'ultima attesa, il proprio
            # RUNTIME_CLOSE_POSITION e la fine di tutte le chiusure ricevute (AUTO_TRADE_MM_RESULT e' l'ultimo evento
            # del consumer), con il bus fermo. Prima: attesa fissa di 4 s piu' un controllo su una chiusura qualsiasi.
            first_t = min(t for _m, t in pending_settle)

            def settled():
                if not all(since(t0, {"RUNTIME_CLOSE_POSITION"}, m) for m, t0 in pending_settle):
                    return False
                closes = len(since(first_t, {"RUNTIME_CLOSE_POSITION"}))
                done = len(since(first_t, {"AUTO_TRADE_MM_RESULT"}))
                return closes > 0 and done >= closes
            wait_for(f"{i}:settle:{','.join(m for m, _t in pending_settle)}", settled)
            pending_settle = []
        elif op == "resettle":
            app.bus.publish("RUNTIME_CLOSE_POSITION", dict(next((d for _, e, d in reversed(EV)
                                                                 if e == "RUNTIME_CLOSE_POSITION" and d.get("market_id") == st["m"]), {})))
            time.sleep(2); wait_for(f"{i}:resettle", lambda: True)
        elif op == "withdraw":
            with brk._lock:
                brk.state.balance -= float(st["amount"])
            time.sleep(0.5)
        elif op == "risk_stop":
            R["risk_stop"] = str(rt.risk_stop("f026_test"))[:200]; time.sleep(4); wait_for(f"{i}:risk_stop", lambda: True)
        elif op == "cashout":
            app.bus.publish("SIGNAL_RECEIVED", {"signal_type": "CASHOUT", "event_name": f"Ev{st['m']} v X",
                                                "source": "F026"})
            time.sleep(4); wait_for(f"{i}:cashout", lambda: True)
        elif op == "replay_cmd":
            # #461 PR29-a: lo stesso CMD_QUICK_BET di un mercato ripubblicato (replay / doppione dello stesso intento)
            last = next((d for _, e, d in reversed(EV) if e == "CMD_QUICK_BET" and d.get("market_id") == st["m"]), None)
            if last is not None:
                app.bus.publish("CMD_QUICK_BET", dict(last))
            time.sleep(2); wait_for(f"{i}:replay_cmd", lambda: True)
        elif op == "crash_after_reset":
            CRASH["armed"] = True; continue
        elif op == "book_down":
            # book del mercato non disponibile allo snapshot pre-submit (PF-ALL-F030)
            _snap = app.betfair_service.get_market_book_snapshot
            app.betfair_service.get_market_book_snapshot = lambda *a, **k: None
            continue
        elif op == "crash_now":
            app.betfair_service._persist_simulation_state_if_needed(force=True)
            R["crashed_at"] = "NOW"; snap("crash"); dump(); os._exit(1)
        elif op == "crash_after_settlement":
            CRASH["after_settlement"] = True; continue
        elif op == "lockdown":
            rt.force_lockdown("f027_test"); time.sleep(0.3)
        elif op == "pause":
            rt.pause(); time.sleep(0.3)
        elif op == "emergency":
            R["emergency"] = str(rt.emergency_stop("f027_test"))[:200]; time.sleep(0.5)
        elif op == "wait":
            time.sleep(st.get("s", 2))
        snap(f"{i}:{op}:{st.get('m', '')}")
    R["stop"] = str(app.stop())[:80]
except Exception:
    R["error"] = traceback.format_exc()[-1500:]
dump()
os._exit(0)
