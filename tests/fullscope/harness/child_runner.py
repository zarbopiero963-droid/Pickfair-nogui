"""F-33 — child runner: HeadlessApp REALE in SIM (SimulationBroker, SQLite su disco) guidato da una lista di passi.

Evoluzione generalizzata di tests/f026/child_f026.py (che resta intatto e in uso dai test F026): stesso principio
(nessun calcolo atteso qui, l'oracolo vive nei test), vocabolario esteso per gli scenari full-scope (#586).

argv: TREE OUT SCENARIO_JSON HOME
SCENARIO_JSON = {"steps": [...], "settings": {...}, "seed": int, "requirements": [...], "exchange_down_at_start": bool}
Passi:
  {"op": "signal", "m": "1.701", "price": 2.0, "side": "BACK"|"LAY", "stake": 3.0?, "sel": 11?, "event": "..."?}
  {"op": "mode", "m": "1.701", "mode": "lost"|"partial"|"nomatch"|"ok"}   trasporto/liquidita' del mercato
  {"op": "book", "m": "1.701", "price": 2.2}                             nuovo book OPEN (prezzo spostato)
  {"op": "settle", "m": "1.701", "win": true, "nowait": false}          mercato CLOSED => settlement reale via poller
  {"op": "resettle", "m": "1.701"}                                       RUNTIME_CLOSE_POSITION ripubblicato (duplicato)
  {"op": "cashout", "m": "1.701"} / {"op": "cashout_all"}                 cashout parziale (evento del mercato) / totale
  {"op": "replay_cmd", "m": "1.701"}                                     stesso CMD_QUICK_BET ripubblicato
  {"op": "withdraw", "amount": 20.0}                                     variazione esterna del saldo exchange
  {"op": "risk_stop"} / {"op": "lockdown"} / {"op": "pause"} / {"op": "emergency"}
  {"op": "settings", "values": {...}}                                    save_settings a caldo
  {"op": "checkpoint", "name": "X", "hold_s": 8}                         scrive HOME/.ckpt_X e attende: il PADRE puo'
                                                                        uccidere il processo con SIGKILL (crash esterno)
  {"op": "crash_now"} / {"op": "crash_on_next_place"} / {"op": "crash_after_settlement"} / {"op": "crash_after_reset"}
  {"op": "crash_before_close_position", "m": "1.701"}                    os._exit(1) quando il consumer sta per ricevere
                                                                        RUNTIME_CLOSE_POSITION del mercato (exchange gia'
                                                                        CLOSED e regolato): sonda RST-03
  {"op": "wait", "s": 2}
Ogni passo produce uno snapshot (fondi exchange, conto, bankroll, picco, drawdown, realizzato, tavoli, ordini SQLite,
ordini e settlement dell'exchange SIM, eventi). Lo stato SIM viene reso durevole dopo ogni passo (l'exchange e'
esterno al bot e sopravvive al suo crash).
"""
import json
import os
import sys
import time
import traceback

TREE, OUT, SCEN, HOME = sys.argv[1:5]
os.makedirs(HOME, exist_ok=True)
os.chdir(HOME)
os.environ["HOME"] = HOME
sys.path.insert(0, TREE)
SPEC = json.loads(SCEN)
steps = SPEC["steps"]
first_run = not os.path.exists(os.path.join(HOME, ".fullscope_started"))
R = {"tree": TREE, "first_run": first_run, "seed": SPEC.get("seed", 0), "requirements": SPEC.get("requirements", []),
     "snaps": [], "events": [], "waits": []}
EV = []
T0 = time.time()
CRASH = {}
INTERESTING = ("QUICK_BET", "SETTLEMENT_", "EMERGENCY", "RUNTIME_LOCK", "SIGNAL_REJ", "RUNTIME_CLOSE", "ROSERPINA",
               "BANKROLL_SYNC", "RISK_STOP", "CASHOUT", "REQ_EXECUTE", "CMD_QUICK", "RUNTIME_START", "AUTO_TRADE")


def dump():
    R["events"] = [{"t": round(t - T0, 3), "e": e, "market_id": d.get("market_id"), "table_id": d.get("table_id"),
                    "reason": d.get("reason") or d.get("reason_code"), "pnl": d.get("pnl"),
                    "bankroll_after": d.get("bankroll_after"), "drawdown_pct": d.get("drawdown_pct"),
                    "customer_ref": d.get("customer_ref") or d.get("customerRef")}
                   for t, e, d in EV if e.startswith(INTERESTING)]
    with open(OUT, "w") as fh:
        json.dump(R, fh, indent=1, default=str)
    sys.stdout.flush()


def persist_exchange(app):
    app.betfair_service._persist_simulation_state_if_needed(force=True)


try:
    import headless_main
    app = headless_main.HeadlessApp()
    app.build(start_services=True)
    orig_publish = app.bus.publish

    def rec(e, d=None):
        payload = d if isinstance(d, dict) else {}
        EV.append((time.time(), e, payload))
        if e == "RUNTIME_CLOSE_POSITION" and CRASH.get("before_close_market") is not None \
                and str(payload.get("market_id")) == str(CRASH["before_close_market"]):
            # l'exchange SIM ha gia' regolato il mercato e persistito; il bot muore PRIMA che il consumer contabilizzi
            persist_exchange(app)
            R["crashed_at"] = "BEFORE_RUNTIME_CLOSE_POSITION:" + str(payload.get("market_id"))
            snap("crash")
            dump()
            os._exit(1)
        if e == "BANKROLL_SYNC_RESULT" and CRASH.get("after_settlement"):
            orig_publish(e, d)
            persist_exchange(app)
            R["crashed_at"] = "BANKROLL_SYNC_RESULT"
            dump()
            os._exit(1)
        if e == "ROSERPINA_AUTO_RESET" and CRASH.get("after_reset"):
            persist_exchange(app)
            R["crashed_at"] = "ROSERPINA_AUTO_RESET"
            dump()
            os._exit(1)
        return orig_publish(e, d)
    app.bus.publish = rec

    if first_run:
        base = {"simulation.starting_balance": 100.0, "settlement.poll_enabled": True, "settlement.poll_sec": 1}
        base.update(SPEC.get("settings") or {})
        app.settings_service.save_settings(base)
        with open(os.path.join(HOME, ".fullscope_started"), "w") as fh:
            fh.write("1")
    rt = app.runtime
    if SPEC.get("exchange_down_at_start"):
        _lco = app.betfair_service.list_current_orders

        def _down(*a, **k):
            raise RuntimeError("EXCHANGE_DOWN_TEST")
        app.betfair_service.list_current_orders = _down
    R["start"] = str(rt.start(simulation_mode=True, execution_mode="SIM"))[:300]
    R["mode_after_start"] = getattr(rt.mode, "value", str(rt.mode))
    R["rebuild"] = dict(getattr(rt, "_last_position_rebuild", None) or {})
    if SPEC.get("exchange_down_at_start"):
        app.betfair_service.list_current_orders = _lco
    brk = app.betfair_service.get_simulation_broker()
    MODE = {}
    BOOK = {}
    real_place = brk.place_bet
    real_orders = brk.place_orders

    def crash_if_armed(where):
        if CRASH.get("on_place"):
            persist_exchange(app)
            R["crashed_at"] = where
            dump()
            os._exit(1)

    def place_bet(*a, **k):
        m = str(k.get("market_id") or "")
        if MODE.get(m) == "lost":
            real_place(*a, **k)
            raise TimeoutError("response lost after send")
        out = real_place(*a, **k)
        crash_if_armed("PLACE_BET:" + m)
        return out

    def place_orders(*a, **k):
        out = real_orders(*a, **k)
        crash_if_armed("PLACE_ORDERS:" + str(k.get("market_id") or ""))
        return out
    brk.place_bet = place_bet
    brk.place_orders = place_orders

    def book(m, status="OPEN", winner=None):
        info = BOOK.setdefault(m, {"price": 2.0, "side": "BACK", "sel": 11})
        price = float(info["price"])
        side = str(info.get("side") or "BACK").upper()
        if MODE.get(m) == "nomatch":
            price = round(price - 0.2, 2) if side == "BACK" else round(price + 0.2, 2)
        size = 1.0 if MODE.get(m) == "partial" else 100.0
        # il lato del book che abbina l'ordine: BACK si abbina su availableToBack <= prezzo chiesto,
        # LAY su availableToLay >= prezzo chiesto (semantica SimulationBroker #383 F6)
        back_px = price if side == "BACK" else round(price - 0.02, 2)
        lay_px = round(price + 0.02, 2) if side == "BACK" else price
        sel = int(info.get("sel") or 11)
        runners = []
        for s in (sel, sel + 1):
            st = "ACTIVE" if status == "OPEN" else ("WINNER" if s == winner else "LOSER")
            runners.append({"selectionId": s, "status": st,
                            "ex": {"availableToBack": [{"price": back_px, "size": size}],
                                   "availableToLay": [{"price": lay_px, "size": size}]}})
        brk.update_market_book({"marketId": m, "status": status, "inplay": False, "runners": runners,
                                "event_name": info.get("event") or f"Ev{m} v X", "market_name": "Match Odds"})

    def snap(tag):
        persist_exchange(app)
        funds = brk.get_account_funds()
        tabs = [{"table_id": t.get("table_id"), "status": str(t.get("status")), "exposure": t.get("current_exposure"),
                 "loss": t.get("loss_amount"), "market_id": t.get("market_id"), "event_key": t.get("current_event_key")}
                for t in rt.table_manager.snapshot() if "FREE" not in str(t.get("status"))]
        orders = []
        rows = app.db._execute("SELECT customer_ref, status, reason, payload_json, response_json FROM orders",
                               fetch=True, commit=False) or []
        for r in rows:
            r = dict(r)
            p = json.loads(r["payload_json"] or "{}")
            try:
                rep = (json.loads(r["response_json"] or "{}").get("instructionReports") or [{}])[0]
            except Exception:  # noqa: BLE001
                rep = {}
            orders.append({"market_id": p.get("market_id"), "status": r["status"], "reason": r.get("reason"),
                           "stake": p.get("stake"), "price": p.get("price"), "side": p.get("bet_type") or p.get("side"),
                           "matched": rep.get("sizeMatched"), "customer_ref": r.get("customer_ref")})
        d = rt.risk_desk
        acct = dict(getattr(rt, "_account_snapshot", None) or {})
        exch = [{"bet_id": o.get("betId"), "ref": o.get("customerOrderRef"), "market_id": o.get("marketId"),
                 "selection_id": o.get("selectionId"), "side": o.get("side"),
                 "price": (o.get("priceSize") or {}).get("price"), "size": (o.get("priceSize") or {}).get("size"),
                 "matched": o.get("sizeMatched"), "status": o.get("status")}
                for o in (brk.list_current_orders().get("currentOrders") or [])]
        settled = [{"market_id": x.get("market_id"), "settlement_id": x.get("settlement_id"),
                    "gross_pnl": x.get("gross_pnl"), "net_pnl": x.get("net_pnl")} for x in brk.list_settlements()]
        R["snaps"].append({"tag": tag, "mode": getattr(rt.mode, "value", str(rt.mode)),
                           "exch_orders": exch, "exch_settlements": settled,
                           "emergency": bool(getattr(rt, "_emergency_stopped", False)), "funds": funds,
                           "bankroll": d.bankroll_current, "peak": d.equity_peak, "account": acct,
                           "cash_peak": getattr(rt, "_cash_equity_peak", None), "drawdown_pct": d.drawdown_pct(),
                           "realized": d.realized_pnl, "tables": tabs, "orders": orders,
                           "auto_resets": sum(1 for _, e, _ in EV if e == "ROSERPINA_AUTO_RESET"),
                           "reconciliation_required": bool(getattr(rt, "_reconciliation_required", False))})

    OUTCOMES = {"QUICK_BET_SUCCESS", "QUICK_BET_FAILED", "QUICK_BET_AMBIGUOUS", "QUICK_BET_ACCEPTED",
                "QUICK_BET_PARTIAL", "QUICK_BET_FILLED", "QUICK_BET_DUPLICATE", "QUICK_BET_ROLLBACK_DONE"}
    SUB = []
    for _name in OUTCOMES | {"SIGNAL_REJECTED"}:
        app.bus.subscribe(_name, lambda d, _n=_name: SUB.append((time.time(), _n, d if isinstance(d, dict) else {})))

    def bus_idle():
        return getattr(app.bus._queue, "unfinished_tasks", 0) == 0

    def wait_for(tag, pred, deadline_s=30.0):
        t_start = time.time()
        stable = 0
        while time.time() < t_start + deadline_s:
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
        m = st.get("m", "")
        if op == "mode":
            MODE[m] = st["mode"]
            continue
        if op in ("crash_on_next_place", "crash_after_settlement", "crash_after_reset", "crash_before_close_position"):
            key = {"crash_on_next_place": "on_place", "crash_after_settlement": "after_settlement",
                   "crash_after_reset": "after_reset", "crash_before_close_position": "before_close_market"}[op]
            CRASH[key] = m if op == "crash_before_close_position" else True
            continue
        if op == "signal":
            side = str(st.get("side", "BACK")).upper()
            BOOK[m] = {"price": st.get("price", 2.0), "side": side, "sel": st.get("sel", 11),
                       "event": st.get("event")}
            book(m)
            t_sig = time.time()
            rows_before = order_rows(m)
            sig = {"market_id": m, "selection_id": int(st.get("sel", 11)), "price": st.get("price", 2.0),
                   "side": side, "bet_type": side, "event_name": st.get("event") or f"Ev{m} v X",
                   "market_name": "Match Odds", "runner_name": f"Ev{m}", "simulation_mode": True,
                   "source": st.get("source", "FULLSCOPE")}
            if "stake" in st:
                sig["stake"] = st["stake"]
            app.bus.publish("SIGNAL_RECEIVED", sig)
            wait_for(f"{i}:signal:{m}",
                     lambda: any(r not in rows_before and r[1] != "INFLIGHT" for r in order_rows(m))
                     or since(t_sig, OUTCOMES, m, SUB) or since(t_sig, {"SIGNAL_REJECTED"}, None, SUB)
                     or since(t_sig, {"SIGNAL_REJECTED"}))
        elif op == "book":
            BOOK.setdefault(m, {"price": 2.0, "side": "BACK", "sel": 11})["price"] = st.get("price", 2.0)
            book(m)
            time.sleep(0.5)
            wait_for(f"{i}:book:{m}", lambda: True)
        elif op == "settle":
            t_settle = time.time()
            sel = int(BOOK.get(m, {}).get("sel") or 11)
            book(m, status="CLOSED", winner=sel if st.get("win") else sel + 1)
            pending_settle.append((m, t_settle))
            if st.get("nowait"):
                continue
            first_t = min(t for _m, t in pending_settle)

            def settled():
                if not all(since(t0, {"RUNTIME_CLOSE_POSITION"}, mm) for mm, t0 in pending_settle):
                    return False
                closes = len(since(first_t, {"RUNTIME_CLOSE_POSITION"}))
                done = len(since(first_t, {"AUTO_TRADE_MM_RESULT"}))
                return closes > 0 and done >= closes
            wait_for(f"{i}:settle:{','.join(mm for mm, _t in pending_settle)}", settled)
            pending_settle = []
        elif op == "resettle":
            last = next((d for _, e, d in reversed(EV) if e == "RUNTIME_CLOSE_POSITION" and d.get("market_id") == m), {})
            app.bus.publish("RUNTIME_CLOSE_POSITION", dict(last))
            time.sleep(2)
            wait_for(f"{i}:resettle", lambda: True)
        elif op == "withdraw":
            with brk._lock:
                brk.state.balance -= float(st["amount"])
            time.sleep(0.5)
        elif op == "risk_stop":
            R["risk_stop"] = str(rt.risk_stop("fullscope_test"))[:200]
            time.sleep(4)
            wait_for(f"{i}:risk_stop", lambda: True)
        elif op == "cashout":
            ev = BOOK.get(m, {}).get("event") or f"Ev{m} v X"
            t_c = time.time()
            app.bus.publish("SIGNAL_RECEIVED", {"signal_type": "CASHOUT", "event_name": ev, "source": "FULLSCOPE"})
            wait_for(f"{i}:cashout:{m}", lambda: since(t_c, {"CASHOUT_SUCCESS", "CASHOUT_FAILED", "CASHOUT_REQUEST_SUPPRESSED"}))
            time.sleep(1.0)
        elif op == "cashout_all":
            t_c = time.time()
            app.bus.publish("SIGNAL_RECEIVED", {"signal_type": "CASHOUT_ALL", "source": "FULLSCOPE"})
            time.sleep(4)
            wait_for(f"{i}:cashout_all", lambda: True)
        elif op == "replay_cmd":
            last = next((d for _, e, d in reversed(EV) if e == "CMD_QUICK_BET" and d.get("market_id") == m), None)
            if last is not None:
                app.bus.publish("CMD_QUICK_BET", dict(last))
            time.sleep(2)
            wait_for(f"{i}:replay_cmd", lambda: True)
        elif op == "settings":
            app.settings_service.save_settings(dict(st.get("values") or {}))
            time.sleep(0.3)
        elif op == "checkpoint":
            persist_exchange(app)
            snap(f"{i}:checkpoint:{st['name']}")
            dump()
            with open(os.path.join(HOME, f".ckpt_{st['name']}"), "w") as fh:
                fh.write(str(os.getpid()))
            time.sleep(float(st.get("hold_s", 8)))      # il padre puo' uccidere il processo qui (SIGKILL)
            continue
        elif op == "crash_now":
            persist_exchange(app)
            R["crashed_at"] = "NOW"
            snap("crash")
            dump()
            os._exit(1)
        elif op == "lockdown":
            rt.force_lockdown("fullscope_test")
            time.sleep(0.3)
        elif op == "pause":
            rt.pause()
            time.sleep(0.3)
        elif op == "emergency":
            R["emergency"] = str(rt.emergency_stop("fullscope_test"))[:200]
            time.sleep(0.5)
        elif op == "wait":
            time.sleep(float(st.get("s", 2)))
        else:
            raise ValueError(f"passo sconosciuto: {op}")
        snap(f"{i}:{op}:{m}")
    R["stop"] = str(app.stop())[:120]
except Exception:  # noqa: BLE001 - l'errore e' dato del risultato, giudicato dal test
    R["error"] = traceback.format_exc()[-2500:]
dump()
os._exit(0)
