"""Oracolo INDIPENDENTE della campagna PF-ROSERPINA-41-FULL-SIMULATION-02.

Non importa codice Pickfair. Ricalcola in Decimal, dai soli fatti osservati (scenario dichiarato, registro ordini
dell'«exchange» SIM, istanti degli eventi, esiti dichiarati dei mercati), cio' che il bot DOVEVA rispettare, e lo
confronta con cio' che e' successo.

Esiti per controllo: PASS | VIOLATION | KNOWN (difetto gia' registrato, es. F027) | NA (non esercitato) |
BLOCKED (non valutabile: dato mancante, harness).
Semantica dei limiti (dal contratto del codice, documentata nel report):
- esposizione ordine: BACK = stake, LAY = stake*(quota-1) (validators.order_exposure, #461 PR28);
- vincita: BACK = stake*(quota-1), LAY = stake; MAX_WIN e' blocco hard su vincita ED esposizione (H-14);
- cap singolo = min(B*max_single_bet_pct, max_stake_abs); totale = B*max_total_exposure_pct; evento =
  B*max_event_exposure_pct, con B = bankroll del MM all'istante della decisione;
- max_open_exposure (euro assoluti) sul totale aperto ordine incluso; max_exposure_stop: totale aperto (ordine
  incluso) che RAGGIUNGE il limite => stop pre-submit, quindi ogni ordine piazzato ha totale < limite;
- commissione 4.5% sul netto POSITIVO del mercato, una volta per mercato.
"""
import json
import os
import sys
from decimal import Decimal as D, getcontext

getcontext().prec = 34
EPS = D("0.000001")
CENT = D("0.01")


def d(x):
    return D(str(x)) if x is not None else None


def exposure(side, stake, price):
    return stake if side == "BACK" else stake * (price - 1)


def win_of(side, stake, price):
    return stake * (price - 1) if side == "BACK" else stake


class Check:
    def __init__(self):
        self.res = {}

    def put(self, name, status, detail=""):
        cur = self.res.get(name)
        rank = {"VIOLATION": 4, "BLOCKED": 3, "KNOWN": 2, "PASS": 1, "NA": 0}
        if cur is None or rank[status] > rank[cur["status"]]:
            self.res[name] = {"status": status, "detail": str(detail)[:400]}

    def ok(self, name):
        self.put(name, "PASS")


def market_pnl(orders, winner_is_11):
    """Lordo del mercato dagli ordini MATCHATI (prezzo medio), selezione 11."""
    gross = D(0)
    for o in orders:
        m = d(o.get("matched_size") or 0)
        if m <= 0:
            continue
        p = d(o.get("avg_price_matched") or o.get("price"))
        on11 = int(o.get("selection_id") or 11) == 11
        sel_wins = winner_is_11 if on11 else not winner_is_11
        if o["side"] == "BACK":
            gross += m * (p - 1) if sel_wins else -m
        else:
            gross += -m * (p - 1) if sel_wins else m
    comm = (gross * D("0.045")) if gross > 0 else D(0)
    return gross, comm, gross - comm


def judge(sdir):
    spec = json.load(open(os.path.join(sdir, "spec.json")))
    run = json.load(open(os.path.join(sdir, "run.json")))
    C = Check()
    S = spec["settings"]
    cap = d(spec["capital"])
    outs = []
    for ph in range(len(spec["phases"])):
        p = os.path.join(sdir, f"out{ph}.json")
        if not os.path.exists(p):
            C.put("H0_harness", "BLOCKED", f"phase {ph} senza output: {run['phases']}")
            return finish(spec, C, outs)
        outs.append(json.load(open(p)))
    for ph, o in enumerate(outs):
        if o.get("error"):
            C.put("H0_harness", "BLOCKED", f"phase {ph} error: {o['error'][-300:]}")
        if o.get("dump_error"):
            C.put("H0_harness", "BLOCKED", f"phase {ph} dump_error")
        last = ph == len(outs) - 1
        if last and not (o.get("stop") or {}).get("done"):
            C.put("H1_clean_stop", "VIOLATION", f"stop non completato: {o.get('stop')}")
        if (ph < len(outs) - 1) and not o.get("crashed"):
            C.put("H0_harness", "BLOCKED", "crash atteso non avvenuto")
    if "H0_harness" in C.res:
        return finish(spec, C, outs)
    C.ok("H0_harness")
    if "H1_clean_stop" not in C.res:
        C.ok("H1_clean_stop")

    final = outs[-1]
    border = final.get("broker_orders") or []
    entries = [o for o in border if str(o.get("customer_ref") or "").startswith("sig-")]
    hedges = [o for o in border if o not in entries]
    min_price = d(S.get("roserpina.min_price", 1.01))
    max_win = d(S.get("roserpina.max_win", 10000.0))
    single_pct = d(S["roserpina.max_single_bet_pct"]) / 100
    total_pct = d(S["roserpina.max_total_exposure_pct"]) / 100
    event_pct = d(S["roserpina.max_event_exposure_pct"]) / 100
    stake_abs = d(S.get("roserpina.max_stake_abs") or 0)
    open_abs = d(S.get("roserpina.max_open_exposure")) if S.get("roserpina.max_open_exposure") is not None else None
    stop_abs = d(S.get("roserpina.max_exposure_stop")) if S.get("roserpina.max_exposure_stop") is not None else None
    daily = d(S.get("roserpina.max_daily_loss")) if S.get("roserpina.max_daily_loss") is not None else None
    session = d(S.get("roserpina.max_session_loss")) if S.get("roserpina.max_session_loss") is not None else None
    tables = int(S["roserpina.table_count"])

    # ---- C01 nessun duplicato (registro exchange, tutte le fasi)
    keys = {}
    for o in entries:
        k = (o["market_id"], int(o.get("selection_id") or 0), o["side"])
        keys[k] = keys.get(k, 0) + 1
    dups = {k: v for k, v in keys.items() if v > 1}
    C.put("C01_no_duplicate_order", "VIOLATION" if dups else "PASS", dups)
    refs = [o.get("customer_ref") for o in entries]
    C.put("C01b_unique_customer_ref", "VIOLATION" if len(refs) != len(set(refs)) else "PASS")

    # ---- per ordine d'ingresso: cap e gate al momento della decisione
    outcome = {m: bool(w) for m, w in spec["outcomes"].items()}
    by_ref = {o["customer_ref"]: o for o in entries}
    cum_loss_daily = D(0)
    carried_open = {}            # ref -> (market, exposure) aperti a fine fase precedente
    risk_stopped = False
    placed_seen = 0
    max_concurrent = 0
    for ph, o in enumerate(outs):
        ev = o.get("events") or []
        steps = o.get("steps") or []
        mm = [c for c in (o.get("mm_calls") or []) if c.get("approved")]
        mode0 = o.get("mode_after_start")
        settle_t = {s["m"]: s["t"] for s in steps if s["op"] == "settle"}
        booked = {s["m"]: s for s in steps if s["op"] == "settle"}
        open_now = dict(carried_open)
        cum_loss_session = D(0)
        failed_refs = {e.get("ref") for e in ev if e["e"] in ("QUICK_BET_FAILED",)}
        timeline = []
        for e in ev:
            if e["e"] == "CMD_QUICK_BET":
                timeline.append((e["t"], 1, "cmd", e))
            elif e["e"] == "RISK_STOP_TRIGGERED":
                timeline.append((e["t"], 0, "riskstop", e))
        for s in steps:
            if s["op"] == "settle":
                timeline.append((s["t"], 0, "settle", s))
            if s["op"] == "risk_stop":
                timeline.append((s["t"], 0, "riskstop", s))
        timeline.sort(key=lambda x: (x[0], x[1]))
        for t, _, kind, item in timeline:
            if kind == "riskstop":
                risk_stopped = True
                continue
            if kind == "settle":
                m = item["m"]
                for r in [r for r, (mk, _) in open_now.items() if mk == m]:
                    open_now.pop(r)
                mo = [x for x in border if x["market_id"] == m]
                if mo and item.get("booked"):
                    _, _, net = market_pnl(mo, outcome[m])
                    if net < 0:
                        cum_loss_daily += -net; cum_loss_session += -net
                    else:
                        cum_loss_daily -= net; cum_loss_session -= net
                continue
            # ---- ingresso
            ref = item.get("ref")
            if ref in failed_refs:
                continue          # negato in questa fase: l'eventuale ordine con la stessa ref nasce in un'altra fase
            od = by_ref.get(ref)
            if od is None:
                # comando senza ordine sull'exchange: rifiutato prima dell'invio o non arrivato
                continue
            placed_seen += 1
            side, stake, price = od["side"], d(od["size"]), d(od["price"])
            ex = exposure(side, stake, price)
            # modo al momento: ultimo step iniziato prima del comando
            prev = [s for s in steps if s["t"] <= t]
            mode_before = mode0
            if len(prev) >= 2:
                mode_before = prev[-2].get("mode", mode0)
            if mode_before != "ACTIVE":
                C.put("C11_no_entry_when_not_active", "VIOLATION", f"{ref} in mode {mode_before}")
            else:
                C.ok("C11_no_entry_when_not_active")
            if risk_stopped:
                C.put("C12_no_entry_after_risk_stop", "VIOLATION", ref)
            else:
                C.ok("C12_no_entry_after_risk_stop")
            # bankroll della decisione MM (ultima approvazione con lo stesso stake prima del comando)
            cand = [c for c in mm if c["t"] <= t + 0.001 and c.get("stake") is not None
                    and abs(float(c["stake"]) - float(od["size"])) < 1e-6]
            if not cand:
                C.put("C02_single_cap", "BLOCKED", f"{ref}: decisione MM non abbinata")
                continue
            B = d(cand[-1]["bankroll_current"])
            single = B * single_pct
            if stake_abs > 0:
                single = min(single, stake_abs)
            C.put("C02_single_cap", "VIOLATION" if ex > single + EPS else "PASS", f"{ref} exp={ex} cap={single}")
            C.put("C03_max_win_hard", "VIOLATION" if (win_of(side, stake, price) > max_win + EPS or ex > max_win + EPS)
                  else "PASS", f"{ref} win={win_of(side, stake, price)} exp={ex} max_win={max_win}")
            C.put("C04_min_price", "VIOLATION" if price < min_price else "PASS", f"{ref} {price}<{min_price}")
            tot = sum((x for _, x in open_now.values()), D(0)) + ex
            C.put("C05_total_exposure_cap", "VIOLATION" if tot > B * total_pct + EPS else "PASS",
                  f"{ref} open+this={tot} cap={B * total_pct}")
            C.put("C06_event_exposure_cap", "VIOLATION" if ex > B * event_pct + EPS else "PASS",
                  f"{ref} exp={ex} cap={B * event_pct}")
            if open_abs is not None:
                C.put("C07_max_open_exposure_abs", "VIOLATION" if tot > open_abs + EPS else "PASS",
                      f"{ref} {tot}>{open_abs}")
            if stop_abs is not None:
                C.put("C08_exposure_stop_before_submit", "VIOLATION" if tot >= stop_abs - EPS else "PASS",
                      f"{ref} {tot}>={stop_abs}")
            conc = len(open_now) + 1
            max_concurrent = max(max_concurrent, conc)
            C.put("C09_tables_bound", "VIOLATION" if conc > tables else "PASS", f"{conc}>{tables}")
            if daily is not None:
                C.put("C10_daily_loss_stop", "VIOLATION" if cum_loss_daily >= daily - EPS else "PASS",
                      f"{ref} perdita giornaliera {cum_loss_daily} >= {daily} (fase {ph})")
            if session is not None:
                C.put("C10b_session_loss_stop", "VIOLATION" if cum_loss_session >= session - EPS else "PASS",
                      f"{ref} perdita sessione {cum_loss_session} >= {session}")
            # C14 (F030): liquidita' eseguibile ricalcolata dal book DICHIARATO dello scenario.
            if S.get("roserpina.liquidity_guard_enabled", True) and not S.get("roserpina.liquidity_warning_only", True):
                bk = spec["books"].get(od["market_id"], {})
                size = D(str(bk.get("back_size", 100.0)))
                for st in spec["phases"][ph] if ph < len(spec["phases"]) else []:
                    if st.get("op") == "mode" and st.get("m") == od["market_id"] and st.get("mode") == "partial":
                        size = D(str(st.get("size", 1.0)))
                liq = size if side == "BACK" else D(0)          # LAY: availableToLay a quota+0.02, mai eseguibile
                need = max(stake * d(S.get("roserpina.liquidity_multiplier", 3.0)),
                           d(S.get("roserpina.min_liquidity_absolute", 50.0)))
                C.put("C14_liquidity_guard_single_entry", "VIOLATION" if liq < need else "PASS",
                      f"{ref} liq={liq} need={need}")
            if od.get("market_id") in settle_t and settle_t[od["market_id"]] < t:
                C.put("C13_no_entry_on_closed_market", "VIOLATION", ref)
            open_now[ref] = (od["market_id"], ex)
        carried_open = open_now

    # ---- contabilita' indipendente (fine scenario: tutti i mercati regolati)
    mkts = sorted({o["market_id"] for o in border})
    ledger = (final.get("broker_state") or {}).get("market_commission_ledger") or {}
    tot_net = D(0)
    for m in mkts:
        mo = [x for x in border if x["market_id"] == m]
        gross, comm, net = market_pnl(mo, outcome[m])
        settled = any(x.get("status") == "SETTLED" for x in mo) or m in ledger
        matched = any(d(x.get("matched_size") or 0) > 0 for x in mo)
        if not matched:
            continue
        if not settled:
            C.put("A0_market_settled_on_exchange", "VIOLATION", f"{m} non regolato")
            continue
        tot_net += net
        row = ledger.get(m)
        if row is None:
            C.put("A1_gross_matches_oracle", "BLOCKED", f"{m} assente dal ledger")
            continue
        C.put("A1_gross_matches_oracle", "VIOLATION" if abs(d(row["gross"]) - gross) > CENT else "PASS",
              f"{m} sim={row['gross']} oracle={gross}")
        C.put("A2_commission_once_4_5_on_positive_net",
              "VIOLATION" if abs(d(row["commission"]) - comm) > CENT else "PASS",
              f"{m} sim={row['commission']} oracle={comm}")
    funds = final.get("broker_funds") or {}
    if funds.get("total") is not None:
        C.put("A3_conservation_total", "VIOLATION" if abs(d(funds["total"]) - (cap + tot_net)) > CENT else "PASS",
              f"total={funds['total']} oracle={cap + tot_net}")
    # contabilizzazione lato bot dei settlement
    for ph, o in enumerate(outs):
        for s in o.get("steps") or []:
            if s["op"] != "settle":
                continue
            mo = [x for x in border if x["market_id"] == s["m"] and d(x.get("matched_size") or 0) > 0]
            if not mo:
                continue
            if s.get("booked"):
                C.ok("A4_settlement_booked_by_bot")
            elif s.get("mode_before") not in ("ACTIVE", None):
                C.put("A4_settlement_booked_by_bot", "KNOWN", f"F027: {s['m']} in {s.get('mode_before')}")
            else:
                C.put("A4_settlement_booked_by_bot", "VIOLATION", f"{s['m']} fase {ph} non contabilizzato")
    rt = final.get("runtime") or {}
    if rt.get("realized") is not None and "A4_settlement_booked_by_bot" in C.res and \
            C.res["A4_settlement_booked_by_bot"]["status"] == "PASS":
        # realized del RiskDesk e' di sessione (dall'ultimo start): si confronta con i mercati contabilizzati
        # nell'ultima fase. La perdita giornaliera attraverso il riavvio e' verificata da C10.
        # ...e l'auto-reset azzera il realized del ciclo (reset_recovery_cycle; le perdite restano nella baseline
        # giornaliera): si contano solo i settlement dopo l'ultimo ROSERPINA_AUTO_RESET.
        resets = [e["t"] for e in outs[-1].get("events") or [] if e["e"] == "ROSERPINA_AUTO_RESET"]
        t_reset = max(resets) if resets else -1.0
        sess = D(0)
        for s in outs[-1].get("steps") or []:
            if s["op"] == "settle" and s.get("booked") and s["t"] > t_reset:
                mo = [x for x in border if x["market_id"] == s["m"]]
                if mo:
                    sess += market_pnl(mo, outcome[s["m"]])[2]
        C.put("A5_runtime_realized_equals_exchange_net",
              "VIOLATION" if abs(d(rt["realized"]) - sess) > CENT else "PASS",
              f"runtime={rt['realized']} oracle_session={sess} fasi={len(outs)}")
    for o in outs:
        for sy in o.get("bankroll_syncs") or []:
            if sy.get("caller") in ("start", "_sync_bankroll_post_settlement", "get_status") and sy.get("available") is not None:
                C.put("A6_stake_base_is_available", "VIOLATION" if abs(d(sy["value"]) - d(sy["available"])) > CENT
                      else "PASS", f"{sy}")
    # tavoli liberati a fine scenario (tranne riserve incerte dichiarate)
    tabs = [t for t in rt.get("tables") or [] if t.get("event_key")]
    uncertain_mkts = {x["market_id"] for x in border if spec["phases"] and
                      any(st.get("op") == "mode" and st.get("m") == x["market_id"] and st.get("mode") in ("lost", "unknown")
                          for st in spec["phases"][0])}
    leaked = [t for t in tabs if t.get("market_id") not in uncertain_mkts]
    zero_matched = {x["market_id"] for x in entries if d(x.get("matched_size") or 0) == 0
                    and x.get("status") in ("LAPSED", "CANCELLED")}
    f027 = {s["m"] for o in outs for s in o.get("steps") or []
            if s["op"] == "settle" and not s.get("booked") and s.get("mode_before") not in ("ACTIVE", None)}
    real_leak = [t for t in leaked if t.get("market_id") not in zero_matched and t.get("market_id") not in f027]
    f027_leak = [t for t in leaked if t.get("market_id") in f027]
    if any(s["op"] == "settle" for s in spec["phases"][-1]):
        if real_leak:
            C.put("A7_no_leaked_table_after_settlement", "VIOLATION", real_leak)
        elif f027_leak:
            C.put("A7_no_leaked_table_after_settlement", "KNOWN", f"F027 settlement non contabilizzato fuori ACTIVE: {f027_leak}")
        elif leaked:
            C.put("A7_no_leaked_table_after_settlement", "KNOWN", f"F028 zero-matched LAPSED/CANCELLED: {leaked}")
        else:
            C.ok("A7_no_leaked_table_after_settlement")
    # crash/riavvio
    if len(outs) > 1:
        p1 = outs[1]
        C.put("R1_restart_rebuild_ok", "PASS" if (p1.get("runtime") or {}).get("rebuild", {}).get("ok", True) else "KNOWN",
              (p1.get("runtime") or {}).get("rebuild"))
        resent = [s for s in spec["phases"][1] if s["op"] == "signal"]
        for s in resent:
            pre = [x for x in border if x["market_id"] == s["m"] and str(x.get("customer_ref", "")).startswith("sig-")]
            C.put("R2_no_replay_after_restart", "VIOLATION" if len(pre) > 1 else "PASS", f"{s['m']}: {len(pre)}")
    C.res["_stats"] = {"status": "NA", "detail": json.dumps({"entries": len(entries), "hedges": len(hedges),
                                                               "placed_seen": placed_seen, "max_concurrent": max_concurrent})}
    return finish(spec, C, outs)


def finish(spec, C, outs):
    sts = [v["status"] for k, v in C.res.items() if not k.startswith("_")]
    verdict = ("VIOLATION" if "VIOLATION" in sts else "BLOCKED" if "BLOCKED" in sts
               else "PASS_WITH_KNOWN" if "KNOWN" in sts else "PASS")
    return {"id": spec["id"], "capital": spec["capital"], "profile": spec["profile"], "recovery": spec["recovery"],
            "tables": spec["tables"], "capmode": spec["capmode"], "sequence": spec["sequence"],
            "levels": spec["levels"], "verdict": verdict, "checks": C.res}


if __name__ == "__main__":
    print(json.dumps(judge(sys.argv[1]), indent=1))
