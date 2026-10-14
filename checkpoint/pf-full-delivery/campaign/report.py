"""Report della campagna: per capitale, per controllo, matrice RSR-01..41 x percorsi, influenza osservata.
Uso: python report.py <outdir> <inventory.json> <report.json> <report.md>
"""
import collections, glob, json, os, random, sys

OUT, INV, RJ, RM = sys.argv[1:5]
verd = {r["id"]: r for r in json.load(open(os.path.join(OUT, "verdicts.json")))}
inv = json.load(open(INV))

PATHS = ["entry_placed", "entry_blocked_mm", "entry_denied_engine", "duplicate_blocked", "settle_win", "settle_loss",
         "commission_charged", "partial_match", "unmatched_lapsed_or_cancelled", "rejected_by_exchange",
         "ambiguous_outcome", "cashout_hedge", "risk_stop", "lockdown", "auto_reset", "crash_restart",
         "no_table_available", "daily_or_session_stop"]


def paths_of(sdir, spec):
    outs = []
    for ph in range(len(spec["phases"])):
        p = os.path.join(sdir, f"out{ph}.json")
        if os.path.exists(p):
            outs.append(json.load(open(p)))
    if not outs:
        return set(), collections.Counter()
    f = set(); reasons = collections.Counter()
    final = outs[-1]
    border = final.get("broker_orders") or []
    entries = [o for o in border if str(o.get("customer_ref", "")).startswith("sig-")]
    if entries: f.add("entry_placed")
    if any(0 < (o.get("matched_size") or 0) < (o.get("size") or 0) - 1e-9 for o in entries): f.add("partial_match")
    if any((o.get("matched_size") or 0) == 0 and o.get("status") in ("LAPSED", "CANCELLED") for o in entries):
        f.add("unmatched_lapsed_or_cancelled")
    if any(not str(o.get("customer_ref", "")).startswith("sig-") for o in border): f.add("cashout_hedge")
    led = (final.get("broker_state") or {}).get("market_commission_ledger") or {}
    if any(float(r.get("gross", 0)) > 0 for r in led.values()): f.add("settle_win")
    if any(float(r.get("gross", 0)) < 0 for r in led.values()): f.add("settle_loss")
    if any(float(r.get("commission", 0)) > 0 for r in led.values()): f.add("commission_charged")
    if len(outs) > 1: f.add("crash_restart")
    for o in outs:
        for e in o.get("events") or []:
            n = e["e"]
            if n == "SIGNAL_REJECTED":
                r = str(e.get("reason") or "").split(":")[0]
                reasons["mm:" + r] += 1
                if r == "duplicato_bloccato": f.add("duplicate_blocked")
                elif r == "nessun_tavolo_disponibile": f.add("no_table_available")
                elif r.startswith(("daily_loss", "session_loss", "max_daily", "max_session")): f.add("daily_or_session_stop")
                else: f.add("entry_blocked_mm")
            elif n in ("QUICK_BET_DUPLICATE",): f.add("duplicate_blocked")
            elif n == "QUICK_BET_AMBIGUOUS": f.add("ambiguous_outcome")
            elif n == "RISK_STOP_TRIGGERED": f.add("risk_stop")
            elif n == "ROSERPINA_AUTO_RESET": f.add("auto_reset")
            elif n in ("SESSION_LOSS_BREACH_TRIGGERED", "DAILY_LOSS_BREACH_TRIGGERED"): f.add("daily_or_session_stop")
        for s in o.get("steps") or []:
            if s.get("mode") == "LOCKDOWN": f.add("lockdown")
            if s["op"] == "risk_stop": f.add("risk_stop")
        for r in o.get("db_orders") or []:
            if r.get("status") == "DENIED":
                f.add("entry_denied_engine"); reasons["engine:" + str((r.get("extra") or {}).get("reason"))] += 1
            if r.get("status") == "FAILED": f.add("rejected_by_exchange")
    return f, reasons


rows = []
for sdir in sorted(glob.glob(os.path.join(OUT, "[SDXF]*"))):
    sid = os.path.basename(sdir)
    spec = json.load(open(os.path.join(sdir, "spec.json")))
    v = verd.get(sid) or {"verdict": "BLOCKED", "checks": {}}
    f, reasons = paths_of(sdir, spec)
    rows.append({"id": sid, "spec": spec, "verdict": v["verdict"], "checks": v["checks"], "paths": sorted(f),
                 "reasons": dict(reasons)})

# ---------------------------------------------------------------- per capitale
by_cap = collections.defaultdict(collections.Counter)
for r in rows:
    by_cap[r["spec"]["capital"]][r["verdict"]] += 1
    by_cap[r["spec"]["capital"]]["total"] += 1
# ---------------------------------------------------------------- per controllo
by_check = collections.defaultdict(collections.Counter)
viol = []
for r in rows:
    for k, c in r["checks"].items():
        if k.startswith("_"):
            continue
        by_check[k][c["status"]] += 1
        if c["status"] in ("VIOLATION", "BLOCKED"):
            viol.append({"id": r["id"], "check": k, "status": c["status"], "detail": c["detail"]})
# ---------------------------------------------------------------- matrice RSR x percorsi
GROUP = {"roserpina.target_profit_cycle_pct": "profile", "roserpina.max_single_bet_pct": "profile",
         "roserpina.max_total_exposure_pct": "profile", "roserpina.max_event_exposure_pct": "profile",
         "roserpina.auto_reset_drawdown_pct": "profile", "roserpina.defense_drawdown_pct": "profile",
         "roserpina.lockdown_drawdown_pct": "profile", "roserpina.max_stake_pct": "profile",
         "roserpina.risk_profile": "profile", "roserpina.table_count": "tables",
         "roserpina.max_recovery_tables": "recovery", "roserpina.allow_recovery": "recovery",
         "roserpina.max_stake_abs": "capmode", "roserpina.max_open_exposure": "capmode"}


def group_value(r, key):
    s = r["spec"]
    if key in s.get("levels", {}):
        return f"L{s['levels'][key]}"
    g = GROUP.get(key)
    if g:
        return str(s.get(g))
    if key in s.get("settings", {}):
        return "fixed:" + str(s["settings"][key])
    return None


base_rows = [r for r in rows if r["id"].startswith("S")]
matrix = []
for it in inv:
    key = it["settings_key"]
    entry = {"id": it["id"], "name": it["name"], "key": key, "status_at_head": it.get("status")}
    if it.get("status") == "MISSING":
        entry.update(campaign="BLOCKED", reason="parametro assente al HEAD (solo PR #505): non configurabile, non testabile")
        matrix.append(entry); continue
    groups = collections.defaultdict(list)
    for r in base_rows:
        gv = group_value(r, key)
        if gv is not None:
            groups[gv].append(r)
    entry["levels"] = {g: len(v) for g, v in groups.items()}
    entry["paths"] = {}
    for p in PATHS:
        entry["paths"][p] = {g: sum(1 for r in v if p in r["paths"]) for g, v in groups.items()}
    entry["verdicts"] = {g: dict(collections.Counter(r["verdict"] for r in v)) for g, v in groups.items()}
    if len(groups) > 1:
        feats = sorted(set(PATHS) | {k for r in base_rows for k in r["reasons"]})
        labelled = [(g, r) for g, v in groups.items() for r in v]

        def max_diff(pairs):
            best = 0.0
            for ft in feats:
                acc = collections.defaultdict(lambda: [0, 0])
                for g, r in pairs:
                    acc[g][1] += 1
                    if ft in r["paths"] or r["reasons"].get(ft):
                        acc[g][0] += 1
                fr = [a / max(1, b) for a, b in acc.values()]
                best = max(best, max(fr) - min(fr))
            return best
        infl = max_diff(labelled)
        rnd = random.Random(f"perm:{key}")
        labels = [g for g, _ in labelled]
        null = []
        for _ in range(40):                       # soglia di rumore: etichette permutate
            rnd.shuffle(labels)
            null.append(max_diff(list(zip(labels, [r for _, r in labelled]))))
        null.sort()
        floor = null[int(0.95 * (len(null) - 1))]
        entry["influence"] = round(infl, 3)
        entry["noise_floor_p95"] = round(floor, 3)
        entry["campaign"] = ("VARIED_INFLUENCE_OBSERVED" if infl > floor
                             else "VARIED_NO_INFLUENCE_ABOVE_NOISE")
    else:
        entry["campaign"] = "FIXED_SINGLE_VALUE" if groups else "NOT_COVERED"
    matrix.append(entry)

rep = {"generated": len(rows), "by_capital": {str(k): dict(v) for k, v in sorted(by_cap.items())},
       "by_check": {k: dict(v) for k, v in sorted(by_check.items())}, "violations": viol, "matrix": matrix,
       "verdicts": dict(collections.Counter(r["verdict"] for r in rows)),
       "paths_coverage": {p: sum(1 for r in rows if p in r["paths"]) for p in PATHS}}
json.dump(rep, open(RJ, "w"), indent=1)

L = [f"# Campagna — {len(rows)} scenari eseguiti", "", f"Verdetti: {rep['verdicts']}", "", "## Per capitale", "",
     "| capitale | totale | PASS | VIOLATION | BLOCKED |", "|---|---|---|---|---|"]
for k, v in sorted(by_cap.items()):
    L.append(f"| {k} | {v['total']} | {v.get('PASS', 0)} | {v.get('VIOLATION', 0)} | {v.get('BLOCKED', 0)} |")
L += ["", "## Per controllo", "", "| controllo | PASS | KNOWN | VIOLATION | BLOCKED |", "|---|---|---|---|---|"]
for k, v in sorted(by_check.items()):
    L.append(f"| {k} | {v.get('PASS', 0)} | {v.get('KNOWN', 0)} | {v.get('VIOLATION', 0)} | {v.get('BLOCKED', 0)} |")
L += ["", "## Copertura percorsi", ""] + [f"- {p}: {n}" for p, n in rep["paths_coverage"].items()]
L += ["", "## Matrice RSR x percorsi", "", "| RSR | parametro | esito campagna | influenza | rumore p95 | livelli |", "|---|---|---|---|---|---|"]
for m in matrix:
    L.append(f"| {m['id']} | {m['name']} | {m['campaign']} | {m.get('influence', '')} | {m.get('noise_floor_p95', '')} | {m.get('levels', m.get('reason', ''))} |")
L += ["", "## Violazioni / bloccati", ""] + [f"- {x['id']} {x['check']} {x['status']}: {x['detail'][:200]}" for x in viol[:200]]
open(RM, "w").write("\n".join(L) + "\n")
print(json.dumps({"verdicts": rep["verdicts"], "by_capital": rep["by_capital"]}))
