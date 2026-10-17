"""P8 / PF-MASTER-CERTIFICATION-351 §6 — collaudo multi-tavolo crash/recovery sul codice REALE (HeadlessApp, SIM).

Usa il child di produzione tests/f026/child_f026.py (stessa app, stesso DB SQLite reale, broker SIM reale).
Oracle definito PRIMA dell'esecuzione (vedi EXPECT). Prova NON certificante: SIM/OFFLINE, nessun exchange reale.

argv: TREE N OUTDIR
"""
import hashlib, json, os, subprocess, sys, tempfile, time

TREE, N, OUT = sys.argv[1], int(sys.argv[2]), sys.argv[3]
PY = os.environ.get("PF_PY", sys.executable)
CHILD = os.path.join(TREE, "tests", "f026", "child_f026.py")
os.makedirs(OUT, exist_ok=True)
SETTINGS = {"roserpina.lockdown_drawdown_pct": 60.0, "roserpina.table_count": N,
            "roserpina.max_total_exposure_pct": 100.0, "roserpina.max_event_exposure_pct": 100.0,
            "roserpina.max_single_bet_pct": 25.0}
MARKETS = [f"1.9{i:02d}" for i in range(1, N + 1)]


def run(steps, home, tag, flags=None):
    out = os.path.join(OUT, f"{tag}.json")
    env = dict(os.environ)
    if os.environ.get("PF_NETGUARD"):
        env["PYTHONPATH"] = os.environ["PF_NETGUARD"]
    p = subprocess.run([PY, CHILD, TREE, out, json.dumps({"steps": steps, "settings": SETTINGS, **(flags or {})}), home],
                       env=env, capture_output=True, text=True, timeout=600)
    open(os.path.join(OUT, f"{tag}.stderr.txt"), "w").write(p.stderr[-20000:])
    r = json.load(open(out)); r["_rc"] = p.returncode
    return r


# ---------------------------------------------------------------- oracle (scritto prima dell'esecuzione)
# tavolo i: lato BACK se i dispari, LAY se pari; modo: i%4==1 normale (matched), ==2 partial, ==3 nomatch (unmatched),
# ==0 lost (risposta persa dopo l'invio => AMBIGUOUS, riserva trattenuta). Cashout sul tavolo 1 prima del crash.
def expected(n):
    exp = {}
    for i, m in enumerate(MARKETS, 1):
        side = "BACK" if i % 2 else "LAY"
        mode = ["lost", "normal", "partial", "nomatch"][i % 4]
        exp[m] = {"side": side, "mode": mode,
                  # dopo il crash e il riavvio: una sola riga ordine per mercato (mai duplicati),
                  # tavolo ricostruito se l'ordine e' vivo (matched/partial/unmatched), AMBIGUOUS mai FAILED,
                  "max_order_rows": 1,
                  "rebuilt_table": mode in ("normal", "partial", "nomatch"),
                  "ambiguous_allowed": mode == "lost"}
    return exp


EXPECT = expected(N)
json.dump({"n": N, "settings": SETTINGS, "expect": EXPECT}, open(os.path.join(OUT, "oracle.json"), "w"), indent=1)

steps1 = []
for i, m in enumerate(MARKETS, 1):
    mode = EXPECT[m]["mode"]
    if mode != "normal":
        steps1.append({"op": "mode", "m": m, "mode": mode})
    steps1.append({"op": "signal", "m": m, "price": 2.0, "side": EXPECT[m]["side"]})
steps1.append({"op": "wait", "s": 2})
steps1.append({"op": "cashout", "m": MARKETS[0]})
steps1.append({"op": "crash_now"})

home = tempfile.mkdtemp(prefix=f"p8_{N}_")
t0 = time.time()
first = run(steps1, home, "run1_crash")
second = run([{"op": "wait", "s": 3}], home, "run2_restart")
third = run([{"op": "wait", "s": 2}, {"op": "crash_now"}], home, "run3_crash_again")       # crash multiplo
fourth = run([{"op": "wait", "s": 3}], home, "run4_restart2")

# ---------------------------------------------------------------- verifiche
def last(r):
    return r["snaps"][-1]


findings = []
s_crash, s_rest, s_rest2 = last(first), last(second), last(fourth)
for m, e in EXPECT.items():
    rows = [o for o in s_rest["orders"] if o["market_id"] == m and o["status"] != "DUPLICATE_BLOCKED"]
    if len(rows) > e["max_order_rows"]:
        findings.append(f"{m}: {len(rows)} righe ordine dopo il riavvio (duplicato?) {rows}")
    statuses = {o["status"] for o in rows}
    if e["ambiguous_allowed"] and statuses & {"FAILED"} and not statuses & {"AMBIGUOUS", "INFLIGHT", "SUBMITTED"}:
        findings.append(f"{m}: risposta persa classificata FAILED (deve restare incerta) {rows}")
    tables = [t for t in s_rest["tables"] if t.get("market_id") == m]
    if e["rebuilt_table"] and not tables and rows and not statuses & {"FAILED", "REJECTED"}:
        findings.append(f"{m}: tavolo non ricostruito al riavvio per ordine vivo {rows}")
# esposizione: somma tavoli vs esposizione exchange (SIM) dopo il riavvio
tab_exp = sum(float(t.get("exposure") or 0) for t in s_rest["tables"])
ex_exp = float((s_rest["funds"] or {}).get("exposure") or 0)
# modalita' dopo riavvio: con AMBIGUOUS pendenti deve essere PAUSED (riconciliazione), altrimenti ACTIVE
amb = any(o["status"] == "AMBIGUOUS" for o in s_rest["orders"])
mode_ok = (s_rest["mode"] == "PAUSED") if amb else (s_rest["mode"] in ("ACTIVE", "PAUSED"))
if not mode_ok:
    findings.append(f"modo dopo riavvio {s_rest['mode']} con AMBIGUOUS={amb}")
# crash multiplo: stesso numero di righe ordine non-duplicate dopo il secondo riavvio
n1 = len([o for o in s_rest["orders"] if o["status"] != "DUPLICATE_BLOCKED"])
n2 = len([o for o in s_rest2["orders"] if o["status"] != "DUPLICATE_BLOCKED"])
if n2 != n1:
    findings.append(f"righe ordine cambiate fra i riavvii: {n1} -> {n2}")

report = {"n": N, "tree": TREE, "sha": subprocess.run(["git", "-C", TREE, "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip(),
          "elapsed_s": round(time.time() - t0, 1), "home": home,
          "run1": {"mode": s_crash["mode"], "orders": s_crash["orders"], "tables": s_crash["tables"], "funds": s_crash["funds"],
                   "events": [e for e in first["events"] if e["e"].startswith(("QUICK_BET", "SIGNAL_REJ", "CASHOUT", "RISK"))]},
          "run2_restart": {"mode": s_rest["mode"], "start": second.get("start"), "rebuild": second.get("rebuild"),
                           "orders": s_rest["orders"], "tables": s_rest["tables"], "funds": s_rest["funds"], "bankroll": s_rest["bankroll"]},
          "run4_restart2": {"mode": s_rest2["mode"], "rebuild": fourth.get("rebuild"), "orders": s_rest2["orders"], "tables": s_rest2["tables"]},
          "exposure_tables_vs_exchange": {"tables": tab_exp, "exchange": ex_exp},
          "findings": findings, "verdict": "PASS" if not findings else "FAIL"}
json.dump(report, open(os.path.join(OUT, "report.json"), "w"), indent=1, default=str)
h = {f: hashlib.sha256(open(os.path.join(OUT, f), "rb").read()).hexdigest() for f in sorted(os.listdir(OUT)) if f != "MANIFEST.sha256"}
open(os.path.join(OUT, "MANIFEST.sha256"), "w").write("".join(f"{v}  {k}\n" for k, v in h.items()))
print(json.dumps({"n": N, "verdict": report["verdict"], "findings": findings, "mode_after_restart": s_rest["mode"],
                  "orders_after_restart": [(o["market_id"], o["side"], o["status"], o["matched"]) for o in s_rest["orders"]],
                  "tables_after_restart": [(t["table_id"], t["market_id"], t["status"], t["exposure"]) for t in s_rest["tables"]],
                  "exposure": report["exposure_tables_vs_exchange"], "elapsed_s": report["elapsed_s"]}, default=str, indent=1))
