"""Oracolo indipendente dello scenario critico multi-tavolo (MISTA + incerti + crash + riavvio, SQLite reale).
Uso: python3 critical_oracle.py <out_dir> <tag_albero>
"""
import json
import sys
from decimal import Decimal as D, ROUND_HALF_UP

OUT, TREE = sys.argv[1], sys.argv[2]
COMM = D("0.045")


def q(x):
    return D(str(x)).quantize(D("0.0001"), rounding=ROUND_HALF_UP)


pre = json.load(open(f"{OUT}/critical_pre_{TREE}.json"))
post = json.load(open(f"{OUT}/critical_post_{TREE}.json"))
modes = pre["sim_declared"]["transport_modes"]
res = {"tree": TREE, "errors": [(pre.get("error") or "")[-300:], (post.get("error") or "")[-300:]],
       "crashed": any(x["kind"] == "CRASH_21_00" for x in pre["timeline"])}
pre_back = [c for c in pre.get("broker_calls", []) if ":" not in c]
post_back = [c for c in post.get("broker_calls", []) if ":" not in c]
res["broker_calls_pre"] = pre_back
res["broker_calls_post"] = post_back
res["no_duplicate_send_after_restart"] = not (set(pre_back) & set(post_back))
last = post["snapshots"][-1]
rows = {}
for o in last["orders"]:
    rows.setdefault(o["market_id"], []).append(o["status"])
res["db_orders_final"] = rows
expect = {"lost": {"AMBIGUOUS"}, "unknown": {"AMBIGUOUS"}, "slow_crash": {"INFLIGHT", "AMBIGUOUS"},
          "partial": {"SUBMITTED"}, "ok": {"SUBMITTED"}}
res["status_ok"] = {m: (set(s) - {"DUPLICATE_BLOCKED"}) <= expect[modes.get(m, "ok")] and len([x for x in s if x != "DUPLICATE_BLOCKED"]) == 1
                    for m, s in rows.items()}
# exchange: ordini BACK abbinati (verita' del broker, sopravvissuta al crash)
fill = {}
for r_ in (pre, post):
    for c in r_["timeline"]:
        if c["kind"] == "broker_call" and (c.get("side") or "BACK") == "BACK":
            bk = [b for b in r_.get("book_log", []) if b["market_id"] == c["market_id"] and b["status"] == "OPEN" and b["t"] <= c["t"]]
            if bk:
                fill.setdefault(c["market_id"], D(str(bk[-1]["back"])))
back = {}
for snap in pre["snapshots"] + post["snapshots"]:
    for bo in snap.get("broker_orders", []):
        if str(bo.get("side")).upper() == "BACK" and float(bo.get("matched") or 0) > 0:
            back.setdefault(bo["market_id"], (D(str(bo["matched"])), fill.get(bo["market_id"], D(str(bo["price"])))))
outcome = {}
for r in (pre, post):
    for x in r["timeline"]:
        if x["kind"] == "settle":
            outcome[x["market_id"]] = x["sim_outcome"] == "WIN"
tot, detail = D(0), []
for m, (mb, pb) in sorted(back.items()):
    if m in outcome:
        v = mb * (pb - 1) * (1 - COMM) if outcome[m] else -mb
        tot += v
        detail.append({"market_id": m, "win": outcome[m], "matched": str(q(mb)), "avg_price": str(pb), "pnl": str(q(v))})
total = D(str(last["broker_funds"].get("total", 100)))
res["pnl"] = {"oracle": str(q(tot)), "broker_delta": str(q(total - D(100))), "detail": detail,
              "runtime_realized_pre": str(q(pre["snapshots"][-1].get("realized_pnl") or 0)),
              "runtime_realized_post": str(q(last.get("realized_pnl") or 0))}
res["broker_matches_oracle"] = q(tot) == q(total - D(100))
res["bankroll_vs_equity"] = [(s["tag"], str(q(s["bankroll_current"])), str(q(s["broker_funds"].get("total", 0))))
                             for s in pre["snapshots"] + post["snapshots"]]
res["bankroll_equals_equity_after_restart"] = q(post["snapshots"][0]["bankroll_current"]) == q(post["snapshots"][0]["broker_funds"].get("total", 0))
res["auto_resets"] = [pre["snapshots"][-1].get("auto_resets"), last.get("auto_resets")]
res["tables_after_restart"] = [(t["table_id"], t["market_id"]) for t in post["snapshots"][0]["tables"]]
res["open_positions_after_restart"] = sorted(m for m in back if m not in {x["market_id"] for x in pre["timeline"] if x["kind"] == "settle"})
res["F018_tables_rebuilt"] = set(res["open_positions_after_restart"]) <= {t[1] for t in res["tables_after_restart"]}
print(json.dumps(res, indent=1, default=str))
