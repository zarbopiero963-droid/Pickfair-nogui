"""Lettura complementare dei VIOLATION C01/C01b/R2 dell'oracle base sulla campagna con Microstake nativo.

NON modifica oracle.py (oracle indipendente, invariato). Per ogni gruppo (mercato, selezione, lato) con piu' righe
nel registro ordini dell'exchange SIM (dedotte per bet_id attraverso le fasi) verifica l'impronta del Microstake:
ESATTAMENTE una riga viva (non CANCELLED o con matched > 0), tutte le altre CANCELLED con matched 0 (stub),
stesso customer_ref (stessa saga), somma del matched == size della riga viva (nessuna esposizione doppia).
Qualunque gruppo che non rispetta TUTTE le condizioni e' un duplicato reale.  argv: OUTDIR
"""
import collections, glob, json, sys

OUT = sys.argv[1]
v = json.load(open(OUT + "/verdicts.json"))
real, footprint, groups = [], 0, 0
for r in v:
    flagged = {k for k, c in r["checks"].items() if c["status"] == "VIOLATION" and k in ("C01_no_duplicate_order", "C01b_unique_customer_ref", "R2_no_replay_after_restart")}
    if not flagged:
        continue
    rows = {}
    for f in sorted(glob.glob(f"{OUT}/{r['id']}/out*.json")):
        for b in json.load(open(f))["broker_orders"]:
            rows[b["bet_id"]] = b                                   # stato finale per bet_id (fasi successive sovrascrivono)
    byk = collections.defaultdict(list)
    for b in rows.values():
        byk[(b["market_id"], b["selection_id"], b["side"])].append(b)
    for k, g in byk.items():
        if len(g) < 2:
            continue
        groups += 1
        live = [b for b in g if b["status"] != "CANCELLED" or b["matched_size"] > 0]
        stubs = [b for b in g if b["status"] == "CANCELLED" and b["matched_size"] == 0]
        ok = (len(live) == 1 and len(stubs) == len(g) - 1 and len({b["customer_ref"] for b in g}) == 1
              and abs(sum(b["matched_size"] for b in g) - live[0]["matched_size"]) < 1e-9
              and live[0]["matched_size"] <= live[0]["size"] + 1e-9)
        if ok:
            footprint += 1
        else:
            real.append({"id": r["id"], "key": list(k), "rows": [(b["status"], b["size"], b["matched_size"], b["price"], b["customer_ref"]) for b in g]})
res = {"scenari_flaggati": sum(1 for r in v if any(c["status"] == "VIOLATION" for c in r["checks"].values())),
       "gruppi_multiriga": groups, "impronta_microstake": footprint, "duplicati_reali": len(real), "dettaglio_reali": real}
json.dump(res, open(OUT + "/footprint_check.json", "w"), indent=1)
print(json.dumps({k: v for k, v in res.items() if k != "dettaglio_reali"}), "\n", json.dumps(real[:5], indent=1))
