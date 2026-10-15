"""Crosswalk di evidenze A06 (RSR-01..41 x 10 assi) dal branch ccr-bcc81683-ud0hgc. Solo prove eseguite; nessun PASS
dedotto. Uso: build.py <repo> <code_sha> <out_prefix>"""
import json, re, sys
repo, sha, out = sys.argv[1:4]
AXES = ["GUI", "VALID", "SAVE", "LOAD", "RESTART", "APPLY", "PARITY", "RACE", "FAILSAFE", "AUDIT"]
rows = {}
for line in open(f"{repo}/docs/roserpina41_parameter_dossier.md"):
    m = re.match(r"\| (RSR-\d\d) \| ([^|]+)\| ([^|]+)\|.*\| ([^|]+)\| ([^|]+)\|\s*$", line)
    if m:
        rows[m.group(1)] = {"name": m.group(2).strip(), "key": m.group(3).strip().strip("`"),
                            "activation_test": m.group(4).strip(), "dossier_verdict": m.group(5).strip()}
src = open(f"{repo}/tests/roserpina41/test_rsr_targeted.py").read()
params = re.findall(r'\("(\w+)", ([^,]+), ([^)]+)\)', src.split("PARAMS = [")[1].split("]\n")[0])
persist = {k for k, _, _ in params}
numeric_default = {k for k, v, d in params if "." in v and d.strip() != "None"}
hard = set(re.findall(r'"(\w+)"', src.split("HARD_STOPS = [")[1].split("]")[0]))
T = "tests/roserpina41/test_rsr_targeted.py"
cells = []
for rid in sorted(rows):
    r = rows[rid]; k = r["key"].replace("roserpina.", "")
    absent = "BLOCKED" in r["dossier_verdict"] and ("assente" in r["dossier_verdict"].lower() or rid >= "RSR-38")
    def c(axis, status, evidence="", note=""):
        cells.append({"id": f"PF-RSR-{rid[4:]}-{axis}", "rsr": rid, "axis": axis, "key": r["key"], "status": status,
                      "code_sha": sha if status in ("PASS", "FAIL") else None, "evidence": evidence, "note": note})
    if rid >= "RSR-38":
        for a in AXES:
            c(a, "BLOCKED", note="parametro assente al HEAD: esiste solo nella PR #505 (protetta)")
        continue
    c("GUI", "NOT VERIFIED", note="nessuna prova GUI per-parametro in questo branch; vedi lotti GUI su checkpoint/gate-a-close02")
    c("VALID", "BLOCKED", f"{T}::test_rsr_unreadable_numeric_value_falls_back_to_code_default[{k}]" if k in numeric_default else "",
      "il loader torna al default di codice, non all'ultimo valore valido; range ammessi = decisioni owner Q1/Q2 aperte")
    if k in persist:
        c("SAVE", "PASS", f"{T}::test_rsr_persisted_and_reloaded_after_restart[{k}]", "SettingsService + SQLite reale su file")
        c("LOAD", "PASS" if k in numeric_default or k in hard else "NOT VERIFIED",
          f"{T}::test_rsr_persisted_and_reloaded_after_restart[{k}]" + (f"; ::test_rsr_unreadable_numeric_value_falls_back_to_code_default[{k}]" if k in numeric_default else "")
          + (f"; ::test_rsr_hard_stop_unreadable_is_never_silently_disabled[{k}]" if k in hard else ""),
          "ricarica + valore illeggibile" if (k in numeric_default or k in hard) else "solo ricarica; assente/corrotto non distinti da un test")
        c("RESTART", "NOT VERIFIED", f"{T}::test_rsr_persisted_and_reloaded_after_restart[{k}]",
          "nuove istanze sullo stesso DB nello STESSO processo: non e' la terminazione+nuovo processo richiesta dall'asse")
    else:
        for a in ("SAVE", "LOAD", "RESTART"):
            c(a, "MISSING", note="chiave non coperta dal test di persistenza")
    if rid in ("RSR-08", "RSR-09", "RSR-12"):
        cells[-1].update(status="PASS", code_sha=sha, evidence="tests/f026/test_f026_financial.py::" +
                         ("test_F032_01_expansion_after_net_profit_and_after_restart" if rid != "RSR-12" else "test_RSR12_01_recovery_tables_survive_crash_and_restart"),
                         note="processo figlio terminato (crash) e nuovo processo sullo stesso stato")
    v = r["dossier_verdict"]
    if v.startswith("PASS"):
        c("APPLY", "PASS", "scheda docs/roserpina41_parameter_dossier.md: " + r["activation_test"], "effetto attivato e osservato (scheda dei 41 parametri)" + ("; " + v if v != "PASS" else ""))
    elif v.startswith("NOT VERIFIED"):
        c("APPLY", "NOT VERIFIED", r["activation_test"], v)
    else:
        c("APPLY", "FAIL" if v.startswith("FAIL") else "BLOCKED", r["activation_test"], v)
    c("PARITY", "NOT VERIFIED", note="BACK/LAY, GUI/headless/MCP e SIM/LIVE non confrontati per-parametro; MCP non implementato")
    if rid == "RSR-12":
        c("RACE", "PASS", "tests/f026/test_f026_financial.py::test_RSR12_02_idle_recovery_tables_count_on_the_signal_path; tests/f029/",
          "piu' segnali/tavoli: il limite non si perde con tavoli inattivi (MD10 ucciso)")
    else:
        c("RACE", "NOT VERIFIED", note="modifica concorrente della configurazione non provata")
    if k in hard:
        c("FAILSAFE", "PASS", f"{T}::test_rsr_hard_stop_unreadable_is_never_silently_disabled[{k}]", "valore corrotto non disattiva lo stop")
    elif rid == "RSR-12":
        c("FAILSAFE", "PASS", "tests/f026/test_f032_rsr12_units.py::test_rsr12_unreadable_state_is_fail_closed", "stato illeggibile => capacita' esaurita")
    else:
        c("FAILSAFE", "NOT VERIFIED")
    c("AUDIT", "NOT VERIFIED", note="ricostruzione valore/decisione/correlation_id non provata per-parametro")
from collections import Counter
cnt = Counter(x["status"] for x in cells)
doc = {"schema": "pf-a06-crosswalk/1", "code_sha": sha, "axes_source": "#461 comment 6085510337",
       "scope": "contributo di evidenze ad A06 (#506); NON e' il verdetto A06 ne' Gate A", "total": len(cells),
       "counts": dict(cnt), "cells": cells}
json.dump(doc, open(out + ".json", "w"), indent=1, ensure_ascii=False)
with open(out + ".md", "w") as fh:
    fh.write(f"# A06 — crosswalk di evidenze RSR-01..41 x 10 assi @ {sha[:12]}\n\nContributo, non verdetto A06/Gate A. Assi: #461 c.6085510337. "
             f"Totale {len(cells)}: " + ", ".join(f"{k} {v}" for k, v in sorted(cnt.items())) + "\n\n| RSR | " + " | ".join(AXES) + " |\n|---|" + "---|" * 10 + "\n")
    for rid in sorted(rows):
        st = {x["axis"]: x["status"] for x in cells if x["rsr"] == rid}
        fh.write(f"| {rid} | " + " | ".join(st[a] for a in AXES) + " |\n")
print(len(rows), len(cells), dict(cnt))
