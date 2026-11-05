"""Riesegue la campagna G7 dal registro `tests/g7/g7_scenarios.json` e scrive il file di evidenza (JSON).

Ogni esito cita: SHA dell'albero (`git rev-parse HEAD`), albero pulito o no (`git status --porcelain`), ID scenario,
test, seed, requisiti, esito pytest (PASS / FAIL / XFAIL / NOT_TESTED se il test non esiste sull'albero) e durata.
Lo strumento NON decide se l'esito e' certificativo: lo e' solo su uno SHA pubblicato (head di PR o main), lo stabilisce
chi verifica (regola 3 di G7). Nessuna rete, nessun ordine: solo pytest sui test SIM del registro.

Uso: python3 tools/sim_report/run_g7.py --out art/g7/<sha>.json [--only G7-S12 ...]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
REGISTRY = os.path.join(ROOT, "tests", "g7", "g7_scenarios.json")


def _git(*args: str) -> str:
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True).stdout.strip()


def load_registry(path: str = REGISTRY) -> Dict[str, Any]:
    with open(path, encoding="utf-8") as fh:
        reg = json.load(fh)
    ids = [s["id"] for s in reg["scenarios"]]
    if len(ids) != len(set(ids)):
        raise ValueError("ID di scenario duplicati nel registro G7")
    return reg


def classify(returncode: int, stdout: str) -> str:
    """Esito di una esecuzione pytest di un solo scenario."""
    last = stdout.strip().splitlines()[-1] if stdout.strip() else ""
    if "no tests ran" in last or "not found" in stdout or "ERROR: file or directory not found" in stdout:
        return "NOT_TESTED"
    if returncode == 0 and " skipped" in last and " passed" not in last and "xfailed" not in last:
        return "NOT_TESTED"
    if returncode == 0 and "xfailed" in last and " passed" not in last:
        return "XFAIL"
    if returncode == 0:
        return "PASS"
    return "FAIL"


#: Gravita' degli esiti: un requisito prende lo stato peggiore dei suoi scenari ed e' PASS solo se TUTTI sono PASS.
#: XFAIL (difetto noto), UNSUPPORTED e NOT_TESTED non contribuiscono mai a un PASS.
SEVERITY = ("FAIL", "XFAIL", "UNSUPPORTED", "NOT_TESTED", "PASS")


def requirement_status(results: List[Dict[str, Any]]) -> Dict[str, str]:
    by_req: Dict[str, List[str]] = {}
    for r in results:
        for req in r["requirements"]:
            by_req.setdefault(req, []).append(r["outcome"])
    return {req: min(outcomes, key=SEVERITY.index) for req, outcomes in sorted(by_req.items())}


def evidence_mode(tree_sha: str, tree_clean: bool, published_sha: Optional[str]) -> str:
    """``development`` salvo albero pulito con SHA uguale a quello pubblicato dichiarato: allora
    ``certification_candidate``. La certificazione la decide comunque il verificatore (solo SHA pubblicati)."""
    if published_sha and tree_clean and tree_sha and tree_sha == published_sha:
        return "certification_candidate"
    return "development"


def run_scenario(sc: Dict[str, Any]) -> Dict[str, Any]:
    row = {k: sc[k] for k in ("id", "test", "requirements", "seeds", "requires")}
    if sc.get("unsupported"):
        row.update(outcome="UNSUPPORTED", detail=str(sc["unsupported"]), duration_s=0.0)
        return row
    path = sc["test"].split("::", 1)[0]
    if not os.path.exists(os.path.join(ROOT, path)):
        row.update(outcome="NOT_TESTED", detail="test assente su questo albero", duration_s=0.0)
        return row
    started = time.monotonic()
    proc = subprocess.run([sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-rs", sc["test"]],
                          cwd=ROOT, capture_output=True, text=True)
    row.update(outcome=classify(proc.returncode, proc.stdout), duration_s=round(time.monotonic() - started, 2),
               detail=(proc.stdout.strip().splitlines() or [""])[-1][:300])
    return row


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Riesegue la campagna G7 e scrive l'evidenza")
    ap.add_argument("--out", required=True)
    ap.add_argument("--only", nargs="*", default=None)
    ap.add_argument("--published-sha", default=None,
                    help="SHA pubblicato (head di PR o main) che l'albero deve essere per un candidato certificativo")
    args = ap.parse_args(argv)
    reg = load_registry()
    scenarios = [s for s in reg["scenarios"] if not args.only or s["id"] in args.only]
    tree_sha, tree_clean = _git("rev-parse", "HEAD"), _git("status", "--porcelain") == ""
    results = [run_scenario(s) for s in scenarios]
    evidence = {"schema": "pf-g7-evidence/2", "tree_sha": tree_sha, "tree_clean": tree_clean,
                "published_sha": args.published_sha,
                "registry_sha256": hashlib.sha256(open(REGISTRY, "rb").read()).hexdigest(),
                "mode": evidence_mode(tree_sha, tree_clean, args.published_sha),
                "python": sys.version.split()[0],
                "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "certification": "DECISA DAL VERIFICATORE: mai su mode=development",
                "requirements": requirement_status(results),
                "results": results}
    os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(evidence, fh, indent=2, ensure_ascii=False)
    bad = [r for r in evidence["results"] if r["outcome"] == "FAIL"]
    print(json.dumps({r["id"]: r["outcome"] for r in evidence["results"]}))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
