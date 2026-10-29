"""F-33 — tracciabilita' dei requisiti UNVERIFIED (#586 PICKFAIR_UNVERIFIED_CLOSURE_TRACKER).

Raccoglie i marker ``@pytest.mark.pf_requirement("ID", ...)``, registra l'esito reale di ogni test e scrive
``requirements_report.json``: per requisito i test eseguiti e i loro esiti, se e' COPERTO (almeno un test
``passed`` sullo SHA del report), i requisiti del catalogo senza alcun test (``missing``), SHA, seed, data.

Regole: xfail, skip, failed, error NON coprono. Un ID non presente nel catalogo e' segnalato (``unknown_ids``), non
scartato in silenzio. Il report e' dato, non verdetto: la certificazione la emette Claude sulla base di questo e
delle altre evidenze (politica #586 c.6504736869).
"""
from __future__ import annotations

import datetime as _dt
import json
import os
import subprocess
from typing import Any, Dict, List, Optional

HERE = os.path.dirname(os.path.abspath(__file__))
CATALOG_PATH = os.path.join(HERE, "catalog.json")
REPORT_ENV = "PF_FULLSCOPE_ART"           # directory degli artifact (default: art/fullscope/<sha>/ sotto la radice)
SEED_ENV = "PF_SEED"                      # seed riproducibile degli scenari/generatore (default 0)

COVERING_OUTCOMES = {"passed"}


def load_catalog(path: str = CATALOG_PATH) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def repo_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


def head_sha(root: Optional[str] = None) -> str:
    try:
        out = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root or repo_root(), capture_output=True,
                             text=True, timeout=10, check=False)
        return out.stdout.strip() if out.returncode == 0 else ""
    except Exception:  # noqa: BLE001
        return ""


def seed() -> int:
    try:
        return int(os.environ.get(SEED_ENV, "0"))
    except ValueError:
        return 0


def artifact_dir(sha: str) -> str:
    base = os.environ.get(REPORT_ENV) or os.path.join(repo_root(), "art", "fullscope", sha or "nosha")
    os.makedirs(base, exist_ok=True)
    return base


class RequirementTracker:
    """Stato di sessione: nodeid -> (ids, esito finale)."""

    def __init__(self, catalog: Optional[Dict[str, Any]] = None) -> None:
        self.catalog = catalog or load_catalog()
        self.known = {r["id"]: r for r in self.catalog["requirements"]}
        self.records: Dict[str, Dict[str, Any]] = {}

    def register(self, nodeid: str, ids: List[str]) -> None:
        rec = self.records.setdefault(nodeid, {"ids": [], "outcome": None, "phases": {}})
        for i in ids:
            if i not in rec["ids"]:
                rec["ids"].append(i)

    def record(self, nodeid: str, phase: str, outcome: str, xfail: bool = False) -> None:
        rec = self.records.get(nodeid)
        if rec is None:
            return
        rec["phases"][phase] = outcome
        # esito finale: un setup/teardown fallito o un xfail non vale "passed"
        if phase == "call":
            rec["outcome"] = "xfail" if xfail else outcome
        elif outcome in {"failed", "error"} and rec["outcome"] in (None, "passed"):
            rec["outcome"] = "error"
        elif phase == "setup" and outcome == "skipped":
            rec["outcome"] = "skipped"

    def report(self, sha: str, seed_value: int) -> Dict[str, Any]:
        per_req: Dict[str, Dict[str, Any]] = {}
        unknown: Dict[str, List[str]] = {}
        for nodeid, rec in self.records.items():
            for i in rec["ids"]:
                target = per_req if i in self.known else unknown.setdefault(i, [])
                if i in self.known:
                    entry = target.setdefault(i, {"tests": [], "covered": False})
                    entry["tests"].append({"nodeid": nodeid, "outcome": rec["outcome"]})
                    if rec["outcome"] in COVERING_OUTCOMES:
                        entry["covered"] = True
                else:
                    target.append(nodeid)
        covered = sorted(i for i, e in per_req.items() if e["covered"])
        attempted_not_covered = sorted(i for i, e in per_req.items() if not e["covered"])
        missing = sorted(i for i in self.known if i not in per_req)
        return {
            "schema": "pf-requirements-report/1",
            "generated_at": _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds"),
            "repo_sha": sha,
            "seed": seed_value,
            "catalog": self.catalog.get("source", ""),
            "counts": {"catalog": len(self.known), "covered": len(covered),
                       "attempted_not_covered": len(attempted_not_covered), "missing": len(missing),
                       "unknown_ids": len(unknown)},
            "covered": covered,
            "attempted_not_covered": attempted_not_covered,
            "missing": [{"id": i, "primary_pr": self.known[i]["primary_pr"]} for i in missing],
            "unknown_ids": unknown,
            "requirements": {i: {**per_req.get(i, {"tests": [], "covered": False}),
                                 "primary_pr": self.known[i]["primary_pr"],
                                 "status_initial": self.known[i]["status_initial"]} for i in sorted(self.known)},
            "rule": "covered = almeno un test pf_requirement(id) con esito 'passed' su repo_sha; xfail/skip/fail non coprono",
        }


def tracker_row(report: Dict[str, Any], req_id: str) -> str:
    """Riga pronta per PICKFAIR_UNVERIFIED_CLOSURE_TRACKER (markdown)."""
    e = report["requirements"][req_id]
    tests = ", ".join(t["nodeid"].split("::")[-1] for t in e["tests"]) or "nessuno"
    verdict = "COVERED (test passed)" if e["covered"] else ("ATTEMPTED (non passed)" if e["tests"] else "MISSING")
    return f"| {req_id} | {e['status_initial']} | {e['primary_pr']} | {tests} | — | requirements_report.json | {report['repo_sha'][:7]} | {verdict} | aperto |"
