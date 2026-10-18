#!/usr/bin/env python3
"""pf_trace — registro atomico di tracciabilità e validatore (PF-TRACEABILITY-ALL-01, #461 c.6225689722).

Registro: `docs/traceability/requirements.json` (lista di record). Ogni record collega decisione owner → requisito →
issue → codice → test → evidenza → finding → gate. Stati: PLANNED, IMPLEMENTED, TESTED, VERIFIED, CERTIFIED, BLOCKED,
NOT VERIFIED. Le evidenze sono append-only: il validatore confronta con la versione committata (`git show HEAD:…`) e
rifiuta ogni rimozione o riscrittura di un'evidenza precedente. Un test dichiarato in un record TESTED/VERIFIED/CERTIFIED
e' coperto solo da un'evidenza che lo nomina (campo `tests` esplicito, oppure node-id, file o directory nel `command`):
mai per implicito (PR #512 ciclo 9); test non coperto => warning, FAIL con `--strict`.

Comandi: `validate [--registry PATH] [--repo PATH] [--strict]`, `report` (markdown). Solo stdlib.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from collections import Counter

DEFAULT_REGISTRY = "docs/traceability/requirements.json"
STATES = {"PLANNED", "IMPLEMENTED", "TESTED", "VERIFIED", "CERTIFIED", "BLOCKED", "NOT VERIFIED"}
ACTIVITY = {"ACTIVE", "SUSPENDED", "DEFERRED"}
RESULTS = {"PASS", "FAIL", "BLOCKED", "NOT RUN", "NOT VERIFIED"}
REQUIRED = ("id", "text", "source", "owner_decision", "activity", "status", "issue", "component", "code", "oracle",
            "tests", "evidence", "findings", "reviewer", "gate", "limitations")
SHA_RE = re.compile(r"^[0-9a-f]{7,40}$")
TEST_RE = re.compile(r"^(?P<file>[\w./-]+\.py)(::(?P<name>[\w\[\]\-., =]+))?$")


def evidence_covers(evidence, test_ref):
    """True se l'evidenza e' legata in modo verificabile al test dichiarato (rilievo Codex #512 ciclo 9).

    Associazione esplicita (`tests` non vuoto) oppure derivata dal `command` dell'evidenza: il comando
    nomina il node-id esatto, il file del test o una directory che lo contiene. Mai implicita: un'evidenza
    senza `tests` e con un comando che non nomina il test NON lo copre (prima il fallback `[t]` faceva
    coprire ogni test dichiarato a qualunque PASS)."""
    ref = str(test_ref or "").strip()
    if not ref:
        return False
    test_file = ref.split("::", 1)[0]
    explicit = evidence.get("tests")
    if isinstance(explicit, list) and explicit:
        return any(str(x).strip() in (ref, test_file) for x in explicit)
    for token in re.split(r"[\s;,()]+", str(evidence.get("command") or "")):
        token = token.strip().strip("'\"")
        if not token or token.startswith("-"):
            continue
        if token == ref or token == test_file:
            return True
        if "::" in token or token.endswith(".py"):
            continue                       # node-id o file di un ALTRO test
        prefix = token.rstrip("/") + "/"
        if prefix.startswith("tests/") and test_file.startswith(prefix):
            return True                    # directory che contiene il test
    return False


def load(path):
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict) or not isinstance(data.get("requirements"), list):
        raise ValueError("registro: atteso {'schema':..., 'requirements': [...]}")
    return data


def committed(path, repo):
    p = subprocess.run(["git", "-C", repo, "show", f"HEAD:{path}"], capture_output=True)
    if p.returncode != 0:
        return None
    try:
        return json.loads(p.stdout.decode())
    except ValueError:
        return None


def commit_status(repo, sha):
    """OK se `sha` e' un commit del repository, MISSING se non lo e', NO_REPO se `repo` non e' un repository git,
    SHALLOW_MISSING se manca ma il clone e' shallow (la storia non e' tutta disponibile)."""
    if subprocess.run(["git", "-C", repo, "rev-parse", "--git-dir"], capture_output=True).returncode != 0:
        return "NO_REPO"
    if subprocess.run(["git", "-C", repo, "cat-file", "-e", f"{sha}^{{commit}}"], capture_output=True).returncode == 0:
        return "OK"
    shallow = subprocess.run(["git", "-C", repo, "rev-parse", "--is-shallow-repository"], capture_output=True, text=True)
    return "SHALLOW_MISSING" if shallow.stdout.strip() == "true" else "MISSING"


def test_exists(repo, ref):
    m = TEST_RE.match(ref)
    if not m:
        return False
    path = os.path.join(repo, m.group("file"))
    if not os.path.isfile(path):
        return False
    name = m.group("name")
    if not name:
        return True
    func = name.split("[", 1)[0].split("::")[-1]
    with open(path, encoding="utf-8", errors="ignore") as fh:
        text = fh.read()
    return re.search(rf"^\s*(async\s+)?def\s+{re.escape(func)}\s*\(", text, re.M) is not None


def validate(data, repo=".", previous=None):
    errors, warnings = [], []
    reqs = data["requirements"]
    ids = Counter(r.get("id") for r in reqs)
    for rid, n in ids.items():
        if n > 1:
            errors.append(f"{rid}: id duplicato ({n})")
    sha_cache = {}
    prev_ev = {}
    if previous:
        for r in previous.get("requirements", []):
            prev_ev[r.get("id")] = r.get("evidence") or []
    for r in reqs:
        rid = r.get("id", "?")
        for k in REQUIRED:
            if k not in r:
                errors.append(f"{rid}: campo mancante {k}")
        status, activity = r.get("status"), r.get("activity")
        if status not in STATES:
            errors.append(f"{rid}: stato non ammesso {status!r}")
        if activity not in ACTIVITY:
            errors.append(f"{rid}: activity non ammessa {activity!r}")
        tests, evidence = r.get("tests") or [], r.get("evidence") or []
        for t in tests:
            if not test_exists(repo, t):
                errors.append(f"{rid}: test inesistente {t}")
        for e in evidence:
            if not isinstance(e, dict):
                errors.append(f"{rid}: evidenza malformata"); continue
            if not SHA_RE.match(str(e.get("sha", ""))):
                errors.append(f"{rid}: evidenza senza SHA valido ({e.get('command', '')[:40]})")
            else:
                # rilievo Codex #512: uno SHA sintatticamente valido deve risolversi a un commit reale
                st = sha_cache.get(e["sha"])
                if st is None:
                    st = sha_cache[e["sha"]] = commit_status(repo, e["sha"])
                if st == "MISSING":
                    errors.append(f"{rid}: evidenza con SHA {e['sha']} non risolto a un commit del repository")
                elif st == "NO_REPO":
                    errors.append(f"{rid}: evidenza con SHA {e['sha']} non verificabile (repo non git)")
                elif st == "SHALLOW_MISSING":
                    warnings.append(f"{rid}: evidenza con SHA {e['sha']} non verificabile in un clone shallow")
            if e.get("result") not in RESULTS:
                errors.append(f"{rid}: evidenza con result non ammesso {e.get('result')!r}")
            for k in ("command", "timestamp", "environment"):
                if not e.get(k):
                    errors.append(f"{rid}: evidenza senza {k}")
        passes = [e for e in evidence if isinstance(e, dict) and e.get("result") == "PASS"]
        fails = [e for e in evidence if isinstance(e, dict) and e.get("result") == "FAIL"]
        latest = evidence[-1] if evidence and isinstance(evidence[-1], dict) else None
        if status in {"TESTED", "VERIFIED", "CERTIFIED"}:
            if not tests:
                errors.append(f"{rid}: {status} senza test")
            if not passes:
                errors.append(f"{rid}: {status} senza evidenza PASS")
            if latest and latest.get("result") != "PASS":
                # rilievo Codex #512 su 034af59: conta l'ULTIMA evidenza applicabile, che deve essere PASS;
                # un BLOCKED / NOT RUN / NOT VERIFIED successivo a un PASS invalida lo stato come un FAIL
                errors.append(f"{rid}: {status} ma l'ultima evidenza e' {latest.get('result')} "
                              "(contraddizione: l'ultima evidenza deve essere PASS)")
        if status in {"VERIFIED", "CERTIFIED"} and not r.get("reviewer"):
            errors.append(f"{rid}: {status} senza revisore")
        if status == "CERTIFIED" and (not r.get("gate") or not passes):
            errors.append(f"{rid}: CERTIFIED senza gate o senza prove")
        if status == "IMPLEMENTED" and not (r.get("code") or []):
            errors.append(f"{rid}: IMPLEMENTED senza riferimenti al codice")
        if activity != "ACTIVE" and status in {"TESTED", "VERIFIED", "CERTIFIED"}:
            errors.append(f"{rid}: requisito {activity} contato come {status}")
        if status == "PLANNED" and passes:
            warnings.append(f"{rid}: PLANNED ma esistono evidenze PASS: aggiornare lo stato")
        open_findings = [f for f in (r.get("findings") or []) if isinstance(f, dict) and f.get("state") not in {"FIXED", "ACCEPTED", "CLOSED"}]
        if open_findings and status in {"VERIFIED", "CERTIFIED"}:
            errors.append(f"{rid}: {status} con finding aperti {[f.get('id') for f in open_findings]}")
        if fails and not open_findings and status not in {"BLOCKED", "NOT VERIFIED", "PLANNED", "IMPLEMENTED", "TESTED"}:
            errors.append(f"{rid}: evidenza FAIL senza finding tracciato")
        if rid in prev_ev:
            old = prev_ev[rid]
            if evidence[:len(old)] != old:
                errors.append(f"{rid}: evidenze precedenti rimosse o riscritte (append-only violato)")
        for t in tests:
            if not any(evidence_covers(e, t) for e in evidence if isinstance(e, dict)) and status in {"TESTED", "VERIFIED", "CERTIFIED"}:
                warnings.append(f"{rid}: test {t} senza evidenza che lo esegua")
    if previous:
        gone = set(prev_ev) - set(ids)
        for rid in sorted(gone):
            errors.append(f"{rid}: requisito rimosso dal registro (storico non cancellabile)")
    return errors, warnings


def report(data):
    reqs = data["requirements"]
    c = Counter(r.get("status") for r in reqs)
    lines = [f"# Tracciabilità — {len(reqs)} requisiti", "", "| Stato | N |", "|---|---|"]
    lines += [f"| {k} | {v} |" for k, v in sorted(c.items())]
    lines += ["", "| ID | Stato | Decisione | Test | Evidenze | Finding aperti |", "|---|---|---|---|---|---|"]
    for r in reqs:
        open_f = [f.get("id") for f in (r.get("findings") or []) if f.get("state") not in {"FIXED", "ACCEPTED", "CLOSED"}]
        lines.append(f"| {r['id']} | {r['status']} | {r.get('owner_decision', '')} | {len(r.get('tests') or [])} | "
                     f"{len(r.get('evidence') or [])} | {', '.join(open_f) or '-'} |")
    return "\n".join(lines) + "\n"


def main(argv=None):
    ap = argparse.ArgumentParser(prog="pf_trace")
    sub = ap.add_subparsers(dest="cmd", required=True)
    v = sub.add_parser("validate"); v.add_argument("--registry", default=DEFAULT_REGISTRY); v.add_argument("--repo", default=".")
    v.add_argument("--strict", action="store_true", help="anche i warning sono errori")
    r = sub.add_parser("report"); r.add_argument("--registry", default=DEFAULT_REGISTRY)
    a = ap.parse_args(argv)
    try:
        data = load(os.path.join(a.repo if a.cmd == "validate" else ".", a.registry) if not os.path.isabs(a.registry) else a.registry)
    except (OSError, ValueError) as exc:
        print(json.dumps({"status": "ERROR", "reason": f"REGISTRY_UNREADABLE:{exc}"}))
        return 2
    if a.cmd == "report":
        print(report(data)); return 0
    errors, warnings = validate(data, a.repo, committed(a.registry, a.repo))
    status = "PASS" if not errors and not (a.strict and warnings) else "FAIL"
    print(json.dumps({"status": status, "requirements": len(data["requirements"]), "errors": errors, "warnings": warnings},
                     ensure_ascii=False, indent=1))
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
