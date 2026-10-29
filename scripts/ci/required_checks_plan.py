#!/usr/bin/env python3
"""Piano dei required check di `main` (#562 PR-C2): verifica, confronto col ruleset reale, valutazione di un head.

Non modifica nulla su GitHub. Il piano (`ops/ci/required_checks.json`) descrive lo stato attuale e le fasi; ogni fase
la applica l'owner dopo il proprio GO. Comandi:

    required_checks_plan.py validate --plan P
    required_checks_plan.py diff     --plan P --ruleset R.json          # R = GET /repos/{repo}/rulesets/{id}
    required_checks_plan.py payload  --plan P --phase N                 # regola `required_status_checks` dopo N fasi
    required_checks_plan.py evaluate --plan P --phase N --sha S --check-runs C.json --statuses T.json
    required_checks_plan.py matrix                                       # matrice Actions <-> Buildkite (markdown)

Regole (fail-closed):
- nessuna fase toglie un controllo (`overlap`, `full-suite`, `readiness`) che era coperto: prima si aggiunge il
  sostituto, poi si toglie il vecchio, quindi nessuna finestra senza protezione;
- un required check e' identificato da contesto **e** app che lo pubblica (`integration_id`): lo stesso nome
  pubblicato da un'altra app non conta;
- `evaluate` e' piu' severo di GitHub: per un required check vale solo `success` sullo SHA esatto. `skipped` e
  `neutral` di un check run (che GitHub accetta) qui bloccano; mancante, pending, failure, error e qualunque stato
  sconosciuto bloccano.

Exit: 0 = OK, 1 = piano non valido / divergenza / head bloccato, 2 = uso o input illeggibile.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import re
import sys
from pathlib import Path
from typing import Any, Iterable, Mapping

CONTROLS = ("overlap", "full-suite", "readiness")
KINDS = ("check_run", "status")
_SHA_RE = re.compile(r"[0-9a-f]{40}", re.ASCII)
_PENDING_RUN = frozenset({"queued", "in_progress", "waiting", "requested", "pending"})


class PlanError(Exception):
    """Piano malformato o incoerente."""


def _require(cond: bool, message: str) -> None:
    if not cond:
        raise PlanError(message)


def validate(plan: Mapping[str, Any]) -> list[set]:
    """Controlla il piano e restituisce l'insieme dei check richiesti dopo ogni fase (indice 0 = stato attuale)."""
    _require(isinstance(plan, dict), "piano: oggetto JSON atteso")
    pubs, checks = plan.get("publishers"), plan.get("checks")
    _require(isinstance(pubs, dict) and pubs, "publishers mancanti")
    for name, p in pubs.items():
        _require(isinstance(p, dict) and isinstance(p.get("integration_id"), int)
                 and not isinstance(p.get("integration_id"), bool) and p["integration_id"] > 0,
                 f"publisher {name}: integration_id intero positivo")
        _require(p.get("kind") in KINDS, f"publisher {name}: kind fra {KINDS}")
    _require(isinstance(checks, dict) and checks, "checks mancanti")
    seen = set()
    for key, c in checks.items():
        _require(isinstance(c, dict) and isinstance(c.get("context"), str) and c["context"].strip(),
                 f"check {key}: context mancante")
        _require(c.get("publisher") in pubs, f"check {key}: publisher sconosciuto")
        _require(c.get("control") in CONTROLS, f"check {key}: control fra {CONTROLS}")
        ident = (c["context"], pubs[c["publisher"]]["integration_id"])
        _require(ident not in seen, f"check {key}: duplicato di contesto e app")
        seen.add(ident)
    current = plan.get("current")
    _require(isinstance(current, dict) and isinstance(current.get("required"), list), "current.required mancante")
    _require(isinstance(current.get("strict"), bool), "current.strict booleano")
    state = set(current["required"])
    _require(state <= set(checks), "current: check sconosciuti")
    states = [set(state)]
    phases = plan.get("phases")
    _require(isinstance(phases, list) and phases, "phases mancanti")
    for i, ph in enumerate(phases, 1):
        _require(isinstance(ph, dict) and isinstance(ph.get("name"), str), f"fase {i}: nome mancante")
        add, rem = ph.get("add"), ph.get("remove")
        _require(isinstance(add, list) and isinstance(rem, list), f"fase {i}: add/remove liste")
        _require(set(add) <= set(checks) and set(rem) <= set(checks), f"fase {i}: check sconosciuti")
        _require(not set(add) & state, f"fase {i}: aggiunge un check gia' richiesto")
        _require(set(rem) <= state, f"fase {i}: toglie un check non richiesto")
        _require(isinstance(ph.get("strict"), bool), f"fase {i}: strict booleano")
        _require(isinstance(ph.get("precondition"), str) and len(ph["precondition"]) > 10,
                 f"fase {i}: precondizione mancante")
        before = {checks[k]["control"] for k in state}
        state = (state | set(add)) - set(rem)
        after = {checks[k]["control"] for k in state}
        # nessuna finestra: ogni controllo coperto prima resta coperto dopo la fase
        _require(before <= after, f"fase {i}: toglie la copertura di {sorted(before - after)}")
        states.append(set(state))
    never = plan.get("never_required") or []
    _require(isinstance(never, list) and all(isinstance(n, dict) and n.get("publisher") in pubs for n in never),
             "never_required malformato")
    forbidden = {(n["context"], n["publisher"]) for n in never}
    for i, st in enumerate(states):
        bad = sorted(k for k in st if (checks[k]["context"], checks[k]["publisher"]) in forbidden)
        _require(not bad, f"fase {i}: {bad} non puo' mai essere un required check")
    _require({checks[k]["control"] for k in states[-1]} == set(CONTROLS),
             "stato finale: overlap, full-suite e readiness devono essere tutti obbligatori")
    return states


def required_after(plan: Mapping[str, Any], phase: int) -> list[dict]:
    states = validate(plan)
    if not 0 <= phase < len(states):
        raise PlanError(f"fase {phase} inesistente (0..{len(states) - 1})")
    pubs, checks = plan["publishers"], plan["checks"]
    return [{"context": checks[k]["context"], "integration_id": pubs[checks[k]["publisher"]]["integration_id"]}
            for k in sorted(states[phase])]


def strict_after(plan: Mapping[str, Any], phase: int) -> bool:
    return plan["current"]["strict"] if phase == 0 else plan["phases"][phase - 1]["strict"]


def payload(plan: Mapping[str, Any], phase: int) -> dict:
    """La regola `required_status_checks` del ruleset dopo `phase` fasi, pronta per la UI o l'API."""
    required = required_after(plan, phase)                  # valida piano e fase prima di leggere `strict`
    return {"type": "required_status_checks", "parameters": {
        "strict_required_status_checks_policy": strict_after(plan, phase), "do_not_enforce_on_create": False,
        "required_status_checks": sorted(required, key=lambda r: (r["context"], r["integration_id"]))}}


def live_required(ruleset: Mapping[str, Any]) -> tuple[list[dict], bool]:
    rules = [r for r in ruleset.get("rules") or [] if isinstance(r, dict) and r.get("type") == "required_status_checks"]
    if len(rules) != 1:
        raise PlanError(f"ruleset: attesa una sola regola required_status_checks, trovate {len(rules)}")
    params = rules[0].get("parameters") or {}
    req = params.get("required_status_checks")
    if not isinstance(req, list) or not all(isinstance(r, dict) for r in req):
        raise PlanError("ruleset: required_status_checks illeggibile")
    return ([{"context": r.get("context"), "integration_id": r.get("integration_id")} for r in req],
            bool(params.get("strict_required_status_checks_policy")))


def diff(plan: Mapping[str, Any], ruleset: Mapping[str, Any]) -> list[str]:
    """Differenze fra il ruleset reale e lo stato `current` del piano: vuoto = piano allineato."""
    problems = []
    if ruleset.get("id") != plan["ruleset"]["id"]:
        problems.append(f"ruleset id {ruleset.get('id')} diverso dal piano {plan['ruleset']['id']}")
    if ruleset.get("enforcement") != "active":
        problems.append(f"ruleset non attivo: {ruleset.get('enforcement')}")
    if ruleset.get("bypass_actors"):
        problems.append("ruleset con bypass_actors: un required check si potrebbe saltare")
    live, strict = live_required(ruleset)
    key = lambda r: (str(r["context"]), r["integration_id"])            # noqa: E731
    want = sorted(required_after(plan, 0), key=key)
    if sorted(live, key=key) != want:
        problems.append(f"required check reali {sorted(live, key=key)} diversi dal piano {want}")
    if strict != plan["current"]["strict"]:
        problems.append(f"strict reale {strict} diverso dal piano {plan['current']['strict']}")
    return problems


def evaluate(plan: Mapping[str, Any], phase: int, sha: str, check_runs: Iterable[Mapping[str, Any]],
             statuses: Iterable[Mapping[str, Any]]) -> list[str]:
    """Motivi di blocco dell'head `sha` per i required check dopo `phase` fasi; vuoto = mergiabile per questi check.
    `check_runs` = elenco API `check-runs` (con `app.id`, `head_sha`); `statuses` = elenco API `statuses`, dal piu'
    recente, con `creator.login`."""
    if not _SHA_RE.fullmatch(sha or ""):
        raise PlanError("SHA non valido")
    pubs = plan["publishers"]
    by_id = {p["integration_id"]: p for p in pubs.values()}
    reasons = []
    for req in required_after(plan, phase):
        pub = by_id[req["integration_id"]]
        name = f"{req['context']} (app {req['integration_id']})"
        if pub["kind"] == "check_run":
            runs = [r for r in check_runs if isinstance(r, dict) and r.get("name") == req["context"]
                    and (r.get("app") or {}).get("id") == req["integration_id"]]
            if not runs:
                reasons.append(f"{name}: assente sull'head")
                continue
            if any(r.get("head_sha") != sha for r in runs):
                reasons.append(f"{name}: riferito a un altro SHA")
                continue
            for r in runs:
                status, concl = r.get("status"), r.get("conclusion")
                if status != "completed" or status in _PENDING_RUN:
                    reasons.append(f"{name}: non concluso ({status})")
                elif concl != "success":
                    reasons.append(f"{name}: {concl} (vale solo success)")
        else:
            creator = pub.get("status_creator")
            latest = next((s for s in statuses if isinstance(s, dict) and s.get("context") == req["context"]
                           and (s.get("creator") or {}).get("login") == creator), None)
            if latest is None:
                reasons.append(f"{name}: assente sull'head")
            elif latest.get("state") != "success":
                reasons.append(f"{name}: {latest.get('state')} (vale solo success)")
    return reasons


MATRIX_BEGIN = "<!-- matrix:begin (generata da `scripts/ci/required_checks_plan.py matrix`) -->"
MATRIX_END = "<!-- matrix:end -->"


def matrix_markdown() -> str:
    """Tabella workflow Actions -> step della full-suite (o motivo), dalle mappe di `scripts/pr_full_suite_pipeline.py`:
    una sola fonte, quindi la matrice del documento non puo' divergere dal generatore (test di sincronia)."""
    path = Path(__file__).resolve().parents[1] / "pr_full_suite_pipeline.py"
    spec = importlib.util.spec_from_file_location("pf_full_suite_pipeline_for_matrix", path)
    fsp = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = fsp
    spec.loader.exec_module(fsp)
    rows = [(wf, "MAP (su PR)", ", ".join(f"`{s}`" for s in st)) for wf, st in fsp.WORKFLOW_MAP.items()]
    rows += [(wf, "EXTRA (fuori PR)", ", ".join(f"`{s}`" for s in st)) for wf, st in fsp.WORKFLOW_EXTRA.items()]
    rows += [(wf, "EXCLUDED", r) for wf, r in fsp.WORKFLOW_EXCLUDED.items()]
    rows += [(wf, "NOT_MIGRATED", r) for wf, r in fsp.WORKFLOW_NOT_MIGRATED.items()]
    lines = ["| Workflow Actions | Gruppo | Step Buildkite (`full-suite`) / motivo |", "|---|---|---|"]
    lines += [f"| `{wf}` | {g} | {t.replace('|', '/')} |" for wf, g, t in sorted(rows)]
    return "\n".join(lines) + "\n"


def _load(path: str) -> Any:
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError) as exc:
        raise SystemExit(f"input illeggibile {path}: {exc}") from None


def main(argv: list) -> int:
    ap = argparse.ArgumentParser(prog="required_checks_plan.py")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("matrix")
    for name in ("validate", "diff", "payload", "evaluate"):
        p = sub.add_parser(name)
        p.add_argument("--plan", required=True)
        if name == "diff":
            p.add_argument("--ruleset", required=True)
        if name in ("payload", "evaluate"):
            p.add_argument("--phase", type=int, required=True)
        if name == "evaluate":
            p.add_argument("--sha", required=True)
            p.add_argument("--check-runs", required=True)
            p.add_argument("--statuses", required=True)
    args = ap.parse_args(argv)
    if args.cmd == "matrix":
        sys.stdout.write(matrix_markdown())
        return 0
    plan = _load(args.plan)
    try:
        if args.cmd == "validate":
            states = validate(plan)
            for i, st in enumerate(states):
                print(f"fase {i}: {sorted(st)}")
            return 0
        if args.cmd == "diff":
            problems = diff(plan, _load(args.ruleset))
            print("\n".join(problems) if problems else "OK: ruleset reale allineato allo stato attuale del piano")
            return 1 if problems else 0
        if args.cmd == "payload":
            print(json.dumps(payload(plan, args.phase), indent=2, sort_keys=True))
            return 0
        runs = _load(args.check_runs)
        runs = runs.get("check_runs") if isinstance(runs, dict) else runs
        reasons = evaluate(plan, args.phase, args.sha, runs or [], _load(args.statuses) or [])
        print("\n".join(f"BLOCK: {r}" for r in reasons) if reasons else "OK: tutti i required check success")
        return 1 if reasons else 0
    except PlanError as exc:
        print(f"PIANO NON VALIDO: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
