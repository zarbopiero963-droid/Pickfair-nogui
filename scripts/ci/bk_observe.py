#!/usr/bin/env python3
"""Osservabilità Buildkite in SOLA LETTURA della lane trusted (#562): build, step, log e artifact.

Solo richieste GET verso api.buildkite.com, host e pipeline fissi. Nessun token nel codice: nell'ambiente cloud di
Claude lo aggiunge il proxy di rete (Network secret); altrove si legge da BUILDKITE_API_TOKEN, mai stampato.
Ogni rapporto è legato a pipeline, numero di build, SHA esatto e `pf_intent`. Un dato mancante o una risposta
illeggibile è UNKNOWN, una build in corso è PENDING: mai PASS.

  bk_observe.py whoami
  bk_observe.py find      --sha <40 hex> | --intent <pf_intent>
  bk_observe.py report    --build 31 --sha <40 hex> --intent <pf_intent> [--expect-steps 49] [--budget 7200]
  bk_observe.py log       --build 31 --step w1-pr-check [--tail 200]
  bk_observe.py artifacts --build 31

Codici di uscita di `report`: 0 PASS, 1 FAIL, 3 PENDING, 4 UNKNOWN. Documentazione: docs/ci_buildkite_observability.md.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Callable

API = "https://api.buildkite.com/v2"
ORG = "zarbopiero963-droid"
PIPELINE = "pf-dedicated-ci"
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
WRITE_SCOPE_PREFIXES = ("write_", "delete_")
LAUNCHER_KEYS = ("pf-full-suite-launch",)   # step generatore della full-suite (dispatcher, da origin/main)

#: stati Buildkite di una build ancora viva
BUILD_ACTIVE = {"creating", "scheduled", "running", "blocked", "canceling", "failing"}
#: stati di uno step che non è ancora finito
STEP_ACTIVE = {"pending", "waiting", "scheduled", "assigned", "accepted", "running", "limited", "limiting",
               "reserved", "waiting_failed", "canceling", "timing_out"}
#: stati di uno step che non ha girato: non sono mai un PASS
STEP_NOT_RUN = {"skipped", "canceled", "broken", "blocked", "expired", "not_run"}
EXIT = {"PASS": 0, "FAIL": 1, "PENDING": 3, "UNKNOWN": 4}

Getter = Callable[..., object]


class Unknown(Exception):
    """Risposta assente o illeggibile: il verdetto è UNKNOWN."""


def http_get(path: str, params: dict | None = None, raw: bool = False):
    url = API + path + ("?" + urllib.parse.urlencode(params, doseq=True) if params else "")
    req = urllib.request.Request(url, method="GET", headers={"Accept": "text/plain" if raw else "application/json"})
    bearer = os.environ.get("BUILDKITE_API_TOKEN")
    if bearer:
        req.add_header("Authorization", "Bearer " + bearer)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310 - host fisso, solo GET
            body = resp.read().decode("utf-8", "replace")
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise Unknown(f"GET {path}: {type(exc).__name__}") from None
    if raw:
        return body
    try:
        return json.loads(body)
    except ValueError:
        raise Unknown(f"GET {path}: risposta non JSON") from None


def builds_path() -> str:
    return f"/organizations/{ORG}/pipelines/{PIPELINE}/builds"


def ts(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None


def secs(start, end):
    return round((ts(end) - ts(start)).total_seconds()) if start and end else None


def script_steps(build: dict) -> list:
    jobs = build.get("jobs")
    if not isinstance(jobs, list):
        raise Unknown("build senza elenco dei job")
    return [j for j in jobs if j.get("type") == "script" and j.get("step_key") not in LAUNCHER_KEYS]


def find_builds(get: Getter, sha: str | None = None, intent: str | None = None) -> list:
    params: dict = {"per_page": 100}
    if sha:
        if not SHA_RE.match(sha):
            raise ValueError("SHA non valido: servono 40 caratteri esadecimali minuscoli")
        params["commit"] = sha
    if intent:
        params["meta_data[pf_intent]"] = intent
    found = get(builds_path(), params)
    if not isinstance(found, list):
        raise Unknown("elenco build illeggibile")
    return found


def evaluate(build: dict, others: list, sha: str, intent: str, expect_steps: int | None, budget: int | None) -> dict:
    """Verdetto della build: PASS solo se SHA, intento, unicità, step, esito e durata tornano tutti."""
    meta = build.get("meta_data") or {}
    steps = script_steps(build)
    problems: list = []
    if build.get("commit") != sha:
        problems.append(f"SHA diverso: build {build.get('commit')} != atteso {sha}")
    if meta.get("pf_intent") != intent:
        problems.append(f"pf_intent diverso: {meta.get('pf_intent')!r} != {intent!r}")
    dupes = sorted(b.get("number") for b in others if b.get("number") != build.get("number"))
    if dupes:
        problems.append(f"build duplicate per lo stesso SHA/intento: {dupes}")
    if expect_steps is not None and len(steps) != expect_steps:
        problems.append(f"step: {len(steps)} != attesi {expect_steps}")
    not_run = [j.get("step_key") for j in steps if j.get("state") in STEP_NOT_RUN]
    failed = [j.get("step_key") for j in steps
              if j.get("state") not in STEP_ACTIVE | STEP_NOT_RUN | {"passed"}]
    if not_run:
        problems.append(f"step NOT RUN: {not_run}")
    if failed:
        problems.append(f"step falliti: {failed}")
    duration = secs(build.get("started_at"), build.get("finished_at"))
    if budget and duration is not None and duration > budget:
        problems.append(f"durata {duration}s oltre il budget {budget}s")
    state = build.get("state")
    if problems and (failed or not_run or dupes or build.get("commit") != sha or meta.get("pf_intent") != intent):
        verdict = "FAIL"
    elif state in BUILD_ACTIVE or any(j.get("state") in STEP_ACTIVE for j in steps):
        verdict = "PENDING"
    elif problems or state != "passed":
        verdict = "FAIL" if state in {"failed", "canceled", "passed"} else "UNKNOWN"
    elif duration is None:
        verdict = "UNKNOWN"
    else:
        verdict = "PASS"
    return {"verdict": verdict, "problems": problems, "steps": steps, "duration": duration, "meta": meta}


def render(build: dict, result: dict) -> str:
    steps = result["steps"]
    counts = ", ".join(f"{s}={sum(1 for j in steps if j.get('state') == s)}"
                       for s in sorted({str(j.get('state')) for j in steps}))
    meta = result["meta"]
    out = [f"# Buildkite {PIPELINE} #{build.get('number')} — {build.get('state')}",
           f"commit {build.get('commit')} · branch {build.get('branch')} · source {build.get('source')} · "
           f"pf_intent {meta.get('pf_intent')} · pf_pr {meta.get('pf_pr')}",
           f"creata {build.get('created_at')} · avviata {build.get('started_at')} · finita {build.get('finished_at')}"
           f" · durata {result['duration'] if result['duration'] is not None else 'IN CORSO'} s",
           f"step (escluso il launcher): {len(steps)} · {counts}", "", "| step | stato | exit | durata s |",
           "|---|---|---|---|"]
    out += [f"| `{j.get('step_key')}` | {j.get('state')} | {j.get('exit_status')} | "
            f"{secs(j.get('started_at'), j.get('finished_at')) or ''} |" for j in steps]
    out += [f"PROBLEMA: {p}" for p in result["problems"]]
    out.append(f"\nVERDETTO_BUILD = {result['verdict']}")
    return "\n".join(out)


def cmd_whoami(get: Getter, _args) -> int:
    info = get("/access-token")
    if not isinstance(info, dict):
        raise Unknown("access-token illeggibile")
    scopes = sorted(info.get("scopes") or [])
    print(json.dumps({"uuid": info.get("uuid"), "description": info.get("description"), "scopes": scopes,
                      "expires_at": info.get("expires_at")}, indent=2))
    extra = [s for s in scopes if s.startswith(WRITE_SCOPE_PREFIXES)]
    if extra:
        print(f"ATTENZIONE: il token ha permessi oltre la sola lettura: {extra}", file=sys.stderr)
        return 2
    return 0


def cmd_find(get: Getter, args) -> int:
    for b in find_builds(get, args.sha, args.intent):
        print(b.get("number"), b.get("state"), b.get("commit"), b.get("source"),
              (b.get("meta_data") or {}).get("pf_intent"), b.get("created_at"), b.get("web_url"))
    return 0


def cmd_report(get: Getter, args) -> int:
    build = get(f"{builds_path()}/{args.build}")
    if not isinstance(build, dict):
        raise Unknown("build illeggibile")
    others = find_builds(get, sha=args.sha) + find_builds(get, intent=args.intent)
    unique = {b.get("number"): b for b in others}.values()
    result = evaluate(build, list(unique), args.sha, args.intent, args.expect_steps, args.budget)
    print(render(build, result))
    return EXIT[result["verdict"]]


def cmd_log(get: Getter, args) -> int:
    build = get(f"{builds_path()}/{args.build}")
    for j in script_steps(build):
        if j.get("step_key") == args.step:
            lines = str(get(f"{builds_path()}/{args.build}/jobs/{j['id']}/log", raw=True)).splitlines()
            print("\n".join(lines[-args.tail:] if args.tail else lines))
            return 0
    print(f"step {args.step!r} assente dalla build", file=sys.stderr)
    return 2


def cmd_artifacts(get: Getter, args) -> int:
    for a in get(f"{builds_path()}/{args.build}/artifacts", {"per_page": 100}):
        print(a.get("job_id"), a.get("state"), a.get("path"), a.get("file_size"), a.get("sha1sum"))
    return 0


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("whoami")
    f = sub.add_parser("find")
    g = f.add_mutually_exclusive_group(required=True)
    g.add_argument("--sha")
    g.add_argument("--intent")
    r = sub.add_parser("report")
    r.add_argument("--build", type=int, required=True)
    r.add_argument("--sha", required=True)
    r.add_argument("--intent", required=True)
    r.add_argument("--expect-steps", type=int)
    r.add_argument("--budget", type=int, default=7200)
    lg = sub.add_parser("log")
    lg.add_argument("--build", type=int, required=True)
    lg.add_argument("--step", required=True)
    lg.add_argument("--tail", type=int, default=200)
    a = sub.add_parser("artifacts")
    a.add_argument("--build", type=int, required=True)
    return p


COMMANDS = {"whoami": cmd_whoami, "find": cmd_find, "report": cmd_report, "log": cmd_log, "artifacts": cmd_artifacts}


def main(argv=None, get: Getter = http_get) -> int:
    args = parser().parse_args(argv)
    try:
        return COMMANDS[args.cmd](get, args)
    except Unknown as exc:
        print(f"VERDETTO_BUILD = UNKNOWN ({exc})")
        return EXIT["UNKNOWN"]
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
