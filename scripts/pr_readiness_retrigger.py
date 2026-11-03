#!/usr/bin/env python3
"""PF-CI-PERFORMANCE-FINAL-01 — riattivazione della Merge readiness quando il PRE-LIVE si conclude (event-driven).

La VPS esegue la certificazione, GitHub ne verifica il risultato, la Merge readiness decide. Con
``--pending-external-exit-after`` la readiness non occupa un runner per tutta la certificazione: termina NON pronta
(stato PENDING_EXTERNAL). Quando Pickfair Dedicated conclude il check ``pf-betfair-certification`` su uno SHA, GitHub
emette ``check_run: completed`` e il workflow ``pr-readiness-retrigger.yml`` (dal branch di default, mai codice
della PR) chiama questo script, che **rilancia la run di Merge readiness gia' esistente per quell'head**: la nuova
valutazione e' completa e il check resta attaccato all'head della PR.

Regole (fail-closed, idempotenti):
- solo eventi ``completed`` del check ``pf-betfair-certification`` dell'app ``pickfair-dedicated`` (chiunque puo'
  creare check con un'altra app o con un altro nome: non contano);
- solo PR aperte dello stesso repository (niente fork) il cui head e' ESATTAMENTE lo SHA del check: un risultato
  vecchio non viene mai attribuito a un head nuovo; se ``check_run.pull_requests`` e' vuoto si usano le PR del commit
  (``GET /commits/<sha>/pulls``) con gli stessi filtri;
- si rilancia solo l'ultima run ``pull_request`` di Merge readiness per quello SHA, se e' conclusa e NON e'
  ``success``; una run in corso non si tocca (doppia pubblicazione), una gia' verde non si rilancia;
- una run in corso partita PRIMA della conclusione del PRE-LIVE (per esempio mentre esce PENDING_EXTERNAL) puo' non
  averne visto l'esito: si attende che termini, entro ``--wait-running-seconds``, e poi si decide (rilievo di Sol su
  #603: prima l'unico evento andava perso). Una run partita DOPO legge gia' l'esito e non si attende;
- al massimo ``MAX_ATTEMPTS`` tentativi per run (anti-loop e anti-replay: un evento ripetuto trova la run in corso
  o al limite);
- nessuna scrittura oltre al rerun; ogni richiesta ha un timeout; un errore API non produce nulla (nessun verde).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

TRUSTED_CHECK = "pf-betfair-certification"
TRUSTED_APP = "pickfair-dedicated"
READINESS_WORKFLOW = "pr-merge-readiness.yml"
MAX_ATTEMPTS = 3
REQUEST_TIMEOUT_SECONDS = float(os.environ.get("PF_GH_CALL_TIMEOUT_SECONDS", "30"))
_SHA = re.compile(r"[0-9a-f]{40}")


class RetriggerError(RuntimeError):
    pass


def validate_event(event: Mapping[str, Any]) -> Tuple[str, List[int]]:
    """``(sha, [numeri PR])`` se l'evento e' un completamento fidato del PRE-LIVE, altrimenti ``RetriggerError``."""
    if event.get("action") != "completed":
        raise RetriggerError("NOT_COMPLETED")
    cr = event.get("check_run") or {}
    if cr.get("name") != TRUSTED_CHECK:
        raise RetriggerError("NOT_THE_PRELIVE_CHECK")
    if ((cr.get("app") or {}).get("slug")) != TRUSTED_APP:
        raise RetriggerError("UNTRUSTED_APP")
    if cr.get("status") != "completed":
        raise RetriggerError("CHECK_NOT_COMPLETED")
    sha = str(cr.get("head_sha") or "")
    if not _SHA.fullmatch(sha):
        raise RetriggerError("MALFORMED_SHA")
    prs = sorted({int(p["number"]) for p in cr.get("pull_requests") or []
                  if isinstance(p, dict) and isinstance(p.get("number"), int)})
    return sha, prs


def plan_reruns(repository: str, sha: str, prs: Mapping[int, Mapping[str, Any]],
                runs: List[Mapping[str, Any]], completed_at: Optional[str] = None) -> List[Dict[str, Any]]:
    """Decisioni per ogni PR: ``{"pr", "action": RERUN|SKIP|WAIT, "reason", "run_id"?}``. Funzione pura.

    WAIT = run in corso partita prima della conclusione del PRE-LIVE (o senza orari leggibili): va attesa."""
    out: List[Dict[str, Any]] = []
    candidates = sorted((r for r in runs if r.get("head_sha") == sha and r.get("event") == "pull_request"
                         and str(r.get("path") or "").endswith(READINESS_WORKFLOW)),
                        key=lambda r: int(r.get("id") or 0), reverse=True)
    for number, pr in sorted(prs.items()):
        if pr.get("state") != "open":
            out.append({"pr": number, "action": "SKIP", "reason": "PR_NOT_OPEN"})
            continue
        if ((pr.get("head") or {}).get("repo") or {}).get("full_name") != repository:
            out.append({"pr": number, "action": "SKIP", "reason": "FORK_PR"})
            continue
        if (pr.get("head") or {}).get("sha") != sha:
            out.append({"pr": number, "action": "SKIP", "reason": "STALE_SHA"})       # risultato vecchio, head nuovo
            continue
        mine = [r for r in candidates if any(p.get("number") == number for p in r.get("pull_requests") or [])
                or (r.get("head_branch") and r.get("head_branch") == (pr.get("head") or {}).get("ref"))]
        if not mine:
            out.append({"pr": number, "action": "SKIP", "reason": "NO_READINESS_RUN"})
            continue
        run = mine[0]
        if run.get("status") != "completed":
            started = str(run.get("run_started_at") or "")
            sees_result = bool(completed_at and started and started >= str(completed_at))   # ISO-8601 UTC
            out.append({"pr": number, "action": "SKIP" if sees_result else "WAIT", "reason": "READINESS_RUNNING",
                        "run_id": run.get("id")})
        elif run.get("conclusion") == "success":
            out.append({"pr": number, "action": "SKIP", "reason": "ALREADY_READY", "run_id": run.get("id")})
        elif int(run.get("run_attempt") or 1) >= MAX_ATTEMPTS:
            out.append({"pr": number, "action": "SKIP", "reason": "ATTEMPTS_EXHAUSTED", "run_id": run.get("id")})
        else:
            out.append({"pr": number, "action": "RERUN", "reason": "PRELIVE_COMPLETED", "run_id": run.get("id")})
    return out


class GitHub:
    """GET sul repository e una sola scrittura: ``POST /actions/runs/<id>/rerun``."""

    def __init__(self, repository: str, token: str, http: Optional[Callable[..., Tuple[int, bytes]]] = None):
        self.repository, self.token = repository, token
        self.http = http or self._urllib

    def _urllib(self, method: str, url: str) -> Tuple[int, bytes]:
        import urllib.error
        import urllib.request
        req = urllib.request.Request(url, method=method, data=b"" if method == "POST" else None,
                                     headers={"Accept": "application/vnd.github+json"})
        if self.token:
            req.add_unredirected_header("Authorization", f"Bearer {self.token}")
        try:
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SECONDS) as resp:  # noqa: S310
                return resp.status, resp.read()
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read() or b""

    def call(self, method: str, path: str) -> Any:
        allowed_post = re.fullmatch(rf"/repos/{re.escape(self.repository)}/actions/runs/\d+/rerun", path)
        if not path.startswith(f"/repos/{self.repository}/") or not (
                method == "GET" or (method == "POST" and allowed_post)):
            raise RetriggerError(f"REQUEST_REFUSED:{method} {path}")
        status, body = self.http(method, f"https://api.github.com{path}")
        if status not in (200, 201):
            raise RetriggerError(f"HTTP_{status}:{path}")
        return json.loads(body.decode("utf-8") or "{}")


def run(event: Mapping[str, Any], gh: GitHub, wait_seconds: float = 0, poll_seconds: float = 15,
        sleep: Callable[[float], None] = time.sleep, clock: Callable[[], float] = time.monotonic) -> Dict[str, Any]:
    try:
        sha, numbers = validate_event(event)
    except RetriggerError as exc:
        return {"actions": [], "skipped": str(exc)}
    completed_at = str((event.get("check_run") or {}).get("completed_at") or "") or None
    if not numbers:
        # GitHub puo' lasciare vuoto ``check_run.pull_requests`` (Grok su #603): si cercano le PR del commit, tenendo
        # solo quelle aperte, dello stesso repository e con head ESATTAMENTE questo SHA (plan_reruns lo ricontrolla).
        pulls = gh.call("GET", f"/repos/{gh.repository}/commits/{sha}/pulls?per_page=100")
        numbers = sorted({int(p["number"]) for p in pulls if isinstance(p, dict) and isinstance(p.get("number"), int)
                          and p.get("state") == "open"
                          and ((p.get("head") or {}).get("repo") or {}).get("full_name") == gh.repository
                          and (p.get("head") or {}).get("sha") == sha})

    def current_plan() -> List[Dict[str, Any]]:
        # Si rilegge tutto a ogni giro: un head cambiato durante l'attesa diventa STALE_SHA, mai un rerun.
        prs = {n: gh.call("GET", f"/repos/{gh.repository}/pulls/{n}") for n in numbers}
        runs = gh.call("GET", f"/repos/{gh.repository}/actions/workflows/{READINESS_WORKFLOW}/runs"
                              f"?head_sha={sha}&event=pull_request&per_page=50").get("workflow_runs", [])
        return plan_reruns(gh.repository, sha, prs, runs, completed_at)

    deadline = clock() + max(0.0, float(wait_seconds))
    plan = current_plan()
    while any(i["action"] == "WAIT" for i in plan) and clock() + poll_seconds <= deadline:
        sleep(poll_seconds)
        plan = current_plan()
    for item in plan:
        if item["action"] == "WAIT":                 # ancora in corso al limite: nessuna scrittura, motivo esplicito
            item["action"] = "SKIP"
    for item in plan:
        if item["action"] == "RERUN":
            gh.call("POST", f"/repos/{gh.repository}/actions/runs/{item['run_id']}/rerun")
            item["done"] = True
    return {"sha": sha, "actions": plan}


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Rilancia la Merge readiness al completamento del PRE-LIVE")
    ap.add_argument("--repo", required=True)
    ap.add_argument("--event", default=os.environ.get("GITHUB_EVENT_PATH", ""))
    ap.add_argument("--wait-running-seconds", type=float, default=0,
                    help="attesa massima di una readiness ancora in corso partita prima della fine del PRE-LIVE")
    ap.add_argument("--poll-seconds", type=float, default=15)
    args = ap.parse_args(argv)
    with open(args.event, encoding="utf-8") as fh:
        event = json.load(fh)
    try:
        result = run(event, GitHub(args.repo, os.environ.get("GH_TOKEN", "")),
                     wait_seconds=args.wait_running_seconds, poll_seconds=args.poll_seconds)
    except (RetriggerError, OSError, ValueError) as exc:
        print(json.dumps({"error": f"{type(exc).__name__}: {exc}"}))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
