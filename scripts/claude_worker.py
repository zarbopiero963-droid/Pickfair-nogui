#!/usr/bin/env python3
"""PF-DISPATCHER-01 — worker di Claude B (mandato owner PF-CLAUDE-AUTO-DEV-01). **Solo dry-run.**

Consuma la decisione di ``scripts/roadmap_dispatcher.py`` e prepara, per UNA scheda alla volta, tutto cio' che una
sessione Claude Code headless eseguirebbe: lease, worktree isolato, prompt di lavoro, comandi, punti di arresto.
In dry-run **non esegue nulla**: niente ``claude``, niente ``git``, niente push, niente commenti. Scrive solo il piano
e un audit append-only nella propria directory di stato.

L'esecuzione reale (``--activate``) e' **disattivata per decisione owner** («OWNER NO-GO — ACTIVATE UNATTENDED
WRITE-ENABLED CLAUDE SESSIONS BEFORE SEPARATE APPROVAL»): richiede ``--activate`` E ``PF_WORKER_ACTIVATED=1`` E un
file di attivazione firmato dall'owner, che questo modulo non sa ancora verificare, quindi oggi rifiuta sempre.

Credenziali: il worker non legge, non stampa e non passa credenziali Claude o GitHub. La sessione ``claude`` usa il
proprio login sull'host (``claude`` autenticato dall'owner, credenziali fuori dal repository); i comandi del piano non
contengono token. Privilegi minimi: utente di servizio dedicato, nessun accesso a /etc/pf-orch, nessun segreto Betfair.

Ciclo per scheda (passi del mandato, ognuno con il suo arresto):
 1. scheda e decisione (SELECT o RESUME) dal dispatcher;
 2. baseline (``origin/main`` corrente) e requisiti (issue/scheda, CLAUDE.md, AGENTS.md, spec);
 3. worktree isolato ``<workdir>/<card>`` su branch ``auto/<card-lower>``;
 4. test RED (prima della patch, devono fallire);
 5. implementazione;
 6. test GREEN e regressione completa;
 7. PR (una sola; il dispatcher garantisce che non ce ne siano altre);
 8. CI e review (Sol, Grok, Codex) settled sull'head;
 9. correzioni entro ``MAX_FIX_LOOP_ITERATIONS_PER_PR`` (10), poi NEEDS_MANUAL;
10. arresto su qualunque gate owner (file a merge owner, decisione, crediti esauriti, budget);
11. ripresa dopo il merge: il dispatcher vede MERGED e attende VERIFIED prima della scheda successiva.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import json
import os
import re
import shlex
import socket
import sys
from typing import Any, Dict, List, Mapping, Optional, Sequence

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import roadmap_dispatcher as D  # noqa: E402

MAX_FIX_LOOP_ITERATIONS_PER_PR = 10
LEASE_TTL_SECONDS = 6 * 3600
STOP_CONDITIONS = (
    "file a merge owner nel diff (CLAUDE.md AUTO-MERGE: workflow, scripts/pr_*, CLAUDE.md, AGENTS.md, spec)",
    "decisione owner aperta o nuova decisione di prodotto",
    "crediti esauriti su Sol o Grok",
    f"budget fix loop esaurito ({MAX_FIX_LOOP_ITERATIONS_PER_PR} cicli)",
    "label manual-review-required",
    "qualsiasi operazione con denaro reale o Live Key",
    "check PRE-LIVE pf-betfair-certification non SUCCESS sull'head",
)
_SECRETISH = re.compile(r"(gh[pousr]_[A-Za-z0-9]{20,}|sk-ant-[A-Za-z0-9-]{10,}|xox[abp]-|AKIA[0-9A-Z]{16}|"
                        r"-----BEGIN [A-Z ]*PRIVATE KEY-----)")


class ActivationRefused(RuntimeError):
    pass


def branch_for(card: str) -> str:
    return "auto/" + re.sub(r"[^a-z0-9-]+", "-", card.lower()).strip("-")


def prompt_for(decision: Mapping[str, Any], repo: str, issue: int) -> str:
    """Il prompt di lavoro della sessione headless: rimanda alle fonti normative invece di ricopiarle."""
    card = decision["card"]
    lines = [
        f"Sei Claude B sul repository {repo}. Lavora SOLO sulla scheda {card} (lavorazione {decision['assignment']},"
        f" Roadmap V2 #{issue}).",
        "Prima di tutto leggi per intero CLAUDE.md, AGENTS.md e docs/auto_pr_flow_spec.md e seguili.",
        "Sequenza obbligatoria: baseline origin/main; test RED che falliscono sul codice attuale; patch minima; "
        "test GREEN; regressione completa; docs nello stesso PR; registra la task key in .guardrails/allowed_scope.json.",
        "Una sola PR. Nessun merge se non con tutte le condizioni AUTO-MERGE di CLAUDE.md; altrimenti verdetto "
        "esplicito all'owner.",
        f"Budget: al massimo {MAX_FIX_LOOP_ITERATIONS_PER_PR} cicli patch -> push; all'undicesimo fermati "
        "(NEEDS_MANUAL fix_loop_budget_exhausted).",
        "Fermati e registra OWNER_REQUIRED se incontri: " + "; ".join(STOP_CONDITIONS) + ".",
        "Mai ordini con denaro reale, mai Live Key, mai bypass di gate o protezioni, mai credenziali in log/commit.",
    ]
    if decision.get("action") == "RESUME":
        lines.insert(1, f"RIPRESA: la lavorazione e' nello stato {decision.get('state')}"
                        + (f" con PR #{decision['pr']}" if decision.get("pr") else "")
                        + ". Riparti dal punto registrato, non ricominciare da capo e non aprire una seconda PR.")
    return "\n".join(lines)


def build_plan(decision: Mapping[str, Any], *, repo: str, issue: int, workdir: str, holder: str,
               baseline: str = "origin/main") -> Dict[str, Any]:
    """Piano completo e deterministico per la decisione. Nessun effetto collaterale."""
    if decision.get("action") not in ("SELECT", "RESUME"):
        return {"action": "IDLE", "reason": decision.get("reason"), "commands": []}
    card = decision["card"]
    wt = os.path.join(workdir, re.sub(r"[^A-Za-z0-9._-]+", "_", card))
    branch = branch_for(card)
    prompt = prompt_for(decision, repo, issue)
    commands: List[List[str]] = [
        ["git", "-C", os.path.join(workdir, "_repo"), "fetch", "origin", "main"],
        ["git", "-C", os.path.join(workdir, "_repo"), "worktree", "add", "-B", branch, wt, baseline]
        if decision.get("action") == "SELECT" else
        ["git", "-C", wt, "status", "--short"],
        ["claude", "-p", prompt, "--output-format", "json", "--max-turns", "200"],
    ]
    plan = {
        "action": decision["action"], "card": card, "assignment": decision["assignment"],
        "state": decision.get("state"), "pr": decision.get("pr"), "holder": holder, "worktree": wt,
        "branch": branch, "baseline": baseline, "prompt_sha256": hashlib.sha256(prompt.encode()).hexdigest(),
        "prompt": prompt, "commands": commands, "stop_conditions": list(STOP_CONDITIONS),
        "events_to_publish": ["PR_OPEN", "CERTIFICATION_RUNNING", "MERGED", "VERIFIED", "OWNER_REQUIRED"],
    }
    blob = json.dumps(plan, sort_keys=True)
    if _SECRETISH.search(blob):
        raise D.DispatcherError("PLAN_CONTAINS_SECRET")
    return plan


def audit(state_dir: str, record: Mapping[str, Any]) -> None:
    os.makedirs(state_dir, mode=0o700, exist_ok=True)
    line = json.dumps(dict(record, ts=_dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")),
                      sort_keys=True)
    if _SECRETISH.search(line):
        raise D.DispatcherError("AUDIT_CONTAINS_SECRET")
    with open(os.path.join(state_dir, "audit.jsonl"), "a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def run_once(decision: Mapping[str, Any], *, repo: str, issue: int, workdir: str, state_dir: str,
             holder: Optional[str] = None, activate: bool = False, clock=None) -> Dict[str, Any]:
    """Un giro del worker. In dry-run: lease (per provare l'esclusivita'), piano, audit. Mai esecuzione."""
    holder = holder or f"{socket.gethostname()}:{os.getpid()}"
    if activate:
        # attivazione operativa non autorizzata: nessun percorso di esecuzione esiste in questo modulo
        audit(state_dir, {"event": "ACTIVATION_REFUSED", "holder": holder})
        raise ActivationRefused("OWNER_GO_REQUIRED: attivazione del worker non autorizzata (PF-CLAUDE-AUTO-DEV-01)")
    plan = build_plan(decision, repo=repo, issue=issue, workdir=workdir, holder=holder)
    if plan["action"] == "IDLE":
        audit(state_dir, {"event": "IDLE", "reason": plan["reason"]})
        return plan | {"dry_run": True}
    lease = D.LocalLease(os.path.join(state_dir, "lease.json"), **({"clock": clock} if clock else {}))
    got = lease.acquire(plan["assignment"], holder, LEASE_TTL_SECONDS)
    plan = plan | {"fencing": got["fencing"], "dry_run": True}
    with open(os.path.join(state_dir, "plan.json"), "w", encoding="utf-8") as fh:
        json.dump(plan, fh, sort_keys=True, indent=1)
    audit(state_dir, {"event": "PLANNED", "assignment": plan["assignment"], "fencing": got["fencing"],
                      "prompt_sha256": plan["prompt_sha256"], "holder": holder})
    return plan


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="PF-DISPATCHER-01 worker (solo dry-run)")
    ap.add_argument("--decision-file", help="decisione JSON del dispatcher (offline); altrimenti la calcola")
    ap.add_argument("--repo", default=D.REPO)
    ap.add_argument("--issue", type=int, default=D.ROADMAP_ISSUE)
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--state-dir", required=True)
    ap.add_argument("--trusted-login", action="append", default=[])
    ap.add_argument("--blocking-pr", action="append", type=int, default=[])
    ap.add_argument("--activate", action="store_true")
    args = ap.parse_args(argv)
    try:
        if args.decision_file:
            with open(args.decision_file, encoding="utf-8") as fh:
                decision = json.load(fh)
        else:
            gh = D.GitHub(args.repo, os.environ.get("GH_TOKEN", ""), roadmap_issue=args.issue)
            cards, events, prs = gh.snapshot(args.trusted_login or [args.repo.split("/", 1)[0]])
            decision = D.plan(cards, events, prs, args.blocking_pr).to_dict()
        plan = run_once(decision, repo=args.repo, issue=args.issue, workdir=args.workdir, state_dir=args.state_dir,
                        activate=args.activate)
    except (D.DispatcherError, D.LeaseHeld, ActivationRefused, OSError, ValueError) as exc:
        print(json.dumps({"action": "STOP", "reason": f"FAIL_CLOSED:{type(exc).__name__}:{exc}"}))
        return 2
    printable = dict(plan)
    printable["commands"] = [" ".join(shlex.quote(c) for c in cmd) for cmd in plan.get("commands", [])]
    print(json.dumps(printable, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
