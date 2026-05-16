#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

FAIL_STATES = {"FAILURE", "ERROR", "ACTION_REQUIRED", "TIMED_OUT"}
PENDING_STATES = {"", "PENDING", "QUEUED", "IN_PROGRESS", "REQUESTED", "WAITING"}
CANCELLED_STATES = {"CANCELLED", "CANCELED"}

SAFE_AUTOFIX_WORKFLOW = "277606083"

SELF_CHECK_NAMES = {
    "safe pr autofix",
    "pr autofix safe supervisor",
    "autofix pr until checks are green",
    "pr automation controller",
}

DO_NOT_LAUNCH_AUTOFIX_FOR = {
    "merge readiness",
    "pr merge readiness",
    "pr flow guardrails",
    "refresh stale self checks",
}


def run(cmd: list[str], *, json_out: bool = False, check: bool = True) -> Any:
    try:
        out = subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as exc:
        if check:
            raise
        out = exc.output or ""
    if json_out:
        return json.loads(out or "null")
    return out


def norm_state(value: Any) -> str:
    return str(value or "").strip().upper()


def name_of(check: dict[str, Any]) -> str:
    return str(check.get("name") or check.get("context") or "").strip()


def url_of(check: dict[str, Any]) -> str:
    return str(check.get("detailsUrl") or check.get("targetUrl") or check.get("url") or "").strip()


def check_state(check: dict[str, Any]) -> str:
    return norm_state(check.get("conclusion") or check.get("state") or check.get("status"))


def is_self_check(check: dict[str, Any]) -> bool:
    name = name_of(check).lower()
    url = url_of(check).lower()
    return (
        name in SELF_CHECK_NAMES
        or "pr-autofix-safe-supervisor" in url
        or "pr-automation-controller" in url
        or "auto-pr-codex-fix" in url
        or "pr-autofix-selfhosted" in url
        or "auto-pr-e2e-autofix" in url
    )


def is_cancelled(check: dict[str, Any]) -> bool:
    return check_state(check) in CANCELLED_STATES


def is_pending(check: dict[str, Any]) -> bool:
    return check_state(check) in PENDING_STATES


def is_failure(check: dict[str, Any]) -> bool:
    return check_state(check) in FAIL_STATES


def extract_run_id(url: str) -> str:
    m = re.search(r"/actions/runs/(\d+)", str(url or ""))
    return m.group(1) if m else ""


def pr_view(repo: str, pr: str) -> dict[str, Any]:
    return run(
        [
            "gh",
            "pr",
            "view",
            pr,
            "--repo",
            repo,
            "--json",
            "number,title,state,isDraft,mergeable,mergeStateStatus,headRefName,headRefOid,statusCheckRollup,url",
        ],
        json_out=True,
    )


def compact_check(check: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": name_of(check),
        "state": check_state(check),
        "url": url_of(check),
        "self": is_self_check(check),
    }


def active_safe_autofix_runs(repo: str) -> list[dict[str, Any]]:
    runs = run(
        [
            "gh",
            "run",
            "list",
            "--repo",
            repo,
            "--workflow",
            SAFE_AUTOFIX_WORKFLOW,
            "--limit",
            "30",
            "--json",
            "databaseId,status,conclusion,event,createdAt,url",
        ],
        json_out=True,
        check=False,
    )
    if not isinstance(runs, list):
        return []
    return [r for r in runs if str(r.get("status") or "") != "completed"]


def rerun_cancelled_checks(
    *,
    repo: str,
    checks: list[dict[str, Any]],
    dry_run: bool,
    max_reruns: int,
) -> list[dict[str, Any]]:
    rerun: list[dict[str, Any]] = []
    seen: set[str] = set()

    for check in checks:
        if len(rerun) >= max_reruns:
            break
        if not is_cancelled(check):
            continue
        if is_self_check(check):
            continue

        run_id = extract_run_id(url_of(check))
        if not run_id or run_id in seen:
            continue
        seen.add(run_id)

        item = {
            "run_id": run_id,
            "check": name_of(check),
            "url": url_of(check),
            "action": "would_rerun" if dry_run else "rerun",
        }

        if not dry_run:
            out = run(["gh", "run", "rerun", run_id, "--repo", repo], check=False)
            item["output"] = out.strip()[-500:]

        rerun.append(item)

    return rerun


def launch_safe_autofix(
    *,
    repo: str,
    pr: str,
    dry_run: bool,
    max_rounds: str,
    pending_wait_seconds: str,
) -> dict[str, Any]:
    active = active_safe_autofix_runs(repo)
    if active:
        return {
            "action": "skip_launch_safe_autofix",
            "reason": "safe_autofix_already_active",
            "active": active,
        }

    cmd = [
        "gh",
        "workflow",
        "run",
        SAFE_AUTOFIX_WORKFLOW,
        "--repo",
        repo,
        "--ref",
        "main",
        "-f",
        f"pr_number={pr}",
        "-f",
        f"max_rounds={max_rounds}",
        "-f",
        f"pending_wait_seconds={pending_wait_seconds}",
        "-f",
        "dry_run=false",
    ]

    if dry_run:
        return {"action": "would_launch_safe_autofix", "cmd": cmd}

    out = run(cmd, check=False)
    return {
        "action": "launch_safe_autofix",
        "output": out.strip()[-1000:],
    }


def should_launch_autofix(checks: list[dict[str, Any]]) -> tuple[bool, list[dict[str, Any]]]:
    launchable: list[dict[str, Any]] = []

    for check in checks:
        if not is_failure(check):
            continue
        if is_self_check(check):
            continue

        name = name_of(check).lower()
        if name in DO_NOT_LAUNCH_AUTOFIX_FOR:
            continue

        launchable.append(compact_check(check))

    return bool(launchable), launchable


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True)
    ap.add_argument("--pr", required=True)
    ap.add_argument("--output", default=".pr-controller/decision.json")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--pending-wait-seconds", type=int, default=300)
    ap.add_argument("--poll-interval-seconds", type=int, default=20)
    ap.add_argument("--max-reruns", type=int, default=6)
    ap.add_argument("--safe-max-rounds", default="1")
    ap.add_argument("--safe-pending-wait-seconds", default="600")
    args = ap.parse_args()

    started = time.time()
    decision: dict[str, Any] = {
        "repo": args.repo,
        "pr": args.pr,
        "dry_run": args.dry_run,
        "actions": [],
        "warnings": [],
        "errors": [],
    }

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)

    try:
        pr = pr_view(args.repo, args.pr)
    except Exception as exc:
        decision["next_action"] = "error"
        decision["errors"].append(f"failed_to_load_pr: {exc}")
        Path(args.output).write_text(json.dumps(decision, indent=2, sort_keys=True), encoding="utf-8")
        print(json.dumps(decision, indent=2, sort_keys=True))
        return 0

    checks = pr.get("statusCheckRollup") or []

    decision.update(
        {
            "state": pr.get("state"),
            "isDraft": pr.get("isDraft"),
            "mergeable": pr.get("mergeable"),
            "mergeStateStatus": pr.get("mergeStateStatus"),
            "headRefName": pr.get("headRefName"),
            "headRefOid": pr.get("headRefOid"),
            "url": pr.get("url"),
        }
    )

    if pr.get("state") != "OPEN":
        decision["next_action"] = "skip_not_open"
        Path(args.output).write_text(json.dumps(decision, indent=2, sort_keys=True), encoding="utf-8")
        print(json.dumps(decision, indent=2, sort_keys=True))
        return 0

    if pr.get("isDraft"):
        decision["next_action"] = "skip_draft"
        Path(args.output).write_text(json.dumps(decision, indent=2, sort_keys=True), encoding="utf-8")
        print(json.dumps(decision, indent=2, sort_keys=True))
        return 0

    # Wait for pending checks before taking action.
    while True:
        checks = pr.get("statusCheckRollup") or []
        pending = [compact_check(c) for c in checks if is_pending(c) and not is_self_check(c)]
        decision["pending_count"] = len(pending)
        decision["pending"] = pending[:40]

        if not pending:
            break

        elapsed = time.time() - started
        if elapsed >= args.pending_wait_seconds:
            decision["next_action"] = "pending_wait_budget_exhausted"
            decision["warnings"].append("pending checks still present after wait budget")
            break

        time.sleep(max(1, args.poll_interval_seconds))
        pr = pr_view(args.repo, args.pr)

    checks = pr.get("statusCheckRollup") or []

    cancelled = [compact_check(c) for c in checks if is_cancelled(c) and not is_self_check(c)]
    decision["cancelled"] = cancelled

    if cancelled:
        rerun = rerun_cancelled_checks(
            repo=args.repo,
            checks=checks,
            dry_run=args.dry_run,
            max_reruns=args.max_reruns,
        )
        decision["actions"].append({"type": "rerun_cancelled", "items": rerun})
        decision["next_action"] = "rerun_cancelled_checks" if rerun else "cancelled_checks_no_rerun_target"
        Path(args.output).write_text(json.dumps(decision, indent=2, sort_keys=True), encoding="utf-8")
        print(json.dumps(decision, indent=2, sort_keys=True))
        return 0

    blockers = [compact_check(c) for c in checks if is_failure(c) and not is_self_check(c)]
    ignored_self = [compact_check(c) for c in checks if (is_failure(c) or is_cancelled(c) or is_pending(c)) and is_self_check(c)]
    decision["blockers"] = blockers
    decision["ignored_self_checks"] = ignored_self

    should_launch, launchable = should_launch_autofix(checks)
    decision["launchable_for_safe_autofix"] = launchable

    if should_launch:
        action = launch_safe_autofix(
            repo=args.repo,
            pr=args.pr,
            dry_run=args.dry_run,
            max_rounds=args.safe_max_rounds,
            pending_wait_seconds=args.safe_pending_wait_seconds,
        )
        decision["actions"].append(action)
        decision["next_action"] = action.get("action")
    elif blockers:
        decision["next_action"] = "manual_or_infrastructure_blockers"
    elif decision.get("pending_count", 0):
        decision["next_action"] = "wait_pending"
    else:
        decision["next_action"] = "checks_green_or_no_action"

    Path(args.output).write_text(json.dumps(decision, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(decision, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
