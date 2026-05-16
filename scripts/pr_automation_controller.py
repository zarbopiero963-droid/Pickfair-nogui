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
AUTOFIX_COMMIT_ACTOR_ALLOWLIST = {"github-actions[bot]", "codex[bot]"}
CLEAN_SCOPE_ALLOWED_DEFAULT = [
    "scripts/pr_automation_controller.py",
    ".github/workflows/pr-automation-controller-v2.yml",
    "scripts/pr_clean_scope_rebuild.py",
]
CLEAN_SCOPE_FORBIDDEN_DEFAULT = [
    "order_manager.py",
    "core/reconciliation_engine.py",
    "guardrails/",
    "scripts/pr_flow_automation.py",
    ".github/scripts/",
]

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


def parse_csvish(value: str | None) -> list[str]:
    if not value:
        return []
    parts = re.split(r"[,\n]", value)
    return [p.strip() for p in parts if p.strip()]


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


def pr_files(repo: str, pr: str) -> list[str]:
    out = run(
        ["gh", "api", f"repos/{repo}/pulls/{pr}/files", "--paginate"],
        json_out=True,
        check=False,
    )
    if not isinstance(out, list):
        return []
    files: list[str] = []
    for row in out:
        if not isinstance(row, dict):
            continue
        name = str(row.get("filename") or "").strip()
        if name:
            files.append(name)
    return files


def pr_commits(repo: str, pr: str) -> list[dict[str, Any]]:
    out = run(
        ["gh", "api", f"repos/{repo}/pulls/{pr}/commits", "--paginate"],
        json_out=True,
        check=False,
    )
    return out if isinstance(out, list) else []


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


def path_matches(path: str, pattern: str) -> bool:
    p = pattern.strip()
    if not p:
        return False
    if p.endswith("/"):
        return path.startswith(p)
    return path == p


def find_forbidden_files(files: list[str], forbidden_patterns: list[str]) -> list[str]:
    hit: list[str] = []
    for f in files:
        if any(path_matches(f, p) for p in forbidden_patterns):
            hit.append(f)
    return sorted(set(hit))


def has_allowlisted_file(files: list[str], allowlist_patterns: list[str]) -> bool:
    return any(any(path_matches(file_path, pattern) for pattern in allowlist_patterns) for file_path in files)


def collect_clean_scope_signals(
    *,
    files: list[str],
    commits: list[dict[str, Any]],
    allowlist: list[str],
    forbidden: list[str],
    commit_limit: int,
) -> dict[str, Any]:
    forbidden_files = find_forbidden_files(files, forbidden)
    has_allowed = has_allowlisted_file(files, allowlist)
    limit_exceeded, autofix_commit_count = detect_autofix_limit_exceeded(commits, commit_limit)
    possible_oscillation = detect_possible_oscillation(commits)
    return {
        "forbidden_files": forbidden_files,
        "has_allowlisted_file": has_allowed,
        "autofix_commit_count": autofix_commit_count,
        "autofix_commit_limit_exceeded": limit_exceeded,
        "possible_autofix_oscillation": possible_oscillation,
    }


def detect_autofix_limit_exceeded(
    commits: list[dict[str, Any]],
    commit_limit: int,
) -> tuple[bool, int]:
    count = 0
    for c in commits:
        if not isinstance(c, dict):
            continue
        author = (c.get("author") or {}) if isinstance(c.get("author"), dict) else {}
        login = str(author.get("login") or "").strip().lower()
        msg = str(((c.get("commit") or {}).get("message") if isinstance(c.get("commit"), dict) else "") or "")
        first = msg.splitlines()[0].lower() if msg else ""
        if login in AUTOFIX_COMMIT_ACTOR_ALLOWLIST or "autofix" in first:
            count += 1
    return count > max(0, commit_limit), count


def detect_possible_oscillation(commits: list[dict[str, Any]]) -> bool:
    normalized: list[str] = []
    for c in commits[:8]:
        if not isinstance(c, dict):
            continue
        msg = str(((c.get("commit") or {}).get("message") if isinstance(c.get("commit"), dict) else "") or "")
        first = msg.splitlines()[0].strip().lower()
        if first:
            normalized.append(first)
    if len(normalized) < 4:
        return False
    repeats = len(normalized) - len(set(normalized))
    return repeats >= 2


def run_clean_scope_rebuild(
    *,
    repo: str,
    pr: str,
    head_branch: str,
    dry_run: bool,
    allowlist: list[str],
    forbidden: list[str],
) -> dict[str, Any]:
    decision_path = f"pr-clean-scope-rebuild-{pr}/decision.json"
    cmd = [
        sys.executable,
        "scripts/pr_clean_scope_rebuild.py",
        "--repo",
        repo,
        "--pr",
        pr,
        "--branch",
        head_branch,
        "--decision-out",
        decision_path,
    ]
    if allowlist:
        cmd.extend(["--allowlist", ",".join(allowlist)])
    if forbidden:
        cmd.extend(["--forbidden", ",".join(forbidden)])
    if dry_run:
        cmd.append("--dry-run")

    out = run(cmd, check=False)
    return {
        "action": "would_launch_clean_scope_rebuild" if dry_run else "launch_clean_scope_rebuild",
        "cmd": cmd,
        "output_tail": str(out).strip()[-2000:],
        "decision_path": decision_path,
    }


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
    ap.add_argument("--clean-scope-rebuild", action="store_true")
    ap.add_argument("--clean-scope-rebuild-mode", default="disabled", choices=["disabled", "detect", "execute"])
    ap.add_argument("--clean-scope-commit-limit", type=int, default=3)
    ap.add_argument("--clean-scope-allowlist", default="")
    ap.add_argument("--clean-scope-forbidden", default="")
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
    files = pr_files(args.repo, args.pr)
    commits = pr_commits(args.repo, args.pr)

    decision.update(
        {
            "state": pr.get("state"),
            "isDraft": pr.get("isDraft"),
            "mergeable": pr.get("mergeable"),
            "mergeStateStatus": pr.get("mergeStateStatus"),
            "headRefName": pr.get("headRefName"),
            "headRefOid": pr.get("headRefOid"),
            "url": pr.get("url"),
            "clean_scope_rebuild": bool(args.clean_scope_rebuild),
            "clean_scope_rebuild_mode": args.clean_scope_rebuild_mode,
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

    allowlist = parse_csvish(args.clean_scope_allowlist) or CLEAN_SCOPE_ALLOWED_DEFAULT
    forbidden = parse_csvish(args.clean_scope_forbidden) or CLEAN_SCOPE_FORBIDDEN_DEFAULT
    clean_scope_signals = collect_clean_scope_signals(
        files=files,
        commits=commits,
        allowlist=allowlist,
        forbidden=forbidden,
        commit_limit=args.clean_scope_commit_limit,
    )
    forbidden_files = clean_scope_signals["forbidden_files"]
    has_allowed = bool(clean_scope_signals["has_allowlisted_file"])
    limit_exceeded = bool(clean_scope_signals["autofix_commit_limit_exceeded"])
    autofix_commit_count = int(clean_scope_signals["autofix_commit_count"])
    possible_oscillation = bool(clean_scope_signals["possible_autofix_oscillation"])
    clean_scope_enabled = args.clean_scope_rebuild or args.clean_scope_rebuild_mode != "disabled"

    decision["clean_scope"] = {
        "enabled": clean_scope_enabled,
        "mode": args.clean_scope_rebuild_mode,
        "allowlist": allowlist,
        "forbidden": forbidden,
        "forbidden_files": forbidden_files,
        "has_allowlisted_file": has_allowed,
        "autofix_commit_limit": args.clean_scope_commit_limit,
        "autofix_commit_count": autofix_commit_count,
        "autofix_commit_limit_exceeded": limit_exceeded,
        "possible_autofix_oscillation": possible_oscillation,
    }

    scope_contaminated = bool(forbidden_files)
    rebuild_triggered = clean_scope_enabled and (scope_contaminated or limit_exceeded or possible_oscillation)
    safe_first_ok = should_launch and not scope_contaminated and not limit_exceeded and not possible_oscillation

    if safe_first_ok:
        action = launch_safe_autofix(
            repo=args.repo,
            pr=args.pr,
            dry_run=args.dry_run,
            max_rounds=args.safe_max_rounds,
            pending_wait_seconds=args.safe_pending_wait_seconds,
        )
        decision["actions"].append(action)
        decision["next_action"] = action.get("action")
    elif rebuild_triggered and args.clean_scope_rebuild_mode == "execute":
        action = run_clean_scope_rebuild(
            repo=args.repo,
            pr=args.pr,
            head_branch=str(pr.get("headRefName") or ""),
            dry_run=args.dry_run,
            allowlist=allowlist,
            forbidden=forbidden,
        )
        decision["actions"].append(action)
        decision["next_action"] = action.get("action")
    elif rebuild_triggered and args.clean_scope_rebuild_mode == "detect":
        decision["next_action"] = "clean_scope_rebuild_required_detect_mode"
    elif should_launch:
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
