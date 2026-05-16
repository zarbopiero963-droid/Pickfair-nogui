#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import subprocess
import time
from pathlib import Path
from typing import Any

FAIL_STATES = {"FAILURE", "ERROR", "ACTION_REQUIRED", "TIMED_OUT"}
PENDING_STATES = {"", "PENDING", "QUEUED", "IN_PROGRESS", "REQUESTED", "WAITING"}
CANCELLED_STATES = {"CANCELLED", "CANCELED"}

SAFE_AUTOFIX_WORKFLOW = "277606083"
CONTROLLER_WORKFLOW = "PR Automation Controller V2"

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

FORBIDDEN_PATHS = {
    "order_manager.py",
    "core/reconciliation_engine.py",
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


def pr_view(repo: str, pr_number: str) -> dict[str, Any]:
    return run(
        [
            "gh",
            "pr",
            "view",
            pr_number,
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
            "databaseId,status,conclusion,event,createdAt,url,headBranch,displayTitle",
        ],
        json_out=True,
        check=False,
    )
    if not isinstance(runs, list):
        return []
    return [entry for entry in runs if str(entry.get("status") or "") != "completed"]


def active_controller_rebuild_runs(repo: str, pr_number: str) -> list[dict[str, Any]]:
    runs = run(
        [
            "gh", "run", "list", "--repo", repo, "--workflow", CONTROLLER_WORKFLOW,
            "--limit", "40", "--json", "databaseId,status,conclusion,event,displayTitle,url",
        ],
        json_out=True,
        check=False,
    )
    if not isinstance(runs, list):
        return []

    pr_pattern = re.compile(rf"\bpr {re.escape(pr_number)}\b")
    active: list[dict[str, Any]] = []
    for run_item in runs:
        if str(run_item.get("status") or "") == "completed":
            continue
        title = str(run_item.get("displayTitle") or "").lower()
        if pr_pattern.search(title) and "clean" in title:
            active.append(run_item)
    return active


def rerun_cancelled_checks(*, repo: str, checks: list[dict[str, Any]], dry_run: bool, max_reruns: int) -> list[dict[str, Any]]:
    rerun: list[dict[str, Any]] = []
    seen: set[str] = set()

    for check in checks:
        if len(rerun) >= max_reruns:
            break
        if not is_cancelled(check) or is_self_check(check):
            continue

        run_id = extract_run_id(url_of(check))
        if not run_id or run_id in seen:
            continue
        seen.add(run_id)

        item: dict[str, Any] = {
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


def launch_safe_autofix(*, repo: str, pr_number: str, dry_run: bool, max_rounds: str, pending_wait_seconds: str) -> dict[str, Any]:
    active = active_safe_autofix_runs(repo)
    if active:
        return {"action": "skip_launch_safe_autofix", "reason": "safe_autofix_already_active", "active": active}

    cmd = [
        "gh", "workflow", "run", SAFE_AUTOFIX_WORKFLOW, "--repo", repo, "--ref", "main",
        "-f", f"pr_number={pr_number}",
        "-f", f"max_rounds={max_rounds}",
        "-f", f"pending_wait_seconds={pending_wait_seconds}",
        "-f", "dry_run=false",
    ]
    if dry_run:
        return {"action": "would_launch_safe_autofix", "cmd": cmd}
    out = run(cmd, check=False)
    return {"action": "launch_safe_autofix", "output": out.strip()[-1000:]}


def should_launch_autofix(checks: list[dict[str, Any]]) -> tuple[bool, list[dict[str, Any]]]:
    launchable: list[dict[str, Any]] = []
    for check in checks:
        if not is_failure(check) or is_self_check(check):
            continue
        name = name_of(check).lower()
        if name in DO_NOT_LAUNCH_AUTOFIX_FOR:
            continue
        launchable.append(compact_check(check))
    return bool(launchable), launchable


def list_changed_files(head_ref: str) -> list[str]:
    run(["git", "fetch", "origin", "main", head_ref], check=True)
    out = run(["git", "diff", "--name-only", f"origin/main...origin/{head_ref}"], check=False)
    return [line.strip() for line in out.splitlines() if line.strip()]


def is_forbidden(path: str) -> bool:
    if path in FORBIDDEN_PATHS:
        return True
    if path.startswith("guardrails/"):
        return True
    if path.startswith(".github/scripts/"):
        return True
    if path.startswith(".github/workflows/") and path != ".github/workflows/pr-automation-controller-v2.yml":
        return True
    return False


def analyze_recent_history(conclusions: list[str]) -> tuple[bool, bool]:
    latest_three = conclusions[:3]
    latest_four = conclusions[:4]
    exhausted = len(latest_three) == 3 and all(state in FAIL_STATES | CANCELLED_STATES for state in latest_three)
    oscillating = (
        len(latest_four) == 4
        and len(set(latest_four)) > 1
        and all(state in FAIL_STATES | CANCELLED_STATES for state in latest_four)
    )
    return exhausted, oscillating


def completed_runs_for_branch(runs: list[dict[str, Any]], head_branch: str) -> list[dict[str, Any]]:
    return [
        item
        for item in runs
        if str(item.get("headBranch") or "") == head_branch and str(item.get("status") or "") == "completed"
    ]


def build_history_summary(recent: list[dict[str, Any]]) -> dict[str, Any]:
    conclusions = [norm_state(item.get("conclusion")) for item in recent]
    exhausted, oscillating = analyze_recent_history(conclusions)
    return {
        "recent": recent,
        "conclusions": conclusions,
        "exhausted": exhausted,
        "oscillating": oscillating,
    }


def wait_for_pending_checks(
    *,
    pr_data: dict[str, Any],
    repo: str,
    pr_number: str,
    decision: dict[str, Any],
    started: float,
    pending_wait_seconds: int,
    poll_interval_seconds: int,
) -> tuple[dict[str, Any], bool]:
    while True:
        checks = pr_data.get("statusCheckRollup") or []
        pending = [compact_check(check) for check in checks if is_pending(check) and not is_self_check(check)]
        decision["pending_count"] = len(pending)
        decision["pending"] = pending[:40]
        if not pending:
            return pr_data, False
        if (time.time() - started) >= pending_wait_seconds:
            decision["next_action"] = "pending_wait_budget_exhausted"
            decision["warnings"].append("pending checks still present after wait budget")
            return pr_data, True
        time.sleep(max(1, poll_interval_seconds))
        pr_data = pr_view(repo, pr_number)


def should_rebuild_scope(clean_scope_rebuild: str, forbidden: list[str], history: dict[str, Any]) -> tuple[bool, list[str]]:
    should_rebuild = False
    reasons: list[str] = []
    mode = str(clean_scope_rebuild or "auto").lower()

    if mode in {"true", "1", "yes", "on", "force"}:
        return True, ["manual_clean_scope_rebuild"]
    if mode not in {"auto", ""}:
        return False, reasons

    if forbidden:
        should_rebuild = True
        reasons.append("forbidden_or_out_of_scope_files_in_diff")
    if history.get("exhausted"):
        should_rebuild = True
        reasons.append("safe_autofix_exhausted")
    if history.get("oscillating"):
        should_rebuild = True
        reasons.append("safe_autofix_oscillating")
    return should_rebuild, reasons


def safe_autofix_history(repo: str, head_branch: str) -> dict[str, Any]:
    runs = run(
        [
            "gh", "run", "list", "--repo", repo, "--workflow", SAFE_AUTOFIX_WORKFLOW, "--limit", "25",
            "--json", "databaseId,status,conclusion,headBranch,createdAt,url",
        ],
        json_out=True,
        check=False,
    )
    if not isinstance(runs, list):
        runs = []
    recent = completed_runs_for_branch(runs, head_branch)[:6]
    return build_history_summary(recent)


def parse_args() -> argparse.Namespace:
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
    ap.add_argument("--clean-scope-rebuild", default="auto")
    ap.add_argument("--clean-scope-rebuild-mode", default="auto")
    return ap.parse_args()


def initialize_decision(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "repo": args.repo,
        "pr": args.pr,
        "dry_run": args.dry_run,
        "actions": [],
        "warnings": [],
        "errors": [],
    }


def write_decision(path: str, decision: dict[str, Any]) -> int:
    Path(path).write_text(json.dumps(decision, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(decision, indent=2, sort_keys=True))
    return 0


def handle_non_open_pr(pr_data: dict[str, Any], decision: dict[str, Any], output_path: str) -> int | None:
    if pr_data.get("state") != "OPEN":
        decision["next_action"] = "skip_not_open"
        return write_decision(output_path, decision)
    if pr_data.get("isDraft"):
        decision["next_action"] = "skip_draft"
        return write_decision(output_path, decision)
    return None


def run_cancelled_flow(args: argparse.Namespace, checks: list[dict[str, Any]], decision: dict[str, Any], output_path: str) -> int | None:
    cancelled = [compact_check(check) for check in checks if is_cancelled(check) and not is_self_check(check)]
    decision["cancelled"] = cancelled
    if not cancelled:
        return None

    rerun = rerun_cancelled_checks(repo=args.repo, checks=checks, dry_run=args.dry_run, max_reruns=args.max_reruns)
    decision["actions"].append({"type": "rerun_cancelled", "items": rerun})
    decision["next_action"] = "rerun_cancelled_checks" if rerun else "cancelled_checks_no_rerun_target"
    return write_decision(output_path, decision)


def maybe_launch_clean_scope_rebuild(
    *,
    args: argparse.Namespace,
    decision: dict[str, Any],
    should_rebuild: bool,
    mode: str,
    rebuild_reasons: list[str],
) -> bool:
    if not should_rebuild:
        return False
    active_rebuild = active_controller_rebuild_runs(args.repo, args.pr)
    if active_rebuild:
        decision["actions"].append(
            {
                "type": "clean_scope_rebuild",
                "action": "skip",
                "reason": "rebuild_already_active",
                "active": active_rebuild,
            }
        )
        decision["next_action"] = "skip_launch_clean_scope_rebuild"
        return True
    decision["actions"].append(
        {"type": "clean_scope_rebuild", "action": "launch", "mode": mode, "reasons": rebuild_reasons}
    )
    decision["next_action"] = "launch_clean_scope_rebuild"
    return True


def maybe_launch_safe_autofix(
    *, args: argparse.Namespace, checks: list[dict[str, Any]], decision: dict[str, Any]
) -> bool:
    should_launch, launchable = should_launch_autofix(checks)
    decision["launchable_for_safe_autofix"] = launchable
    if not should_launch:
        return False
    action = launch_safe_autofix(
        repo=args.repo,
        pr_number=args.pr,
        dry_run=args.dry_run,
        max_rounds=args.safe_max_rounds,
        pending_wait_seconds=args.safe_pending_wait_seconds,
    )
    decision["actions"].append(action)
    decision["next_action"] = action.get("action")
    return True


def decide_next_action(args: argparse.Namespace, checks: list[dict[str, Any]], decision: dict[str, Any], pr_data: dict[str, Any]) -> None:
    blockers = [compact_check(check) for check in checks if is_failure(check) and not is_self_check(check)]
    decision["blockers"] = blockers

    head_ref = str(pr_data.get("headRefName") or "")
    changed_files = list_changed_files(head_ref)
    forbidden = [path for path in changed_files if is_forbidden(path)]
    decision["diff_files"] = changed_files
    decision["forbidden_in_diff"] = forbidden

    history = safe_autofix_history(args.repo, head_ref)
    decision["safe_autofix_history"] = history

    should_rebuild, rebuild_reasons = should_rebuild_scope(args.clean_scope_rebuild, forbidden, history)
    mode = str(args.clean_scope_rebuild_mode or "auto").lower()
    if maybe_launch_clean_scope_rebuild(
        args=args,
        decision=decision,
        should_rebuild=should_rebuild,
        mode=mode,
        rebuild_reasons=rebuild_reasons,
    ):
        return

    if maybe_launch_safe_autofix(args=args, checks=checks, decision=decision):
        return

    if blockers:
        decision["next_action"] = "manual_or_infrastructure_blockers"
    elif decision.get("pending_count", 0):
        decision["next_action"] = "wait_pending"
    else:
        decision["next_action"] = "checks_green_or_no_action"


def main() -> int:
    args = parse_args()
    started = time.time()
    decision = initialize_decision(args)
    output_path = args.output
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    try:
        pr_data = pr_view(args.repo, args.pr)
    except Exception as exc:
        decision["next_action"] = "error"
        decision["errors"].append(f"failed_to_load_pr: {exc}")
        return write_decision(output_path, decision)

    decision.update({
        "state": pr_data.get("state"),
        "isDraft": pr_data.get("isDraft"),
        "mergeable": pr_data.get("mergeable"),
        "mergeStateStatus": pr_data.get("mergeStateStatus"),
        "headRefName": pr_data.get("headRefName"),
        "headRefOid": pr_data.get("headRefOid"),
        "url": pr_data.get("url"),
        "clean_scope_rebuild": args.clean_scope_rebuild,
        "clean_scope_rebuild_mode": args.clean_scope_rebuild_mode,
    })

    early = handle_non_open_pr(pr_data, decision, output_path)
    if early is not None:
        return early

    pr_data, _pending_exhausted = wait_for_pending_checks(
        pr_data=pr_data,
        repo=args.repo,
        pr_number=args.pr,
        decision=decision,
        started=started,
        pending_wait_seconds=args.pending_wait_seconds,
        poll_interval_seconds=args.poll_interval_seconds,
    )

    checks = pr_data.get("statusCheckRollup") or []
    cancelled_result = run_cancelled_flow(args, checks, decision, output_path)
    if cancelled_result is not None:
        return cancelled_result

    decide_next_action(args, checks, decision, pr_data)
    return write_decision(output_path, decision)


if __name__ == "__main__":
    raise SystemExit(main())
