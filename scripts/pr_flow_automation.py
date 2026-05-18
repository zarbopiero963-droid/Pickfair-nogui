#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import http.client
import json
import os
import re
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path
from typing import Any

SELF_CHECK_NAMES = {
    "safe pr autofix",
    "pr autofix safe supervisor",
    "merge readiness",
    "pr merge readiness",
}

OK_STATES = {"SUCCESS", "SKIPPED", "NEUTRAL"}
PENDING_STATES = {"", "PENDING", "QUEUED", "IN_PROGRESS", "WAITING", "REQUESTED"}
BAD_STATES = {"FAILURE", "ERROR", "ACTION_REQUIRED", "CANCELLED", "TIMED_OUT", "STARTUP_FAILURE", "STALE"}

FLOW_WORKFLOWS = {
    "PR Autofix Safe Supervisor",
    "PR Merge Readiness",
    "PR Self Check Refresh",
    "PR Flow Guardrails",
}


def sh(cmd: list[str], *, check: bool = True) -> str:
    p = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if check and p.returncode != 0:
        raise RuntimeError(f"command failed: {' '.join(cmd)}\nSTDOUT:\n{p.stdout}\nSTDERR:\n{p.stderr}")
    return p.stdout.strip()


def gh_json(cmd: list[str]) -> Any:
    return json.loads(sh(cmd))


def norm_state(value: Any) -> str:
    return str(value or "").strip().upper()


def check_name(check: dict[str, Any]) -> str:
    return str(check.get("name") or check.get("context") or "").strip()


def check_url(check: dict[str, Any]) -> str:
    return str(check.get("detailsUrl") or check.get("targetUrl") or check.get("url") or "").strip()


def is_self_check(check: dict[str, Any]) -> bool:
    name = check_name(check).lower()
    url = check_url(check).lower()
    return (
        name in SELF_CHECK_NAMES
        or "pr-autofix-safe-supervisor" in url
        or "pr-merge-readiness" in url
        or "pr-self-check-refresh" in url
        or "pr-flow-guardrails" in url
    )


def pr_view(repo: str, pr: str) -> dict[str, Any]:
    return gh_json([
        "gh", "pr", "view", str(pr),
        "--repo", repo,
        "--json",
        "state,isDraft,mergeable,mergeStateStatus,reviewDecision,headRefOid,baseRefName,headRefName,statusCheckRollup,url,mergedAt,mergedBy,mergeCommit",
    ])


def split_checks(pr: dict[str, Any], *, ignore_self: bool = True) -> dict[str, list[dict[str, Any]]]:
    blockers: list[dict[str, Any]] = []
    pending: list[dict[str, Any]] = []
    ignored: list[dict[str, Any]] = []
    self_stale: list[dict[str, Any]] = []

    for raw in pr.get("statusCheckRollup") or []:
        item = {
            "name": check_name(raw),
            "state": norm_state(raw.get("conclusion") or raw.get("state") or raw.get("status")),
            "url": check_url(raw),
            "is_self_check": is_self_check(raw),
        }

        if ignore_self and item["is_self_check"]:
            item["ignored"] = True
            ignored.append(item)
            if item["state"] in BAD_STATES:
                self_stale.append(item)
            continue

        item["ignored"] = False
        if item["state"] in PENDING_STATES:
            pending.append(item)
        elif item["state"] not in OK_STATES:
            blockers.append(item)

    return {
        "blockers": blockers,
        "pending": pending,
        "ignored": ignored,
        "self_stale": self_stale,
    }


def effective_merge_state_ok(merge_state: str, blockers: list[dict[str, Any]], pending: list[dict[str, Any]]) -> bool:
    state = norm_state(merge_state)
    if state == "CLEAN":
        return True
    if state == "UNSTABLE" and not blockers and not pending:
        return True
    return False



def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def codacy_api_token() -> str:
    if os.environ.get("GITHUB_ACTIONS") != "true":
        raise RuntimeError("CODACY_API_TOKEN is only trusted inside GitHub Actions")
    token = os.environ.get("CODACY_API_TOKEN", "")
    if not token:
        raise RuntimeError("GitHub Actions CODACY_API_TOKEN secret is unavailable")
    return token


def codacy_url(repo: str, pr_number: str) -> str:
    owner, repo_name = repo.split("/", 1)
    provider = os.environ.get("CODACY_PROVIDER", "gh")
    params = urllib.parse.urlencode({
        "status": "new",
        "onlyPotential": "false",
        "limit": "100",
    })
    return (
        "https://api.codacy.com/api/v3/analysis/"
        f"organizations/{provider}/{urllib.parse.quote(owner, safe='')}/"
        f"repositories/{urllib.parse.quote(repo_name, safe='')}/"
        f"pull-requests/{urllib.parse.quote(str(pr_number), safe='')}/issues?{params}"
    )


def validate_codacy_url(url: str) -> None:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != "https" or parsed.netloc != "api.codacy.com":
        raise RuntimeError("invalid Codacy API URL")


def fetch_codacy_json(url: str, token: str) -> Any:
    validate_codacy_url(url)
    parsed = urllib.parse.urlparse(url)
    target = parsed.path
    if parsed.query:
        target = f"{target}?{parsed.query}"
    connection = http.client.HTTPSConnection(parsed.netloc, timeout=30)
    try:
        connection.request("GET", target, headers={"api-token": token})
        response = connection.getresponse()
        status = int(response.status)
        payload = response.read().decode("utf-8")
    except OSError as exc:
        raise RuntimeError("Codacy API request failed") from exc
    finally:
        connection.close()
    if status >= 400:
        raise RuntimeError(f"Codacy API request failed with status {status}")
    return json.loads(payload or "{}")


def dict_items_from_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def codacy_issue_items(body: Any) -> list[dict[str, Any]]:
    if isinstance(body, list):
        return dict_items_from_list(body)
    if not isinstance(body, dict):
        return []
    for key in ("data", "issues", "results", "items"):
        items = dict_items_from_list(body.get(key))
        if items:
            return items
    return []


def issue_dict_from_item(item: dict[str, Any]) -> dict[str, Any]:
    candidate = item.get("commitIssue")
    return candidate if isinstance(candidate, dict) else item


def nested_dict(source: dict[str, Any], key: str) -> dict[str, Any]:
    value = source.get(key)
    return value if isinstance(value, dict) else {}


def first_nonempty(*values: Any) -> Any:
    for value in values:
        if value:
            return value
    return ""


def codacy_issue_record(item: dict[str, Any]) -> dict[str, Any]:
    issue = issue_dict_from_item(item)
    pattern_info = nested_dict(issue, "patternInfo")
    tool_info = nested_dict(issue, "toolInfo") or nested_dict(issue, "tool")
    return {
        "filePath": first_nonempty(issue.get("filePath"), issue.get("filename")),
        "lineNumber": issue.get("lineNumber"),
        "tool": first_nonempty(tool_info.get("name"), issue.get("toolName")),
        "patternId": first_nonempty(
            pattern_info.get("id"),
            issue.get("patternId"),
            issue.get("patternID"),
        ),
        "severity": first_nonempty(
            pattern_info.get("severityLevel"),
            pattern_info.get("level"),
            issue.get("severity"),
        ),
        "message": first_nonempty(issue.get("message")),
    }


def codacy_task_lines(records: list[dict[str, Any]]) -> list[str]:
    lines = [
        "# Current Codacy API issues",
        "",
        f"Total: {len(records)}",
        "",
        "Fix only these current Codacy blockers. Preserve PR scope.",
        "",
    ]
    for index, record in enumerate(records, 1):
        tool_pattern = "/".join(
            str(value) for value in (record["tool"], record["patternId"]) if value
        ) or "unknown-tool-pattern"
        location = f"{record['filePath']}:{record['lineNumber']}"
        lines.append(
            f"{index}. {location} {tool_pattern} {record['severity']} - {record['message']}"
        )
    return lines


def fetch_codacy_pr_issues(repo: str, pr_number: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    body = fetch_codacy_json(codacy_url(repo, pr_number), codacy_api_token())
    raw = body if isinstance(body, dict) else {"data": body}
    return raw, codacy_issue_items(body)


def write_codacy_task(outdir: Path, raw: dict[str, Any], issues: list[dict[str, Any]]) -> None:
    records = [codacy_issue_record(item) for item in issues]
    write_json(outdir / "codacy-raw.json", raw)
    (outdir / "codacy-task.md").write_text(
        "\n".join(codacy_task_lines(records)) + "\n",
        encoding="utf-8",
    )


def codacy_is_blocking(repo: str, pr_number: str) -> bool:
    pr = pr_view(repo, pr_number)
    checks = split_checks(pr, ignore_self=True)
    return any("codacy" in f"{item['name']} {item['url']}".lower() for item in checks["blockers"])


def post_or_update_comment(repo: str, pr: str, marker: str, body: str) -> None:
    payload_path = Path(".pr-flow-comment-payload.json")
    payload_path.write_text(json.dumps({"body": body}), encoding="utf-8")
    comments = gh_json(["gh", "api", f"repos/{repo}/issues/{pr}/comments", "--paginate"])
    existing_id = None
    for c in comments:
        if marker in (c.get("body") or ""):
            existing_id = c.get("id")
            break
    if existing_id:
        sh(["gh", "api", f"repos/{repo}/issues/comments/{existing_id}", "--method", "PATCH", "--input", str(payload_path)])
    else:
        sh(["gh", "api", f"repos/{repo}/issues/{pr}/comments", "--method", "POST", "--input", str(payload_path)])
    payload_path.unlink(missing_ok=True)


def build_decision(repo: str, pr_number: str, *, ignore_self: bool = True) -> dict[str, Any]:
    pr = pr_view(repo, pr_number)
    checks = split_checks(pr, ignore_self=ignore_self)

    already_merged = bool(pr.get("mergedAt"))
    reasons: list[str] = []

    if already_merged:
        can_merge = True
    else:
        if pr.get("state") != "OPEN":
            reasons.append(f"PR state is {pr.get('state')}, expected OPEN")
        if pr.get("isDraft"):
            reasons.append("PR is draft")
        if pr.get("mergeable") != "MERGEABLE":
            reasons.append(f"mergeable is {pr.get('mergeable')}, expected MERGEABLE")
        if not effective_merge_state_ok(str(pr.get("mergeStateStatus") or ""), checks["blockers"], checks["pending"]):
            reasons.append(f"mergeStateStatus is {pr.get('mergeStateStatus')}, expected CLEAN")
        if checks["blockers"]:
            reasons.append(f"{len(checks['blockers'])} real blocking check(s)")
        if checks["pending"]:
            reasons.append(f"{len(checks['pending'])} real pending check(s)")
        can_merge = not reasons

    return {
        "repo": repo,
        "pr": str(pr_number),
        "url": pr.get("url"),
        "state": pr.get("state"),
        "already_merged": already_merged,
        "mergedAt": pr.get("mergedAt"),
        "mergedBy": (pr.get("mergedBy") or {}).get("login") if isinstance(pr.get("mergedBy"), dict) else pr.get("mergedBy"),
        "mergeCommit": (pr.get("mergeCommit") or {}).get("oid") if isinstance(pr.get("mergeCommit"), dict) else pr.get("mergeCommit"),
        "headRefName": pr.get("headRefName"),
        "headRefOid": pr.get("headRefOid"),
        "baseRefName": pr.get("baseRefName"),
        "mergeable": pr.get("mergeable"),
        "mergeStateStatus": pr.get("mergeStateStatus"),
        "reviewDecision": pr.get("reviewDecision"),
        "can_merge": can_merge,
        "reasons": reasons,
        "blockers": checks["blockers"],
        "pending": checks["pending"],
        "ignored_self_checks": checks["ignored"],
        "self_stale": checks["self_stale"],
        "next_action": "merge_allowed" if can_merge and not already_merged else ("already_merged" if already_merged else "blocked"),
    }


def cmd_readiness(args: argparse.Namespace) -> int:
    deadline = time.time() + args.wait_unknown_seconds
    decision = build_decision(args.repo, args.pr, ignore_self=args.ignore_safe_autofix)

    while (
        args.wait_unknown_seconds > 0
        and time.time() < deadline
        and not decision["already_merged"]
        and (decision["mergeable"] == "UNKNOWN" or decision["mergeStateStatus"] == "UNKNOWN")
    ):
        time.sleep(args.poll_seconds)
        decision = build_decision(args.repo, args.pr, ignore_self=args.ignore_safe_autofix)

    print(json.dumps(decision, indent=2, sort_keys=True))
    if args.output:
        write_json(Path(args.output), decision)
    return 0 if decision["can_merge"] or args.no_fail else 1


def current_run_id(url: str) -> str:
    m = re.search(r"/actions/runs/(\d+)", url or "")
    return m.group(1) if m else ""


def cmd_cleanup(args: argparse.Namespace) -> int:
    pr = pr_view(args.repo, args.pr)
    branch = pr.get("headRefName") or ""
    head = pr.get("headRefOid") or ""

    runs = gh_json([
        "gh", "run", "list",
        "--repo", args.repo,
        "--limit", str(args.limit),
        "--json", "databaseId,status,conclusion,event,workflowName,createdAt,headBranch,headSha,url",
    ])

    candidates = []
    for r in runs:
        if r.get("status") == "completed":
            continue
        wf = r.get("workflowName") or ""
        same_pr = r.get("headBranch") == branch or r.get("headSha") == head
        if wf in FLOW_WORKFLOWS and same_pr:
            candidates.append(r)

    by_workflow: dict[str, list[dict[str, Any]]] = {}
    for r in candidates:
        by_workflow.setdefault(r.get("workflowName") or "", []).append(r)

    cancelled = []
    kept = []
    for wf, items in by_workflow.items():
        items.sort(key=lambda x: x.get("createdAt") or "", reverse=True)
        keep = items[: args.keep_per_workflow]
        old = items[args.keep_per_workflow :]
        kept.extend(keep)
        for r in old:
            entry = {
                "databaseId": r.get("databaseId"),
                "workflowName": wf,
                "status": r.get("status"),
                "url": r.get("url"),
                "dry_run": args.dry_run,
            }
            if not args.dry_run:
                sh(["gh", "run", "cancel", str(r["databaseId"]), "--repo", args.repo], check=False)
            cancelled.append(entry)

    result = {
        "action": "cleanup_stale_runs",
        "repo": args.repo,
        "pr": str(args.pr),
        "branch": branch,
        "head": head,
        "kept": kept,
        "cancelled": cancelled,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    if args.output:
        write_json(Path(args.output), result)
    return 0


def safe_autofix_commits(branch_ref: str, pr: str) -> list[str]:
    out = sh([
        "git", "log", "--format=%H", "--grep", f"Safe autofix PR {pr}", branch_ref,
    ], check=False)
    return [x for x in out.splitlines() if x.strip()]


def changed_files_for_commit(commit: str) -> list[str]:
    out = sh(["git", "show", "--name-only", "--format=", commit], check=False)
    return [x for x in out.splitlines() if x.strip()]


def cmd_preflight(args: argparse.Namespace) -> int:
    pr = pr_view(args.repo, args.pr)
    checks = split_checks(pr, ignore_self=True)
    issues: list[str] = []
    warnings: list[str] = []

    codacy_blocking = any("codacy" in (b["name"] + " " + b["url"]).lower() for b in checks["blockers"])
    if codacy_blocking and os.environ.get("HAS_CODACY_API_TOKEN", "").lower() not in {"true", "1", "yes"}:
        issues.append("CODACY_API_TOKEN missing while Codacy is blocking")

    if os.environ.get("HAS_PICKFAIR_ACTIONS_TOKEN", "").lower() not in {"true", "1", "yes"}:
        warnings.append("PICKFAIR_ACTIONS_TOKEN appears missing/empty")

    branch = pr.get("headRefName") or ""
    branch_ref = f"origin/{branch}"
    sh(["git", "fetch", "origin", branch], check=False)

    commits = safe_autofix_commits(branch_ref, str(args.pr)) if branch else []
    file_touches: dict[str, int] = {}
    for c in commits:
        for f in changed_files_for_commit(c):
            file_touches[f] = file_touches.get(f, 0) + 1

    if len(commits) > args.max_safe_autofix_commits:
        issues.append(f"safe autofix commit limit exceeded: {len(commits)} > {args.max_safe_autofix_commits}")

    oscillating = [
        {"file": f, "touches": n}
        for f, n in sorted(file_touches.items())
        if n >= args.oscillation_touch_limit
    ]
    if oscillating and codacy_blocking:
        issues.append("possible autofix oscillation detected while Codacy is still blocking")

    result = {
        "repo": args.repo,
        "pr": str(args.pr),
        "state": pr.get("state"),
        "head": pr.get("headRefOid"),
        "branch": branch,
        "codacy_blocking": codacy_blocking,
        "safe_autofix_commits": commits,
        "file_touches": file_touches,
        "oscillating_files": oscillating,
        "issues": issues,
        "warnings": warnings,
        "ok": not issues,
    }

    print(json.dumps(result, indent=2, sort_keys=True))
    if args.output:
        write_json(Path(args.output), result)

    if issues and args.comment:
        marker = "<!-- pickfair-pr-flow-preflight -->"
        body = marker + "\n" + "## PR flow preflight stopped\n\n" + "\n".join(f"- {x}" for x in issues)
        if warnings:
            body += "\n\nWarnings:\n" + "\n".join(f"- {x}" for x in warnings)
        post_or_update_comment(args.repo, str(args.pr), marker, body)

    return 0 if not issues or args.no_fail else 1


def cmd_report(args: argparse.Namespace) -> int:
    decision = build_decision(args.repo, args.pr, ignore_self=True)
    outdir = Path(args.outdir)
    write_json(outdir / "pr-flow-decision.json", decision)

    if args.comment:
        marker = "<!-- pickfair-pr-flow-report -->"
        status = "✅ Merge allowed" if decision["can_merge"] and not decision["already_merged"] else (
            "✅ Already merged" if decision["already_merged"] else "⛔ Blocked"
        )
        lines = [
            marker,
            f"## PR flow report: {status}",
            "",
            f"- PR: #{args.pr}",
            f"- State: `{decision['state']}`",
            f"- Mergeable: `{decision['mergeable']}`",
            f"- Merge state: `{decision['mergeStateStatus']}`",
            f"- Real blockers: `{len(decision['blockers'])}`",
            f"- Real pending: `{len(decision['pending'])}`",
            f"- Ignored self-checks: `{len(decision['ignored_self_checks'])}`",
            f"- Next action: `{decision['next_action']}`",
        ]
        if decision["reasons"]:
            lines.append("")
            lines.append("Reasons:")
            lines.extend(f"- {r}" for r in decision["reasons"])
        post_or_update_comment(args.repo, str(args.pr), marker, "\n".join(lines) + "\n")

    print(json.dumps(decision, indent=2, sort_keys=True))
    return 0 if decision["can_merge"] or decision["already_merged"] or args.no_fail else 1


def cmd_codacy_task(args: argparse.Namespace) -> int:
    outdir = Path(args.outdir)
    blocking = codacy_is_blocking(args.repo, args.pr)
    try:
        raw, issues = fetch_codacy_pr_issues(args.repo, args.pr)
    except Exception as exc:
        result = {
            "repo": args.repo,
            "pr": str(args.pr),
            "ok": False,
            "codacy_blocking": blocking,
            "error": f"{type(exc).__name__}: {exc}",
        }
        print(json.dumps(result, indent=2, sort_keys=True), file=sys.stderr)
        return 1 if blocking else 0

    write_codacy_task(outdir, raw, issues)
    result = {
        "repo": args.repo,
        "pr": str(args.pr),
        "ok": True,
        "codacy_blocking": blocking,
        "issues_returned": len(issues),
        "codacy_raw": str(outdir / "codacy-raw.json"),
        "codacy_task": str(outdir / "codacy-task.md"),
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def cmd_canary(args: argparse.Namespace) -> int:
    ts = dt.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    branch = f"test/safe-autofix-canary-{ts}"
    filename = ".safe-autofix-auto-trigger-test.md"

    if args.mode == "cleanup":
        prs = gh_json([
            "gh", "pr", "list", "--repo", args.repo,
            "--state", "open",
            "--search", "safe autofix canary in:title",
            "--json", "number,headRefName,title",
        ])
        closed = []
        for pr in prs:
            number = str(pr["number"])
            sh(["gh", "pr", "close", number, "--repo", args.repo, "--delete-branch"], check=False)
            closed.append(number)
        result = {"action": "cleanup", "closed": closed}
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    sh(["git", "fetch", "origin", "main"])
    sh(["git", "checkout", "-B", branch, "origin/main"])
    Path(filename).write_text(
        "# TASK: safe-autofix-auto-trigger-test\n"
        f"safe autofix canary {ts}\n",
        encoding="utf-8",
    )
    sh(["git", "add", filename])
    sh(["git", "commit", "-m", "test: safe autofix canary"])
    sh(["git", "push", "-u", "origin", branch])

    title = f"test: safe autofix canary {ts}"
    body = (
        "Temporary canary PR for the automatic PR flow.\n\n"
        "Expected path:\n"
        "1. Codacy reports a controlled markdownlint issue.\n"
        "2. Safe autofix collects context.\n"
        "3. Codex fixes only allowed files.\n"
        "4. Merge readiness becomes clean.\n"
        "5. This PR can be closed after validation.\n"
    )
    url = sh([
        "gh", "pr", "create", "--repo", args.repo,
        "--base", "main", "--head", branch,
        "--title", title, "--body", body,
    ])
    print(json.dumps({"action": "created", "branch": branch, "url": url}, indent=2, sort_keys=True))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("readiness")
    p.add_argument("--repo", required=True)
    p.add_argument("--pr", required=True)
    p.add_argument("--ignore-safe-autofix", action="store_true")
    p.add_argument("--wait-unknown-seconds", type=int, default=0)
    p.add_argument("--poll-seconds", type=int, default=10)
    p.add_argument("--output", default="")
    p.add_argument("--no-fail", action="store_true")
    p.set_defaults(func=cmd_readiness)

    p = sub.add_parser("cleanup")
    p.add_argument("--repo", required=True)
    p.add_argument("--pr", required=True)
    p.add_argument("--limit", type=int, default=100)
    p.add_argument("--keep-per-workflow", type=int, default=1)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--output", default="")
    p.set_defaults(func=cmd_cleanup)

    p = sub.add_parser("preflight")
    p.add_argument("--repo", required=True)
    p.add_argument("--pr", required=True)
    p.add_argument("--max-safe-autofix-commits", type=int, default=3)
    p.add_argument("--oscillation-touch-limit", type=int, default=3)
    p.add_argument("--output", default="")
    p.add_argument("--comment", action="store_true")
    p.add_argument("--no-fail", action="store_true")
    p.set_defaults(func=cmd_preflight)

    p = sub.add_parser("report")
    p.add_argument("--repo", required=True)
    p.add_argument("--pr", required=True)
    p.add_argument("--outdir", default=".pr-flow")
    p.add_argument("--comment", action="store_true")
    p.add_argument("--no-fail", action="store_true")
    p.set_defaults(func=cmd_report)

    p = sub.add_parser("codacy-task")
    p.add_argument("--repo", required=True)
    p.add_argument("--pr", required=True)
    p.add_argument("--outdir", default=".autofix/context")
    p.set_defaults(func=cmd_codacy_task)

    p = sub.add_parser("canary")
    p.add_argument("--repo", required=True)
    p.add_argument("--mode", choices=["create", "cleanup"], default="create")
    p.set_defaults(func=cmd_canary)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
