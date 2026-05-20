#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
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

ALLOWED_COMMAND_FAMILIES = {"gh", "git", "python", "python3", "pytest"}


def validate_command(cmd: list[str]) -> list[str]:
    """Validate and normalize a shell command."""
    if not cmd:
        raise ValueError("empty command")
    validate_command_family(Path(str(cmd[0])).name)
    validate_command_args(cmd)
    safe_cmd = list(cmd)
    executable = shutil.which(safe_cmd[0])
    if not executable:
        raise ValueError(f"command not found: {safe_cmd[0]}")
    safe_cmd[0] = executable
    return safe_cmd


def validate_command_family(family: str) -> None:
    """Reject command families outside the local allowlist."""
    if family not in ALLOWED_COMMAND_FAMILIES:
        raise ValueError(f"command family not allowed: {family}")


def validate_command_args(cmd: list[str]) -> None:
    """Reject non-string or null-byte command arguments."""
    for arg in cmd:
        if not isinstance(arg, str) or "\x00" in arg:
            raise ValueError("invalid command argument")


def sh(cmd: list[str], *, check: bool = True) -> str:
    safe_cmd = validate_command(cmd)
    proc = subprocess.run(  # nosec B603  # nosemgrep
        safe_cmd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if check and proc.returncode:
        raise RuntimeError(
            f"command failed: {' '.join(safe_cmd)}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )
    return proc.stdout.strip()


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
    """Return CODACY_API_TOKEN only when running inside GitHub Actions."""
    if os.environ.get("GITHUB_ACTIONS") != "true":
        raise RuntimeError("CODACY_API_TOKEN is only trusted inside GitHub Actions")
    token = os.environ.get("CODACY_API_TOKEN", "")
    if not token:
        raise RuntimeError("GitHub Actions CODACY_API_TOKEN secret is unavailable")
    return token


def codacy_url(repo: str, pr_number: str) -> str:
    """Build the Codacy pull-request issues endpoint URL."""
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
    """Validate that the Codacy endpoint uses the expected HTTPS host."""
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != "https" or parsed.netloc != "api.codacy.com":
        raise RuntimeError("invalid Codacy API URL")


def codacy_request_target(parsed: urllib.parse.ParseResult) -> str:
    """Build Codacy API request target from a parsed URL."""
    return f"{parsed.path}?{parsed.query}" if parsed.query else parsed.path


def codacy_https_request(target: str, token: str) -> tuple[int, str]:
    """Execute a Codacy HTTPS GET request and return status and payload text."""
    request = urllib.request.Request(
        f"https://api.codacy.com{target}",
        headers={"api-token": token},
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:  # nosec B310  # nosemgrep
            status = int(getattr(response, "status", 200))
            payload = response.read().decode("utf-8")
    except OSError as exc:
        raise RuntimeError("Codacy API request failed") from exc
    return status, payload


def codacy_http_response(url: str, token: str) -> tuple[int, str]:
    """Request Codacy API data and return status code and decoded payload."""
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != "https" or parsed.netloc != "api.codacy.com":
        raise RuntimeError("invalid Codacy API URL")
    return codacy_https_request(codacy_request_target(parsed), token)


def fetch_codacy_json(url: str, token: str) -> Any:
    """Fetch JSON payload from the validated Codacy API endpoint."""
    validate_codacy_url(url)
    parsed = urllib.parse.urlparse(url)
    _ = codacy_request_target(parsed)
    status, payload = codacy_http_response(url, token)
    if status >= 400:
        raise RuntimeError(f"Codacy API request failed with status {status}")
    return json.loads(payload or "{}")


def dict_items_from_list(value: Any) -> list[dict[str, Any]]:
    """Return only dictionary entries when the payload is a list."""
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def codacy_issue_items(body: Any) -> list[dict[str, Any]]:
    """Extract issue dictionaries from known Codacy response shapes."""
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
    """Unwrap nested Codacy `commitIssue` payloads into a single issue dict."""
    candidate = item.get("commitIssue")
    return candidate if isinstance(candidate, dict) else item


def nested_dict(source: dict[str, Any], key: str) -> dict[str, Any]:
    """Return a nested dictionary value or an empty dictionary."""
    value = source.get(key)
    return value if isinstance(value, dict) else {}


def first_nonempty(*values: Any) -> Any:
    """Return the first truthy value or an empty string."""
    for value in values:
        if value:
            return value
    return ""


def codacy_issue_record(item: dict[str, Any]) -> dict[str, Any]:
    """Normalize a Codacy issue item into a flat task record."""
    issue = issue_dict_from_item(item)
    pattern_info = nested_dict(issue, "patternInfo")
    tool_info = nested_dict(issue, "toolInfo") or nested_dict(issue, "tool")
    pattern_id = first_nonempty(pattern_info.get("id"), issue.get("patternId"), issue.get("patternID"))
    severity = first_nonempty(pattern_info.get("severityLevel"), pattern_info.get("level"), issue.get("severity"))
    return {
        "filePath": first_nonempty(issue.get("filePath"), issue.get("filename")),
        "lineNumber": issue.get("lineNumber"),
        "tool": first_nonempty(tool_info.get("name"), issue.get("toolName")),
        "patternId": pattern_id,
        "severity": severity,
        "message": first_nonempty(issue.get("message")),
    }


def codacy_task_lines(records: list[dict[str, Any]]) -> list[str]:
    """Render normalized Codacy issue records as markdown lines."""
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
    """Fetch and normalize Codacy pull-request issues."""
    body = fetch_codacy_json(codacy_url(repo, pr_number), codacy_api_token())
    raw = body if isinstance(body, dict) else {"data": body}
    return raw, codacy_issue_items(body)


def write_codacy_task(outdir: Path, raw: dict[str, Any], issues: list[dict[str, Any]]) -> None:
    """Persist Codacy raw payload and normalized task markdown in outdir."""
    records = [codacy_issue_record(item) for item in issues]
    write_json(outdir / "codacy-raw.json", raw)
    (outdir / "codacy-task.md").write_text(
        "\n".join(codacy_task_lines(records)) + "\n",
        encoding="utf-8",
    )


def codacy_is_blocking(repo: str, pr_number: str) -> bool:
    """Return True when Codacy appears among blocking PR checks."""
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


def build_telegram_summary(context: dict[str, Any]) -> dict[str, Any]:
    """Build a Telegram-ready summary payload from workflow context."""
    codacy = context.get("codacy")
    codacy_dict = codacy if isinstance(codacy, dict) else {}
    review = context.get("review")
    review_dict = review if isinstance(review, dict) else {}
    return {
        "pr_number": context.get("pr"),
        "head_sha": context.get("headRefOid"),
        "codacy_classification": codacy_dict.get("classification"),
        "github_codacy_check_state": context.get("github_codacy_check_state"),
        "codacy_api_issue_count": codacy_dict.get("issues_returned"),
        "active_unresolved_review_count": review_dict.get("unresolved_active"),
        "next_action": context.get("next_action"),
    }


def should_notify_ready_to_merge(context: dict[str, Any]) -> bool:
    """Return True when current context indicates PR is ready to merge."""
    bad = context.get("bad")
    unresolved_active = context.get("unresolved_active")
    mergeable = context.get("mergeable")
    merge_state_status = context.get("mergeStateStatus")
    return (
        isinstance(bad, list)
        and not bad
        and unresolved_active == 0
        and mergeable == "MERGEABLE"
        and merge_state_status == "CLEAN"
    )


def eligible_review_comments_for_auto_resolve(nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return only active unresolved review comments eligible for auto-resolve."""
    return [
        node
        for node in nodes
        if isinstance(node, dict)
        and node.get("isResolved") is False
        and node.get("isOutdated") is False
    ]


def classify_codacy_rule_conflict(issues: list[dict[str, Any]]) -> dict[str, Any]:
    """Detect contradictory Codacy D203/D211 rule findings on the same entity."""
    grouped_patterns: dict[tuple[str, str, str, str], set[str]] = {}
    for key, pattern_id in _iter_d203_d211_rule_records(issues):
        grouped_patterns.setdefault(key, set()).add(pattern_id)
        if len(grouped_patterns[key]) == 2:
            return _codacy_rule_conflict_result()
    return {"classification": "none", "next_action": ""}


def _iter_d203_d211_rule_records(
    issues: list[dict[str, Any]],
) -> list[tuple[tuple[str, str, str, str], str]]:
    records: list[tuple[tuple[str, str, str, str], str]] = []
    for issue in issues:
        if not isinstance(issue, dict):
            continue
        pattern_id = _codacy_rule_id(issue)
        if pattern_id not in {"D203", "D211"}:
            continue
        records.append((_codacy_rule_location_key(issue), pattern_id))
    return records


def _codacy_rule_id(issue: dict[str, Any]) -> str:
    return str(issue.get("patternId") or issue.get("patternID") or "").strip().upper()


def _codacy_rule_location_key(issue: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        _normalized_issue_file(issue),
        _normalized_issue_line(issue),
        _normalized_issue_column(issue),
        _normalized_issue_symbol(issue),
    )


def _normalized_issue_file(issue: dict[str, Any]) -> str:
    return _normalized_issue_field(issue, "filePath", "filename")


def _normalized_issue_line(issue: dict[str, Any]) -> str:
    return _normalized_issue_field(issue, "lineNumber", "line", "startLine")


def _normalized_issue_column(issue: dict[str, Any]) -> str:
    return _normalized_issue_field(issue, "column", "startColumn")


def _normalized_issue_symbol(issue: dict[str, Any]) -> str:
    return _normalized_issue_field(issue, "symbol", "entity")


def _normalized_issue_field(issue: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = issue.get(key)
        if value not in (None, ""):
            return str(value).strip()
    return ""


def _codacy_rule_conflict_result() -> dict[str, str]:
    return {
        "classification": "codacy_rule_conflict",
        "next_action": "needs_manual_codacy_rule_conflict",
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
    sh(["git", "fetch", "origin", branch], check=True)

    commits = safe_autofix_commits(branch_ref, str(args.pr)) if branch else []
    file_touches: dict[str, int] = {}
    for c in commits:
        for f in changed_files_for_commit(c):
            file_touches[f] = file_touches.get(f, 0) + 1

    if codacy_blocking and len(commits) > args.max_safe_autofix_commits:
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
    decision["ready_to_merge_notification"] = should_notify_ready_to_merge({
        "bad": decision.get("blockers"),
        "unresolved_active": 0,
        "mergeable": decision.get("mergeable"),
        "mergeStateStatus": decision.get("mergeStateStatus"),
    })
    decision["review_auto_resolve_candidates"] = len(eligible_review_comments_for_auto_resolve([]))
    decision["telegram_summary"] = build_telegram_summary({
        "pr": decision.get("pr"),
        "headRefOid": decision.get("headRefOid"),
        "codacy": {},
        "github_codacy_check_state": "",
        "review": {"unresolved_active": 0},
        "next_action": decision.get("next_action"),
    })
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
    """Write current Codacy issues to task artifacts for the autofix loop."""
    outdir = Path(args.outdir)
    blocking = codacy_is_blocking(args.repo, args.pr)
    try:
        raw, issues = fetch_codacy_pr_issues(args.repo, args.pr)
    except (RuntimeError, ValueError, OSError) as exc:
        return codacy_task_error_result(args, blocking, exc)

    write_codacy_task(outdir, raw, issues)
    codacy_rule_conflict = classify_codacy_rule_conflict(issues)
    result = {
        "repo": args.repo,
        "pr": str(args.pr),
        "ok": True,
        "codacy_blocking": blocking,
        "issues_returned": len(issues),
        "codacy_rule_conflict": codacy_rule_conflict,
        "codacy_raw": str(outdir / "codacy-raw.json"),
        "codacy_task": str(outdir / "codacy-task.md"),
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def is_non_fast_forward_push_error(exc: Exception) -> bool:
    """Return True when a push failure looks like a non-fast-forward conflict."""
    message = str(exc).lower()
    return "non-fast-forward" in message or "failed to push some refs" in message


@dataclass
class PushResultContext:
    repo: str
    branch: str
    retried: bool
    needs_manual: bool
    error: str | None = None
    initial_error: str | None = None


@dataclass
class PushRetryResult:
    ok: bool
    status: str
    ctx: PushResultContext

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "ok": self.ok,
            "status": self.status,
            "repo": self.ctx.repo,
            "branch": self.ctx.branch,
            "retried": self.ctx.retried,
            "needs_manual": self.ctx.needs_manual,
        }
        if self.ctx.error is not None:
            result["error"] = self.ctx.error
        if self.ctx.initial_error is not None:
            result["initial_error"] = self.ctx.initial_error
        return result


@dataclass
class NonFastForwardRetryContext:
    repo: str
    branch: str
    initial_exc: RuntimeError


def _push_initial(run_func: Any, remote: str, branch: str) -> None:
    run_func(["git", "push", remote, branch], check=True)


def _fetch_branch(run_func: Any, remote: str, branch: str) -> None:
    run_func(["git", "fetch", remote, branch], check=True)


def _push_force_with_lease(run_func: Any, remote: str, branch: str) -> None:
    run_func(["git", "push", remote, branch, "--force-with-lease"], check=True)


def push_retry_with_force_lease(run_func: Any, remote: str, branch: str) -> None:
    """Fetch latest remote branch, then retry push with force-with-lease once."""
    _fetch_branch(run_func, remote, branch)
    _push_force_with_lease(run_func, remote, branch)


def _build_push_result(
    ok: bool,
    status: str,
    ctx: PushResultContext,
) -> dict[str, Any]:
    return PushRetryResult(ok=ok, status=status, ctx=ctx).to_dict()


def _failed_push_result(repo: str, branch: str, exc: RuntimeError) -> dict[str, Any]:
    return _build_push_result(
        False,
        "failed",
        PushResultContext(repo=repo, branch=branch, retried=False, needs_manual=False, error=str(exc)),
    )


def _needs_manual_push_result(
    repo: str,
    branch: str,
    initial_exc: RuntimeError,
    retry_exc: RuntimeError,
) -> dict[str, Any]:
    return _build_push_result(
        False,
        "needs_manual",
        PushResultContext(
            repo=repo,
            branch=branch,
            retried=True,
            needs_manual=True,
            error=str(retry_exc),
            initial_error=str(initial_exc),
        ),
    )


def _retry_non_fast_forward_push(
    run_func: Any,
    ctx: NonFastForwardRetryContext,
    remote: str,
) -> dict[str, Any]:
    try:
        push_retry_with_force_lease(run_func, remote, ctx.branch)
        return _build_push_result(
            True, "success", PushResultContext(repo=ctx.repo, branch=ctx.branch, retried=True, needs_manual=False)
        )
    except RuntimeError as retry_exc:
        return _needs_manual_push_result(ctx.repo, ctx.branch, ctx.initial_exc, retry_exc)


def push_with_retry_once(
    run_func: Any,
    repo: str,
    branch: str,
    *,
    remote: str = "origin",
) -> dict[str, Any]:
    """Push once, recover once on non-fast-forward, then stop with explicit status."""
    try:
        _push_initial(run_func, remote, branch)
        return _build_push_result(
            True, "success", PushResultContext(repo=repo, branch=branch, retried=False, needs_manual=False)
        )
    except RuntimeError as exc:
        if not is_non_fast_forward_push_error(exc):
            return _failed_push_result(repo, branch, exc)
        retry_ctx = NonFastForwardRetryContext(repo=repo, branch=branch, initial_exc=exc)
        return _retry_non_fast_forward_push(run_func, retry_ctx, remote)


def codacy_task_error_result(
    args: argparse.Namespace,
    blocking: bool,
    exc: Exception,
) -> int:
    """Return the command result for Codacy-task fetch failures."""
    result = {
        "repo": args.repo,
        "pr": str(args.pr),
        "ok": False,
        "codacy_blocking": blocking,
        "error": f"{type(exc).__name__}: {exc}",
    }
    print(json.dumps(result, indent=2, sort_keys=True), file=sys.stderr)
    return 1 if blocking else 0


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
    push_status = push_with_retry_once(sh, args.repo, branch)
    if not push_status["ok"]:
        raise RuntimeError(f"cannot push canary branch: {push_status.get('error', 'unknown error')}")

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
