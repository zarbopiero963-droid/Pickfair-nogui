#!/usr/bin/env python3
"""PR automation controller for safe-autofix and clean-scope escalation."""
# pylint: disable=too-many-arguments,too-many-locals,too-many-branches
# pylint: disable=too-many-statements,missing-function-docstring,invalid-name,duplicate-code
# pylint: disable=line-too-long,broad-exception-caught
from __future__ import annotations

import argparse
import http.client
import json
import os
import re
import subprocess  # nosec B404
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

FAIL_STATES = {"FAILURE", "ERROR", "ACTION_REQUIRED", "TIMED_OUT"}
PENDING_STATES = {"", "PENDING", "QUEUED", "IN_PROGRESS", "REQUESTED", "WAITING"}
CANCELLED_STATES = {"CANCELLED", "CANCELED"}

SAFE_AUTOFIX_WORKFLOW = "277606083"
AUTOFIX_COMMIT_ACTOR_ALLOWLIST = {"github-actions[bot]", "codex[bot]"}
CLEAN_SCOPE_ALLOWED_DEFAULT = [
    ".github/workflows/pr-autofix-selfhosted.yml",
    ".github/workflows/pr-autofix-safe-supervisor.yml",
    ".github/workflows/pr-automation-controller-v2.yml",
    ".github/workflows/pr-flow-guardrails.yml",
    ".github/workflows/pr-merge-readiness.yml",
    "scripts/pr_automation_controller.py",
    "scripts/pr_clean_scope_rebuild.py",
    "scripts/pr_flow_automation.py",
    "tests/scripts/test_pr_automation_controller.py",
    "tests/scripts/test_pr_flow_automation.py",
]
CLEAN_SCOPE_FORBIDDEN_DEFAULT = [
    "order_manager.py",
    "core/reconciliation_engine.py",
    "guardrails/",
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


ALLOWED_COMMAND_FAMILIES = {"gh", "git", "python", "python3", "pytest"}


def command_family(command: str) -> str:
    return Path(str(command)).name


def validate_command_family(cmd: Sequence[str]) -> None:
    if not cmd:
        raise ValueError("empty command")
    family = command_family(cmd[0])
    if family not in ALLOWED_COMMAND_FAMILIES:
        raise ValueError(f"command family not allowed: {family}")


def validate_command_arg(arg: str) -> None:
    if not isinstance(arg, str) or "\x00" in arg:
        raise ValueError("invalid command argument")


def validate_command_args(cmd: Sequence[str]) -> None:
    for arg in cmd:
        validate_command_arg(arg)


def combined_process_output(proc: subprocess.CompletedProcess[str]) -> str:
    return (proc.stdout or "") + (proc.stderr or "")


def parse_json_output(raw: str, cmd: Sequence[str]) -> Any:
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON output for command {cmd!r}: {raw}") from exc


def raise_if_command_failed(
    proc: subprocess.CompletedProcess[str],
    cmd: Sequence[str],
    output: str,
    check: bool,
) -> None:
    if check and proc.returncode:
        raise subprocess.CalledProcessError(proc.returncode, cmd, output=output)


def run(cmd: Sequence[str], *, json_out: bool = False, check: bool = True) -> Any:
    validate_command_family(cmd)
    validate_command_args(cmd)
    safe_cmd = list(cmd)
    proc = subprocess.run(  # nosec B603  # nosemgrep: python.lang.security.audit.dangerous-subprocess-use-audit.dangerous-subprocess-use-audit
        safe_cmd,  # nosemgrep: python.lang.security.audit.dangerous-subprocess-use-tainted-env-args.dangerous-subprocess-use-tainted-env-args
        text=True,
        capture_output=True,
        check=False,
    )
    out = combined_process_output(proc)
    raise_if_command_failed(proc, safe_cmd, out, check)
    return parse_json_output(out, safe_cmd) if json_out else out


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
    url_markers = (
        "pr-autofix-safe-supervisor",
        "pr-automation-controller",
        "auto-pr-codex-fix",
        "pr-autofix-selfhosted",
        "auto-pr-e2e-autofix",
    )
    return name in SELF_CHECK_NAMES or any(marker in url for marker in url_markers)


def is_cancelled(check: dict[str, Any]) -> bool:
    return check_state(check) in CANCELLED_STATES


def is_pending(check: dict[str, Any]) -> bool:
    return check_state(check) in PENDING_STATES


def is_failure(check: dict[str, Any]) -> bool:
    return check_state(check) in FAIL_STATES


def is_codacy_check(check: dict[str, Any]) -> bool:
    return "codacy" in f"{name_of(check)} {url_of(check)}".lower()


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
    return filenames_from_payload(out) if isinstance(out, list) else []


def filename_from_payload_row(row: Any) -> str:
    if not isinstance(row, dict):
        return ""
    return str(row.get("filename") or "").strip()


def filenames_from_payload(payload: list[Any]) -> list[str]:
    return [name for row in payload if (name := filename_from_payload_row(row))]


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
    runs = run(safe_autofix_runs_command(repo), json_out=True, check=False)
    if not isinstance(runs, list):
        return []
    return [r for r in runs if str(r.get("status") or "") != "completed"]


def safe_autofix_runs_command(repo: str) -> list[str]:
    return [
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
    ]



@dataclass(frozen=True)
class RerunConfig:

    """Configuration for rerunning cancelled checks."""

    repo: str
    dry_run: bool
    max_reruns: int


@dataclass(frozen=True)
class SafeAutofixConfig:

    """Configuration for invoking the safe autofix workflow."""

    repo: str
    pr_number: str
    dry_run: bool
    max_rounds: str
    pending_wait_seconds: str


@dataclass(frozen=True)
class CleanScopeRules:

    """Allowlist and forbidden scope constraints for clean rebuild mode."""

    allowlist: tuple[str, ...]
    forbidden: tuple[str, ...]
    commit_limit: int


@dataclass(frozen=True)
class CleanRebuildConfig:

    """Configuration for launching a clean-scope rebuild workflow."""

    repo: str
    pr_number: str
    head_branch: str
    dry_run: bool
    rules: CleanScopeRules


@dataclass(frozen=True)
class PendingWaitConfig:
    """Data container used by the automation flow."""

    repo: str
    pr_number: str
    started: float
    wait_seconds: int
    poll_interval_seconds: int


@dataclass(frozen=True)
class CleanScopeReport:

    """Data container used by the automation flow."""

    enabled: bool
    mode: str
    rules: CleanScopeRules
    signals: dict[str, Any]


@dataclass(frozen=True)
class NextActionContext:
    """Data container used by the automation flow."""

    args: argparse.Namespace
    pr: dict[str, Any]
    checks: list[dict[str, Any]]
    files: list[str]
    commits: list[dict[str, Any]]
    blockers: list[dict[str, Any]]
    decision: dict[str, Any]


def cancelled_non_self_checks(checks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [check for check in checks if is_cancelled(check) and not is_self_check(check)]


def already_seen_run(run_id: str, seen: set[str]) -> bool:
    return not run_id or run_id in seen


def rerun_item(check: dict[str, Any], run_id: str, dry_run: bool) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "check": name_of(check),
        "url": url_of(check),
        "action": "would_rerun" if dry_run else "rerun",
    }


def attach_rerun_output(item: dict[str, Any], repo: str, dry_run: bool) -> None:
    if dry_run:
        return
    out = run(["gh", "run", "rerun", str(item["run_id"]), "--repo", repo], check=False)
    item["output"] = out.strip()[-500:]


def rerun_cancelled_checks(
    checks: list[dict[str, Any]],
    config: RerunConfig,
) -> list[dict[str, Any]]:
    rerun: list[dict[str, Any]] = []
    seen: set[str] = set()
    for check in cancelled_non_self_checks(checks):
        if len(rerun) >= config.max_reruns:
            break
        run_id = extract_run_id(url_of(check))
        if already_seen_run(run_id, seen):
            continue
        seen.add(run_id)
        item = rerun_item(check, run_id, config.dry_run)
        attach_rerun_output(item, config.repo, config.dry_run)
        rerun.append(item)
    return rerun


def safe_autofix_command(config: SafeAutofixConfig) -> list[str]:
    return [
        "gh",
        "workflow",
        "run",
        SAFE_AUTOFIX_WORKFLOW,
        "--repo",
        config.repo,
        "--ref",
        "main",
        "-f",
        f"pr_number={config.pr_number}",
        "-f",
        f"max_rounds={config.max_rounds}",
        "-f",
        f"pending_wait_seconds={config.pending_wait_seconds}",
        "-f",
        "dry_run=false",
    ]


def launch_safe_autofix(config: SafeAutofixConfig) -> dict[str, Any]:
    active = active_safe_autofix_runs(config.repo)
    if active:
        return {
            "action": "skip_launch_safe_autofix",
            "reason": "safe_autofix_already_active",
            "active": active,
        }
    cmd = safe_autofix_command(config)
    if config.dry_run:
        return {"action": "would_launch_safe_autofix", "cmd": cmd}
    out = run(cmd, check=False)
    return {"action": "launch_safe_autofix", "output": out.strip()[-1000:]}


def should_launch_autofix(checks: list[dict[str, Any]]) -> tuple[bool, list[dict[str, Any]]]:
    launchable: list[dict[str, Any]] = []
    for check in checks:
        if not is_failure(check) or is_self_check(check):
            continue
        name = name_of(check).lower()
        if name not in DO_NOT_LAUNCH_AUTOFIX_FOR:
            launchable.append(compact_check(check))
    return bool(launchable), launchable


def path_matches(path: str, pattern: str) -> bool:
    pattern_text = pattern.strip()
    if not pattern_text:
        return False
    if pattern_text.endswith("/"):
        return path.startswith(pattern_text)
    return path == pattern_text


def matching_files(files: list[str], patterns: Sequence[str]) -> list[str]:
    return sorted({
        file_path
        for file_path in files
        if any(path_matches(file_path, pattern) for pattern in patterns)
    })


def find_forbidden_files(files: list[str], forbidden_patterns: Sequence[str]) -> list[str]:
    return matching_files(files, forbidden_patterns)


def has_allowlisted_file(files: list[str], allowlist_patterns: Sequence[str]) -> bool:
    return any(
        path_matches(file_path, pattern)
        for file_path in files
        for pattern in allowlist_patterns
    )


def commit_author_login(commit: dict[str, Any]) -> str:
    raw_author = commit.get("author")
    author: dict[str, Any] = raw_author if isinstance(raw_author, dict) else {}
    return str(author.get("login") or "").strip().lower()


def commit_first_line(commit: dict[str, Any]) -> str:
    raw_payload = commit.get("commit")
    payload: dict[str, Any] = raw_payload if isinstance(raw_payload, dict) else {}
    message = str(payload.get("message") or "")
    return message.splitlines()[0].lower() if message else ""


def is_autofix_commit(commit: dict[str, Any]) -> bool:
    login = commit_author_login(commit)
    first_line = commit_first_line(commit)
    return login in AUTOFIX_COMMIT_ACTOR_ALLOWLIST or "autofix" in first_line


def detect_autofix_limit_exceeded(
    commits: list[dict[str, Any]],
    commit_limit: int,
) -> tuple[bool, int]:
    count = sum(
        1
        for commit in commits
        if isinstance(commit, dict) and is_autofix_commit(commit)
    )
    return count > max(0, commit_limit), count


def detect_possible_oscillation(commits: list[dict[str, Any]]) -> bool:
    normalized = [
        commit_first_line(commit).strip().lower()
        for commit in commits[:8]
        if isinstance(commit, dict) and commit_first_line(commit).strip()
    ]
    if len(normalized) < 4:
        return False
    repeats = len(normalized) - len(set(normalized))
    return repeats >= 2


def collect_clean_scope_signals(
    files: list[str],
    commits: list[dict[str, Any]],
    rules: CleanScopeRules,
) -> dict[str, Any]:
    limit_exceeded, commit_count = detect_autofix_limit_exceeded(
        commits,
        rules.commit_limit,
    )
    return {
        "forbidden_files": find_forbidden_files(files, rules.forbidden),
        "has_allowlisted_file": has_allowlisted_file(files, rules.allowlist),
        "autofix_commit_count": commit_count,
        "autofix_commit_limit_exceeded": limit_exceeded,
        "possible_autofix_oscillation": detect_possible_oscillation(commits),
    }


def clean_rebuild_command(config: CleanRebuildConfig, decision_path: str) -> list[str]:
    return [
        sys.executable,
        "scripts/pr_clean_scope_rebuild.py",
        "--repo",
        config.repo,
        "--pr",
        config.pr_number,
        "--branch",
        config.head_branch,
        "--allowlist",
        ",".join(config.rules.allowlist),
        "--forbidden",
        ",".join(config.rules.forbidden),
        "--decision-out",
        decision_path,
    ]


def run_clean_scope_rebuild(config: CleanRebuildConfig) -> dict[str, Any]:
    decision_path = f"pr-clean-scope-rebuild-{config.pr_number}/decision.json"
    cmd = clean_rebuild_command(config, decision_path)
    if config.dry_run:
        cmd.append("--dry-run")
    out = run(cmd, check=False)
    action = "would_launch_clean_scope_rebuild" if config.dry_run else "launch_clean_scope_rebuild"
    return {
        "action": action,
        "cmd": cmd,
        "output_tail": str(out).strip()[-2000:],
        "decision_path": decision_path,
    }


def write_decision(output_path: str, decision: dict[str, Any]) -> int:
    Path(output_path).write_text(json.dumps(decision, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(decision, indent=2, sort_keys=True))
    return 0


def pending_checks_from_pr(pr: dict[str, Any]) -> list[dict[str, Any]]:
    checks = pr.get("statusCheckRollup") or []
    return [compact_check(check) for check in checks if is_pending(check) and not is_self_check(check)]


def store_pending_report(decision: dict[str, Any], pending: list[dict[str, Any]]) -> None:
    decision["pending_count"] = len(pending)
    decision["pending"] = pending[:40]


def pending_wait_elapsed(config: PendingWaitConfig) -> bool:
    return time.time() - config.started >= config.wait_seconds


def wait_for_pending_checks(
    pr: dict[str, Any],
    config: PendingWaitConfig,
    decision: dict[str, Any],
) -> tuple[dict[str, Any], bool]:
    while True:
        pending = pending_checks_from_pr(pr)
        store_pending_report(decision, pending)
        if not pending:
            return pr, False
        if pending_wait_elapsed(config):
            decision["next_action"] = "pending_wait_budget_exhausted"
            decision["warnings"].append("pending checks still present after wait budget")
            return pr, True
        time.sleep(max(1, config.poll_interval_seconds))
        pr = pr_view(config.repo, config.pr_number)


def handle_cancelled_checks(
    checks: list[dict[str, Any]],
    config: RerunConfig,
    decision: dict[str, Any],
) -> bool:
    cancelled = [compact_check(check) for check in cancelled_non_self_checks(checks)]
    decision["cancelled"] = cancelled
    if not cancelled:
        return False
    rerun = rerun_cancelled_checks(checks, config)
    decision["actions"].append({"type": "rerun_cancelled", "items": rerun})
    decision["next_action"] = "rerun_cancelled_checks" if rerun else "cancelled_checks_no_rerun_target"
    return True


def build_clean_scope_rules(args: argparse.Namespace) -> CleanScopeRules:
    return CleanScopeRules(
        allowlist=tuple(parse_csvish(args.clean_scope_allowlist) or CLEAN_SCOPE_ALLOWED_DEFAULT),
        forbidden=tuple(parse_csvish(args.clean_scope_forbidden) or CLEAN_SCOPE_FORBIDDEN_DEFAULT),
        commit_limit=args.clean_scope_commit_limit,
    )


def build_clean_scope_context(
    args: argparse.Namespace,
    files: list[str],
    commits: list[dict[str, Any]],
) -> tuple[CleanScopeRules, dict[str, Any]]:
    rules = build_clean_scope_rules(args)
    return rules, collect_clean_scope_signals(files, commits, rules)


def clean_scope_is_enabled(args: argparse.Namespace) -> bool:
    return bool(args.clean_scope_rebuild or args.clean_scope_rebuild_mode != "disabled")


def clean_scope_blocks_safe_autofix(signals: dict[str, Any]) -> bool:
    return bool(
        signals["forbidden_files"]
        or signals["autofix_commit_limit_exceeded"]
        or signals["possible_autofix_oscillation"]
    )


def clean_scope_should_rebuild(args: argparse.Namespace, signals: dict[str, Any]) -> bool:
    return clean_scope_is_enabled(args) and clean_scope_blocks_safe_autofix(signals)


def store_clean_scope_report(decision: dict[str, Any], report: CleanScopeReport) -> None:
    decision["clean_scope"] = {
        "enabled": report.enabled,
        "mode": report.mode,
        "allowlist": report.rules.allowlist,
        "forbidden": report.rules.forbidden,
        "forbidden_files": report.signals["forbidden_files"],
        "has_allowlisted_file": bool(report.signals["has_allowlisted_file"]),
        "autofix_commit_limit": report.rules.commit_limit,
        "autofix_commit_count": int(report.signals["autofix_commit_count"]),
        "autofix_commit_limit_exceeded": bool(report.signals["autofix_commit_limit_exceeded"]),
        "possible_autofix_oscillation": bool(report.signals["possible_autofix_oscillation"]),
    }


def safe_autofix_config_from_args(args: argparse.Namespace) -> SafeAutofixConfig:
    return SafeAutofixConfig(
        repo=args.repo,
        pr_number=args.pr,
        dry_run=args.dry_run,
        max_rounds=args.safe_max_rounds,
        pending_wait_seconds=args.safe_pending_wait_seconds,
    )


def clean_rebuild_config_from_context(
    ctx: NextActionContext,
    rules: CleanScopeRules,
) -> CleanRebuildConfig:
    return CleanRebuildConfig(
        repo=ctx.args.repo,
        pr_number=ctx.args.pr,
        head_branch=str(ctx.pr.get("headRefName") or ""),
        dry_run=ctx.args.dry_run,
        rules=rules,
    )


def record_action(decision: dict[str, Any], action: dict[str, Any]) -> None:
    decision["actions"].append(action)
    decision["next_action"] = action.get("action")


def set_no_launch_next_action(decision: dict[str, Any], blockers: list[dict[str, Any]]) -> None:
    if blockers:
        decision["next_action"] = "manual_or_infrastructure_blockers"
        return
    if decision.get("pending_count", 0):
        decision["next_action"] = "wait_pending"
        return
    decision["next_action"] = "checks_green_or_no_action"


def maybe_launch_safe_first(
    ctx: NextActionContext,
    should_launch: bool,
    signals: dict[str, Any],
) -> bool:
    if not should_launch or clean_scope_blocks_safe_autofix(signals):
        return False
    action = launch_safe_autofix(safe_autofix_config_from_args(ctx.args))
    record_action(ctx.decision, action)
    return True


def maybe_launch_clean_rebuild(
    ctx: NextActionContext,
    rules: CleanScopeRules,
    signals: dict[str, Any],
) -> bool:
    if not clean_scope_should_rebuild(ctx.args, signals):
        return False
    if ctx.args.clean_scope_rebuild_mode == "detect":
        ctx.decision["next_action"] = "clean_scope_rebuild_required_detect_mode"
        return True
    if ctx.args.clean_scope_rebuild_mode != "execute":
        return False
    action = run_clean_scope_rebuild(clean_rebuild_config_from_context(ctx, rules))
    record_action(ctx.decision, action)
    return True


def clean_scope_report_from_context(
    ctx: NextActionContext,
    rules: CleanScopeRules,
    signals: dict[str, Any],
) -> CleanScopeReport:
    return CleanScopeReport(
        enabled=clean_scope_is_enabled(ctx.args),
        mode=ctx.args.clean_scope_rebuild_mode,
        rules=rules,
        signals=signals,
    )


def safe_autofix_still_allowed(
    ctx: NextActionContext,
    should_launch: bool,
    signals: dict[str, Any],
) -> bool:
    valid_modes = {"disabled", "detect", "execute"}
    mode = str(ctx.args.clean_scope_rebuild_mode or "")
    return should_launch and mode in valid_modes and not clean_scope_blocks_safe_autofix(signals)


def dict_items_from_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def first_issue_list(body: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ("data", "issues", "results", "items"):
        items = dict_items_from_list(body.get(key))
        if items:
            return items
    return []


def codacy_issue_items(body: Any) -> list[dict[str, Any]]:
    if isinstance(body, list):
        return dict_items_from_list(body)
    if not isinstance(body, dict):
        return []
    return first_issue_list(body)


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
    pattern = nested_dict(issue, "patternInfo")
    tool = nested_dict(issue, "toolInfo")
    return {
        "filePath": first_nonempty(issue.get("filePath"), issue.get("filename")),
        "lineNumber": issue.get("lineNumber"),
        "message": first_nonempty(issue.get("message")),
        "patternId": first_nonempty(pattern.get("id"), issue.get("patternId")),
        "category": first_nonempty(pattern.get("category")),
        "severity": first_nonempty(pattern.get("severityLevel"), pattern.get("level")),
        "tool": first_nonempty(tool.get("name")),
    }


def codacy_api_token() -> str:
    if os.environ.get("GITHUB_ACTIONS") != "true":
        raise RuntimeError("Codacy API token is only trusted inside GitHub Actions")
    if os.environ.get("HAS_CODACY_API_TOKEN", "").lower() not in {"true", "1", "yes"}:
        raise RuntimeError("GitHub Actions CODACY_API_TOKEN secret is unavailable")
    token = os.environ.get("CODACY_API_TOKEN", "")
    if not token:
        raise RuntimeError("GitHub Actions CODACY_API_TOKEN secret is empty")
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


def codacy_request_target(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != "https" or parsed.netloc != "api.codacy.com":
        raise RuntimeError("invalid Codacy API URL")
    return f"{parsed.path}?{parsed.query}" if parsed.query else parsed.path


def fetch_json(url: str, token: str) -> Any:
    target = codacy_request_target(url)
    conn = http.client.HTTPSConnection("api.codacy.com", timeout=30)
    try:
        conn.request("GET", target, headers={"api-token": token})
        response = conn.getresponse()
        status = int(response.status)
        payload = response.read().decode("utf-8")
    except OSError as exc:
        raise RuntimeError("Codacy API request failed") from exc
    finally:
        conn.close()
    if status >= 400:
        raise RuntimeError(f"Codacy API request failed with status {status}")
    return json.loads(payload or "{}")


def fetch_codacy_pr_issues(repo: str, pr_number: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    token = codacy_api_token()
    url = codacy_url(repo, pr_number)
    validate_codacy_url(url)
    body = fetch_json(url, token)
    raw = body if isinstance(body, dict) else {"data": body}
    return raw, codacy_issue_items(body)


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def codacy_task_lines(records: list[dict[str, Any]]) -> list[str]:
    lines = [
        "# Current Codacy API issues",
        "",
        f"Total: {len(records)}",
        "",
        "Fix only these current Codacy blockers. Preserve PR scope.",
        "Codex must not call Codacy or DeepSource APIs directly.",
        "",
    ]
    for index, record in enumerate(records, 1):
        lines.append(
            f"{index}. {record['filePath']}:{record['lineNumber']} "
            f"{record['patternId']} {record['severity']} {record['tool']} - {record['message']}"
        )
    return lines


def write_codacy_task(outdir: Path, raw: dict[str, Any], issues: list[dict[str, Any]]) -> None:
    outdir.mkdir(parents=True, exist_ok=True)
    records = [codacy_issue_record(item) for item in issues]
    write_json(outdir / "codacy-raw.json", raw)
    write_json(outdir / "codacy-issues.json", records)
    (outdir / "codacy-task.md").write_text(
        "\n".join(codacy_task_lines(records)) + "\n",
        encoding="utf-8",
    )


def cmd_codacy_task(args: argparse.Namespace) -> int:
    outdir = Path(args.outdir)
    raw, issues = fetch_codacy_pr_issues(args.repo, args.pr)
    write_codacy_task(outdir, raw, issues)
    result = {
        "repo": args.repo,
        "pr": str(args.pr),
        "issues_returned": len(issues),
        "codacy_raw": str(outdir / "codacy-raw.json"),
        "codacy_issues": str(outdir / "codacy-issues.json"),
        "codacy_task": str(outdir / "codacy-task.md"),
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def codacy_evidence_without_checks() -> dict[str, Any]:
    return {
        "checks": [],
        "check_blocking": False,
        "api_available": False,
        "api_ok": False,
        "issues_returned": 0,
        "blocking": False,
        "ignored": False,
        "reason": "no Codacy check blocker",
    }


def codacy_api_status(
    repo: str,
    pr_number: str,
) -> tuple[bool, list[dict[str, Any]], str]:
    try:
        _, issues = fetch_codacy_pr_issues(repo, pr_number)
        return True, issues, f"Codacy API returned {len(issues)} current issue(s)"
    except Exception as exc:
        return False, [], f"Codacy API unavailable ({type(exc).__name__}): {exc}"


def codacy_evidence_from_api(
    codacy_checks: list[dict[str, Any]],
    api_ok: bool,
    issues: list[dict[str, Any]],
    reason: str,
) -> dict[str, Any]:
    return {
        "checks": codacy_checks,
        "check_blocking": bool(codacy_checks),
        "api_available": api_ok,
        "api_ok": api_ok,
        "issues_returned": len(issues),
        "blocking": bool(codacy_checks) if not api_ok else bool(issues),
        "ignored": api_ok and not issues,
        "reason": reason,
    }


def controller_codacy_blocking_evidence(
    repo: str,
    pr_number: str,
    codacy_checks: list[dict[str, Any]],
) -> dict[str, Any]:
    if not codacy_checks:
        return codacy_evidence_without_checks()
    api_ok, issues, reason = codacy_api_status(repo, pr_number)
    return codacy_evidence_from_api(codacy_checks, api_ok, issues, reason)


def decide_next_action(ctx: NextActionContext) -> None:
    should_launch, launchable = should_launch_autofix(ctx.checks)
    ctx.decision["launchable_for_safe_autofix"] = launchable
    rules, signals = build_clean_scope_context(ctx.args, ctx.files, ctx.commits)
    store_clean_scope_report(ctx.decision, clean_scope_report_from_context(ctx, rules, signals))
    if maybe_launch_safe_first(ctx, should_launch, signals):
        return
    if maybe_launch_clean_rebuild(ctx, rules, signals):
        return
    if safe_autofix_still_allowed(ctx, should_launch, signals):
        record_action(ctx.decision, launch_safe_autofix(safe_autofix_config_from_args(ctx.args)))
        return
    set_no_launch_next_action(ctx.decision, ctx.blockers)


def parse_controller_args() -> argparse.Namespace:
    if len(sys.argv) > 1 and sys.argv[1] == "codacy-task":
        parser = argparse.ArgumentParser()
        parser.add_argument("command")
        parser.add_argument("--repo", required=True)
        parser.add_argument("--pr", required=True)
        parser.add_argument("--outdir", default=".autofix/context")
        return parser.parse_args()
    parser = argparse.ArgumentParser()
    add_controller_core_args(parser)
    add_controller_clean_scope_args(parser)
    args = parser.parse_args()
    normalize_controller_args(args)
    return args


def add_controller_core_args(parser: argparse.ArgumentParser) -> None:
    for name in ("repo", "pr"):
        parser.add_argument(f"--{name}", required=True)
    parser.add_argument("--output", default=".pr-controller/decision.json")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--pending-wait-seconds", type=int, default=300)
    parser.add_argument("--poll-interval-seconds", type=int, default=20)
    parser.add_argument("--max-reruns", type=int, default=6)
    parser.add_argument("--safe-max-rounds", default="1")
    parser.add_argument("--safe-pending-wait-seconds", default="600")


def add_controller_clean_scope_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--clean-scope-rebuild", action="store_true")
    parser.add_argument(
        "--clean-scope-rebuild-mode",
        default="disabled",
        choices=["disabled", "detect", "execute", "auto"],
    )
    parser.add_argument("--clean-scope-commit-limit", type=int, default=3)
    parser.add_argument("--clean-scope-allowlist", default="")
    parser.add_argument("--clean-scope-forbidden", default="")


def normalize_controller_args(args: argparse.Namespace) -> None:
    if getattr(args, "clean_scope_rebuild_mode", None) != "auto":
        return
    args.clean_scope_rebuild_mode = "execute" if args.clean_scope_rebuild else "disabled"


def initial_decision(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "repo": args.repo,
        "pr": args.pr,
        "dry_run": args.dry_run,
        "actions": [],
        "warnings": [],
        "errors": [],
    }


def load_pr_or_record_error(
    args: argparse.Namespace,
    decision: dict[str, Any],
) -> dict[str, Any] | None:
    try:
        return pr_view(args.repo, args.pr)
    except (subprocess.CalledProcessError, ValueError, OSError, RuntimeError) as exc:
        decision["next_action"] = "error"
        decision["errors"].append(f"failed_to_load_pr: {exc}")
        return None


def update_decision_pr_metadata(
    decision: dict[str, Any],
    args: argparse.Namespace,
    pr: dict[str, Any],
) -> None:
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


def skip_action_for_pr(pr: dict[str, Any]) -> str | None:
    if pr.get("state") != "OPEN":
        return "skip_not_open"
    if pr.get("isDraft"):
        return "skip_draft"
    return None


def is_real_blocker(check: dict[str, Any]) -> bool:
    return is_failure(check) and not is_self_check(check)


def codacy_evidence_for_checks(
    repo: str,
    pr: str,
    checks: list[dict[str, Any]],
) -> dict[str, Any]:
    codacy_blockers = [
        compact_check(check)
        for check in checks
        if is_real_blocker(check) and is_codacy_check(check)
    ]
    return controller_codacy_blocking_evidence(repo, pr, codacy_blockers)


def ignored_codacy_checks(
    checks: list[dict[str, Any]],
    codacy: dict[str, Any],
) -> list[dict[str, Any]]:
    if not codacy.get("ignored"):
        return []
    return [
        compact_check(check)
        for check in checks
        if is_real_blocker(check) and is_codacy_check(check)
    ]


def filter_ignored_codacy_checks(
    checks: list[dict[str, Any]],
    codacy: dict[str, Any],
) -> list[dict[str, Any]]:
    if not codacy.get("ignored"):
        return checks
    return [check for check in checks if not is_codacy_check(check)]


def is_reportable_self_check(check: dict[str, Any]) -> bool:
    return is_self_check(check) and (
        is_failure(check) or is_cancelled(check) or is_pending(check)
    )


def set_check_buckets(
    decision: dict[str, Any],
    checks: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    blockers = [compact_check(check) for check in checks if is_real_blocker(check)]
    ignored_self = [compact_check(check) for check in checks if is_reportable_self_check(check)]
    decision["blockers"] = blockers
    decision["ignored_self_checks"] = ignored_self
    return blockers


def pending_wait_config(args: argparse.Namespace, started: float) -> PendingWaitConfig:
    return PendingWaitConfig(
        repo=args.repo,
        pr_number=args.pr,
        started=started,
        wait_seconds=args.pending_wait_seconds,
        poll_interval_seconds=args.poll_interval_seconds,
    )


def rerun_config(args: argparse.Namespace) -> RerunConfig:
    return RerunConfig(
        repo=args.repo,
        dry_run=args.dry_run,
        max_reruns=args.max_reruns,
    )


def collect_pr_context(
    args: argparse.Namespace,
    decision: dict[str, Any],
    started: float,
) -> tuple[dict[str, Any] | None, list[str], list[dict[str, Any]], bool]:
    pr = load_pr_or_record_error(args, decision)
    if pr is None:
        return None, [], [], True
    files = pr_files(args.repo, args.pr)
    commits = pr_commits(args.repo, args.pr)
    update_decision_pr_metadata(decision, args, pr)
    skip_action = skip_action_for_pr(pr)
    if skip_action:
        decision["next_action"] = skip_action
        return pr, files, commits, True
    pr, wait_exhausted = wait_for_pending_checks(pr, pending_wait_config(args, started), decision)
    return pr, files, commits, wait_exhausted


def finish_after_cancelled_checks(
    args: argparse.Namespace,
    decision: dict[str, Any],
    checks: list[dict[str, Any]],
) -> bool:
    return handle_cancelled_checks(checks, rerun_config(args), decision)


def build_next_action_context(
    args: argparse.Namespace,
    decision: dict[str, Any],
    pr: dict[str, Any],
    changed: tuple[list[str], list[dict[str, Any]]],
) -> NextActionContext:
    checks = pr.get("statusCheckRollup") or []
    codacy = codacy_evidence_for_checks(args.repo, args.pr, checks)
    effective_checks = filter_ignored_codacy_checks(checks, codacy)
    blockers = set_check_buckets(decision, effective_checks)
    decision["codacy"] = codacy
    decision["ignored_codacy_checks"] = ignored_codacy_checks(checks, codacy)
    files, commits = changed
    return NextActionContext(args, pr, effective_checks, files, commits, blockers, decision)


def run_controller(args: argparse.Namespace, decision: dict[str, Any]) -> int:
    started = time.time()
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    pr, files, commits, finished = collect_pr_context(args, decision, started)
    if finished or pr is None:
        return write_decision(args.output, decision)
    checks = pr.get("statusCheckRollup") or []
    if finish_after_cancelled_checks(args, decision, checks):
        return write_decision(args.output, decision)
    ctx = build_next_action_context(args, decision, pr, (files, commits))
    decide_next_action(ctx)
    return write_decision(args.output, decision)


def main() -> int:
    args = parse_controller_args()
    if getattr(args, "command", "") == "codacy-task":
        return cmd_codacy_task(args)
    return run_controller(args, initial_decision(args))


if __name__ == "__main__":
    raise SystemExit(main())
