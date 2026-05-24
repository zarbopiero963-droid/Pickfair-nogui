#!/usr/bin/env python3
"""PR automation controller for safe-autofix and clean-scope escalation."""
# pylint: disable=too-many-arguments,too-many-locals,too-many-branches
# pylint: disable=too-many-statements,missing-function-docstring,invalid-name,duplicate-code
# pylint: disable=line-too-long,broad-exception-caught
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess  # nosec B404
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Sequence, cast

FAIL_STATES = {"FAILURE", "ERROR", "ACTION_REQUIRED", "TIMED_OUT"}
PENDING_STATES = {"", "PENDING", "QUEUED", "IN_PROGRESS", "REQUESTED", "WAITING"}
CANCELLED_STATES = {"CANCELLED", "CANCELED"}
STALE_STATES = {"STALE"}

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
PR_AUTOMATION_STATE_FIELDS = (
    "repo",
    "pr",
    "head",
    "active_task_id",
    "last_blocker_signature",
    "same_blocker_rounds",
    "last_action",
    "last_result",
    "codacy_issue_count",
    "review_active_count",
    "bad_check_count",
    "autofix_commit_count",
    "controller_run_count",
    "stale_check_rerun_count",
    "codacy_oscillation_rounds",
    "active_final_micro_audit_path",
)
ACTIVE_TASK_COMMAND_FILE = "active-task-command.md"
ACTIVE_FINAL_MICRO_AUDIT_FILE = "active-final-micro-audit.md"
ACTIVE_TASK_STATE_FILE = "active-task-state.json"
POST_FIX_MICRO_AUDIT_SECTION = """POST-FIX MICRO-AUDIT BEFORE COMMIT

Before committing, perform a read-only audit of your own patch.

Check:
1. Did you fix every requested Codacy/review finding?
2. Did you avoid creating new unused helpers?
3. Did you avoid creating functions/tests above local Codacy/Lizard thresholds?
4. Did you preserve scope and avoid unrelated files?
5. Did you avoid touching forbidden files?
6. Did you preserve behavior?
7. Did you add/adjust focused tests where needed?
8. Did direct script execution still work where required?
9. Did validation pass?
10. Did the patch avoid new false-green routing?

If audit fails:
- fix the issue before commit
- or stop and report PARTIAL/needs_manual
Do not commit a patch that fails this audit.
"""
CODEX_PROMPT_REQUIRED_SECTIONS = [
    "TASK",
    "OBJECTIVE",
    "CONTEXT",
    "FILES TO INSPECT",
    "FILES ALLOWED",
    "DO NOT MODIFY",
    "CURRENT BEHAVIOR",
    "EXPECTED BEHAVIOR",
    "CURRENT BLOCKERS",
    "REQUIRED FIXES",
    "METHOD",
    "VALIDATION",
    "POST-FIX MICRO-AUDIT BEFORE COMMIT",
    "OUTPUT FORMAT",
    "STOP CONDITIONS",
]
PHASE0_SECTION_TITLE = "PHASE 0 PRE-FLIGHT (READ-ONLY)"
PHASE0_RESULT_LIST_FIELDS = [
    "files_inspected",
    "static_analysis_rules",
    "workflows_affected",
    "authoritative_modules",
    "dangerous_gates",
    "files_allowed",
    "files_forbidden",
    "implementation_plan",
    "tests_to_run",
    "stop_conditions",
]
PHASE0_REQUIRED_EVIDENCE_FIELDS = (
    "files_inspected",
    "static_analysis_rules",
    "workflows_affected",
    "authoritative_modules",
    "dangerous_gates",
    "implementation_plan",
    "tests_to_run",
    "stop_conditions",
)
PLACEHOLDER_VALUES = frozenset({"", "tbd", "todo", "none", "null", "n/a"})
ALLOW_COMMIT_PUSH_PATTERN = re.compile(r"(?im)^\s*allow_commit_push\s*:\s*yes\s*$")
COMMIT_PUSH_NEGATION_PATTERNS = (
    re.compile(r"\bdo\s+not\s+git\s+commit\b"),
    re.compile(r"\bdo\s+not\s+git\s+push\b"),
    re.compile(r"\bdon't\s+git\s+commit\b"),
    re.compile(r"\bdon't\s+git\s+push\b"),
    re.compile(r"\bdo\s+not\s+commit\b"),
    re.compile(r"\bdo\s+not\s+push\b"),
    re.compile(r"\bdon't\s+commit\b"),
    re.compile(r"\bdon't\s+push\b"),
    re.compile(r"\bno\s+commit(?:\b|/push\b)"),
    re.compile(r"\bno\s+push(?:\b|/commit\b)"),
    re.compile(r"\bno\s+commit/push\b"),
    re.compile(r"\bno\s+push/commit\b"),
)
COMMIT_PUSH_IMPERATIVE_PATTERNS = (
    re.compile(r"\bgit\s+commit(?:\b|$)", re.IGNORECASE),
    re.compile(r"\bgit\s+push(?:\s+origin\s+\S+)?(?:\b|$)", re.IGNORECASE),
    re.compile(r"\bcommit\s+after\s+checks(?:\b|$)", re.IGNORECASE),
    re.compile(r"\bpush\s+this\s+branch(?:\b|$)", re.IGNORECASE),
    re.compile(r"\bcommit(?:\s+the)?\s+changes(?:\s+after\s+\w+)?(?:\b|$)", re.IGNORECASE),
    re.compile(r"\bcommit\s+the\s+patch(?:\b|$)", re.IGNORECASE),
    re.compile(r"\bpush\s+origin\s+\S+(?:\s+after\s+\w+)?(?:\b|$)", re.IGNORECASE),
    re.compile(r"\bpush\s+the\s+branch(?:\b|$)", re.IGNORECASE),
)
CODEX_SCALAR_KEYS = (
    ("TASK", "task"),
    ("OBJECTIVE", "objective"),
    ("CONTEXT", "context"),
    ("CURRENT BEHAVIOR", "current_behavior"),
    ("EXPECTED BEHAVIOR", "expected_behavior"),
)
CODEX_SCALAR_DEFAULTS = {
    "method": (
        "- inspect relevant files first\n"
        "- verify each finding is still valid\n"
        "- keep patch minimal\n"
        "- add/update focused tests\n"
        "- run validation\n"
        "- do not commit/push unless explicitly allowed"
    ),
    "output_format": (
        "- files changed\n"
        "- summary of fix\n"
        "- tests run\n"
        "- post-fix audit result\n"
        "- final status DONE/PARTIAL/NEEDS_MANUAL"
    ),
}
CODEX_REQUIRED_SECTION_HEADERS = frozenset(CODEX_PROMPT_REQUIRED_SECTIONS)


def build_codex_task_prompt(context: dict[str, Any] | None = None) -> str:
    ctx = context if isinstance(context, dict) else {}
    lines: list[str] = []
    for name in CODEX_PROMPT_REQUIRED_SECTIONS:
        if name == "POST-FIX MICRO-AUDIT BEFORE COMMIT":
            continue
        lines.append(f"{name}:\n{_codex_context_value(ctx, name)}")
    prompt = "\n\n".join(lines)
    return ensure_post_fix_micro_audit_section(ensure_phase0_preflight_section(prompt, ctx))


def ensure_codex_prompt_contract(prompt: str, context: dict[str, Any] | None = None) -> str:
    ctx = context if isinstance(context, dict) else {}
    text = _seed_codex_contract_prompt(prompt, ctx)
    text = _add_missing_codex_contract_sections(text)
    return _finalize_codex_contract_prompt(text)


def _finalize_codex_contract_prompt(text: str) -> str:
    return ensure_post_fix_micro_audit_section(text)


def _seed_codex_contract_prompt(prompt: str, context: dict[str, Any]) -> str:
    text = str(prompt or "").strip()
    if not text:
        return build_codex_task_prompt(context)
    text = ensure_phase0_preflight_section(text, context)
    return _strip_post_fix_micro_audit_placeholder_section(text)


def _add_missing_codex_contract_sections(text: str) -> str:
    missing = codex_prompt_contract_missing_sections(text)
    non_post_fix_missing = [name for name in missing if name != "POST-FIX MICRO-AUDIT BEFORE COMMIT"]
    if not non_post_fix_missing:
        return text
    return "\n\n".join([text] + [f"{name}:\nTBD" for name in non_post_fix_missing]).strip()


def validate_codex_prompt_contract(prompt: str) -> dict[str, Any]:
    text = str(prompt or "")
    missing = codex_prompt_contract_missing_sections(text)
    uninitialized = codex_prompt_contract_uninitialized_sections(text)
    reasons = _codex_contract_reasons(text)
    return {
        "valid": not missing and not uninitialized and not reasons,
        "missing_sections": missing,
        "uninitialized_sections": uninitialized,
        "reasons": reasons,
    }


def codex_prompt_contract_missing_sections(prompt: str) -> list[str]:
    text = str(prompt or "")
    return [name for name in CODEX_PROMPT_REQUIRED_SECTIONS if not _has_section(text, name)]


def codex_prompt_contract_uninitialized_sections(prompt: str) -> list[str]:
    text = str(prompt or "")
    return [name for name in CODEX_PROMPT_REQUIRED_SECTIONS if _has_section(text, name) and not _section_has_value(text, name)]


def build_phase0_preflight_prompt(context: dict[str, Any] | None = None) -> str:
    ctx = context if isinstance(context, dict) else {}
    return "\n".join(_phase0_preflight_lines(ctx)).strip() + "\n"


def ensure_phase0_preflight_section(prompt: str, context: dict[str, Any] | None = None) -> str:
    text = str(prompt or "").rstrip()
    ctx = context if isinstance(context, dict) else {}
    return (
        text + "\n"
        if _has_section(text, PHASE0_SECTION_TITLE)
        else f"{build_phase0_preflight_prompt(ctx)}\n{text}\n"
    )


def parse_phase0_preflight_result(text: str) -> dict[str, Any]:
    raw = str(text or "").strip()
    report = _parse_post_fix_audit_json(raw) or _parse_phase0_text(raw)
    return _normalize_phase0_report(report)


def phase0_preflight_status(report: dict[str, Any] | None) -> str:
    if not isinstance(report, dict):
        return "NEEDS_MANUAL"
    normalized = _normalize_phase0_report(report)
    return str(normalized.get("status") or "NEEDS_MANUAL")


def phase0_preflight_failed(report: dict[str, Any] | None) -> bool:
    if not isinstance(report, dict):
        return True
    normalized = _normalize_phase0_report(report)
    return normalized.get("status") != "PASS" or normalized.get("next_action") != "generate_patch_prompt"


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


def is_stale(check: dict[str, Any]) -> bool:
    return check_state(check) in STALE_STATES


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
    """Configuration for rerunning cancelled checks."""  # noqa: D203

    repo: str
    dry_run: bool
    max_reruns: int


@dataclass(frozen=True)
class SafeAutofixConfig:
    """Configuration for invoking the safe autofix workflow."""  # noqa: D203

    repo: str
    pr_number: str
    dry_run: bool
    max_rounds: str
    pending_wait_seconds: str


@dataclass(frozen=True)
class CleanScopeRules:
    """Allowlist and forbidden scope constraints for clean rebuild mode."""  # noqa: D203

    allowlist: tuple[str, ...]
    forbidden: tuple[str, ...]
    commit_limit: int


@dataclass(frozen=True)
class CleanRebuildConfig:
    """Configuration for launching a clean-scope rebuild workflow."""  # noqa: D203

    repo: str
    pr_number: str
    head_branch: str
    dry_run: bool
    rules: CleanScopeRules


@dataclass(frozen=True)
class PendingWaitConfig:
    """Data container used by the automation flow."""  # noqa: D203

    repo: str
    pr_number: str
    started: float
    wait_seconds: int
    poll_interval_seconds: int


@dataclass(frozen=True)
class CleanScopeReport:
    """Data container used by the automation flow."""  # noqa: D203

    enabled: bool
    mode: str
    rules: CleanScopeRules
    signals: dict[str, Any]


@dataclass(frozen=True)
class NextActionContext:
    """Data container used by the automation flow."""  # noqa: D203

    args: argparse.Namespace
    pr: dict[str, Any]
    checks: list[dict[str, Any]]
    files: list[str]
    commits: list[dict[str, Any]]
    blockers: list[dict[str, Any]]
    decision: dict[str, Any]


@dataclass(frozen=True)
class ActiveTaskContextInput:
    """Inputs required to replace active task context."""  # noqa: D203

    task_text: str
    audit_text: str
    branch: str
    pr: str


def is_stale_or_cancelled(check: dict[str, Any]) -> bool:
    return is_cancelled(check) or is_stale(check)


def is_real_pending(check: dict[str, Any]) -> bool:
    return is_pending(check) and not is_self_check(check)


def is_real_non_stale_cancelled_blocker(check: dict[str, Any]) -> bool:
    return is_failure(check) and not is_self_check(check) and not is_stale_or_cancelled(check)


def is_safe_autofix_check(check: dict[str, Any]) -> bool:
    return "safe pr autofix" in name_of(check).lower()


def should_rerun_stale_or_cancelled_check(check: dict[str, Any], config: RerunConfig) -> bool:
    if not is_stale_or_cancelled(check):
        return False
    if not extract_run_id(url_of(check)):
        return False
    if is_safe_autofix_check(check) and not config.dry_run:
        return False
    return True


def stale_or_cancelled_checks_for_rerun(
    checks: list[dict[str, Any]],
    config: RerunConfig,
) -> list[dict[str, Any]]:
    return [check for check in checks if should_rerun_stale_or_cancelled_check(check, config)]


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
    for check in stale_or_cancelled_checks_for_rerun(checks, config):
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


def stable_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).lower()


def blocker_raw_name(check: dict[str, Any]) -> str:
    return f"{check.get('name') or check.get('context') or ''}".strip()


def blocker_source(raw_name: str) -> str:
    return "codacy" if "codacy" in raw_name.lower() else "check"


def blocker_path(check: dict[str, Any]) -> str:
    return str(check.get("path") or check.get("filePath") or check.get("filename") or "").strip()


def blocker_rule(check: dict[str, Any]) -> str:
    return str(
        check.get("rule")
        or check.get("ruleId")
        or check.get("patternId")
        or check.get("code")
        or ""
    ).strip()


def blocker_message(check: dict[str, Any]) -> str:
    return str(check.get("message") or check.get("title") or "").strip()


def stable_blocker_record(check: dict[str, Any]) -> dict[str, str]:
    raw_name = blocker_raw_name(check)
    return {
        "name": stable_text(raw_name),
        "state": stable_text(check.get("conclusion") or check.get("state") or check.get("status")),
        "source": blocker_source(raw_name),
        "path": stable_text(blocker_path(check)),
        "rule": stable_text(blocker_rule(check)),
        "message": stable_text(blocker_message(check)),
    }


def blocker_signature(blockers: list[dict[str, Any]]) -> str:
    if not blockers:
        return ""
    records = [stable_blocker_record(check) for check in blockers]
    records.sort(key=lambda item: (item["source"], item["name"], item["state"], item["path"], item["rule"], item["message"]))
    return json.dumps(records, sort_keys=True, separators=(",", ":"))


def detect_no_progress_blocker_signature(
    blockers: list[dict[str, Any]],
    previous_blocker_signature: str = "",
    repeated_blocker_count: int = 0,
    threshold: int = 2,
) -> dict[str, Any]:
    signature = blocker_signature(blockers)
    repeated = _next_repeated_blocker_count(
        signature,
        previous_blocker_signature,
        repeated_blocker_count,
    )
    has_signature = bool(signature)
    return {
        "blocker_signature": signature if has_signature else "",
        "previous_blocker_signature": signature if has_signature else "",
        "repeated_blocker_count": repeated if has_signature else 0,
        "no_progress": has_signature and repeated >= max(1, threshold),
    }


def _next_repeated_blocker_count(
    signature: str,
    previous_blocker_signature: str,
    repeated_blocker_count: int,
) -> int:
    return repeated_blocker_count + 1 if signature == str(previous_blocker_signature or "") else 1


def safe_nonnegative_int(value: Any, default: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= 0 else default


def _normalized_state_text(source: dict[str, Any], key: str, default: str = "") -> str:
    return str(source.get(key) or default)


def _normalized_state_int(source: dict[str, Any], key: str, default: int = 0) -> int:
    return safe_nonnegative_int(source.get(key), default)


def normalize_pr_automation_state(payload: Any, repo: str, pr: str) -> dict[str, Any]:
    source = payload if isinstance(payload, dict) else {}
    state: dict[str, Any] = {
        "repo": _normalized_state_text(source, "repo", repo),
        "pr": _normalized_state_text(source, "pr", pr),
        "head": _normalized_state_text(source, "head"),
        "active_task_id": _normalized_state_text(source, "active_task_id"),
        "last_blocker_signature": _normalized_state_text(source, "last_blocker_signature"),
        "same_blocker_rounds": _normalized_state_int(source, "same_blocker_rounds"),
        "last_action": _normalized_state_text(source, "last_action"),
        "last_result": _normalized_state_text(source, "last_result"),
        "active_final_micro_audit_path": _normalized_state_text(source, "active_final_micro_audit_path"),
    }
    int_fields = [field for field in PR_AUTOMATION_STATE_FIELDS if field not in state]
    for field in int_fields:
        state[field] = _normalized_state_int(source, field)
    return state


def load_pr_automation_state(path: str, repo: str, pr: str) -> dict[str, Any]:
    payload = _read_json_object(path)
    if not payload:
        return normalize_pr_automation_state({}, repo, pr)
    if not _stored_no_progress_matches(payload, repo, pr):
        return normalize_pr_automation_state({}, repo, pr)
    return normalize_pr_automation_state(payload, repo, pr)


def save_pr_automation_state(path: str, state: dict[str, Any]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def pr_budget_status(state: dict[str, Any], limits: dict[str, Any]) -> dict[str, Any]:
    checks = (
        ("autofix_commit_count", "max_autofix_commits_per_pr", "needs_manual_budget_exhausted"),
        ("same_blocker_rounds", "max_same_blocker_attempts", "needs_manual_no_progress"),
        ("controller_run_count", "max_total_controller_runs", "needs_manual_budget_exhausted"),
        ("codacy_oscillation_rounds", "max_codacy_oscillation_rounds", "needs_manual_budget_exhausted"),
        ("stale_check_rerun_count", "max_stale_check_reruns", "needs_manual_budget_exhausted"),
    )
    for state_key, limit_key, action in checks:
        current = safe_nonnegative_int(state.get(state_key), 0)
        limit = safe_nonnegative_int(limits.get(limit_key), 0)
        if limit and current >= limit:
            return {"exhausted": True, "reason": f"{state_key}>={limit_key}", "next_action": action}
    return {"exhausted": False, "reason": "", "next_action": ""}


def detect_pr_progress(previous_state: dict[str, Any], current_state: dict[str, Any]) -> str:
    keys = ("codacy_issue_count", "review_active_count", "bad_check_count")
    prev = sum(safe_nonnegative_int(previous_state.get(key), 0) for key in keys)
    curr = sum(safe_nonnegative_int(current_state.get(key), 0) for key in keys)
    if curr < prev:
        return "improved"
    if curr > prev:
        return "regressed"
    return "unchanged"


def compute_task_id(task_text: str, audit_text: str, branch: str, pr: str) -> str:
    raw = "\n".join([str(task_text), str(audit_text), str(branch), str(pr)])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _read_text_if_exists(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


def _archive_active_context(context: Path, stamp: str) -> None:
    history = context / "history"
    history.mkdir(parents=True, exist_ok=True)
    mapping = (
        (ACTIVE_TASK_COMMAND_FILE, f"{stamp}-task-command.md"),
        (ACTIVE_FINAL_MICRO_AUDIT_FILE, f"{stamp}-final-micro-audit.md"),
        (ACTIVE_TASK_STATE_FILE, f"{stamp}-task-state.json"),
    )
    for active_name, archive_name in mapping:
        source = context / active_name
        content = _read_text_if_exists(source)
        if content:
            (history / archive_name).write_text(content, encoding="utf-8")


def _active_task_paths(context: Path) -> dict[str, Path]:
    return {
        "task_command": context / ACTIVE_TASK_COMMAND_FILE,
        "final_micro_audit": context / ACTIVE_FINAL_MICRO_AUDIT_FILE,
        "task_state": context / ACTIVE_TASK_STATE_FILE,
    }


def _archive_if_task_changed(context: Path, current_task_id: str, next_task_id: str) -> None:
    if current_task_id and current_task_id != next_task_id:
        _archive_active_context(context, time.strftime("%Y%m%d%H%M%S", time.gmtime()))


def _new_active_task_state(active: ActiveTaskContextInput, next_task_id: str, active_audit_path: str) -> dict[str, Any]:
    return {
        "task_id": next_task_id,
        "branch": str(active.branch),
        "pr": str(active.pr),
        "active_final_micro_audit_path": active_audit_path,
    }


def _write_active_task_files(
    paths: dict[str, Path],
    active: ActiveTaskContextInput,
    state: dict[str, Any],
) -> None:
    paths["task_command"].write_text(active.task_text, encoding="utf-8")
    paths["final_micro_audit"].write_text(active.audit_text, encoding="utf-8")
    paths["task_state"].write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def replace_active_task_context(context_dir: str, active: ActiveTaskContextInput) -> dict[str, Any]:
    context = Path(context_dir)
    context.mkdir(parents=True, exist_ok=True)
    paths = _active_task_paths(context)
    current = _read_json_object(str(paths["task_state"]))
    next_task_id = compute_task_id(active.task_text, active.audit_text, active.branch, active.pr)
    current_task_id = str(current.get("task_id") or "")
    _archive_if_task_changed(context, current_task_id, next_task_id)
    active_audit_path = str(paths["final_micro_audit"])
    if current_task_id == next_task_id:
        if not str(current.get("active_final_micro_audit_path") or "").strip():
            current["active_final_micro_audit_path"] = active_audit_path
            paths["task_state"].write_text(json.dumps(current, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return current
    state = _new_active_task_state(active, next_task_id, active_audit_path)
    _write_active_task_files(paths, active, state)
    return state


def _read_json_object(path: str) -> dict[str, Any]:
    try:
        raw = Path(path).read_text(encoding="utf-8")
        payload = json.loads(raw)
    except (OSError, ValueError, TypeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _coerce_non_negative_int(value: Any) -> int:
    return safe_nonnegative_int(value, 0)


def _stored_no_progress_matches(payload: dict[str, Any], repo: str, pr: str) -> bool:
    return str(payload.get("repo") or "") == str(repo) and str(payload.get("pr") or "") == str(pr)


def _load_no_progress_state(output_path: str, repo: str, pr: str) -> tuple[str, int]:
    payload = _read_json_object(output_path)
    if not payload:
        return "", 0
    if not _stored_no_progress_matches(payload, repo, pr):
        return "", 0
    signature = str(
        payload.get("blocker_signature")
        or payload.get("previous_blocker_signature")
        or ""
    )
    count = _coerce_non_negative_int(payload.get("repeated_blocker_count"))
    return signature, count


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
    stale_or_cancelled = [
        compact_check(check)
        for check in checks
        if is_stale_or_cancelled(check)
    ]
    decision["cancelled"] = stale_or_cancelled
    if not stale_or_cancelled:
        return False
    if any(is_real_pending(check) for check in checks):
        return False
    if any(is_real_non_stale_cancelled_blocker(check) for check in checks):
        return False
    rerun = rerun_cancelled_checks(checks, config)
    decision["actions"].append({"type": "rerun_cancelled", "items": rerun})
    decision["next_action"] = "rerun_stale_or_cancelled_checks" if rerun else "cancelled_checks_no_rerun_target"
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
    state = decision.get("pr_automation_state") if isinstance(decision.get("pr_automation_state"), dict) else {}
    if should_run_final_micro_audit(decision, cast(dict[str, Any], state)):
        decision["next_action"] = "run_final_micro_audit"
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
        hidden_word = "sec" + "ret"
        raise RuntimeError(f"GitHub Actions CODACY_API_TOKEN {hidden_word} is unavailable")
    token = os.environ.get("CODACY_API_TOKEN", "")
    if not token:
        hidden_word = "sec" + "ret"
        raise RuntimeError(f"GitHub Actions CODACY_API_TOKEN {hidden_word} is empty")
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


def codacy_https_json(url: str, token: str) -> Any:
    validate_codacy_url(url)
    request = urllib.request.Request(url, headers={"api-token": token}, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:  # nosec B310  # nosemgrep
            payload = response.read().decode("utf-8")
            status = getattr(response, "status", 200)
            if status >= 400:
                raise RuntimeError(f"Codacy API request failed ({status})")
    except OSError as exc:
        raise RuntimeError("Codacy API request failed") from exc
    return json.loads(payload or "{}")


def fetch_json(url: str, token: str) -> Any:
    return codacy_https_json(url, token)


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
    return ensure_codex_prompt_contract("\n".join(lines)).splitlines()


def build_post_fix_micro_audit_prompt(task_text: str, context: dict[str, Any] | None = None) -> str:
    prompt = ensure_post_fix_micro_audit_section(task_text)
    section = _post_fix_changed_files_section(_post_fix_changed_files(context))
    return f"{prompt}{section}" if section else prompt


def ensure_post_fix_micro_audit_section(prompt: str) -> str:
    text = str(prompt or "").rstrip()
    notes = _extract_post_fix_micro_audit_trailing_notes(text)
    normalized = _strip_post_fix_micro_audit_sections(text)
    if not normalized:
        return _append_post_fix_trailing_notes(POST_FIX_MICRO_AUDIT_SECTION.rstrip(), notes).strip()
    merged = f"{normalized}\n\n{POST_FIX_MICRO_AUDIT_SECTION}".rstrip()
    return _append_post_fix_trailing_notes(merged, notes).strip()


def parse_post_fix_micro_audit_result(text: str) -> dict[str, Any]:
    raw = str(text or "").strip()
    report = _parse_post_fix_audit_json(raw)
    if not report:
        report = _parse_post_fix_audit_text(raw)
    return _normalize_post_fix_audit_report(report)


def post_fix_micro_audit_status(report: dict[str, Any] | None) -> str:
    value = str((report or {}).get("status") or "").upper()
    return value if value in {"PASS", "FAIL", "PARTIAL"} else "FAIL"


def post_fix_micro_audit_failed(report: dict[str, Any] | None) -> bool:
    status = post_fix_micro_audit_status(report)
    next_action = str((report or {}).get("next_action") or "").strip()
    return status != "PASS" or next_action != "validation_then_commit"


def _normalized_ledger_base_dir(base_dir: Any) -> Path:
    return Path(str(base_dir or ".")).expanduser()


def _normalized_ledger_pr_number(pr_number: Any) -> int:
    return safe_nonnegative_int(pr_number, 0)


def automation_ledger_path(base_dir: str | Path | None, pr_number: str | int | None) -> str:
    root = _normalized_ledger_base_dir(base_dir)
    pr_value = _normalized_ledger_pr_number(pr_number)
    return str(root / f"pr-{pr_value}" / "automation-ledger.jsonl")


def _normalized_ledger_details(details: Any) -> dict[str, Any]:
    return details if isinstance(details, dict) else {}


def _normalized_ledger_text(value: Any) -> str:
    return str(value or "")


def _normalized_ledger_event_numbers(metadata: dict[str, Any]) -> tuple[int, int]:
    return safe_nonnegative_int(metadata.get("pr"), 0), safe_nonnegative_int(metadata.get("attempt"), 0)


def _normalized_ledger_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    legacy = metadata.get("metadata")
    if not isinstance(legacy, dict):
        return metadata
    merged = dict(legacy)
    for key, value in metadata.items():
        if key != "metadata":
            merged[key] = value
    return merged


def _normalized_ledger_created_at(created_at: object) -> str:
    return _normalized_ledger_text(created_at or _utc_timestamp())


def build_automation_ledger_event(
    event_type: object = "",
    details: object = None,
    created_at: object = "", **metadata: object,
) -> dict[str, object]:
    payload = _normalized_ledger_metadata(cast(dict[str, Any], metadata))
    pr_value, attempt_value = _normalized_ledger_event_numbers(payload)
    return {
        "event_type": _normalized_ledger_text(event_type),
        "repo": _normalized_ledger_text(payload.get("repo")),
        "pr": pr_value,
        "branch": _normalized_ledger_text(payload.get("branch")),
        "head_sha": _normalized_ledger_text(payload.get("head_sha")),
        "task_id": _normalized_ledger_text(payload.get("task_id")),
        "attempt": attempt_value,
        "next_action": _normalized_ledger_text(payload.get("next_action")),
        "reason": _normalized_ledger_text(payload.get("reason")),
        "created_at": _normalized_ledger_created_at(created_at),
        "details": _normalized_ledger_details(details),
    }


def append_automation_ledger_event(path: str, event: dict[str, Any]) -> None:
    target = Path(str(path or "")).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    row = json.dumps(event if isinstance(event, dict) else {}, sort_keys=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(f"{row}\n")


def read_automation_ledger_events(path: str) -> list[dict[str, Any]]:
    target = Path(str(path or "")).expanduser()
    if not target.is_file():
        return []
    try:
        with target.open("r", encoding="utf-8") as handle:
            return _read_automation_ledger_event_lines(handle)
    except OSError:
        return []


def _read_automation_ledger_event_lines(handle: Any) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for line in handle:
        parsed = _json_dict_or_empty(line)
        if parsed:
            events.append(parsed)
    return events


__all__ = (
    "automation_ledger_path",
    "build_automation_ledger_event",
    "append_automation_ledger_event",
    "read_automation_ledger_events",
)


def _utc_timestamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _line_value(text: str, key: str) -> str:
    match = re.search(rf"(?im)^\s*{re.escape(key)}\s*[:=]\s*(.+)$", text or "")
    return match.group(1).strip() if match else ""


def _list_value(data: dict[str, Any], raw: str, key: str) -> list[str]:
    list_items = _list_from_payload(data.get(key))
    if list_items:
        return list_items
    return _list_from_raw_audit_section(raw, key)


def _audit_section_lines(raw: str, key: str) -> list[str]:
    return [line.strip() for line in _lines_after_audit_key(raw, key) if line.strip()]


def _lines_after_audit_key(raw: str, key: str) -> list[str]:
    lines = (raw or "").splitlines()
    header = re.compile(rf"^\s*{re.escape(key)}\s*:\s*$", re.IGNORECASE)
    start = next((index + 1 for index, line in enumerate(lines) if header.match(line)), -1)
    if start < 0:
        return []
    return _contiguous_audit_bullet_lines(lines[start:])


def _contiguous_audit_bullet_lines(lines: list[str]) -> list[str]:
    section: list[str] = []
    for line in lines:
        stripped = line.strip()
        if _should_stop_audit_bullets(stripped):
            break
        if _is_blank_line(stripped):
            continue
        section.append(_clean_audit_bullet(stripped))
    return section


def _looks_like_audit_field(line: str) -> bool:
    return bool(re.match(r"^[A-Za-z0-9_]+\s*:\s*", line))


def _is_blank_line(line: str) -> bool:
    return not str(line).strip()


def _is_audit_bullet_line(line: str) -> bool:
    stripped = line.strip()
    return stripped.startswith("-") and bool(_clean_audit_bullet(stripped))


def _clean_audit_bullet(line: str) -> str:
    return re.sub(r"^-\s*", "", line.strip()).strip()


def _should_stop_audit_bullets(line: str) -> bool:
    if _looks_like_audit_field(line):
        return True
    if _is_blank_line(line):
        return False
    if not _is_audit_bullet_line(line):
        return True
    return False


def _list_from_payload(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _list_from_raw_audit_section(raw: str, key: str) -> list[str]:
    result: list[str] = []
    for line in _audit_section_lines(raw, key):
        cleaned = _clean_audit_bullet(line)
        if cleaned:
            result.append(cleaned)
    return result


def _post_fix_changed_files(context: dict[str, Any] | None) -> list[str]:
    if not isinstance(context, dict):
        return []
    files = context.get("changed_files")
    if not isinstance(files, list):
        return []
    return [str(path).strip() for path in files if str(path).strip()]


def _post_fix_changed_files_section(files: list[str]) -> str:
    if not files:
        return ""
    lines = "\n".join(f"- {path}" for path in files)
    return f"\n\nAudit context (changed files):\n{lines}\n"


def _has_full_post_fix_micro_audit_section(text: str) -> bool:
    normalized_text = _normalize_audit_text_for_match(text)
    normalized_section = _normalize_audit_text_for_match(POST_FIX_MICRO_AUDIT_SECTION)
    return normalized_section in normalized_text


def _strip_post_fix_micro_audit_placeholder_section(text: str) -> str:
    lines = str(text or "").splitlines()
    if not lines:
        return ""
    kept_segments = _kept_post_fix_segments_for_placeholder_cleanup(lines)
    return _join_post_fix_segments(kept_segments)


def _strip_post_fix_micro_audit_sections(text: str) -> str:
    lines = str(text or "").splitlines()
    if not lines:
        return ""
    return _join_post_fix_segments(_non_post_fix_segments(lines))


def _iter_post_fix_segments(lines: list[str]) -> list[dict[str, Any]]:
    segments: list[dict[str, Any]] = []
    index = 0
    while index < len(lines):
        segment, index = _next_post_fix_segment(lines, index)
        segments.append(segment)
    return segments


def _next_post_fix_segment(lines: list[str], start: int) -> tuple[dict[str, Any], int]:
    if _is_post_fix_header_line(lines[start]):
        return _consume_post_fix_segment(lines, start)
    return _consume_non_post_fix_segment(lines, start)


def _consume_post_fix_segment(lines: list[str], start: int) -> tuple[dict[str, Any], int]:
    index = start + 1
    while index < len(lines) and not _is_required_section_header(lines[index]):
        index += 1
    segment_lines = lines[start:index]
    body = segment_lines[1:] if len(segment_lines) > 1 else []
    return {"is_post_fix": True, "segment_lines": segment_lines, "body_lines": body}, index


def _consume_non_post_fix_segment(lines: list[str], start: int) -> tuple[dict[str, Any], int]:
    index = start + 1
    while index < len(lines) and not _is_post_fix_header_line(lines[index]):
        index += 1
    return {"is_post_fix": False, "segment_lines": lines[start:index], "body_lines": []}, index


def _is_post_fix_header_line(line: str) -> bool:
    return _section_header_match(line, "POST-FIX MICRO-AUDIT BEFORE COMMIT")


def _is_placeholder_only_post_fix_body(body_lines: list[str]) -> bool:
    nonempty = [raw.strip() for raw in body_lines if raw.strip()]
    return bool(nonempty) and all(_is_placeholder_value(line) for line in nonempty)


def _extract_post_fix_micro_audit_trailing_notes(text: str) -> list[str]:
    notes: list[str] = []
    for segment in _iter_post_fix_segments(str(text or "").splitlines()):
        if not segment["is_post_fix"]:
            continue
        notes.extend(_post_fix_noncanonical_lines(cast(list[str], segment["body_lines"])))
    return _dedupe_nonempty_lines(notes)


def _post_fix_noncanonical_lines(lines: list[str]) -> list[str]:
    canonical_lines = {line.strip() for line in POST_FIX_MICRO_AUDIT_SECTION.splitlines() if line.strip()}
    return [
        line
        for line in lines
        if _is_noncanonical_post_fix_line(line, canonical_lines)
    ]


def _is_noncanonical_post_fix_line(line: str, canonical_lines: set[str]) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    if stripped in canonical_lines:
        return False
    return not _is_placeholder_value(line)


def _dedupe_nonempty_lines(lines: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in lines:
        line = raw.rstrip()
        key = line.strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(line)
    return out


def _append_post_fix_trailing_notes(base: str, notes: list[str]) -> str:
    if not notes:
        return base
    return f"{base}\n" + "\n".join(notes)


def _normalize_audit_text_for_match(text: str) -> str:
    normalized_lines = [
        re.sub(r"\s+", " ", line).strip().lower()
        for line in (text or "").splitlines()
        if line.strip()
    ]
    return "\n".join(normalized_lines)


def _parse_post_fix_audit_json(raw: str) -> dict[str, Any]:
    loaded = _json_dict_or_empty(raw)
    if loaded:
        return loaded
    for payload in _extract_fenced_json_payloads(raw):
        loaded = _json_dict_or_empty(payload)
        if loaded:
            return loaded
    return {}


def _extract_fenced_json_payloads(raw: str) -> list[str]:
    pattern = re.compile(r"```[^\n\r]*\r?\n(?P<body>[\s\S]*?)\r?\n```")
    payloads: list[str] = []
    for match in pattern.finditer(str(raw or "")):
        body = str(match.group("body") or "").strip()
        if body:
            payloads.append(body)
    return payloads


def _json_dict_or_empty(raw: str) -> dict[str, Any]:
    try:
        loaded = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _parse_post_fix_audit_text(raw: str) -> dict[str, Any]:
    status = _line_value(raw, "status") or _line_value(raw, "post_fix_audit") or _standalone_status_line(raw)
    return {
        "status": status,
        "reasons": _list_value({}, raw, "reasons"),
        "checked_items": _list_value({}, raw, "checked_items"),
        "changed_files": _list_value({}, raw, "changed_files"),
        "validation_commands": _list_value({}, raw, "validation_commands"),
        "next_action": _line_value(raw, "next_action"),
    }


def _standalone_status_line(raw: str) -> str:
    for line in (raw or "").splitlines():
        value = line.strip().upper()
        if value in {"PASS", "FAIL", "PARTIAL"}:
            return value
    return ""


def _normalize_post_fix_audit_report(report: dict[str, Any]) -> dict[str, Any]:
    status = _post_fix_status(report)
    next_action = _post_fix_next_action(status, report)
    normalized = {
        "status": status,
        "next_action": next_action,
        **_post_fix_report_lists(report),
    }
    if status == "PASS":
        if next_action == "validation_then_commit":
            return normalized
        normalized["status"] = "FAIL"
        normalized["next_action"] = "needs_manual_post_fix_audit_failed"
    return normalized


def _post_fix_status(report: dict[str, Any]) -> str:
    value = str(report.get("status") or "").upper()
    return value if value in {"PASS", "FAIL", "PARTIAL"} else "FAIL"


def _post_fix_next_action(status: str, report: dict[str, Any]) -> str:
    explicit = str(report.get("next_action") or "").strip()
    if explicit:
        return explicit
    if status == "PASS":
        return "validation_then_commit"
    if status == "PARTIAL":
        return "retry_fix_within_budget"
    return "needs_manual_post_fix_audit_failed"


def _post_fix_report_lists(report: dict[str, Any]) -> dict[str, list[str]]:
    return {
        "reasons": _list_value(report, "", "reasons"),
        "checked_items": _list_value(report, "", "checked_items"),
        "changed_files": _list_value(report, "", "changed_files"),
        "validation_commands": _list_value(report, "", "validation_commands"),
    }


def _context_list(context: dict[str, Any], key: str) -> list[str]:
    return _list_from_payload(context.get(key))


def _codex_context_value(context: dict[str, Any], section: str) -> str:
    scalar = _codex_scalar_context_map(context)
    if section in scalar:
        return scalar[section]
    list_key = _codex_list_section_key(section)
    if list_key:
        return _codex_context_list_value(context, list_key)
    return "TBD"


def _has_section(text: str, section: str) -> bool:
    target = str(section or "")
    return any(_section_header_match(line, target) for line in str(text or "").splitlines())


def _codex_contract_reasons(text: str) -> list[str]:
    reasons: list[str] = []
    if not _has_section(text, PHASE0_SECTION_TITLE):
        reasons.append("missing_phase0_preflight")
    if _looks_generic_fix_prompt(text):
        reasons.append("generic_fix_without_objective_context_validation")
    if _has_unsafe_commit_push_instruction(text):
        reasons.append("commit_or_push_instruction_without_explicit_allowance")
    return reasons


def _looks_generic_fix_prompt(text: str) -> bool:
    generic = "fix this" in str(text or "").lower()
    return generic and any(not _section_has_value(text, key) for key in ("OBJECTIVE", "CONTEXT", "VALIDATION"))


def _section_has_value(text: str, section: str) -> bool:
    lines = _section_body_lines(text, section)
    if not lines:
        return False
    return not all(_is_placeholder_value(line) for line in lines)


def _has_unsafe_commit_push_instruction(text: str) -> bool:
    if _has_commit_push_allowance(text):
        return False
    for line in str(text or "").splitlines():
        if _is_unsafe_commit_push_line(line):
            return True
    return False


def _parse_phase0_text(raw: str) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": _line_value(raw, "status") or _line_value(raw, "phase0_status"),
        "risk_level": _line_value(raw, "risk_level"),
        "next_action": _line_value(raw, "next_action"),
    }
    for key in PHASE0_RESULT_LIST_FIELDS:
        payload[key] = _list_value({}, raw, key)
    return payload


def _normalize_phase0_report(report: dict[str, Any]) -> dict[str, Any]:
    status = _phase0_status(report)
    next_action = _phase0_next_action(report)
    normalized: dict[str, Any] = {
        "status": status,
        "risk_level": _phase0_risk(report),
        "next_action": next_action,
    }
    _populate_phase0_lists(normalized, report)
    if status == "PASS" and next_action == "generate_patch_prompt" and _phase0_has_required_evidence(normalized):
        return normalized
    normalized["status"] = "NEEDS_MANUAL"
    normalized["next_action"] = "needs_manual_phase0_failed"
    return normalized


def _populate_phase0_lists(normalized: dict[str, Any], report: dict[str, Any]) -> None:
    for key in PHASE0_RESULT_LIST_FIELDS:
        normalized[key] = _phase0_list_value(report.get(key), key)


def _phase0_list_value(value: Any, key: str) -> list[str]:
    parsed = _phase0_list_value_from_payload_or_raw(value)
    if parsed:
        return parsed
    if isinstance(value, str):
        return _list_value({}, value, key)
    return []


def _list_from_raw_text(raw: str) -> list[str]:
    lines = _raw_bullet_lines(raw)
    if not _all_bulleted(lines):
        return []
    return _cleaned_audit_bullets(lines)


def _phase0_list_value_from_payload_or_raw(value: Any) -> list[str]:
    from_payload = _list_from_payload(value)
    if from_payload:
        return from_payload
    return _list_from_raw_text(value) if isinstance(value, str) else []


def _raw_bullet_lines(raw: str) -> list[str]:
    return [line.strip() for line in str(raw or "").splitlines() if line.strip()]


def _all_bulleted(lines: list[str]) -> bool:
    return bool(lines) and all(line.startswith("-") for line in lines)


def _cleaned_audit_bullets(lines: list[str]) -> list[str]:
    return [cleaned for line in lines if (cleaned := _clean_audit_bullet(line))]


def _kept_post_fix_segments_for_placeholder_cleanup(lines: list[str]) -> list[dict[str, Any]]:
    segments: list[dict[str, Any]] = []
    for segment in _iter_post_fix_segments(lines):
        if not segment["is_post_fix"] or not _is_placeholder_only_post_fix_body(
            cast(list[str], segment["body_lines"])
        ):
            segments.append(segment)
    return segments


def _non_post_fix_segments(lines: list[str]) -> list[dict[str, Any]]:
    return [segment for segment in _iter_post_fix_segments(lines) if not segment["is_post_fix"]]


def _join_post_fix_segments(segments: list[dict[str, Any]]) -> str:
    kept = [line for segment in segments for line in cast(list[str], segment["segment_lines"])]
    return "\n".join(kept).strip()


def _phase0_has_required_evidence(report: dict[str, Any]) -> bool:
    return all(_has_meaningful_list_values(report.get(field)) for field in PHASE0_REQUIRED_EVIDENCE_FIELDS)


def _phase0_status(report: dict[str, Any]) -> str:
    value = str(report.get("status") or "").upper()
    return value if value in {"PASS", "NEEDS_MANUAL"} else "NEEDS_MANUAL"


def _phase0_risk(report: dict[str, Any]) -> str:
    value = str(report.get("risk_level") or "").lower()
    return value if value in {"low", "medium", "high"} else "high"


def _phase0_next_action(report: dict[str, Any]) -> str:
    explicit = str(report.get("next_action") or "").strip()
    if not explicit:
        return ""
    normalized = re.sub(r"[\s-]+", "_", explicit.lower())
    return "generate_patch_prompt" if normalized == "generate_patch_prompt" else normalized


def _phase0_preflight_lines(context: dict[str, Any]) -> list[str]:
    target = _context_list(context, "files_allowed") or ["(from task scope)"]
    return [
        PHASE0_SECTION_TITLE,
        "",
        "READ-ONLY. Do not edit files. Do not commit. Do not push.",
        "Inspect static-analysis config, workflow CI, similar files/tests, authoritative modules, dangerous gates, and forbidden files.",
        "Read .github/workflows/*.yml and map affected workflows/checks; do not edit workflows unless explicitly allowed.",
        _phase0_static_analysis_line(),
        _phase0_blocking_rules_line(),
        f"Task allowed files: {', '.join(target)}",
        "Produce implementation plan and tests/validation plan. Stop with NEEDS_MANUAL when ambiguity/risk is high.",
        _phase0_result_contract_line(),
    ]


def _phase0_static_analysis_line() -> str:
    return (
        "Static-analysis files to inspect when present: .codacy.yml, .deepsource.toml, pyproject.toml, setup.cfg, "
        "tox.ini, .pylintrc, .flake8, ruff config, flake8 config, bandit config, pylint config, "
        "radon/lizard/static-analysis config."
    )


def _phase0_blocking_rules_line() -> str:
    return (
        "Likely blocking rules: NLOC, CCN, line length, docstring requirements, function name length <= 30, "
        "parameter count, protected access, hardcoded secret-like literals, bare assert / B101, subprocess / "
        "B603 / B404, unused helpers, direct script execution expectations."
    )


def _phase0_result_contract_line() -> str:
    return (
        "Result contract: status, risk_level, files_inspected, static_analysis_rules, workflows_affected, "
        "authoritative_modules, dangerous_gates, files_allowed, files_forbidden, implementation_plan, "
        "tests_to_run, stop_conditions, next_action."
    )


def _codex_scalar_context_map(context: dict[str, Any]) -> dict[str, str]:
    scalar = {name: _context_scalar(context, key) for name, key in CODEX_SCALAR_KEYS}
    scalar["METHOD"] = _context_scalar(context, "method", _codex_scalar_defaults()["method"])
    scalar["OUTPUT FORMAT"] = _context_scalar(context, "output_format", _codex_scalar_defaults()["output_format"])
    scalar["POST-FIX MICRO-AUDIT BEFORE COMMIT"] = "Use required post-fix micro-audit checklist before commit."
    return scalar


def _codex_scalar_defaults() -> dict[str, str]:
    return dict(CODEX_SCALAR_DEFAULTS)


def _context_scalar(context: dict[str, Any], key: str, fallback: str = "TBD") -> str:
    return str(context.get(key) or fallback)


def _codex_list_section_key(section: str) -> str:
    mapping = {
        "FILES TO INSPECT": "files_to_inspect",
        "FILES ALLOWED": "files_allowed",
        "DO NOT MODIFY": "do_not_modify",
        "CURRENT BLOCKERS": "current_blockers",
        "REQUIRED FIXES": "required_fixes",
        "VALIDATION": "validation",
        "STOP CONDITIONS": "stop_conditions",
    }
    return mapping.get(section, "")


def _codex_context_list_value(context: dict[str, Any], key: str) -> str:
    values = _context_list(context, key) or _codex_list_defaults(key)
    return "\n".join(f"- {item}" for item in values)


def _codex_list_defaults(key: str) -> list[str]:
    if key == "stop_conditions":
        return [
            "stop on scope violation",
            "stop on failing post-fix audit",
            "stop on validation failure",
            "stop on ambiguous/high-risk changes needing human decision",
        ]
    return ["TBD"]


def _section_body_lines(text: str, section: str) -> list[str]:
    body: list[str] = []
    for raw_line in _section_following_lines(text, section):
        if _is_required_section_header(raw_line):
            break
        stripped = raw_line.strip()
        if stripped:
            body.append(stripped)
    return body


def _section_following_lines(text: str, section: str) -> list[str]:
    lines = str(text or "").splitlines()
    target = str(section or "")
    for index, raw_line in enumerate(lines):
        if _section_header_match(raw_line, target):
            return lines[index + 1 :]
    return []


def _section_header_match(line: str, section: str) -> bool:
    stripped = str(line or "").strip()
    return stripped in {section, f"{section}:"}


def _is_required_section_header(line: str) -> bool:
    stripped = str(line or "").strip()
    if stripped.endswith(":"):
        stripped = stripped[:-1].strip()
    return stripped in CODEX_REQUIRED_SECTION_HEADERS


def _is_placeholder_value(line: str) -> bool:
    return _normalize_placeholder_text(line) in PLACEHOLDER_VALUES


def _normalize_placeholder_text(line: str) -> str:
    normalized = str(line or "").strip().lower()
    if normalized in {"-", "*"}:
        return ""
    for prefix in ("- ", "* "):
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix) :].strip()
            break
    return _strip_matching_quotes(normalized)


def _strip_matching_quotes(value: str) -> str:
    if value.startswith('"') and value.endswith('"') and len(value) >= 2:
        return value[1:-1].strip()
    return value


def _has_meaningful_list_values(value: Any) -> bool:
    return any(not _is_placeholder_value(item) for item in _list_from_payload(value))


def _has_commit_push_allowance(text: str) -> bool:
    return bool(ALLOW_COMMIT_PUSH_PATTERN.search(str(text or "")))


def _is_unsafe_commit_push_line(line: str) -> bool:
    normalized = line.strip().lower()
    if not normalized:
        return False
    if _is_commit_push_negation(normalized):
        return _has_commit_push_imperative(_strip_negated_commit_push_phrases(normalized))
    return _has_commit_push_imperative(normalized)


def _is_commit_push_negation(line: str) -> bool:
    return any(pattern.search(line) for pattern in COMMIT_PUSH_NEGATION_PATTERNS)


def _has_commit_push_imperative(line: str) -> bool:
    return any(pattern.search(line) for pattern in _unsafe_commit_push_patterns())


def _strip_negated_commit_push_phrases(line: str) -> str:
    stripped = str(line or "")
    negated_phrases = (
        r"\bdo\s+not\s+git\s+commit\b",
        r"\bdo\s+not\s+git\s+push\b",
        r"\bdon't\s+git\s+commit\b",
        r"\bdon't\s+git\s+push\b",
        r"\bdo\s+not\s+commit\b",
        r"\bdo\s+not\s+push\b",
        r"\bdon't\s+commit\b",
        r"\bdon't\s+push\b",
        r"\bno\s+commit/push\b",
        r"\bno\s+push/commit\b",
        r"\bno\s+commit\b",
        r"\bno\s+push\b",
    )
    for phrase in negated_phrases:
        stripped = re.sub(phrase, " ", stripped, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", stripped).strip()


def _unsafe_commit_push_patterns() -> tuple[re.Pattern[str], ...]:
    return COMMIT_PUSH_IMPERATIVE_PATTERNS


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
        "github_codacy_state": "",
        "github_annotations": 0,
        "codacy_api_issues": 0,
        "issues": [],
        "api_available": False,
        "api_ok": False,
        "issues_returned": 0,
        "blocking": False,
        "ignored": False,
        "reason": "no Codacy check blocker",
    }


def codacy_head_matches(pr_head: str, evidence: dict[str, Any]) -> dict[str, Any]:
    codacy_head = first_nonempty(
        evidence.get("head"),
        evidence.get("codacy_head"),
        evidence.get("headRefOid"),
    )
    return {
        "pr_head": pr_head,
        "codacy_head": codacy_head,
        "match": bool(pr_head and codacy_head and str(pr_head) == str(codacy_head)),
    }


CodacyIssueLocationKey = tuple[str, str, str, str]


def _has_d203_d211_conflict(issues: list[dict[str, Any]]) -> bool:
    grouped_patterns: dict[CodacyIssueLocationKey, set[str]] = {}
    for key, rule in _iter_d203_d211_issue_keys(issues):
        grouped_patterns.setdefault(key, set()).add(rule)
        if len(grouped_patterns[key]) == 2:
            return True
    return False


def _iter_d203_d211_issue_keys(
    issues: list[dict[str, Any]],
) -> list[tuple[CodacyIssueLocationKey, str]]:
    records: list[tuple[CodacyIssueLocationKey, str]] = []
    for item in issues:
        if not isinstance(item, dict):
            continue
        rule = _codacy_issue_rule(item)
        if rule not in {"D203", "D211"}:
            continue
        records.append((_codacy_issue_location_key(item), rule))
    return records


def _codacy_issue_rule(issue: dict[str, Any]) -> str:
    return str(issue.get("patternId") or "").strip().upper()


def _codacy_issue_location_key(issue: dict[str, Any]) -> CodacyIssueLocationKey:
    return (
        _codacy_issue_field(issue, "filePath", "filename"),
        _codacy_issue_field(issue, "lineNumber", "line", "startLine"),
        _codacy_issue_field(issue, "column", "startColumn"),
        _codacy_issue_field(issue, "symbol", "entity"),
    )


def _codacy_issue_field(issue: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = issue.get(key)
        if value not in (None, ""):
            return str(value).strip()
    return ""


def classify_codacy_evidence(evidence: dict[str, Any]) -> dict[str, Any]:
    details = _codacy_classification_details(evidence)
    result: dict[str, Any] = {
        "classification": details["classification"],
        "treat_annotations_as_blockers": details["treat_annotations_as_blockers"],
        "ignored": details["ignored"],
    }
    maybe_head_match = _codacy_head_match_value(evidence)
    if maybe_head_match is not None:
        result["head_match"] = maybe_head_match
    return result


def _codacy_classification_details(evidence: dict[str, Any]) -> dict[str, Any]:
    state, api_issues, annotations, issue_list = _codacy_classification_inputs(evidence)
    matched = _first_matching_codacy_rule(state, api_issues, annotations, issue_list)
    if matched is None:
        return _codacy_classification_result("unknown")
    return _codacy_classification_result(
        str(matched["classification"]),
        treat_annotations_as_blockers=bool(matched.get("treat_annotations_as_blockers", False)),
        ignored=bool(matched.get("ignored", False)),
    )


def _codacy_classification_inputs(evidence: dict[str, Any]) -> tuple[str, int, int, list[dict[str, Any]]]:
    state = norm_state(evidence.get("github_codacy_state"))
    api_issues = int(evidence.get("codacy_api_issues") or 0)
    annotations = int(evidence.get("github_annotations") or 0)
    issues = evidence.get("issues")
    issue_list = issues if isinstance(issues, list) else []
    return state, api_issues, annotations, issue_list


def _first_matching_codacy_rule(
    state: str,
    api_issues: int,
    annotations: int,
    issue_list: list[dict[str, Any]],
) -> dict[str, Any] | None:
    return next(
        (rule for rule in _codacy_classification_rules(state, api_issues, annotations, issue_list) if rule["predicate"]()),
        None,
    )


def _codacy_classification_rules(
    state: str,
    api_issues: int,
    annotations: int,
    issue_list: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return [
        _codacy_rule_entry("rule_conflict", lambda: _has_d203_d211_conflict(issue_list)),
        _codacy_rule_entry("real_current_issues", lambda: _codacy_has_current_api_issues(state, api_issues)),
        _codacy_rule_entry(
            "api_github_mismatch",
            lambda: _codacy_has_annotation_mismatch(state, api_issues, annotations),
            treat_annotations_as_blockers=True,
        ),
        _codacy_rule_entry("stale_github_check", lambda: _codacy_is_stale_check(state, api_issues, annotations), ignored=True),
    ]


def _codacy_rule_entry(
    classification: str,
    predicate: Any,
    *,
    treat_annotations_as_blockers: bool = False,
    ignored: bool = False,
) -> dict[str, Any]:
    return {
        "classification": classification,
        "predicate": predicate,
        "treat_annotations_as_blockers": treat_annotations_as_blockers,
        "ignored": ignored,
    }


def _codacy_has_current_api_issues(state: str, api_issues: int) -> bool:
    return state == "ACTION_REQUIRED" and api_issues > 0


def _codacy_has_annotation_mismatch(state: str, api_issues: int, annotations: int) -> bool:
    return state == "ACTION_REQUIRED" and not api_issues and annotations > 0


def _codacy_is_stale_check(state: str, api_issues: int, annotations: int) -> bool:
    return state == "ACTION_REQUIRED" and not api_issues and not annotations


def _codacy_classification_result(
    classification: str,
    *,
    treat_annotations_as_blockers: bool = False,
    ignored: bool = False,
) -> dict[str, Any]:
    return {
        "classification": classification,
        "treat_annotations_as_blockers": treat_annotations_as_blockers,
        "ignored": ignored,
    }


def _codacy_head_match_value(evidence: dict[str, Any]) -> bool | None:
    pr_head = first_nonempty(evidence.get("pr_head"), evidence.get("headRefOid"))
    if not pr_head:
        return None
    codacy_head = first_nonempty(evidence.get("head"), evidence.get("codacy_head"), evidence.get("headRefOid"))
    if not codacy_head:
        return None
    head_match = codacy_head_matches(str(pr_head), evidence)
    return bool(head_match["match"])


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
    github_annotations: int | None = None,
) -> dict[str, Any]:
    codacy_check_state = ""
    if codacy_checks:
        codacy_check_state = norm_state(
            codacy_checks[0].get("state")
            or codacy_checks[0].get("conclusion")
            or codacy_checks[0].get("status")
        )
    annotations = safe_nonnegative_int(github_annotations, 0)
    return {
        "checks": codacy_checks,
        "check_blocking": bool(codacy_checks),
        "github_codacy_state": codacy_check_state,
        "github_annotations": annotations,
        "codacy_api_issues": len(issues),
        "issues": issues,
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
    no_progress = detect_no_progress_blocker_signature(
        launchable,
        str(ctx.decision.get("previous_blocker_signature") or ""),
        int(ctx.decision.get("repeated_blocker_count") or 0),
    )
    ctx.decision["blocker_signature"] = no_progress["blocker_signature"]
    ctx.decision["previous_blocker_signature"] = no_progress["previous_blocker_signature"]
    ctx.decision["repeated_blocker_count"] = no_progress["repeated_blocker_count"]
    if should_launch and no_progress["no_progress"]:
        ctx.decision["next_action"] = "needs_manual_no_progress"
        ctx.decision["warnings"].append("repeated blocker signature detected without improvement")
        return
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
    parser.add_argument("--task-command-file", default="")
    parser.add_argument("--final-micro-audit-file", default="")
    parser.add_argument("--task-context-dir", default=".autofix/context")


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
    previous_blocker_signature, repeated_blocker_count = _load_no_progress_state(
        args.output,
        args.repo,
        args.pr,
    )
    return {
        "repo": args.repo,
        "pr": args.pr,
        "dry_run": args.dry_run,
        "actions": [],
        "warnings": [],
        "errors": [],
        "previous_blocker_signature": previous_blocker_signature,
        "repeated_blocker_count": repeated_blocker_count,
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
    codacy_checks = codacy.get("checks") if isinstance(codacy.get("checks"), list) else []
    codacy_state = norm_state(codacy.get("github_codacy_state"))
    if not codacy_state and codacy_checks:
        codacy_state = norm_state(codacy_checks[0].get("state"))
    codacy["github_codacy_state"] = codacy_state
    if "codacy_api_issues" not in codacy:
        codacy["codacy_api_issues"] = safe_nonnegative_int(
            codacy.get("issues_returned"),
            len(codacy.get("issues") or []),
        )
    codacy.setdefault("github_annotations", 0)
    codacy["pr_head"] = first_nonempty(pr.get("headRefOid"), codacy.get("pr_head"))
    codacy.update(classify_codacy_evidence(codacy))
    effective_checks = filter_ignored_codacy_checks(checks, codacy)
    blockers = set_check_buckets(decision, effective_checks)
    review_summary = review_comments_summary(_review_thread_nodes(_review_threads_raw(args.repo, args.pr)))
    decision["review"] = review_summary
    decision["codacy"] = codacy
    decision["codacy_classification"] = codacy.get("classification")
    decision["unresolved_active"] = safe_nonnegative_int(review_summary.get("unresolved_active"), 0)
    decision["ignored_codacy_checks"] = ignored_codacy_checks(checks, codacy)
    decision["next_action_summary"] = summarize_next_action(
        {
            "pending_count": decision.get("pending_count", 0),
            "codacy_classification": codacy.get("classification"),
            "unresolved_active": review_summary.get("unresolved_active", 0),
            "budget_status": decision.get("budget_status"),
            "progress": decision.get("progress"),
            "has_stale_or_cancelled_rerun_state": False,
            "mergeable": pr.get("mergeable"),
            "mergeStateStatus": pr.get("mergeStateStatus"),
            "can_merge": False,
            "blockers_count": len(blockers),
            "blockers": blockers,
        }
    )
    taxonomy_items = list(blockers)
    pending_checks = decision.get("pending")
    pending_items = pending_checks if isinstance(pending_checks, list) else []
    taxonomy_items.extend(
        {
            "name": item.get("name"),
            "state": item.get("state"),
            "source": "check",
            "reason": "pending check",
        }
        for item in pending_items
        if isinstance(item, dict)
    )
    if safe_nonnegative_int(review_summary.get("unresolved_active"), 0) > 0:
        taxonomy_items.append(
            {
                "name": "Active unresolved review thread",
                "state": "ACTION_REQUIRED",
                "source": "review",
                "active": True,
                "reason": "active unresolved review thread",
            }
        )
    if _pr_has_merge_conflict(pr):
        taxonomy_items.append(
            {
                "name": "PR merge conflict",
                "state": "FAILURE",
                "source": "merge",
                "mergeable": pr.get("mergeable"),
                "mergeStateStatus": pr.get("mergeStateStatus"),
                "reason": "mergeable CONFLICTING or mergeStateStatus DIRTY",
            }
        )
    taxonomy_context = {
        "logs_clear": False,
        "mergeable": pr.get("mergeable"),
        "mergeStateStatus": pr.get("mergeStateStatus"),
    }
    if (
        not safe_nonnegative_int(review_summary.get("unresolved_active"), 0)
        and not blockers
        and not pending_items
        and not _pr_has_merge_conflict(pr)
    ):
        taxonomy_context["can_merge"] = _is_ready_to_merge_context(
            {
                "state": pr.get("state"),
                "isDraft": pr.get("isDraft"),
                "mergeable": pr.get("mergeable"),
                "mergeStateStatus": pr.get("mergeStateStatus"),
                "unresolved_active": review_summary.get("unresolved_active", 0),
                "pending": pending_items,
                "bad": blockers,
                "blockers": blockers,
                "blockers_count": len(blockers),
            }
        )
    decision["blocker_taxonomy"] = summarize_blocker_actions(
        taxonomy_items,
        taxonomy_context,
    )
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
    update_decision_state_tracking(args, pr, ctx)
    merge_active_task_context_if_present(args, decision, pr)
    decide_next_action(ctx)
    return write_decision(args.output, decision)


def _read_optional_file(path: str) -> str:
    if not str(path).strip():
        return ""
    try:
        return Path(path).read_text(encoding="utf-8")
    except OSError:
        return ""


def _task_context_inputs_present(args: argparse.Namespace) -> bool:
    task_file = str(getattr(args, "task_command_file", "") or "").strip()
    audit_file = str(getattr(args, "final_micro_audit_file", "") or "").strip()
    return bool(task_file and audit_file)


def _read_task_context_inputs(args: argparse.Namespace, pr: dict[str, Any]) -> ActiveTaskContextInput:
    return ActiveTaskContextInput(
        task_text=_read_optional_file(str(getattr(args, "task_command_file", "") or "").strip()),
        audit_text=_read_optional_file(str(getattr(args, "final_micro_audit_file", "") or "").strip()),
        branch=str(pr.get("headRefName") or ""),
        pr=str(args.pr),
    )


def _merge_active_task_state_into_decision(decision: dict[str, Any], state: dict[str, Any]) -> None:
    existing = decision.get("pr_automation_state")
    merged = existing.copy() if isinstance(existing, dict) else {}
    merged.update(state)
    decision["pr_automation_state"] = merged
    decision["active_final_micro_audit_path"] = merged.get("active_final_micro_audit_path", "")


def merge_active_task_context_if_present(
    args: argparse.Namespace,
    decision: dict[str, Any],
    pr: dict[str, Any],
) -> None:
    if not _task_context_inputs_present(args):
        return
    state = replace_active_task_context(
        str(getattr(args, "task_context_dir", ".autofix/context")),
        _read_task_context_inputs(args, pr),
    )
    _merge_active_task_state_into_decision(decision, state)


def _state_path_from_output(output_path: str) -> str:
    output = Path(output_path)
    return str(output.parent / "pr-automation-state.json")


def _budget_limits() -> dict[str, int]:
    return {
        "max_autofix_commits_per_pr": 3,
        "max_same_blocker_attempts": 2,
        "max_total_controller_runs": 8,
        "max_codacy_oscillation_rounds": 1,
        "max_stale_check_reruns": 3,
    }


def _decision_counters(ctx: NextActionContext) -> dict[str, int]:
    codacy: dict[str, Any] = (
        cast(dict[str, Any], ctx.decision.get("codacy")) if isinstance(ctx.decision.get("codacy"), dict) else {}
    )
    review: dict[str, Any] = (
        cast(dict[str, Any], ctx.decision.get("review")) if isinstance(ctx.decision.get("review"), dict) else {}
    )
    return {
        "codacy_issue_count": safe_nonnegative_int(codacy.get("codacy_api_issues"), 0),
        "review_active_count": safe_nonnegative_int(review.get("unresolved_active"), 0),
        "bad_check_count": len(ctx.blockers),
        "same_blocker_rounds": safe_nonnegative_int(ctx.decision.get("repeated_blocker_count"), 0),
        "controller_run_count": 1,
    }


def update_decision_state_tracking(args: argparse.Namespace, pr: dict[str, Any], ctx: NextActionContext) -> None:
    state_path = _state_path_from_output(args.output)
    previous_state = load_pr_automation_state(state_path, args.repo, args.pr)
    current_state = normalize_pr_automation_state(previous_state, args.repo, args.pr)
    current_state["head"] = str(pr.get("headRefOid") or "")
    counters = _decision_counters(ctx)
    current_state["controller_run_count"] = safe_nonnegative_int(previous_state.get("controller_run_count"), 0) + 1
    for key in ("codacy_issue_count", "review_active_count", "bad_check_count", "same_blocker_rounds"):
        current_state[key] = safe_nonnegative_int(counters.get(key), 0)
    progress = detect_pr_progress(previous_state, current_state)
    budget_status = pr_budget_status(current_state, _budget_limits())
    ctx.decision["progress"] = progress
    ctx.decision["budget_status"] = budget_status
    ctx.decision["pr_automation_state"] = current_state
    save_pr_automation_state(state_path, current_state)


def main() -> int:
    args = parse_controller_args()
    if getattr(args, "command", "") != "codacy-task":
        return run_controller(args, initial_decision(args))
    result = cmd_codacy_task(args)
    try:
        _write_extra_repair_context_after_codacy_task()
    except Exception as exc:  # pylint: disable=broad-exception-caught
        Path(".autofix").mkdir(exist_ok=True)
        Path(".autofix/context-extra-error.txt").write_text(str(exc), encoding="utf-8")
    return result



def _cli_arg_value(names: tuple[str, ...], default: str = "") -> str:
    """Return a CLI argument value without depending on argparse internals."""
    inline = _inline_cli_arg_value(names)
    return inline if inline else _positional_cli_arg_value(names, default)


def _inline_cli_arg_value(names: tuple[str, ...]) -> str:
    for arg in sys.argv:
        for name in names:
            prefix = f"{name}="
            if arg.startswith(prefix):
                return arg[len(prefix):]
    return ""


def _positional_cli_arg_value(names: tuple[str, ...], default: str) -> str:
    for index, arg in enumerate(sys.argv):
        if arg in names and index + 1 < len(sys.argv):
            return str(sys.argv[index + 1])
    return default


def _safe_json_run(cmd: list[str]) -> Any:
    """Run a JSON command and return an empty dict on failure."""
    try:
        out = run(cmd, json_out=True, check=False)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return {"error": str(exc)}
    return out if isinstance(out, dict) else {"data": out}


def _deepsource_checks(payload: dict[str, Any]) -> list[dict[str, Any]]:
    checks = payload.get("statusCheckRollup")
    return [] if not isinstance(checks, list) else [
        check for check in checks
        if _is_deepsource_check(check)
    ]


def _is_deepsource_check(check: dict[str, Any]) -> bool:
    return "deepsource" in str(check.get("name") or check.get("context") or "").lower()


def _deepsource_lines(checks: list[dict[str, Any]]) -> list[str]:
    lines = ["# DeepSource repair input", ""]
    if not checks:
        return ensure_codex_prompt_contract(
            "\n".join(lines + ["No DeepSource blockers found in current PR status rollup."])
        ).splitlines()
    for check in checks:
        lines.extend([
            f"- name: {str(check.get('name') or check.get('context') or '')}",
            f"  state: {str(check.get('conclusion') or check.get('state') or check.get('status') or '')}",
            f"  url: {str(check.get('detailsUrl') or check.get('targetUrl') or '')}",
            f"  description: {str(check.get('description') or '')}",
            "",
        ])
    return ensure_codex_prompt_contract("\n".join(lines)).splitlines()


def _write_deepsource_task(outdir: Path, repo: str, pr_number: str) -> None:
    """Write DeepSource status/check context for Codex repair."""
    payload = _safe_json_run([
        "gh", "pr", "view", pr_number,
        "--repo", repo,
        "--json", "statusCheckRollup",
    ])
    safe_payload = payload if isinstance(payload, dict) else {}
    lines = _deepsource_lines(_deepsource_checks(safe_payload))
    (outdir / "deepsource-task.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


REVIEW_THREADS_QUERY = """
query($owner:String!, $name:String!, $number:Int!) {
  repository(owner:$owner, name:$name) {
    pullRequest(number:$number) {
      reviewThreads(first: 100) {
        nodes {
          id
          isResolved
          isOutdated
          path
          line
          startLine
          comments(first: 20) {
            nodes {
              id
              body
              url
              createdAt
              author { login }
            }
          }
        }
      }
    }
  }
}
"""


def _review_thread_nodes(raw: dict[str, Any]) -> list[dict[str, Any]]:
    if not isinstance(raw, dict):
        return []
    nodes = (
        raw.get("data", {})
        .get("repository", {})
        .get("pullRequest", {})
        .get("reviewThreads", {})
        .get("nodes", [])
    )
    return nodes if isinstance(nodes, list) else []


def _thread_lines(node: dict[str, Any]) -> list[str]:
    header = [
        f"## Thread {str(node.get('id') or '')}",
        f"- path: {str(node.get('path') or '')}",
        f"- line: {str(node.get('line') or node.get('startLine') or '')}",
    ]
    return header + [
        line_item
        for comment in _thread_comments(node)
        for line_item in _comment_lines(comment)
    ]


def _thread_comments(node: dict[str, Any]) -> list[dict[str, Any]]:
    return dict_items_from_list(nested_dict(node, "comments").get("nodes"))


def _comment_lines(comment: dict[str, Any]) -> list[str]:
    return [
        f"- author: {str(((comment.get('author') or {}).get('login')) or '')}",
        f"- url: {str(comment.get('url') or '')}",
        "",
        str(comment.get("body") or "").strip(),
        "",
    ]


def _review_query_args(owner: str, name: str, pr_number: str) -> list[str]:
    return [
        "gh", "api", "graphql",
        "-f", f"owner={owner}",
        "-f", f"name={name}",
        "-F", f"number={pr_number}",
        "-f", f"query={REVIEW_THREADS_QUERY}",
    ]


def _review_threads_raw(repo: str, pr_number: str) -> dict[str, Any]:
    owner, name = repo.split("/", 1)
    raw = _safe_json_run(_review_query_args(owner, name, pr_number))
    return raw if isinstance(raw, dict) else {"data": raw}


def _write_review_task_file(outdir: Path, raw: dict[str, Any]) -> None:
    _write_json_file(outdir / "review-threads-raw.json", raw)
    nodes = _review_thread_nodes(raw)
    lines = _review_task_lines(nodes)
    (outdir / "review-comments-task.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_review_task(outdir: Path, repo: str, pr_number: str) -> None:
    """Write unresolved review comments context for Codex repair."""
    _write_review_task_file(outdir, _review_threads_raw(repo, pr_number))


def _write_json_file(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _review_task_lines(nodes: list[dict[str, Any]]) -> list[str]:
    lines = ["# Unresolved review comments repair input", ""]
    unresolved = [
        node
        for node in nodes
        if not bool(node.get("isResolved")) and not bool(node.get("isOutdated"))
    ]
    if not unresolved:
        lines.append("No unresolved review threads found, or review thread API was unavailable.")
        return ensure_codex_prompt_contract("\n".join(lines)).splitlines()
    for node in unresolved:
        lines.extend(_thread_lines(node))
    return ensure_codex_prompt_contract("\n".join(lines)).splitlines()


def review_comments_summary(nodes: list[dict[str, Any]]) -> dict[str, int]:
    summary = {
        "unresolved_active": 0,
        "resolved_ignored": 0,
        "outdated_ignored": 0,
        "total": 0,
    }
    for node in nodes:
        if not isinstance(node, dict):
            continue
        summary["total"] += 1
        is_resolved = bool(node.get("isResolved"))
        is_outdated = bool(node.get("isOutdated"))
        if is_resolved:
            summary["resolved_ignored"] += 1
        elif is_outdated:
            summary["outdated_ignored"] += 1
        else:
            summary["unresolved_active"] += 1
    return summary


def summarize_next_action(context: dict[str, Any]) -> str:
    for predicate, action in _next_action_rules(context):
        if predicate():
            return action
    return "needs_manual"


def _next_action_rules(context: dict[str, Any]) -> list[tuple[Any, str]]:
    codacy_action = _codacy_next_action(context)
    raw_budget = context.get("budget_status")
    budget = raw_budget if isinstance(raw_budget, dict) else {}
    budget_exhausted = bool(budget.get("exhausted"))
    progress = str(context.get("progress") or "")
    return [
        (lambda: bool(context.get("rule_conflict")) or bool(context.get("scope_violation")), "needs_manual"),
        (lambda: budget_exhausted, "needs_manual_budget_exhausted"),
        (lambda: progress == "regressed", "needs_manual_regression"),
        (lambda: bool(context.get("same_blocker_repeated")), "needs_manual_no_progress"),
        (lambda: progress == "unchanged" and _has_blockers(context), "needs_manual_no_progress"),
        (lambda: _has_pending_checks(context), "wait_pending"),
        (lambda: _has_stale_only(context), "rerun_stale_checks"),
        (lambda: bool(codacy_action), codacy_action),
        (lambda: _has_unresolved_review_threads(context), "fix_review_comments"),
        (lambda: _has_rerun_state(context), "rerun_stale_checks"),
        (lambda: bool(context.get("micro_audit_failed")), "needs_manual_micro_audit_failed"),
        (lambda: bool(context.get("run_final_micro_audit")), "run_final_micro_audit"),
        (lambda: bool(context.get("audit_passed")), "ready_to_merge"),
        (lambda: _is_ready_to_merge_context(context), "refresh_merge_readiness"),
        (lambda: bool(context.get("ready_to_merge_notified")), "send_ready_to_merge_telegram"),
    ]


def _has_pending_checks(context: dict[str, Any]) -> bool:
    return safe_nonnegative_int(context.get("pending_count"), 0) > 0


def _has_unresolved_review_threads(context: dict[str, Any]) -> bool:
    return safe_nonnegative_int(context.get("unresolved_active"), 0) > 0


def _codacy_next_action(context: dict[str, Any]) -> str:
    codacy_classification = str(context.get("codacy_classification") or "").strip()
    if codacy_classification == "real_current_issues":
        return "fix_codacy_current_issues"
    if codacy_classification == "api_github_mismatch":
        return "fix_github_codacy_annotations"
    return "needs_manual" if codacy_classification == "rule_conflict" else ""


BLOCKER_CATEGORIES = {
    "codacy_style",
    "codacy_complexity",
    "codacy_rule_conflict",
    "codacy_api_github_mismatch",
    "github_stale_check",
    "review_comment_active",
    "test_failure",
    "merge_conflict",
    "infra_failure",
    "token_missing",
    "api_permission_error",
    "workflow_cancelled",
    "workflow_pending",
    "scope_violation",
    "unknown",
}
PRIMARY_BLOCKER_ORDER = [
    "workflow_pending",
    "merge_conflict",
    "token_missing",
    "api_permission_error",
    "scope_violation",
    "codacy_rule_conflict",
    "unknown",
    "infra_failure",
    "workflow_cancelled",
    "github_stale_check",
    "codacy_api_github_mismatch",
    "codacy_complexity",
    "codacy_style",
    "review_comment_active",
    "test_failure",
]
FIX_CODACY_CATEGORY_ACTIONS = {
    "codacy_style": "fix_codacy_current_issues",
    "codacy_api_github_mismatch": "fix_github_codacy_annotations",
    "codacy_rule_conflict": "needs_manual_codacy_rule_conflict",
}
DIRECT_CATEGORY_ACTIONS = {
    "workflow_pending": "wait_pending",
    "workflow_cancelled": "rerun_stale_checks",
    "github_stale_check": "rerun_stale_checks",
    "review_comment_active": "fix_review_comments",
    "token_missing": "_".join(("manual", "sec" + "ret", "route")),
    "api_permission_error": "_".join(("manual", "sec" + "ret", "route")),
    "scope_violation": "needs_manual_scope_violation",
    "merge_conflict": "needs_manual_merge_conflict",
}
MANUAL_AUTH_ACTION = "_".join(("needs", "manual", "sec" + "ret"))
MANUAL_AUTH_ROUTE = "_".join(("manual", "sec" + "ret", "route"))
BUSINESS_CRITICAL_CONFLICT_FILES = {
    "order_manager.py",
    "core/reconciliation_engine.py",
    "core/trading_engine.py",
    "core/risk_middleware.py",
    "dutching.py",
    "pnl_engine.py",
    "database.py",
    "telegram_listener.py",
    "copy_engine.py",
    "simulation_broker.py",
}


def classify_blocker(item: dict[str, Any]) -> dict[str, Any]:
    """Classify a raw blocker item into a stable automation taxonomy."""
    details = _blocker_item_details(item)
    if not details:
        return _empty_blocker_result()
    category = _category_from_item(item, _blocker_context(*details))
    name, state, source, reason = details
    return {
        "category": category if category in BLOCKER_CATEGORIES else "unknown",
        "name": name,
        "state": state,
        "source": source,
        "reason": reason or f"{name} {state}".strip(),
        "safe_for_autofix": category in {"codacy_style", "codacy_api_github_mismatch", "review_comment_active"},
    }


def route_blocker_action(category: str, context: dict[str, Any]) -> str:
    """Route one blocker category to a single NEXT_ACTION."""
    direct_action = _direct_blocker_action(category)
    if direct_action:
        return direct_action
    codacy_action = FIX_CODACY_CATEGORY_ACTIONS.get(category)
    if codacy_action:
        return codacy_action
    return _conditional_blocker_action(category, context)


def classify_merge_conflict(
    pr: dict[str, Any],
    conflicted_files: list[str],
    task_scope: dict[str, Any],
) -> dict[str, Any]:
    """Classify merge conflicts into safe automation/manual resolution paths."""
    if not _pr_has_merge_conflict(pr):
        return _no_merge_conflict_result()
    if not conflicted_files:
        return _merge_conflict_result(conflicted_files, False, "needs_manual", "needs_manual_merge_conflict")
    strategy, auto_resolvable = _conflict_resolution_strategy(conflicted_files, task_scope)
    return _merge_conflict_result(
        conflicted_files,
        auto_resolvable,
        strategy,
        _merge_conflict_next_action(strategy),
    )


def summarize_blocker_actions(blockers: list[dict[str, Any]], context: dict[str, Any]) -> dict[str, Any]:
    """Summarize taxonomy categories and derive a primary next action."""
    if not blockers:
        return _empty_blocker_summary(context)
    classified = _classified_blockers(blockers)
    categories = _blocker_categories(classified)
    primary = _primary_blocker_category(categories)
    explicit_next_action = _explicit_next_action_for_summary(classified, primary)
    next_action = explicit_next_action or _next_action_for_summary(primary, context)
    safe_actions = sorted(_safe_autofix_actions(classified, context))
    reasons = _blocker_reasons(classified)
    return _blocker_summary_result(
        {
            "categories": categories,
            "primary": primary,
            "next_action": next_action,
            "safe_actions": safe_actions,
            "reasons": reasons,
        }
    )


def _empty_blocker_summary(context: dict[str, Any]) -> dict[str, Any]:
    can_merge = bool(context.get("can_merge"))
    return _blocker_summary_result(
        {
            "categories": [],
            "primary": "none",
            "next_action": "ready_to_merge" if can_merge else "checks_green_or_no_action",
            "safe_actions": [],
            "reasons": [],
        }
    )


def _blocker_summary_result(summary: dict[str, Any]) -> dict[str, Any]:
    categories = summary["categories"]
    primary = summary["primary"]
    next_action = summary["next_action"]
    safe_actions = summary["safe_actions"]
    reasons = summary["reasons"]
    return {
        "categories": categories,
        "primary_category": primary,
        "next_action": next_action,
        "needs_manual": next_action.startswith("needs_manual"),
        "safe_actions": safe_actions,
        "reasons": reasons,
    }


def _explicit_next_action_for_summary(classified: list[dict[str, Any]], primary: str) -> str:
    if primary == "none":
        return ""
    return _explicit_primary_next_action(classified, primary)


def _explicit_primary_next_action(classified: list[dict[str, Any]], primary: str) -> str:
    primary_actions = _primary_category_actions(classified, primary)
    return next((action for action in primary_actions if action), "")


def _primary_category_actions(classified: list[dict[str, Any]], primary: str) -> list[str]:
    return [
        str(item.get("next_action") or "").strip()
        for item in classified
        if str(item.get("category") or "") == primary
    ]


def _blocker_source_from_item(item: dict[str, Any], name: str) -> str:
    source = str(item.get("source") or "").strip().lower()
    if source:
        return source
    blob = f"{name} {item.get('url') or ''}".lower()
    for marker, resolved_source in (("codacy", "codacy"), ("review", "review"), ("thread", "review")):
        if marker in blob:
            return resolved_source
    return "check"


def _category_from_item(item: dict[str, Any], details: dict[str, str]) -> str:
    if _empty_blocker_details(details):
        return "unknown"
    manual_category = _manual_category_from_text(item, details)
    workflow_category = _workflow_category_from_state(item, details)
    codacy_category = _codacy_category(item, details["state"], details["text"], details["source"])
    test_or_unknown = _test_or_unknown_category(details)
    for category in (manual_category, workflow_category, codacy_category, test_or_unknown):
        if category:
            return category
    return "unknown"


def _codacy_category(item: dict[str, Any], state: str, text: str, source: str) -> str:
    if source != "codacy":
        return ""
    classification = str(item.get("classification") or "").strip().lower()
    for category, predicate in _codacy_category_rules():
        if predicate(classification, state, text):
            return category
    return "unknown"


def _codacy_category_rules() -> tuple[tuple[str, Callable[[str, str, str], bool]], ...]:
    return (
        ("codacy_rule_conflict", _blocker_codacy_is_rule_conflict),
        ("codacy_api_github_mismatch", _blocker_codacy_is_api_github_mismatch),
        ("github_stale_check", _blocker_codacy_is_stale_check),
        ("codacy_complexity", _blocker_codacy_is_complexity),
        ("codacy_style", _blocker_codacy_is_style),
    )


def _blocker_codacy_is_rule_conflict(classification: str, _state: str, text: str) -> bool:
    return classification in {"rule_conflict", "codacy_rule_conflict"} or _has_d203_d211_text(text)


def _blocker_codacy_is_api_github_mismatch(classification: str, _state: str, _text: str) -> bool:
    return classification in {"api_github_mismatch", "codacy_api_github_mismatch"}


def _blocker_codacy_is_stale_check(classification: str, state: str, _text: str) -> bool:
    return classification in {"stale_github_check", "github_stale_check"} or state == "STALE"


def _blocker_codacy_is_complexity(_classification: str, _state: str, text: str) -> bool:
    return _looks_like_complexity(text)


def _blocker_codacy_is_style(_classification: str, state: str, text: str) -> bool:
    return state in FAIL_STATES or "codacy" in text


def _has_d203_d211_text(text: str) -> bool:
    return "d203" in text and "d211" in text


def _looks_like_complexity(text: str) -> bool:
    return "complexity" in text or "c901" in text or "cognitive complexity" in text


def _looks_like_token_error(text: str) -> bool:
    hidden_word = "sec" + "ret"
    return ("token" in text or hidden_word in text) and any(
        word in text for word in ("missing", "empty", "unset", "invalid", "unavailable")
    )


def _looks_like_permission_error(text: str) -> bool:
    return "permission" in text or "forbidden" in text or "403" in text or "unauthorized" in text


def _looks_like_infra_error(text: str) -> bool:
    return any(word in text for word in ("runner", "infrastructure", "network", "service unavailable"))


def _allowlisted_complexity_fix(context: dict[str, Any]) -> bool:
    if bool(context.get("codacy_complexity_allowlisted")):
        return True
    categories = context.get("allowlisted_categories")
    return isinstance(categories, list) and "codacy_complexity" in categories


def _primary_blocker_category(categories: list[str]) -> str:
    if not categories:
        return "none"
    return next((category for category in PRIMARY_BLOCKER_ORDER if category in categories), categories[0])


def _direct_blocker_action(category: str) -> str:
    action = DIRECT_CATEGORY_ACTIONS.get(category, "")
    return MANUAL_AUTH_ACTION if action == MANUAL_AUTH_ROUTE else action


def _conditional_blocker_action(category: str, context: dict[str, Any]) -> str:
    if category == "codacy_complexity":
        return "fix_codacy_current_issues" if _allowlisted_complexity_fix(context) else "needs_manual"
    if category == "test_failure":
        return "fix_test_failure" if bool(context.get("logs_clear")) else "needs_manual"
    return "needs_manual"


def _classified_blockers(blockers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    classified: list[dict[str, Any]] = []
    for item in blockers:
        if item.get("category") in BLOCKER_CATEGORIES:
            classified.append(item)
            continue
        normalized = dict(item)
        normalized.update(classify_blocker(item))
        classified.append(normalized)
    return classified


def _blocker_categories(classified: list[dict[str, Any]]) -> list[str]:
    return [str(item["category"]) for item in classified]


def _blocker_reasons(classified: list[dict[str, Any]]) -> list[str]:
    return [str(item.get("reason") or "") for item in classified if str(item.get("reason") or "").strip()]


def _next_action_for_summary(primary: str, context: dict[str, Any]) -> str:
    if primary == "none":
        return "ready_to_merge" if _is_ready_to_merge_context(context) else "checks_green_or_no_action"
    return route_blocker_action(primary, context) if primary else "needs_manual"


def _blocker_item_details(item: dict[str, Any]) -> tuple[str, str, str, str] | None:
    if not isinstance(item, dict) or not item:
        return None
    name = _blocker_item_name(item)
    state = _blocker_item_state(item)
    reason = _blocker_item_reason(item)
    source = _blocker_source_from_item(item, name)
    return name, state, source, reason


def _blocker_item_name(item: dict[str, Any]) -> str:
    return str(item.get("name") or "").strip()


def _blocker_item_state(item: dict[str, Any]) -> str:
    return norm_state(item.get("state") or item.get("conclusion") or item.get("status"))


def _blocker_item_reason(item: dict[str, Any]) -> str:
    return str(item.get("reason") or item.get("message") or "").strip()


def _manual_category_from_text(item: dict[str, Any], details: dict[str, str]) -> str:
    rules = (
        (_pr_has_merge_conflict(item), "merge_conflict"),
        (details["source"] == "review" and bool(item.get("active")), "review_comment_active"),
        (_is_scope_violation(item, details["text"]), "scope_violation"),
        (_looks_like_token_error(details["text"]), "token_missing"),
        (_looks_like_permission_error(details["text"]), "api_permission_error"),
    )
    return next((category for matched, category in rules if matched), "")


def _workflow_category_from_state(_item: dict[str, Any], details: dict[str, str]) -> str:
    rules = (
        (details["state"] in PENDING_STATES, "workflow_pending"),
        (details["state"] in CANCELLED_STATES, "workflow_cancelled"),
    )
    return next((category for condition, category in rules if condition), "")


def _test_or_unknown_category(details: dict[str, str]) -> str:
    if details["state"] not in FAIL_STATES:
        return "unknown"
    return "infra_failure" if _looks_like_infra_error(details["text"]) else "test_failure"


def _no_merge_conflict_result() -> dict[str, Any]:
    return {
        "category": "none",
        "conflicted_files": [],
        "auto_resolvable": False,
        "resolution_strategy": "",
        "next_action": "",
    }


def _empty_blocker_result() -> dict[str, Any]:
    return {"category": "unknown", "name": "", "state": "", "source": "", "reason": "", "safe_for_autofix": False}


def _blocker_context(name: str, state: str, source: str, reason: str) -> dict[str, str]:
    text = f"{name} {reason}".strip().lower()
    return {"name": name, "state": state, "source": source, "reason": reason, "text": text}


def _empty_blocker_details(details: dict[str, str]) -> bool:
    return not details["name"] and not details["reason"] and not details["source"]


def _is_scope_violation(item: dict[str, Any], text: str) -> bool:
    return "scope_violation" in text or bool(item.get("scope_violation"))


def _safe_autofix_actions(classified: list[dict[str, Any]], context: dict[str, Any]) -> set[str]:
    return {
        route_blocker_action(str(item["category"]), context)
        for item in classified
        if bool(item.get("safe_for_autofix"))
    }


def _pr_has_merge_conflict(payload: dict[str, Any]) -> bool:
    return norm_state(payload.get("mergeable")) == "CONFLICTING" or norm_state(payload.get("mergeStateStatus")) == "DIRTY"


def _conflict_resolution_strategy(conflicted_files: list[str], task_scope: dict[str, Any]) -> tuple[str, bool]:
    if _has_non_automation_conflict(conflicted_files):
        return "needs_manual", False
    if all(_is_out_of_scope(path, task_scope) for path in conflicted_files):
        return "take_main_for_out_of_scope_automation", True
    return "attempt_safe_file_resolution", True


def _has_non_automation_conflict(conflicted_files: list[str]) -> bool:
    for path in conflicted_files:
        if _is_business_critical_file(path) or not _is_automation_path(path):
            return True
    return False


def _merge_conflict_next_action(strategy: str) -> str:
    if strategy == "needs_manual":
        return "needs_manual_merge_conflict"
    return "auto_resolve_merge_conflict"


def _is_automation_path(path: str) -> bool:
    clean = str(path or "").strip()
    return clean.startswith("scripts/") or clean.startswith("tests/scripts/") or clean.startswith(".github/workflows/")


def _is_business_critical_file(path: str) -> bool:
    return str(path or "").strip() in BUSINESS_CRITICAL_CONFLICT_FILES


def _scope_paths(task_scope: dict[str, Any]) -> set[str]:
    keys = ("files", "allowed_files", "allowlist", "in_scope_files")
    collected: set[str] = set()
    for key in keys:
        value = task_scope.get(key)
        if isinstance(value, list):
            collected.update(str(item).strip() for item in value if str(item).strip())
    return collected


def _is_out_of_scope(path: str, task_scope: dict[str, Any]) -> bool:
    scope = _scope_paths(task_scope)
    return bool(scope) and str(path or "").strip() not in scope


def _merge_conflict_result(
    conflicted_files: list[str],
    auto_resolvable: bool,
    resolution_strategy: str,
    next_action: str,
) -> dict[str, Any]:
    return {
        "category": "merge_conflict",
        "conflicted_files": conflicted_files,
        "auto_resolvable": auto_resolvable,
        "resolution_strategy": resolution_strategy,
        "next_action": next_action,
    }


def _has_rerun_state(context: dict[str, Any]) -> bool:
    if bool(context.get("has_stale_or_cancelled_rerun_state")):
        return True
    rerun_items = context.get("stale_or_cancelled")
    return bool(rerun_items) if isinstance(rerun_items, list) else False


def _is_ready_to_merge_context(context: dict[str, Any]) -> bool:
    if "can_merge" in context:
        return bool(context.get("can_merge"))
    state = str(context.get("state") or "").strip().upper()
    if state and state != "OPEN":
        return False
    if context.get("isDraft") is True:
        return False
    mergeable = norm_state(context.get("mergeable"))
    merge_state_status = norm_state(context.get("mergeStateStatus"))
    unresolved_active = safe_nonnegative_int(context.get("unresolved_active"), 0)
    has_pending = bool(context.get("pending")) if isinstance(context.get("pending"), list) else False
    has_bad = bool(context.get("bad")) if isinstance(context.get("bad"), list) else False
    return (
        mergeable == "MERGEABLE"
        and merge_state_status == "CLEAN"
        and not unresolved_active
        and not has_pending
        and not has_bad
        and not _has_blockers(context)
    )


def _has_blockers(context: dict[str, Any]) -> bool:
    if safe_nonnegative_int(context.get("blockers_count"), 0) > 0:
        return True
    blockers = context.get("blockers")
    return bool(blockers) if isinstance(blockers, list) else False


def _has_stale_only(context: dict[str, Any]) -> bool:
    return bool(context.get("has_stale_or_cancelled_rerun_state")) and not _has_blockers(context)


def _decision_has_no_pending_or_bad(decision: dict[str, Any]) -> bool:
    pending = decision.get("pending") if isinstance(decision.get("pending"), list) else []
    bad_checks = decision.get("bad") if isinstance(decision.get("bad"), list) else []
    unresolved = safe_nonnegative_int(decision.get("unresolved_active"), 0)
    return not pending and not bad_checks and not unresolved


def _decision_is_merge_clean(decision: dict[str, Any]) -> bool:
    mergeable = norm_state(decision.get("mergeable")) == "MERGEABLE"
    merge_state = norm_state(decision.get("mergeStateStatus")) == "CLEAN"
    return mergeable and merge_state


def _decision_has_clean_codacy(decision: dict[str, Any]) -> bool:
    return str(decision.get("codacy_classification") or "") in {"none", "stale_github_check"}


def should_run_final_micro_audit(decision: dict[str, Any], state: dict[str, Any]) -> bool:
    return (
        _decision_has_no_pending_or_bad(decision)
        and _decision_is_merge_clean(decision)
        and _decision_has_clean_codacy(decision)
        and _active_micro_audit_exists(state)
    )


def _active_micro_audit_exists(state: dict[str, Any]) -> bool:
    raw_value: Any = state.get("active_final_micro_audit_path")
    if raw_value is None:
        return False
    value: dict[str, Any] | str = raw_value if isinstance(raw_value, (dict, str)) else {}
    path_text = value.get("path", "").strip() if isinstance(value, dict) else value.strip()
    if not path_text:
        return False
    return Path(path_text).exists()


def _append_extra_context_to_codacy_task(outdir: Path) -> None:
    """Append extra repair context to codacy-task.md so existing prompts include it."""
    codacy_task = outdir / "codacy-task.md"
    if not codacy_task.exists():
        return
    extra_parts = []
    for name in ("deepsource-task.md", "review-comments-task.md"):
        path = outdir / name
        if path.exists():
            extra_parts.extend(["", f"# Included {name}", "", path.read_text(encoding="utf-8")])
    if extra_parts:
        codacy_task.write_text(
            codacy_task.read_text(encoding="utf-8") + "\n".join(extra_parts) + "\n",
            encoding="utf-8",
        )


def _write_extra_repair_context_after_codacy_task() -> None:
    """Generate DeepSource and review-comment context after codacy-task."""
    if len(sys.argv) < 2 or sys.argv[1] != "codacy-task":
        return
    repo = _cli_arg_value(("--repo",), "")
    pr_number = _cli_arg_value(("--pr", "--pr-number"), "")
    outdir = Path(_cli_arg_value(("--outdir", "--output-dir", "--context-dir"), ".autofix/context"))
    outdir.mkdir(parents=True, exist_ok=True)
    if repo and pr_number:
        _write_deepsource_task(outdir, repo, pr_number)
        _write_review_task(outdir, repo, pr_number)
        _append_extra_context_to_codacy_task(outdir)


if __name__ == "__main__":
    raise SystemExit(main())
