#!/usr/bin/env python3
"""Clean-scope rebuild helper for PR automation control."""
# pylint: disable=too-many-branches,too-many-statements,too-many-locals,line-too-long,broad-exception-caught,missing-function-docstring,duplicate-code
from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import subprocess  # nosec B404
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

SAFE_BRANCH_RE = re.compile(r"^[A-Za-z0-9._/\-]+$")
SAFE_REPO_RE = re.compile(r"^[A-Za-z0-9_.\-]+/[A-Za-z0-9_.\-]+$")
SAFE_FILE_RE = re.compile(r"^[A-Za-z0-9_./\-]+$")
ALLOWED_COMMAND_FAMILIES = {"gh", "git", "python", "python3", "pytest"}


@dataclass(frozen=True)
class RebuildArgs:
    """Arguments required to run a clean-scope rebuild."""

    repo: str
    pr_number: str
    branch: str
    allowlist: list[str]
    forbidden: list[str]
    decision_out: str
    dry_run: bool


@dataclass(frozen=True)
class CleanBranches:
    """Resolved branch names and original head SHA for rebuild operations."""

    old_head: str
    backup_branch: str
    clean_branch: str


def validate_command_family(cmd: Sequence[str]) -> None:
    if not cmd:
        raise ValueError("empty command")
    command_name = Path(str(cmd[0])).name
    if command_name not in ALLOWED_COMMAND_FAMILIES:
        raise ValueError(f"command family not allowed: {command_name}")


def validate_command_arg(arg: str) -> None:
    if not isinstance(arg, str) or "\x00" in arg:
        raise ValueError("invalid command argument")


def validate_command_args(cmd: Sequence[str]) -> None:
    for arg in cmd:
        validate_command_arg(arg)


def combined_process_output(proc: subprocess.CompletedProcess[str]) -> str:
    return (proc.stdout or "") + (proc.stderr or "")


def command_display(cmd: Sequence[str]) -> str:
    return " ".join(cmd)


def raise_if_command_failed(
    proc: subprocess.CompletedProcess[str],
    cmd: Sequence[str],
    output: str,
    check: bool,
) -> None:
    if check and proc.returncode:
        raise RuntimeError(f"command failed ({proc.returncode}): {command_display(cmd)}\n{output}")


def run(cmd: Sequence[str], check: bool = True) -> tuple[int, str]:
    validate_command_family(cmd)
    validate_command_args(cmd)
    safe_cmd = list(cmd)
    proc = subprocess.run(  # nosec B603  # nosemgrep: python.lang.security.audit.dangerous-subprocess-use-audit.dangerous-subprocess-use-audit
        safe_cmd,  # nosemgrep: python.lang.security.audit.dangerous-subprocess-use-tainted-env-args.dangerous-subprocess-use-tainted-env-args
        text=True,
        capture_output=True,
        check=False,
    )
    output = combined_process_output(proc)
    raise_if_command_failed(proc, safe_cmd, output, check)
    return proc.returncode, output


def parse_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def path_matches(path: str, pattern: str) -> bool:
    if pattern.endswith("/"):
        return path.startswith(pattern)
    return path == pattern


def filter_matching(files: list[str], patterns: Sequence[str]) -> list[str]:
    return sorted({
        file_path
        for file_path in files
        if any(path_matches(file_path, pattern) for pattern in patterns)
    })


def filter_allowlisted(files: list[str], allowlist: Sequence[str]) -> list[str]:
    return filter_matching(files, allowlist)


def filter_forbidden(files: list[str], forbidden: Sequence[str]) -> list[str]:
    return filter_matching(files, forbidden)


def parse_json_list(raw: str) -> list[Any]:
    payload = json.loads(raw or "[]")
    return payload if isinstance(payload, list) else []


def filenames_from_payload(payload: list[Any]) -> list[str]:
    return [name for row in payload if (name := filename_from_row(row))]


def filename_from_row(row: Any) -> str:
    return str(row.get("filename") or "").strip() if isinstance(row, dict) else ""


def pr_files(repo: str, pr_number: str) -> list[str]:
    _, output = run(["gh", "api", f"repos/{repo}/pulls/{pr_number}/files", "--paginate"])
    return filenames_from_payload(parse_json_list(output))


def changed_python_files(files: list[str]) -> list[str]:
    return [file_path for file_path in files if file_path.endswith(".py")]


def timestamp_utc() -> str:
    now = dt.datetime.now(dt.timezone.utc)
    return f"{now.year:04d}{now.month:02d}{now.day:02d}T{now.hour:02d}{now.minute:02d}{now.second:02d}Z"


def parse_args() -> RebuildArgs:
    parser = argparse.ArgumentParser()
    for name in ("repo", "pr", "branch", "allowlist", "forbidden"):
        parser.add_argument(f"--{name}", required=True)
    parser.add_argument("--decision-out", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    return RebuildArgs(
        args.repo, args.pr, args.branch, parse_csv(args.allowlist),
        parse_csv(args.forbidden), args.decision_out, args.dry_run,
    )


def validate_file_path(file_path: str) -> None:
    if not SAFE_FILE_RE.match(file_path):
        raise SystemExit(f"invalid file path: {file_path}")
    if file_path.startswith("/") or "/../" in f"/{file_path}/" or file_path.startswith("-"):
        raise SystemExit(f"unsafe file path: {file_path}")


def validate_match(value: str, pattern: re.Pattern[str], message: str) -> None:
    if not pattern.match(value):
        raise SystemExit(message)


def validate_inputs(args: RebuildArgs) -> None:
    validate_match(args.repo, SAFE_REPO_RE, "invalid --repo format")
    if not args.pr_number.isdigit():
        raise SystemExit("invalid --pr format")
    validate_match(args.branch, SAFE_BRANCH_RE, "invalid --branch format")
    for file_path in args.allowlist + args.forbidden:
        validate_file_path(file_path.rstrip("/") or file_path)


def initial_decision(args: RebuildArgs) -> dict[str, Any]:
    return {
        "action": "clean_scope_rebuild", "pr": args.pr_number, "branch": args.branch,
        "allowlist": args.allowlist, "forbidden": args.forbidden,
        "push_succeeded": False, "final_status": "blocked",
        "restored_files": [], "refused_files": [], "tests_run": [], "test_results": [],
    }


def write_decision(out_path: Path, decision: dict[str, Any]) -> int:
    out_path.write_text(json.dumps(decision, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(decision, indent=2, sort_keys=True))
    return 0


def block_status(out_path: Path, decision: dict[str, Any], status: str) -> int:
    decision["final_status"] = status
    return write_decision(out_path, decision)


def prepare_scope(args: RebuildArgs, decision: dict[str, Any]) -> tuple[list[str], list[str]]:
    all_files = pr_files(args.repo, args.pr_number)
    restored_files = filter_allowlisted(all_files, args.allowlist)
    refused_files = filter_forbidden(all_files, args.forbidden)
    decision["restored_files"] = restored_files
    decision["refused_files"] = refused_files
    for file_path in restored_files:
        validate_file_path(file_path)
    return restored_files, refused_files


def stop_for_scope_blockers(
    out_path: Path,
    decision: dict[str, Any],
    restored_files: list[str],
    refused_files: list[str],
) -> bool:
    status = scope_block_status(restored_files, refused_files)
    if not status:
        return False
    block_status(out_path, decision, status)
    return True


def scope_block_status(restored_files: list[str], refused_files: list[str]) -> str:
    if not restored_files:
        return "blocked_allowlist_empty"
    return "blocked_forbidden_files_detected" if refused_files else ""


def build_branch_names(args: RebuildArgs) -> tuple[str, str]:
    timestamp = timestamp_utc()
    backup_branch = f"backup/pr-{args.pr_number}-before-clean-scope-rebuild-{timestamp}"
    clean_branch = f"clean/pr-{args.pr_number}-scope-rebuild-{timestamp}"
    return backup_branch, clean_branch


def fetch_heads(args: RebuildArgs, decision: dict[str, Any]) -> CleanBranches:
    backup_branch, clean_branch = build_branch_names(args)
    run(["git", "fetch", "origin", "main", args.branch])
    _, old_head = run(["git", "rev-parse", f"origin/{args.branch}"])
    decision["old_head"] = old_head.strip()
    decision["backup_branch"] = backup_branch
    return CleanBranches(old_head.strip(), backup_branch, clean_branch)


def create_backup_and_clean_branch(args: RebuildArgs, branches: CleanBranches) -> None:
    run(["git", "push", "origin", f"origin/{args.branch}:refs/heads/{branches.backup_branch}"])
    run(["git", "checkout", "-B", branches.clean_branch, "origin/main"])


def restore_files(args: RebuildArgs, restored_files: list[str]) -> None:
    for file_path in restored_files:
        try:
            run(["git", "checkout", f"origin/{args.branch}", "--", file_path])
        except RuntimeError as exc:
            if "did not match any file(s) known to git" not in str(exc):
                raise


def append_test_result(decision: dict[str, Any], name: str, result: str) -> None:
    decision["tests_run"].append(name)
    decision["test_results"].append(result)


def run_diff_guard(decision: dict[str, Any]) -> None:
    run(["git", "diff", "--check"])
    append_test_result(decision, "git diff --check", "pass")


def run_compile_guard(decision: dict[str, Any], restored_files: list[str]) -> None:
    python_files = changed_python_files(restored_files)
    if not python_files:
        return
    executable = str(sys.executable)
    executable_name = Path(executable).name
    if executable_name.startswith("python3"):
        family = "python3"
    elif executable_name.startswith("python"):
        family = "python"
    else:
        family = executable_name
    validate_command_family([family])
    run([executable, "-m", "py_compile", *python_files])
    append_test_result(decision, "python3 -m py_compile <changed python files>", "pass")


def changed_files_against_main() -> list[str]:
    _, output = run(["git", "diff", "--name-only", "origin/main...HEAD"])
    return [line.strip() for line in output.splitlines() if line.strip()]


def stop_if_forbidden_remains(
    args: RebuildArgs,
    decision: dict[str, Any],
    out_path: Path,
) -> bool:
    remaining_forbidden = filter_forbidden(changed_files_against_main(), args.forbidden)
    if not remaining_forbidden:
        return False
    decision["refused_files"] = remaining_forbidden
    block_status(out_path, decision, "blocked_forbidden_files_would_remain")
    return True


def focused_tests_needed(restored_files: list[str]) -> bool:
    focused_suffixes = ("/order_manager.py", "/core/reconciliation_engine.py")
    focused_exact = {suffix.lstrip("/") for suffix in focused_suffixes}
    return any(file_path in focused_exact or file_path.endswith(focused_suffixes) for file_path in restored_files)


def run_focused_tests(decision: dict[str, Any], restored_files: list[str]) -> None:
    if not focused_tests_needed(restored_files):
        return
    cmd = [
        "pytest", "-q", "tests/unit/test_order_manager_contracts.py",
        "tests/reconciliation/test_reconciliation_hardening.py", "-x",
    ]
    return_code, _ = run(cmd, check=False)
    append_test_result(decision, "focused order/reconciliation tests", "pass" if not return_code else "fail")


def ensure_git_identity() -> None:
    """Configure git identity for automated clean-scope commits."""
    run(["git", "config", "user.name", "pickfair-safe-autofix-bot"])
    run(["git", "config", "user.email", "pickfair-autofix-bot@users.noreply.github.com"])


def commit_and_push(args: RebuildArgs, decision: dict[str, Any], restored_files: list[str]) -> None:
    ensure_git_identity()
    run(["git", "add", "--", *restored_files])
    run(["git", "commit", "-m", f"Clean rebuild PR {args.pr_number} scope"])
    run(["git", "push", "--force-with-lease", "origin", f"HEAD:{args.branch}"])
    _, new_head = run(["git", "rev-parse", "HEAD"])
    decision["new_head"] = new_head.strip()
    decision["push_succeeded"] = True
    decision["final_status"] = "success"


def execute_rebuild(args: RebuildArgs, decision: dict[str, Any], out_path: Path) -> int:
    restored_files, refused_files = prepare_scope(args, decision)
    if stop_for_scope_blockers(out_path, decision, restored_files, refused_files):
        return 0
    branches = fetch_heads(args, decision)
    if args.dry_run:
        return block_status(out_path, decision, "dry_run")
    create_backup_and_clean_branch(args, branches)
    restore_files(args, restored_files)
    run_diff_guard(decision)
    run_compile_guard(decision, restored_files)
    run_focused_tests(decision, restored_files)
    if stop_if_forbidden_remains(args, decision, out_path):
        return 0
    commit_and_push(args, decision, restored_files)
    return write_decision(out_path, decision)


def main() -> int:
    args = parse_args()
    validate_inputs(args)
    decision = initial_decision(args)
    out_path = Path(args.decision_out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        return execute_rebuild(args, decision, out_path)
    except (RuntimeError, ValueError, OSError, subprocess.CalledProcessError) as exc:
        decision["error"] = str(exc)
        if decision.get("final_status") != "blocked":
            decision["final_status"] = "error"
        return write_decision(out_path, decision)


if __name__ == "__main__":
    raise SystemExit(main())
