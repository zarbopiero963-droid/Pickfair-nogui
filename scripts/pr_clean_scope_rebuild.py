#!/usr/bin/env python3
"""Clean-scope rebuild helper for PR automation control."""
# pylint: disable=too-many-branches,too-many-statements,too-many-locals,line-too-long,broad-exception-caught
from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import subprocess  # nosec B404
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SAFE_BRANCH_RE = re.compile(r"^[A-Za-z0-9._/\-]+$")
SAFE_REPO_RE = re.compile(r"^[A-Za-z0-9_.\-]+/[A-Za-z0-9_.\-]+$")
SAFE_FILE_RE = re.compile(r"^[A-Za-z0-9_./\-]+$")
ALLOWED_COMMAND_FAMILIES = {"gh", "git", "python", "python3", "pytest"}


@dataclass(frozen=True)
class RebuildArgs:
    """Parsed clean-scope rebuild command arguments."""
    repo: str
    pr_number: str
    branch: str
    allowlist: list[str]
    forbidden: list[str]
    decision_out: str
    dry_run: bool


@dataclass(frozen=True)
class CleanBranches:
    """Branch names and head SHA used during clean rebuild."""
    old_head: str
    backup_branch: str
    clean_branch: str


def validate_command_family(cmd: list[str]) -> None:
    """Reject commands outside the local command-family allowlist."""
    if not cmd:
        raise ValueError("empty command")
    command_name = Path(str(cmd[0])).name
    if command_name not in ALLOWED_COMMAND_FAMILIES:
        raise ValueError(f"command family not allowed: {command_name}")


def run(cmd: list[str], check: bool = True) -> tuple[int, str]:
    """Run a subprocess command and return returncode/output."""
    validate_command_family(cmd)
    proc = subprocess.run(  # nosec B603 # nosemgrep: python.lang.security.audit.dangerous-subprocess-use-audit,python.lang.security.audit.dangerous-subprocess-use-tainted-env-args
        cmd,
        text=True,
        capture_output=True,
        check=False,
    )
    output = (proc.stdout or "") + (proc.stderr or "")
    if check and proc.returncode:
        raise RuntimeError(f"command failed ({proc.returncode}): {' '.join(cmd)}\n{output}")
    return proc.returncode, output


def parse_csv(value: str) -> list[str]:
    """Parse comma-separated values into a normalized list."""
    return [item.strip() for item in value.split(",") if item.strip()]


def path_matches(path: str, pattern: str) -> bool:
    """Match exact paths or directory prefixes ending with '/'."""
    if pattern.endswith("/"):
        return path.startswith(pattern)
    return path == pattern


def filter_allowlisted(files: list[str], allowlist: list[str]) -> list[str]:
    """Return changed files that match allowlisted patterns."""
    output = [
        file_path
        for file_path in files
        if any(path_matches(file_path, pattern) for pattern in allowlist)
    ]
    return sorted(set(output))


def filter_forbidden(files: list[str], forbidden: list[str]) -> list[str]:
    """Return changed files that match forbidden patterns."""
    output = [
        file_path
        for file_path in files
        if any(path_matches(file_path, pattern) for pattern in forbidden)
    ]
    return sorted(set(output))


def parse_json_list(raw: str) -> list[Any]:
    """Run this small step of the clean-scope rebuild flow."""
    payload = json.loads(raw or "[]")
    return payload if isinstance(payload, list) else []


def filenames_from_payload(payload: list[Any]) -> list[str]:
    """Run this small step of the clean-scope rebuild flow."""
    names: list[str] = []
    for row in payload:
        if isinstance(row, dict):
            name = str(row.get("filename") or "").strip()
            if name:
                names.append(name)
    return names


def pr_files(repo: str, pr_number: str) -> list[str]:
    """Fetch changed file paths for the PR from GitHub."""
    _, output = run(["gh", "api", f"repos/{repo}/pulls/{pr_number}/files", "--paginate"])
    return filenames_from_payload(parse_json_list(output))


def changed_python_files(files: list[str]) -> list[str]:
    """Return python files from a path list."""
    return [file_path for file_path in files if file_path.endswith(".py")]


def timestamp_utc() -> str:
    """Run this small step of the clean-scope rebuild flow."""
    now = dt.datetime.now(dt.timezone.utc)
    return (
        f"{now.year:04d}{now.month:02d}{now.day:02d}T"
        f"{now.hour:02d}{now.minute:02d}{now.second:02d}Z"
    )


def parse_args() -> RebuildArgs:
    """Run this small step of the clean-scope rebuild flow."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", required=True)
    parser.add_argument("--pr", required=True)
    parser.add_argument("--branch", required=True)
    parser.add_argument("--allowlist", required=True)
    parser.add_argument("--forbidden", required=True)
    parser.add_argument("--decision-out", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    return RebuildArgs(
        repo=args.repo,
        pr_number=args.pr,
        branch=args.branch,
        allowlist=parse_csv(args.allowlist),
        forbidden=parse_csv(args.forbidden),
        decision_out=args.decision_out,
        dry_run=args.dry_run,
    )


def validate_file_path(file_path: str) -> None:
    """Run this small step of the clean-scope rebuild flow."""
    if not SAFE_FILE_RE.match(file_path):
        raise SystemExit(f"invalid file path: {file_path}")
    if file_path.startswith("/") or "/../" in f"/{file_path}/" or file_path.startswith("-"):
        raise SystemExit(f"unsafe file path: {file_path}")


def validate_inputs(args: RebuildArgs) -> None:
    """Run this small step of the clean-scope rebuild flow."""
    if not SAFE_REPO_RE.match(args.repo):
        raise SystemExit("invalid --repo format")
    if not args.pr_number.isdigit():
        raise SystemExit("invalid --pr format")
    if not SAFE_BRANCH_RE.match(args.branch):
        raise SystemExit("invalid --branch format")
    for file_path in args.allowlist + args.forbidden:
        validate_file_path(file_path.rstrip("/") or file_path)


def initial_decision(args: RebuildArgs) -> dict[str, Any]:
    """Run this small step of the clean-scope rebuild flow."""
    return {
        "action": "clean_scope_rebuild",
        "pr": args.pr_number,
        "branch": args.branch,
        "allowlist": args.allowlist,
        "forbidden": args.forbidden,
        "push_succeeded": False,
        "final_status": "blocked",
        "restored_files": [],
        "refused_files": [],
        "tests_run": [],
        "test_results": [],
    }


def write_decision(out_path: Path, decision: dict[str, Any]) -> int:
    """Run this small step of the clean-scope rebuild flow."""
    out_path.write_text(json.dumps(decision, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(decision, indent=2, sort_keys=True))
    return 0


def block_status(out_path: Path, decision: dict[str, Any], status: str) -> int:
    """Run this small step of the clean-scope rebuild flow."""
    decision["final_status"] = status
    return write_decision(out_path, decision)


def prepare_scope(args: RebuildArgs, decision: dict[str, Any]) -> tuple[list[str], list[str]]:
    """Run this small step of the clean-scope rebuild flow."""
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
    """Run this step of the clean-scope rebuild flow."""
    if not restored_files:
        block_status(out_path, decision, "blocked_allowlist_empty")
        return True
    if refused_files:
        block_status(out_path, decision, "blocked_forbidden_files_detected")
        return True
    return False


def build_branch_names(args: RebuildArgs) -> tuple[str, str]:
    """Run this small step of the clean-scope rebuild flow."""
    timestamp = timestamp_utc()
    backup_branch = f"backup/pr-{args.pr_number}-before-clean-scope-rebuild-{timestamp}"
    clean_branch = f"clean/pr-{args.pr_number}-scope-rebuild-{timestamp}"
    return backup_branch, clean_branch


def fetch_heads(args: RebuildArgs, decision: dict[str, Any]) -> CleanBranches:
    """Run this small step of the clean-scope rebuild flow."""
    backup_branch, clean_branch = build_branch_names(args)
    run(["git", "fetch", "origin", "main", args.branch])
    _, old_head = run(["git", "rev-parse", f"origin/{args.branch}"])
    decision["old_head"] = old_head.strip()
    decision["backup_branch"] = backup_branch
    return CleanBranches(
        old_head=old_head.strip(),
        backup_branch=backup_branch,
        clean_branch=clean_branch,
    )


def create_backup_and_clean_branch(args: RebuildArgs, branches: CleanBranches) -> None:
    """Run this small step of the clean-scope rebuild flow."""
    run(["git", "push", "origin", f"origin/{args.branch}:refs/heads/{branches.backup_branch}"])
    run(["git", "checkout", "-B", branches.clean_branch, "origin/main"])


def restore_files(args: RebuildArgs, restored_files: list[str]) -> None:
    """Run this small step of the clean-scope rebuild flow."""
    for file_path in restored_files:
        run(["git", "checkout", f"origin/{args.branch}", "--", file_path])


def append_test_result(decision: dict[str, Any], name: str, result: str) -> None:
    """Run this small step of the clean-scope rebuild flow."""
    decision["tests_run"].append(name)
    decision["test_results"].append(result)


def run_diff_guard(decision: dict[str, Any]) -> None:
    """Run this small step of the clean-scope rebuild flow."""
    run(["git", "diff", "--check"])
    append_test_result(decision, "git diff --check", "pass")


def run_compile_guard(decision: dict[str, Any], restored_files: list[str]) -> None:
    """Run this small step of the clean-scope rebuild flow."""
    python_files = changed_python_files(restored_files)
    if not python_files:
        return
    run([sys.executable, "-m", "py_compile", *python_files])
    append_test_result(decision, "python3 -m py_compile <changed python files>", "pass")


def changed_files_against_main() -> list[str]:
    """Run this small step of the clean-scope rebuild flow."""
    _, output = run(["git", "diff", "--name-only", "origin/main...HEAD"])
    return [line.strip() for line in output.splitlines() if line.strip()]


def stop_if_forbidden_remains(
    args: RebuildArgs,
    decision: dict[str, Any],
    out_path: Path,
) -> bool:
    """Run this step of the clean-scope rebuild flow."""
    remaining_forbidden = filter_forbidden(changed_files_against_main(), args.forbidden)
    if not remaining_forbidden:
        return False
    decision["refused_files"] = remaining_forbidden
    block_status(out_path, decision, "blocked_forbidden_files_would_remain")
    return True


def focused_tests_needed(restored_files: list[str]) -> bool:
    """Run this small step of the clean-scope rebuild flow."""
    focused_files = {"order_manager.py", "core/reconciliation_engine.py"}
    return any(file_path in focused_files for file_path in restored_files)


def run_focused_tests(decision: dict[str, Any], restored_files: list[str]) -> None:
    """Run this small step of the clean-scope rebuild flow."""
    if not focused_tests_needed(restored_files):
        return
    cmd = [
        "pytest",
        "-q",
        "tests/unit/test_order_manager_contracts.py",
        "tests/reconciliation/test_reconciliation_hardening.py",
        "-x",
    ]
    run(cmd)
    append_test_result(decision, "focused order/reconciliation tests", "pass")


def commit_and_push(args: RebuildArgs, decision: dict[str, Any], restored_files: list[str]) -> None:
    """Run this small step of the clean-scope rebuild flow."""
    run(["git", "add", "--", *restored_files])
    run(["git", "commit", "-m", f"Clean rebuild PR {args.pr_number} scope"])
    run(["git", "push", "--force-with-lease", "origin", f"HEAD:{args.branch}"])
    _, new_head = run(["git", "rev-parse", "HEAD"])
    decision["new_head"] = new_head.strip()
    decision["push_succeeded"] = True
    decision["final_status"] = "success"


def execute_rebuild(args: RebuildArgs, decision: dict[str, Any], out_path: Path) -> int:
    """Run this small step of the clean-scope rebuild flow."""
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
    """Run this small step of the clean-scope rebuild flow."""
    args = parse_args()
    validate_inputs(args)
    decision = initial_decision(args)
    out_path = Path(args.decision_out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        return execute_rebuild(args, decision, out_path)
    except Exception as exc:
        decision["error"] = str(exc)
        if decision.get("final_status") == "blocked":
            decision["final_status"] = "error"
        return write_decision(out_path, decision)


if __name__ == "__main__":
    raise SystemExit(main())
