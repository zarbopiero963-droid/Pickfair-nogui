#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any

ALLOWED_WORKFLOW = ".github/workflows/pr-automation-controller-v2.yml"
FORBIDDEN_EXACT = {"order_manager.py", "core/reconciliation_engine.py"}
ALLOWED_COMMANDS = {
    ("gh", "pr", "view"),
    ("git", "fetch"),
    ("git", "diff", "--name-only"),
    ("git", "push"),
    ("git", "ls-remote"),
    ("git", "checkout"),
    ("git", "diff", "--check"),
    ("git", "add", "-A"),
    ("git", "commit"),
    ("python3", "-m", "py_compile"),
    ("pytest", "-q"),
}
GIT_PATTERNS: list[tuple[tuple[str, ...], int]] = [
    (("git", "fetch"), 3),
    (("git", "diff", "--name-only"), 4),
    (("git", "push"), 3),
    (("git", "ls-remote", "--heads"), 5),
    (("git", "checkout"), 3),
    (("git", "diff", "--check"), 3),
    (("git", "add", "-A"), 3),
    (("git", "commit"), 3),
]


def _is_allowed_command(cmd: list[str]) -> bool:
    return any(len(cmd) >= len(prefix) and tuple(cmd[: len(prefix)]) == prefix for prefix in ALLOWED_COMMANDS)


def run(cmd: list[str], check: bool = True) -> str:
    out, code = run_with_code(cmd)
    if check and code != 0:
        raise subprocess.CalledProcessError(code, cmd, output=out)
    return out


def _run_checked(command: list[str]) -> tuple[str, int]:
    proc = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
    return (proc.stdout or ""), int(proc.returncode)


def _run_gh(cmd: list[str]) -> tuple[str, int]:
    if cmd[:3] == ["gh", "pr", "view"] and len(cmd) >= 4:
        return _run_checked(["gh", "pr", "view", cmd[3], *cmd[4:]])
    raise ValueError(f"unsupported gh command: {' '.join(cmd)}")


def _run_git(cmd: list[str]) -> tuple[str, int]:
    matches_allowed = any(
        cmd[: len(prefix)] == list(prefix) and len(cmd) >= min_len for prefix, min_len in GIT_PATTERNS
    )
    if matches_allowed:
        return _run_checked(cmd)
    raise ValueError(f"unsupported git command: {' '.join(cmd)}")


def _run_python(cmd: list[str]) -> tuple[str, int]:
    if cmd[:3] == ["python3", "-m", "py_compile"] and len(cmd) == 4:
        return _run_checked(["python3", "-m", "py_compile", cmd[3]])
    raise ValueError(f"unsupported python command: {' '.join(cmd)}")


def _run_pytest(cmd: list[str]) -> tuple[str, int]:
    if cmd[:2] == ["pytest", "-q"] and len(cmd) >= 3:
        return _run_checked(["pytest", "-q", *cmd[2:]])
    raise ValueError(f"unsupported pytest command: {' '.join(cmd)}")


def run_with_code(cmd: list[str]) -> tuple[str, int]:
    if not cmd or not _is_allowed_command(cmd):
        raise ValueError(f"unsupported command: {' '.join(cmd) if cmd else '<empty>'}")
    if cmd[0] == "gh":
        return _run_gh(cmd)
    if cmd[0] == "git":
        return _run_git(cmd)
    if cmd[0] == "python3":
        return _run_python(cmd)
    if cmd[0] == "pytest":
        return _run_pytest(cmd)
    raise ValueError(f"unsupported command: {' '.join(cmd)}")


def pr_view(repo: str, pr_number: str) -> dict[str, Any]:
    return json.loads(
        run([
            "gh", "pr", "view", pr_number, "--repo", repo, "--json", "number,headRefName,headRefOid,url"
        ])
    )


def is_forbidden(path: str) -> bool:
    if path in FORBIDDEN_EXACT:
        return True
    if path.startswith("guardrails/"):
        return True
    if path.startswith(".github/scripts/"):
        return True
    if path.startswith(".github/workflows/") and path != ALLOWED_WORKFLOW:
        return True
    return False


def changed_files(base: str, head: str) -> list[str]:
    out = run(["git", "diff", "--name-only", f"{base}...{head}"], check=False)
    return [x.strip() for x in out.splitlines() if x.strip()]


def write_decision(outp: Path, decision: dict[str, Any]) -> None:
    payload = json.dumps(decision, indent=2, sort_keys=True)
    outp.write_text(payload, encoding="utf-8")
    print(payload)


def fail_and_exit(outp: Path, decision: dict[str, Any], status: str) -> int:
    decision["final_status"] = status
    write_decision(outp, decision)
    return 0


def run_py_compile_checks(decision: dict[str, Any], files: list[str]) -> bool:
    for p in files:
        cmd = ["python3", "-m", "py_compile", p]
        decision["tests_run"].append(" ".join(cmd))
        out, code = run_with_code(cmd)
        ok = code == 0
        decision["test_results"].append({"cmd": " ".join(cmd), "ok": ok, "output": out[-500:]})
        if not ok:
            return False
    return True


def run_required_tests(decision: dict[str, Any]) -> bool:
    cmd = [
        "pytest", "-q",
        "tests/unit/test_order_manager_contracts.py",
        "tests/reconciliation/test_reconciliation_hardening.py",
        "-x",
    ]
    decision["tests_run"].append(" ".join(cmd))
    out, code = run_with_code(cmd)
    ok = code == 0
    decision["test_results"].append({"cmd": " ".join(cmd), "ok": ok, "output": out[-2000:]})
    return ok


def initialize_decision(args: argparse.Namespace) -> tuple[Path, dict[str, Any]]:
    outp = Path(args.output)
    outp.parent.mkdir(parents=True, exist_ok=True)
    decision: dict[str, Any] = {
        "action": "clean_scope_rebuild",
        "pr": args.pr,
        "mode": args.mode,
        "tests_run": [],
        "test_results": [],
        "push_succeeded": False,
        "final_status": "failed",
    }
    return outp, decision


def collect_rebuild_inputs(repo: str, pr_number: str, decision: dict[str, Any]) -> tuple[str, str, list[str], list[str], str]:
    pr_info = pr_view(repo, pr_number)
    pr_branch = str(pr_info.get("headRefName") or "")
    old_head = str(pr_info.get("headRefOid") or "")
    decision["branch"] = pr_branch
    decision["old_head"] = old_head

    run(["git", "fetch", "origin", "main", pr_branch])
    diff_files = changed_files("origin/main", f"origin/{pr_branch}")
    refused = [f for f in diff_files if is_forbidden(f)]
    restored = [f for f in diff_files if f not in refused]
    decision["restored_files"] = restored
    decision["refused_files"] = refused

    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    backup_branch = f"backup/pr-{pr_number}-before-clean-scope-rebuild-{ts}"
    decision["backup_branch"] = backup_branch
    return pr_branch, old_head, diff_files, restored, backup_branch


def push_backup_ref(old_head: str, backup_branch: str) -> bool:
    if not old_head:
        return False
    run(["git", "push", "origin", f"{old_head}:refs/heads/{backup_branch}"], check=False)
    verify = run(["git", "ls-remote", "--heads", "origin", backup_branch], check=False)
    return bool(verify.strip())


def rebuild_allowed_scope(*, pr_number: str, pr_branch: str, restored: list[str], ts: str) -> None:
    work_branch = f"clean-scope-rebuild-pr-{pr_number}-{ts}"
    run(["git", "checkout", "-B", work_branch, "origin/main"])
    if restored:
        run(["git", "checkout", f"origin/{pr_branch}", "--", *restored])


def run_validation_suite(decision: dict[str, Any], diff_files: list[str]) -> str | None:
    diff_check = run(["git", "diff", "--check"], check=False)
    if diff_check.strip():
        decision["diff_check"] = diff_check[-2000:]
        return "diff_check_failed"

    py_changed = [f for f in changed_files("origin/main", "HEAD") if f.endswith(".py")]
    if not run_py_compile_checks(decision, py_changed):
        return "py_compile_failed"

    require_heavy = any(p in diff_files for p in FORBIDDEN_EXACT)
    if require_heavy and not run_required_tests(decision):
        return "required_tests_failed"

    final_diff = changed_files("origin/main", "HEAD")
    still_forbidden = [f for f in final_diff if is_forbidden(f)]
    decision["forbidden_after_rebuild"] = still_forbidden
    if still_forbidden:
        return "forbidden_files_remain"
    return None


def commit_and_push(repo: str, pr_number: str, pr_branch: str, old_head: str, decision: dict[str, Any]) -> None:
    run(["git", "add", "-A"])
    run(["git", "commit", "-m", f"Clean rebuild PR {pr_number} scope"], check=False)

    push = run(["git", "push", "--force-with-lease", "origin", f"HEAD:{pr_branch}"], check=False)
    decision["push_output"] = push[-1500:]

    updated = pr_view(repo, pr_number)
    new_head = str(updated.get("headRefOid") or "")
    decision["new_head"] = new_head
    decision["push_succeeded"] = bool(new_head and new_head != old_head)
    decision["final_status"] = "success" if decision["push_succeeded"] else "push_failed"


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True)
    ap.add_argument("--pr", required=True)
    ap.add_argument("--mode", default="auto")
    ap.add_argument("--output", default="pr-clean-scope-rebuild/decision.json")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    outp, decision = initialize_decision(args)

    try:
        pr_branch, old_head, diff_files, restored, backup_branch = collect_rebuild_inputs(args.repo, args.pr, decision)
        backup_ok = push_backup_ref(old_head, backup_branch)
        decision["backup_branch_pushed"] = backup_ok
        if not backup_ok:
            return fail_and_exit(outp, decision, "backup_push_failed")

        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        rebuild_allowed_scope(pr_number=args.pr, pr_branch=pr_branch, restored=restored, ts=ts)

        status = run_validation_suite(decision, diff_files)
        if status:
            return fail_and_exit(outp, decision, status)

        if not restored:
            return fail_and_exit(outp, decision, "allowlist_empty_refuse_force_push")

        commit_and_push(args.repo, args.pr, pr_branch, old_head, decision)
    except (subprocess.CalledProcessError, ValueError, OSError) as exc:
        decision["final_status"] = "exception"
        decision["error"] = str(exc)

    write_decision(outp, decision)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
