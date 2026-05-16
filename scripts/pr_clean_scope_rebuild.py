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
SAFE_COMMANDS = {"gh", "git", "python3", "pytest"}


def run(cmd: list[str], check: bool = True) -> str:
    out, code = run_with_code(cmd)
    if check and code != 0:
        raise subprocess.CalledProcessError(code, cmd, output=out)
    return out


def run_with_code(cmd: list[str]) -> tuple[str, int]:
    if not cmd or cmd[0] not in SAFE_COMMANDS:
        raise ValueError(f"unsupported command: {cmd[0] if cmd else '<empty>'}")
    proc = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
    return (proc.stdout or ""), int(proc.returncode)


def pr_view(repo: str, pr: str) -> dict[str, Any]:
    return json.loads(
        run([
            "gh", "pr", "view", pr, "--repo", repo, "--json", "number,headRefName,headRefOid,url"
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


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True)
    ap.add_argument("--pr", required=True)
    ap.add_argument("--mode", default="auto")
    ap.add_argument("--output", default="pr-clean-scope-rebuild/decision.json")
    args = ap.parse_args()

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

    try:
        pr = pr_view(args.repo, args.pr)
        pr_branch = str(pr.get("headRefName") or "")
        old_head = str(pr.get("headRefOid") or "")
        decision["branch"] = pr_branch
        decision["old_head"] = old_head

        run(["git", "fetch", "origin", "main", pr_branch])

        diff_files = changed_files("origin/main", f"origin/{pr_branch}")
        refused = [f for f in diff_files if is_forbidden(f)]
        restored = [f for f in diff_files if f not in refused]
        decision["restored_files"] = restored
        decision["refused_files"] = refused

        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        backup_branch = f"backup/pr-{args.pr}-before-clean-scope-rebuild-{ts}"
        decision["backup_branch"] = backup_branch

        backup_pushed = False
        if old_head:
            run(["git", "push", "origin", f"{old_head}:refs/heads/{backup_branch}"], check=False)
            verify = run(["git", "ls-remote", "--heads", "origin", backup_branch], check=False)
            backup_pushed = bool(verify.strip())
        decision["backup_branch_pushed"] = backup_pushed
        if not backup_pushed:
            return fail_and_exit(outp, decision, "backup_push_failed")

        work_branch = f"clean-scope-rebuild-pr-{args.pr}-{ts}"
        run(["git", "checkout", "-B", work_branch, "origin/main"])

        if restored:
            run(["git", "checkout", f"origin/{pr_branch}", "--", *restored])

        diff_check = run(["git", "diff", "--check"], check=False)
        if diff_check.strip():
            decision["diff_check"] = diff_check[-2000:]
            return fail_and_exit(outp, decision, "diff_check_failed")

        py_changed = [f for f in changed_files("origin/main", "HEAD") if f.endswith(".py")]
        if not run_py_compile_checks(decision, py_changed):
            return fail_and_exit(outp, decision, "py_compile_failed")

        require_heavy = any(p in diff_files for p in FORBIDDEN_EXACT)
        if require_heavy and not run_required_tests(decision):
            return fail_and_exit(outp, decision, "required_tests_failed")

        final_diff = changed_files("origin/main", "HEAD")
        still_forbidden = [f for f in final_diff if is_forbidden(f)]
        decision["forbidden_after_rebuild"] = still_forbidden
        if still_forbidden:
            return fail_and_exit(outp, decision, "forbidden_files_remain")

        if not restored:
            return fail_and_exit(outp, decision, "allowlist_empty_refuse_force_push")

        run(["git", "add", "-A"])
        commit_msg = f"Clean rebuild PR {args.pr} scope"
        run(["git", "commit", "-m", commit_msg], check=False)

        push = run(["git", "push", "--force-with-lease", "origin", f"HEAD:{pr_branch}"], check=False)
        decision["push_output"] = push[-1500:]

        updated = pr_view(args.repo, args.pr)
        new_head = str(updated.get("headRefOid") or "")
        decision["new_head"] = new_head
        decision["push_succeeded"] = bool(new_head and new_head != old_head)
        decision["final_status"] = "success" if decision["push_succeeded"] else "push_failed"

    except Exception as exc:
        decision["final_status"] = "exception"
        decision["error"] = str(exc)

    write_decision(outp, decision)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
