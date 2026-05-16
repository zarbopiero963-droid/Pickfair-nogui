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


def run(cmd: list[str], check: bool = True) -> str:
    try:
        return subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as exc:
        if check:
            raise
        return exc.output or ""


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
            decision["final_status"] = "backup_push_failed"
            outp.write_text(json.dumps(decision, indent=2, sort_keys=True), encoding="utf-8")
            print(json.dumps(decision, indent=2, sort_keys=True))
            return 0

        work_branch = f"clean-scope-rebuild-pr-{args.pr}-{ts}"
        run(["git", "checkout", "-B", work_branch, "origin/main"])

        if restored:
            run(["git", "checkout", f"origin/{pr_branch}", "--", *restored])

        diff_check = run(["git", "diff", "--check"], check=False)
        if diff_check.strip():
            decision["final_status"] = "diff_check_failed"
            decision["diff_check"] = diff_check[-2000:]
            outp.write_text(json.dumps(decision, indent=2, sort_keys=True), encoding="utf-8")
            print(json.dumps(decision, indent=2, sort_keys=True))
            return 0

        py_changed = [f for f in changed_files("origin/main", "HEAD") if f.endswith(".py")]
        for p in py_changed:
            cmd = ["python3", "-m", "py_compile", p]
            decision["tests_run"].append(" ".join(cmd))
            out = run(cmd, check=False)
            ok = out.strip() == ""
            decision["test_results"].append({"cmd": " ".join(cmd), "ok": ok, "output": out[-500:]})
            if not ok:
                decision["final_status"] = "py_compile_failed"
                outp.write_text(json.dumps(decision, indent=2, sort_keys=True), encoding="utf-8")
                print(json.dumps(decision, indent=2, sort_keys=True))
                return 0

        require_heavy = "order_manager.py" in diff_files or "core/reconciliation_engine.py" in diff_files
        if require_heavy:
            cmd = [
                "pytest", "-q",
                "tests/unit/test_order_manager_contracts.py",
                "tests/reconciliation/test_reconciliation_hardening.py",
                "-x",
            ]
            decision["tests_run"].append(" ".join(cmd))
            out = run(cmd, check=False)
            ok = "failed" not in out.lower()
            decision["test_results"].append({"cmd": " ".join(cmd), "ok": ok, "output": out[-2000:]})
            if not ok:
                decision["final_status"] = "required_tests_failed"
                outp.write_text(json.dumps(decision, indent=2, sort_keys=True), encoding="utf-8")
                print(json.dumps(decision, indent=2, sort_keys=True))
                return 0

        final_diff = changed_files("origin/main", "HEAD")
        still_forbidden = [f for f in final_diff if is_forbidden(f)]
        decision["forbidden_after_rebuild"] = still_forbidden
        if still_forbidden:
            decision["final_status"] = "forbidden_files_remain"
            outp.write_text(json.dumps(decision, indent=2, sort_keys=True), encoding="utf-8")
            print(json.dumps(decision, indent=2, sort_keys=True))
            return 0

        if not restored:
            decision["final_status"] = "allowlist_empty_refuse_force_push"
            outp.write_text(json.dumps(decision, indent=2, sort_keys=True), encoding="utf-8")
            print(json.dumps(decision, indent=2, sort_keys=True))
            return 0

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

    outp.write_text(json.dumps(decision, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(decision, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
