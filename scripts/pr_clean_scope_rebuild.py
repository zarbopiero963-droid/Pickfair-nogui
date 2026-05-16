#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
from pathlib import Path


def run(cmd: list[str], check: bool = True) -> tuple[int, str]:
    proc = subprocess.run(cmd, text=True, capture_output=True, check=False)
    out = (proc.stdout or "") + (proc.stderr or "")
    if check and proc.returncode != 0:
        raise RuntimeError(f"command failed ({proc.returncode}): {' '.join(cmd)}\n{out}")
    return proc.returncode, out


def parse_csv(value: str) -> list[str]:
    return [v.strip() for v in value.split(",") if v.strip()]


def path_matches(path: str, pattern: str) -> bool:
    if pattern.endswith("/"):
        return path.startswith(pattern)
    return path == pattern


def filter_allowlisted(files: list[str], allowlist: list[str]) -> list[str]:
    out: list[str] = []
    for f in files:
        if any(path_matches(f, p) for p in allowlist):
            out.append(f)
    return sorted(set(out))


def filter_forbidden(files: list[str], forbidden: list[str]) -> list[str]:
    out: list[str] = []
    for f in files:
        if any(path_matches(f, p) for p in forbidden):
            out.append(f)
    return sorted(set(out))


def pr_files(repo: str, pr: str) -> list[str]:
    _, out = run(["gh", "api", f"repos/{repo}/pulls/{pr}/files", "--paginate"])
    payload = json.loads(out or "[]")
    names: list[str] = []
    if isinstance(payload, list):
        for row in payload:
            if not isinstance(row, dict):
                continue
            name = str(row.get("filename") or "").strip()
            if name:
                names.append(name)
    return names


def changed_python_files(files: list[str]) -> list[str]:
    return [f for f in files if f.endswith(".py")]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True)
    ap.add_argument("--pr", required=True)
    ap.add_argument("--branch", required=True)
    ap.add_argument("--allowlist", required=True)
    ap.add_argument("--forbidden", required=True)
    ap.add_argument("--decision-out", required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    allowlist = parse_csv(args.allowlist)
    forbidden = parse_csv(args.forbidden)

    decision: dict[str, object] = {
        "action": "clean_scope_rebuild",
        "pr": args.pr,
        "branch": args.branch,
        "allowlist": allowlist,
        "forbidden": forbidden,
        "push_succeeded": False,
        "final_status": "blocked",
        "restored_files": [],
        "refused_files": [],
        "tests_run": [],
        "test_results": [],
    }
    out_path = Path(args.decision_out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        all_files = pr_files(args.repo, args.pr)
        restored_files = filter_allowlisted(all_files, allowlist)
        refused_files = filter_forbidden(all_files, forbidden)
        decision["restored_files"] = restored_files
        decision["refused_files"] = refused_files

        if not restored_files:
            decision["final_status"] = "blocked_allowlist_empty"
            out_path.write_text(json.dumps(decision, indent=2, sort_keys=True), encoding="utf-8")
            print(json.dumps(decision, indent=2, sort_keys=True))
            return 0

        if refused_files:
            decision["final_status"] = "blocked_forbidden_files_detected"
            out_path.write_text(json.dumps(decision, indent=2, sort_keys=True), encoding="utf-8")
            print(json.dumps(decision, indent=2, sort_keys=True))
            return 0

        ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        backup_branch = f"backup/pr-{args.pr}-before-clean-scope-rebuild-{ts}"
        clean_branch = f"clean/pr-{args.pr}-scope-rebuild-{ts}"

        run(["git", "fetch", "origin", "main", args.branch])
        _, old_head = run(["git", "rev-parse", f"origin/{args.branch}"])
        old_head = old_head.strip()
        decision["old_head"] = old_head
        decision["backup_branch"] = backup_branch

        if args.dry_run:
            decision["final_status"] = "dry_run"
            out_path.write_text(json.dumps(decision, indent=2, sort_keys=True), encoding="utf-8")
            print(json.dumps(decision, indent=2, sort_keys=True))
            return 0

        run(["git", "push", "origin", f"origin/{args.branch}:refs/heads/{backup_branch}"])
        run(["git", "checkout", "-B", clean_branch, "origin/main"])

        for rel in restored_files:
            run(["git", "checkout", f"origin/{args.branch}", "--", rel])

        run(["git", "diff", "--check"])
        decision["tests_run"] = ["git diff --check"]
        decision["test_results"] = ["pass"]

        py_files = changed_python_files(restored_files)
        if py_files:
            run([sys.executable, "-m", "py_compile", *py_files])
            decision["tests_run"].append("python3 -m py_compile <changed python files>")
            decision["test_results"].append("pass")

        needs_focused = any(f in {"order_manager.py", "core/reconciliation_engine.py"} for f in restored_files)
        if needs_focused:
            decision["tests_run"].append("focused tests")
            decision["test_results"].append("fail_not_allowed_order_or_reconciliation_changed")
            decision["final_status"] = "blocked_focused_tests_required"
            out_path.write_text(json.dumps(decision, indent=2, sort_keys=True), encoding="utf-8")
            print(json.dumps(decision, indent=2, sort_keys=True))
            return 0

        run(["git", "add", "--", *restored_files])
        run(["git", "commit", "-m", f"Clean rebuild PR {args.pr} scope"])
        run(["git", "push", "--force-with-lease", "origin", f"HEAD:{args.branch}"])
        _, new_head = run(["git", "rev-parse", "HEAD"])

        decision["new_head"] = new_head.strip()
        decision["push_succeeded"] = True
        decision["final_status"] = "success"
    except Exception as exc:
        decision["error"] = str(exc)
        if "final_status" not in decision or decision["final_status"] == "blocked":
            decision["final_status"] = "error"

    out_path.write_text(json.dumps(decision, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(decision, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
