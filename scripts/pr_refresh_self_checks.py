#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from typing import Any


SELF_CHECK_NAMES = {
    "safe pr autofix",
    "pr autofix safe supervisor",
    "merge readiness",
    "pr merge readiness",
}

STALE_SELF_STATES = {
    "FAILURE",
    "ERROR",
    "ACTION_REQUIRED",
    "CANCELLED",
    "TIMED_OUT",
}

OK_STATES = {
    "SUCCESS",
    "SKIPPED",
    "NEUTRAL",
}


def run_json(cmd: list[str]) -> Any:
    out = subprocess.check_output(cmd, text=True)
    return json.loads(out)


def run_text(cmd: list[str]) -> str:
    return subprocess.check_output(cmd, text=True).strip()


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
    )


def extract_run_id(url: str) -> str:
    match = re.search(r"/actions/runs/(\d+)", url or "")
    return match.group(1) if match else ""


def get_run_attempt(repo: str, run_id: str) -> int:
    try:
        raw = run_text(["gh", "api", f"repos/{repo}/actions/runs/{run_id}", "--jq", ".run_attempt"])
        return int(raw or "0")
    except Exception:
        return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default=os.environ.get("REPO", ""))
    ap.add_argument("--pr", default=os.environ.get("PR_NUMBER", ""))
    ap.add_argument("--dry-run", action="store_true", default=os.environ.get("DRY_RUN", "false").lower() == "true")
    ap.add_argument("--max-rerun-attempts", type=int, default=int(os.environ.get("MAX_RERUN_ATTEMPTS", "2")))
    args = ap.parse_args()

    if not args.repo or not args.pr or args.pr == "null":
        print(json.dumps({"action": "skip", "reason": "missing repo or pr_number"}, indent=2))
        return 0

    pr = run_json([
        "gh", "pr", "view", str(args.pr),
        "--repo", args.repo,
        "--json", "state,mergeable,mergeStateStatus,headRefOid,statusCheckRollup",
    ])

    checks = pr.get("statusCheckRollup") or []

    real_bad: list[dict[str, Any]] = []
    self_stale: list[dict[str, Any]] = []
    self_pending: list[dict[str, Any]] = []

    for check in checks:
        item = {
            "name": check_name(check),
            "state": norm_state(check.get("conclusion") or check.get("state") or check.get("status")),
            "url": check_url(check),
            "self": is_self_check(check),
        }

        if item["self"]:
            if item["state"] in STALE_SELF_STATES:
                self_stale.append(item)
            elif item["state"] in {"", "PENDING", "IN_PROGRESS", "QUEUED"}:
                self_pending.append(item)
            continue

        if item["state"] not in OK_STATES:
            real_bad.append(item)

    decision: dict[str, Any] = {
        "pr": str(args.pr),
        "state": pr.get("state"),
        "head": pr.get("headRefOid"),
        "mergeable": pr.get("mergeable"),
        "mergeStateStatus": pr.get("mergeStateStatus"),
        "real_bad": real_bad,
        "self_stale": self_stale,
        "self_pending": self_pending,
        "dry_run": args.dry_run,
        "rerun": [],
        "skipped": [],
    }

    if pr.get("state") != "OPEN":
        decision["action"] = "skip"
        decision["reason"] = "PR is not open"
        print(json.dumps(decision, indent=2, sort_keys=True))
        return 0

    if real_bad:
        decision["action"] = "skip"
        decision["reason"] = "real blockers or pending checks exist"
        print(json.dumps(decision, indent=2, sort_keys=True))
        return 0

    if not self_stale:
        decision["action"] = "skip"
        decision["reason"] = "no stale self checks"
        print(json.dumps(decision, indent=2, sort_keys=True))
        return 0

    seen_runs: set[str] = set()
    for item in self_stale:
        run_id = extract_run_id(item["url"])
        if not run_id:
            decision["skipped"].append({**item, "reason": "missing run id"})
            continue
        if run_id in seen_runs:
            continue
        seen_runs.add(run_id)

        attempt = get_run_attempt(args.repo, run_id)
        if attempt >= args.max_rerun_attempts:
            decision["skipped"].append({**item, "run_id": run_id, "run_attempt": attempt, "reason": "max attempts reached"})
            continue

        entry = {**item, "run_id": run_id, "run_attempt": attempt}
        if not args.dry_run:
            subprocess.check_call(["gh", "run", "rerun", run_id, "--repo", args.repo])
            entry["requested"] = True
        else:
            entry["requested"] = False
        decision["rerun"].append(entry)

    decision["action"] = "rerun" if decision["rerun"] else "skip"
    print(json.dumps(decision, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
