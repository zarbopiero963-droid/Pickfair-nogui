#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from typing import Any


BAD_STATES = {
    "FAILURE",
    "ERROR",
    "ACTION_REQUIRED",
    "CANCELLED",
    "TIMED_OUT",
    "STARTUP_FAILURE",
}

PENDING_STATES = {
    "",
    "PENDING",
    "QUEUED",
    "IN_PROGRESS",
    "WAITING",
    "REQUESTED",
    None,
}


def run_json(cmd: list[str]) -> Any:
    raw = subprocess.check_output(cmd, text=True)
    return json.loads(raw)


def norm_state(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def check_name(check: dict[str, Any]) -> str:
    return str(check.get("name") or check.get("context") or "").strip()


def check_url(check: dict[str, Any]) -> str:
    return str(check.get("detailsUrl") or check.get("targetUrl") or check.get("url") or "").strip()


def check_state(check: dict[str, Any]) -> str:
    return norm_state(check.get("conclusion") or check.get("state") or check.get("status"))


def is_safe_autofix_self_check(check, url: str = "") -> bool:
    if isinstance(check, dict):
        name = str(check.get("name") or check.get("context") or "").strip().lower()
        url_l = str(check.get("url") or check.get("detailsUrl") or check.get("targetUrl") or "").strip().lower()
    else:
        name = str(check or "").strip().lower()
        url_l = str(url or "").strip().lower()

    return (
        name in {
            "safe pr autofix",
            "pr autofix safe supervisor",
            "merge readiness",
            "pr merge readiness",
        }
        or "pr-autofix-safe-supervisor" in url_l
        or "pr-merge-readiness" in url_l
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default=os.environ.get("REPO", ""))
    ap.add_argument("--pr", default=os.environ.get("PR", ""))
    ap.add_argument("--ignore-safe-autofix", action="store_true")
    ap.add_argument("--output", default="")
    ap.add_argument("--fail", action="store_true", help="exit non-zero when PR is not merge-ready")
    args = ap.parse_args()

    if not args.repo:
        print("REPO missing", file=sys.stderr)
        return 2
    if not args.pr:
        print("PR missing", file=sys.stderr)
        return 2

    pr = run_json([
        "gh", "pr", "view", str(args.pr),
        "--repo", args.repo,
        "--json",
        "state,isDraft,mergeable,mergeStateStatus,reviewDecision,headRefOid,baseRefName,headRefName,statusCheckRollup,url",
    ])

    checks = pr.get("statusCheckRollup") or []
    normalized: list[dict[str, Any]] = []

    for c in checks:
        item = {
            "name": check_name(c),
            "state": check_state(c),
            "url": check_url(c),
            "is_safe_autofix_self_check": is_safe_autofix_self_check(c),
        }
        if args.ignore_safe_autofix and item["is_safe_autofix_self_check"]:
            item["ignored"] = True
        else:
            item["ignored"] = False
        normalized.append(item)

    active = [c for c in normalized if not c["ignored"]]
    blockers = [c for c in active if c["state"] in BAD_STATES]
    pending = [c for c in active if c["state"] in PENDING_STATES]

    state = str(pr.get("state") or "")
    is_draft = bool(pr.get("isDraft"))
    mergeable = str(pr.get("mergeable") or "")
    merge_state = str(pr.get("mergeStateStatus") or "")

    reasons: list[str] = []
    if state != "OPEN":
        reasons.append(f"PR state is {state}, expected OPEN")
    if is_draft:
        reasons.append("PR is draft")
    if mergeable != "MERGEABLE":
        reasons.append(f"mergeable is {mergeable}, expected MERGEABLE")
    if merge_state != "CLEAN":
        reasons.append(f"mergeStateStatus is {merge_state}, expected CLEAN")
    if blockers:
        reasons.append(f"{len(blockers)} blocking check(s)")
    if pending:
        reasons.append(f"{len(pending)} pending check(s)")

    can_merge = not reasons

    result = {
        "can_merge": can_merge,
        "reasons": reasons,
        "state": state,
        "isDraft": is_draft,
        "mergeable": mergeable,
        "mergeStateStatus": merge_state,
        "reviewDecision": pr.get("reviewDecision"),
        "headRefOid": pr.get("headRefOid"),
        "baseRefName": pr.get("baseRefName"),
        "headRefName": pr.get("headRefName"),
        "url": pr.get("url"),
        "blockers": blockers,
        "pending": pending,
        "ignored": [c for c in normalized if c["ignored"]],
        "checks": normalized,
    }

    text = json.dumps(result, indent=2, sort_keys=True)
    print(text)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(text + "\n")

    if args.fail and not can_merge:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
