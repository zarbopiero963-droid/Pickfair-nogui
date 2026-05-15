#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any

OUT_DIR = Path(".autofix/context")
OUT_DIR.mkdir(parents=True, exist_ok=True)

PR_NUMBER = os.environ.get("PR_NUMBER") or os.environ.get("INPUT_PR_NUMBER") or ""
REPO_FULL = os.environ.get("REPO") or os.environ.get("GITHUB_REPOSITORY") or "zarbopiero963-droid/Pickfair-nogui"

MAX_WAIT = int(os.environ.get("REVIEW_CONTEXT_MAX_WAIT_SECONDS", "900"))
MIN_GRACE = int(os.environ.get("REVIEW_CONTEXT_MIN_GRACE_SECONDS", "180"))
POLL = int(os.environ.get("REVIEW_CONTEXT_POLL_SECONDS", "60"))
STABLE_POLLS = int(os.environ.get("REVIEW_CONTEXT_STABLE_POLLS", "2"))

owner, repo = REPO_FULL.split("/", 1)

RED_STATES = {"FAILURE", "ERROR", "ACTION_REQUIRED", "TIMED_OUT", "CANCELLED", "STARTUP_FAILURE"}
PENDING_STATES = {"", "PENDING", "IN_PROGRESS", "QUEUED", "REQUESTED", "WAITING", "EXPECTED"}

THREAD_QUERY = """
query($owner:String!, $repo:String!, $number:Int!) {
  repository(owner:$owner, name:$repo) {
    pullRequest(number:$number) {
      reviewThreads(first:100) {
        nodes {
          id
          isResolved
          path
          line
          comments(last:5) {
            nodes {
              author { login }
              createdAt
              url
              body
            }
          }
        }
      }
    }
  }
}
"""

def run(cmd: list[str]) -> tuple[int, str, str]:
    result = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return result.returncode, result.stdout, result.stderr

def state_of(check: dict[str, Any]) -> str:
    return str(check.get("conclusion") or check.get("state") or check.get("status") or "").upper()

def get_checks() -> dict[str, Any]:
    code, out, err = run([
        "gh", "pr", "view", PR_NUMBER,
        "--repo", REPO_FULL,
        "--json", "headRefOid,statusCheckRollup,mergeStateStatus",
    ])
    if code != 0:
        return {"error": err.strip(), "headRefOid": "", "statusCheckRollup": []}
    return json.loads(out)

def get_threads_digest() -> list[dict[str, Any]]:
    code, out, err = run([
        "gh", "api", "graphql",
        "-f", f"owner={owner}",
        "-f", f"repo={repo}",
        "-F", f"number={int(PR_NUMBER)}",
        "-f", f"query={THREAD_QUERY}",
    ])
    if code != 0:
        return [{"error": err.strip()}]

    payload = json.loads(out)
    nodes = (
        payload.get("data", {})
        .get("repository", {})
        .get("pullRequest", {})
        .get("reviewThreads", {})
        .get("nodes", [])
    )

    digest = []
    for thread in nodes:
        if thread.get("isResolved"):
            continue

        comments = thread.get("comments", {}).get("nodes", []) or []
        last = comments[-1] if comments else {}
        body = last.get("body") or ""
        digest.append({
            "id": thread.get("id"),
            "path": thread.get("path"),
            "line": thread.get("line"),
            "last_author": (last.get("author") or {}).get("login"),
            "last_createdAt": last.get("createdAt"),
            "last_body_sha": hashlib.sha256(body.encode("utf-8")).hexdigest(),
        })

    return sorted(digest, key=lambda item: str(item.get("id") or ""))

def snapshot() -> dict[str, Any]:
    checks_payload = get_checks()
    checks = checks_payload.get("statusCheckRollup", []) or []

    red = []
    pending = []

    for check in checks:
        state = state_of(check)
        item = {
            "name": check.get("name") or check.get("context"),
            "state": state,
            "url": check.get("detailsUrl") or check.get("targetUrl"),
        }
        if state in RED_STATES:
            red.append(item)
        elif state in PENDING_STATES:
            pending.append(item)

    return {
        "head": checks_payload.get("headRefOid"),
        "mergeState": checks_payload.get("mergeStateStatus"),
        "red": sorted(red, key=lambda item: str(item.get("name"))),
        "pending": sorted(pending, key=lambda item: str(item.get("name"))),
        "threads": get_threads_digest(),
    }

started = time.time()
last_key = None
stable = 0
last_snapshot: dict[str, Any] = {}

while True:
    snap = snapshot()
    last_snapshot = snap

    elapsed = int(time.time() - started)
    red_count = len(snap.get("red", []))
    pending_count = len(snap.get("pending", []))
    thread_count = len(snap.get("threads", []))

    key = json.dumps(snap, sort_keys=True)
    if key == last_key:
        stable += 1
    else:
        stable = 1
        last_key = key

    print(
        f"Review context wait: red={red_count} pending={pending_count} "
        f"unresolved_threads={thread_count} stable={stable}/{STABLE_POLLS} elapsed={elapsed}s"
    )

    if elapsed >= MIN_GRACE and pending_count == 0 and stable >= STABLE_POLLS:
        print("Review context is stable.")
        break

    if elapsed >= MAX_WAIT:
        print("Review context max wait reached; continuing with latest available context.")
        break

    time.sleep(POLL)

(OUT_DIR / "pr-context-stability.json").write_text(
    json.dumps(last_snapshot, indent=2, ensure_ascii=False),
    encoding="utf-8",
)
