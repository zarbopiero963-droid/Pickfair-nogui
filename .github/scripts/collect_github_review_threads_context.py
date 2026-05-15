#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any

OUT_DIR = Path(".autofix/context")
OUT_DIR.mkdir(parents=True, exist_ok=True)

PR_NUMBER = os.environ.get("PR_NUMBER") or os.environ.get("INPUT_PR_NUMBER")
REPO_FULL = os.environ.get("REPO") or os.environ.get("GITHUB_REPOSITORY") or "zarbopiero963-droid/Pickfair-nogui"

if not PR_NUMBER:
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    if event_path and Path(event_path).exists():
        try:
            event = json.loads(Path(event_path).read_text(encoding="utf-8"))
            PR_NUMBER = str(
                event.get("inputs", {}).get("pr_number")
                or event.get("pull_request", {}).get("number")
                or event.get("number")
                or ""
            )
        except Exception:
            PR_NUMBER = ""

owner, repo = REPO_FULL.split("/", 1)

QUERY = """
query($owner:String!, $repo:String!, $number:Int!) {
  repository(owner:$owner, name:$repo) {
    pullRequest(number:$number) {
      reviewThreads(first:100) {
        nodes {
          id
          isResolved
          path
          line
          comments(last:20) {
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

def run_graphql() -> dict[str, Any]:
    cmd = [
        "gh", "api", "graphql",
        "-f", f"owner={owner}",
        "-f", f"repo={repo}",
        "-F", f"number={int(PR_NUMBER)}",
        "-f", f"query={QUERY}",
    ]
    result = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        return {"error": result.stderr.strip(), "threads": []}
    return json.loads(result.stdout)

try:
    payload = run_graphql()
    nodes = (
        payload.get("data", {})
        .get("repository", {})
        .get("pullRequest", {})
        .get("reviewThreads", {})
        .get("nodes", [])
    )
except Exception as exc:
    payload = {"error": str(exc), "threads": []}
    nodes = []

threads: list[dict[str, Any]] = []
for thread in nodes:
    if thread.get("isResolved"):
        continue

    comments = thread.get("comments", {}).get("nodes", []) or []
    normalized_comments = []
    for comment in comments:
        normalized_comments.append({
            "author": (comment.get("author") or {}).get("login"),
            "createdAt": comment.get("createdAt"),
            "url": comment.get("url"),
            "body": comment.get("body") or "",
        })

    threads.append({
        "id": thread.get("id"),
        "path": thread.get("path"),
        "line": thread.get("line"),
        "isResolved": thread.get("isResolved"),
        "comments": normalized_comments,
    })

(OUT_DIR / "github-review-threads.json").write_text(
    json.dumps({"unresolved": threads}, indent=2, ensure_ascii=False),
    encoding="utf-8",
)

lines = ["# Unresolved GitHub review threads", ""]
if not threads:
    lines.append("No unresolved review threads found.")
else:
    for thread in threads:
        lines.append(f"## {thread.get('path') or 'unknown'}:{thread.get('line') or 'unknown'}")
        lines.append(f"- thread_id: `{thread.get('id')}`")
        for comment in thread.get("comments", []):
            lines.append("")
            lines.append(f"### {comment.get('author') or 'unknown'} at {comment.get('createdAt') or 'unknown'}")
            lines.append(f"URL: {comment.get('url') or ''}")
            lines.append("")
            lines.append(comment.get("body") or "")
            lines.append("")
        lines.append("---")
        lines.append("")

(OUT_DIR / "github-review-threads.md").write_text(
    "\n".join(lines),
    encoding="utf-8",
)

print(f"Collected unresolved GitHub review threads: {len(threads)}")
