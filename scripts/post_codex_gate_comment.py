#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import sys
from typing import Any, Dict, List


COMMENT_MARKER = "<!-- codex-bug-gate-comment -->"


def _run(cmd: List[str]) -> str:
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(
            f"Command failed: {' '.join(cmd)}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )
    return proc.stdout


def _load_event() -> Dict[str, Any]:
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    if not event_path:
        raise RuntimeError("GITHUB_EVENT_PATH not set")
    with open(event_path, "r", encoding="utf-8") as f:
        return json.load(f)


def _get_repo_and_pr(event: Dict[str, Any]) -> tuple[str, int]:
    repo = os.environ.get("GITHUB_REPOSITORY")
    if not repo:
        raise RuntimeError("GITHUB_REPOSITORY not set")

    pr = event.get("pull_request")
    if pr and pr.get("number"):
        return repo, int(pr["number"])

    issue = event.get("issue")
    if issue and issue.get("pull_request") and issue.get("number"):
        return repo, int(issue["number"])

    raise RuntimeError("Could not determine PR number from event payload")


def _list_issue_comments(repo: str, pr_number: int) -> List[Dict[str, Any]]:
    raw = _run(["gh", "api", f"/repos/{repo}/issues/{pr_number}/comments?per_page=100"])
    data = json.loads(raw)
    if not isinstance(data, list):
        raise RuntimeError("Unexpected GitHub API response for issue comments")
    return data


def _find_existing_comment_id(comments: List[Dict[str, Any]]) -> int | None:
    for c in comments:
        body = c.get("body") or ""
        if COMMENT_MARKER in body:
            return int(c["id"])
    return None


def _is_permission_error(exc: RuntimeError) -> bool:
    msg = str(exc)
    return "Resource not accessible by integration" in msg or "HTTP 403" in msg


def main() -> int:
    if len(sys.argv) < 2:
        raise SystemExit("Usage: post_codex_gate_comment.py <comment_markdown_file>")

    md_path = sys.argv[1]
    with open(md_path, "r", encoding="utf-8") as f:
        body = f.read()

    event = _load_event()
    repo, pr_number = _get_repo_and_pr(event)
    comments = _list_issue_comments(repo, pr_number)

    final_body = f"{COMMENT_MARKER}\n{body}"
    existing_id = _find_existing_comment_id(comments)

    if existing_id is None:
        try:
            _run(
                [
                    "gh",
                    "api",
                    f"/repos/{repo}/issues/{pr_number}/comments",
                    "-f",
                    f"body={final_body}",
                ]
            )
            print("Created PR comment.")
        except RuntimeError as exc:
            if _is_permission_error(exc):
                print("Skipping PR comment: insufficient permissions (likely fork PR).")
                return 0
            raise
    else:
        try:
            _run(
                [
                    "gh",
                    "api",
                    f"/repos/{repo}/issues/comments/{existing_id}",
                    "-X",
                    "PATCH",
                    "-f",
                    f"body={final_body}",
                ]
            )
            print("Updated existing PR comment.")
        except RuntimeError as exc:
            if _is_permission_error(exc):
                print("Skipping PR comment update: insufficient permissions (likely fork PR).")
                return 0
            raise

    return 0


if __name__ == "__main__":
    raise SystemExit(main())