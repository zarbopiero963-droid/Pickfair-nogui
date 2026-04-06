#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from typing import Any, Dict, List, Optional


CODEX_AUTHORS = {
    "chatgpt-codex-connector[bot]",
    "chatgpt-codex-connector",
    "openai-codex[bot]",
}


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


def _fetch_review_comments(repo: str, pr_number: int) -> List[Dict[str, Any]]:
    cmd = [
        "gh",
        "api",
        f"/repos/{repo}/pulls/{pr_number}/comments?per_page=100",
    ]
    raw = _run(cmd)
    data = json.loads(raw)
    if not isinstance(data, list):
        raise RuntimeError("Unexpected GitHub API response for review comments")
    return data


def _is_codex_comment(comment: Dict[str, Any]) -> bool:
    user = comment.get("user") or {}
    login = (user.get("login") or "").strip()
    return login in CODEX_AUTHORS or "codex" in login.lower()


def _normalize_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _extract_bug_class(body: str) -> str:
    b = body.lower()

    mapping = [
        ("state_persistence", ["persist", "save", "reload", "restart", "silently dropped", "writes", "read"]),
        ("filename_collision", ["same second", "overwrites", "filename", "collision", "path"]),
        ("stale_state_false_positive", ["stale", "recent", "snapshot", "timestamp", "row exists"]),
        ("data_loss", ["discard", "lost", "drops", "overwrite", "silently"]),
        ("time_window_bug", ["window", "freshness", "timestamp", "recent"]),
        ("contract_mismatch", ["contract", "mismatch", "invariant"]),
        ("alert_lifecycle_bug", ["alert", "resolve", "reopen", "incident"]),
        ("null_handling_bug", ["none", "null", "missing", "invalid"]),
        ("recovery_gap", ["recovery", "restart", "reconcile"]),
        ("dedup_break", ["dedup", "duplicate"]),
        ("idempotency_break", ["idempotent", "idempotency"]),
        ("state_machine_violation", ["state machine", "transition", "illegal state"]),
    ]

    for cls, keywords in mapping:
        if any(k in b for k in keywords):
            return cls

    return "unknown"


def _extract_title(body: str) -> str:
    lines = [x.strip() for x in (body or "").splitlines() if x.strip()]
    if not lines:
        return "Untitled finding"

    for line in lines[:5]:
        if "badge" in line.lower():
            continue
        return line[:200]

    return lines[0][:200]


def _extract_claim(body: str) -> str:
    text = _normalize_whitespace(body)
    return text[:500]


def _find_line(comment: Dict[str, Any]) -> Optional[int]:
    for key in ("line", "original_line", "start_line", "position", "original_position"):
        value = comment.get(key)
        if isinstance(value, int):
            return value
    return None


def _make_finding(comment: Dict[str, Any]) -> Dict[str, Any]:
    body = comment.get("body") or ""
    return {
        "id": f"review-comment-{comment.get('id')}",
        "source": "github_review_comment",
        "author": ((comment.get("user") or {}).get("login") or ""),
        "path": comment.get("path") or "",
        "line": _find_line(comment),
        "side": comment.get("side"),
        "outdated": bool(comment.get("outdated", False)),
        "url": comment.get("html_url") or "",
        "title": _extract_title(body),
        "claim": _extract_claim(body),
        "bug_class": _extract_bug_class(body),
        "body": body,
    }


def main() -> int:
    event = _load_event()
    repo, pr_number = _get_repo_and_pr(event)
    comments = _fetch_review_comments(repo, pr_number)

    findings: List[Dict[str, Any]] = []
    for c in comments:
        if _is_codex_comment(c):
            findings.append(_make_finding(c))

    output = {
        "repo": repo,
        "pr_number": pr_number,
        "count": len(findings),
        "findings": findings,
    }

    out_path = sys.argv[1] if len(sys.argv) > 1 else "codex_findings.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"Wrote {len(findings)} Codex findings to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())