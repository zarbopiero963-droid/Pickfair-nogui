#!/usr/bin/env python3
"""Parse Codex review comments into the structured findings the bug gate scores.

OUTDATED DETECTION (fix): the gate treats `outdated` comments as NOISE
(`codex_bug_gate.py: _classify` / `_should_fail`), but the REST endpoint
`/pulls/{pr}/comments` does NOT return a boolean `outdated` field — so the
previous `comment.get("outdated", False)` was ALWAYS False and the gate counted
already-fixed (outdated) comments as REAL_BUG, producing false-red gates on
every iteration of a PR.

We now resolve the outdated/resolved status authoritatively via the GraphQL
`reviewThreads { isOutdated isResolved }` API (a comment whose thread is
outdated — its code changed — or resolved is not a live finding). If GraphQL is
unavailable we fall back to the REST comments endpoint and infer outdated from a
null `position` (GitHub nulls `position` once a comment no longer maps to the
current diff).
"""
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


# ---------------------------------------------------------------------------
# Fetch — GraphQL (authoritative isOutdated/isResolved) with REST fallback
# ---------------------------------------------------------------------------

# A review THREAD corresponds to a single finding location: its opening
# (first) comment IS the Codex finding; later comments are replies/discussion,
# never new findings. We therefore read only the first comment per thread —
# this also makes inner-comment pagination unnecessary (no finding can be
# truncated past a comment page) while paginating the threads themselves.
_REVIEW_THREADS_QUERY = (
    "query($owner:String!,$name:String!,$number:Int!,$after:String){"
    " repository(owner:$owner,name:$name){ pullRequest(number:$number){"
    " reviewThreads(first:100,after:$after){"
    " nodes{ isResolved isOutdated"
    " comments(first:1){ nodes{ databaseId author{login} path line originalLine body url } } }"
    " pageInfo{ hasNextPage endCursor } } } } }"
)


def _flatten_thread(thread: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Turn a GraphQL review thread into REST-shaped comment dicts.

    Each comment inherits the thread-level outdated/resolved status: a comment
    whose thread is outdated (code changed) or resolved is not a live finding,
    so the gate must not count it. Only the opening comment (the finding) is
    read per thread.
    """
    not_live = bool(thread.get("isOutdated")) or bool(thread.get("isResolved"))
    comments = ((thread.get("comments") or {}).get("nodes")) or []
    rows: List[Dict[str, Any]] = []
    for c in comments:
        if not isinstance(c, dict):
            continue
        rows.append(
            {
                "id": c.get("databaseId"),
                "user": {"login": ((c.get("author") or {}).get("login") or "")},
                "path": c.get("path") or "",
                "line": c.get("line"),
                "original_line": c.get("originalLine"),
                "body": c.get("body") or "",
                "html_url": c.get("url") or "",
                # Authoritative signal the rest of the parser consumes.
                "outdated": not_live,
            }
        )
    return rows


def _fetch_review_comments_graphql(repo: str, pr_number: int) -> List[Dict[str, Any]]:
    owner, name = str(repo).split("/", 1)
    comments: List[Dict[str, Any]] = []
    after: Optional[str] = None
    while True:
        cmd = [
            "gh", "api", "graphql",
            "-f", f"owner={owner}",
            "-f", f"name={name}",
            "-F", f"number={pr_number}",
            "-f", f"query={_REVIEW_THREADS_QUERY}",
        ]
        if after:
            cmd.extend(["-f", f"after={after}"])
        data = json.loads(_run(cmd))
        # GraphQL-over-HTTP can return 200 with `errors` and partial/omitted
        # data. Raising here (instead of silently returning the comments
        # collected so far) lets the REST fallback run, so a transient
        # resolver/permission/schema issue never makes the gate see zero
        # findings.
        if data.get("errors"):
            raise RuntimeError(f"GraphQL returned errors: {data.get('errors')}")
        threads = (
            ((data.get("data") or {}).get("repository") or {}).get("pullRequest") or {}
        ).get("reviewThreads")
        if not isinstance(threads, dict):
            raise RuntimeError("GraphQL response missing reviewThreads (partial/empty)")
        for node in threads.get("nodes") or []:
            if isinstance(node, dict):
                comments.extend(_flatten_thread(node))
        page = threads.get("pageInfo") or {}
        if page.get("hasNextPage") and page.get("endCursor"):
            after = page["endCursor"]
        else:
            break
    return comments


def _fetch_review_comments_rest(repo: str, pr_number: int) -> List[Dict[str, Any]]:
    # --paginate follows every page so the fallback is not capped at 100
    # comments. Without --slurp each page is emitted as a separate JSON array
    # (so json.loads would fail on >1 page); --slurp wraps the pages into a
    # single array of page-arrays which we then flatten.
    cmd = [
        "gh",
        "api",
        "--paginate",
        "--slurp",
        f"/repos/{repo}/pulls/{pr_number}/comments?per_page=100",
    ]
    data = json.loads(_run(cmd))
    if not isinstance(data, list):
        raise RuntimeError("Unexpected GitHub API response for review comments")
    comments: List[Dict[str, Any]] = []
    for item in data:
        if isinstance(item, list):  # a page (array of comments)
            comments.extend(c for c in item if isinstance(c, dict))
        elif isinstance(item, dict):  # already a flat comment
            comments.append(item)
    return comments


def _fetch_review_comments(repo: str, pr_number: int) -> List[Dict[str, Any]]:
    """Authoritative GraphQL fetch, degrading to REST if GraphQL is unavailable."""
    try:
        return _fetch_review_comments_graphql(repo, pr_number)
    except (RuntimeError, json.JSONDecodeError, OSError) as exc:  # pragma: no cover - network/credential dependent
        print(
            f"GraphQL review-thread fetch failed ({exc}); falling back to REST",
            file=sys.stderr,
        )
        return _fetch_review_comments_rest(repo, pr_number)


def _is_codex_comment(comment: Dict[str, Any]) -> bool:
    user = comment.get("user") or {}
    login = (user.get("login") or "").strip()
    return login in CODEX_AUTHORS or "codex" in login.lower()


def _is_outdated(comment: Dict[str, Any]) -> bool:
    """Whether a comment is outdated/resolved (i.e. NOT a live finding).

    Honours an explicit boolean from the GraphQL path. The REST fallback infers
    it from a null `position` (GitHub nulls it when a comment no longer maps to
    the current diff), but FAIL-CLOSED: a null position alone is not enough —
    file-level comments (`subject_type == "file"`) and comments still carrying a
    current `line`/`side` anchor are live despite a null position. Treating a
    live finding as outdated would let the gate drop a real unresolved bug, so
    when in doubt we keep it live.
    """
    for key in ("outdated", "isOutdated", "is_outdated"):
        value = comment.get(key)
        if isinstance(value, bool):
            return value
    if "position" not in comment:
        return False
    if comment.get("position") is not None:
        return False
    # Null position — only outdated if it is a genuinely stale line comment.
    if comment.get("subject_type") == "file":
        return False
    if comment.get("line") is not None or comment.get("side"):
        return False
    return True


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
        "outdated": _is_outdated(comment),
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
