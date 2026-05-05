from __future__ import annotations

import json
import os
import re
from urllib.parse import urlparse, urlunparse
import urllib.request
from typing import Any


GITHUB_API = "https://api.github.com"
_GITHUB_API_URL = urlparse(GITHUB_API)
_REPO_PART_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")
_REPO_SLUG_PATTERN = re.compile(r"^([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)$")


def gh_request(
    method: str,
    url: str,
    token: str,
    data: dict[str, Any] | None = None,
) -> Any:
    body = None
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "codex-pr-automation",
    }

    if data is not None:
        body = json.dumps(data).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    with urllib.request.urlopen(req) as resp:
        payload = resp.read().decode("utf-8")
        return json.loads(payload) if payload else None


def _validate_repo_slug(repo: str) -> tuple[str, str]:
    match = _REPO_SLUG_PATTERN.fullmatch(repo)
    if not match:
        raise ValueError("GITHUB_REPOSITORY must be in owner/repo format")
    return match.group(1), match.group(2)


def _validate_issue_number(value: str) -> int:
    value_text = str(value).strip()
    if not value_text.isdecimal():
        raise ValueError("PR_NUMBER must be a positive integer")
    issue_number = int(value_text)
    if issue_number <= 0:
        raise ValueError("PR_NUMBER must be a positive integer")
    return issue_number


def _build_github_comments_url(owner: str, repo: str, issue_number: int) -> str:
    """Build a comments URL with defensive re-validation for standalone-safe use."""
    if not _REPO_PART_PATTERN.fullmatch(owner) or not _REPO_PART_PATTERN.fullmatch(repo):
        raise ValueError("owner/repo contains invalid characters")
    if issue_number <= 0:
        raise ValueError("issue_number must be a positive integer")
    path = f"/repos/{owner}/{repo}/issues/{issue_number}/comments"
    return urlunparse((_GITHUB_API_URL.scheme, _GITHUB_API_URL.netloc, path, "", "", ""))


def build_comment(mode: str) -> str:
    if mode == "ci_failure":
        return """<!-- codex-auto:ci-failure -->
@codex Continue on the SAME open PR.

Rules:
- Do not open a new PR
- Fix only what is needed to make the required tests pass
- Keep scope limited to the active task
- Return exact tests run and exact files changed
"""
    if mode == "base_conflict":
        return """<!-- codex-auto:base-conflict -->
@codex The active PR has conflict with the base branch.

Rules:
- Resolve it on the SAME branch and SAME PR
- Do not open a new PR
- Re-run the required tests after resolution
- Return exact files touched during conflict resolution
"""
    raise ValueError(f"Unsupported mode: {mode}")


def already_commented(
    owner: str,
    repo: str,
    pr_number: int,
    token: str,
    marker: str,
) -> bool:
    url = f"{_build_github_comments_url(owner, repo, pr_number)}?per_page=100"
    comments = gh_request("GET", url, token)
    assert isinstance(comments, list)

    for comment in reversed(comments):
        body = comment.get("body", "")
        if marker in body:
            return True
    return False


def main() -> int:
    token = os.environ["GITHUB_TOKEN"]
    repository = os.environ["GITHUB_REPOSITORY"]
    pr_number = _validate_issue_number(os.environ["PR_NUMBER"])
    mode = os.environ["MODE"].strip()

    owner, repo = _validate_repo_slug(repository)
    marker = (
        "<!-- codex-auto:ci-failure -->"
        if mode == "ci_failure"
        else "<!-- codex-auto:base-conflict -->"
    )

    if already_commented(owner, repo, pr_number, token, marker):
        print(f"Comment already exists for PR #{pr_number} and mode={mode}")
        return 0

    comment_body = build_comment(mode)
    url = _build_github_comments_url(owner, repo, pr_number)
    gh_request("POST", url, token, {"body": comment_body})
    print(f"Posted {mode} comment on PR #{pr_number}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
