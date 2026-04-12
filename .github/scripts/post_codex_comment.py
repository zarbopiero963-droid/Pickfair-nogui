from __future__ import annotations

import json
import os
import sys
import urllib.request
from typing import Any


GITHUB_API = "https://api.github.com"


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
    url = f"{GITHUB_API}/repos/{owner}/{repo}/issues/{pr_number}/comments?per_page=100"
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
    pr_number = int(os.environ["PR_NUMBER"])
    mode = os.environ["MODE"].strip()

    owner, repo = repository.split("/", 1)
    marker = (
        "<!-- codex-auto:ci-failure -->"
        if mode == "ci_failure"
        else "<!-- codex-auto:base-conflict -->"
    )

    if already_commented(owner, repo, pr_number, token, marker):
        print(f"Comment already exists for PR #{pr_number} and mode={mode}")
        return 0

    comment_body = build_comment(mode)
    url = f"{GITHUB_API}/repos/{owner}/{repo}/issues/{pr_number}/comments"
    gh_request("POST", url, token, {"body": comment_body})
    print(f"Posted {mode} comment on PR #{pr_number}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())