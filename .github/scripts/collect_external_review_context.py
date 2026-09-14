#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


OUT_DIR = Path(".autofix/context")
OUT_DIR.mkdir(parents=True, exist_ok=True)

PR_NUMBER = os.environ.get("PR_NUMBER") or os.environ.get("GITHUB_EVENT_INPUTS_PR_NUMBER")
REPO_FULL = os.environ.get("GITHUB_REPOSITORY", "zarbopiero963-droid/Pickfair-nogui")

if "/" not in REPO_FULL:
    REPO_FULL = "zarbopiero963-droid/Pickfair-nogui"

OWNER, REPO = REPO_FULL.split("/", 1)

DEEPSOURCE_TOKEN = os.environ.get("DEEPSOURCE_API_TOKEN", "")


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def request_json(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    payload: dict[str, Any] | None = None,
    timeout: int = 45,
) -> dict[str, Any]:
    data = None
    method = "GET"

    final_headers = {
        "Accept": "application/json",
        "User-Agent": "pickfair-autofix-context/1.0",
    }

    if headers:
        final_headers.update(headers)

    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        method = "POST"
        final_headers["Content-Type"] = "application/json"

    req = urllib.request.Request(
        url,
        data=data,
        headers=final_headers,
        method=method,
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            body = response.read().decode("utf-8", errors="replace")
            return {
                "ok": True,
                "status": response.status,
                "body": json.loads(body) if body.strip() else {},
            }
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return {
            "ok": False,
            "status": exc.code,
            "error": body[:4000],
        }
    except Exception as exc:
        return {
            "ok": False,
            "status": None,
            "error": f"{type(exc).__name__}: {exc}",
        }


def compact_text(value: Any, limit: int = 500) -> str:
    text = "" if value is None else str(value)
    text = " ".join(text.split())
    return text[:limit]




def collect_deepsource() -> dict[str, Any]:
    if not PR_NUMBER:
        return {"ok": False, "error": "PR_NUMBER missing"}

    query = """
query($name: String!, $login: String!, $vcsProvider: VCSProvider!, $number: Int!) {
  repository(name: $name, login: $login, vcsProvider: $vcsProvider) {
    name
    pullRequest(number: $number) {
      title
      state
      summary {
        issuesRaised
        issuesResolved
      }
      latestAnalysisRun {
        status
      }
      issues(first: 100) {
        edges {
          node {
            path
            beginLine
            endLine
            beginColumn
            endColumn
            severity
            category
            title
            explanation
            shortcode
            isSuppressed
          }
        }
      }
    }
  }
}
"""

    payload = {
        "query": query,
        "variables": {
            "name": REPO,
            "login": OWNER,
            "vcsProvider": "GITHUB",
            "number": int(PR_NUMBER),
        },
    }

    headers: dict[str, str] = {}
    if DEEPSOURCE_TOKEN:
        headers["Authorization"] = f"Bearer {DEEPSOURCE_TOKEN}"

    result = request_json(
        "https://api.deepsource.com/graphql/",
        headers=headers,
        payload=payload,
    )
    result["token_configured"] = bool(DEEPSOURCE_TOKEN)
    result["url_template"] = "https://api.deepsource.com/graphql/"
    return result






def format_deepsource(result: dict[str, Any]) -> str:
    lines = [
        "## DeepSource API",
        f"- token_configured: {result.get('token_configured')}",
        f"- ok: {result.get('ok')}",
        f"- status: {result.get('status')}",
    ]

    if not result.get("ok"):
        lines.append(f"- error: {compact_text(result.get('error'))}")
        return "\n".join(lines) + "\n"

    body = result.get("body") if isinstance(result.get("body"), dict) else {}

    if body.get("errors"):
        lines.append(f"- graphql_errors: {compact_text(body.get('errors'))}")

    pr = (
        body.get("data", {})
        .get("repository", {})
        .get("pullRequest", {})
    )

    if pr:
        lines.append(f"- pr_state: {pr.get('state')}")
        lines.append(f"- latest_analysis_status: {pr.get('latestAnalysisRun', {}).get('status')}")
        lines.append(f"- summary: {pr.get('summary')}")

    edges = (
        pr.get("issues", {}).get("edges", [])
        if isinstance(pr, dict)
        else []
    )

    if not edges:
        lines.append("- issues: none returned or unavailable")
        return "\n".join(lines) + "\n"

    lines.append(f"- issues_returned: {len(edges)}")

    for idx, edge in enumerate(edges[:50], 1):
        node = edge.get("node", {}) if isinstance(edge, dict) else {}
        path = node.get("path") or "unknown"
        line = node.get("beginLine") or "unknown"
        severity = node.get("severity") or "unknown"
        category = node.get("category") or "unknown"
        shortcode = node.get("shortcode") or "unknown"
        title = node.get("title") or ""
        explanation = node.get("explanation") or ""

        lines.append(
            f"{idx}. {path}:{line} [{severity}/{category}] {shortcode} — "
            f"{compact_text(title or explanation)}"
        )

    return "\n".join(lines) + "\n"


def main() -> int:
    deepsource = collect_deepsource()

    write_json(OUT_DIR / "deepsource-pr-issues.json", deepsource)

    md = [
        "# External analyzer API context",
        "",
        f"Repo: {REPO_FULL}",
        f"PR: #{PR_NUMBER or 'unknown'}",
        "",
        format_deepsource(deepsource),
        "",
    ]

    (OUT_DIR / "external-review-context.md").write_text(
        "\n".join(md),
        encoding="utf-8",
    )

    print("External analyzer API context written:")
    print(f"- {OUT_DIR / 'deepsource-pr-issues.json'}")
    print(f"- {OUT_DIR / 'external-review-context.md'}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
