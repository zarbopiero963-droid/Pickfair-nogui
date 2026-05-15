#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

OUT = Path(".autofix/context")
OUT.mkdir(parents=True, exist_ok=True)

PR_NUMBER = int(os.environ["PR_NUMBER"])
REPO_FULL = os.environ.get("GITHUB_REPOSITORY") or os.environ.get("REPO") or "zarbopiero963-droid/Pickfair-nogui"
OWNER, REPO_NAME = REPO_FULL.split("/", 1)

GH_TOKEN = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
CODACY_API_TOKEN = os.environ.get("CODACY_API_TOKEN", "")
DEEPSOURCE_API_TOKEN = os.environ.get("DEEPSOURCE_API_TOKEN", "")


def write_json(path: str, data: Any) -> None:
    Path(path).write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")


def request_json(url: str, *, method: str = "GET", headers: dict[str, str] | None = None, body: Any = None) -> tuple[int, Any]:
    payload = None
    req_headers = headers or {}
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
        req_headers = {"Content-Type": "application/json", **req_headers}

    req = urllib.request.Request(url, data=payload, method=method, headers=req_headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8", "replace")
            return resp.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", "replace")
        try:
            parsed = json.loads(raw)
        except Exception:
            parsed = {"error": raw}
        return exc.code, parsed


def gh_graphql(query: str, variables: dict[str, Any]) -> Any:
    if not GH_TOKEN:
        raise RuntimeError("GH_TOKEN/GITHUB_TOKEN missing")
    status, data = request_json(
        "https://api.github.com/graphql",
        method="POST",
        headers={
            "Authorization": f"Bearer {GH_TOKEN}",
            "Accept": "application/vnd.github+json",
        },
        body={"query": query, "variables": variables},
    )
    if status >= 300 or data.get("errors"):
        raise RuntimeError(f"GitHub GraphQL failed: status={status} body={data}")
    return data


def normalize_check(node: dict[str, Any]) -> dict[str, str]:
    typename = node.get("__typename", "")
    if typename == "CheckRun":
        return {
            "name": node.get("name") or "",
            "state": node.get("conclusion") or node.get("status") or "",
            "url": node.get("detailsUrl") or "",
            "source": "github_check_run",
        }
    if typename == "StatusContext":
        return {
            "name": node.get("context") or "",
            "state": node.get("state") or "",
            "url": node.get("targetUrl") or "",
            "source": "github_status_context",
        }
    return {
        "name": node.get("name") or node.get("context") or "",
        "state": node.get("conclusion") or node.get("state") or node.get("status") or "",
        "url": node.get("detailsUrl") or node.get("targetUrl") or "",
        "source": typename or "unknown",
    }


def is_red(state: str) -> bool:
    return state in {"FAILURE", "ERROR", "ACTION_REQUIRED", "TIMED_OUT", "STARTUP_FAILURE"}


def is_pending(state: str) -> bool:
    return state in {"", "PENDING", "QUEUED", "IN_PROGRESS", "REQUESTED", "WAITING", "EXPECTED"}


def collect_github() -> dict[str, Any]:
    query = """
    query($owner:String!, $repo:String!, $number:Int!) {
      repository(owner:$owner, name:$repo) {
        pullRequest(number:$number) {
          number
          title
          url
          headRefName
          headRefOid
          baseRefName
          mergeStateStatus
          isDraft
          comments(last:50) {
            nodes {
              author { login }
              createdAt
              url
              body
            }
          }
          reviews(last:50) {
            nodes {
              author { login }
              createdAt
              url
              state
              body
            }
          }
          files(first:100) {
            nodes {
              path
              additions
              deletions
              changeType
            }
          }
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
          statusCheckRollup {
            contexts(first:100) {
              nodes {
                __typename
                ... on CheckRun {
                  name
                  status
                  conclusion
                  detailsUrl
                }
                ... on StatusContext {
                  context
                  state
                  targetUrl
                }
              }
            }
          }
        }
      }
    }
    """
    data = gh_graphql(query, {"owner": OWNER, "repo": REPO_NAME, "number": PR_NUMBER})
    pr = data["data"]["repository"]["pullRequest"]
    rollup = pr.get("statusCheckRollup") or {}
    contexts = rollup.get("contexts") or {}
    checks = [normalize_check(n) for n in contexts.get("nodes", [])]
    blockers = [c for c in checks if is_red(c["state"])]
    pending = [c for c in checks if is_pending(c["state"])]
    pr["normalizedChecks"] = checks
    pr["blockers"] = blockers
    pr["pending"] = pending
    return pr


def collect_codacy(blockers: list[dict[str, str]]) -> tuple[list[dict[str, Any]], str | None]:
    if not any("codacy" in (b["name"] + " " + b["url"]).lower() for b in blockers):
        return [], None

    if not CODACY_API_TOKEN:
        return [], "CODACY_API_TOKEN missing while Codacy is blocking"

    url = (
        "https://api.codacy.com/api/v3/analysis/organizations/gh/"
        f"{urllib.parse.quote(OWNER)}/repositories/{urllib.parse.quote(REPO_NAME)}"
        f"/pull-requests/{PR_NUMBER}/issues?status=new&limit=100"
    )

    status, data = request_json(url, headers={"api-token": CODACY_API_TOKEN})
    write_json(str(OUT / "codacy-raw.json"), data)

    if status >= 300:
        return [], f"Codacy API failed status={status}"

    issues: list[dict[str, Any]] = []
    for item in data.get("data", []):
        issue = item.get("commitIssue") or {}
        pattern = issue.get("patternInfo") or {}
        tool = issue.get("toolInfo") or {}
        path = issue.get("filePath") or ""
        rule = pattern.get("id") or ""
        message = issue.get("message") or ""
        level = pattern.get("severityLevel") or pattern.get("level") or ""

        risky_info = (
            tool.get("name") == "Lizard"
            and level.lower() == "info"
            and any(x in rule.lower() for x in ["nloc", "ccn", "parameter-count"])
        )

        issues.append({
            "source": "codacy",
            "file": path,
            "line": issue.get("lineNumber"),
            "rule": rule,
            "tool": tool.get("name") or "",
            "level": level,
            "message": message,
            "deltaType": item.get("deltaType"),
            "safe_autofix": not risky_info,
            "requires_manual_or_config": risky_info,
        })

    return issues, None


def collect_deepsource(blockers: list[dict[str, str]]) -> tuple[list[dict[str, Any]], str | None]:
    if not any("deepsource" in (b["name"] + " " + b["url"]).lower() for b in blockers):
        return [], None

    if not DEEPSOURCE_API_TOKEN:
        return [], "DEEPSOURCE_API_TOKEN missing while DeepSource is blocking"

    query = """
    query($name: String!, $login: String!, $vcsProvider: VCSProvider!, $number: Int!) {
      repository(name: $name, login: $login, vcsProvider: $vcsProvider) {
        pullRequest(number: $number) {
          title
          state
          summary { issuesRaised issuesResolved }
          latestAnalysisRun { status }
        }
      }
    }
    """
    status, data = request_json(
        "https://api.deepsource.com/graphql/",
        method="POST",
        headers={"Authorization": f"Bearer {DEEPSOURCE_API_TOKEN}"},
        body={
            "query": query,
            "variables": {
                "name": REPO_NAME,
                "login": OWNER,
                "vcsProvider": "GITHUB",
                "number": PR_NUMBER,
            },
        },
    )
    write_json(str(OUT / "deepsource-raw.json"), data)

    if status >= 300 or data.get("errors"):
        return [], f"DeepSource API failed status={status}"

    return [{
        "source": "deepsource",
        "file": "",
        "line": None,
        "rule": "deepsource-summary",
        "tool": "DeepSource",
        "level": "unknown",
        "message": json.dumps(data.get("data", {}), sort_keys=True),
        "safe_autofix": True,
        "requires_manual_or_config": False,
    }], None


def main() -> int:
    errors: list[str] = []
    pr = collect_github()
    write_json(str(OUT / "github-pr.json"), pr)

    checks = pr["normalizedChecks"]
    blockers = pr["blockers"]
    pending = pr["pending"]

    codacy_issues, codacy_error = collect_codacy(blockers)
    if codacy_error:
        errors.append(codacy_error)

    ds_issues, ds_error = collect_deepsource(blockers)
    if ds_error:
        errors.append(ds_error)

    review_threads = []
    for thread in pr.get("reviewThreads", {}).get("nodes", []):
        if thread.get("isResolved"):
            continue
        review_threads.append({
            "source": "github_review_thread",
            "file": thread.get("path") or "",
            "line": thread.get("line"),
            "rule": "review-thread",
            "tool": "GitHub",
            "level": "review",
            "message": "\n\n".join((c.get("body") or "") for c in thread.get("comments", {}).get("nodes", [])),
            "safe_autofix": True,
            "requires_manual_or_config": False,
        })

    all_issues = codacy_issues + ds_issues + review_threads

    risky_files = sorted({
        i["file"] for i in all_issues
        if i.get("file") and i.get("requires_manual_or_config") is True
    })
    risky_file_set = set(risky_files)

    safe_candidate_files = {
        i["file"] for i in all_issues
        if i.get("file") and i.get("safe_autofix") is True
    }

    # Safety rule:
    # if any analyzer marks a file as risky/manual/config-only,
    # the file must not be sent to Codex even if an older review thread also mentions it.
    safe_files = sorted(safe_candidate_files - risky_file_set)

    (OUT / "issues.jsonl").write_text(
        "".join(json.dumps(i, sort_keys=True) + "\n" for i in all_issues),
        encoding="utf-8",
    )
    (OUT / "allowed-files.txt").write_text(
        "".join(f + "\n" for f in safe_files),
        encoding="utf-8",
    )

    codacy_blocking = any(
        "codacy" in ((b.get("name") or "") + " " + (b.get("url") or "")).lower()
        for b in blockers
    )
    codacy_safe_count = sum(1 for i in codacy_issues if i.get("safe_autofix") is True)
    codacy_risky_count = sum(1 for i in codacy_issues if i.get("requires_manual_or_config") is True)
    codacy_manual_only = codacy_blocking and bool(codacy_issues) and codacy_safe_count == 0 and codacy_risky_count > 0

    manual_reasons: list[str] = []
    if codacy_manual_only:
        manual_reasons.append("Codacy is blocking, but all Codacy API issues are risky/manual/config-only.")
    if bool(blockers) and not pending and not safe_files:
        manual_reasons.append("No safe allowed files remain after excluding risky/manual/config-only files.")

    fatal_context_error = bool(errors)
    requires_manual = bool(blockers) and not pending and bool(manual_reasons)
    should_fix = (
        bool(blockers)
        and not pending
        and bool(safe_files)
        and not fatal_context_error
        and not requires_manual
    )

    summary = {
        "head": pr.get("headRefOid"),
        "branch": pr.get("headRefName"),
        "pending_count": len(pending),
        "blocker_count": len(blockers),
        "blockers": blockers,
        "safe_files": safe_files,
        "risky_files": risky_files,
        "codacy_blocking": codacy_blocking,
        "codacy_safe_count": codacy_safe_count,
        "codacy_risky_count": codacy_risky_count,
        "manual_reasons": manual_reasons,
        "errors": errors,
    }
    write_json(str(OUT / "decision.json"), summary)

    task = [
        f"# Safe autofix handoff for PR #{PR_NUMBER}",
        "",
        f"Repository: {REPO_FULL}",
        f"Branch: {pr.get('headRefName')}",
        f"Head SHA: {pr.get('headRefOid')}",
        "",
        "## Blocking checks",
        *[
            f"- {b['name']} [{b['state']}] {b['url']}"
            for b in blockers
        ],
        "",
        "## Normalized issues",
        *[
            f"- {i['source']} {i.get('file')}:{i.get('line')} {i.get('rule')} {i.get('message')} safe_autofix={i.get('safe_autofix')}"
            for i in all_issues
        ],
        "",
        "## Allowed files",
        *[f"- {f}" for f in safe_files],
        "",
        "## Risky/manual/config-only files",
        *[f"- {f}" for f in risky_files],
        "",
        "## Hard rules for Codex",
        "- Do not edit files outside Allowed files.",
        "- Do not refactor files only listed as risky/manual/config-only.",
        "- If only Lizard Info complexity issues remain, stop and report configuration/manual decision needed.",
        "- Make the smallest possible patch.",
        "- Do not create empty retrigger commits.",
        "- Do not merge or create PRs.",
    ]
    (OUT / "codex-task.md").write_text("\n".join(task) + "\n", encoding="utf-8")

    (OUT / "decision.env").write_text(
        "\n".join([
            f"HEAD_SHA={pr.get('headRefOid')}",
            f"BRANCH={pr.get('headRefName')}",
            f"PENDING_COUNT={len(pending)}",
            f"BLOCKER_COUNT={len(blockers)}",
            f"SAFE_FILE_COUNT={len(safe_files)}",
            f"SHOULD_FIX={'true' if should_fix else 'false'}",
            f"REQUIRES_MANUAL={'true' if requires_manual else 'false'}",
            f"FATAL_CONTEXT_ERROR={'true' if fatal_context_error else 'false'}",
        ]) + "\n",
        encoding="utf-8",
    )

    print(json.dumps(summary, indent=2, sort_keys=True))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
