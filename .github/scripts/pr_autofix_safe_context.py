#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import subprocess
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



def is_self_autofix_check(check: dict[str, str]) -> bool:
    """Return True for this workflow's own PR status check.

    The safe supervisor must not wait on, or be blocked by, its own check run.
    Otherwise a pull_request-triggered run can wait for itself and keep the PR
    UNSTABLE even when all real checks are green.
    """
    name = (check.get("name") or check.get("context") or "").strip().lower()
    return name in {
        "safe pr autofix",
            "autofix pr until checks are green",
        "pr autofix safe supervisor",
        "safe-autofix",
    }

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
    decision_checks = [c for c in checks if not is_self_autofix_check(c)]
    ignored_self_autofix_checks = [c for c in checks if is_self_autofix_check(c)]
    blockers = [c for c in decision_checks if is_red(c["state"])]
    pending = [c for c in decision_checks if is_pending(c["state"])]
    pr["normalizedChecks"] = checks
    pr["decisionChecks"] = decision_checks
    pr["ignoredSelfAutofixChecks"] = ignored_self_autofix_checks
    pr["blockers"] = blockers
    pr["pending"] = pending
    return pr


def _compact_text(value: Any, limit: int = 300) -> str:
    text = " ".join(str(value or "").split())
    return text[:limit]






















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



def _extract_github_run_id(url: str) -> str:
    match = re.search(r"/actions/runs/(\d+)", str(url or ""))
    return match.group(1) if match else ""


def _ultra_compact_text(value: Any, limit: int = 1800) -> str:
    text = "\n".join(line.rstrip() for line in str(value or "").splitlines())
    text = text.strip()
    return text[-limit:] if len(text) > limit else text


def _ultra_candidate_file(log_text: str) -> tuple[str, int | None]:
    pattern = re.compile(
        r"\b((?:tests|core|services|controllers|observability|recovery|ui_panels|ai|ops|sql)/"
        r"[A-Za-z0-9_./-]+\.(?:py|sql|md|yml|yaml|json|toml|ini|txt|sh)|"
        r"[A-Za-z0-9_./-]+\.py)(?::(\d+))?"
    )
    for match in pattern.finditer(log_text or ""):
        path = match.group(1)
        if path.startswith((".github/", "scripts/", ".guardrails/", "guardrails/")):
            continue
        line_raw = match.group(2)
        try:
            line = int(line_raw) if line_raw else None
        except Exception:
            line = None
        return path, line
    return "", None


def _ultra_is_risky_file(path: str) -> bool:
    return (
        not path
        or path.startswith((".github/", "scripts/", ".guardrails/", "guardrails/"))
        or "/.github/" in path
        or "/scripts/" in path
    )


def collect_ultra_check_issues(blockers: list[dict[str, str]]) -> list[dict[str, Any]]:
    targets = [
        b for b in blockers
        if "ultra-check" in ((b.get("name") or "") + " " + (b.get("url") or "")).lower()
    ]
    if not targets:
        return []

    repo_full = os.environ.get("REPO") or f"{OWNER}/{REPO_NAME}"
    issues: list[dict[str, Any]] = []
    seen: set[str] = set()

    for blocker in targets:
        run_id = _extract_github_run_id(blocker.get("url", ""))
        if not run_id or run_id in seen:
            continue
        seen.add(run_id)

        try:
            log = subprocess.check_output(
                ["gh", "run", "view", run_id, "--repo", repo_full, "--log"],
                text=True,
                stderr=subprocess.STDOUT,
                timeout=120,
            )
        except Exception as exc:
            log = f"Could not fetch ultra-check log for run {run_id}: {exc}"

        interesting_re = re.compile(
            r"FAILED|FAILURES|ERROR|AssertionError|Traceback|pytest|ruff|mypy|flake|lint|guard|ultra|exit code",
            re.IGNORECASE,
        )
        interesting = [line for line in log.splitlines() if interesting_re.search(line)]
        excerpt = "\n".join(interesting[-90:]) if interesting else "\n".join(log.splitlines()[-120:])
        excerpt = _ultra_compact_text(excerpt, 2200)

        file_path, line = _ultra_candidate_file(excerpt or log)
        risky = _ultra_is_risky_file(file_path)

        issue = {
            "source": "github_action_ultra_check",
            "file": file_path or "__ultra_check_log__",
            "line": line,
            "rule": "ultra_check_failure",
            "tool": "GitHub Actions ultra-check",
            "level": "failure",
            "message": _ultra_compact_text(excerpt, 1200),
            "safe_autofix": not risky,
            "requires_manual_or_config": risky,
        }

        if risky:
            issue["codex_fix_instruction"] = (
                "The GitHub Actions run / ultra-check failed, but the failing file could not be mapped "
                "to a safe source/test file. Do not guess. Inspect the ultra-check log and stop if the "
                "fix would require workflows, scripts, guardrail JSON, or unrelated files."
            )
        else:
            issue["codex_fix_instruction"] = (
                f"GitHub Actions run / ultra-check failed and points to {file_path}:{line}. "
                "Inspect the reported file and failure log, verify the failure is still valid, then make "
                "the smallest safe source/test change. Do not edit workflows, scripts, guardrail JSON, "
                "or unrelated files. Relevant ultra-check excerpt: "
                f"{excerpt!r}"
            )

        issues.append(issue)

    return issues

def main() -> int:
    errors: list[str] = []
    pr = collect_github()
    write_json(str(OUT / "github-pr.json"), pr)

    checks = pr["normalizedChecks"]
    blockers = pr["blockers"]
    pending = pr["pending"]

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

    ultra_issues = collect_ultra_check_issues(blockers)
    all_issues = ds_issues + review_threads + ultra_issues

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

    manual_reasons: list[str] = []
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
        "ignored_self_autofix_checks": pr.get("ignoredSelfAutofixChecks", []),
        "safe_files": safe_files,
        "risky_files": risky_files,
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
