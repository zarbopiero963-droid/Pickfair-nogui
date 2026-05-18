"""Tests for PR flow automation decisions."""

import argparse

import scripts.pr_flow_automation as flow


def _check(name: str, state: str, url: str = "") -> dict[str, str]:
    return {"name": name, "conclusion": state, "detailsUrl": url}


def test_preflight_does_not_apply_safe_autofix_limit_when_codacy_api_is_clear(monkeypatch):
    """Safe-autofix history is informational once Codacy is clear."""
    monkeypatch.delenv("HAS_CODACY_API_TOKEN", raising=False)
    args = argparse.Namespace(max_safe_autofix_commits=3)

    issues = flow.preflight_issues(
        args,
        codacy_blocking=False,
        codacy_api_available=True,
        commits=["a", "b", "c", "d"],
        oscillating=[{"file": "scripts/pr_flow_automation.py", "touches": 4}],
    )

    assert not issues


def test_preflight_preserves_safe_autofix_limit_when_codacy_api_is_blocking(monkeypatch):
    """Codacy blockers still enforce the safe-autofix commit limit."""
    monkeypatch.setenv("HAS_CODACY_API_TOKEN", "true")
    args = argparse.Namespace(max_safe_autofix_commits=3)

    issues = flow.preflight_issues(
        args,
        codacy_blocking=True,
        codacy_api_available=True,
        commits=["a", "b", "c", "d"],
        oscillating=[],
    )

    assert issues == ["safe autofix commit limit exceeded: 4 > 3"]


def test_preflight_allows_clean_scope_execute_when_safe_autofix_limit_exceeded(monkeypatch):
    """Clean-scope execute mode can take over after the safe-autofix limit."""
    monkeypatch.setenv("HAS_CODACY_API_TOKEN", "true")
    args = argparse.Namespace(
        max_safe_autofix_commits=3,
        clean_scope_rebuild_mode="execute",
        allow_controller_dispatch=False,
    )

    issues = flow.preflight_issues(
        args,
        codacy_blocking=True,
        codacy_api_available=True,
        commits=["a", "b", "c", "d"],
        oscillating=[{"file": "scripts/pr_clean_scope_rebuild.py", "touches": 9}],
    )

    assert not issues


def test_preflight_allows_controller_dispatch_when_safe_autofix_limit_exceeded(monkeypatch):
    """Explicit controller dispatch can proceed past the safe-autofix limit."""
    monkeypatch.setenv("HAS_CODACY_API_TOKEN", "true")
    args = argparse.Namespace(
        max_safe_autofix_commits=3,
        clean_scope_rebuild_mode="disabled",
        allow_controller_dispatch=True,
    )

    issues = flow.preflight_issues(
        args,
        codacy_blocking=True,
        codacy_api_available=True,
        commits=["a", "b", "c", "d"],
        oscillating=[{"file": "scripts/pr_clean_scope_rebuild.py", "touches": 9}],
    )

    assert not issues


def test_codacy_task_normalizes_common_issue_fields(tmp_path, monkeypatch):
    """Codacy API output is persisted raw and rendered into a concise task file."""
    monkeypatch.setenv("CODACY_API_TOKEN", "token")
    monkeypatch.setattr(
        flow,
        "pr_view",
        lambda _repo, _pr: {
            "statusCheckRollup": [
                _check("Codacy Static Code Analysis", "ACTION_REQUIRED", "https://api.codacy.com/check")
            ]
        },
    )
    monkeypatch.setattr(
        flow,
        "fetch_codacy_issues",
        lambda _url, _token: (
            200,
            {
                "data": [
                    {
                        "filePath": "scripts/pr_flow_automation.py",
                        "lineNumber": 42,
                        "patternId": "PY001",
                        "tool": {"name": "ruff"},
                        "severity": "Medium",
                        "message": "Example issue",
                    }
                ]
            },
        ),
    )
    args = argparse.Namespace(repo="owner/repo", pr="225", outdir=str(tmp_path))

    assert flow.cmd_codacy_task(args) == 0

    assert (tmp_path / "codacy-raw.json").exists()
    task = (tmp_path / "codacy-task.md").read_text(encoding="utf-8")
    assert "scripts/pr_flow_automation.py" in task
    assert "PY001 / ruff" in task
    assert "Example issue" in task


def test_codacy_task_fails_closed_when_api_fetch_fails(tmp_path, monkeypatch):
    """An ACTION_REQUIRED Codacy check must not become an empty task on API failure."""
    monkeypatch.setenv("CODACY_API_TOKEN", "token")
    monkeypatch.setattr(
        flow,
        "pr_view",
        lambda _repo, _pr: {
            "statusCheckRollup": [
                _check("Codacy Static Code Analysis", "ACTION_REQUIRED", "https://api.codacy.com/check")
            ]
        },
    )

    def fail_fetch(_url, _token):
        raise RuntimeError("boom")

    monkeypatch.setattr(flow, "fetch_codacy_issues", fail_fetch)
    args = argparse.Namespace(repo="owner/repo", pr="225", outdir=str(tmp_path))

    assert flow.cmd_codacy_task(args) == 1
    task = (tmp_path / "codacy-task.md").read_text(encoding="utf-8")
    assert "Codacy API fetch failed closed" in task


def test_merge_readiness_ignores_stale_codacy_action_required_when_api_is_clear(monkeypatch):
    """A stale Codacy check does not block readiness when the API has no issues."""
    monkeypatch.setenv("CODACY_API_TOKEN", "token")
    monkeypatch.setattr(
        flow,
        "pr_view",
        lambda _repo, _pr: {
            "state": "OPEN",
            "isDraft": False,
            "mergeable": "MERGEABLE",
            "mergeStateStatus": "BLOCKED",
            "headRefOid": "abc123",
            "headRefName": "branch",
            "baseRefName": "main",
            "statusCheckRollup": [
                _check(
                    "Codacy Static Code Analysis",
                    "ACTION_REQUIRED",
                    "https://app.codacy.com/gh/owner/repo/pull-requests/225",
                )
            ],
        },
    )
    monkeypatch.setattr(flow, "fetch_codacy_issues", lambda _url, _token: (200, {"data": []}))

    decision = flow.build_decision("owner/repo", "225", ignore_self=True)

    assert decision["can_merge"] is True
    assert not decision["blockers"]
    assert decision["ignored_codacy_checks"][0]["name"] == "Codacy Static Code Analysis"
    assert decision["codacy"]["blocking"] is False


def test_merge_readiness_preserves_codacy_action_required_when_api_has_issues(monkeypatch):
    """Current Codacy issues keep ACTION_REQUIRED checks blocking."""
    monkeypatch.setenv("CODACY_API_TOKEN", "token")
    monkeypatch.setattr(
        flow,
        "pr_view",
        lambda _repo, _pr: {
            "state": "OPEN",
            "isDraft": False,
            "mergeable": "MERGEABLE",
            "mergeStateStatus": "UNSTABLE",
            "headRefOid": "abc123",
            "headRefName": "branch",
            "baseRefName": "main",
            "statusCheckRollup": [
                _check(
                    "Codacy Static Code Analysis",
                    "ACTION_REQUIRED",
                    "https://app.codacy.com/gh/owner/repo/pull-requests/225",
                )
            ],
        },
    )
    monkeypatch.setattr(
        flow,
        "fetch_codacy_issues",
        lambda _url, _token: (200, {"data": [{"filename": "scripts/pr_flow_automation.py"}]}),
    )

    decision = flow.build_decision("owner/repo", "225", ignore_self=True)

    assert decision["can_merge"] is False
    assert decision["blockers"][0]["name"] == "Codacy Static Code Analysis"
    assert decision["codacy"]["blocking"] is True
