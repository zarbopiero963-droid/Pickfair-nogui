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
