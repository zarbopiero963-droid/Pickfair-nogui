"""Tests for PR automation controller decisions."""

import argparse

import scripts.pr_automation_controller as controller


def _check(name: str, state: str, url: str = "") -> dict[str, str]:
    return {"name": name, "conclusion": state, "detailsUrl": url}


def _args() -> argparse.Namespace:
    return argparse.Namespace(
        repo="owner/repo",
        pr="225",
        dry_run=True,
        clean_scope_rebuild=False,
        clean_scope_rebuild_mode="disabled",
        clean_scope_commit_limit=3,
        clean_scope_allowlist="",
        clean_scope_forbidden="",
        safe_max_rounds="1",
        safe_pending_wait_seconds="600",
    )


def test_controller_does_not_launch_safe_autofix_for_stale_codacy_action_required(monkeypatch):
    """A stale Codacy ACTION_REQUIRED check is ignored once the Codacy API is clear."""
    monkeypatch.setattr(
        controller.flow,
        "codacy_blocking_evidence",
        lambda _repo, _pr, _blockers: {
            "blocking": False,
            "ignored": True,
            "api_available": True,
            "api_ok": True,
            "issues_returned": 0,
            "checks": _blockers,
        },
    )
    monkeypatch.setattr(controller, "active_safe_autofix_runs", lambda _repo: [])
    decision: dict = {"actions": [], "warnings": [], "errors": []}
    pr = {
        "statusCheckRollup": [
            _check(
                "Codacy Static Code Analysis",
                "ACTION_REQUIRED",
                "https://app.codacy.com/gh/owner/repo/pull-requests/225",
            )
        ]
    }

    ctx = controller.build_next_action_context(_args(), decision, pr, ([], []))
    controller.decide_next_action(ctx)

    assert decision["next_action"] == "checks_green_or_no_action"
    assert decision["launchable_for_safe_autofix"] == []
    assert decision["blockers"] == []
    assert decision["ignored_codacy_checks"][0]["name"] == "Codacy Static Code Analysis"


def test_controller_preserves_safe_autofix_launch_for_current_codacy_blocker(monkeypatch):
    """Current Codacy blockers remain launchable for safe autofix."""
    monkeypatch.setattr(
        controller.flow,
        "codacy_blocking_evidence",
        lambda _repo, _pr, _blockers: {
            "blocking": True,
            "ignored": False,
            "api_available": True,
            "api_ok": True,
            "issues_returned": 1,
            "checks": _blockers,
        },
    )
    monkeypatch.setattr(controller, "active_safe_autofix_runs", lambda _repo: [])
    decision: dict = {"actions": [], "warnings": [], "errors": []}
    pr = {
        "statusCheckRollup": [
            _check(
                "Codacy Static Code Analysis",
                "ACTION_REQUIRED",
                "https://app.codacy.com/gh/owner/repo/pull-requests/225",
            )
        ]
    }

    ctx = controller.build_next_action_context(_args(), decision, pr, ([], []))
    controller.decide_next_action(ctx)

    assert decision["next_action"] == "would_launch_safe_autofix"
    assert decision["launchable_for_safe_autofix"][0]["name"] == "Codacy Static Code Analysis"
    assert decision["blockers"][0]["name"] == "Codacy Static Code Analysis"
    assert decision["actions"]
