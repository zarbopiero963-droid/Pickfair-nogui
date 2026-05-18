"""Tests for PR automation controller decisions."""
# pylint: disable=invalid-name,duplicate-code

import argparse
from unittest import TestCase

import scripts.pr_automation_controller as controller

ASSERTIONS = TestCase()


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


def _stub_codacy_evidence(monkeypatch, *, blocking: bool, ignored: bool, issues: int) -> None:
    monkeypatch.setattr(
        controller,
        "controller_codacy_blocking_evidence",
        lambda _repo, _pr, _blockers: {
            "blocking": blocking,
            "ignored": ignored,
            "api_available": True,
            "api_ok": True,
            "issues_returned": issues,
            "checks": _blockers,
        },
    )


def _codacy_pr() -> dict[str, list[dict[str, str]]]:
    return {
        "statusCheckRollup": [
            _check(
                "Codacy Static Code Analysis",
                "ACTION_REQUIRED",
                "https://app.codacy.com/gh/owner/repo/pull-requests/225",
            )
        ]
    }


def _controller_decision(monkeypatch, *, blocking: bool, ignored: bool) -> dict:
    _stub_codacy_evidence(monkeypatch, blocking=blocking, ignored=ignored, issues=int(blocking))
    monkeypatch.setattr(controller, "active_safe_autofix_runs", lambda _repo: [])
    decision: dict = {"actions": [], "warnings": [], "errors": []}
    ctx = controller.build_next_action_context(_args(), decision, _codacy_pr(), ([], []))
    controller.decide_next_action(ctx)
    return decision


def test_controller_does_not_launch_safe_autofix_for_stale_codacy_action_required(monkeypatch):
    """A stale Codacy ACTION_REQUIRED check is ignored once the Codacy API is clear."""
    decision = _controller_decision(monkeypatch, blocking=False, ignored=True)

    ASSERTIONS.assertEqual(decision["next_action"], "checks_green_or_no_action")
    ASSERTIONS.assertEqual(decision["launchable_for_safe_autofix"], [])
    ASSERTIONS.assertEqual(decision["blockers"], [])
    ASSERTIONS.assertEqual(
        decision["ignored_codacy_checks"][0]["name"],
        "Codacy Static Code Analysis",
    )


def test_controller_preserves_safe_autofix_launch_for_current_codacy_blocker(monkeypatch):
    """Current Codacy blockers remain launchable for safe autofix."""
    decision = _controller_decision(monkeypatch, blocking=True, ignored=False)

    ASSERTIONS.assertEqual(decision["next_action"], "would_launch_safe_autofix")
    ASSERTIONS.assertEqual(
        decision["launchable_for_safe_autofix"][0]["name"],
        "Codacy Static Code Analysis",
    )
    ASSERTIONS.assertEqual(decision["blockers"][0]["name"], "Codacy Static Code Analysis")
    ASSERTIONS.assertTrue(decision["actions"])


def test_codacy_api_token_is_not_trusted_outside_github_actions(monkeypatch):
    """Local shell CODACY_API_TOKEN is not accepted as Codacy API authority."""
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    monkeypatch.setenv("HAS_CODACY_API_TOKEN", "true")
    monkeypatch.setenv("CODACY_API_TOKEN", "local-token")

    with ASSERTIONS.assertRaisesRegex(RuntimeError, "only trusted inside GitHub Actions"):
        controller.codacy_api_token()


def test_codacy_task_writes_raw_response_and_normalized_issue(tmp_path):
    """Controller Codacy task writer persists raw API response and normalized task context."""
    raw = {"data": [{"filePath": "scripts/pr_automation_controller.py", "lineNumber": 12, "message": "Fix me"}]}

    controller.write_codacy_task(tmp_path, raw, raw["data"])

    ASSERTIONS.assertTrue((tmp_path / "codacy-raw.json").exists())
    ASSERTIONS.assertTrue((tmp_path / "codacy-issues.json").exists())
    task = (tmp_path / "codacy-task.md").read_text(encoding="utf-8")
    ASSERTIONS.assertIn("scripts/pr_automation_controller.py:12", task)
    ASSERTIONS.assertIn("Fix me", task)


def test_clean_scope_defaults_allow_pr_flow_automation_script():
    """Automation PR can update pr_flow_automation without clean-scope refusal."""
    rules = controller.build_clean_scope_rules(_args())
    signals = controller.collect_clean_scope_signals(
        ["scripts/pr_flow_automation.py"],
        [],
        rules,
    )

    ASSERTIONS.assertTrue(signals["has_allowlisted_file"])
    ASSERTIONS.assertEqual(signals["forbidden_files"], [])
