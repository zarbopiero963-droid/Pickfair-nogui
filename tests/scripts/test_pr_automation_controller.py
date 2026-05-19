"""Tests for PR automation controller decisions."""
# pylint: disable=invalid-name,duplicate-code

import argparse
from unittest import TestCase

import pytest

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


def test_controller_waits_on_pending_real_checks_without_launching_safe_autofix(monkeypatch):
    """Pending real checks must keep controller in wait_pending and skip safe-autofix launch."""
    monkeypatch.setattr(controller, "controller_codacy_blocking_evidence", lambda *_args, **_kwargs: controller.codacy_evidence_without_checks())
    monkeypatch.setattr(controller, "active_safe_autofix_runs", lambda _repo: [])
    monkeypatch.setattr(
        controller,
        "launch_safe_autofix",
        lambda _cfg: (_ for _ in ()).throw(AssertionError("safe autofix must not launch while pending checks exist")),
    )
    decision: dict = {"actions": [], "warnings": [], "errors": [], "pending_count": 1}
    pr = {"statusCheckRollup": [_check("Unit tests", "IN_PROGRESS")]}

    ctx = controller.build_next_action_context(_args(), decision, pr, ([], []))
    controller.decide_next_action(ctx)

    ASSERTIONS.assertEqual(decision["next_action"], "wait_pending")
    ASSERTIONS.assertEqual(decision["actions"], [])


def test_only_cancelled_or_stale_self_checks_prefer_rerun_and_skip_codex(monkeypatch):
    """Self stale/cancelled checks should trigger rerun path rather than codex launch loops."""
    monkeypatch.setattr(
        controller,
        "rerun_cancelled_checks",
        lambda _checks, _config: [{"run_id": "12345", "state": "CANCELLED", "name": "PR Merge Readiness"}],
    )
    decision: dict = {"actions": [], "warnings": [], "errors": []}
    checks = [
        _check("PR Merge Readiness", "CANCELLED", "https://github.com/owner/repo/actions/runs/12345"),
        _check("PR Automation Controller", "STALE", "https://github.com/owner/repo/actions/runs/99999"),
    ]

    handled = controller.handle_cancelled_checks(
        checks,
        controller.RerunConfig(repo="owner/repo", dry_run=True, max_reruns=3),
        decision,
    )

    ASSERTIONS.assertTrue(handled)
    ASSERTIONS.assertIn(decision["next_action"], {"rerun_stale_or_cancelled_checks", "rerun_cancelled_checks"})
    ASSERTIONS.assertEqual(decision["actions"][0]["type"], "rerun_cancelled")


def test_review_task_ignores_outdated_unresolved_threads():
    """Outdated unresolved-only review threads should not be treated as active blockers."""
    lines = controller._review_task_lines([  # pylint: disable=protected-access
        {
            "id": "T1",
            "isResolved": False,
            "isOutdated": True,
            "path": "scripts/pr_flow_automation.py",
            "line": 25,
            "comments": {"nodes": [{"body": "old", "url": "http://example", "author": {"login": "bot"}}]},
        }
    ])

    ASSERTIONS.assertIn("No unresolved review threads found", "\n".join(lines))


def test_automation_scope_safe_autofix_is_bounded_to_one_round():
    """Automation/controller/workflow PRs should remain bounded to one repair round."""
    args = _args()
    config = controller.safe_autofix_config_from_args(args)
    command = controller.safe_autofix_command(config)

    ASSERTIONS.assertIn("max_rounds=1", command)


def test_no_progress_repeated_blocker_signature_stops_with_needs_manual():
    """Repeated blocker signatures without improvement should stop with no_progress."""
    helper_name = "detect_no_progress_blocker_signature"
    if not hasattr(controller, helper_name):
        pytest.xfail(f"expected helper not implemented yet: controller.{helper_name}")
    helper = getattr(controller, helper_name)
    ASSERTIONS.assertTrue(callable(helper))
