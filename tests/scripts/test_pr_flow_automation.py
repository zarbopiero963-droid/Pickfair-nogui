"""Tests for PR flow automation decisions."""
# pylint: disable=invalid-name,duplicate-code

import argparse
import json
from typing import Any, cast
from unittest import TestCase

import scripts.pr_automation_controller as controller
import scripts.pr_flow_automation as flow

ASSERTIONS = TestCase()


def _check(name: str, state: str, url: str = "") -> dict[str, str]:
    return {"name": name, "conclusion": state, "detailsUrl": url}


def _codacy_check() -> dict[str, str]:
    return _check(
        "Codacy Static Code Analysis",
        "ACTION_REQUIRED",
        "https://app.codacy.com/gh/owner/repo/pull-requests/225",
    )


def _codacy_issue(path: str = "scripts/pr_flow_automation.py") -> dict[str, object]:
    return {
        "filePath": path,
        "lineNumber": 42,
        "patternId": "PY001",
        "tool": {"name": "ruff"},
        "severity": "Medium",
        "message": "Example issue",
    }


def test_split_checks_ignores_self_checks_when_requested():
    """Self-check failures are reported separately from real blockers."""
    checks = {
        "statusCheckRollup": [
            _check("PR Merge Readiness", "FAILURE"),
            _check("Unit tests", "SUCCESS"),
            _codacy_check(),
        ]
    }

    buckets = flow.split_checks(checks, ignore_self=True)

    ASSERTIONS.assertEqual(buckets["blockers"][0]["name"], "Codacy Static Code Analysis")
    ASSERTIONS.assertEqual(buckets["ignored"][0]["name"], "PR Merge Readiness")
    ASSERTIONS.assertEqual(buckets["self_stale"][0]["name"], "PR Merge Readiness")


def test_codacy_task_normalizes_common_issue_fields(tmp_path):
    """Codacy API output is persisted raw and rendered into a concise task file."""
    controller.write_codacy_task(tmp_path, {"data": [_codacy_issue()]}, [_codacy_issue()])

    task = (tmp_path / "codacy-task.md").read_text(encoding="utf-8")

    ASSERTIONS.assertTrue((tmp_path / "codacy-raw.json").exists())
    ASSERTIONS.assertTrue((tmp_path / "codacy-issues.json").exists())
    ASSERTIONS.assertIn("scripts/pr_flow_automation.py", task)
    ASSERTIONS.assertIn("PY001", task)
    ASSERTIONS.assertIn("Example issue", task)


def test_flow_codacy_task_writes_raw_response_and_task_file(tmp_path, monkeypatch):
    """The flow codacy-task command writes Codacy context for safe autofix."""
    raw = {"data": [_codacy_issue()]}
    monkeypatch.setattr(flow, "codacy_is_blocking", lambda _repo, _pr: True)
    monkeypatch.setattr(flow, "fetch_codacy_pr_issues", lambda _repo, _pr: (raw, raw["data"]))

    result = flow.cmd_codacy_task(
        argparse.Namespace(repo="owner/repo", pr="225", outdir=str(tmp_path))
    )

    ASSERTIONS.assertEqual(result, 0)
    ASSERTIONS.assertTrue((tmp_path / "codacy-raw.json").exists())
    task = (tmp_path / "codacy-task.md").read_text(encoding="utf-8")
    ASSERTIONS.assertIn("scripts/pr_flow_automation.py:42", task)
    ASSERTIONS.assertIn("ruff/PY001", task)
    ASSERTIONS.assertIn("Medium", task)
    ASSERTIONS.assertIn("Example issue", task)


def test_flow_codacy_task_fails_closed_when_blocking_api_fails(tmp_path, monkeypatch, capsys):
    """Codacy API failures remain blocking when the Codacy check is blocking."""
    monkeypatch.setattr(flow, "codacy_is_blocking", lambda _repo, _pr: True)
    monkeypatch.setattr(
        flow,
        "fetch_codacy_pr_issues",
        lambda _repo, _pr: (_ for _ in ()).throw(RuntimeError("Codacy API request failed")),
    )

    result = flow.cmd_codacy_task(
        argparse.Namespace(repo="owner/repo", pr="225", outdir=str(tmp_path))
    )

    ASSERTIONS.assertEqual(result, 1)
    ASSERTIONS.assertIn("Codacy API request failed", capsys.readouterr().err)


def test_flow_codacy_api_token_is_only_trusted_in_github_actions(monkeypatch):
    """Local CODACY_API_TOKEN values are ignored outside GitHub Actions."""
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    monkeypatch.setenv("CODACY_API_TOKEN", "local-token")

    with ASSERTIONS.assertRaisesRegex(RuntimeError, "only trusted inside GitHub Actions"):
        flow.codacy_api_token()


def test_codacy_blocking_evidence_ignores_stale_check_when_api_is_clear(monkeypatch):
    """A stale Codacy ACTION_REQUIRED check is ignored once the Codacy API is clear."""
    monkeypatch.setattr(controller, "fetch_codacy_pr_issues", lambda _repo, _pr: ({}, []))

    evidence = controller.controller_codacy_blocking_evidence("owner/repo", "225", [_codacy_check()])

    ASSERTIONS.assertFalse(evidence["blocking"])
    ASSERTIONS.assertTrue(evidence["ignored"])
    ASSERTIONS.assertEqual(evidence["issues_returned"], 0)


def test_codacy_blocking_evidence_preserves_current_blocker(monkeypatch):
    """Current Codacy issues keep ACTION_REQUIRED checks blocking."""
    monkeypatch.setattr(
        controller,
        "fetch_codacy_pr_issues",
        lambda _repo, _pr: ({"data": [_codacy_issue()]}, [_codacy_issue()]),
    )

    evidence = controller.controller_codacy_blocking_evidence("owner/repo", "225", [_codacy_check()])

    ASSERTIONS.assertTrue(evidence["blocking"])
    ASSERTIONS.assertFalse(evidence["ignored"])
    ASSERTIONS.assertEqual(evidence["issues_returned"], 1)


def test_build_decision_reports_real_blockers_and_merge_state(monkeypatch):
    """Readiness records real blockers without counting ignored self-checks."""
    monkeypatch.setattr(
        flow,
        "pr_view",
        lambda _repo, _pr: {
            "state": "OPEN",
            "isDraft": False,
            "mergeable": "MERGEABLE",
            "mergeStateStatus": "UNSTABLE",
            "statusCheckRollup": [_check("PR Merge Readiness", "FAILURE"), _codacy_check()],
        },
    )

    decision = flow.build_decision("owner/repo", "225", ignore_self=True)

    ASSERTIONS.assertFalse(decision["can_merge"])
    ASSERTIONS.assertEqual(decision["blockers"][0]["name"], "Codacy Static Code Analysis")
    ASSERTIONS.assertEqual(decision["ignored_self_checks"][0]["name"], "PR Merge Readiness")
    ASSERTIONS.assertIn("blocker_taxonomy", decision)
    ASSERTIONS.assertEqual(decision["blocker_taxonomy"]["next_action"], "fix_codacy_current_issues")
    ASSERTIONS.assertEqual(decision["next_action"], "fix_codacy_current_issues")


def test_build_decision_includes_active_review_threads_in_taxonomy(monkeypatch):
    """Active unresolved review threads should route taxonomy to fix_review_comments."""
    _stub_review_thread_pr_view(monkeypatch)
    decision = _build_decision_with_review_threads()
    _assert_review_thread_routing(decision)
    ASSERTIONS.assertFalse(decision["can_merge"])


def _stub_review_thread_pr_view(monkeypatch) -> None:
    monkeypatch.setattr(
        flow,
        "pr_view",
        lambda _repo, _pr: {
            "state": "OPEN",
            "isDraft": True,
            "mergeable": "MERGEABLE",
            "mergeStateStatus": "CLEAN",
            "statusCheckRollup": [],
        },
    )


def _build_decision_with_review_threads() -> dict[str, object]:
    return flow.build_decision(
        "owner/repo",
        "225",
        ignore_self=True,
        review_threads=[
            {"id": "thread-1", "isResolved": False, "isOutdated": False, "path": "a.py", "line": 9},
            {"id": "thread-2", "isResolved": True, "isOutdated": False, "path": "b.py", "line": 3},
        ],
    )


def _assert_review_thread_routing(decision: dict[str, object]) -> None:
    taxonomy = cast(dict[str, Any], decision["blocker_taxonomy"])
    ASSERTIONS.assertIn("review_comment_active", taxonomy["categories"])
    ASSERTIONS.assertEqual(taxonomy["primary_category"], "review_comment_active")
    ASSERTIONS.assertIn("active unresolved review thread", " ".join(cast(list[str], taxonomy["reasons"])))
    ASSERTIONS.assertEqual(taxonomy["next_action"], "fix_review_comments")
    ASSERTIONS.assertEqual(decision["next_action"], "fix_review_comments")


def _decision_clean_checks_with_active_review_thread(monkeypatch) -> dict[str, object]:
    monkeypatch.setattr(
        flow,
        "pr_view",
        lambda _repo, _pr: {
            "state": "OPEN",
            "isDraft": False,
            "mergeable": "MERGEABLE",
            "mergeStateStatus": "CLEAN",
            "statusCheckRollup": [_check("Unit tests", "SUCCESS")],
        },
    )
    return flow.build_decision(
        "owner/repo",
        "225",
        ignore_self=True,
        review_threads=[{"id": "thread-1", "isResolved": False, "isOutdated": False, "path": "a.py", "line": 9}],
    )


def test_build_decision_clean_checks_with_active_review_thread_blocks_merge(monkeypatch):
    """Active unresolved review thread must block merge even when checks are clean."""
    decision = _decision_clean_checks_with_active_review_thread(monkeypatch)
    taxonomy = cast(dict[str, Any], decision["blocker_taxonomy"])
    ASSERTIONS.assertFalse(decision["can_merge"])
    ASSERTIONS.assertEqual(decision["next_action"], "fix_review_comments")
    ASSERTIONS.assertEqual(taxonomy["primary_category"], "review_comment_active")
    ASSERTIONS.assertIn("active unresolved review thread", " ".join(cast(list[str], taxonomy["reasons"])))


def test_apply_taxonomy_next_action_does_not_promote_when_can_merge_false():
    """Taxonomy ready_to_merge cannot override blocked decision when can_merge is false."""
    decision = {
        "already_merged": False,
        "can_merge": False,
        "next_action": "blocked",
        "blocker_taxonomy": {"categories": ["none"], "next_action": "ready_to_merge"},
    }

    flow._apply_taxonomy_next_action(decision)  # pylint: disable=protected-access
    ASSERTIONS.assertEqual(decision["next_action"], "blocked")


def test_apply_taxonomy_next_action_review_blocker_overrides_ready_to_merge():
    """Active review blockers must force fix_review_comments even if taxonomy says ready_to_merge."""
    decision = {
        "already_merged": False,
        "can_merge": True,
        "next_action": "ready_to_merge",
        "blocker_taxonomy": {"categories": ["review_comment_active"], "next_action": "ready_to_merge"},
    }

    flow._apply_taxonomy_next_action(decision)  # pylint: disable=protected-access
    ASSERTIONS.assertEqual(decision["next_action"], "fix_review_comments")


def test_apply_taxonomy_next_action_review_blocker_does_not_override_high_priority_action():
    """Review blockers cannot overwrite higher-priority remediation actions."""
    decision = {
        "already_merged": False,
        "can_merge": False,
        "next_action": "wait_pending",
        "blocker_taxonomy": {"categories": ["review_comment_active"], "next_action": "fix_review_comments"},
    }

    flow._apply_taxonomy_next_action(decision)  # pylint: disable=protected-access
    ASSERTIONS.assertEqual(decision["next_action"], "wait_pending")


def test_apply_taxonomy_next_action_review_blocker_not_review_only_keeps_ready_action():
    """Active review blocker must prevent ready_to_merge even when not review-only."""
    decision = {
        "already_merged": False,
        "can_merge": True,
        "next_action": "ready_to_merge",
        "blocker_taxonomy": {
            "categories": ["review_comment_active", "workflow_pending"],
            "next_action": "fix_review_comments",
        },
    }

    flow._apply_taxonomy_next_action(decision)  # pylint: disable=protected-access
    ASSERTIONS.assertEqual(decision["next_action"], "fix_review_comments")


def test_apply_taxonomy_next_action_does_not_override_manual_secret_with_review_action():
    """Review blockers cannot overwrite manual-secret remediation."""
    decision = {
        "already_merged": False,
        "can_merge": False,
        "next_action": "needs_manual_secret",
        "blocker_taxonomy": {"categories": ["review_comment_active"], "next_action": "fix_review_comments"},
    }

    flow._apply_taxonomy_next_action(decision)  # pylint: disable=protected-access
    ASSERTIONS.assertEqual(decision["next_action"], "needs_manual_secret")


def test_apply_taxonomy_next_action_review_only_blocker_routes_to_fix_review_comments():
    """Review-only blocker routes to fix_review_comments."""
    decision = {
        "already_merged": False,
        "can_merge": False,
        "next_action": "checks_green_or_no_action",
        "blocker_taxonomy": {"categories": ["review_comment_active"], "next_action": "fix_review_comments"},
    }

    flow._apply_taxonomy_next_action(decision)  # pylint: disable=protected-access
    ASSERTIONS.assertEqual(decision["next_action"], "fix_review_comments")


def test_apply_taxonomy_next_action_does_not_override_merge_conflict_with_review_action():
    """Review blockers cannot overwrite merge-conflict manual remediation."""
    decision = {
        "already_merged": False,
        "can_merge": False,
        "next_action": "needs_manual_merge_conflict",
        "blocker_taxonomy": {"categories": ["review_comment_active"], "next_action": "fix_review_comments"},
    }

    flow._apply_taxonomy_next_action(decision)  # pylint: disable=protected-access
    ASSERTIONS.assertEqual(decision["next_action"], "needs_manual_merge_conflict")


def test_apply_taxonomy_next_action_does_not_override_rerun_stale_checks_with_review_action():
    """Review blockers cannot overwrite stale-check rerun action."""
    decision = {
        "already_merged": False,
        "can_merge": False,
        "next_action": "rerun_stale_checks",
        "blocker_taxonomy": {"categories": ["review_comment_active"], "next_action": "fix_review_comments"},
    }

    flow._apply_taxonomy_next_action(decision)  # pylint: disable=protected-access
    ASSERTIONS.assertEqual(decision["next_action"], "rerun_stale_checks")


def test_apply_taxonomy_next_action_does_not_override_scope_violation_with_review_action():
    """Review blockers cannot overwrite manual scope-violation remediation."""
    decision = {
        "already_merged": False,
        "can_merge": False,
        "next_action": "needs_manual_scope_violation",
        "blocker_taxonomy": {"categories": ["review_comment_active"], "next_action": "fix_review_comments"},
    }

    flow._apply_taxonomy_next_action(decision)  # pylint: disable=protected-access
    ASSERTIONS.assertEqual(decision["next_action"], "needs_manual_scope_violation")


def test_apply_taxonomy_next_action_does_not_override_codacy_rule_conflict_with_review_action():
    """Review blockers cannot overwrite manual Codacy-rule-conflict remediation."""
    decision = {
        "already_merged": False,
        "can_merge": False,
        "next_action": "needs_manual_codacy_rule_conflict",
        "blocker_taxonomy": {"categories": ["review_comment_active"], "next_action": "fix_review_comments"},
    }

    flow._apply_taxonomy_next_action(decision)  # pylint: disable=protected-access
    ASSERTIONS.assertEqual(decision["next_action"], "needs_manual_codacy_rule_conflict")


def test_apply_taxonomy_next_action_does_not_override_auto_resolve_merge_conflict_with_review_action():
    """Review blockers cannot overwrite merge-conflict auto-resolution action."""
    decision = {
        "already_merged": False,
        "can_merge": False,
        "next_action": "auto_resolve_merge_conflict",
        "blocker_taxonomy": {"categories": ["review_comment_active"], "next_action": "fix_review_comments"},
    }

    flow._apply_taxonomy_next_action(decision)  # pylint: disable=protected-access
    ASSERTIONS.assertEqual(decision["next_action"], "auto_resolve_merge_conflict")


def _preflight_args() -> argparse.Namespace:
    return argparse.Namespace(
        repo="owner/repo",
        pr="225",
        output="",
        max_safe_autofix_commits=3,
        oscillation_touch_limit=2,
        comment=False,
        no_fail=False,
    )


def _preflight_pr_view(_repo: str, _pr: str) -> dict[str, object]:
    return {
        "state": "OPEN",
        "headRefName": "feature/branch",
        "headRefOid": "abc123",
        "statusCheckRollup": [_check("Unit tests", "SUCCESS")],
    }


def test_preflight_commit_limit_only_blocks_when_codacy_is_blocking(monkeypatch):
    """Preflight does not fail only because of autofix history when Codacy is clear."""
    monkeypatch.setattr(flow, "pr_view", _preflight_pr_view)
    monkeypatch.setattr(flow, "sh", lambda *_args, **_kwargs: "")
    monkeypatch.setattr(
        flow,
        "safe_autofix_commits",
        lambda *_args, **_kwargs: ["a", "b", "c", "d"],
    )
    monkeypatch.setattr(
        flow,
        "changed_files_for_commit",
        lambda *_args, **_kwargs: ["scripts/pr_clean_scope_rebuild.py"],
    )

    rc = flow.cmd_preflight(_preflight_args())

    ASSERTIONS.assertEqual(rc, 0)


def test_push_with_retry_once_succeeds_on_first_push():
    """First push success returns success without retry."""
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], *, check: bool = True) -> str:
        ASSERTIONS.assertTrue(check)
        calls.append(list(cmd))
        return ""

    result = flow.push_with_retry_once(fake_run, "owner/repo", "feature/branch")

    ASSERTIONS.assertTrue(result["ok"])
    ASSERTIONS.assertEqual(result["status"], "success")
    ASSERTIONS.assertFalse(result["retried"])
    ASSERTIONS.assertFalse(result["needs_manual"])
    ASSERTIONS.assertEqual(calls, [["git", "push", "origin", "feature/branch"]])


def test_push_with_retry_once_non_fast_forward_then_retry_success():
    """Non-fast-forward push does one fetch and one retry push."""
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], *, check: bool = True) -> str:
        ASSERTIONS.assertTrue(check)
        calls.append(list(cmd))
        if len(calls) == 1:
            raise RuntimeError("failed to push some refs to origin (non-fast-forward)")
        return ""

    result = flow.push_with_retry_once(fake_run, "owner/repo", "feature/branch")

    ASSERTIONS.assertTrue(result["ok"])
    ASSERTIONS.assertEqual(result["status"], "success")
    ASSERTIONS.assertTrue(result["retried"])
    ASSERTIONS.assertFalse(result["needs_manual"])
    ASSERTIONS.assertEqual(
        calls,
        [
            ["git", "push", "origin", "feature/branch"],
            ["git", "fetch", "origin", "feature/branch"],
            ["git", "push", "origin", "feature/branch", "--force-with-lease"],
        ],
    )


def test_push_with_retry_once_non_fast_forward_then_retry_fails_needs_manual():
    """A failed retry after non-fast-forward returns needs_manual with no loop."""
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], *, check: bool = True) -> str:
        ASSERTIONS.assertTrue(check)
        calls.append(list(cmd))
        if len(calls) == 1:
            raise RuntimeError("non-fast-forward update rejected")
        if len(calls) == 3:
            raise RuntimeError("failed to push some refs")
        return ""

    result = flow.push_with_retry_once(fake_run, "owner/repo", "feature/branch")

    ASSERTIONS.assertFalse(result["ok"])
    ASSERTIONS.assertEqual(result["status"], "needs_manual")
    ASSERTIONS.assertTrue(result["retried"])
    ASSERTIONS.assertTrue(result["needs_manual"])
    ASSERTIONS.assertIn("failed to push some refs", result["error"])
    ASSERTIONS.assertEqual(
        calls,
        [
            ["git", "push", "origin", "feature/branch"],
            ["git", "fetch", "origin", "feature/branch"],
            ["git", "push", "origin", "feature/branch", "--force-with-lease"],
        ],
    )


def _automation_controller_args() -> argparse.Namespace:
    return argparse.Namespace(
        clean_scope_allowlist="",
        clean_scope_forbidden="",
        clean_scope_commit_limit=3,
        clean_scope_rebuild=False,
        clean_scope_rebuild_mode="disabled",
        repo="owner/repo",
        pr="225",
        dry_run=True,
        safe_max_rounds="1",
        safe_pending_wait_seconds="600",
    )


def test_automation_change_prs_should_enable_bounded_repair_mode():
    """Automation/workflow/controller changes should be bounded to one repair round."""
    files = [
        "scripts/pr_automation_controller.py",
        ".github/workflows/pr-automation-controller-v2.yml",
    ]
    rules = controller.build_clean_scope_rules(_automation_controller_args())
    signals = controller.collect_clean_scope_signals(files, [], rules)

    ASSERTIONS.assertTrue(signals["has_allowlisted_file"])
    ASSERTIONS.assertFalse(signals["autofix_commit_limit_exceeded"])


def test_build_decision_ready_to_merge_condition_true(monkeypatch):
    """Ready-to-merge condition maps to ready_to_merge for a clean merge context."""
    monkeypatch.setattr(flow, "pr_view", _ready_to_merge_pr_view)
    decision = flow.build_decision("owner/repo", "225", ignore_self=True)

    _assert_ready_to_merge_decision(decision)


def test_build_decision_draft_pr_not_promoted_to_ready_to_merge(monkeypatch):
    """Draft PR remains blocked even with clean checks because can_merge is false."""
    monkeypatch.setattr(
        flow,
        "pr_view",
        lambda _repo, _pr: {
            "state": "OPEN",
            "isDraft": True,
            "mergeable": "MERGEABLE",
            "mergeStateStatus": "CLEAN",
            "statusCheckRollup": [_check("Unit tests", "SUCCESS")],
        },
    )
    decision = flow.build_decision("owner/repo", "225", ignore_self=True)
    taxonomy = cast(dict[str, Any], decision["blocker_taxonomy"])

    ASSERTIONS.assertFalse(decision["can_merge"])
    ASSERTIONS.assertEqual(decision["next_action"], "blocked")
    ASSERTIONS.assertNotEqual(decision["next_action"], "checks_green_or_no_action")
    ASSERTIONS.assertNotEqual(decision["next_action"], "ready_to_merge")
    ASSERTIONS.assertEqual(taxonomy["next_action"], "checks_green_or_no_action")


def test_apply_taxonomy_next_action_does_not_demote_blocked_to_checks_green_or_no_action():
    """Non-mergeable blocked decision must not be demoted by empty taxonomy action."""
    decision = {
        "already_merged": False,
        "can_merge": False,
        "next_action": "blocked",
        "blocker_taxonomy": {"categories": [], "next_action": "checks_green_or_no_action"},
    }

    flow._apply_taxonomy_next_action(decision)  # pylint: disable=protected-access
    ASSERTIONS.assertEqual(decision["next_action"], "blocked")


def test_merge_conflict_taxonomy_items_uses_classifier_output():
    """Merge-conflict taxonomy item should come from classify_merge_conflict output."""
    items = flow._merge_conflict_taxonomy_items(  # pylint: disable=protected-access
        {
            "mergeable": "CONFLICTING",
            "mergeStateStatus": "DIRTY",
            "conflicted_files": ["scripts/pr_flow_automation.py"],
        }
    )

    ASSERTIONS.assertEqual(len(items), 1)
    ASSERTIONS.assertEqual(items[0]["category"], "merge_conflict")
    ASSERTIONS.assertEqual(items[0]["next_action"], "auto_resolve_merge_conflict")


def test_merge_conflict_taxonomy_items_clean_pr_is_empty():
    """Clean PR must not produce merge_conflict taxonomy items."""
    items = flow._merge_conflict_taxonomy_items(  # pylint: disable=protected-access
        {"mergeable": "MERGEABLE", "mergeStateStatus": "CLEAN"}
    )
    ASSERTIONS.assertEqual(items, [])


def test_telegram_ready_summary_contract():
    """Telegram-ready summary should include all required report keys."""
    if not hasattr(flow, "build_telegram_summary"):
        raise NotImplementedError("build_telegram_summary not implemented")
    summary = flow.build_telegram_summary(_telegram_summary_context())
    for key in _required_telegram_summary_keys():
        ASSERTIONS.assertIn(key, summary)


def test_should_notify_ready_to_merge_true_when_all_conditions_match():
    """Ready-to-merge notification only triggers for a fully clean context."""
    should_notify = flow.should_notify_ready_to_merge(
        {
            "bad": [],
            "unresolved_active": 0,
            "mergeable": "MERGEABLE",
            "mergeStateStatus": "CLEAN",
        }
    )

    ASSERTIONS.assertTrue(should_notify)


def test_auto_resolve_review_comments_contract_active_only():
    """Only active unresolved review comments should be eligible for auto-resolve."""
    if not hasattr(flow, "eligible_review_comments_for_auto_resolve"):
        raise NotImplementedError("eligible_review_comments_for_auto_resolve not implemented")
    eligible = flow.eligible_review_comments_for_auto_resolve(
        [
            {"id": "a", "isResolved": False, "isOutdated": False},
            {"id": "b", "isResolved": True, "isOutdated": False},
            {"id": "c", "isResolved": False, "isOutdated": True},
        ]
    )
    ASSERTIONS.assertEqual([item["id"] for item in eligible], ["a"])


def test_d203_d211_rule_conflict_detection_contract():
    """D203 and D211 on same file/symbol should classify as codacy rule conflict needing manual action."""
    if not hasattr(flow, "classify_codacy_rule_conflict"):
        raise NotImplementedError("classify_codacy_rule_conflict not implemented")
    result = flow.classify_codacy_rule_conflict(
        [
            {"filePath": "scripts/pr_flow_automation.py", "patternId": "D203", "symbol": "ClassX"},
            {"filePath": "scripts/pr_flow_automation.py", "patternId": "D211", "symbol": "ClassX"},
        ]
    )
    ASSERTIONS.assertEqual(result["classification"], "codacy_rule_conflict")
    ASSERTIONS.assertEqual(result["next_action"], "needs_manual_codacy_rule_conflict")


def test_d203_d211_same_file_same_line_conflict_even_if_messages_differ():
    """D203/D211 conflicts should be detected by location even when messages differ."""
    result = flow.classify_codacy_rule_conflict(
        [
            {
                "filePath": "scripts/pr_flow_automation.py",
                "patternId": "D203",
                "lineNumber": 42,
                "message": "blank line required",
            },
            {
                "filePath": "scripts/pr_flow_automation.py",
                "patternId": "D211",
                "lineNumber": 42,
                "message": "blank line not allowed",
            },
        ]
    )
    ASSERTIONS.assertEqual(result["classification"], "codacy_rule_conflict")


def _stub_pr_view_for_report(monkeypatch) -> None:
    monkeypatch.setattr(
        flow,
        "build_decision",
        lambda *_args, **_kwargs: {
            "repo": "owner/repo",
            "pr": "225",
            "headRefOid": "abc123",
            "state": "OPEN",
            "already_merged": False,
            "can_merge": False,
            "mergeable": "MERGEABLE",
            "mergeStateStatus": "CLEAN",
            "next_action": "blocked",
            "reasons": [],
            "blockers": [_codacy_check()],
            "pending": [],
            "ignored_self_checks": [],
        },
    )


def _stub_codacy_for_report(monkeypatch) -> None:
    monkeypatch.setattr(
        flow,
        "fetch_codacy_pr_issues",
        lambda *_args: (
            {},
            [
                {"filePath": "a.py", "patternId": "D203", "lineNumber": 1},
                {"filePath": "a.py", "patternId": "D211", "lineNumber": 1},
            ],
        ),
    )


def _stub_review_threads_for_report(monkeypatch) -> None:
    monkeypatch.setattr(
        flow,
        "gh_json",
        lambda *_args, **_kwargs: {
            "data": {
                "repository": {
                    "pullRequest": {
                        "reviewThreads": {
                            "nodes": [
                                {"id": "a", "isResolved": False, "isOutdated": False},
                                {"id": "b", "isResolved": True, "isOutdated": False},
                            ]
                        }
                    }
                }
            }
        },
    )


def test_fetch_all_review_threads_paginates(monkeypatch):
    """Review thread collection should continue through all GraphQL pages."""
    responses = iter(
        [
            {
                "data": {
                    "repository": {
                        "pullRequest": {
                            "reviewThreads": {
                                "nodes": [{"id": "a", "isResolved": True, "isOutdated": False}],
                                "pageInfo": {"hasNextPage": True, "endCursor": "cursor-1"},
                            }
                        }
                    }
                }
            },
            {
                "data": {
                    "repository": {
                        "pullRequest": {
                            "reviewThreads": {
                                "nodes": [{"id": "b", "isResolved": False, "isOutdated": False}],
                                "pageInfo": {"hasNextPage": False, "endCursor": "cursor-2"},
                            }
                        }
                    }
                }
            },
        ]
    )
    monkeypatch.setattr(flow, "gh_json", lambda *_args, **_kwargs: next(responses))

    threads = flow.fetch_all_review_threads("owner/repo", "225")

    ASSERTIONS.assertEqual([thread["id"] for thread in threads], ["a", "b"])


def test_cmd_report_second_page_unresolved_review_thread_blocks_merge(tmp_path, monkeypatch):
    """An unresolved thread from a later page should route to fix_review_comments."""
    monkeypatch.setattr(flow, "pr_view", _ready_to_merge_pr_view)
    monkeypatch.setattr(flow, "fetch_codacy_pr_issues", lambda *_args: ({}, []))
    responses = iter(
        [
            {
                "data": {
                    "repository": {
                        "pullRequest": {
                            "reviewThreads": {
                                "nodes": [{"id": "a", "isResolved": True, "isOutdated": False}],
                                "pageInfo": {"hasNextPage": True, "endCursor": "cursor-1"},
                            }
                        }
                    }
                }
            },
            {
                "data": {
                    "repository": {
                        "pullRequest": {
                            "reviewThreads": {
                                "nodes": [{"id": "b", "isResolved": False, "isOutdated": False}],
                                "pageInfo": {"hasNextPage": False, "endCursor": "cursor-2"},
                            }
                        }
                    }
                }
            },
        ]
    )
    monkeypatch.setattr(flow, "gh_json", lambda *_args, **_kwargs: next(responses))

    rc = flow.cmd_report(
        argparse.Namespace(repo="owner/repo", pr="225", outdir=str(tmp_path), comment=False, no_fail=True)
    )
    decision = json.loads((tmp_path / "pr-flow-decision.json").read_text(encoding="utf-8"))

    ASSERTIONS.assertEqual(rc, 0)
    ASSERTIONS.assertFalse(decision["can_merge"])
    ASSERTIONS.assertEqual(decision["next_action"], "fix_review_comments")
    ASSERTIONS.assertEqual(decision["review"]["unresolved_active"], 1)
    ASSERTIONS.assertEqual(decision["review"]["total_threads"], 2)


def _stub_report_output_paths(monkeypatch) -> None:
    _stub_pr_view_for_report(monkeypatch)
    _stub_review_threads_for_report(monkeypatch)
    _stub_codacy_for_report(monkeypatch)


def _stub_cmd_report_inputs(monkeypatch) -> None:
    _stub_report_output_paths(monkeypatch)


def _run_cmd_report_no_fail(tmp_path) -> int:
    return flow.cmd_report(
        argparse.Namespace(
            repo="owner/repo",
            pr="225",
            outdir=str(tmp_path),
            comment=False,
            no_fail=True,
        )
    )


def _assert_cmd_report_context_output(tmp_path) -> None:
    decision = json.loads((tmp_path / "pr-flow-decision.json").read_text(encoding="utf-8"))
    ASSERTIONS.assertEqual(decision["review_auto_resolve_candidates"], 1)
    ASSERTIONS.assertEqual(decision["telegram_summary"]["pr_number"], "225")
    ASSERTIONS.assertEqual(decision["telegram_summary"]["head_sha"], "abc123")
    ASSERTIONS.assertEqual(decision["telegram_summary"]["codacy_classification"], "codacy_rule_conflict")
    ASSERTIONS.assertEqual(decision["telegram_summary"]["github_codacy_check_state"], "ACTION_REQUIRED")
    ASSERTIONS.assertEqual(decision["telegram_summary"]["active_unresolved_review_count"], 1)
    ASSERTIONS.assertFalse(decision["ready_to_merge_notification"])


def test_cmd_report_wires_real_context_into_helpers(tmp_path, monkeypatch):
    """Report should use actual decision/codacy/review context instead of placeholders."""
    _stub_cmd_report_inputs(monkeypatch)
    rc = _run_cmd_report_no_fail(tmp_path)
    ASSERTIONS.assertEqual(rc, 0)
    _assert_cmd_report_context_output(tmp_path)


def test_taxonomy_summary_prioritizes_pending_and_manual_blockers():
    """Pending blockers take priority, then manual blockers over autofix routes."""
    pending = flow.summarize_blocker_actions(
        [{"name": "CI", "state": "IN_PROGRESS"}, {"name": "Codacy", "state": "FAILURE", "source": "codacy"}],
        {},
    )
    ASSERTIONS.assertEqual(pending["primary_category"], "workflow_pending")
    ASSERTIONS.assertEqual(pending["next_action"], "wait_pending")
    manual = flow.summarize_blocker_actions(
        [{"name": "Scope", "state": "FAILURE", "reason": "scope_violation"}],
        {},
    )
    ASSERTIONS.assertEqual(manual["next_action"], "needs_manual_scope_violation")


def test_route_blocker_action_test_failure_requires_clear_logs():
    """test_failure routes to autofix only with explicit clear logs context."""
    ASSERTIONS.assertEqual(flow.route_blocker_action("test_failure", {}), "needs_manual")
    ASSERTIONS.assertEqual(flow.route_blocker_action("test_failure", {"logs_clear": True}), "fix_test_failure")


def _report_without_codacy_check_decision() -> dict[str, Any]:
    return {
        "repo": "owner/repo",
        "pr": "225",
        "headRefOid": "abc123",
        "state": "OPEN",
        "already_merged": False,
        "can_merge": False,
        "mergeable": "MERGEABLE",
        "mergeStateStatus": "CLEAN",
        "next_action": "blocked",
        "reasons": [],
        "blockers": [],
        "pending": [],
        "ignored_self_checks": [],
    }


def _stub_report_without_codacy_check_build_decision(monkeypatch) -> None:
    monkeypatch.setattr(
        flow,
        "build_decision",
        lambda *_args, **_kwargs: _report_without_codacy_check_decision(),
    )


def _stub_report_without_codacy_check_api(monkeypatch) -> None:
    monkeypatch.setattr(flow, "fetch_codacy_pr_issues", lambda *_args: ({}, []))
    monkeypatch.setattr(
        flow,
        "gh_json",
        lambda *_args, **_kwargs: {"data": {"repository": {"pullRequest": {"reviewThreads": {"nodes": []}}}}},
    )


def _stub_report_without_codacy_check(monkeypatch, tmp_path) -> argparse.Namespace:
    _stub_report_without_codacy_check_build_decision(monkeypatch)
    _stub_report_without_codacy_check_api(monkeypatch)
    return argparse.Namespace(repo="owner/repo", pr="225", outdir=str(tmp_path), comment=False, no_fail=True)


def _read_report_decision(path) -> dict[str, object]:
    return json.loads((path / "pr-flow-decision.json").read_text(encoding="utf-8"))


def _assert_no_codacy_check_not_stale(decision: dict[str, object]) -> None:
    codacy = cast(dict[str, Any], decision["codacy"])
    ASSERTIONS.assertEqual(codacy["classification"], "none")
    ASSERTIONS.assertFalse(codacy["treat_annotations_as_blockers"])


def test_cmd_report_no_codacy_check_and_zero_issues_is_not_stale(tmp_path, monkeypatch):
    """No Codacy check with empty API issues should remain classification none."""
    args = _stub_report_without_codacy_check(monkeypatch, tmp_path)
    rc = flow.cmd_report(args)
    decision = _read_report_decision(tmp_path)

    ASSERTIONS.assertEqual(rc, 0)
    _assert_no_codacy_check_not_stale(decision)


def test_cmd_report_codacy_check_zero_issues_is_stale(tmp_path, monkeypatch):
    """Codacy check with empty API issues should classify as stale_github_check."""
    _stub_pr_view_for_report(monkeypatch)
    monkeypatch.setattr(flow, "fetch_codacy_pr_issues", lambda *_args: ({}, []))
    monkeypatch.setattr(
        flow,
        "gh_json",
        lambda *_args, **_kwargs: {
            "data": {"repository": {"pullRequest": {"reviewThreads": {"nodes": []}}}}
        },
    )

    rc = flow.cmd_report(
        argparse.Namespace(repo="owner/repo", pr="225", outdir=str(tmp_path), comment=False, no_fail=True)
    )
    decision = json.loads((tmp_path / "pr-flow-decision.json").read_text(encoding="utf-8"))

    ASSERTIONS.assertEqual(rc, 0)
    ASSERTIONS.assertEqual(decision["codacy"]["classification"], "stale_github_check")
    ASSERTIONS.assertFalse(decision["codacy"]["treat_annotations_as_blockers"])


def _ready_to_merge_pr_view(_repo: str, _pr: str) -> dict[str, object]:
    return {
        "state": "OPEN",
        "isDraft": False,
        "mergeable": "MERGEABLE",
        "mergeStateStatus": "CLEAN",
        "reviewDecision": "APPROVED",
        "headRefOid": "abc123",
        "statusCheckRollup": [_check("Unit tests", "SUCCESS")],
    }


def _assert_ready_to_merge_decision(decision: dict[str, object]) -> None:
    taxonomy = cast(dict[str, Any], decision["blocker_taxonomy"])
    ASSERTIONS.assertEqual(decision["blockers"], [])
    ASSERTIONS.assertEqual(decision["pending"], [])
    ASSERTIONS.assertEqual(decision["mergeable"], "MERGEABLE")
    ASSERTIONS.assertEqual(decision["mergeStateStatus"], "CLEAN")
    ASSERTIONS.assertTrue(decision["can_merge"])
    ASSERTIONS.assertEqual(decision["next_action"], "ready_to_merge")
    ASSERTIONS.assertEqual(taxonomy["next_action"], "ready_to_merge")


def _telegram_summary_context() -> dict[str, object]:
    return {
        "pr": "225",
        "headRefOid": "abc123",
        "codacy": {"classification": "real_current_issues", "issues_returned": 2},
        "github_codacy_check_state": "ACTION_REQUIRED",
        "review": {"unresolved_active": 1},
        "next_action": "fix_codacy_current_issues",
    }


def _required_telegram_summary_keys() -> tuple[str, ...]:
    return (
        "pr_number",
        "head_sha",
        "codacy_classification",
        "github_codacy_check_state",
        "codacy_api_issue_count",
        "active_unresolved_review_count",
        "next_action",
    )
