"""Tests for PR flow automation decisions."""
# pylint: disable=invalid-name,duplicate-code

import argparse
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
