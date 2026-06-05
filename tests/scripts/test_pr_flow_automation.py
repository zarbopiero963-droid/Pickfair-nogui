"""Tests for PR flow automation decisions."""
# pylint: disable=invalid-name,duplicate-code

import argparse
import inspect
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


def _phase0_pass_payload() -> dict[str, Any]:
    return {
        "status": "PASS",
        "risk_level": "medium",
        "next_action": "generate_patch_prompt",
        "files_inspected": ["scripts/pr_flow_automation.py"],
        "static_analysis_rules": ["CCN<=10"],
        "workflows_affected": ["pr-flow-guardrails"],
        "authoritative_modules": ["scripts/pr_flow_automation.py"],
        "dangerous_gates": ["cmd_codacy_task"],
        "implementation_plan": ["fix parser"],
        "tests_to_run": ["python3 -m pytest tests/scripts/test_pr_flow_automation.py -q"],
        "stop_conditions": ["stop on scope violation"],
    }


def _assert_phase0_pass(raw: str) -> None:
    report = flow.parse_phase0_preflight_result(raw)
    ASSERTIONS.assertEqual(report["status"], "PASS")
    ASSERTIONS.assertEqual(flow.phase0_preflight_status(report), "PASS")
    ASSERTIONS.assertFalse(flow.phase0_preflight_failed(report))


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
    ASSERTIONS.assertIn("POST-FIX MICRO-AUDIT BEFORE COMMIT", task)


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
    ASSERTIONS.assertIn("Do not commit a patch that fails this audit.", task)


def test_post_fix_micro_audit_helpers_do_not_trigger_final_merge_audit():
    """Post-fix helper should not alter final merge-audit routing."""
    report = flow.parse_post_fix_micro_audit_result("status: PASS")
    ASSERTIONS.assertFalse(flow.post_fix_micro_audit_failed(report))
    ASSERTIONS.assertEqual(flow.post_fix_micro_audit_status(report), "PASS")
    ASSERTIONS.assertEqual(report["next_action"], "validation_then_commit")


def test_flow_codex_prompt_contract_helpers_available():
    """Flow module re-exports codex prompt helpers with phase-0 section in generated prompt."""
    prompt = flow.build_codex_task_prompt({"task": "x", "objective": "y"})
    ASSERTIONS.assertIn("TASK:", prompt)
    ASSERTIONS.assertIn("PHASE 0 PRE-FLIGHT (READ-ONLY)", prompt)
    ASSERTIONS.assertEqual(flow.codex_prompt_contract_missing_sections(prompt), [])


def test_flow_ensure_phase0_preflight_section_accepts_one_arg():
    """Flow wrapper should keep one-argument compatibility."""
    ensured = flow.ensure_phase0_preflight_section("TASK:\nFix lint")
    ASSERTIONS.assertIn("PHASE 0 PRE-FLIGHT (READ-ONLY)", ensured)


def test_flow_ensure_phase0_preflight_section_accepts_prompt_and_context():
    """Flow wrapper should accept optional context without raising type errors."""
    ensured = flow.ensure_phase0_preflight_section(
        "TASK:\nFix lint",
        {"files_allowed": ["scripts/pr_automation_controller.py"]},
    )
    ASSERTIONS.assertIn("PHASE 0 PRE-FLIGHT (READ-ONLY)", ensured)


def test_flow_ensure_phase0_preflight_section_preserves_files_allowed_from_context():
    """Flow wrapper should preserve explicit files_allowed in inserted Phase 0."""
    ensured = flow.ensure_phase0_preflight_section(
        "TASK:\nFix lint",
        {"files_allowed": ["scripts/pr_automation_controller.py"]},
    )
    ASSERTIONS.assertIn("Task allowed files: scripts/pr_automation_controller.py", ensured)
    ASSERTIONS.assertNotIn("Task allowed files: (from task scope)", ensured)


def test_flow_phase0_parser_supports_fenced_json_with_prose():
    """Phase-0 parser accepts prose-wrapped fenced JSON and keeps PASS routing."""
    payload: dict[str, Any] = _phase0_pass_payload()
    raw = "\n".join(["preflight result follows", "```json", json.dumps(payload), "```"])
    _assert_phase0_pass(raw)


def test_flow_phase0_parser_supports_json_string_list_evidence_fields():
    """Flow wrapper should parse string-encoded bullet lists in required evidence fields."""
    payload = _phase0_pass_payload()
    payload["files_inspected"] = "- scripts/a.py"
    payload["tests_to_run"] = "- pytest"
    report = flow.parse_phase0_preflight_result(json.dumps(payload))
    ASSERTIONS.assertEqual(report["status"], "PASS")
    ASSERTIONS.assertEqual(report["files_inspected"], ["scripts/a.py"])
    ASSERTIONS.assertEqual(report["tests_to_run"], ["pytest"])
    ASSERTIONS.assertFalse(flow.phase0_preflight_failed(report))


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


def _full_pr3h_push_gate_context() -> dict[str, Any]:
    return {
        "automation_mode": "live",
        "post_fix_audit": "PASS",
        "validation_passed": True,
        "current_head_matches": True,
        "dirty_worktree": False,
        "task_no_commit_push": False,
        "scope_allowed": True,
        "rollback_attempted": False,
        "rollback_succeeded": True,
        "can_push": True,
        "can_commit": True,
    }


def _retry_refresh_gate_context() -> dict[str, Any]:
    return {
        "retry_push_gate_context": {
            "explicitly_refreshed": True,
            "gate_context": _full_pr3h_push_gate_context(),
        }
    }


def _assert_gate_shape(result: dict[str, Any], expected: dict[str, Any]) -> None:
    ASSERTIONS.assertEqual(
        result,
        {
            "allowed": expected["allowed"],
            "can_push": expected["allowed"],
            "reason": expected["reason"],
            "next_action": expected["next_action"],
            "needs_manual": expected["needs_manual"],
        },
    )


def _allow_push_gate_response() -> dict[str, Any]:
    return {
        "allowed": True,
        "can_push": True,
        "reason": "allowed",
        "next_action": "proceed",
        "needs_manual": False,
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

    result = flow.push_with_retry_once(
        fake_run,
        "owner/repo",
        "feature/branch",
        context=_full_pr3h_push_gate_context(),
    )

    ASSERTIONS.assertTrue(result["ok"])
    ASSERTIONS.assertEqual(result["status"], "success")
    ASSERTIONS.assertFalse(result["retried"])
    ASSERTIONS.assertFalse(result["needs_manual"])
    ASSERTIONS.assertEqual(calls, [["git", "push", "origin", "feature/branch"]])


def test_ensure_post_fix_audit_gate_before_push_shape_stable_for_allowed(monkeypatch):
    """Allowed gate response keeps the expected contract keys and values."""
    monkeypatch.setattr(flow.controller, "can_push_after_post_fix_audit", lambda _context: _allow_push_gate_response())
    result = flow.ensure_post_fix_audit_gate_before_push({"post_fix_audit": "PASS"})
    _assert_gate_shape(
        result,
        {
            "allowed": True,
            "reason": "allowed",
            "next_action": "proceed",
            "needs_manual": False,
        },
    )


def test_ensure_post_fix_audit_gate_before_push_shape_stable_for_denied(monkeypatch):
    """Denied gate response keeps fail-closed defaults when fields are missing."""
    monkeypatch.setattr(flow.controller, "can_push_after_post_fix_audit", lambda _context: {})
    result = flow.ensure_post_fix_audit_gate_before_push({"post_fix_audit": "FAIL"})
    _assert_gate_shape(
        result,
        {
            "allowed": False,
            "reason": "post_fix_audit_gate_denied",
            "next_action": "needs_manual",
            "needs_manual": True,
        },
    )


def test_push_with_retry_once_non_fast_forward_then_retry_success():
    """Non-fast-forward push does one fetch and one retry push."""
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], *, check: bool = True) -> str:
        ASSERTIONS.assertTrue(check)
        calls.append(list(cmd))
        if len(calls) == 1:
            raise RuntimeError("failed to push some refs to origin (non-fast-forward)")
        return ""

    result = flow.push_with_retry_once(
        fake_run,
        "owner/repo",
        "feature/branch",
        context={**_full_pr3h_push_gate_context(), **_retry_refresh_gate_context()},
    )

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

    result = flow.push_with_retry_once(
        fake_run,
        "owner/repo",
        "feature/branch",
        context={**_full_pr3h_push_gate_context(), **_retry_refresh_gate_context()},
    )

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


def test_push_with_retry_once_blocks_before_initial_push_without_pr3h_evidence():
    """Missing PR3H evidence fail-closes before initial push."""
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **_kwargs: Any) -> str:
        calls.append(list(cmd))
        return ""

    result = flow.push_with_retry_once(fake_run, "owner/repo", "feature/branch")

    ASSERTIONS.assertFalse(result["ok"])
    ASSERTIONS.assertEqual(result["status"], "needs_manual")
    ASSERTIONS.assertFalse(result["retried"])
    ASSERTIONS.assertTrue(result["needs_manual"])
    ASSERTIONS.assertEqual(calls, [])


def test_push_with_retry_once_non_fast_forward_blocks_before_retry_without_pr3h_evidence():
    """Retry path is guarded; force-with-lease is not called when PR3H evidence is missing."""
    calls: list[list[str]] = []
    gate_ctx = _full_pr3h_push_gate_context()

    def fake_run(cmd: list[str], *, check: bool = True) -> str:
        ASSERTIONS.assertTrue(check)
        calls.append(list(cmd))
        if len(calls) == 1:
            gate_ctx["post_fix_audit"] = "FAIL"
            raise RuntimeError("failed to push some refs to origin (non-fast-forward)")
        return ""

    result = flow.push_with_retry_once(fake_run, "owner/repo", "feature/branch", context=gate_ctx)

    ASSERTIONS.assertFalse(result["ok"])
    ASSERTIONS.assertEqual(result["status"], "needs_manual")
    ASSERTIONS.assertTrue(result["retried"])
    ASSERTIONS.assertTrue(result["needs_manual"])
    ASSERTIONS.assertEqual(calls, [["git", "push", "origin", "feature/branch"]])


def test_push_with_retry_once_non_fast_forward_allows_retry_with_refreshed_retry_gate_context():
    """Retry path proceeds only when explicit refreshed retry context is provided."""
    calls: list[list[str]] = []
    gate_ctx = {**_full_pr3h_push_gate_context(), **_retry_refresh_gate_context()}

    def fake_run(cmd: list[str], *, check: bool = True) -> str:
        ASSERTIONS.assertTrue(check)
        calls.append(list(cmd))
        if len(calls) == 1:
            raise RuntimeError("failed to push some refs to origin (non-fast-forward)")
        return ""

    result = flow.push_with_retry_once(fake_run, "owner/repo", "feature/branch", context=gate_ctx)

    ASSERTIONS.assertTrue(result["ok"])
    ASSERTIONS.assertEqual(result["status"], "success")
    ASSERTIONS.assertTrue(result["retried"])
    ASSERTIONS.assertEqual(calls[1], ["git", "fetch", "origin", "feature/branch"])
    ASSERTIONS.assertEqual(calls[2], ["git", "push", "origin", "feature/branch", "--force-with-lease"])


def test_push_with_retry_once_non_fast_forward_blocks_retry_with_stale_retry_gate_context():
    """Retry path denies when retry gate context is present but not explicitly refreshed."""
    calls: list[list[str]] = []
    gate_ctx = {**_full_pr3h_push_gate_context(), "retry_push_gate_context": {"explicitly_refreshed": False}}

    def fake_run(cmd: list[str], *, check: bool = True) -> str:
        ASSERTIONS.assertTrue(check)
        calls.append(list(cmd))
        if len(calls) == 1:
            raise RuntimeError("failed to push some refs to origin (non-fast-forward)")
        return ""

    result = flow.push_with_retry_once(fake_run, "owner/repo", "feature/branch", context=gate_ctx)

    ASSERTIONS.assertFalse(result["ok"])
    ASSERTIONS.assertTrue(result["retried"])
    ASSERTIONS.assertEqual(calls, [["git", "push", "origin", "feature/branch"]])


def test_cmd_canary_requires_explicit_pr3h_gate_context(monkeypatch):
    """Canary create must fail-closed without explicit invocation context."""
    calls: list[list[str]] = []

    def _fake_sh(cmd: list[str], *, check: bool = True) -> str:
        ASSERTIONS.assertTrue(check)
        calls.append(list(cmd))
        return ""

    monkeypatch.setattr(flow, "sh", _fake_sh)
    monkeypatch.setattr(flow.Path, "write_text", lambda *_args, **_kwargs: 1)
    with ASSERTIONS.assertRaisesRegex(RuntimeError, "missing required --post-fix-gate-context"):
        flow.cmd_canary(argparse.Namespace(repo="owner/repo", mode="create", post_fix_gate_context=""))
    ASSERTIONS.assertEqual(calls, [])


def test_cmd_canary_cleanup_does_not_require_create_gate_context(monkeypatch):
    """Canary cleanup mode should run without requiring create-mode gate context."""
    calls: list[list[str]] = []

    monkeypatch.setattr(
        flow,
        "gh_json",
        lambda _cmd: [{"number": 1, "headRefName": "test/safe-autofix-canary-1", "title": "safe autofix canary"}],
    )

    def _fake_sh(cmd: list[str], *, check: bool = True) -> str:
        calls.append(list(cmd))
        ASSERTIONS.assertFalse(check)
        return ""

    monkeypatch.setattr(flow, "sh", _fake_sh)
    result = flow.cmd_canary(argparse.Namespace(repo="owner/repo", mode="cleanup", post_fix_gate_context=""))

    ASSERTIONS.assertEqual(result, 0)
    ASSERTIONS.assertEqual(
        calls,
        [["gh", "pr", "close", "1", "--repo", "owner/repo", "--delete-branch"]],
    )


def test_cmd_canary_passes_explicit_pr3h_gate_context_to_push_with_retry_once(monkeypatch):
    """Canary create path must pass provided explicit context to guarded push helper."""
    captured: dict[str, Any] = {}
    monkeypatch.setattr(flow, "sh", lambda *_args, **_kwargs: "")
    monkeypatch.setattr(flow.Path, "write_text", lambda *_args, **_kwargs: 1)
    explicit_ctx = json.dumps(_full_pr3h_push_gate_context())
    monkeypatch.setattr(
        flow,
        "push_with_retry_once",
        lambda _run, _repo, _branch, *, remote="origin", context=None: (
            captured.update({"remote": remote, "context": context}) or {"ok": False, "error": "stop"}
        ),
    )
    with ASSERTIONS.assertRaises(RuntimeError):
        flow.cmd_canary(
            argparse.Namespace(repo="owner/repo", mode="create", post_fix_gate_context=explicit_ctx)
        )
    ASSERTIONS.assertEqual(captured["remote"], "origin")
    ASSERTIONS.assertEqual(captured["context"], _full_pr3h_push_gate_context())


def test_cmd_canary_create_denied_gate_aborts_before_mutation(monkeypatch):
    """Canary create must abort before fetch/checkout/write when gate denies."""
    calls: list[list[str]] = []
    monkeypatch.setattr(flow, "sh", lambda cmd, *, check=True: (calls.append(list(cmd)) or ""))
    monkeypatch.setattr(flow.Path, "write_text", lambda *_args, **_kwargs: 1)
    denied_ctx = json.dumps({"post_fix_audit": "FAIL"})

    with ASSERTIONS.assertRaisesRegex(RuntimeError, "post-fix audit gate denied before canary mutation"):
        flow.cmd_canary(argparse.Namespace(repo="owner/repo", mode="create", post_fix_gate_context=denied_ctx))

    ASSERTIONS.assertEqual(calls, [])


def test_cmd_canary_create_malformed_gate_context_aborts_before_mutation(monkeypatch):
    """Canary create must fail on malformed context before any mutation command."""
    calls: list[list[str]] = []
    monkeypatch.setattr(flow, "sh", lambda cmd, *, check=True: (calls.append(list(cmd)) or ""))
    monkeypatch.setattr(flow.Path, "write_text", lambda *_args, **_kwargs: 1)

    with ASSERTIONS.assertRaises(json.JSONDecodeError):
        flow.cmd_canary(argparse.Namespace(repo="owner/repo", mode="create", post_fix_gate_context="{"))

    ASSERTIONS.assertEqual(calls, [])


def test_canary_timestamp_utc_shape_is_branch_safe_without_strftime() -> None:
    """Canary timestamp format should stay branch-safe and avoid strftime."""
    value = flow.canary_timestamp_utc()
    ASSERTIONS.assertRegex(value, r"^\d{8}-\d{6}$")
    ASSERTIONS.assertNotIn("strftime", inspect.getsource(flow.canary_timestamp_utc))


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


def test_classify_merge_conflict_reports_merge_conflict_category():
    """Public merge-conflict classifier should report merge_conflict for conflicting PR data."""
    result = flow.classify_merge_conflict(
        {
            "mergeable": "CONFLICTING",
            "mergeStateStatus": "DIRTY",
            "conflicted_files": ["scripts/pr_flow_automation.py"],
        },
        ["scripts/pr_flow_automation.py"],
        {},
    )
    ASSERTIONS.assertEqual(result["category"], "merge_conflict")
    ASSERTIONS.assertEqual(result["next_action"], "needs_manual_merge_conflict")


def test_classify_merge_conflict_clean_pr_not_merge_conflict():
    """Public merge-conflict classifier should not report merge_conflict for clean PR data."""
    result = flow.classify_merge_conflict(
        {"mergeable": "MERGEABLE", "mergeStateStatus": "CLEAN"},
        [],
        {},
    )
    ASSERTIONS.assertNotEqual(result.get("category"), "merge_conflict")


def test_merge_conflict_taxonomy_items_passes_effective_task_scope(monkeypatch):
    """Taxonomy merge-conflict classifier should receive explicit task scope unchanged."""
    captured: dict[str, Any] = {}

    def _fake_classifier(
        pr_data: dict[str, Any],
        conflicted_files: list[str],
        task_scope: dict[str, Any],
    ) -> dict[str, Any]:
        captured["pr_data"] = pr_data
        captured["conflicted_files"] = conflicted_files
        captured["task_scope"] = task_scope
        return {"category": "merge_conflict", "next_action": "needs_manual_merge_conflict"}

    monkeypatch.setattr(flow.controller, "classify_merge_conflict", _fake_classifier)
    pr_data = {
        "mergeable": "CONFLICTING",
        "mergeStateStatus": "DIRTY",
        "conflicted_files": ["scripts/pr_flow_automation.py"],
    }
    scope = {"files": ["scripts/pr_flow_automation.py"]}
    taxonomy_items = getattr(flow, "_merge_conflict_taxonomy_items")
    items = taxonomy_items(pr_data, scope)

    ASSERTIONS.assertEqual(len(items), 1)
    ASSERTIONS.assertEqual(cast(dict[str, Any], captured["task_scope"]), scope)
    ASSERTIONS.assertEqual(
        cast(list[str], captured["conflicted_files"]),
        ["scripts/pr_flow_automation.py"],
    )


def test_merge_conflict_taxonomy_scope_fallback_manual_when_scope_absent():
    """Missing task scope must preserve manual/default-deny merge-conflict routing."""
    pr_data = {
        "mergeable": "CONFLICTING",
        "mergeStateStatus": "DIRTY",
        "conflicted_files": ["scripts/pr_flow_automation.py"],
    }
    taxonomy_items = getattr(flow, "_merge_conflict_taxonomy_items")
    items = taxonomy_items(pr_data, {})

    ASSERTIONS.assertEqual(len(items), 1)
    item = items[0]
    ASSERTIONS.assertEqual(item.get("category"), "merge_conflict")
    ASSERTIONS.assertTrue(bool(item.get("denied")))
    ASSERTIONS.assertEqual(item.get("next_action"), "needs_manual_merge_conflict")
    ASSERTIONS.assertIn(item.get("reason"), {"path_default_deny_empty_allowlist", "path_not_allowlisted"})


def test_effective_merge_conflict_scope_prefers_explicit_scope_from_pr_data():
    """Explicit task scope in PR data should propagate to merge-conflict classification input."""
    scope_resolver = getattr(flow, "_effective_merge_conflict_scope")
    result = scope_resolver(
        {
            "task_scope": {"files": ["scripts/pr_flow_automation.py"], "ignored": ["x"]},
            "scope": {"files": ["scripts/other.py"]},
        },
        {},
    )
    ASSERTIONS.assertEqual(result, {"files": ["scripts/pr_flow_automation.py"]})


def test_extract_effective_scope_equivalence_with_expected_keys_and_trimming():
    """Scope extraction should preserve supported keys, values, and default-deny semantics."""
    extractor = getattr(flow, "_extract_effective_scope")
    scope = {
        "files": [" scripts/a.py ", "", None],
        "allowed_files": ["tests/scripts/test_pr_flow_automation.py"],
        "allowlist": [123, " scripts/pr_flow_automation.py "],
        "in_scope_files": [],
        "extra": ["not-copied"],
    }
    ASSERTIONS.assertEqual(
        extractor(scope),
        {
            "files": ["scripts/a.py"],
            "allowed_files": ["tests/scripts/test_pr_flow_automation.py"],
            "allowlist": ["scripts/pr_flow_automation.py"],
        },
    )


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


def _review_nodes_for_auto_resolve() -> list[dict[str, Any]]:
    return [
        {"id": "a", "body": "stale advisory nit", "isResolved": False, "isOutdated": False},
        {
            "id": "b",
            "body": "active bypass in guard path",
            "isResolved": False,
            "isOutdated": False,
            "reproducible": True,
        },
        {"id": "c", "body": "future scope roadmap", "isResolved": False, "isOutdated": False},
    ]


def _review_evidence_context(**extra: Any) -> dict[str, Any]:
    context: dict[str, Any] = {
        "current_head_sha": "abc",
        "evidence_head_sha": "abc",
        "checks_green": True,
        "tests": ["pytest"],
    }
    context.update(extra)
    return context


def _deepsource_python_failure_check(**extra: Any) -> dict[str, Any]:
    check: dict[str, Any] = {
        "name": "DeepSource: Python",
        "provider": "deepsource",
        "status": "COMPLETED",
        "conclusion": "FAILURE",
    }
    check.update(extra)
    return check


def _deepsource_advisory_status_context(**extra: Any) -> dict[str, Any]:
    context: dict[str, Any] = {
        "current_head_sha": "abc",
        "evidence_head_sha": "abc",
        "evidence_present": True,
        "deepsource_advisory_evidence": "Cyclomatic complexity readability advisory",
        "codacy_state": "SUCCESS",
        "codacy_annotations_count": 0,
        "unresolved_active": 0,
        "pending_checks": [],
        "checks_green": True,
        "deepsource_required_current_head_check_failing": False,
    }
    context.update(extra)
    return context


def test_deepsource_advisory_status_full_evidence_nonblocking():
    """Completed DeepSource Python advisory failure is nonblocking with full current evidence."""
    decision = flow.deepsource_advisory_status_nonblocking_evidence(
        _deepsource_python_failure_check(),
        _deepsource_advisory_status_context(),
    )
    ASSERTIONS.assertTrue(decision["nonblocking"])
    ASSERTIONS.assertEqual(decision["next_action"], "nonblocking")


def test_deepsource_advisory_status_missing_evidence_head_sha_manual():
    """Missing evidence head SHA keeps DeepSource advisory status manual."""
    decision = flow.deepsource_advisory_status_nonblocking_evidence(
        _deepsource_python_failure_check(),
        _deepsource_advisory_status_context(evidence_head_sha=""),
    )
    ASSERTIONS.assertFalse(decision["nonblocking"])
    ASSERTIONS.assertEqual(decision["reason"], "missing_evidence_head_sha")


def test_deepsource_advisory_status_stale_evidence_head_sha_manual():
    """Stale evidence head SHA keeps DeepSource advisory status manual."""
    decision = flow.deepsource_advisory_status_nonblocking_evidence(
        _deepsource_python_failure_check(),
        _deepsource_advisory_status_context(evidence_head_sha="old"),
    )
    ASSERTIONS.assertFalse(decision["nonblocking"])
    ASSERTIONS.assertEqual(decision["reason"], "evidence_head_mismatch")


def test_deepsource_advisory_status_missing_codacy_annotations_manual():
    """Missing Codacy annotation evidence keeps DeepSource advisory status manual."""
    context = _deepsource_advisory_status_context()
    context.pop("codacy_annotations_count")
    decision = flow.deepsource_advisory_status_nonblocking_evidence(
        _deepsource_python_failure_check(),
        context,
    )
    ASSERTIONS.assertFalse(decision["nonblocking"])
    ASSERTIONS.assertEqual(decision["reason"], "codacy_evidence_not_clear")


def test_deepsource_advisory_status_codacy_action_required_or_annotations_manual():
    """Codacy action-required or annotation evidence keeps advisory status manual."""
    for context in (
        _deepsource_advisory_status_context(codacy_state="ACTION_REQUIRED"),
        _deepsource_advisory_status_context(codacy_annotations_count=1),
    ):
        decision = flow.deepsource_advisory_status_nonblocking_evidence(
            _deepsource_python_failure_check(),
            context,
        )
        ASSERTIONS.assertFalse(decision["nonblocking"])
        ASSERTIONS.assertEqual(decision["reason"], "codacy_evidence_not_clear")


def test_deepsource_advisory_status_noncompleted_deepsource_manual():
    """Cancelled, timed-out, stale, or in-progress DeepSource states stay manual."""
    checks = (
        _deepsource_python_failure_check(conclusion="CANCELLED"),
        _deepsource_python_failure_check(conclusion="TIMED_OUT"),
        _deepsource_python_failure_check(conclusion="STALE"),
        _deepsource_python_failure_check(status="IN_PROGRESS"),
    )
    for check in checks:
        decision = flow.deepsource_advisory_status_nonblocking_evidence(
            check,
            _deepsource_advisory_status_context(),
        )
        ASSERTIONS.assertFalse(decision["nonblocking"])
        ASSERTIONS.assertEqual(decision["reason"], "deepsource_status_not_completed_failure")


def test_deepsource_advisory_status_url_only_advisory_words_manual():
    """Advisory words in URLs alone must not create advisory evidence."""
    decision = flow.deepsource_advisory_status_nonblocking_evidence(
        _deepsource_python_failure_check(detailsUrl="https://example.test/readability-advisory"),
        _deepsource_advisory_status_context(deepsource_advisory_evidence=""),
    )
    ASSERTIONS.assertFalse(decision["nonblocking"])
    ASSERTIONS.assertEqual(decision["reason"], "missing_advisory_evidence")


def test_deepsource_advisory_status_safety_security_fail_open_manual():
    """DeepSource safety/security/fail-open/crash signals stay manual."""
    decision = flow.deepsource_advisory_status_nonblocking_evidence(
        _deepsource_python_failure_check(),
        _deepsource_advisory_status_context(
            deepsource_advisory_evidence="Readability advisory but fail-open crash regression remains"
        ),
    )
    ASSERTIONS.assertFalse(decision["nonblocking"])
    ASSERTIONS.assertEqual(decision["reason"], "deepsource_safety_or_correctness_signal")


def test_deepsource_advisory_status_non_deepsource_failure_manual():
    """Non-DeepSource failures are never treated as advisory nonblocking."""
    decision = flow.deepsource_advisory_status_nonblocking_evidence(
        _deepsource_python_failure_check(name="Unit tests", provider="github-actions"),
        _deepsource_advisory_status_context(),
    )
    ASSERTIONS.assertFalse(decision["nonblocking"])
    ASSERTIONS.assertEqual(decision["reason"], "not_deepsource_python")


def test_auto_resolve_review_comments_contract_evidence_only_filters_strictly():
    """When evidence_only=True, only deterministic EVIDENCE_RESOLVE threads are eligible."""
    if not hasattr(flow, "eligible_review_comments_for_auto_resolve"):
        raise NotImplementedError("eligible_review_comments_for_auto_resolve not implemented")
    eligible = flow.eligible_review_comments_for_auto_resolve(
        _review_nodes_for_auto_resolve(),
        _review_evidence_context(evidence_only=True),
    )
    ASSERTIONS.assertEqual([item["id"] for item in eligible], ["a"])


def test_auto_resolve_review_comments_contract_non_evidence_mode_allows_valid_triage_outputs():
    """When evidence_only=False, deterministic triage outputs are eligible except resolved/inactive nodes."""
    eligible = flow.eligible_review_comments_for_auto_resolve(
        [
            {"id": "p", "body": "active bypass in guard path", "isResolved": False, "reproducible": True},
            {"id": "e", "body": "stale advisory nit", "isResolved": False},
            {"id": "m", "body": "future scope roadmap", "isResolved": False},
            {"id": "r", "body": "already resolved", "isResolved": True},
        ],
        _review_evidence_context(),
    )
    ASSERTIONS.assertEqual([item["id"] for item in eligible], ["p", "e", "m"])


def test_auto_resolve_review_comments_contract_fails_closed_without_evidence_context():
    """Missing evidence context should fail closed for evidence-only resolution."""
    eligible = flow.eligible_review_comments_for_auto_resolve(
        [{"id": "a", "body": "stale advisory nit", "isResolved": False}],
        {"evidence_only": True},
    )
    ASSERTIONS.assertEqual(eligible, [])


def test_auto_resolve_review_comments_contract_outdated_bypass_is_excluded_from_auto_resolve():
    """Outdated bypass thread is prefiltered from auto-resolve eligibility."""
    eligible = flow.eligible_review_comments_for_auto_resolve(
        [{"id": "a", "body": "stale advisory but fail open bypass remains", "isResolved": False, "isOutdated": True}],
        _review_evidence_context(),
    )
    ASSERTIONS.assertEqual(eligible, [])


def test_auto_resolve_review_comments_contract_ignores_malformed_nodes():
    """Malformed/non-dict nodes are ignored without raising type errors."""
    eligible = flow.eligible_review_comments_for_auto_resolve(
        [
            "not-a-dict",
            None,
            {"id": "ok", "body": "stale advisory nit", "isResolved": False},
        ],
        _review_evidence_context(),
    )
    ASSERTIONS.assertEqual([item["id"] for item in eligible], ["ok"])


def test_auto_resolve_review_comments_contract_outdated_normal_without_evidence_context_excluded():
    """Outdated advisory thread without evidence context must not inflate active blockers."""
    eligible = flow.eligible_review_comments_for_auto_resolve(
        [{"id": "o1", "body": "stale advisory nit", "isResolved": False, "isOutdated": True}],
        {"checks_green": True},
    )
    ASSERTIONS.assertEqual(eligible, [])


def test_auto_resolve_review_comments_contract_outdated_snake_case_without_evidence_context_excluded():
    """Outdated advisory thread using snake_case field must not inflate active blockers."""
    eligible = flow.eligible_review_comments_for_auto_resolve(
        [{"id": "o1s", "body": "stale advisory nit", "isResolved": False, "is_outdated": True}],
        {"checks_green": True},
    )
    ASSERTIONS.assertEqual(eligible, [])


def test_auto_resolve_review_comments_contract_resolved_snake_case_excluded():
    """Resolved thread using snake_case field must be excluded from eligibility."""
    eligible = flow.eligible_review_comments_for_auto_resolve(
        [{"id": "r1s", "body": "already resolved", "is_resolved": True}],
        _review_evidence_context(),
    )
    ASSERTIONS.assertEqual(eligible, [])


def test_auto_resolve_review_comments_contract_resolved_camel_case_excluded():
    """Resolved thread using camelCase field remains excluded from eligibility."""
    eligible = flow.eligible_review_comments_for_auto_resolve(
        [{"id": "r1", "body": "already resolved", "isResolved": True}],
        _review_evidence_context(),
    )
    ASSERTIONS.assertEqual(eligible, [])


def test_auto_resolve_review_comments_contract_outdated_snake_case_excluded_with_full_context():
    """Outdated snake_case thread remains excluded when evidence context is present."""
    eligible = flow.eligible_review_comments_for_auto_resolve(
        [{"id": "o2s", "body": "stale advisory nit", "isResolved": False, "is_outdated": True}],
        _review_evidence_context(),
    )
    ASSERTIONS.assertEqual(eligible, [])


def test_auto_resolve_review_comments_contract_active_unresolved_node_still_eligible():
    """Active unresolved thread remains eligible in non-evidence mode."""
    eligible = flow.eligible_review_comments_for_auto_resolve(
        [{"id": "a1", "body": "stale advisory nit", "isResolved": False, "is_outdated": False}],
        _review_evidence_context(evidence_only=False),
    )
    ASSERTIONS.assertEqual([item["id"] for item in eligible], ["a1"])


def test_deepsource_advisory_cannot_enter_patch_loop():
    """Deepsource advisory comments must not become synthetic review blockers."""
    eligible = flow.eligible_review_comments_for_auto_resolve(
        [
            {
                "id": "ds1",
                "author": "deepsource[bot]",
                "body": "Cyclomatic complexity and readability advisory",
                "isResolved": False,
            }
        ],
        _review_evidence_context(evidence_only=False),
    )
    ASSERTIONS.assertEqual(eligible, [])


def test_deepsource_duplicate_complexity_comment_not_patch_eligible_in_flow():
    """Deepsource duplicate complexity comments must not route PATCH_REQUIRED."""
    thread = {
        "id": "ds1b",
        "author": "deepsource[bot]",
        "body": "Duplicate cyclomatic complexity readability advisory",
        "isResolved": False,
    }
    triage = controller.triage_review_thread_contract(
        thread,
        _review_evidence_context(evidence_only=False),
    )
    eligible = flow.eligible_review_comments_for_auto_resolve(
        [thread],
        _review_evidence_context(evidence_only=False),
    )
    ASSERTIONS.assertNotEqual(triage["decision"], "PATCH_REQUIRED")
    ASSERTIONS.assertEqual(eligible, [])


def test_deepsource_broad_manual_remains_visible_in_flow():
    """Deepsource broad refactor NEEDS_MANUAL should remain visible."""
    eligible = flow.eligible_review_comments_for_auto_resolve(
        [
            {
                "id": "ds-manual",
                "author": "deepsource[bot]",
                "body": "Broad refactor suggestion for future roadmap",
                "isResolved": False,
            }
        ],
        _review_evidence_context(evidence_only=False),
    )
    ASSERTIONS.assertEqual([item["id"] for item in eligible], ["ds-manual"])


def test_deepsource_advisory_evidence_only_can_resolve_with_current_head_tests():
    """Deepsource advisory resolution is allowed only through evidence-only routing."""
    eligible = flow.eligible_review_comments_for_auto_resolve(
        [
            {
                "id": "ds2",
                "author": "deepsource-app",
                "body": "Maintainability readability advisory",
                "isResolved": False,
            }
        ],
        _review_evidence_context(evidence_only=True),
    )
    ASSERTIONS.assertEqual([item["id"] for item in eligible], ["ds2"])


def _deepsource_required_check_context(**extra: Any) -> dict[str, Any]:
    """Build Deepsource required-check context for flow eligibility tests."""
    check = {
        "name": "DeepSource: Python",
        "conclusion": "FAILURE",
        "required": True,
    } | extra.pop("check", {})
    return {"current_head_sha": "abc", "failing_current_head_checks": [check]} | extra


def test_deepsource_required_current_head_failure_remains_patch_eligible():
    """Deepsource required current-head failure remains patch eligible."""
    eligible = flow.eligible_review_comments_for_auto_resolve(
        [
            {
                "id": "ds3",
                "author": "DeepSource: Python",
                "body": "Cyclomatic complexity is high",
                "isResolved": False,
            }
        ],
        _deepsource_required_check_context(check={"head_sha": "abc"}),
    )
    ASSERTIONS.assertEqual([item["id"] for item in eligible], ["ds3"])


def test_deepsource_required_check_missing_current_head_fails_closed_in_flow():
    """Deepsource required check without current head is not patch eligible."""
    eligible = flow.eligible_review_comments_for_auto_resolve(
        [{"id": "ds4", "author": "DeepSource: Python", "body": "Required check is failing"}],
        _deepsource_required_check_context(current_head_sha="", check={"head_sha": "abc"}),
    )
    ASSERTIONS.assertEqual(eligible, [])


def test_deepsource_required_check_missing_check_head_fails_closed_in_flow():
    """Deepsource required check without check head is not patch eligible."""
    eligible = flow.eligible_review_comments_for_auto_resolve(
        [{"id": "ds5", "author": "DeepSource: Python", "body": "Required check is failing"}],
        _deepsource_required_check_context(),
    )
    ASSERTIONS.assertEqual(eligible, [])


def test_unknown_author_complexity_text_still_fails_closed_in_flow():
    """Unknown author complexity text remains visible as manual flow work."""
    eligible = flow.eligible_review_comments_for_auto_resolve(
        [
            {
                "id": "u1",
                "author": "unknown-reviewer",
                "body": "Cyclomatic complexity is high",
                "isResolved": False,
            }
        ],
        _review_evidence_context(evidence_only=False),
    )
    ASSERTIONS.assertEqual([item["id"] for item in eligible], ["u1"])


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


def _review_threads_page(ids: list[tuple[str, bool]], has_next: bool, end_cursor: str) -> dict[str, Any]:
    return {
        "data": {
            "repository": {
                "pullRequest": {
                    "reviewThreads": {
                        "nodes": [
                            {"id": thread_id, "isResolved": is_resolved, "isOutdated": False}
                            for thread_id, is_resolved in ids
                        ],
                        "pageInfo": {"hasNextPage": has_next, "endCursor": end_cursor},
                    }
                }
            }
        }
    }


def _stub_two_page_review_threads(monkeypatch) -> None:
    responses = iter(
        [
            _review_threads_page([("a", True)], has_next=True, end_cursor="cursor-1"),
            _review_threads_page([("b", False)], has_next=False, end_cursor="cursor-2"),
        ]
    )
    monkeypatch.setattr(flow, "gh_json", lambda *_args, **_kwargs: next(responses))


def test_fetch_all_review_threads_paginates(monkeypatch):
    """Review thread collection should continue through all GraphQL pages."""
    _stub_two_page_review_threads(monkeypatch)
    threads = flow.fetch_all_review_threads("owner/repo", "225")
    ASSERTIONS.assertEqual([thread["id"] for thread in threads], ["a", "b"])


def test_fetch_all_review_threads_first_page_omits_empty_cursor(monkeypatch):
    """First GraphQL request should not pass an empty after cursor."""
    captured: list[list[str]] = []
    responses = iter([_review_threads_page([], has_next=False, end_cursor="")])

    def _fake_gh_json(cmd: list[str], **_kwargs):
        captured.append(cmd)
        return next(responses)

    monkeypatch.setattr(flow, "gh_json", _fake_gh_json)
    flow.fetch_all_review_threads("owner/repo", "225")
    ASSERTIONS.assertFalse(any(part == "after=" for part in captured[0]))


def test_fetch_all_review_threads_second_page_sends_cursor(monkeypatch):
    """Second GraphQL request should pass after=<cursor>."""
    captured: list[list[str]] = []
    responses = iter(
        [
            _review_threads_page([], has_next=True, end_cursor="cursor-1"),
            _review_threads_page([], has_next=False, end_cursor="cursor-2"),
        ]
    )

    def _fake_gh_json(cmd: list[str], **_kwargs):
        captured.append(cmd)
        return next(responses)

    monkeypatch.setattr(flow, "gh_json", _fake_gh_json)
    flow.fetch_all_review_threads("owner/repo", "225")
    ASSERTIONS.assertTrue(any(part == "after=cursor-1" for part in captured[1]))


def _run_cmd_report_second_page_thread(tmp_path, monkeypatch) -> dict[str, object]:
    monkeypatch.setattr(flow, "pr_view", _ready_to_merge_pr_view)
    monkeypatch.setattr(flow, "fetch_codacy_pr_issues", lambda *_args: ({}, []))
    _stub_two_page_review_threads(monkeypatch)
    rc = flow.cmd_report(
        argparse.Namespace(repo="owner/repo", pr="225", outdir=str(tmp_path), comment=False, no_fail=True)
    )
    ASSERTIONS.assertEqual(rc, 0)
    return json.loads((tmp_path / "pr-flow-decision.json").read_text(encoding="utf-8"))


def _assert_report_blocked_by_second_page_review_thread(decision: dict[str, object]) -> None:
    review = cast(dict[str, Any], decision["review"])
    ASSERTIONS.assertFalse(decision["can_merge"])
    ASSERTIONS.assertEqual(decision["next_action"], "fix_review_comments")
    ASSERTIONS.assertEqual(review["unresolved_active"], 1)
    ASSERTIONS.assertEqual(review["total_threads"], 2)


def test_cmd_report_second_page_unresolved_review_thread_blocks_merge(tmp_path, monkeypatch):
    """An unresolved thread from a later page should route to fix_review_comments."""
    decision = _run_cmd_report_second_page_thread(tmp_path, monkeypatch)
    _assert_report_blocked_by_second_page_review_thread(decision)


def test_cmd_readiness_passes_review_threads_to_build_decision(monkeypatch):
    """Readiness should fetch review threads and pass them to build_decision."""
    expected_threads = [{"id": "thread-1", "isResolved": False, "isOutdated": False}]
    captured: dict[str, object] = {}

    monkeypatch.setattr(flow, "fetch_all_review_threads", lambda *_args: expected_threads)

    def _fake_build_decision(_repo: str, _pr: str, *, ignore_self: bool, review_threads=None):
        captured["ignore_self"] = ignore_self
        captured["review_threads"] = review_threads
        return {
            "already_merged": False,
            "can_merge": False,
            "mergeable": "MERGEABLE",
            "mergeStateStatus": "CLEAN",
            "reasons": [],
            "next_action": "blocked",
        }

    monkeypatch.setattr(flow, "build_decision", _fake_build_decision)
    rc = flow.cmd_readiness(
        argparse.Namespace(
            repo="owner/repo",
            pr="225",
            ignore_safe_autofix=True,
            wait_unknown_seconds=0,
            poll_seconds=0,
            output="",
            no_fail=True,
        )
    )

    ASSERTIONS.assertEqual(rc, 0)
    ASSERTIONS.assertEqual(captured["review_threads"], expected_threads)


def test_cmd_readiness_active_review_thread_blocks_merge(monkeypatch):
    """Readiness should remain blocked when review threads include an active unresolved thread."""
    monkeypatch.setattr(
        flow,
        "fetch_all_review_threads",
        lambda *_args: [{"id": "thread-1", "isResolved": False, "isOutdated": False}],
    )

    def _fake_build_decision(_repo: str, _pr: str, *, ignore_self: bool, review_threads=None):
        ASSERTIONS.assertEqual(ignore_self, True)
        ASSERTIONS.assertEqual(review_threads, [{"id": "thread-1", "isResolved": False, "isOutdated": False}])
        return {
            "already_merged": False,
            "can_merge": False,
            "mergeable": "MERGEABLE",
            "mergeStateStatus": "CLEAN",
            "reasons": ["active unresolved review thread"],
            "next_action": "fix_review_comments",
        }

    monkeypatch.setattr(flow, "build_decision", _fake_build_decision)
    rc = flow.cmd_readiness(
        argparse.Namespace(
            repo="owner/repo",
            pr="225",
            ignore_safe_autofix=True,
            wait_unknown_seconds=0,
            poll_seconds=0,
            output="",
            no_fail=False,
        )
    )

    ASSERTIONS.assertEqual(rc, 1)


def _stub_readiness_fetch_failure(monkeypatch) -> None:
    monkeypatch.setattr(
        flow,
        "fetch_all_review_threads",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("api down")),
    )


def _run_readiness_cmd_failure_case(output: str = "") -> int:
    return flow.cmd_readiness(
        argparse.Namespace(
            repo="owner/repo",
            pr="225",
            ignore_safe_autofix=True,
            wait_unknown_seconds=0,
            poll_seconds=0,
            output=output,
            no_fail=False,
        )
    )


def _ready_to_merge_decision_payload() -> dict[str, Any]:
    return {
        "already_merged": False,
        "can_merge": True,
        "mergeable": "MERGEABLE",
        "mergeStateStatus": "CLEAN",
        "reasons": [],
        "next_action": "ready_to_merge",
    }


def _stub_build_decision_ready_to_merge(monkeypatch) -> None:
    monkeypatch.setattr(flow, "build_decision", lambda *_args, **_kwargs: _ready_to_merge_decision_payload())


def test_cmd_readiness_review_thread_fetch_failure_fails_closed(monkeypatch):
    """Readiness must fail closed when review thread fetch fails."""
    captured: dict[str, object] = {}
    _stub_readiness_fetch_failure(monkeypatch)

    def _fake_build_decision(_repo: str, _pr: str, *, ignore_self: bool, review_threads=None):
        ASSERTIONS.assertTrue(ignore_self)
        captured["review_threads"] = review_threads
        return {
            "already_merged": False,
            "can_merge": True,
            "mergeable": "MERGEABLE",
            "mergeStateStatus": "CLEAN",
            "reasons": [],
            "next_action": "ready_to_merge",
        }

    monkeypatch.setattr(flow, "build_decision", _fake_build_decision)
    rc = _run_readiness_cmd_failure_case()
    ASSERTIONS.assertEqual(rc, 1)
    ASSERTIONS.assertIsNone(captured["review_threads"])


def _assert_fail_closed_readiness_decision(rc: int, output: Any) -> None:
    decision = json.loads(output.read_text(encoding="utf-8"))
    ASSERTIONS.assertEqual(rc, 1)
    ASSERTIONS.assertFalse(decision["can_merge"])
    ASSERTIONS.assertEqual(decision["next_action"], "needs_manual_review_api")
    ASSERTIONS.assertIn("review_threads_api_unavailable", decision["reasons"])


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


def test_cmd_readiness_review_thread_fetch_failure_writes_fail_closed_decision(tmp_path, monkeypatch):
    """Readiness output should persist fail-closed decision when review API is unavailable."""
    _stub_readiness_fetch_failure(monkeypatch)
    _stub_build_decision_ready_to_merge(monkeypatch)
    output = tmp_path / "readiness.json"
    rc = _run_readiness_cmd_failure_case(str(output))
    _assert_fail_closed_readiness_decision(rc, output)




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
