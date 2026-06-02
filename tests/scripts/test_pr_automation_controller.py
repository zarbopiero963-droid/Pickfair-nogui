"""Tests for PR automation controller decisions."""
# pylint: disable=invalid-name,duplicate-code

import argparse
import copy
import hashlib
import json
import os
import re
import shlex
from pathlib import Path
from typing import Any, cast
from unittest import TestCase

import scripts.pr_automation_controller as controller

ASSERTIONS = TestCase()
NEXT_ACTION_ALLOWED = {
    "wait_pending",
    "fix_codacy_current_issues",
    "fix_github_codacy_annotations",
    "rerun_stale_checks",
    "run_final_micro_audit",
    "refresh_merge_readiness",
    "send_ready_to_merge_telegram",
    "needs_manual_no_progress",
    "needs_manual_regression",
    "needs_manual_budget_exhausted",
    "needs_manual_micro_audit_failed",
    "needs_manual",
    "ready_to_merge",
}


def _check(name: str, state: str, url: str = "") -> dict[str, str]:
    return {"name": name, "conclusion": state, "detailsUrl": url}


def _args() -> argparse.Namespace:
    return argparse.Namespace(
        repo="owner/repo",
        pr="225",
        output=".pr-controller/decision.json",
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


def _codacy_state_payload(
    *,
    state: str = "ACTION_REQUIRED",
    api_issues: int = 0,
    annotations: int = 0,
    issues: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    return {
        "github_codacy_state": state,
        "codacy_api_issues": api_issues,
        "github_annotations": annotations,
        "issues": issues or [],
    }


def _next_action_context(**overrides: object) -> dict[str, object]:
    context: dict[str, object] = {
        "pending_count": 0,
        "codacy_classification": "real_current_issues",
        "unresolved_active": 2,
        "can_merge": False,
    }
    context.update(overrides)
    return context


def _controller_decision(monkeypatch, *, blocking: bool, ignored: bool) -> dict:
    _stub_codacy_evidence(monkeypatch, blocking=blocking, ignored=ignored, issues=int(blocking))
    monkeypatch.setattr(controller, "active_safe_autofix_runs", lambda _repo: [])
    decision: dict = {"actions": [], "warnings": [], "errors": []}
    ctx = controller.build_next_action_context(_args(), decision, _codacy_pr(), ([], []))
    controller.decide_next_action(ctx)
    return decision


def _prepare_decision_tracking_test(monkeypatch, tmp_path) -> tuple[dict[str, Any], dict[str, Any], Any]:
    monkeypatch.setattr(controller, "_state_path_from_output", lambda _output: str(tmp_path / "pr-state-test.json"))
    monkeypatch.setattr(controller, "load_pr_automation_state", _mock_loaded_tracking_state)
    monkeypatch.setattr(controller, "save_pr_automation_state", lambda _path, _state: None)
    decision: dict[str, Any] = {"actions": [], "warnings": [], "errors": [], "pending_count": 0}
    pr: dict[str, Any] = {
        "statusCheckRollup": [],
        "mergeable": "MERGEABLE",
        "mergeStateStatus": "CLEAN",
        "headRefOid": "abc",
    }
    ctx = controller.build_next_action_context(_args(), decision, pr, ([], []))
    return decision, pr, ctx


def _mock_loaded_tracking_state(_path, _repo, _pr) -> dict[str, Any]:
    return controller.normalize_pr_automation_state(
        {"codacy_issue_count": 3, "review_active_count": 2, "bad_check_count": 1},
        "owner/repo",
        "225",
    )


def _active_context_test_inputs(tmp_path):
    task_file = tmp_path / "task.md"
    audit_file = tmp_path / "audit.md"
    context_dir = tmp_path / "ctx"
    task_file.write_text("task-body", encoding="utf-8")
    audit_file.write_text("audit-body", encoding="utf-8")
    args = argparse.Namespace(
        pr="229",
        task_command_file=str(task_file),
        final_micro_audit_file=str(audit_file),
        task_context_dir=str(context_dir),
    )
    decision: dict[str, Any] = {"pr_automation_state": {"controller_run_count": 3}}
    return args, decision, context_dir


def _assert_active_context_merge(decision: dict[str, Any], context_dir) -> None:
    ASSERTIONS.assertEqual((context_dir / "active-task-command.md").read_text(encoding="utf-8"), "task-body")
    ASSERTIONS.assertEqual((context_dir / "active-final-micro-audit.md").read_text(encoding="utf-8"), "audit-body")
    persisted = json.loads((context_dir / "active-task-state.json").read_text(encoding="utf-8"))
    ASSERTIONS.assertEqual(
        persisted["active_final_micro_audit_path"], str(context_dir / "active-final-micro-audit.md")
    )
    ASSERTIONS.assertEqual(decision["pr_automation_state"]["controller_run_count"], 3)
    ASSERTIONS.assertEqual(
        decision["active_final_micro_audit_path"], str(context_dir / "active-final-micro-audit.md")
    )


def _full_codex_contract_prompt() -> str:
    return controller.build_codex_task_prompt(
        {
            "task": "Fix lint",
            "objective": "Address blockers",
            "context": "Current PR repair",
            "current_behavior": "Some checks fail",
            "expected_behavior": "Checks pass after focused fix",
            "method": "Apply scoped code and test changes",
            "output_format": "Required response template",
            "files_to_inspect": ["scripts/pr_automation_controller.py"],
            "files_allowed": ["scripts/pr_automation_controller.py", "tests/scripts/test_pr_automation_controller.py"],
            "do_not_modify": ["scripts/pr_flow_automation.py"],
            "current_blockers": ["Prompt contract placeholder handling"],
            "required_fixes": ["Reject placeholder-only required section values"],
            "validation": ["python3 -m pytest tests/scripts/test_pr_automation_controller.py -q"],
            "stop_conditions": ["Do not edit unrelated files"],
        }
    )


def _default_prompt_context() -> dict[str, Any]:
    return {
        "task": "Fix prompt defaults",
        "objective": "Keep required contract sections initialized",
        "context": "Current PR repair request",
        "current_behavior": "Required sections can be uninitialized",
        "expected_behavior": "Required sections are always initialized",
        "files_to_inspect": ["scripts/pr_automation_controller.py"],
        "files_allowed": ["scripts/pr_automation_controller.py", "tests/scripts/test_pr_automation_controller.py"],
        "do_not_modify": ["scripts/pr_flow_automation.py"],
        "current_blockers": ["METHOD/OUTPUT FORMAT/STOP CONDITIONS default to placeholders"],
        "required_fixes": ["Provide safe non-placeholder defaults"],
        "validation": ["python3 -m pytest tests/scripts/test_pr_automation_controller.py -q"],
    }


def _phase0_pass_payload() -> dict[str, Any]:
    return {
        "status": "PASS",
        "risk_level": "low",
        "next_action": "generate_patch_prompt",
        "files_inspected": ["scripts/pr_automation_controller.py"],
        "static_analysis_rules": ["CCN<=10"],
        "workflows_affected": ["pr-flow-guardrails"],
        "authoritative_modules": ["scripts/pr_automation_controller.py"],
        "dangerous_gates": ["validate_codex_prompt_contract"],
        "files_allowed": ["scripts/pr_automation_controller.py"],
        "files_forbidden": ["scripts/pr_flow_automation.py"],
        "implementation_plan": ["patch validator"],
        "tests_to_run": ["python3 -m pytest tests/scripts/test_pr_automation_controller.py -q"],
        "stop_conditions": ["stop on scope violation"],
    }


def _assert_prompt_contains_lines(prompt: str, lines: tuple[str, ...]) -> None:
    for line in lines:
        ASSERTIONS.assertIn(line, prompt)


def _set_section_value(prompt: str, section: str, value: str) -> str:
    pattern = re.compile(
        rf"(?ms)^(?P<header>{re.escape(section)}:\n)(?P<body>.*?)(?=^[A-Z0-9][A-Z0-9 _-]*\s*(?::\s*)?$|\Z)"
    )
    return pattern.sub(rf"\g<header>{value}\n", prompt, count=1)


def _assert_contract_rejects_without_allowance(method_line: str) -> None:
    prompt = "\n".join([_full_codex_contract_prompt(), method_line])
    result = controller.validate_codex_prompt_contract(prompt)
    ASSERTIONS.assertIn("commit_or_push_instruction_without_explicit_allowance", result["reasons"])
    ASSERTIONS.assertFalse(result["valid"])


def _assert_contract_accepts_safe_commit_push_wording(method_line: str) -> None:
    prompt = "\n".join([_full_codex_contract_prompt(), method_line])
    result = controller.validate_codex_prompt_contract(prompt)
    ASSERTIONS.assertTrue(result["valid"])


def _assert_retry_task_blocks_commit_push(retry_task: str) -> None:
    ASSERTIONS.assertIn("Do not run git add/commit/push.", retry_task)
    ASSERTIONS.assertIn("Do not run git commit.", retry_task)
    ASSERTIONS.assertIn("Do not run git push.", retry_task)


def _extended_prompt_context() -> dict[str, Any]:
    context = _default_prompt_context()
    context.update(
        {
            "task": "workflow hygiene",
            "objective": "contracted prompts",
            "current_behavior": "Placeholder-only values can pass validation",
            "expected_behavior": "Placeholder-only values must fail validation",
            "method": "Update prompt-contract value checks and tests",
            "output_format": "Required response format",
            "do_not_modify": ["scripts/pr_flow_automation.py", "tests/scripts/test_pr_flow_automation.py"],
            "current_blockers": ["validate_codex_prompt_contract accepts placeholder-only required sections"],
            "required_fixes": ["Invalidate placeholder-only section values for required sections"],
            "stop_conditions": ["Do not modify unrelated files"],
        }
    )
    return context


def _required_contract_prompt_with_caps_values() -> str:
    return "\n".join(
        _required_contract_prompt_caps_intro()
        + _required_contract_prompt_caps_scope()
        + _required_contract_prompt_caps_contract()
        + _required_contract_prompt_caps_output()
    )


def _required_contract_prompt_caps_intro() -> list[str]:
    return [
        "TASK:",
        "Fix parser behavior",
        "OBJECTIVE:",
        "PASS",
        "CONTEXT:",
        "CI",
    ]


def _required_contract_prompt_caps_scope() -> list[str]:
    return [
        "FILES TO INSPECT:",
        "- scripts/pr_automation_controller.py",
        "FILES ALLOWED:",
        "- scripts/pr_automation_controller.py",
        "- tests/scripts/test_pr_automation_controller.py",
        "DO NOT MODIFY:",
        "- scripts/pr_flow_automation.py",
    ]


def _required_contract_prompt_caps_contract() -> list[str]:
    return [
        "CURRENT BEHAVIOR:",
        "Uppercase values can be parsed as headers",
        "EXPECTED BEHAVIOR:",
        "Only required section headers stop section parsing",
        "CURRENT BLOCKERS:",
        "- CONTEXT value CI appears uninitialized",
        "REQUIRED FIXES:",
        "- limit section boundary recognition to required headers",
    ]


def _required_contract_prompt_caps_output() -> list[str]:
    return [
        "METHOD:",
        "- keep patch scoped",
        "VALIDATION:",
        "- python3 -m pytest tests/scripts/test_pr_automation_controller.py -q",
        "POST-FIX MICRO-AUDIT BEFORE COMMIT:",
        "Use required post-fix micro-audit checklist before commit.",
        "OUTPUT FORMAT:",
        "- final status DONE/PARTIAL/NEEDS_MANUAL",
        "STOP CONDITIONS:",
        "- do not edit unrelated files",
    ]


def _assert_contract_examples_rejected(method_lines: tuple[str, ...]) -> None:
    for method_line in method_lines:
        _assert_contract_rejects_without_allowance(method_line)


def _assert_contract_examples_accepted(method_lines: tuple[str, ...]) -> None:
    for method_line in method_lines:
        _assert_contract_accepts_safe_commit_push_wording(method_line)


def _contract_examples_rejected() -> tuple[str, ...]:
    return (
        "METHOD:\ngit commit",
        "METHOD:\ngit push",
        "METHOD:\ngit push origin branch",
        "METHOD:\ncommit changes",
        "METHOD:\nCommit the changes after checks.",
        "METHOD:\ncommit the patch",
        "METHOD:\nCommit after checks.",
        "METHOD:\npush origin branch",
        "METHOD:\nPush origin main after checks.",
        "METHOD:\npush the branch",
        "METHOD:\nPush this branch.",
    )


def _contract_examples_accepted() -> tuple[str, ...]:
    return (
        "METHOD:\ndo not commit",
        "METHOD:\ndo not push",
        "METHOD:\ndo not git push",
        "METHOD:\nno commit/push",
        "METHOD:\nBefore committing, perform audit.",
        "METHOD:\nPOST-FIX MICRO-AUDIT BEFORE COMMIT",
    )


def _default_method_output_stop_contract_lines() -> tuple[str, ...]:
    return (
        "- inspect relevant files first",
        "- verify each finding is still valid",
        "- keep patch minimal",
        "- add/update focused tests",
        "- run validation",
        "- do not commit/push unless explicitly allowed",
        "- files changed",
        "- summary of fix",
        "- tests run",
        "- post-fix audit result",
        "- final status DONE/PARTIAL/NEEDS_MANUAL",
        "- stop on scope violation",
        "- stop on failing post-fix audit",
        "- stop on validation failure",
        "- stop on ambiguous/high-risk changes needing human decision",
    )


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
    ASSERTIONS.assertIn("POST-FIX MICRO-AUDIT BEFORE COMMIT", task)
    ASSERTIONS.assertIn("Do not commit a patch that fails this audit.", task)


def test_ensure_post_fix_micro_audit_section_adds_no_commit_rule_once():
    """Post-fix audit section should be appended once and include no-commit-on-fail rule."""
    prompt = controller.ensure_post_fix_micro_audit_section("Fix these findings only.")
    twice = controller.ensure_post_fix_micro_audit_section(prompt)
    ASSERTIONS.assertIn("POST-FIX MICRO-AUDIT BEFORE COMMIT", prompt)
    ASSERTIONS.assertIn("Do not commit a patch that fails this audit.", prompt)
    ASSERTIONS.assertEqual(twice.count("POST-FIX MICRO-AUDIT BEFORE COMMIT"), 1)


def test_ensure_post_fix_micro_audit_section_is_idempotent_when_full_section_exists():
    """A complete audit section should not be appended again."""
    prompt = f"Fix these findings.\n\n{controller.POST_FIX_MICRO_AUDIT_SECTION}"
    updated = controller.ensure_post_fix_micro_audit_section(prompt)
    ASSERTIONS.assertEqual(updated.count("POST-FIX MICRO-AUDIT BEFORE COMMIT"), 1)
    ASSERTIONS.assertEqual(updated.count("Do not commit a patch that fails this audit."), 1)


def test_ensure_post_fix_micro_audit_section_appends_when_only_title_phrase_exists():
    """A bare title mention must still append the full checklist section."""
    prompt = "Reviewer note: remember POST-FIX MICRO-AUDIT BEFORE COMMIT before final push."
    updated = controller.ensure_post_fix_micro_audit_section(prompt)
    ASSERTIONS.assertEqual(updated.count("POST-FIX MICRO-AUDIT BEFORE COMMIT"), 2)
    ASSERTIONS.assertIn("Do not commit a patch that fails this audit.", updated)
    ASSERTIONS.assertIn("Check:", updated)


def test_build_post_fix_micro_audit_prompt_adds_changed_files_context_section():
    """Changed files list should append the audit-context section with one bullet per file."""
    prompt = controller.build_post_fix_micro_audit_prompt(
        "Fix these findings.",
        {"changed_files": ["scripts/pr_automation_controller.py", "tests/scripts/test_pr_automation_controller.py"]},
    )
    ASSERTIONS.assertIn("Audit context (changed files):", prompt)
    ASSERTIONS.assertIn("- scripts/pr_automation_controller.py", prompt)
    ASSERTIONS.assertIn("- tests/scripts/test_pr_automation_controller.py", prompt)


def test_build_post_fix_micro_audit_prompt_omits_changed_files_context_for_invalid_inputs():
    """Invalid/blank changed_files values should not append the audit-context section."""
    for context in (None, {"changed_files": "scripts/x.py"}, {"changed_files": []}, {"changed_files": ["", "  "]}):
        prompt = controller.build_post_fix_micro_audit_prompt("Fix these findings.", context)
        ASSERTIONS.assertNotIn("Audit context (changed files):", prompt)


def test_build_post_fix_micro_audit_prompt_filters_blank_changed_files_entries():
    """Changed-files context should keep exactly one bullet per non-empty path."""
    prompt = controller.build_post_fix_micro_audit_prompt(
        "Fix these findings.",
        {"changed_files": [" scripts/a.py ", "", "  ", "tests/a_test.py"]},
    )
    ASSERTIONS.assertIn("Audit context (changed files):", prompt)
    ASSERTIONS.assertEqual(prompt.count("\n- scripts/a.py\n"), 1)
    ASSERTIONS.assertEqual(prompt.count("\n- tests/a_test.py\n"), 1)


def test_build_codex_task_prompt_includes_required_sections():
    """Generated codex prompt contains required sections and validates successfully."""
    prompt = controller.build_codex_task_prompt(_extended_prompt_context())
    expected_headers = tuple(
        f"{section}:"
        for section in controller.CODEX_PROMPT_REQUIRED_SECTIONS
        if section != "POST-FIX MICRO-AUDIT BEFORE COMMIT"
    )
    _assert_prompt_contains_lines(prompt, expected_headers)
    ASSERTIONS.assertIn("POST-FIX MICRO-AUDIT BEFORE COMMIT", prompt)
    ASSERTIONS.assertIn("PHASE 0 PRE-FLIGHT (READ-ONLY)", prompt)
    ASSERTIONS.assertTrue(controller.validate_codex_prompt_contract(prompt)["valid"])


def test_build_codex_task_prompt_contains_single_canonical_post_fix_section():
    """Generated task prompt must include exactly one canonical post-fix checklist section."""
    prompt = controller.build_codex_task_prompt(_default_prompt_context())
    ASSERTIONS.assertEqual(prompt.count("POST-FIX MICRO-AUDIT BEFORE COMMIT"), 1)
    ASSERTIONS.assertIn("Did you fix every requested Codacy/review finding?", prompt)
    ASSERTIONS.assertIn("Do not commit a patch that fails this audit.", prompt)
    ASSERTIONS.assertTrue(controller.validate_codex_prompt_contract(prompt)["valid"])


def test_build_codex_task_prompt_phase0_preserves_explicit_files_allowed():
    """Phase 0 insertion should keep caller files_allowed values, not fallback placeholder text."""
    prompt = controller.build_codex_task_prompt({"files_allowed": ["scripts/pr_automation_controller.py"]})
    ASSERTIONS.assertIn("Task allowed files: scripts/pr_automation_controller.py", prompt)
    ASSERTIONS.assertNotIn("Task allowed files: (from task scope)", prompt)


def test_build_codex_task_prompt_minimal_context_populates_required_contract_defaults():
    """Minimal context keeps required contract sections initialized and valid."""
    prompt = controller.build_codex_task_prompt(_default_prompt_context())
    result = controller.validate_codex_prompt_contract(prompt)
    ASSERTIONS.assertTrue(result["valid"])
    ASSERTIONS.assertEqual(result["missing_sections"], [])
    ASSERTIONS.assertEqual(result["uninitialized_sections"], [])


def test_build_codex_task_prompt_defaults_include_method_output_stop_contract_text():
    """Default METHOD/OUTPUT FORMAT/STOP CONDITIONS sections include concrete contract bullets."""
    prompt = controller.build_codex_task_prompt(_default_prompt_context())
    _assert_prompt_contains_lines(prompt, _default_method_output_stop_contract_lines())


def test_ensure_codex_prompt_contract_appends_missing_sections():
    """Contract-enforcer appends missing sections and keeps required audit/preflight headers."""
    prompt = controller.ensure_codex_prompt_contract("TASK:\nFix this")
    ASSERTIONS.assertEqual(controller.codex_prompt_contract_missing_sections(prompt), [])
    ASSERTIONS.assertIn("POST-FIX MICRO-AUDIT BEFORE COMMIT", prompt)
    ASSERTIONS.assertEqual(prompt.count("POST-FIX MICRO-AUDIT BEFORE COMMIT"), 1)
    ASSERTIONS.assertIn("Did you fix every requested Codacy/review finding?", prompt)
    ASSERTIONS.assertIn("Do not commit a patch that fails this audit.", prompt)
    ASSERTIONS.assertIn("PHASE 0 PRE-FLIGHT (READ-ONLY)", prompt)
    result = controller.validate_codex_prompt_contract(_full_codex_contract_prompt())
    ASSERTIONS.assertTrue(result["valid"])


def test_ensure_codex_prompt_contract_replaces_placeholder_post_fix_with_canonical_section():
    """Placeholder post-fix section is removed and replaced by one canonical checklist section."""
    without_post_fix = re.sub(
        r"(?ms)^POST-FIX MICRO-AUDIT BEFORE COMMIT:\n.*?(?=^[A-Z0-9][A-Z0-9 _-]*\s*(?::\s*)?$|\Z)",
        "",
        _full_codex_contract_prompt(),
    ).strip()
    placeholder = "POST-FIX MICRO-AUDIT BEFORE COMMIT:\nTBD\n"
    prompt = controller.ensure_codex_prompt_contract(f"{without_post_fix}\n\n{placeholder}")
    ASSERTIONS.assertEqual(prompt.count("POST-FIX MICRO-AUDIT BEFORE COMMIT"), 1)
    ASSERTIONS.assertIn("Did you fix every requested Codacy/review finding?", prompt)
    ASSERTIONS.assertIn("Do not commit a patch that fails this audit.", prompt)
    result = controller.validate_codex_prompt_contract(prompt)
    ASSERTIONS.assertNotIn("POST-FIX MICRO-AUDIT BEFORE COMMIT", result["uninitialized_sections"])
    ASSERTIONS.assertTrue(result["valid"])


def test_ensure_codex_prompt_contract_replaces_noncanonical_post_fix_section_with_canonical():
    """Canonical post-fix section must be restored while preserving trailing owner/task notes."""
    prompt = controller.ensure_codex_prompt_contract(
        "\n".join(
            [
                "TASK:\nFix this",
                "OBJECTIVE:\nAddress blockers",
                "CONTEXT:\nCurrent PR repair",
                "VALIDATION:\n- python3 -m pytest -q",
                "POST-FIX MICRO-AUDIT BEFORE COMMIT:\n- custom note only",
            ]
        )
    )
    ASSERTIONS.assertEqual(prompt.count("POST-FIX MICRO-AUDIT BEFORE COMMIT"), 1)
    ASSERTIONS.assertIn("- custom note only", prompt)
    ASSERTIONS.assertIn("Did you fix every requested Codacy/review finding?", prompt)


def test_ensure_codex_prompt_contract_preserves_post_fix_trailing_owner_notes():
    """Normalization must preserve non-required trailing owner/task notes in post-fix section."""
    prompt = controller.ensure_codex_prompt_contract(
        "\n".join(
            [
                "TASK:\nFix this",
                "OBJECTIVE:\nAddress blockers",
                "CONTEXT:\nCurrent PR repair",
                "VALIDATION:\n- python3 -m pytest -q",
                "POST-FIX MICRO-AUDIT BEFORE COMMIT:",
                "- owner note: keep branch unchanged",
                "- task note: no workflow edits",
            ]
        )
    )
    ASSERTIONS.assertEqual(prompt.count("POST-FIX MICRO-AUDIT BEFORE COMMIT"), 1)
    ASSERTIONS.assertIn("- owner note: keep branch unchanged", prompt)
    ASSERTIONS.assertIn("- task note: no workflow edits", prompt)
    ASSERTIONS.assertIn("Do not commit a patch that fails this audit.", prompt)


def test_validate_codex_prompt_contract_reports_missing_sections_for_incomplete_prompt():
    """Incomplete prompt fails contract validation and reports required missing sections."""
    result = controller.validate_codex_prompt_contract("TASK:\nfix this")
    ASSERTIONS.assertIn("OBJECTIVE", result["missing_sections"])
    ASSERTIONS.assertIn("CONTEXT", result["missing_sections"])
    ASSERTIONS.assertFalse(result["valid"])


def test_validate_codex_prompt_contract_rejects_prompt_missing_phase0_preflight():
    """Prompt missing PHASE 0 preflight must be invalid with explicit reason."""
    prompt = _full_codex_contract_prompt().replace("PHASE 0 PRE-FLIGHT (READ-ONLY)\n\n", "", 1)
    result = controller.validate_codex_prompt_contract(prompt)
    ASSERTIONS.assertFalse(result["valid"])
    ASSERTIONS.assertIn("missing_phase0_preflight", result["reasons"])


def test_validate_codex_prompt_contract_requires_exact_task_header_not_prefix_text():
    """Prefix prose like Task allowed files must not satisfy required TASK header."""
    prompt = "\n".join(
        [
            "PHASE 0 PRE-FLIGHT (READ-ONLY)",
            "Task allowed files: scripts/pr_automation_controller.py",
            "OBJECTIVE:\nAddress blockers",
            "CONTEXT:\nScope is limited",
            "VALIDATION:\n- python3 -m pytest -q",
        ]
    )
    result = controller.validate_codex_prompt_contract(prompt)
    ASSERTIONS.assertIn("TASK", result["missing_sections"])
    ASSERTIONS.assertFalse(result["valid"])


def test_validate_codex_prompt_contract_requires_exact_objective_header_not_prefix_text():
    """Prefix prose like Objective details must not satisfy required OBJECTIVE header."""
    prompt = "\n".join(
        [
            "PHASE 0 PRE-FLIGHT (READ-ONLY)",
            "TASK:\nAddress blockers",
            "Objective details:\nScope is limited",
            "CONTEXT:\nScope is limited",
            "VALIDATION:\n- python3 -m pytest -q",
        ]
    )
    result = controller.validate_codex_prompt_contract(prompt)
    ASSERTIONS.assertIn("OBJECTIVE", result["missing_sections"])
    ASSERTIONS.assertFalse(result["valid"])


def test_validate_codex_prompt_contract_treats_all_caps_context_value_as_body_value():
    """All-caps values like CI must remain section body values, not headers."""
    prompt = _set_section_value(_full_codex_contract_prompt(), "CONTEXT", "CI")
    result = controller.validate_codex_prompt_contract(prompt)
    ASSERTIONS.assertNotIn("CONTEXT", result["uninitialized_sections"])
    ASSERTIONS.assertTrue(result["valid"])


def test_validate_codex_prompt_contract_treats_all_caps_objective_value_as_body_value():
    """All-caps values like PASS must remain section body values, not headers."""
    prompt = _set_section_value(_full_codex_contract_prompt(), "OBJECTIVE", "PASS")
    result = controller.validate_codex_prompt_contract(prompt)
    ASSERTIONS.assertNotIn("OBJECTIVE", result["uninitialized_sections"])
    ASSERTIONS.assertTrue(result["valid"])


def test_validate_codex_prompt_contract_required_headers_still_bound_section_bodies():
    """Recognized required headers still terminate a prior section body."""
    ensured = controller.ensure_phase0_preflight_section(_required_contract_prompt_with_caps_values())
    result = controller.validate_codex_prompt_contract(ensured)
    ASSERTIONS.assertEqual(result["missing_sections"], [])
    ASSERTIONS.assertEqual(result["uninitialized_sections"], [])
    ASSERTIONS.assertTrue(result["valid"])


def test_build_phase0_preflight_prompt_includes_static_analysis_checklist():
    """Phase-0 prompt includes static-analysis config files likely needed for triage."""
    prompt = controller.build_phase0_preflight_prompt({})
    ASSERTIONS.assertIn(".codacy.yml", prompt)
    ASSERTIONS.assertIn(".deepsource.toml", prompt)
    ASSERTIONS.assertIn("ruff config", prompt)
    ASSERTIONS.assertIn("radon/lizard/static-analysis config", prompt)


def test_build_phase0_preflight_prompt_includes_workflow_ci_awareness():
    """Phase-0 prompt includes workflow-read instructions while preserving read-only behavior."""
    prompt = controller.build_phase0_preflight_prompt({})
    ASSERTIONS.assertIn(".github/workflows/*.yml", prompt)
    ASSERTIONS.assertIn("affected workflows/checks", prompt)
    ASSERTIONS.assertIn("do not edit workflows unless explicitly allowed", prompt)


def test_build_phase0_preflight_prompt_includes_read_only_rules():
    """Phase-0 prompt explicitly preserves no-edit/no-commit/no-push guardrails."""
    prompt = controller.build_phase0_preflight_prompt({})
    ASSERTIONS.assertIn("READ-ONLY", prompt)
    ASSERTIONS.assertIn("Do not edit files", prompt)
    ASSERTIONS.assertIn("Do not commit", prompt)
    ASSERTIONS.assertIn("Do not push", prompt)


def test_parse_phase0_preflight_result_pass_json():
    """PASS JSON payload normalizes to PASS with generate_patch_prompt next action."""
    report = controller.parse_phase0_preflight_result(json.dumps(_phase0_pass_payload()))
    ASSERTIONS.assertEqual(report["status"], "PASS")
    ASSERTIONS.assertEqual(report["risk_level"], "low")
    ASSERTIONS.assertEqual(report["next_action"], "generate_patch_prompt")
    ASSERTIONS.assertFalse(controller.phase0_preflight_failed(report))


def test_parse_phase0_preflight_result_pass_json_phase0_preflight_key():
    """JSON PHASE_0_PREFLIGHT PASS with valid next_action should pass."""
    payload = _phase0_pass_payload()
    payload.pop("status", None)
    payload["PHASE_0_PREFLIGHT"] = "PASS"
    report = controller.parse_phase0_preflight_result(json.dumps(payload))
    ASSERTIONS.assertEqual(report["status"], "PASS")
    ASSERTIONS.assertEqual(report["next_action"], "generate_patch_prompt")


def test_parse_phase0_preflight_result_pass_json_status_key():
    """JSON status PASS with valid next_action should pass."""
    payload = _phase0_pass_payload()
    report = controller.parse_phase0_preflight_result(json.dumps(payload))
    ASSERTIONS.assertEqual(report["status"], "PASS")
    ASSERTIONS.assertEqual(report["next_action"], "generate_patch_prompt")


def test_parse_phase0_preflight_result_pass_json_uppercase_next_action_normalizes():
    """Complete PASS report with uppercase next_action should normalize and pass."""
    payload = _phase0_pass_payload()
    payload["next_action"] = "GENERATE_PATCH_PROMPT"
    report = controller.parse_phase0_preflight_result(json.dumps(payload))
    ASSERTIONS.assertEqual(report["status"], "PASS")
    ASSERTIONS.assertEqual(report["next_action"], "generate_patch_prompt")
    ASSERTIONS.assertFalse(controller.phase0_preflight_failed(report))


def test_parse_phase0_preflight_result_pass_json_mixed_case_next_action_normalizes():
    """Complete PASS report with mixed-case next_action should normalize and pass."""
    payload = _phase0_pass_payload()
    payload["next_action"] = "Generate_Patch_Prompt"
    report = controller.parse_phase0_preflight_result(json.dumps(payload))
    ASSERTIONS.assertEqual(report["status"], "PASS")
    ASSERTIONS.assertEqual(report["next_action"], "generate_patch_prompt")
    ASSERTIONS.assertFalse(controller.phase0_preflight_failed(report))


def test_parse_phase0_preflight_result_pass_json_spaced_next_action_normalizes():
    """Complete PASS report with spaced next_action should normalize and pass."""
    payload = _phase0_pass_payload()
    payload["next_action"] = "  Generate Patch Prompt  "
    report = controller.parse_phase0_preflight_result(json.dumps(payload))
    ASSERTIONS.assertEqual(report["status"], "PASS")
    ASSERTIONS.assertEqual(report["next_action"], "generate_patch_prompt")
    ASSERTIONS.assertFalse(controller.phase0_preflight_failed(report))


def test_parse_phase0_preflight_result_pass_json_hyphenated_next_action_normalizes():
    """Complete PASS report with hyphenated next_action should normalize and pass."""
    payload = _phase0_pass_payload()
    payload["next_action"] = "generate-patch-prompt"
    report = controller.parse_phase0_preflight_result(json.dumps(payload))
    ASSERTIONS.assertEqual(report["status"], "PASS")
    ASSERTIONS.assertEqual(report["next_action"], "generate_patch_prompt")
    ASSERTIONS.assertFalse(controller.phase0_preflight_failed(report))


def test_parse_phase0_preflight_result_parses_json_string_list_fields():
    """String-encoded bullet lists in JSON evidence fields should parse as lists."""
    payload = _phase0_pass_payload()
    payload["files_inspected"] = "- scripts/a.py"
    payload["tests_to_run"] = "- pytest"
    report = controller.parse_phase0_preflight_result(json.dumps(payload))
    ASSERTIONS.assertEqual(report["status"], "PASS")
    ASSERTIONS.assertEqual(report["files_inspected"], ["scripts/a.py"])
    ASSERTIONS.assertEqual(report["tests_to_run"], ["pytest"])
    ASSERTIONS.assertFalse(controller.phase0_preflight_failed(report))


def test_parse_phase0_preflight_result_incomplete_pass_fails_closed():
    """PASS payload missing required evidence fields must fail closed to NEEDS_MANUAL."""
    report = controller.parse_phase0_preflight_result(json.dumps({"status": "PASS"}))
    ASSERTIONS.assertEqual(report["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(report["next_action"], "needs_manual_phase0_failed")
    ASSERTIONS.assertTrue(controller.phase0_preflight_failed(report))


def test_parse_phase0_preflight_result_complete_pass_missing_next_action_fails_closed():
    """Complete PASS payload without next_action must fail closed to NEEDS_MANUAL."""
    payload = _phase0_pass_payload()
    payload.pop("next_action")
    report = controller.parse_phase0_preflight_result(json.dumps(payload))
    ASSERTIONS.assertEqual(report["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(report["next_action"], "needs_manual_phase0_failed")
    ASSERTIONS.assertTrue(controller.phase0_preflight_failed(report))


def test_parse_phase0_preflight_result_json_explicit_needs_manual_stops_before_later_pass():
    """First explicit PHASE_0_PREFLIGHT NEEDS_MANUAL candidate is authoritative."""
    manual = _phase0_pass_payload()
    manual.pop("status", None)
    manual["PHASE_0_PREFLIGHT"] = "NEEDS_MANUAL"
    manual["next_action"] = "needs_manual_phase0_failed"
    later_pass = _phase0_pass_payload()
    raw = "\n".join(["```json", json.dumps(manual), "```", "```json", json.dumps(later_pass), "```"])
    report = controller.parse_phase0_preflight_result(raw)
    ASSERTIONS.assertEqual(report["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(report["next_action"], "needs_manual_phase0_failed")


def test_parse_phase0_preflight_result_json_explicit_snake_needs_manual_stops_before_later_pass():
    """First explicit phase_0_preflight NEEDS_MANUAL candidate is authoritative."""
    manual = _phase0_pass_payload()
    manual.pop("status", None)
    manual["phase_0_preflight"] = "NEEDS_MANUAL"
    manual["next_action"] = "needs_manual_phase0_failed"
    later_pass = _phase0_pass_payload()
    raw = "\n".join(["```json", json.dumps(manual), "```", "```json", json.dumps(later_pass), "```"])
    report = controller.parse_phase0_preflight_result(raw)
    ASSERTIONS.assertEqual(report["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(report["next_action"], "needs_manual_phase0_failed")


def test_parse_phase0_preflight_result_json_manual_generate_patch_action_normalizes_to_manual():
    """NEEDS_MANUAL JSON candidate must not leak patch-like next_action."""
    payload = _phase0_pass_payload()
    payload.pop("status", None)
    payload["PHASE_0_PREFLIGHT"] = "NEEDS_MANUAL"
    payload["next_action"] = "generate_patch_prompt"
    report = controller.parse_phase0_preflight_result(json.dumps(payload))
    ASSERTIONS.assertEqual(report["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(report["next_action"], "needs_manual_phase0_failed")


def test_parse_phase0_preflight_result_phase0_pass_empty_next_action_fails_closed():
    """PHASE_0_PREFLIGHT PASS with empty next_action fails closed."""
    payload = _phase0_pass_payload()
    payload.pop("status", None)
    payload["PHASE_0_PREFLIGHT"] = "PASS"
    payload["next_action"] = "   "
    report = controller.parse_phase0_preflight_result(json.dumps(payload))
    ASSERTIONS.assertEqual(report["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(report["next_action"], "needs_manual_phase0_failed")


def test_parse_phase0_preflight_result_json_string_bullets_drop_empty_entries():
    """JSON string bullet evidence drops empty list items."""
    payload = _phase0_pass_payload()
    payload["files_inspected"] = "- scripts/a.py\n-\n-   \n- scripts/b.py"
    report = controller.parse_phase0_preflight_result(json.dumps(payload))
    ASSERTIONS.assertEqual(report["files_inspected"], ["scripts/a.py", "scripts/b.py"])


def test_parse_phase0_preflight_result_json_list_skips_none_and_empty_entries():
    """JSON list evidence skips None and empty values."""
    payload = _phase0_pass_payload()
    payload["files_inspected"] = ["scripts/a.py", None, "  ", "scripts/b.py"]
    report = controller.parse_phase0_preflight_result(json.dumps(payload))
    ASSERTIONS.assertEqual(report["files_inspected"], ["scripts/a.py", "scripts/b.py"])


def test_parse_phase0_preflight_result_json_pass_required_evidence_object_fails_closed():
    """Required evidence object values must fail closed."""
    payload = _phase0_pass_payload()
    payload["files_inspected"] = {"file": "scripts/pr_automation_controller.py"}
    report = controller.parse_phase0_preflight_result(json.dumps(payload))
    ASSERTIONS.assertEqual(report["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(report["next_action"], "needs_manual_phase0_failed")


def test_parse_phase0_preflight_result_json_pass_required_evidence_object_list_fails_closed():
    """Required evidence list with only object entries must fail closed."""
    payload = _phase0_pass_payload()
    payload["files_inspected"] = [{"file": "scripts/pr_automation_controller.py"}]
    report = controller.parse_phase0_preflight_result(json.dumps(payload))
    ASSERTIONS.assertEqual(report["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(report["next_action"], "needs_manual_phase0_failed")


def test_parse_phase0_preflight_result_fenced_json_with_prose_parses():
    """Prose-wrapped fenced JSON payload should parse."""
    payload = json.dumps(_phase0_pass_payload())
    report = controller.parse_phase0_preflight_result(f"preflight result follows\n```json\n{payload}\n```")
    ASSERTIONS.assertEqual(report["status"], "PASS")
    ASSERTIONS.assertEqual(report["next_action"], "generate_patch_prompt")


def test_phase0_preflight_failed_fails_closed_for_incomplete_pass_report():
    """phase0_preflight_failed should reject PASS reports without required evidence."""
    ASSERTIONS.assertTrue(controller.phase0_preflight_failed({"status": "PASS"}))


def test_phase0_preflight_helpers_direct_raw_pass_report_normalizes():
    """Direct helpers normalize complete PASS dict reports and accept generate_patch_prompt variants."""
    for action in ("generate_patch_prompt", "GENERATE_PATCH_PROMPT", "Generate_Patch_Prompt"):
        payload = _phase0_pass_payload()
        payload["next_action"] = action
        ASSERTIONS.assertEqual(controller.phase0_preflight_status(payload), "PASS")
        ASSERTIONS.assertFalse(controller.phase0_preflight_failed(payload))


def test_phase0_preflight_helpers_preserve_direct_dict_list_evidence():
    """Direct dict list evidence stays intact and allows PASS routing."""
    payload = _phase0_pass_payload()
    payload["files_inspected"] = [
        "scripts/pr_automation_controller.py",
        "tests/scripts/test_pr_automation_controller.py",
    ]
    payload["static_analysis_rules"] = ["codacy:py/rule", "ruff:F401"]
    payload["workflows_affected"] = ["ci.yml::lint", "ci.yml::tests"]
    ASSERTIONS.assertEqual(controller.phase0_preflight_status(payload), "PASS")
    ASSERTIONS.assertFalse(controller.phase0_preflight_failed(payload))


def test_phase0_preflight_helpers_direct_raw_incomplete_pass_fail_closed():
    """Direct helpers fail closed for incomplete PASS dict reports."""
    ASSERTIONS.assertEqual(controller.phase0_preflight_status({"status": "PASS"}), "NEEDS_MANUAL")
    ASSERTIONS.assertTrue(controller.phase0_preflight_failed({"status": "PASS"}))


def test_phase0_preflight_helpers_direct_raw_invalid_next_action_fails_closed():
    """Direct helpers fail closed when PASS next_action is invalid."""
    payload = _phase0_pass_payload()
    payload["next_action"] = "needs_manual"
    ASSERTIONS.assertEqual(controller.phase0_preflight_status(payload), "NEEDS_MANUAL")
    ASSERTIONS.assertTrue(controller.phase0_preflight_failed(payload))


def test_phase0_preflight_helpers_direct_non_dict_or_none_fail_closed():
    """Direct helpers fail closed for non-dict or None reports."""
    ASSERTIONS.assertEqual(controller.phase0_preflight_status(None), "NEEDS_MANUAL")
    ASSERTIONS.assertTrue(controller.phase0_preflight_failed(None))


def test_phase0_preflight_parser_non_dict_input_fails_closed():
    """String payloads are fail-closed after parser normalization."""
    report = controller.parse_phase0_preflight_result("status: PASS\nnext_action: generate_patch_prompt")
    ASSERTIONS.assertEqual(controller.phase0_preflight_status(report), "NEEDS_MANUAL")
    ASSERTIONS.assertTrue(controller.phase0_preflight_failed(report))


def test_parse_phase0_preflight_result_text_phase0_preflight_equals_pass():
    """Text PHASE_0_PREFLIGHT=PASS parses when next_action is present."""
    report = controller.parse_phase0_preflight_result(
        "\n".join(
            [
                "PHASE_0_PREFLIGHT=PASS",
                "next_action: generate_patch_prompt",
                "risk_level: low",
                "files_inspected: - scripts/pr_automation_controller.py",
                "static_analysis_rules: - ruff:F401",
                "workflows_affected: - .github/workflows/pr-automation-controller-v2.yml",
                "authoritative_modules: - scripts/pr_automation_controller.py",
                "dangerous_gates: - decide_phase0_gate",
                "implementation_plan: - keep patch scoped to Phase 0 parser",
                "files_allowed: - scripts/pr_automation_controller.py",
                "files_forbidden: - scripts/pr_flow_automation.py",
                "tests_to_run: - python3 -m pytest tests/scripts/test_pr_automation_controller.py -q",
                "stop_conditions: - stop on scope violation",
            ]
        )
    )
    ASSERTIONS.assertEqual(report["status"], "PASS")
    ASSERTIONS.assertEqual(report["next_action"], "generate_patch_prompt")


def test_parse_phase0_preflight_result_text_phase0_preflight_colon_pass():
    """Text phase_0_preflight: PASS parses when next_action is present."""
    report = controller.parse_phase0_preflight_result(
        "\n".join(
            [
                "phase_0_preflight: PASS",
                "next_action: generate_patch_prompt",
                "risk_level: low",
                "files_inspected: - scripts/pr_automation_controller.py",
                "static_analysis_rules: - ruff:F401",
                "workflows_affected: - .github/workflows/pr-automation-controller-v2.yml",
                "authoritative_modules: - scripts/pr_automation_controller.py",
                "dangerous_gates: - decide_phase0_gate",
                "implementation_plan: - keep patch scoped to Phase 0 parser",
                "files_allowed: - scripts/pr_automation_controller.py",
                "files_forbidden: - scripts/pr_flow_automation.py",
                "tests_to_run: - python3 -m pytest tests/scripts/test_pr_automation_controller.py -q",
                "stop_conditions: - stop on scope violation",
            ]
        )
    )
    ASSERTIONS.assertEqual(report["status"], "PASS")
    ASSERTIONS.assertEqual(report["next_action"], "generate_patch_prompt")


def test_parse_phase0_preflight_result_needs_manual_text():
    """NEEDS_MANUAL text payload normalizes to fail-closed manual routing."""
    report = controller.parse_phase0_preflight_result("status: NEEDS_MANUAL\nrisk_level: high")
    ASSERTIONS.assertEqual(report["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(report["next_action"], "needs_manual_phase0_failed")
    ASSERTIONS.assertTrue(controller.phase0_preflight_failed(report))


def test_parse_phase0_preflight_result_malformed_fails_closed():
    """Malformed payload fails closed to NEEDS_MANUAL with high risk."""
    report = controller.parse_phase0_preflight_result("```bash\nnot json\n```")
    ASSERTIONS.assertEqual(report["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(report["risk_level"], "high")
    ASSERTIONS.assertEqual(report["next_action"], "needs_manual_phase0_malformed")


def test_build_phase0_readonly_preflight_task_includes_readonly_prohibitions():
    task = controller.build_phase0_readonly_preflight_task(
        "claude_bug_pr7b_phase0_loop_gate",
        "task",
        ["scripts/pr_automation_controller.py"],
        ["scripts/pr_flow_automation.py"],
        task_title="PR7B gate",
        task_scope="PR7B only",
    )
    ASSERTIONS.assertIn("PHASE 0 PRE-FLIGHT (READ-ONLY)", task)
    ASSERTIONS.assertIn("No edit. No commit. No push. No rerun. No resolve.", task)
    ASSERTIONS.assertIn("No workflow modification.", task)
    ASSERTIONS.assertIn("No broad refactor. No broad suppressions. No activation.", task)


def test_build_phase0_readonly_preflight_task_includes_configs_workflows_and_scope_files():
    task = controller.build_phase0_readonly_preflight_task(
        "claude_bug_pr7b_phase0_loop_gate",
        "codacy",
        ["scripts/pr_automation_controller.py"],
        ["scripts/pr_flow_automation.py"],
        pr_number=7,
        branch="chore/pr7b-phase0-loop-gate",
        head_sha="abc123",
        review_comments=[{"id": 1}],
        codacy_annotations=[{"id": 2}],
        failing_checks=[{"name": "lint"}],
    )
    ASSERTIONS.assertIn(".codacy.yml", task)
    ASSERTIONS.assertIn(".deepsource.toml", task)
    ASSERTIONS.assertIn(".github/workflows/*.yml", task)
    ASSERTIONS.assertIn("authoritative_modules", task)
    ASSERTIONS.assertIn("dangerous_gates", task)
    ASSERTIONS.assertIn("files_allowed: scripts/pr_automation_controller.py", task)
    ASSERTIONS.assertIn("files_forbidden: scripts/pr_flow_automation.py", task)
    ASSERTIONS.assertIn("Result contract: status, risk_level", task)
    ASSERTIONS.assertIn("PHASE_0_PREFLIGHT=PASS", task)
    ASSERTIONS.assertIn("PHASE_0_PREFLIGHT=NEEDS_MANUAL", task)


def test_parse_phase0_preflight_result_needs_manual_status_text():
    report = controller.parse_phase0_preflight_result("status: NEEDS_MANUAL\nrisk_level: high")
    ASSERTIONS.assertEqual(report["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(report["next_action"], "needs_manual_phase0_failed")


def test_parse_phase0_preflight_result_missing_or_malformed_status_fails_closed():
    missing = controller.parse_phase0_preflight_result("risk_level: low")
    malformed = controller.parse_phase0_preflight_result("status: MAYBE\nrisk_level: low")
    ASSERTIONS.assertEqual(missing["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(missing["next_action"], "needs_manual_phase0_malformed")
    ASSERTIONS.assertEqual(malformed["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(malformed["next_action"], "needs_manual_phase0_malformed")


def test_decide_phase0_gate_pass_allows_patch():
    decision = controller.decide_phase0_gate(_phase0_pass_payload())
    ASSERTIONS.assertTrue(decision["phase0_required"])
    ASSERTIONS.assertEqual(decision["phase0_status"], "PASS")
    ASSERTIONS.assertTrue(decision["can_patch"])
    ASSERTIONS.assertFalse(decision["needs_manual"])


def test_decide_phase0_gate_blocks_incomplete_pass():
    decision = controller.decide_phase0_gate({"status": "PASS", "next_action": "generate_patch_prompt"})
    ASSERTIONS.assertEqual(decision["phase0_status"], "NEEDS_MANUAL")
    ASSERTIONS.assertFalse(decision["can_patch"])
    ASSERTIONS.assertEqual(decision["next_action"], "needs_manual_phase0_failed")


def test_decide_phase0_gate_needs_manual_blocks_patch():
    decision = controller.decide_phase0_gate({"status": "NEEDS_MANUAL", "next_action": "needs_manual_phase0_failed"})
    ASSERTIONS.assertEqual(decision["phase0_status"], "NEEDS_MANUAL")
    ASSERTIONS.assertFalse(decision["can_patch"])
    ASSERTIONS.assertTrue(decision["needs_manual"])


def test_build_codex_patch_task_after_phase0_pass_includes_impl_task_and_constraints():
    result = controller.build_codex_patch_task_after_phase0(
        "Implement the targeted fix only.",
        _phase0_pass_payload(),
    )
    ASSERTIONS.assertFalse(result["blocked"])
    ASSERTIONS.assertIn("Implement the targeted fix only.", result["patch_task"])
    ASSERTIONS.assertIn("Phase 0 PASS required and verified.", result["patch_task"])
    ASSERTIONS.assertIn("Do not modify workflows.", result["patch_task"])


def test_build_codex_patch_task_after_phase0_non_pass_blocks_patch_task():
    result = controller.build_codex_patch_task_after_phase0(
        "Implement the targeted fix only.",
        {"status": "NEEDS_MANUAL", "next_action": "needs_manual_phase0_failed"},
    )
    ASSERTIONS.assertTrue(result["blocked"])
    ASSERTIONS.assertEqual(result["patch_task"], "")
    ASSERTIONS.assertEqual(result["task"], "")
    ASSERTIONS.assertFalse(result["can_patch"])
    ASSERTIONS.assertIn("next_action", result)
    ASSERTIONS.assertIn("reason", result)
    ASSERTIONS.assertIn("gate", result)


def test_build_codex_patch_task_after_phase0_blocks_incomplete_pass():
    result = controller.build_codex_patch_task_after_phase0(
        "Implement the targeted fix only.",
        {"status": "PASS", "next_action": "generate_patch_prompt"},
    )
    ASSERTIONS.assertTrue(result["blocked"])
    ASSERTIONS.assertFalse(result["can_patch"])
    ASSERTIONS.assertEqual(result["next_action"], "needs_manual_phase0_failed")


def test_phase0_prompt_builders_return_string_types():
    ASSERTIONS.assertIsInstance(controller.build_phase0_preflight_prompt({}), str)
    task = controller.build_phase0_readonly_preflight_task("k", "task", ["a.py"], ["b.py"])
    ASSERTIONS.assertIsInstance(task, str)


def test_phase0_required_for_all_code_edit_triggers():
    for trigger in ("task", "review_comment", "codacy", "deepsource", "github_check", "failing_check"):
        ASSERTIONS.assertTrue(controller.phase0_required_for_trigger(trigger))


def test_phase0_required_for_trigger_uses_phase0_edit_triggers_constant():
    for trigger in controller.PHASE0_EDIT_TRIGGERS:
        ASSERTIONS.assertTrue(controller.phase0_required_for_trigger(trigger))


def test_phase0_helpers_do_not_call_runtime_tools():
    source = Path(controller.__file__).read_text(encoding="utf-8")
    names = (
        "build_phase0_readonly_preflight_task",
        "parse_phase0_preflight_result",
        "decide_phase0_gate",
        "build_codex_patch_task_after_phase0",
    )
    for name in names:
        block = source.split(f"def {name}(", 1)[1].split("\ndef ", 1)[0]
        ASSERTIONS.assertNotIn("process-spawning APIs", block)
        ASSERTIONS.assertNotIn("run(", block)
        ASSERTIONS.assertNotIn("gh ", block)
        ASSERTIONS.assertNotIn("codex ", block)


def test_codacy_task_lines_keep_post_fix_micro_audit_section_included():
    """Codacy task includes the required post-fix micro-audit section."""
    task = "\n".join(controller.codacy_task_lines([]))
    ASSERTIONS.assertIn("POST-FIX MICRO-AUDIT BEFORE COMMIT", task)


def test_validate_codex_prompt_contract_rejects_git_commit_without_allowance():
    """Unsafe imperative git commit instruction fails prompt contract without allowance."""
    prompt = "\n".join(
        [
            "TASK:\nFix lint",
            "OBJECTIVE:\nAddress blockers",
            "CONTEXT:\nScope is limited",
            "VALIDATION:\n- python3 -m pytest -q",
            "METHOD:\nRun git commit after fixes",
        ]
    )
    result = controller.validate_codex_prompt_contract(prompt)
    ASSERTIONS.assertFalse(result["valid"])
    ASSERTIONS.assertIn("commit_or_push_instruction_without_explicit_allowance", result["reasons"])


def test_validate_codex_prompt_contract_rejects_git_push_without_allowance():
    """Unsafe imperative git push instruction fails prompt contract without allowance."""
    prompt = "\n".join(
        [
            "TASK:\nFix lint",
            "OBJECTIVE:\nAddress blockers",
            "CONTEXT:\nScope is limited",
            "VALIDATION:\n- python3 -m pytest -q",
            "METHOD:\nPlease git push origin branch",
        ]
    )
    result = controller.validate_codex_prompt_contract(prompt)
    ASSERTIONS.assertFalse(result["valid"])
    ASSERTIONS.assertIn("commit_or_push_instruction_without_explicit_allowance", result["reasons"])


def test_validate_codex_prompt_contract_rejects_commit_changes_without_allowance():
    """Imperative commit changes instruction fails prompt contract without allowance."""
    prompt = "\n".join(
        [
            "TASK:\nFix lint",
            "OBJECTIVE:\nAddress blockers",
            "CONTEXT:\nScope is limited",
            "VALIDATION:\n- python3 -m pytest -q",
            "METHOD:\nCommit changes after validation.",
        ]
    )
    result = controller.validate_codex_prompt_contract(prompt)
    ASSERTIONS.assertFalse(result["valid"])
    ASSERTIONS.assertIn("commit_or_push_instruction_without_explicit_allowance", result["reasons"])


def test_validate_codex_prompt_contract_rejects_push_origin_branch_without_allowance():
    """Imperative push origin branch instruction fails prompt contract without allowance."""
    prompt = "\n".join(
        [
            "TASK:\nFix lint",
            "OBJECTIVE:\nAddress blockers",
            "CONTEXT:\nScope is limited",
            "VALIDATION:\n- python3 -m pytest -q",
            "METHOD:\nPush origin branch when checks are green.",
        ]
    )
    result = controller.validate_codex_prompt_contract(prompt)
    ASSERTIONS.assertFalse(result["valid"])
    ASSERTIONS.assertIn("commit_or_push_instruction_without_explicit_allowance", result["reasons"])


def test_validate_codex_prompt_contract_allows_commit_push_with_allowance_line():
    """ALLOW_COMMIT_PUSH yes on its own line permits commit/push imperative instructions."""
    prompt = "\n".join(
        [
            _full_codex_contract_prompt(),
            "ALLOW_COMMIT_PUSH: yes",
            "METHOD:\nRun git commit and git push origin branch",
        ]
    )
    result = controller.validate_codex_prompt_contract(prompt)
    ASSERTIONS.assertTrue(result["valid"])


def test_validate_codex_prompt_contract_allows_commit_push_with_case_insensitive_allowance():
    """ALLOW_COMMIT_PUSH key/value variants should permit commit/push imperatives."""
    for allowance in (
        "ALLOW_COMMIT_PUSH: yes",
        "ALLOW_COMMIT_PUSH: YES",
        "Allow_Commit_Push: Yes",
    ):
        prompt = "\n".join(
            [
                _full_codex_contract_prompt(),
                allowance,
                "METHOD:\nRun git commit and git push origin branch",
            ]
        )
        result = controller.validate_codex_prompt_contract(prompt)
        ASSERTIONS.assertTrue(result["valid"])


def test_validate_codex_prompt_contract_does_not_reject_descriptive_committing_word():
    """Descriptive words like committing do not trigger unsafe commit/push detection."""
    prompt = "\n".join(
        [
            _full_codex_contract_prompt(),
            "METHOD:\nDocument expected steps before committing anything.",
        ]
    )
    result = controller.validate_codex_prompt_contract(prompt)
    ASSERTIONS.assertTrue(result["valid"])


def test_validate_codex_prompt_contract_does_not_reject_post_fix_header_with_commit_word():
    """Required header text with commit wording must remain valid."""
    prompt = "\n".join(
        [
            _full_codex_contract_prompt(),
            "POST-FIX MICRO-AUDIT BEFORE COMMIT",
        ]
    )
    result = controller.validate_codex_prompt_contract(prompt)
    ASSERTIONS.assertTrue(result["valid"])


def test_validate_codex_prompt_contract_does_not_reject_post_fix_micro_audit_header():
    """POST-FIX MICRO-AUDIT BEFORE COMMIT header is mandatory and must remain valid."""
    prompt = _full_codex_contract_prompt()
    result = controller.validate_codex_prompt_contract(prompt)
    ASSERTIONS.assertTrue(result["valid"])


def test_validate_codex_prompt_contract_does_not_reject_do_not_commit_or_push():
    """Negative guardrails about commit/push must not be treated as unsafe instructions."""
    prompt = "\n".join(
        [
            _full_codex_contract_prompt(),
            "METHOD:\nDo not commit. Do not push. no commit/push.",
        ]
    )
    result = controller.validate_codex_prompt_contract(prompt)
    ASSERTIONS.assertTrue(result["valid"])


def test_validate_codex_prompt_contract_rejects_placeholder_objective_tbd_bullet():
    """Placeholder values like - TBD keep objective/context/validation checks invalid."""
    prompt = "\n".join(
        [
            "TASK:\nFix this",
            "OBJECTIVE:\n- TBD",
            "CONTEXT:\nReady",
            "VALIDATION:\n- python3 -m pytest -q",
        ]
    )
    result = controller.validate_codex_prompt_contract(prompt)
    ASSERTIONS.assertIn("generic_fix_without_objective_context_validation", result["reasons"])
    ASSERTIONS.assertFalse(result["valid"])


def test_validate_codex_prompt_contract_rejects_placeholder_validation_list_only_tbd():
    """List-style validation with only placeholders must be treated as missing/uninitialized."""
    prompt = "\n".join(
        [
            "TASK:\nFix this",
            "OBJECTIVE:\nAddress blockers",
            "CONTEXT:\nReady",
            "VALIDATION:\n- TBD",
        ]
    )
    result = controller.validate_codex_prompt_contract(prompt)
    ASSERTIONS.assertIn("generic_fix_without_objective_context_validation", result["reasons"])
    ASSERTIONS.assertFalse(result["valid"])


def test_validate_codex_prompt_contract_rejects_placeholder_objective_variants():
    """TBD/TODO/none/null/n-a placeholders should be treated as uninitialized."""
    for value in ("TBD", "- TBD", "TODO", "- TODO", "none", "null", "n/a", "\"\"", "   "):
        prompt = "\n".join(
            [
                "TASK:\nFix this",
                f"OBJECTIVE:\n{value}",
                "CONTEXT:\nReady",
                "VALIDATION:\n- python3 -m pytest -q",
            ]
        )
        result = controller.validate_codex_prompt_contract(prompt)
        ASSERTIONS.assertIn("generic_fix_without_objective_context_validation", result["reasons"])
        ASSERTIONS.assertFalse(result["valid"])


def test_validate_codex_prompt_contract_rejects_list_style_section_with_only_placeholder_bullets():
    """List-style required sections with only placeholder bullets are invalid."""
    placeholder_cases = {
        "FILES TO INSPECT": "- TBD\n-   ",
        "REQUIRED FIXES": "- TBD\n- TODO",
        "STOP CONDITIONS": "- TBD",
    }
    for section, value in placeholder_cases.items():
        prompt = _set_section_value(_full_codex_contract_prompt(), section, value)
        result = controller.validate_codex_prompt_contract(prompt)
        ASSERTIONS.assertIn(section, result["uninitialized_sections"])
        ASSERTIONS.assertFalse(result["valid"])


def test_validate_codex_prompt_contract_rejects_unsafe_commit_push_imperatives_without_allowance():
    """Imperative unsafe commit/push wording is blocked without explicit allowance."""
    for method_line in (
        "METHOD:\nRun git commit after checks.",
        "METHOD:\nRun git push after checks.",
        "METHOD:\nCommit changes when done.",
        "METHOD:\nPush origin branch after tests.",
    ):
        _assert_contract_rejects_without_allowance(method_line)


def test_validate_codex_prompt_contract_rejects_commit_the_patch_without_allowance():
    """Imperative commit-the-patch wording is blocked without explicit allowance."""
    _assert_contract_rejects_without_allowance("METHOD:\nCommit the patch after checks.")


def test_validate_codex_prompt_contract_rejects_push_the_branch_without_allowance():
    """Imperative push-the-branch wording is blocked without explicit allowance."""
    _assert_contract_rejects_without_allowance("METHOD:\nPush the branch when done.")


def test_validate_codex_prompt_contract_rejects_commit_after_checks_without_allowance():
    """Bare imperative commit-after-checks wording is blocked without explicit allowance."""
    _assert_contract_rejects_without_allowance("METHOD:\nCommit after checks.")


def test_validate_codex_prompt_contract_rejects_push_this_branch_without_allowance():
    """Bare imperative push-this-branch wording is blocked without explicit allowance."""
    _assert_contract_rejects_without_allowance("METHOD:\nPush this branch.")


def test_validate_codex_prompt_contract_rejects_push_origin_main_after_checks_without_allowance():
    """Push-origin-main imperative wording is blocked without explicit allowance."""
    _assert_contract_rejects_without_allowance("METHOD:\nPush origin main after checks.")


def test_validate_codex_prompt_contract_rejects_commit_the_changes_after_checks_without_allowance():
    """Commit-the-changes imperative wording is blocked without explicit allowance."""
    _assert_contract_rejects_without_allowance("METHOD:\nCommit the changes after checks.")


def test_validate_codex_prompt_contract_rejects_mixed_negation_with_real_push_instruction():
    """Mixed lines with negation and an actual push imperative must still be rejected."""
    prompt = "\n".join(
        [
            _full_codex_contract_prompt(),
            "METHOD:\nDo not push to main; git push origin branch",
        ]
    )
    result = controller.validate_codex_prompt_contract(prompt)
    ASSERTIONS.assertIn("commit_or_push_instruction_without_explicit_allowance", result["reasons"])
    ASSERTIONS.assertFalse(result["valid"])


def test_validate_codex_prompt_contract_rejects_mixed_negation_with_real_commit_instruction():
    """Mixed negation plus real commit imperative remains invalid."""
    prompt = "\n".join(
        [
            _full_codex_contract_prompt(),
            "METHOD:\nDo not push to main; commit changes after checks",
        ]
    )
    result = controller.validate_codex_prompt_contract(prompt)
    ASSERTIONS.assertIn("commit_or_push_instruction_without_explicit_allowance", result["reasons"])
    ASSERTIONS.assertFalse(result["valid"])


def test_validate_codex_prompt_contract_accepts_safe_commit_push_descriptions():
    """Descriptive and negative commit/push wording should not be flagged as unsafe."""
    for method_line in (
        "METHOD:\nDocument steps before committing changes.",
        "METHOD:\nPOST-FIX MICRO-AUDIT BEFORE COMMIT",
        "METHOD:\nDo not commit until explicitly approved.",
        "METHOD:\nDo not push until explicitly approved.",
        "METHOD:\nDo not git commit.",
        "METHOD:\nDo not git push.",
        "METHOD:\nDon't git commit.",
        "METHOD:\nDon't git push.",
        "METHOD:\nno commit/push without owner approval.",
    ):
        _assert_contract_accepts_safe_commit_push_wording(method_line)


def test_validate_codex_prompt_contract_accepts_safe_no_commit_or_no_push_variants():
    """Safe no-commit/no-push phrasing remains valid without ALLOW_COMMIT_PUSH."""
    _assert_contract_accepts_safe_commit_push_wording("METHOD:\nNo commit until owner approval.")
    _assert_contract_accepts_safe_commit_push_wording("METHOD:\nNo push until owner approval.")


def test_validate_codex_prompt_contract_commit_push_contract_examples():
    """Contract examples must keep invalid imperatives blocked and safe negations allowed."""
    _assert_contract_examples_rejected(_contract_examples_rejected())
    _assert_contract_examples_accepted(_contract_examples_accepted())


def test_validate_codex_prompt_contract_accepts_appended_pure_negated_git_push_line():
    """Pure prohibition appended to a valid prompt remains valid."""
    prompt = _full_codex_contract_prompt() + "\ndo not git push\n"
    result = controller.validate_codex_prompt_contract(prompt)
    ASSERTIONS.assertTrue(result["valid"])


def test_ensure_phase0_preflight_section_is_idempotent_when_already_present():
    """Existing PHASE 0 section is preserved without duplication or reordering."""
    original = "\n".join(
        [
            controller.PHASE0_SECTION_TITLE,
            "",
            "READ-ONLY. Do not edit files. Do not commit. Do not push.",
            "",
            "TASK:",
            "Fix lint",
        ]
    )
    ensured = controller.ensure_phase0_preflight_section(original)
    ASSERTIONS.assertEqual(ensured, original + "\n")
    ASSERTIONS.assertEqual(ensured.count(controller.PHASE0_SECTION_TITLE), 1)


def test_ensure_phase0_preflight_section_prepends_exactly_once_for_empty_and_missing():
    """Missing/empty input gets exactly one preflight block followed by original content."""
    ensured_empty = controller.ensure_phase0_preflight_section("")
    ASSERTIONS.assertEqual(ensured_empty.count(controller.PHASE0_SECTION_TITLE), 1)
    ASSERTIONS.assertTrue(ensured_empty.startswith(controller.PHASE0_SECTION_TITLE))

    original = "TASK:\nFix lint"
    ensured = controller.ensure_phase0_preflight_section(original)
    ASSERTIONS.assertEqual(ensured.count(controller.PHASE0_SECTION_TITLE), 1)
    ASSERTIONS.assertTrue(ensured.endswith("\nTASK:\nFix lint\n"))


def test_ensure_phase0_preflight_section_preserves_original_content_after_insert():
    """Inserted preflight block must keep original prompt content unchanged afterwards."""
    original = "TASK:\nFix lint\nOBJECTIVE:\nAddress blockers"
    ensured = controller.ensure_phase0_preflight_section(original)
    ASSERTIONS.assertIn("\nTASK:\nFix lint\nOBJECTIVE:\nAddress blockers\n", ensured)


def test_ensure_codex_prompt_contract_preserves_files_allowed_from_context():
    """Contract enforcer should preserve explicit files_allowed when inserting Phase 0."""
    base_prompt = _full_codex_contract_prompt().replace("PHASE 0 PRE-FLIGHT (READ-ONLY)\n\n", "", 1)
    prompt = controller.ensure_codex_prompt_contract(
        base_prompt,
        {"files_allowed": ["scripts/pr_automation_controller.py"]},
    )
    ASSERTIONS.assertIn("Task allowed files: scripts/pr_automation_controller.py", prompt)
    ASSERTIONS.assertNotIn("Task allowed files: (from task scope)", prompt)
    ASSERTIONS.assertTrue(controller.validate_codex_prompt_contract(prompt)["valid"])


def test_parse_post_fix_micro_audit_result_supports_json_payload():
    """JSON payload should parse fields and preserve PASS validation routing."""
    raw = json.dumps({
        "status": "PASS",
        "reasons": ["ok"],
        "changed_files": ["scripts/pr_automation_controller.py"],
        "validation_commands": ["python3 -m pytest -q"],
    })
    report = controller.parse_post_fix_micro_audit_result(raw)
    ASSERTIONS.assertEqual(report["status"], "PASS")
    ASSERTIONS.assertEqual(report["reasons"], ["ok"])
    ASSERTIONS.assertEqual(report["changed_files"], ["scripts/pr_automation_controller.py"])
    ASSERTIONS.assertEqual(report["validation_commands"], ["python3 -m pytest -q"])
    ASSERTIONS.assertEqual(report["next_action"], "validation_then_commit")


def test_parse_post_fix_micro_audit_result_supports_fenced_json_payload_with_language():
    """```json fenced payload should parse as JSON and preserve PASS routing."""
    raw = "\n".join(
        [
            "```json",
            '{"status":"PASS","next_action":"validation_then_commit","reasons":["ok"]}',
            "```",
        ]
    )
    report = controller.parse_post_fix_micro_audit_result(raw)
    ASSERTIONS.assertEqual(report["status"], "PASS")
    ASSERTIONS.assertEqual(report["next_action"], "validation_then_commit")
    ASSERTIONS.assertEqual(report["reasons"], ["ok"])
    ASSERTIONS.assertFalse(controller.post_fix_micro_audit_failed(report))


def test_parse_post_fix_micro_audit_result_supports_fenced_json_payload_without_language():
    """``` fenced payload should parse as JSON and preserve PASS routing."""
    raw = "\n".join(
        [
            "```",
            '{"status":"PASS","next_action":"validation_then_commit"}',
            "```",
        ]
    )
    report = controller.parse_post_fix_micro_audit_result(raw)
    ASSERTIONS.assertEqual(report["status"], "PASS")
    ASSERTIONS.assertEqual(report["next_action"], "validation_then_commit")
    ASSERTIONS.assertFalse(controller.post_fix_micro_audit_failed(report))


def test_parse_post_fix_micro_audit_result_supports_fenced_json_payload_with_surrounding_prose():
    """Prose around fenced JSON should still parse PASS report payload."""
    raw = "\n".join(
        [
            "Here is the audit:",
            "```json",
            '{"status":"PASS","next_action":"validation_then_commit","reasons":["ok"]}',
            "```",
            "No further issues.",
        ]
    )
    report = controller.parse_post_fix_micro_audit_result(raw)
    ASSERTIONS.assertEqual(report["status"], "PASS")
    ASSERTIONS.assertEqual(report["next_action"], "validation_then_commit")
    ASSERTIONS.assertEqual(report["reasons"], ["ok"])
    ASSERTIONS.assertFalse(controller.post_fix_micro_audit_failed(report))


def test_parse_post_fix_micro_audit_result_uses_later_fenced_json_when_first_fence_is_not_json():
    """Parser should try every fenced payload before falling back to text parsing."""
    raw = "\n".join(
        [
            "prose before",
            "```bash",
            "echo 'not json'",
            "```",
            "middle prose",
            "```json",
            '{"status":"PASS","next_action":"validation_then_commit","reasons":["ok"]}',
            "```",
            "prose after",
        ]
    )
    report = controller.parse_post_fix_micro_audit_result(raw)
    ASSERTIONS.assertEqual(report["status"], "PASS")
    ASSERTIONS.assertEqual(report["next_action"], "validation_then_commit")
    ASSERTIONS.assertEqual(report["reasons"], ["ok"])
    ASSERTIONS.assertFalse(controller.post_fix_micro_audit_failed(report))


def test_parse_post_fix_micro_audit_result_partial_text_preserves_status_and_reasons():
    """Text PARTIAL status should remain PARTIAL and default to retry_fix_within_budget."""
    report = controller.parse_post_fix_micro_audit_result(
        "\n".join(
            [
                "status: PARTIAL",
                "reasons:",
                "- needs one more pass",
                "- waiting for targeted retry",
            ]
        )
    )
    ASSERTIONS.assertEqual(report["status"], "PARTIAL")
    ASSERTIONS.assertEqual(report["next_action"], "retry_fix_within_budget")
    ASSERTIONS.assertEqual(report["reasons"], ["needs one more pass", "waiting for targeted retry"])
    ASSERTIONS.assertTrue(controller.post_fix_micro_audit_failed(report))


def test_parse_post_fix_micro_audit_result_supports_yaml_style_lists_and_stops_at_next_field():
    """Text-mode list parsing must stop at the next key and support compact dash bullets."""
    report = controller.parse_post_fix_micro_audit_result(
        "\n".join(
            [
                "status: FAIL",
                "reasons:",
                "-issue one",
                "- issue two",
                "changed_files:",
                "- scripts/a.py",
                "validation_commands:",
                "- python3 -m py_compile",
            ]
        )
    )
    ASSERTIONS.assertEqual(report["reasons"], ["issue one", "issue two"])
    ASSERTIONS.assertEqual(report["changed_files"], ["scripts/a.py"])
    ASSERTIONS.assertEqual(report["validation_commands"], ["python3 -m py_compile"])


def test_parse_post_fix_micro_audit_result_lists_skip_blank_separator_after_header():
    """Blank separators after list headers should be ignored before first bullet."""
    report = controller.parse_post_fix_micro_audit_result(
        "\n".join(
            [
                "status: FAIL",
                "reasons:",
                "",
                "- item one",
                "- item two",
                "changed_files:",
                "- scripts/a.py",
            ]
        )
    )
    ASSERTIONS.assertEqual(report["reasons"], ["item one", "item two"])
    ASSERTIONS.assertEqual(report["changed_files"], ["scripts/a.py"])


def test_parse_post_fix_micro_audit_result_does_not_overcapture_later_section_bullets():
    """List parsing should not consume bullets after section boundaries."""
    report = controller.parse_post_fix_micro_audit_result(
        "\n".join(
            [
                "status: FAIL",
                "reasons:",
                "- first reason",
                "next_action: needs_manual",
                "- not part of reasons",
                "changed_files:",
                "- scripts/a.py",
            ]
        )
    )
    ASSERTIONS.assertEqual(report["reasons"], ["first reason"])
    ASSERTIONS.assertEqual(report["changed_files"], ["scripts/a.py"])


def test_parse_post_fix_micro_audit_result_malformed_list_sections_return_empty_lists():
    """Malformed non-bullet list sections should parse as empty lists."""
    report = controller.parse_post_fix_micro_audit_result(
        "\n".join(
            [
                "status: FAIL",
                "reasons:",
                "not-a-bullet",
                "changed_files:",
                "scripts/a.py",
            ]
        )
    )
    ASSERTIONS.assertEqual(report["reasons"], [])
    ASSERTIONS.assertEqual(report["changed_files"], [])


def test_parse_post_fix_micro_audit_result_malformed_or_missing_fails_closed():
    """Malformed and missing audit reports should route to fail-closed manual handling."""
    malformed = controller.parse_post_fix_micro_audit_result("{")
    malformed_fenced_only = controller.parse_post_fix_micro_audit_result("```bash\nnot json\n```")
    missing = controller.parse_post_fix_micro_audit_result("")

    ASSERTIONS.assertEqual(malformed["status"], "FAIL")
    ASSERTIONS.assertEqual(malformed["next_action"], "needs_manual_post_fix_audit_failed")
    ASSERTIONS.assertTrue(controller.post_fix_micro_audit_failed(malformed))
    ASSERTIONS.assertEqual(malformed_fenced_only["status"], "FAIL")
    ASSERTIONS.assertEqual(malformed_fenced_only["next_action"], "needs_manual_post_fix_audit_failed")
    ASSERTIONS.assertTrue(controller.post_fix_micro_audit_failed(malformed_fenced_only))
    ASSERTIONS.assertEqual(missing["status"], "FAIL")
    ASSERTIONS.assertEqual(missing["next_action"], "needs_manual_post_fix_audit_failed")
    ASSERTIONS.assertTrue(controller.post_fix_micro_audit_failed(missing))


def test_parse_post_fix_micro_audit_result_pass_defaults_to_validation_then_commit():
    """PASS status should route to validation_then_commit."""
    report = controller.parse_post_fix_micro_audit_result("status: PASS")
    ASSERTIONS.assertEqual(report["status"], "PASS")
    ASSERTIONS.assertEqual(report["next_action"], "validation_then_commit")
    ASSERTIONS.assertFalse(controller.post_fix_micro_audit_failed(report))


def test_parse_post_fix_micro_audit_result_supports_common_pass_text_formats():
    """Common human-readable PASS formats should parse as PASS."""
    pass_reports = [
        "status: PASS",
        "Status: PASS",
        "STATUS: PASS",
        "status = PASS",
        "POST_FIX_AUDIT=PASS",
        "Post-fix micro-audit report\nPASS\nall checks completed",
    ]
    for raw in pass_reports:
        report = controller.parse_post_fix_micro_audit_result(raw)
        ASSERTIONS.assertEqual(report["status"], "PASS")
        ASSERTIONS.assertEqual(report["next_action"], "validation_then_commit")
        ASSERTIONS.assertFalse(controller.post_fix_micro_audit_failed(report))


def test_parse_post_fix_micro_audit_result_fail_defaults_to_needs_manual():
    """FAIL status should fail closed when next_action is omitted."""
    report = controller.parse_post_fix_micro_audit_result("status: FAIL\nreasons:\n- threshold exceeded")
    ASSERTIONS.assertEqual(report["status"], "FAIL")
    ASSERTIONS.assertEqual(report["next_action"], "needs_manual_post_fix_audit_failed")
    ASSERTIONS.assertTrue(controller.post_fix_micro_audit_failed(report))


def test_missing_post_fix_micro_audit_result_fails_closed():
    """Missing audit result should fail closed in enforcement helper."""
    report = controller.parse_post_fix_micro_audit_result("")
    ASSERTIONS.assertEqual(controller.post_fix_micro_audit_status(report), "FAIL")
    ASSERTIONS.assertTrue(controller.post_fix_micro_audit_failed(report))


def test_automation_ledger_append_creates_jsonl_and_reads_in_order(tmp_path):
    """Ledger appends events as JSONL and preserves insertion order."""
    path = controller.automation_ledger_path(str(tmp_path), 225)
    first = controller.build_automation_ledger_event(event_type="post_fix_audit_failure", reason="alpha")
    second = controller.build_automation_ledger_event(event_type="post_fix_audit_failure", reason="beta", attempt=2)
    controller.append_automation_ledger_event(path, first)
    controller.append_automation_ledger_event(path, second)
    ASSERTIONS.assertTrue((tmp_path / "pr-225" / "automation-ledger.jsonl").exists())
    events = controller.read_automation_ledger_events(path)
    ASSERTIONS.assertEqual([item["reason"] for item in events], ["alpha", "beta"])
    ASSERTIONS.assertEqual(len((tmp_path / "pr-225" / "automation-ledger.jsonl").read_text().splitlines()), 2)


def test_build_automation_ledger_event_defaults_safe_optionals():
    """Optional fields are normalized to safe defaults."""
    event = controller.build_automation_ledger_event()
    ASSERTIONS.assertEqual(event["event_type"], "")
    ASSERTIONS.assertEqual(event["pr"], 0)
    ASSERTIONS.assertEqual(event["attempt"], 0)
    ASSERTIONS.assertEqual(event["details"], {})
    ASSERTIONS.assertTrue(bool(event["created_at"]))


def test_read_automation_ledger_events_missing_or_malformed_safe(tmp_path):
    """Missing ledger and malformed rows should be handled safely."""
    path = controller.automation_ledger_path(str(tmp_path), 225)
    ASSERTIONS.assertEqual(controller.read_automation_ledger_events(path), [])
    (tmp_path / "pr-225").mkdir(parents=True, exist_ok=True)
    (tmp_path / "pr-225" / "automation-ledger.jsonl").write_text('{"event_type":"ok"}\nnot-json\n', encoding="utf-8")
    events = controller.read_automation_ledger_events(path)
    ASSERTIONS.assertEqual(events, [{"event_type": "ok"}])


def test_automation_ledger_path_accepts_string_pr_number(tmp_path):
    """String PR numbers should normalize to numeric directories."""
    path = controller.automation_ledger_path(str(tmp_path), "225")
    ASSERTIONS.assertEqual(path, str(tmp_path / "pr-225" / "automation-ledger.jsonl"))


def test_automation_ledger_path_handles_invalid_pr_number_safely(tmp_path):
    """None, empty, and invalid PR values should fail-safe to pr-0."""
    ASSERTIONS.assertIn("/pr-0/", controller.automation_ledger_path(str(tmp_path), ""))
    ASSERTIONS.assertIn("/pr-0/", controller.automation_ledger_path(str(tmp_path), None))
    ASSERTIONS.assertIn("/pr-0/", controller.automation_ledger_path(str(tmp_path), "abc"))
    ASSERTIONS.assertIn("/pr-0/", controller.automation_ledger_path(str(tmp_path), -5))


def test_automation_ledger_path_handles_empty_base_dir_safely():
    """Empty base_dir should not crash and should still return a ledger file path."""
    path = controller.automation_ledger_path("", 225)
    ASSERTIONS.assertTrue(path.endswith("pr-225/automation-ledger.jsonl"))


def test_build_automation_ledger_event_normalizes_non_dict_details():
    """Non-dict details should normalize to an empty dict."""
    event = controller.build_automation_ledger_event(details=cast(Any, "bad"))
    ASSERTIONS.assertEqual(event["details"], {})


def test_build_automation_ledger_event_normalizes_numeric_strings():
    """Numeric string pr/attempt values should normalize to ints."""
    event = controller.build_automation_ledger_event(pr="225", attempt="2")
    ASSERTIONS.assertEqual(event["pr"], 225)
    ASSERTIONS.assertEqual(event["attempt"], 2)


def test_build_automation_ledger_event_non_numeric_values_fail_safe():
    """Non-numeric and negative pr/attempt should normalize to 0."""
    event = controller.build_automation_ledger_event(pr="abc", attempt=-1)
    ASSERTIONS.assertEqual(event["pr"], 0)
    ASSERTIONS.assertEqual(event["attempt"], 0)


def test_build_automation_ledger_event_preserves_explicit_created_at():
    """Explicit created_at should be preserved."""
    event = controller.build_automation_ledger_event(created_at="2026-01-02T03:04:05Z")
    ASSERTIONS.assertEqual(event["created_at"], "2026-01-02T03:04:05Z")


def test_read_automation_ledger_events_directory_path_returns_empty(tmp_path):
    """Directory paths should return an empty event list."""
    folder = tmp_path / "ledger-dir"
    folder.mkdir()
    ASSERTIONS.assertEqual(controller.read_automation_ledger_events(str(folder)), [])


def test_parse_post_fix_micro_audit_result_unknown_status_fails_closed():
    """Unknown text status should fail closed."""
    report = controller.parse_post_fix_micro_audit_result("status: UNKNOWN")
    ASSERTIONS.assertEqual(report["status"], "FAIL")
    ASSERTIONS.assertEqual(report["next_action"], "needs_manual_post_fix_audit_failed")
    ASSERTIONS.assertTrue(controller.post_fix_micro_audit_failed(report))


def test_parse_post_fix_micro_audit_result_json_unknown_status_fails_closed():
    """Unknown JSON status should fail closed."""
    report = controller.parse_post_fix_micro_audit_result(json.dumps({"status": "UNKNOWN"}))
    ASSERTIONS.assertEqual(report["status"], "FAIL")
    ASSERTIONS.assertEqual(report["next_action"], "needs_manual_post_fix_audit_failed")
    ASSERTIONS.assertTrue(controller.post_fix_micro_audit_failed(report))


def test_parse_post_fix_micro_audit_result_pass_with_invalid_next_action_fails_closed():
    """PASS with an override action other than validation_then_commit must fail closed."""
    report = controller.parse_post_fix_micro_audit_result("status: PASS\nnext_action: retry_fix_within_budget")
    ASSERTIONS.assertEqual(report["status"], "FAIL")
    ASSERTIONS.assertEqual(report["next_action"], "needs_manual_post_fix_audit_failed")
    ASSERTIONS.assertTrue(controller.post_fix_micro_audit_failed(report))


def test_parse_post_fix_micro_audit_result_pass_with_needs_manual_override_fails_closed():
    """PASS with explicit needs_manual override must fail closed."""
    report = controller.parse_post_fix_micro_audit_result("status: PASS\nnext_action: needs_manual")
    ASSERTIONS.assertEqual(report["status"], "FAIL")
    ASSERTIONS.assertEqual(report["next_action"], "needs_manual_post_fix_audit_failed")
    ASSERTIONS.assertTrue(controller.post_fix_micro_audit_failed(report))


def test_post_fix_micro_audit_status_none_fails_closed():
    """None report should resolve to FAIL status."""
    ASSERTIONS.assertEqual(controller.post_fix_micro_audit_status(None), "FAIL")


def test_post_fix_micro_audit_failed_none_fails_closed():
    """None report should be treated as failed."""
    ASSERTIONS.assertTrue(controller.post_fix_micro_audit_failed(None))


def test_post_fix_micro_audit_failed_empty_dict_fails_closed():
    """Empty report should be treated as failed."""
    ASSERTIONS.assertTrue(controller.post_fix_micro_audit_failed({}))


def test_post_fix_micro_audit_helpers_fail_closed_for_pass_without_required_next_action():
    """PASS without validation_then_commit must fail closed."""
    ASSERTIONS.assertTrue(controller.post_fix_micro_audit_failed({"status": "PASS"}))
    ASSERTIONS.assertTrue(
        controller.post_fix_micro_audit_failed(
            {"status": "PASS", "next_action": "retry_fix_within_budget"}
        )
    )


def test_post_fix_micro_audit_status_malformed_dict_fails_closed():
    """Malformed status values should fail closed."""
    ASSERTIONS.assertEqual(controller.post_fix_micro_audit_status({"status": {"bad": "value"}}), "FAIL")


def test_post_fix_micro_audit_failed_malformed_dict_fails_closed():
    """Malformed report payload should be treated as failed."""
    ASSERTIONS.assertTrue(controller.post_fix_micro_audit_failed({"status": {"bad": "value"}}))


def test_decide_post_fix_audit_retry_pass_does_not_request_retry():
    """PASS should continue normal validation/commit flow without retry."""
    decision = controller.decide_post_fix_audit_retry(
        {"status": "PASS", "next_action": "validation_then_commit"},
        original_task_scope="Fix only PR5B",
        files_allowed=["scripts/pr_automation_controller.py"],
        retry_count=0,
        retry_limit=2,
    )
    ASSERTIONS.assertFalse(decision["should_retry"])
    ASSERTIONS.assertEqual(decision["next_action"], "validation_then_commit")
    ASSERTIONS.assertEqual(decision["retry_task"], "")


def test_decide_post_fix_audit_retry_fail_builds_narrow_retry_task_and_blocks_commit_push():
    """FAIL with budget should build retry task with exact reason and no commit/push instructions."""
    failure_reason = "Missing assertion at tests/scripts/test_pr_automation_controller.py:1570"
    decision = controller.decide_post_fix_audit_retry(
        {"status": "FAIL", "reasons": [failure_reason]},
        original_task_scope="Fix only PR5B",
        files_allowed=["scripts/pr_automation_controller.py", "tests/scripts/test_pr_automation_controller.py"],
        files_forbidden=["scripts/pr_flow_automation.py"],
        retry_count=0,
        retry_limit=2,
    )
    ASSERTIONS.assertTrue(decision["should_retry"])
    ASSERTIONS.assertEqual(decision["next_action"], "retry_fix_within_budget")
    ASSERTIONS.assertEqual(decision["failure_reason"], failure_reason)
    ASSERTIONS.assertIn("Failure reason (exact):", decision["retry_task"])
    ASSERTIONS.assertIn("  " + failure_reason, decision["retry_task"])
    ASSERTIONS.assertIn("- scripts/pr_automation_controller.py", decision["retry_task"])
    ASSERTIONS.assertIn("- tests/scripts/test_pr_automation_controller.py", decision["retry_task"])
    ASSERTIONS.assertIn("- scripts/pr_flow_automation.py", decision["retry_task"])
    _assert_retry_task_blocks_commit_push(decision["retry_task"])


def test_decide_post_fix_audit_retry_partial_builds_retry_task_and_blocks_commit_push():
    """PARTIAL with budget should retry and keep no-commit/no-push instructions."""
    decision = controller.decide_post_fix_audit_retry(
        {"status": "PARTIAL", "reasons": ["Need one more focused fix"]},
        original_task_scope="Fix only PR5B",
        files_allowed=["scripts/pr_automation_controller.py"],
        retry_count=0,
        retry_limit=2,
    )
    ASSERTIONS.assertTrue(decision["should_retry"])
    ASSERTIONS.assertEqual(decision["next_action"], "retry_fix_within_budget")
    ASSERTIONS.assertIn("Do not run git add/commit/push.", decision["retry_task"])


def test_decide_post_fix_audit_retry_same_failure_repeated_needs_manual():
    """Repeated normalized failure reason should stop retrying and route to manual."""
    decision = controller.decide_post_fix_audit_retry(
        {"status": "FAIL", "reasons": ["  Missing   unit test coverage  "]},
        original_task_scope="Fix only PR5B",
        retry_count=0,
        retry_limit=2,
        previous_failure_reasons=["missing unit test coverage"],
    )
    ASSERTIONS.assertFalse(decision["should_retry"])
    ASSERTIONS.assertEqual(decision["next_action"], "needs_manual_post_fix_audit_failed")
    ASSERTIONS.assertTrue(decision["repeated_failure"])


def test_decide_post_fix_audit_retry_budget_exhausted_needs_manual():
    """Exhausted retry budget must fail closed to manual path."""
    decision = controller.decide_post_fix_audit_retry(
        {"status": "FAIL", "reasons": ["remaining lint finding"]},
        original_task_scope="Fix only PR5B",
        retry_count=2,
        retry_limit=2,
    )
    ASSERTIONS.assertFalse(decision["should_retry"])
    ASSERTIONS.assertEqual(decision["next_action"], "needs_manual_post_fix_audit_failed")
    ASSERTIONS.assertTrue(decision["retry_budget_exhausted"])


def test_decide_post_fix_audit_retry_unknown_or_malformed_fails_closed():
    """Unknown/malformed status should fail closed and avoid retry."""
    unknown = controller.decide_post_fix_audit_retry(
        {"status": "UNKNOWN", "reasons": ["mystery state"]},
        original_task_scope="Fix only PR5B",
    )
    malformed = controller.decide_post_fix_audit_retry(
        None,
        original_task_scope="Fix only PR5B",
    )
    ASSERTIONS.assertFalse(unknown["should_retry"])
    ASSERTIONS.assertEqual(unknown["next_action"], "needs_manual_post_fix_audit_failed")
    ASSERTIONS.assertTrue(unknown["malformed_status"])
    ASSERTIONS.assertFalse(malformed["should_retry"])
    ASSERTIONS.assertEqual(malformed["next_action"], "needs_manual_post_fix_audit_failed")
    ASSERTIONS.assertTrue(malformed["malformed_status"])


def test_decide_post_fix_audit_retry_preserves_allowlist_in_retry_task():
    """Retry task should preserve the provided allowlist entries."""
    decision = controller.decide_post_fix_audit_retry(
        {"status": "FAIL", "reasons": ["scope-preserving retry needed"]},
        original_task_scope="Fix only PR5B",
        files_allowed=["scripts/pr_automation_controller.py", "tests/scripts/test_pr_automation_controller.py"],
        retry_count=0,
        retry_limit=1,
    )
    ASSERTIONS.assertIn("Files allowed:", decision["retry_task"])
    ASSERTIONS.assertIn("- scripts/pr_automation_controller.py", decision["retry_task"])
    ASSERTIONS.assertIn("- tests/scripts/test_pr_automation_controller.py", decision["retry_task"])


def test_decide_post_fix_audit_retry_ledger_event_append_no_pr5c_summary_generation(tmp_path):
    """Retry ledger writes remain append-only and do not generate PR5C summary artifacts."""
    ledger = controller.automation_ledger_path(str(tmp_path), 225)
    decision = controller.decide_post_fix_audit_retry(
        {"status": "FAIL", "reasons": ["exact failing reason"]},
        original_task_scope="Fix only PR5B",
        files_allowed=["scripts/pr_automation_controller.py"],
        retry_count=0,
        retry_limit=2,
        ledger_path=ledger,
        ledger_metadata={"pr": 225, "attempt": 1},
    )
    ASSERTIONS.assertTrue(decision["should_retry"])
    events = controller.read_automation_ledger_events(ledger)
    ASSERTIONS.assertEqual(len(events), 1)
    ASSERTIONS.assertEqual(events[0]["event_type"], "post_fix_audit_retry_scheduled")
    ASSERTIONS.assertFalse(events[0]["details"]["malformed_status"])
    ASSERTIONS.assertFalse((tmp_path / "summary.md").exists())
    ASSERTIONS.assertFalse((tmp_path / "pr-225" / "summary.md").exists())


def test_decide_post_fix_audit_retry_multiline_reason_sanitized_in_retry_task():
    """Multiline failure reasons should be rendered as an indented exact block."""
    decision = controller.decide_post_fix_audit_retry(
        {"status": "FAIL", "reasons": ["line 1\nline 2"]},
        original_task_scope="Fix only PR5B",
        files_allowed=["scripts/pr_automation_controller.py"],
        retry_count=0,
        retry_limit=1,
    )
    ASSERTIONS.assertIn("Failure reason (exact):", decision["retry_task"])
    ASSERTIONS.assertIn("  line 1", decision["retry_task"])
    ASSERTIONS.assertIn("  line 2", decision["retry_task"])


def test_decide_post_fix_audit_retry_omits_files_allowed_when_empty():
    """Retry prompt should include allowlist section only when entries are present."""
    decision = controller.decide_post_fix_audit_retry(
        {"status": "FAIL", "reasons": ["narrow retry"]},
        original_task_scope="Fix only PR5B",
        files_allowed=[],
        retry_count=0,
        retry_limit=1,
    )
    ASSERTIONS.assertNotIn("Files allowed:", decision["retry_task"])


def test_decide_post_fix_audit_retry_blocked_ledger_event_includes_flags(tmp_path):
    """Blocked retry should append blocked ledger event with required failure flags/details."""
    ledger = controller.automation_ledger_path(str(tmp_path), 225)
    decision = controller.decide_post_fix_audit_retry(
        {"status": "FAIL", "reasons": ["same failure"]},
        original_task_scope="Fix only PR5B",
        retry_count=1,
        retry_limit=1,
        previous_failure_reasons=["same failure"],
        ledger_path=ledger,
        ledger_metadata={"pr": 225, "attempt": 2},
    )
    ASSERTIONS.assertFalse(decision["should_retry"])
    ASSERTIONS.assertEqual(decision["next_action"], "needs_manual_post_fix_audit_failed")
    events = controller.read_automation_ledger_events(ledger)
    ASSERTIONS.assertEqual(events[-1]["event_type"], "post_fix_audit_retry_blocked")
    details = events[-1]["details"]
    ASSERTIONS.assertEqual(details["failure_reason"], "same failure")
    ASSERTIONS.assertTrue(details["retry_budget_exhausted"])
    ASSERTIONS.assertTrue(details["repeated_failure"])
    ASSERTIONS.assertFalse(details["malformed_status"])


def test_decide_post_fix_audit_retry_normalizes_malformed_retry_options_safely():
    """Non-list retry options should fail closed to empty lists without raising."""
    decision = controller.decide_post_fix_audit_retry(
        {"status": "FAIL", "reasons": ["single failure"]},
        original_task_scope="Fix only PR5B",
        files_allowed="bad",
        files_forbidden="bad",
        previous_failure_reasons=1,
        retry_count=0,
        retry_limit=2,
    )
    ASSERTIONS.assertTrue(decision["should_retry"])
    ASSERTIONS.assertNotIn("Files allowed:", decision["retry_task"])
    ASSERTIONS.assertNotIn("Files forbidden:", decision["retry_task"])


def test_post_fix_retry_public_api_exports_decide_post_fix_audit_retry():
    """Retry decision entrypoint should be exported as part of module public API."""
    ASSERTIONS.assertIn("decide_post_fix_audit_retry", controller.__all__)


def test_decide_post_fix_audit_retry_raw_pass_downgraded_does_not_retry():
    """Raw PASS/PASSED downgraded by normalization must fail closed without retry."""
    for raw_status in ("PASS", "PASSED"):
        decision = controller.decide_post_fix_audit_retry(
            {"status": raw_status, "next_action": "retry_fix_within_budget", "reasons": ["malformed payload"]},
            original_task_scope="Fix only PR5B",
            retry_count=0,
            retry_limit=2,
        )
        ASSERTIONS.assertFalse(decision["should_retry"])
        ASSERTIONS.assertEqual(decision["next_action"], "needs_manual_post_fix_audit_failed")


def test_decide_post_fix_audit_retry_raw_failed_is_retryable():
    """Raw FAILED should behave as retryable FAIL when reason and budget permit."""
    decision = controller.decide_post_fix_audit_retry(
        {"status": "FAILED", "reasons": ["single targeted fix needed"]},
        original_task_scope="Fix only PR5B",
        files_allowed=["scripts/pr_automation_controller.py"],
        retry_count=0,
        retry_limit=2,
    )
    ASSERTIONS.assertTrue(decision["should_retry"])
    ASSERTIONS.assertEqual(decision["next_action"], "retry_fix_within_budget")


def _ledger_event(
    event_type: str = "",
    *,
    next_action: str = "",
    details: dict[str, Any] | None = None,
    **metadata: Any,
) -> dict[str, Any]:
    return controller.build_automation_ledger_event(
        event_type=event_type,
        next_action=next_action,
        details=details or {},
        **metadata,
    )


def test_build_automation_ledger_latest_empty_safe_defaults_to_needs_manual():
    """Empty ledger should be safe and route to needs_manual for insufficient data."""
    latest = controller.build_automation_ledger_latest([], retry_limit=2)
    ASSERTIONS.assertEqual(latest["next_action"], "needs_manual")
    ASSERTIONS.assertIn("insufficient ledger data", latest["reason"])
    ASSERTIONS.assertEqual(latest["event_count"], 0)


def test_decide_ledger_next_action_empty_latest_is_insufficient():
    """Truly empty latest projection should be insufficient."""
    action, reason = controller.decide_automation_ledger_next_action({}, retry_limit=2)
    ASSERTIONS.assertEqual(action, "needs_manual")
    ASSERTIONS.assertIn("insufficient ledger data", reason)


def test_decide_ledger_next_action_with_decision_fields_is_sufficient_retry():
    """Decision fields without event_count should still be policy-evaluable."""
    latest = {
        "last_post_fix_audit": "PASS",
        "last_validation": "FAIL",
        "retry_count": 0,
        "retry_limit": 2,
        "attempt_count": 1,
        "repeated_failure": False,
        "churn_detected": False,
        "has_ready_event": False,
    }
    action, reason = controller.decide_automation_ledger_next_action(latest, retry_limit=2)
    ASSERTIONS.assertEqual(action, "retry")
    ASSERTIONS.assertIn("validation failed after audit pass", reason)


def test_decide_ledger_next_action_with_decision_fields_no_budget_needs_manual():
    """Decision fields plus exhausted budget should stop safely."""
    latest = {
        "last_post_fix_audit": "PASS",
        "last_validation": "FAIL",
        "retry_count": 2,
        "attempt_count": 1,
        "repeated_failure": False,
        "churn_detected": False,
        "has_ready_event": False,
    }
    action, _reason = controller.decide_automation_ledger_next_action(latest, retry_limit=2)
    ASSERTIONS.assertEqual(action, "needs_manual")


def test_build_automation_ledger_latest_ignores_malformed_events_safely():
    """Non-dict ledger rows should be ignored without raising."""
    latest = controller.build_automation_ledger_latest([cast(Any, "bad"), cast(Any, 2), {"event_type": "x"}])
    ASSERTIONS.assertEqual(latest["event_count"], 1)
    ASSERTIONS.assertEqual(latest["latest_event_type"], "x")


def _ledger_latest_fixture() -> dict[str, Any]:
    return controller.build_automation_ledger_latest([_ledger_latest_event()], retry_limit=3)


def _ledger_latest_event() -> dict[str, Any]:
    details = {
        "status": "PASS",
        "validation": "FAIL",
        "retry_count": 1,
        "fixed_blockers": 2,
        "new_blockers": 1,
        "commit_sha": "deadbeef",
        "pushed": True,
    }
    return _ledger_event(
        "post_fix_audit_retry_scheduled",
        attempt=3,
        repo="owner/repo",
        pr=225,
        branch="chore/pr5c",
        head_sha="abc123",
        task_id="claude_bug_pr5c_ledger_summary_retry_policy",
        details=details,
    )


def test_build_automation_ledger_latest_includes_identity_keys():
    """Latest projection should include repository identity keys."""
    latest = _ledger_latest_fixture()
    required = {"repo", "pr", "branch", "head_sha", "task_id"}
    ASSERTIONS.assertTrue(required.issubset(set(latest)))


def test_build_automation_ledger_latest_includes_policy_keys():
    """Latest projection should include policy and decision keys."""
    latest = _ledger_latest_fixture()
    required = {"attempt_count", "retry_count", "repeated_failure", "churn_detected", "next_action", "reason"}
    ASSERTIONS.assertTrue(required.issubset(set(latest)))


def test_build_automation_ledger_latest_includes_status_keys():
    """Latest projection should include blocker and status keys."""
    latest = _ledger_latest_fixture()
    required = {"fixed_blockers", "new_blockers", "last_post_fix_audit", "last_validation", "last_commit_sha", "pushed"}
    ASSERTIONS.assertTrue(required.issubset(set(latest)))


def test_render_automation_ledger_summary_includes_required_fields():
    """Summary markdown should include action/reason/audit/validation status lines."""
    latest = controller.build_automation_ledger_latest(
        [_ledger_event("x", pr=225, head_sha="abc", task_id="t", details={"status": "PASS", "validation": "FAIL"})],
        retry_limit=2,
    )
    summary = controller.render_automation_ledger_summary(latest)
    ASSERTIONS.assertIn("Next action:", summary)
    ASSERTIONS.assertIn("Reason:", summary)
    ASSERTIONS.assertIn("Last post-fix audit:", summary)
    ASSERTIONS.assertIn("Validation:", summary)


def test_ledger_decision_repeated_same_failure_needs_manual():
    """Repeated normalized failure reasons should route to needs_manual."""
    latest = controller.build_automation_ledger_latest(
        [
            _ledger_event("post_fix_audit_failure", reason="Missing   test"),
            _ledger_event("post_fix_audit_failure", reason="missing test"),
        ],
        retry_limit=4,
    )
    ASSERTIONS.assertEqual(latest["next_action"], "needs_manual")
    ASSERTIONS.assertTrue(latest["repeated_failure"])


def test_ledger_repeated_failure_ignores_pass_events_with_pass_reason():
    """PASS/PASSED events with reason PASS must not count as repeated failures."""
    latest = controller.build_automation_ledger_latest(
        [
            _ledger_event(
                "post_fix_audit_retry_blocked",
                reason="PASS",
                next_action="validation_then_commit",
                details={"status": "PASS", "failure_reason": "ok"},
            ),
            _ledger_event(
                "post_fix_audit_retry_blocked",
                reason="PASS",
                next_action="validation_then_commit",
                details={"status": "PASSED", "failure_reason": "ok"},
            ),
        ],
        retry_limit=2,
    )
    ASSERTIONS.assertFalse(latest["repeated_failure"])
    ASSERTIONS.assertEqual(latest["next_action"], "ready")


def test_ledger_repeated_failure_true_for_two_actual_failures_same_reason():
    """Two failure-like rows with same reason should set repeated_failure True."""
    latest = controller.build_automation_ledger_latest(
        [
            _ledger_event("post_fix_audit_retry_blocked", reason="schema drift", details={"status": "FAIL"}),
            _ledger_event("post_fix_audit_failure", reason="schema drift", details={"status": "PARTIAL"}),
        ],
        retry_limit=3,
    )
    ASSERTIONS.assertTrue(latest["repeated_failure"])
    ASSERTIONS.assertEqual(latest["next_action"], "needs_manual")


def test_ledger_decision_latest_scheduled_retry_beats_retry_budget():
    """Latest scheduled retry should run even when retry_count reaches retry_limit."""
    latest = controller.build_automation_ledger_latest(
        [
            _ledger_event(
                "post_fix_audit_retry_scheduled",
                details={"retry_count": 2, "status": "FAIL", "validation": "FAIL"},
            )
        ],
        retry_limit=2,
    )
    ASSERTIONS.assertEqual(latest["next_action"], "retry")
    ASSERTIONS.assertIn("scheduled retry", latest["reason"])


def test_ledger_decision_older_scheduled_then_latest_blocked_failure_is_needs_manual():
    """Older scheduled retry must not override latest blocked failure."""
    latest = controller.build_automation_ledger_latest(
        [
            _ledger_event("post_fix_audit_retry_scheduled", details={"retry_count": 1}),
            _ledger_event("post_fix_audit_retry_blocked", details={"status": "FAIL", "retry_count": 2}),
        ],
        retry_limit=2,
    )
    ASSERTIONS.assertEqual(latest["next_action"], "needs_manual")
    ASSERTIONS.assertIn("blocked automated retry", latest["reason"])


def test_ledger_decision_older_scheduled_then_latest_clean_ready_is_ready():
    """Older scheduled retry must not override latest clean ready."""
    latest = controller.build_automation_ledger_latest(
        [
            _ledger_event("post_fix_audit_retry_scheduled", details={"retry_count": 2}),
            _ledger_event("clean_ready", details={"status": "PASS", "validation": "PASS"}),
        ],
        retry_limit=2,
    )
    ASSERTIONS.assertEqual(latest["next_action"], "ready")


def test_ledger_decision_new_blockers_exceed_fixed_needs_manual():
    """More new blockers than fixed blockers should fail closed to needs_manual."""
    latest = controller.build_automation_ledger_latest(
        [
            _ledger_event(
                "post_fix_audit_retry_blocked",
                details={"fixed_blockers": 1, "new_blockers": 3, "retry_count": 1},
            )
        ],
        retry_limit=1,
    )
    ASSERTIONS.assertEqual(latest["next_action"], "needs_manual")
    ASSERTIONS.assertTrue(bool(latest["reason"]))


def test_ledger_decision_retry_scheduled_with_budget_remaining_is_retry():
    """A scheduled retry with remaining budget should return retry next_action."""
    latest = controller.build_automation_ledger_latest(
        [
            _ledger_event(
                "post_fix_audit_retry_scheduled",
                details={"retry_count": 1, "status": "FAIL", "failure_reason": "targeted issue"},
            )
        ],
        retry_limit=3,
    )
    ASSERTIONS.assertEqual(latest["next_action"], "retry")


def test_ledger_decision_latest_event_type_ready_is_ready():
    """Latest ready event_type should map to ready next_action."""
    latest = controller.build_automation_ledger_latest(
        [_ledger_event("clean_ready", details={"status": "PASS", "validation": "PASS"})],
        retry_limit=1,
    )
    ASSERTIONS.assertEqual(latest["next_action"], "ready")


def test_ledger_decision_latest_next_action_ready_is_ready():
    """Latest next_action ready should map to ready."""
    latest = controller.build_automation_ledger_latest([_ledger_event("x", next_action="ready")], retry_limit=1)
    ASSERTIONS.assertEqual(latest["next_action"], "ready")


def test_ledger_decision_clean_ready_after_exhausted_budget_is_ready():
    """Latest clean ready should stay ready after prior exhausted retry budget."""
    latest = controller.build_automation_ledger_latest(
        [
            _ledger_event("post_fix_audit_retry_scheduled", details={"retry_count": 2, "status": "FAIL"}),
            _ledger_event("clean_ready", details={"status": "PASS", "validation": "PASS"}),
        ],
        retry_limit=2,
    )
    ASSERTIONS.assertEqual(latest["next_action"], "ready")


def test_ledger_decision_clean_ready_wins_after_failure_history():
    """Latest clean ready should win over historical repeated failures."""
    latest = controller.build_automation_ledger_latest(
        [
            _ledger_event("post_fix_audit_failure", reason="same reason", details={"status": "FAIL"}),
            _ledger_event("post_fix_audit_failure", reason="same reason", details={"status": "FAILED"}),
            _ledger_event("clean_ready", details={"status": "PASS", "validation": "PASS"}),
        ],
        retry_limit=4,
    )
    ASSERTIONS.assertEqual(latest["next_action"], "ready")


def test_ledger_decision_clean_ready_wins_after_churn_history():
    """Latest clean ready should win over historical churn flags."""
    latest = controller.build_automation_ledger_latest(
        [
            _ledger_event("post_fix_audit_failure", details={"status": "FAIL"}),
            _ledger_event("post_fix_audit_retry_scheduled"),
            _ledger_event("post_fix_audit_retry_blocked", details={"status": "PARTIAL"}),
            _ledger_event("post_fix_audit_retry_scheduled"),
            _ledger_event("clean_ready", details={"status": "PASS", "validation": "PASS"}),
        ],
        retry_limit=4,
    )
    ASSERTIONS.assertEqual(latest["next_action"], "ready")


def test_ledger_decision_older_ready_then_new_failure_is_not_ready():
    """Older ready should be invalidated by later failure."""
    latest = controller.build_automation_ledger_latest(
        [_ledger_event("clean_ready"), _ledger_event("post_fix_audit_failure", details={"status": "FAIL"})],
        retry_limit=2,
    )
    ASSERTIONS.assertNotEqual(latest["next_action"], "ready")


def test_ledger_decision_latest_blocked_retry_needs_manual():
    """Latest blocked retry must route to needs_manual."""
    latest = controller.build_automation_ledger_latest(
        [_ledger_event("post_fix_audit_retry_blocked", details={"retry_count": 0})],
        retry_limit=2,
    )
    ASSERTIONS.assertEqual(latest["next_action"], "needs_manual")


def test_ledger_decision_latest_blocked_retry_pass_validation_commit_is_ready():
    """Blocked retry with PASS validation_then_commit should stay forward-ready."""
    latest = controller.build_automation_ledger_latest(
        [
            _ledger_event(
                "post_fix_audit_retry_blocked",
                next_action="validation_then_commit",
                details={"status": "PASS", "retry_count": 0},
            )
        ],
        retry_limit=2,
    )
    ASSERTIONS.assertEqual(latest["next_action"], "ready")
    ASSERTIONS.assertIn("validation/commit may proceed", latest["reason"])


def test_ledger_decision_latest_blocked_retry_missing_action_does_not_backfill():
    """Latest blocked failure must fail closed when only older row has ready action."""
    latest = controller.build_automation_ledger_latest(
        [
            _ledger_event("clean_ready", next_action="validation_then_commit", details={"status": "PASS"}),
            _ledger_event("post_fix_audit_retry_blocked", details={"status": "FAIL", "retry_count": 0}),
        ],
        retry_limit=2,
    )
    ASSERTIONS.assertEqual(latest["next_action"], "needs_manual")
    ASSERTIONS.assertIn("blocked automated retry", latest["reason"])


def test_ledger_decision_old_validation_then_commit_then_latest_fail_is_needs_manual():
    """Latest blocked FAIL without next_action must not inherit older forward action."""
    latest = controller.build_automation_ledger_latest(
        [
            _ledger_event("post_fix_audit_retry_blocked", next_action="validation_then_commit"),
            _ledger_event("post_fix_audit_retry_blocked", details={"status": "FAIL", "retry_count": 0}),
        ],
        retry_limit=2,
    )
    ASSERTIONS.assertEqual(latest["next_action"], "needs_manual")


def test_ledger_decision_latest_blocked_retry_partial_stays_needs_manual():
    """Blocked retry with partial/failing semantics must stay needs_manual."""
    latest = controller.build_automation_ledger_latest(
        [_ledger_event("post_fix_audit_retry_blocked", details={"status": "PARTIAL", "retry_count": 0})],
        retry_limit=2,
    )
    ASSERTIONS.assertEqual(latest["next_action"], "needs_manual")


def test_ledger_decision_latest_blocked_retry_pass_can_override_repeated_failure():
    """Latest blocked PASS should be forward-ready despite older repeated failures."""
    latest = controller.build_automation_ledger_latest(
        [
            _ledger_event("post_fix_audit_failure", reason="same failure"),
            _ledger_event("post_fix_audit_failure", reason="same failure"),
            _ledger_event("post_fix_audit_retry_blocked", details={"status": "PASS", "retry_count": 1}),
        ],
        retry_limit=2,
    )
    ASSERTIONS.assertEqual(latest["next_action"], "ready")


def test_ledger_decision_pass_then_validation_fail_with_budget_is_retry():
    """Latest PASS audit and FAIL validation should retry with budget."""
    latest = controller.build_automation_ledger_latest(
        [_ledger_event("x", details={"status": "PASS", "validation": "FAIL", "retry_count": 0})],
        retry_limit=2,
    )
    ASSERTIONS.assertEqual(latest["next_action"], "retry")


def test_ledger_decision_pass_then_validation_fail_no_budget_needs_manual():
    """Latest PASS audit and FAIL validation should stop without budget."""
    latest = controller.build_automation_ledger_latest(
        [_ledger_event("x", details={"status": "PASS", "validation": "FAIL", "retry_count": 1})],
        retry_limit=1,
    )
    ASSERTIONS.assertEqual(latest["next_action"], "needs_manual")


def test_ledger_decision_default_budget_remains_is_retry():
    """Default path with budget remaining should retry."""
    latest = controller.build_automation_ledger_latest([_ledger_event("x")], retry_limit=2)
    ASSERTIONS.assertEqual(latest["next_action"], "retry")


def test_retry_limit_zero_or_negative_normalizes_to_one():
    """Retry limit zero and negative should normalize to one."""
    latest_zero = controller.build_automation_ledger_latest([_ledger_event("x")], retry_limit=0)
    latest_negative = controller.build_automation_ledger_latest([_ledger_event("x")], retry_limit=-2)
    ASSERTIONS.assertEqual(latest_zero["next_action"], "retry")
    ASSERTIONS.assertEqual(latest_negative["next_action"], "retry")


def test_automation_ledger_churn_detected_true():
    """Churn is true for repeated failures plus scheduled retries."""
    events = [
        _ledger_event("post_fix_audit_failure", details={"status": "FAIL"}),
        _ledger_event("post_fix_audit_retry_scheduled"),
        _ledger_event("post_fix_audit_retry_blocked", details={"status": "PARTIAL"}),
        _ledger_event("post_fix_audit_retry_scheduled"),
    ]
    ASSERTIONS.assertTrue(controller.automation_ledger_churn_detected(events))


def test_automation_ledger_churn_detected_false_below_threshold():
    """Churn is false below failure/retry thresholds."""
    events = [_ledger_event("post_fix_audit_failure"), _ledger_event("post_fix_audit_retry_scheduled")]
    ASSERTIONS.assertFalse(controller.automation_ledger_churn_detected(events))


def test_build_automation_ledger_latest_sets_churn_detected_and_needs_manual():
    """Latest projection should flag churn and force needs_manual."""
    events = [
        _ledger_event("post_fix_audit_failure", details={"status": "FAIL"}),
        _ledger_event("post_fix_audit_retry_scheduled"),
        _ledger_event("post_fix_audit_failure", details={"validation": "FAILED"}),
        _ledger_event("post_fix_audit_retry_scheduled", details={"retry_count": 0}),
    ]
    latest = controller.build_automation_ledger_latest(events, retry_limit=4)
    ASSERTIONS.assertTrue(latest["churn_detected"])
    ASSERTIONS.assertEqual(latest["next_action"], "needs_manual")


def test_write_automation_ledger_summary_files_writes_latest_and_summary(tmp_path):
    """Summary writer should create deterministic latest.json and summary.md files."""
    latest = controller.build_automation_ledger_latest([_ledger_event("x", pr=225)], retry_limit=2)
    paths = controller.write_automation_ledger_summary_files(tmp_path / ".autofix" / "ledger", latest)
    latest_path = tmp_path / ".autofix" / "ledger" / "latest.json"
    summary_path = tmp_path / ".autofix" / "ledger" / "summary.md"
    ASSERTIONS.assertEqual(paths["latest_json"], str(latest_path))
    ASSERTIONS.assertEqual(paths["summary_md"], str(summary_path))
    ASSERTIONS.assertTrue(latest_path.exists())
    ASSERTIONS.assertTrue(summary_path.exists())
    persisted = json.loads(latest_path.read_text(encoding="utf-8"))
    ASSERTIONS.assertEqual(persisted["next_action"], latest["next_action"])


def test_build_automation_ledger_latest_does_not_expose_env_secrets(monkeypatch):
    """Latest projection must not include environment secret values."""
    monkeypatch.setenv("SECRET_TOKEN", "ultra-secret-value")
    latest = controller.build_automation_ledger_latest(
        [_ledger_event("post_fix_audit_failure", reason="x", repo="owner/repo", pr=225)],
        retry_limit=2,
    )
    blob = json.dumps(latest, sort_keys=True)
    ASSERTIONS.assertNotIn("SECRET_TOKEN", blob)
    ASSERTIONS.assertNotIn("ultra-secret-value", blob)


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
    monkeypatch.setattr(
        controller,
        "controller_codacy_blocking_evidence",
        lambda *_args, **_kwargs: controller.codacy_evidence_without_checks(),
    )
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
    ASSERTIONS.assertEqual(decision["next_action"], "rerun_stale_or_cancelled_checks")
    ASSERTIONS.assertEqual(decision["actions"][0]["type"], "rerun_cancelled")


def test_pr_flow_guardrails_cancelled_only_reruns_no_safe_launch(monkeypatch):
    """Cancelled PR flow guardrails alone should trigger rerun-only handling."""
    monkeypatch.setattr(
        controller,
        "launch_safe_autofix",
        lambda _cfg: (_ for _ in ()).throw(
            AssertionError("safe autofix must not launch for rerun-only")
        ),
    )
    decision: dict = {"actions": [], "warnings": [], "errors": []}
    checks = [
        _check("PR flow guardrails", "CANCELLED", "https://github.com/owner/repo/actions/runs/101"),
    ]

    handled = controller.handle_cancelled_checks(
        checks,
        controller.RerunConfig(repo="owner/repo", dry_run=True, max_reruns=3),
        decision,
    )

    ASSERTIONS.assertTrue(handled)
    ASSERTIONS.assertEqual(decision["next_action"], "rerun_stale_or_cancelled_checks")
    ASSERTIONS.assertEqual(decision["actions"][0]["type"], "rerun_cancelled")


def test_merge_readiness_cancelled_only_reruns_no_safe_launch(monkeypatch):
    """Cancelled merge readiness alone should trigger rerun-only handling."""
    monkeypatch.setattr(
        controller,
        "launch_safe_autofix",
        lambda _cfg: (_ for _ in ()).throw(
            AssertionError("safe autofix must not launch for rerun-only")
        ),
    )
    decision: dict = {"actions": [], "warnings": [], "errors": []}
    checks = [
        _check("PR Merge Readiness", "CANCELLED", "https://github.com/owner/repo/actions/runs/102"),
    ]

    handled = controller.handle_cancelled_checks(
        checks,
        controller.RerunConfig(repo="owner/repo", dry_run=True, max_reruns=3),
        decision,
    )

    ASSERTIONS.assertTrue(handled)
    ASSERTIONS.assertEqual(decision["next_action"], "rerun_stale_or_cancelled_checks")
    ASSERTIONS.assertEqual(decision["actions"][0]["type"], "rerun_cancelled")


def test_real_blocker_plus_cancelled_check_does_not_use_rerun_only_flow():
    """A real blocker must prevent stale/cancelled-only rerun handling."""
    decision: dict = {"actions": [], "warnings": [], "errors": []}
    checks = [
        _check("Unit tests", "FAILURE", "https://github.com/owner/repo/actions/runs/201"),
        _check("PR flow guardrails", "CANCELLED", "https://github.com/owner/repo/actions/runs/202"),
    ]

    handled = controller.handle_cancelled_checks(
        checks,
        controller.RerunConfig(repo="owner/repo", dry_run=True, max_reruns=3),
        decision,
    )

    ASSERTIONS.assertFalse(handled)
    ASSERTIONS.assertEqual(decision["actions"], [])


def test_pending_check_plus_cancelled_check_waits_pending_no_rerun():
    """A real pending check must prevent stale/cancelled-only rerun handling."""
    decision: dict = {"actions": [], "warnings": [], "errors": []}
    checks = [
        _check("Integration tests", "IN_PROGRESS", "https://github.com/owner/repo/actions/runs/301"),
        _check("PR Merge Readiness", "CANCELLED", "https://github.com/owner/repo/actions/runs/302"),
    ]

    handled = controller.handle_cancelled_checks(
        checks,
        controller.RerunConfig(repo="owner/repo", dry_run=True, max_reruns=3),
        decision,
    )

    ASSERTIONS.assertFalse(handled)


def _gh_run(
    name: str,
    state: str,
    head: str = "",
    run_id: int = 0,
    started: str = "",
    completed: str = "",
) -> dict[str, Any]:
    check: dict[str, Any] = {"name": name, "status": state, "conclusion": state, "id": run_id}
    if run_id:
        check["details_url"] = f"https://github.com/org/repo/actions/runs/{run_id}/job/1"
    if head:
        check["head_sha"] = head
    if started:
        check["started_at"] = started
    if completed:
        check["completed_at"] = completed
    return check


def _with_source(run: dict[str, Any], **fields: Any) -> dict[str, Any]:
    enriched = dict(run)
    for key, value in fields.items():
        enriched[key] = value
    return enriched


def test_stale_head_failed_check_ignored():
    summary = controller.summarize_current_head_check_state(
        "head-new",
        [_gh_run("Unit tests", "FAILURE", "head-old", run_id=11)],
    )
    ASSERTIONS.assertEqual(summary["current_blocker_count"], 0)
    ASSERTIONS.assertEqual(summary["next_action"], "no_action")


def test_stale_count_includes_ignored_stale_duplicate_runs():
    runs = [
        _gh_run("Unit tests", "SUCCESS", "head-new", run_id=2, started="2026-05-25T00:01:00Z"),
        _gh_run("Unit tests", "FAILURE", "head-old", run_id=1, started="2026-05-25T00:00:00Z"),
    ]
    summary = controller.summarize_current_head_check_state("head-new", runs)
    ASSERTIONS.assertIn("1", summary["ignored_stale_run_ids"])
    ASSERTIONS.assertGreaterEqual(summary["stale_count"], 1)


def test_stale_head_stale_duplicate_with_current_success_preserves_ignored_evidence_id():
    runs = [
        _with_source(
            _gh_run("Unit tests", "SUCCESS", "head-new", run_id=2, started="2026-05-25T00:01:00Z"),
            workflowName="unit-tests-ci",
        ),
        {
            "name": "Unit tests",
            "status": "STALE",
            "conclusion": "STALE",
            "head_sha": "head-old",
            "id": 1,
            "started_at": "2026-05-25T00:00:00Z",
            "workflowName": "unit-tests-ci",
        },
    ]
    summary = controller.summarize_current_head_check_state("head-new", runs)
    ASSERTIONS.assertEqual(summary["current_blocker_count"], 0)
    ASSERTIONS.assertIn("1", summary["ignored_stale_run_ids"])
    ASSERTIONS.assertGreaterEqual(summary["stale_count"], 1)


def test_stale_head_failure_duplicate_with_current_success_preserves_ignored_evidence_id():
    runs = [
        _with_source(
            _gh_run("Unit tests", "SUCCESS", "head-new", run_id=22, started="2026-05-25T00:01:00Z"),
            workflowName="unit-tests-ci",
        ),
        {
            "name": "Unit tests",
            "status": "FAILURE",
            "conclusion": "FAILURE",
            "head_sha": "head-old",
            "id": 21,
            "started_at": "2026-05-25T00:00:00Z",
            "workflowName": "unit-tests-ci",
        },
    ]
    summary = controller.summarize_current_head_check_state("head-new", runs)
    ASSERTIONS.assertEqual(summary["current_blocker_count"], 0)
    ASSERTIONS.assertIn("21", summary["ignored_stale_run_ids"])
    ASSERTIONS.assertGreaterEqual(summary["stale_count"], 1)


def test_current_success_with_missing_head_duplicate_is_unknown_head_needs_manual():
    runs = [
        _with_source(
            _gh_run("Unit tests", "SUCCESS", "head-new", run_id=522, started="2026-05-25T00:01:00Z"),
            workflowName="unit-tests-ci",
        ),
        {
            "name": "Unit tests",
            "status": "SUCCESS",
            "conclusion": "SUCCESS",
            "id": 521,
            "started_at": "2026-05-25T00:00:00Z",
            "workflowName": "unit-tests-ci",
        },
    ]
    summary = controller.summarize_current_head_check_state("head-new", runs)
    ASSERTIONS.assertEqual(summary["category"], "unknown_head")
    ASSERTIONS.assertEqual(summary["next_action"], "needs_manual")
    ASSERTIONS.assertTrue(summary["needs_manual"])
    ASSERTIONS.assertGreaterEqual(summary["unknown_head_count"], 1)
    ASSERTIONS.assertIn("521", summary["ignored_unknown_run_ids"])


def test_current_head_failed_check_is_blocker():
    summary = controller.summarize_current_head_check_state(
        "head-new",
        [_gh_run("Unit tests", "FAILURE", "head-new", run_id=12)],
    )
    ASSERTIONS.assertEqual(summary["current_blocker_count"], 1)
    ASSERTIONS.assertEqual(summary["next_action"], "fix_current_head_checks")


def test_newer_current_head_success_suppresses_older_duplicate_failure():
    runs = [
        _gh_run("Unit tests", "FAILURE", "head-new", run_id=1, started="2026-05-25T00:00:00Z"),
        _gh_run("  Unit   tests ", "SUCCESS", "head-new", run_id=2, started="2026-05-25T00:01:00Z"),
    ]
    summary = controller.summarize_current_head_check_state("head-new", runs)
    ASSERTIONS.assertEqual(summary["current_blocker_count"], 0)
    ASSERTIONS.assertEqual(summary["next_action"], "ready")


def test_current_head_pending_check_returns_wait_pending():
    summary = controller.summarize_current_head_check_state(
        "head-new",
        [_gh_run("Integration", "IN_PROGRESS", "head-new", run_id=13)],
    )
    ASSERTIONS.assertEqual(summary["pending_count"], 1)
    ASSERTIONS.assertEqual(summary["next_action"], "wait_pending")


def test_stale_pending_check_does_not_block():
    summary = controller.summarize_current_head_check_state(
        "head-new",
        [_gh_run("Integration", "IN_PROGRESS", "head-old", run_id=14)],
    )
    ASSERTIONS.assertEqual(summary["pending_count"], 0)
    ASSERTIONS.assertEqual(summary["next_action"], "no_action")


def test_current_head_cancelled_pr_flow_guardrails_plans_rerun():
    plan = controller.build_workflow_rerun_plan(
        "head-new",
        [_gh_run("PR flow guardrails", "CANCELLED", "head-new", run_id=101)],
    )
    ASSERTIONS.assertEqual(plan["rerun_run_ids"], ["101"])
    ASSERTIONS.assertTrue(plan["safe_to_rerun"])


def test_current_head_cancelled_pr_flow_guardrails_string_check_id_no_url_not_rerunnable():
    run = _gh_run("PR flow guardrails", "CANCELLED", "head-new", run_id=0)
    run["id"] = "101"
    plan = controller.build_workflow_rerun_plan("head-new", [run, run])
    ASSERTIONS.assertEqual(plan["rerun_run_ids"], [])
    summary = controller.summarize_current_head_check_state("head-new", [run])
    ASSERTIONS.assertEqual(summary["category"], "current_head_cancelled_unrerunnable")
    ASSERTIONS.assertEqual(summary["next_action"], "needs_manual")


def test_current_head_cancelled_merge_readiness_plans_rerun():
    plan = controller.build_workflow_rerun_plan(
        "head-new",
        [_gh_run("PR Merge Readiness", "CANCELLED", "head-new", run_id=102)],
    )
    ASSERTIONS.assertEqual(plan["rerun_run_ids"], ["102"])
    ASSERTIONS.assertTrue(plan["safe_to_rerun"])


def test_current_head_stale_with_actions_url_plans_rerun():
    plan = controller.build_workflow_rerun_plan(
        "head-new",
        [_gh_run("PR Merge Readiness", "STALE", "head-new", run_id=1202)],
    )
    ASSERTIONS.assertEqual(plan["rerun_run_ids"], ["1202"])
    ASSERTIONS.assertTrue(plan["safe_to_rerun"])


def test_current_head_stale_without_actions_url_is_needs_manual():
    run = _gh_run("PR Merge Readiness", "STALE", "head-new", run_id=0)
    run["id"] = "1203"
    plan = controller.build_workflow_rerun_plan("head-new", [run])
    ASSERTIONS.assertEqual(plan["rerun_run_ids"], [])
    ASSERTIONS.assertFalse(plan["safe_to_rerun"])
    summary = controller.summarize_current_head_check_state("head-new", [run])
    ASSERTIONS.assertEqual(summary["category"], "current_head_stale_unrerunnable")
    ASSERTIONS.assertEqual(summary["next_action"], "needs_manual")
    ASSERTIONS.assertTrue(summary["needs_manual"])
    ASSERTIONS.assertFalse(summary["safe_to_rerun"])


def test_stale_cancelled_run_is_ignored():
    plan = controller.build_workflow_rerun_plan(
        "head-new",
        [_gh_run("PR flow guardrails", "CANCELLED", "head-old", run_id=103)],
    )
    ASSERTIONS.assertEqual(plan["rerun_run_ids"], [])
    ASSERTIONS.assertIn("103", plan["ignored_stale_run_ids"])


def test_build_workflow_rerun_plan_preserves_ignored_stale_ids_from_dedupe():
    runs = [
        _with_source(
            _gh_run("PR flow guardrails", "SUCCESS", "head-new", run_id=105, started="2026-05-25T00:01:00Z"),
            workflowName="pr-flow-guardrails",
        ),
        {
            "name": "PR flow guardrails",
            "status": "STALE",
            "conclusion": "STALE",
            "head_sha": "head-old",
            "id": 104,
            "started_at": "2026-05-25T00:00:00Z",
            "workflowName": "pr-flow-guardrails",
        },
    ]
    plan = controller.build_workflow_rerun_plan("head-new", runs)
    ASSERTIONS.assertEqual(plan["rerun_run_ids"], [])
    ASSERTIONS.assertIn("104", plan["ignored_stale_run_ids"])


def test_duplicate_run_ids_are_deduped():
    plan = controller.build_workflow_rerun_plan(
        "head-new",
        [
            _gh_run("PR Merge Readiness", "CANCELLED", "head-new", run_id=201),
            _gh_run("PR Merge Readiness", "CANCELLED", "head-new", run_id=201, started="2026-05-25T00:01:00Z"),
        ],
    )
    ASSERTIONS.assertEqual(plan["rerun_run_ids"], ["201"])


def test_blacklisted_cancelled_current_head_check_can_still_be_rerun_planned():
    plan = controller.build_workflow_rerun_plan(
        "head-new",
        [_gh_run("Refresh stale self checks", "CANCELLED", "head-new", run_id=202)],
    )
    ASSERTIONS.assertEqual(plan["rerun_run_ids"], ["202"])
    ASSERTIONS.assertTrue(plan["safe_to_rerun"])


def test_empty_current_head_check_set_is_not_ready():
    summary = controller.summarize_current_head_check_state("head-new", [])
    ASSERTIONS.assertEqual(summary["category"], "no_current_head_checks")
    ASSERTIONS.assertEqual(summary["next_action"], "wait_pending")
    ASSERTIONS.assertFalse(summary["needs_manual"])
    ASSERTIONS.assertFalse(summary["safe_to_rerun"])


def test_missing_head_fails_closed_unknown_not_current():
    details = controller.classify_github_check_run_staleness("head-new", _gh_run("Unit tests", "FAILURE", "", run_id=9))
    ASSERTIONS.assertEqual(details["category"], "unknown_head")
    ASSERTIONS.assertFalse(details["current_head"])
    ASSERTIONS.assertFalse(details["stale"])
    ASSERTIONS.assertFalse(details["safe_to_rerun"])


def test_workflow_helpers_are_passive_and_no_gh(monkeypatch):
    monkeypatch.setattr(
        controller,
        "run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("run() must not be called")),
    )
    checks = [_gh_run("Unit tests", "SUCCESS", "head-new", run_id=301)]
    controller.normalize_github_check_name(" Unit  tests ")
    controller.github_check_run_head(checks[0])
    controller.classify_github_check_run_staleness("head-new", checks[0])
    controller.dedupe_github_check_runs("head-new", checks)
    controller.build_workflow_rerun_plan("head-new", checks)
    summary = controller.summarize_current_head_check_state("head-new", checks)
    ASSERTIONS.assertEqual(summary["category"], "ready")


def test_summarize_missing_head_check_returns_unknown_head_needs_manual():
    summary = controller.summarize_current_head_check_state(
        "head-new",
        [_gh_run("Unit tests", "SUCCESS", "", run_id=401)],
    )
    ASSERTIONS.assertEqual(summary["category"], "unknown_head")
    ASSERTIONS.assertEqual(summary["next_action"], "needs_manual")
    ASSERTIONS.assertTrue(summary["needs_manual"])


def test_summarize_missing_head_check_direct_contract():
    summary = controller.summarize_current_head_check_state(
        "head-new",
        [{"name": "Unit tests", "status": "SUCCESS", "conclusion": "SUCCESS", "id": 499}],
    )
    ASSERTIONS.assertEqual(summary["category"], "unknown_head")
    ASSERTIONS.assertEqual(summary["next_action"], "needs_manual")
    ASSERTIONS.assertTrue(summary["needs_manual"])


def test_summarize_missing_head_check_with_other_current_head_still_needs_manual():
    summary = controller.summarize_current_head_check_state(
        "head-new",
        [
            _gh_run("Unit tests", "SUCCESS", "head-new", run_id=500),
            _gh_run("Lint", "SUCCESS", "", run_id=501),
        ],
    )
    ASSERTIONS.assertEqual(summary["category"], "unknown_head")
    ASSERTIONS.assertEqual(summary["next_action"], "needs_manual")
    ASSERTIONS.assertTrue(summary["needs_manual"])


def test_summarize_missing_head_variants_return_unknown_head_needs_manual():
    runs = [
        {"name": "Unit tests", "status": "SUCCESS", "conclusion": "SUCCESS", "id": 421, "headSha": ""},
        {"name": "Integration", "status": "SUCCESS", "conclusion": "SUCCESS", "id": 422, "headRefOid": ""},
    ]
    summary = controller.summarize_current_head_check_state("head-new", runs)
    ASSERTIONS.assertEqual(summary["category"], "unknown_head")
    ASSERTIONS.assertEqual(summary["next_action"], "needs_manual")
    ASSERTIONS.assertTrue(summary["needs_manual"])


def test_empty_pr_head_sha_returns_unknown_head_needs_manual():
    summary = controller.summarize_current_head_check_state(
        "",
        [_gh_run("Unit tests", "SUCCESS", "head-new", run_id=402)],
    )
    ASSERTIONS.assertEqual(summary["category"], "unknown_head")
    ASSERTIONS.assertEqual(summary["next_action"], "needs_manual")
    ASSERTIONS.assertTrue(summary["needs_manual"])


def test_dedupe_prefers_current_head_over_newer_stale_duplicate():
    runs = [
        _gh_run("Unit tests", "SUCCESS", "head-new", run_id=403, started="2026-05-25T00:00:00Z"),
        _gh_run("Unit tests", "FAILURE", "head-old", run_id=404, started="2026-05-25T00:10:00Z"),
    ]
    deduped = controller.dedupe_github_check_runs("head-new", runs)
    selected = deduped["selected_runs"][0]
    ASSERTIONS.assertEqual(selected.get("id"), 403)
    ASSERTIONS.assertIn("404", deduped["ignored_stale_run_ids"])


def test_dedupe_keeps_same_name_checks_with_different_workflow_name_separate():
    runs = [
        _with_source(
            _gh_run("build", "SUCCESS", "head-new", run_id=431),
            workflowName="ci-build",
        ),
        _with_source(
            _gh_run("build", "FAILURE", "head-new", run_id=432),
            workflowName="release-build",
        ),
    ]
    deduped = controller.dedupe_github_check_runs("head-new", runs)
    selected_ids = {check.get("id") for check in deduped["selected_runs"]}
    ASSERTIONS.assertEqual(selected_ids, {431, 432})


def test_same_name_distinct_source_failure_counts_as_current_blocker():
    runs = [
        _with_source(_gh_run("build", "SUCCESS", "head-new", run_id=433), source="workflow-a"),
        _with_source(_gh_run("build", "FAILURE", "head-new", run_id=434), source="workflow-b"),
    ]
    summary = controller.summarize_current_head_check_state("head-new", runs)
    ASSERTIONS.assertEqual(summary["category"], "workflow_failure")
    ASSERTIONS.assertEqual(summary["current_blocker_count"], 1)


def test_dedupe_keeps_same_name_same_app_with_different_source_separate():
    runs = [
        _with_source(
            _gh_run("build", "SUCCESS", "head-new", run_id=443),
            source="workflow-a",
            app={"name": "GitHub Actions"},
        ),
        _with_source(
            _gh_run("build", "FAILURE", "head-new", run_id=444),
            source="workflow-b",
            app={"name": "GitHub Actions"},
        ),
    ]
    deduped = controller.dedupe_github_check_runs("head-new", runs)
    selected_ids = {check.get("id") for check in deduped["selected_runs"]}
    ASSERTIONS.assertEqual(selected_ids, {443, 444})


def test_dedupe_coalesces_true_duplicate_same_name_and_source():
    runs = [
        _with_source(
            _gh_run("build", "FAILURE", "head-new", run_id=435, started="2026-05-25T00:00:00Z"),
            workflow_name="ci-build",
        ),
        _with_source(
            _gh_run("build", "SUCCESS", "head-new", run_id=436, started="2026-05-25T00:01:00Z"),
            workflow_name="ci-build",
        ),
    ]
    deduped = controller.dedupe_github_check_runs("head-new", runs)
    ASSERTIONS.assertEqual(len(deduped["selected_runs"]), 1)
    ASSERTIONS.assertEqual(deduped["selected_runs"][0].get("id"), 436)
    ASSERTIONS.assertEqual(deduped["ignored_duplicate_run_ids"], ["435"])


def test_stale_newer_duplicate_same_source_does_not_suppress_current_head():
    runs = [
        _with_source(
            _gh_run("build", "SUCCESS", "head-new", run_id=437, started="2026-05-25T00:00:00Z"),
            app={"name": "github-actions"},
        ),
        _with_source(
            _gh_run("build", "FAILURE", "head-old", run_id=438, started="2026-05-25T00:10:00Z"),
            app={"name": "github-actions"},
        ),
    ]
    deduped = controller.dedupe_github_check_runs("head-new", runs)
    ASSERTIONS.assertEqual(deduped["selected_runs"][0].get("id"), 437)
    ASSERTIONS.assertIn("438", deduped["ignored_stale_run_ids"])


def test_current_head_failure_not_suppressed_by_newer_stale_duplicate():
    runs = [
        _gh_run("Unit tests", "FAILURE", "head-new", run_id=405, started="2026-05-25T00:00:00Z"),
        _gh_run("Unit tests", "SUCCESS", "head-old", run_id=406, started="2026-05-25T00:10:00Z"),
    ]
    summary = controller.summarize_current_head_check_state("head-new", runs)
    ASSERTIONS.assertEqual(summary["category"], "workflow_failure")
    ASSERTIONS.assertEqual(summary["next_action"], "fix_current_head_checks")
    ASSERTIONS.assertEqual(summary["current_blocker_count"], 1)


def test_current_head_pending_not_suppressed_by_newer_stale_duplicate():
    runs = [
        _gh_run("Integration", "IN_PROGRESS", "head-new", run_id=407, started="2026-05-25T00:00:00Z"),
        _gh_run("Integration", "SUCCESS", "head-old", run_id=408, started="2026-05-25T00:10:00Z"),
    ]
    summary = controller.summarize_current_head_check_state("head-new", runs)
    ASSERTIONS.assertEqual(summary["category"], "workflow_pending")
    ASSERTIONS.assertEqual(summary["next_action"], "wait_pending")
    ASSERTIONS.assertEqual(summary["pending_count"], 1)


def test_current_head_cancelled_merge_readiness_rerunnable_even_if_newer_stale_duplicate():
    runs = [
        _gh_run("PR Merge Readiness", "CANCELLED", "head-new", run_id=409, started="2026-05-25T00:00:00Z"),
        _gh_run("PR Merge Readiness", "SUCCESS", "head-old", run_id=410, started="2026-05-25T00:10:00Z"),
    ]
    summary = controller.summarize_current_head_check_state("head-new", runs)
    ASSERTIONS.assertEqual(summary["category"], "workflow_cancelled")
    ASSERTIONS.assertEqual(summary["next_action"], "rerun_stale_checks")
    ASSERTIONS.assertEqual(summary["reason"], "rerunnable cancelled current head checks")
    ASSERTIONS.assertEqual(summary["rerun_run_ids"], ["409"])


def test_current_head_failure_takes_priority_over_rerun_cancelled():
    runs = [
        _gh_run("Unit tests", "FAILURE", "head-new", run_id=1501),
        _gh_run("PR Merge Readiness", "CANCELLED", "head-new", run_id=1502),
    ]
    summary = controller.summarize_current_head_check_state("head-new", runs)
    ASSERTIONS.assertGreaterEqual(summary["current_blocker_count"], 1)
    ASSERTIONS.assertEqual(summary["category"], "workflow_failure")
    ASSERTIONS.assertEqual(summary["next_action"], "fix_current_head_checks")
    ASSERTIONS.assertNotEqual(summary["next_action"], "rerun_stale_checks")
    ASSERTIONS.assertNotEqual(summary["category"], "workflow_cancelled")


def test_stale_only_without_unknown_remains_stale_only_no_action():
    summary = controller.summarize_current_head_check_state(
        "head-new",
        [_gh_run("Unit tests", "SUCCESS", "head-old", run_id=411)],
    )
    ASSERTIONS.assertEqual(summary["category"], "stale_only")
    ASSERTIONS.assertEqual(summary["next_action"], "no_action")


def test_stale_head_stale_does_not_block_current_head_ready():
    runs = [
        _gh_run("Unit tests", "SUCCESS", "head-new", run_id=1311),
        _gh_run("Unit tests", "STALE", "head-old", run_id=1312),
    ]
    summary = controller.summarize_current_head_check_state("head-new", runs)
    ASSERTIONS.assertEqual(summary["category"], "ready")
    ASSERTIONS.assertEqual(summary["next_action"], "ready")
    ASSERTIONS.assertEqual(summary["current_blocker_count"], 0)


def test_current_head_cancelled_without_rerunnable_run_id_needs_manual():
    summary = controller.summarize_current_head_check_state(
        "head-new",
        [{"name": "PR flow guardrails", "status": "CANCELLED", "conclusion": "CANCELLED", "head_sha": "head-new"}],
    )
    ASSERTIONS.assertEqual(summary["category"], "current_head_cancelled_unrerunnable")
    ASSERTIONS.assertEqual(summary["next_action"], "needs_manual")
    ASSERTIONS.assertTrue(summary["needs_manual"])


def test_current_head_cancelled_with_only_check_ids_and_no_actions_url_needs_manual():
    summary = controller.summarize_current_head_check_state(
        "head-new",
        [
            {
                "name": "PR flow guardrails",
                "status": "CANCELLED",
                "conclusion": "CANCELLED",
                "head_sha": "head-new",
                "id": 999,
                "databaseId": 888,
            }
        ],
    )
    ASSERTIONS.assertEqual(summary["category"], "current_head_cancelled_unrerunnable")
    ASSERTIONS.assertEqual(summary["next_action"], "needs_manual")
    ASSERTIONS.assertEqual(summary["rerun_run_ids"], [])
    ASSERTIONS.assertFalse(summary["safe_to_rerun"])
    ASSERTIONS.assertTrue(summary["needs_manual"])


def test_do_not_launch_autofix_for_is_blacklist_not_allowlist():
    checks = [
        _gh_run("Refresh stale self checks", "FAILURE", "head-new", run_id=412),
        _gh_run("Unit tests", "FAILURE", "head-new", run_id=413),
    ]
    should_launch, launchable = controller.should_launch_autofix(checks)
    ASSERTIONS.assertTrue(should_launch)
    ASSERTIONS.assertEqual([item["name"] for item in launchable], ["Unit tests"])


def test_do_not_launch_autofix_for_keeps_guardrail_workflows_blacklisted():
    checks = [
        _gh_run("merge readiness", "FAILURE", "head-new", run_id=414),
        _gh_run("pr merge readiness", "FAILURE", "head-new", run_id=415),
        _gh_run("pr flow guardrails", "FAILURE", "head-new", run_id=416),
        _gh_run("Unit tests", "FAILURE", "head-new", run_id=417),
    ]
    should_launch, launchable = controller.should_launch_autofix(checks)
    ASSERTIONS.assertTrue(should_launch)
    ASSERTIONS.assertEqual([item["name"] for item in launchable], ["Unit tests"])


def test_should_not_launch_autofix_for_pr_flow_guardrails_failure_only():
    checks = [_gh_run("PR flow guardrails", "FAILURE", "head-new", run_id=418)]
    should_launch, launchable = controller.should_launch_autofix(checks)
    ASSERTIONS.assertFalse(should_launch)
    ASSERTIONS.assertEqual(launchable, [])


def test_should_not_launch_autofix_for_merge_readiness_failures_only():
    checks = [
        _gh_run("Merge readiness", "FAILURE", "head-new", run_id=419),
        _gh_run("PR Merge Readiness", "FAILURE", "head-new", run_id=420),
    ]
    should_launch, launchable = controller.should_launch_autofix(checks)
    ASSERTIONS.assertFalse(should_launch)
    ASSERTIONS.assertEqual(launchable, [])


def test_should_not_launch_autofix_for_pr_guard_aliases_failure_only():
    checks = [
        _gh_run("pr-guard", "FAILURE", "head-new", run_id=421),
        _gh_run("guard", "FAILURE", "head-new", run_id=422),
    ]
    should_launch, launchable = controller.should_launch_autofix(checks)
    ASSERTIONS.assertFalse(should_launch)
    ASSERTIONS.assertEqual(launchable, [])


def test_run_id_from_check_prefers_actions_run_url_id_over_check_id():
    check = {
        "id": 999,
        "databaseId": 998,
        "detailsUrl": "https://github.com/org/repo/actions/runs/123456789/jobs/1",
    }
    ASSERTIONS.assertEqual(controller.run_id_from_check(check), "123456789")


def test_rerun_plan_uses_actions_run_url_id_instead_of_check_id():
    run = _gh_run("PR flow guardrails", "CANCELLED", "head-new", run_id=999)
    run["detailsUrl"] = "https://github.com/org/repo/actions/runs/7777777/jobs/2"
    plan = controller.build_workflow_rerun_plan("head-new", [run])
    ASSERTIONS.assertEqual(plan["rerun_run_ids"], ["7777777"])


def test_rerun_plan_prefers_actions_run_url_id_12345_over_check_id_999():
    run = _gh_run("PR flow guardrails", "CANCELLED", "head-new", run_id=999)
    run["details_url"] = "https://github.com/org/repo/actions/runs/12345/job/7"
    plan = controller.build_workflow_rerun_plan("head-new", [run])
    ASSERTIONS.assertEqual(plan["rerun_run_ids"], ["12345"])
    ASSERTIONS.assertNotIn("999", plan["rerun_run_ids"])


def test_blacklisted_names_do_not_block_current_head_cancelled_rerun_plan():
    checks = [
        _gh_run("PR flow guardrails", "CANCELLED", "head-new", run_id=423),
        _gh_run("PR Merge Readiness", "CANCELLED", "head-new", run_id=424),
    ]
    should_launch, launchable = controller.should_launch_autofix(checks)
    ASSERTIONS.assertFalse(should_launch)
    ASSERTIONS.assertEqual(launchable, [])
    plan = controller.build_workflow_rerun_plan("head-new", checks)
    ASSERTIONS.assertEqual(plan["rerun_run_ids"], ["423", "424"])


def test_codacy_head_match_contract_exposes_pr_head_and_evidence_head():
    """Codacy evidence contract should expose headRefOid vs Codacy evidence head and match flag."""
    if hasattr(controller, "codacy_head_matches"):
        evidence = controller.codacy_head_matches("abc123", {"head": "abc123"})
        ASSERTIONS.assertEqual(evidence["pr_head"], "abc123")
        ASSERTIONS.assertEqual(evidence["codacy_head"], "abc123")
        ASSERTIONS.assertTrue(evidence["match"])
        return
    if hasattr(controller, "classify_codacy_evidence"):
        payload = _codacy_state_payload(api_issues=1) | {"headRefOid": "abc123", "codacy_head": "abc123"}
        evidence = controller.classify_codacy_evidence(payload)
        ASSERTIONS.assertTrue(evidence["head_match"])
        return
    raise NotImplementedError("codacy_head_matches/classify_codacy_evidence not implemented")


def test_classify_codacy_states_contract():
    """Codacy classifications should map check/API/annotation evidence deterministically."""
    if not hasattr(controller, "classify_codacy_evidence"):
        raise NotImplementedError("classify_codacy_evidence not implemented")
    _assert_codacy_classifications(controller.classify_codacy_evidence)


def _assert_codacy_classifications(classify: Any) -> None:
    for payload, expected in _codacy_classification_cases():
        ASSERTIONS.assertEqual(classify(payload)["classification"], expected)


def _codacy_classification_cases() -> list[tuple[dict[str, Any], str]]:
    return [
        (_codacy_state_payload(api_issues=3), "real_current_issues"),
        (_codacy_state_payload(annotations=2), "api_github_mismatch"),
        (_codacy_state_payload(), "stale_github_check"),
        (_codacy_rule_conflict_payload(), "rule_conflict"),
    ]


def _codacy_rule_conflict_payload() -> dict[str, Any]:
    return _codacy_state_payload(
        api_issues=2,
        issues=[
            {"filePath": "a.py", "patternId": "D203", "symbol": "ClassA"},
            {"filePath": "a.py", "patternId": "D211", "symbol": "ClassA"},
        ],
    )


def test_codacy_rule_conflict_symbol_less_different_lines_is_conflict():
    """D203/D211 in API issues must be treated as a rule conflict."""
    result = controller.classify_codacy_evidence(
        _codacy_state_payload(
            api_issues=2,
            issues=[
                {"filePath": "a.py", "patternId": "D203", "lineNumber": 10},
                {"filePath": "a.py", "patternId": "D211", "lineNumber": 20},
            ],
        )
    )
    ASSERTIONS.assertEqual(result["classification"], "rule_conflict")


def test_codacy_annotations_fallback_become_real_blockers():
    """Github Codacy annotations must be treated as blockers when API returns zero issues."""
    if not hasattr(controller, "classify_codacy_evidence"):
        raise NotImplementedError("classify_codacy_evidence not implemented")
    result = controller.classify_codacy_evidence(
        {
            "github_codacy_state": "ACTION_REQUIRED",
            "codacy_api_issues": 0,
            "github_annotations": 2,
            "issues": [],
        }
    )
    ASSERTIONS.assertTrue(result["treat_annotations_as_blockers"])
    ASSERTIONS.assertFalse(result.get("ignored", False))


def test_codacy_evidence_from_api_preserves_annotation_field_for_mismatch_path():
    """API evidence should retain github_annotations so mismatch classification stays reachable."""
    evidence = controller.codacy_evidence_from_api(
        [_check("Codacy Static Code Analysis", "ACTION_REQUIRED")],
        True,
        [],
        "ok",
        github_annotations=3,
    )
    classified = controller.classify_codacy_evidence(evidence)

    ASSERTIONS.assertEqual(evidence["github_annotations"], 3)
    ASSERTIONS.assertEqual(classified["classification"], "api_github_mismatch")


def test_codacy_annotation_fallback_api_issues_classifies_real_codacy_issue():
    summary = controller.summarize_codacy_github_annotation_state(
        _codacy_state_payload(api_issues=2, issues=[{"patternId": "X100", "message": "style"}])
    )
    ASSERTIONS.assertEqual(summary["category"], "codacy_style")
    ASSERTIONS.assertEqual(summary["next_action"], "fix_codacy_current_issues")
    ASSERTIONS.assertEqual(summary["api_issue_count"], 2)
    ASSERTIONS.assertTrue(summary["safe_to_patch"])


def test_codacy_annotation_fallback_api_zero_with_annotations_is_mismatch():
    summary = controller.summarize_codacy_github_annotation_state(
        pr_head_sha="head1",
        codacy_api_issues=[],
        codacy_check_run={"status": "completed", "conclusion": "action_required", "head_sha": "head1"},
        github_annotations=[{"path": "x.py", "message": "issue"}],
    )
    ASSERTIONS.assertEqual(summary["category"], "codacy_api_github_mismatch")
    ASSERTIONS.assertEqual(summary["next_action"], "fix_github_codacy_annotations")
    ASSERTIONS.assertEqual(summary["github_annotations_count"], 1)
    ASSERTIONS.assertTrue(summary["safe_to_patch"])


def test_codacy_annotation_fallback_action_required_without_signals_is_stale():
    summary = controller.summarize_codacy_github_annotation_state(_codacy_state_payload())
    ASSERTIONS.assertEqual(summary["category"], "github_stale_check")
    ASSERTIONS.assertEqual(summary["next_action"], "rerun_stale_checks")
    ASSERTIONS.assertTrue(summary["stale"])
    ASSERTIONS.assertFalse(summary["safe_to_patch"])


def test_codacy_annotation_fallback_head_mismatch_is_stale():
    summary = controller.summarize_codacy_github_annotation_state(
        _codacy_state_payload() | {"pr_head": "head-a", "codacy_head": "head-b"}
    )
    ASSERTIONS.assertEqual(summary["category"], "github_stale_check")
    ASSERTIONS.assertEqual(summary["next_action"], "rerun_stale_checks")
    ASSERTIONS.assertEqual(summary["current_head_sha"], "head-a")
    ASSERTIONS.assertEqual(summary["codacy_head_sha"], "head-b")


def test_codacy_annotation_fallback_rule_conflict_routes_manual_conflict():
    summary = controller.summarize_codacy_github_annotation_state(_codacy_rule_conflict_payload())
    ASSERTIONS.assertEqual(summary["category"], "codacy_rule_conflict")
    ASSERTIONS.assertEqual(summary["next_action"], "needs_manual_codacy_rule_conflict")
    ASSERTIONS.assertTrue(summary["rule_conflict"])
    ASSERTIONS.assertFalse(summary["safe_to_patch"])


def test_codacy_annotation_fallback_pending_check_waits():
    summary = controller.summarize_codacy_github_annotation_state(
        _codacy_state_payload(state="IN_PROGRESS", api_issues=0, annotations=0)
    )
    ASSERTIONS.assertEqual(summary["category"], "workflow_pending")
    ASSERTIONS.assertEqual(summary["next_action"], "wait_pending")
    ASSERTIONS.assertFalse(summary["safe_to_patch"])


def test_codacy_annotation_fallback_malformed_payload_fails_closed():
    summary = controller.summarize_codacy_github_annotation_state({"codacy_api_issues": "bad", "issues": "bad"})
    ASSERTIONS.assertEqual(summary["category"], "unknown")
    ASSERTIONS.assertEqual(summary["next_action"], "needs_manual")
    ASSERTIONS.assertEqual(summary["api_issue_count"], 0)
    ASSERTIONS.assertEqual(summary["github_annotations_count"], 0)


def test_codacy_annotation_fallback_classify_wrapper_contains_contract_fields():
    result = controller.classify_codacy_github_annotation_fallback(
        current_head_sha="head1",
        api_issues=[],
        check_run={"status": "completed", "conclusion": "action_required", "head_sha": "head1"},
        annotations=2,
    )
    ASSERTIONS.assertEqual(result["category"], "codacy_api_github_mismatch")
    ASSERTIONS.assertEqual(result["classification"], "codacy_api_github_mismatch")
    ASSERTIONS.assertEqual(result["next_action"], "fix_github_codacy_annotations")
    ASSERTIONS.assertEqual(result["current_head_sha"], "head1")
    ASSERTIONS.assertIn("reason", result)
    ASSERTIONS.assertIn("safe_to_patch", result)
    ASSERTIONS.assertIn("stale", result)
    ASSERTIONS.assertIn("rule_conflict", result)
    ASSERTIONS.assertEqual(result["api_issue_count"], 0)
    ASSERTIONS.assertEqual(result["github_annotations_count"], 2)


def test_codacy_annotation_fallback_head_mismatch_with_keyword_inputs_is_stale():
    result = controller.classify_codacy_github_annotation_fallback(
        pr_head_sha="pr-head",
        codacy_api_issues=[],
        codacy_check_run={"status": "completed", "conclusion": "action_required", "head_sha": "old-head"},
        github_annotations=[],
    )
    ASSERTIONS.assertEqual(result["classification"], "github_stale_check")
    ASSERTIONS.assertEqual(result["next_action"], "rerun_stale_checks")
    ASSERTIONS.assertTrue(result["stale"])
    ASSERTIONS.assertFalse(result["safe_to_patch"])


def test_codacy_annotation_fallback_top_level_headref_mismatch_is_stale():
    summary = controller.summarize_codacy_github_annotation_state(
        pr_head_sha="head-current",
        headRefOid="head-old",
        codacy_api_issues=[],
        github_annotations=[{"path": "x.py", "message": "issue"}],
        codacy_check_run={"status": "completed", "conclusion": "action_required"},
    )
    ASSERTIONS.assertEqual(summary["category"], "github_stale_check")
    ASSERTIONS.assertEqual(summary["next_action"], "rerun_stale_checks")
    ASSERTIONS.assertTrue(summary["stale"])
    ASSERTIONS.assertFalse(summary["safe_to_patch"])
    ASSERTIONS.assertEqual(summary["current_head_sha"], "head-current")
    ASSERTIONS.assertEqual(summary["codacy_head_sha"], "head-old")


def test_codacy_annotation_fallback_top_level_headref_match_is_mismatch():
    summary = controller.summarize_codacy_github_annotation_state(
        pr_head_sha="head-current",
        headRefOid="head-current",
        codacy_api_issues=[],
        github_annotations=[{"path": "x.py", "message": "issue"}],
        codacy_check_run={"status": "completed", "conclusion": "action_required"},
    )
    ASSERTIONS.assertEqual(summary["category"], "codacy_api_github_mismatch")
    ASSERTIONS.assertEqual(summary["next_action"], "fix_github_codacy_annotations")
    ASSERTIONS.assertFalse(summary["stale"])
    ASSERTIONS.assertTrue(summary["safe_to_patch"])


def test_codacy_annotation_fallback_check_run_head_takes_priority_over_top_level_head():
    summary = controller.summarize_codacy_github_annotation_state(
        pr_head_sha="head-current",
        headRefOid="head-current",
        codacy_api_issues=[],
        github_annotations=[{"path": "x.py", "message": "issue"}],
        codacy_check_run={"status": "completed", "conclusion": "action_required", "head_sha": "head-old"},
    )
    ASSERTIONS.assertEqual(summary["category"], "github_stale_check")
    ASSERTIONS.assertEqual(summary["next_action"], "rerun_stale_checks")
    ASSERTIONS.assertTrue(summary["stale"])
    ASSERTIONS.assertFalse(summary["safe_to_patch"])
    ASSERTIONS.assertEqual(summary["current_head_sha"], "head-current")
    ASSERTIONS.assertEqual(summary["codacy_head_sha"], "head-old")


def test_codacy_annotation_fallback_check_run_head_match_is_mismatch():
    summary = controller.summarize_codacy_github_annotation_state(
        pr_head_sha="head-current",
        headRefOid="head-current",
        codacy_api_issues=[],
        github_annotations=[{"path": "x.py", "message": "issue"}],
        codacy_check_run={"status": "completed", "conclusion": "action_required", "head_sha": "head-current"},
    )
    ASSERTIONS.assertEqual(summary["category"], "codacy_api_github_mismatch")
    ASSERTIONS.assertEqual(summary["next_action"], "fix_github_codacy_annotations")
    ASSERTIONS.assertFalse(summary["stale"])
    ASSERTIONS.assertTrue(summary["safe_to_patch"])


def test_codacy_annotation_fallback_checks_head_mismatch_is_stale():
    summary = controller.summarize_codacy_github_annotation_state(
        pr_head_sha="head-current",
        codacy_api_issues=[],
        github_annotations=[{"path": "x.py", "message": "issue"}],
        codacy_check_run={"status": "completed", "conclusion": "action_required"},
        checks=[
            {
                "name": "Codacy Static Code Analysis",
                "status": "completed",
                "conclusion": "action_required",
                "head_sha": "head-old",
                "details_url": "https://app.codacy.com/gh/org/repo/pull-requests/1",
            }
        ],
    )
    ASSERTIONS.assertEqual(summary["category"], "github_stale_check")
    ASSERTIONS.assertEqual(summary["next_action"], "rerun_stale_checks")
    ASSERTIONS.assertTrue(summary["stale"])
    ASSERTIONS.assertFalse(summary["safe_to_patch"])
    ASSERTIONS.assertEqual(summary["codacy_head_sha"], "head-old")


def test_codacy_annotation_fallback_checks_head_match_is_mismatch():
    summary = controller.summarize_codacy_github_annotation_state(
        pr_head_sha="head-current",
        codacy_api_issues=[],
        github_annotations=[{"path": "x.py", "message": "issue"}],
        codacy_check_run={"status": "completed", "conclusion": "action_required"},
        checks=[
            {
                "check_name": "Codacy",
                "app_name": "Codacy",
                "headSha": "head-current",
                "url": "https://app.codacy.com/gh/org/repo/pull-requests/1",
            }
        ],
    )
    ASSERTIONS.assertEqual(summary["category"], "codacy_api_github_mismatch")
    ASSERTIONS.assertEqual(summary["next_action"], "fix_github_codacy_annotations")
    ASSERTIONS.assertFalse(summary["stale"])
    ASSERTIONS.assertTrue(summary["safe_to_patch"])
    ASSERTIONS.assertEqual(summary["codacy_head_sha"], "head-current")


def test_codacy_annotation_fallback_rule_conflict_detects_alias_fields():
    result = controller.classify_codacy_github_annotation_fallback(
        pr_head_sha="head1",
        codacy_api_issues=[
            {"title": "violation: D203 and D211 conflict", "message": "manual rule conflict"},
        ],
        codacy_check_run={"status": "completed", "conclusion": "action_required", "head_sha": "head1"},
        github_annotations=[],
    )
    ASSERTIONS.assertEqual(result["classification"], "codacy_rule_conflict")
    ASSERTIONS.assertEqual(result["next_action"], "needs_manual_codacy_rule_conflict")
    ASSERTIONS.assertTrue(result["rule_conflict"])
    ASSERTIONS.assertFalse(result["safe_to_patch"])


def test_codacy_annotation_fallback_rule_conflict_priority_over_api_with_pattern_id():
    result = controller.classify_codacy_github_annotation_fallback(
        pr_head_sha="head1",
        codacy_api_issues=[
            {"pattern_id": "D203", "message": "one"},
            {"pattern_id": "D211", "message": "two"},
        ],
        codacy_check_run={"status": "completed", "conclusion": "action_required", "head_sha": "head1"},
        github_annotations=0,
    )
    ASSERTIONS.assertEqual(result["classification"], "codacy_rule_conflict")
    ASSERTIONS.assertEqual(result["next_action"], "needs_manual_codacy_rule_conflict")
    ASSERTIONS.assertTrue(result["rule_conflict"])
    ASSERTIONS.assertFalse(result["safe_to_patch"])


def test_codacy_annotation_fallback_rule_conflict_from_github_annotation_text():
    result = controller.classify_codacy_github_annotation_fallback(
        pr_head_sha="head1",
        codacy_api_issues=[{"patternId": "W001", "message": "generic codacy issue"}],
        codacy_check_run={"status": "completed", "conclusion": "action_required", "head_sha": "head1"},
        github_annotations=[{"check_name": "flake8 D203", "title": "doc spacing", "message": "requires D211"}],
    )
    ASSERTIONS.assertEqual(result["classification"], "codacy_rule_conflict")
    ASSERTIONS.assertEqual(result["next_action"], "needs_manual_codacy_rule_conflict")
    ASSERTIONS.assertTrue(result["rule_conflict"])
    ASSERTIONS.assertFalse(result["safe_to_patch"])


def test_codacy_rule_conflict_aggregates_markers_from_separate_api_issues():
    result = controller.classify_codacy_github_annotation_fallback(
        pr_head_sha="head1",
        codacy_api_issues=[
            {"patternId": "D203", "message": "blank line rule"},
            {"patternId": "D211", "message": "no blank line rule"},
        ],
        codacy_check_run={"status": "completed", "conclusion": "action_required", "head_sha": "head1"},
    )
    ASSERTIONS.assertEqual(result["classification"], "codacy_rule_conflict")
    ASSERTIONS.assertEqual(result["next_action"], "needs_manual_codacy_rule_conflict")


def test_codacy_rule_conflict_detected_with_extra_markers_present():
    result = controller.classify_codacy_github_annotation_fallback(
        pr_head_sha="head1",
        codacy_api_issues=[{"message": "D203 D211 W291", "patternId": "W291"}],
        codacy_check_run={"status": "completed", "conclusion": "action_required", "head_sha": "head1"},
    )
    ASSERTIONS.assertEqual(result["classification"], "codacy_rule_conflict")
    ASSERTIONS.assertEqual(result["next_action"], "needs_manual_codacy_rule_conflict")


def test_codacy_rule_conflict_aggregates_api_and_annotation_markers():
    result = controller.classify_codacy_github_annotation_fallback(
        pr_head_sha="head1",
        codacy_api_issues=[{"patternId": "D203", "message": "rule from API"}],
        codacy_check_run={"status": "completed", "conclusion": "action_required", "head_sha": "head1"},
        github_annotations=[{"check_name": "ruff", "title": "doc", "message": "requires D211"}],
    )
    ASSERTIONS.assertEqual(result["classification"], "codacy_rule_conflict")
    ASSERTIONS.assertEqual(result["next_action"], "needs_manual_codacy_rule_conflict")


def test_codacy_dict_form_github_annotation_counts_as_one():
    summary = controller.summarize_codacy_github_annotation_state(
        pr_head_sha="head1",
        codacy_api_issues=[],
        codacy_check_run={"status": "completed", "conclusion": "action_required", "head_sha": "head1"},
        github_annotations={"path": "x.py", "message": "issue"},
    )
    ASSERTIONS.assertEqual(summary["github_annotations_count"], 1)
    ASSERTIONS.assertEqual(summary["category"], "codacy_api_github_mismatch")


def test_codacy_check_run_annotations_count_fallback_without_annotation_payload():
    summary = controller.summarize_codacy_github_annotation_state(
        pr_head_sha="head1",
        codacy_api_issues=[],
        codacy_check_run={
            "status": "completed",
            "conclusion": "action_required",
            "head_sha": "head1",
            "annotations_count": 3,
        },
    )
    ASSERTIONS.assertEqual(summary["github_annotations_count"], 3)
    ASSERTIONS.assertEqual(summary["category"], "codacy_api_github_mismatch")


def test_codacy_check_run_annotations_count_camel_case_fallback_without_annotation_payload():
    summary = controller.summarize_codacy_github_annotation_state(
        pr_head_sha="head1",
        codacy_api_issues=[],
        codacy_check_run={
            "status": "completed",
            "conclusion": "action_required",
            "head_sha": "head1",
            "annotationsCount": 2,
        },
    )
    ASSERTIONS.assertEqual(summary["github_annotations_count"], 2)
    ASSERTIONS.assertEqual(summary["category"], "codacy_api_github_mismatch")


def test_codacy_api_zero_with_scalar_zero_and_annotations_list_routes_to_mismatch():
    summary = controller.summarize_codacy_github_annotation_state(
        pr_head_sha="head1",
        codacy_api_issues=[],
        codacy_check_run={"status": "completed", "conclusion": "action_required", "head_sha": "head1"},
        github_annotations=0,
        annotations=[{"path": "x.py", "message": "issue"}],
    )
    ASSERTIONS.assertEqual(summary["github_annotations_count"], 1)
    ASSERTIONS.assertEqual(summary["category"], "codacy_api_github_mismatch")


def test_codacy_api_zero_with_malformed_github_annotations_and_list_uses_list_fallback():
    summary = controller.summarize_codacy_github_annotation_state(
        pr_head_sha="head1",
        codacy_api_issues=[],
        codacy_check_run={"status": "completed", "conclusion": "action_required", "head_sha": "head1"},
        github_annotations="n/a",
        annotations=[{"path": "x.py", "message": "issue"}],
    )
    ASSERTIONS.assertEqual(summary["github_annotations_count"], 1)
    ASSERTIONS.assertEqual(summary["category"], "codacy_api_github_mismatch")


def test_codacy_malformed_github_annotations_with_d203_d211_annotations_detects_conflict():
    result = controller.classify_codacy_github_annotation_fallback(
        pr_head_sha="head1",
        codacy_api_issues=[],
        codacy_check_run={"status": "completed", "conclusion": "action_required", "head_sha": "head1"},
        github_annotations="n/a",
        annotations=[{"check_name": "flake8 D203", "title": "doc", "message": "requires D211"}],
    )
    ASSERTIONS.assertEqual(result["classification"], "codacy_rule_conflict")
    ASSERTIONS.assertEqual(result["next_action"], "needs_manual_codacy_rule_conflict")


def test_codacy_annotation_items_preferred_over_numeric_count_for_conflict_parsing():
    result = controller.classify_codacy_github_annotation_fallback(
        pr_head_sha="head1",
        codacy_api_issues=[{"patternId": "D203", "message": "rule from API"}],
        codacy_check_run={"status": "completed", "conclusion": "action_required", "head_sha": "head1"},
        github_annotations=2,
        annotations=[{"check_name": "flake8", "title": "doc style", "message": "requires D211"}],
    )
    ASSERTIONS.assertEqual(result["classification"], "codacy_rule_conflict")
    ASSERTIONS.assertEqual(result["next_action"], "needs_manual_codacy_rule_conflict")


def test_codacy_annotation_payload_preserves_detailed_annotation_items():
    payload = controller._codacy_annotation_payload(
        None,
        {
            "pr_head_sha": "head1",
            "codacy_api_issues": [],
            "codacy_check_run": {"status": "completed", "conclusion": "action_required", "head_sha": "head1"},
            "github_annotations": 0,
            "annotations": [{"path": "x.py", "message": "D203 and D211"}],
        },
    )
    ASSERTIONS.assertEqual(payload["github_annotations"], 1)
    ASSERTIONS.assertEqual(payload["github_annotation_items"], [{"path": "x.py", "message": "D203 and D211"}])


def test_codacy_api_zero_with_dict_form_annotation_is_mismatch():
    summary = controller.summarize_codacy_github_annotation_state(
        pr_head_sha="head1",
        codacy_api_issues=[],
        codacy_check_run={"status": "completed", "conclusion": "action_required", "head_sha": "head1"},
        annotations={"path": "x.py", "message": "issue"},
    )
    ASSERTIONS.assertEqual(summary["category"], "codacy_api_github_mismatch")
    ASSERTIONS.assertEqual(summary["next_action"], "fix_github_codacy_annotations")
    ASSERTIONS.assertEqual(summary["github_annotations_count"], 1)


def test_codacy_malformed_payload_stays_unknown_needs_manual():
    summary = controller.summarize_codacy_github_annotation_state(
        {"codacy_api_issues": "bad", "issues": "bad", "github_annotations": "not-a-number"}
    )
    ASSERTIONS.assertEqual(summary["category"], "unknown")
    ASSERTIONS.assertEqual(summary["next_action"], "needs_manual")


def _codacy_head_preservation_pr() -> dict[str, object]:
    return {
        "statusCheckRollup": [_check("Codacy Static Code Analysis", "ACTION_REQUIRED")],
        "headRefOid": "pr-head-sha",
        "mergeable": "MERGEABLE",
        "mergeStateStatus": "CLEAN",
    }


def _codacy_head_preservation_evidence() -> dict[str, Any]:
    return {
        "checks": [_check("Codacy Static Code Analysis", "ACTION_REQUIRED")],
        "check_blocking": True,
        "github_codacy_state": "ACTION_REQUIRED",
        "github_annotations": 0,
        "codacy_api_issues": 1,
        "issues": [{"filePath": "a.py", "patternId": "X"}],
        "api_available": True,
        "api_ok": True,
        "issues_returned": 1,
    }


def _codacy_head_preservation_evidence_state() -> dict[str, Any]:
    return {
        "blocking": True,
        "ignored": False,
        "reason": "test",
        "headRefOid": "codacy-head-sha",
    }


def _stub_codacy_evidence_for_checks(monkeypatch) -> None:
    monkeypatch.setattr(
        controller,
        "codacy_evidence_for_checks",
        lambda *_args, **_kwargs: {
            **_codacy_head_preservation_evidence(),
            **_codacy_head_preservation_evidence_state(),
        },
    )


def _stub_codacy_head_preservation(monkeypatch) -> None:
    _stub_codacy_evidence_for_checks(monkeypatch)
    monkeypatch.setattr(controller, "_review_threads_raw", lambda *_args: {})


def _assert_codacy_head_preserved(decision: dict[str, object]) -> None:
    codacy = cast(dict[str, Any], decision["codacy"])
    ASSERTIONS.assertEqual(codacy["pr_head"], "pr-head-sha")
    ASSERTIONS.assertEqual(codacy["headRefOid"], "codacy-head-sha")


def test_build_next_action_context_stores_pr_head_without_overwriting_codacy_head(monkeypatch):
    """Context should keep PR head in pr_head and preserve Codacy evidence head separately."""
    _stub_codacy_head_preservation(monkeypatch)
    decision: dict[str, object] = {"actions": [], "warnings": [], "errors": []}
    ctx = controller.build_next_action_context(
        _args(), decision, _codacy_head_preservation_pr(), ([], [])
    )
    _assert_codacy_head_preserved(ctx.decision)


def _assert_blocker_categories(cases: list[tuple[dict[str, object], str]]) -> None:
    for payload, expected in cases:
        ASSERTIONS.assertEqual(controller.classify_blocker(payload)["category"], expected)


def _codacy_blocker_cases() -> list[tuple[dict[str, object], str]]:
    return [
        ({"name": "Codacy Static Code Analysis", "state": "FAILURE", "source": "codacy"}, "codacy_style"),
        (
            {"name": "Codacy complexity", "state": "FAILURE", "source": "codacy", "reason": "C901 complexity"},
            "codacy_complexity",
        ),
        (
            {"name": "Codacy", "state": "ACTION_REQUIRED", "source": "codacy", "reason": "D203 and D211 conflict"},
            "codacy_rule_conflict",
        ),
        (
            {
                "name": "Codacy",
                "state": "ACTION_REQUIRED",
                "source": "codacy",
                "classification": "api_github_mismatch",
            },
            "codacy_api_github_mismatch",
        ),
        ({"name": "Codacy", "state": "STALE", "source": "codacy"}, "github_stale_check"),
    ]


def _manual_and_workflow_cases() -> list[tuple[dict[str, object], str]]:
    return _manual_signal_cases() + _workflow_state_cases()


def _manual_signal_cases() -> list[tuple[dict[str, object], str]]:
    return [
        (
            {"name": "Review thread", "state": "ACTION_REQUIRED", "source": "review", "active": True},
            "review_comment_active",
        ),
        ({"name": "Unit tests", "state": "FAILURE", "source": "check"}, "test_failure"),
        (
            {"name": "Infra", "state": "FAILURE", "source": "check", "reason": "runner service unavailable"},
            "infra_failure",
        ),
        ({"name": "Auth", "state": "FAILURE", "reason": "token missing"}, "token_missing"),
        ({"name": "Auth", "state": "FAILURE", "reason": "403 permission denied"}, "api_permission_error"),
    ]


def _workflow_state_cases() -> list[tuple[dict[str, object], str]]:
    return [
        ({"name": "Flow", "state": "CANCELLED"}, "workflow_cancelled"),
        ({"name": "Flow", "state": "IN_PROGRESS"}, "workflow_pending"),
        ({"name": "Scope", "state": "FAILURE", "reason": "scope_violation detected"}, "scope_violation"),
        ({"name": "Merge", "mergeStateStatus": "DIRTY"}, "merge_conflict"),
        ({}, "unknown"),
    ]


def test_blocker_taxonomy_classifies_codacy_categories():
    """Blocker taxonomy maps Codacy inputs to expected categories."""
    _assert_blocker_categories(_codacy_blocker_cases())


def test_blocker_taxonomy_classifies_manual_categories():
    """Blocker taxonomy maps manual/test inputs to expected categories."""
    _assert_blocker_categories(_manual_and_workflow_cases()[:5])


def test_blocker_taxonomy_classifies_workflow_and_unknown_categories():
    """Blocker taxonomy maps workflow/merge/unknown inputs to expected categories."""
    _assert_blocker_categories(_manual_and_workflow_cases()[5:])


def test_route_blocker_action_maps_required_next_actions():
    """Every required taxonomy route maps to the expected NEXT_ACTION."""
    ASSERTIONS.assertEqual(controller.route_blocker_action("workflow_pending", {}), "wait_pending")
    ASSERTIONS.assertEqual(controller.route_blocker_action("workflow_cancelled", {}), "rerun_stale_checks")
    ASSERTIONS.assertEqual(controller.route_blocker_action("github_stale_check", {}), "rerun_stale_checks")
    ASSERTIONS.assertEqual(
        controller.route_blocker_action("codacy_api_github_mismatch", {}),
        "fix_github_codacy_annotations",
    )
    ASSERTIONS.assertEqual(
        controller.route_blocker_action("codacy_rule_conflict", {}),
        "needs_manual_codacy_rule_conflict",
    )
    ASSERTIONS.assertEqual(controller.route_blocker_action("token_missing", {}), "needs_manual_secret")
    ASSERTIONS.assertEqual(controller.route_blocker_action("api_permission_error", {}), "needs_manual_secret")
    ASSERTIONS.assertEqual(controller.route_blocker_action("scope_violation", {}), "needs_manual_scope_violation")
    ASSERTIONS.assertEqual(controller.route_blocker_action("merge_conflict", {}), "needs_manual_merge_conflict")
    ASSERTIONS.assertEqual(controller.route_blocker_action("unknown", {}), "needs_manual")


def test_summarize_blocker_actions_empty_is_non_blocking():
    """Empty blocker list is non-blocking and should not route to manual unknown action."""
    clean = controller.summarize_blocker_actions([], {"can_merge": False})
    mergeable = controller.summarize_blocker_actions([], {"can_merge": True})

    ASSERTIONS.assertEqual(clean["primary_category"], "none")
    ASSERTIONS.assertEqual(clean["next_action"], "checks_green_or_no_action")
    ASSERTIONS.assertNotEqual(clean["primary_category"], "unknown")
    ASSERTIONS.assertFalse(clean["needs_manual"])
    ASSERTIONS.assertEqual(clean["safe_actions"], [])
    ASSERTIONS.assertEqual(clean["reasons"], [])
    ASSERTIONS.assertEqual(mergeable["primary_category"], "none")
    ASSERTIONS.assertEqual(mergeable["next_action"], "ready_to_merge")
    ASSERTIONS.assertNotEqual(mergeable["primary_category"], "unknown")
    ASSERTIONS.assertFalse(mergeable["needs_manual"])
    ASSERTIONS.assertEqual(mergeable["safe_actions"], [])
    ASSERTIONS.assertEqual(mergeable["reasons"], [])


def test_blocker_taxonomy_classifies_token_unavailable_as_manual_secret():
    """Token unavailable wording should be classified as token_missing/manual secret path."""
    payload = {"name": "Auth", "state": "FAILURE", "reason": "codacy api token is unavailable"}
    category = controller.classify_blocker(payload)["category"]
    ASSERTIONS.assertEqual(category, "token_missing")
    ASSERTIONS.assertEqual(controller.route_blocker_action(category, {}), "needs_manual_secret")


def test_summarize_blocker_actions_empty_ready_context_from_can_merge():
    """Empty blocker list uses explicit can_merge as ready_to_merge signal."""
    summary = controller.summarize_blocker_actions(
        [],
        {"can_merge": True, "mergeable": "MERGEABLE", "mergeStateStatus": "CLEAN", "bad": [], "pending": []},
    )

    ASSERTIONS.assertEqual(summary["primary_category"], "none")
    ASSERTIONS.assertEqual(summary["next_action"], "ready_to_merge")
    ASSERTIONS.assertFalse(summary["needs_manual"])


def test_summarize_blocker_actions_preserves_explicit_merge_conflict_next_action():
    """Explicit blocker next_action should override category default routing."""
    summary = controller.summarize_blocker_actions(
        [
            {
                "name": "PR merge conflict",
                "state": "FAILURE",
                "source": "merge",
                "mergeStateStatus": "DIRTY",
                "next_action": "needs_manual_merge_conflict",
                "reason": "merge conflict requires manual resolution",
            }
        ],
        {},
    )

    ASSERTIONS.assertEqual(summary["primary_category"], "merge_conflict")
    ASSERTIONS.assertEqual(summary["next_action"], "needs_manual_merge_conflict")


def test_scope_paths_skips_empty_list_and_continues():
    """Scope parsing should continue scanning keys after an empty list value."""
    scope_paths = controller._scope_paths(  # pylint: disable=protected-access
        {"files": [], "allowlist": ["scripts/x.py"]}
    )

    ASSERTIONS.assertEqual(scope_paths, {"scripts/x.py"})


def test_normalize_task_file_list_ignores_placeholders_and_normalizes_paths():
    values = ["./scripts//pr_automation_controller.py", "none", "N/A", "../escape.py", "tests/../tests/scripts/a.py"]
    normalized = controller.normalize_task_file_list(values)
    ASSERTIONS.assertEqual(normalized, ["scripts/pr_automation_controller.py"])


def test_normalize_task_file_list_rejects_invalid_raw_inputs_before_normalization():
    values = [
        "/repo/scripts/tool.py",
        "C:\\repo\\scripts\\tool.py",
        "D:/repo/scripts/tool.py",
        "../scripts/tool.py",
        "scripts/ x.py",
        "scripts /x.py",
        " scripts/x.py",
        "scripts/x.py ",
        "scripts/pr_automation_controller.py",
    ]
    normalized = controller.normalize_task_file_list(values)
    ASSERTIONS.assertEqual(normalized, ["scripts/pr_automation_controller.py"])


def test_normalize_file_scope_rules_preserves_directory_semantics():
    normalized = controller.normalize_file_scope_rules(["scripts/"])
    ASSERTIONS.assertEqual(normalized, ["scripts/"])


def test_path_matches_scope_rule_supports_exact_dir_and_wildcard():
    ASSERTIONS.assertTrue(
        controller.path_matches_scope_rule("scripts/pr_automation_controller.py", "scripts/pr_automation_controller.py")
    )
    ASSERTIONS.assertTrue(controller.path_matches_scope_rule("scripts/x.py", "scripts/"))
    ASSERTIONS.assertTrue(controller.path_matches_scope_rule(".github/workflows/x.yml", ".github/workflows/*"))
    ASSERTIONS.assertTrue(controller.path_matches_scope_rule("scripts/sub/x.py", "scripts/*"))
    ASSERTIONS.assertFalse(controller.path_matches_scope_rule("scripts2/x.py", "scripts/*"))
    ASSERTIONS.assertFalse(controller.path_matches_scope_rule("scripts_alternate/x.py", "scripts/*"))
    ASSERTIONS.assertFalse(controller.path_matches_scope_rule("scripts2/x.py", "scripts/"))
    ASSERTIONS.assertFalse(controller.path_matches_scope_rule("scripts_alternate/x.py", "scripts/"))
    ASSERTIONS.assertFalse(controller.path_matches_scope_rule("docs", "docs/"))
    ASSERTIONS.assertTrue(controller.path_matches_scope_rule("docs/x.md", "docs/"))
    ASSERTIONS.assertTrue(controller.path_matches_scope_rule("docs/sub/x.md", "docs/"))
    ASSERTIONS.assertFalse(controller.path_matches_scope_rule("docs2/x.md", "docs/"))
    ASSERTIONS.assertFalse(controller.path_matches_scope_rule("docs_alternate/x.md", "docs/"))
    ASSERTIONS.assertTrue(controller.path_matches_scope_rule("secrets/server.key", "*.key"))
    ASSERTIONS.assertTrue(controller.path_matches_scope_rule("config/private.key", "*.key"))
    ASSERTIONS.assertTrue(controller.path_matches_scope_rule("server.pem", "*.pem"))
    ASSERTIONS.assertTrue(controller.path_matches_scope_rule("secrets/server.pem", "*.pem"))
    ASSERTIONS.assertFalse(controller.path_matches_scope_rule("scripts/x.py", "tests/"))


def test_path_matches_scope_rule_supports_file_prefix_trailing_star():
    ASSERTIONS.assertTrue(
        controller.path_matches_scope_rule("scripts/pr_automation_controller.py", "scripts/pr_*")
    )
    ASSERTIONS.assertFalse(controller.path_matches_scope_rule("scripts/pr_private/secret.py", "scripts/pr_*"))
    ASSERTIONS.assertTrue(controller.path_matches_scope_rule("tests/test_example.py", "tests/test_*"))
    ASSERTIONS.assertFalse(controller.path_matches_scope_rule("tests/test_private/secret.py", "tests/test_*"))
    ASSERTIONS.assertFalse(controller.path_matches_scope_rule("scripts/x.py", "scripts/pr_*"))


def test_scope_allows_file_change_allowed_file_passes():
    ASSERTIONS.assertTrue(
        controller.scope_allows_file_change(
            "scripts/pr_automation_controller.py",
            ["scripts/pr_automation_controller.py"],
            [],
        )
    )


def test_scope_allows_file_change_forbidden_wins_and_outside_allowlist_denies():
    ASSERTIONS.assertFalse(
        controller.scope_allows_file_change(
            "scripts/pr_flow_automation.py",
            ["scripts/"],
            ["scripts/pr_flow_automation.py"],
        )
    )
    ASSERTIONS.assertFalse(
        controller.scope_allows_file_change(
            "tests/scripts/test_pr_automation_controller.py",
            ["scripts/pr_automation_controller.py"],
            [],
        )
    )
    ASSERTIONS.assertFalse(controller.scope_allows_file_change("docs", ["docs/"], []))
    ASSERTIONS.assertTrue(controller.scope_allows_file_change("docs/x.md", ["docs/"], []))


def test_scope_allows_file_change_default_forbidden_patterns_block():
    ASSERTIONS.assertFalse(
        controller.scope_allows_file_change(
            ".github/workflows/pr-guard.yml",
            [".github/workflows/pr-guard.yml"],
            [],
        )
    )
    ASSERTIONS.assertFalse(
        controller.scope_allows_file_change(
            "business/core/runtime/engine.py",
            ["business/core/runtime/engine.py"],
            [],
        )
    )
    ASSERTIONS.assertFalse(
        controller.scope_allows_file_change(
            "config/providers/openai.yaml",
            ["config/providers/openai.yaml"],
            [],
        )
    )
    ASSERTIONS.assertFalse(
        controller.scope_allows_file_change(
            "secrets/server.key",
            ["secrets/server.key"],
            [],
        )
    )
    ASSERTIONS.assertFalse(
        controller.scope_allows_file_change(
            "config/private.key",
            ["config/private.key"],
            [],
        )
    )
    ASSERTIONS.assertFalse(
        controller.scope_allows_file_change(
            "server.pem",
            ["server.pem"],
            [],
        )
    )


def test_scope_allows_file_change_missing_allowlist_denies_by_default():
    ASSERTIONS.assertFalse(controller.scope_allows_file_change("scripts/pr_automation_controller.py", [], []))
    ASSERTIONS.assertFalse(controller.scope_allows_file_change("scripts/pr_automation_controller.py", None, []))


def test_scope_allows_file_change_malformed_forbidden_dict_fails_closed():
    ASSERTIONS.assertFalse(
        controller.scope_allows_file_change("scripts/secret.py", ["scripts/"], {"files": ["scripts/secret.py"]})
    )


def test_scope_allows_file_change_invalid_forbidden_entry_fails_closed():
    ASSERTIONS.assertFalse(
        controller.scope_allows_file_change("scripts/secret.py", ["scripts/"], ["scripts/ secret.py"])
    )


def test_scope_allows_file_change_malformed_allowed_dict_fails_closed():
    ASSERTIONS.assertFalse(
        controller.scope_allows_file_change("scripts/x.py", {"files": ["scripts/x.py"]}, [])
    )


def test_scope_allows_file_change_invalid_allowed_entry_fails_closed():
    ASSERTIONS.assertFalse(
        controller.scope_allows_file_change("scripts/x.py", ["scripts/ x.py"], [])
    )


def test_scope_allows_file_change_absolute_path_fails_closed_even_if_normalizable():
    ASSERTIONS.assertFalse(controller.scope_allows_file_change("/scripts/x.py", ["scripts/"], []))


def test_scope_allows_file_change_whitespace_mutated_paths_fail_closed():
    ASSERTIONS.assertFalse(controller.scope_allows_file_change("scripts /x.py", ["scripts/x.py"], []))
    ASSERTIONS.assertFalse(controller.scope_allows_file_change("scripts/ x.py", ["scripts/x.py"], []))
    ASSERTIONS.assertFalse(controller.scope_allows_file_change(" scripts/x.py", ["scripts/x.py"], []))
    ASSERTIONS.assertFalse(controller.scope_allows_file_change("scripts/x.py ", ["scripts/x.py"], []))


def test_scope_allows_file_change_windows_absolute_path_fails_closed():
    ASSERTIONS.assertFalse(controller.scope_allows_file_change("C:\\repo\\scripts\\tool.py", ["scripts/"], []))
    ASSERTIONS.assertFalse(controller.scope_allows_file_change("D:/repo/scripts/tool.py", ["scripts/"], []))


def test_scope_allows_file_change_literal_backslash_path_fails_closed():
    ASSERTIONS.assertFalse(controller.scope_allows_file_change("scripts\\tool.py", ["scripts/"], []))


def _invalid_changed_paths() -> list[str]:
    return [
        "../secrets/server.key",
        "../../README.md",
        "/absolute/path",
        "C:\\repo\\scripts\\tool.py",
        "D:/repo/scripts/tool.py",
        "scripts /x.py",
        "scripts/ x.py",
        "scripts/\tpr_tool.py",
        "scripts/\u00a0pr_tool.py",
        "scripts\t/x.py",
        " scripts/x.py",
        "scripts/x.py ",
        "",
        "none",
        "n/a",
        "tbd",
    ]


def test_scope_allows_file_change_invalid_changed_file_matrix_fails_closed():
    for path in _invalid_changed_paths():
        ASSERTIONS.assertFalse(controller.scope_allows_file_change(path, ["scripts/"], []))


def test_classify_changed_file_scope_absolute_path_is_invalid_path():
    result = controller.classify_changed_file_scope("/scripts/x.py", ["scripts/"], [])
    ASSERTIONS.assertEqual(result["classification"], "invalid_path")
    ASSERTIONS.assertEqual(result["path"], "/scripts/x.py")


def test_classify_changed_file_scope_whitespace_mutated_paths_are_invalid_path():
    for path in ("scripts /x.py", "scripts/ x.py", " scripts/x.py", "scripts/x.py "):
        result = controller.classify_changed_file_scope(path, ["scripts/x.py"], [])
        ASSERTIONS.assertEqual(result["classification"], "invalid_path")


def test_classify_changed_file_scope_windows_absolute_path_is_invalid_path():
    for path in ("C:\\repo\\scripts\\tool.py", "D:/repo/scripts/tool.py"):
        result = controller.classify_changed_file_scope(path, ["scripts/"], [])
        ASSERTIONS.assertEqual(result["classification"], "invalid_path")


def test_classify_changed_file_scope_literal_backslash_path_is_invalid_path():
    result = controller.classify_changed_file_scope("scripts\\tool.py", ["scripts/"], [])
    ASSERTIONS.assertEqual(result["classification"], "invalid_path")
    ASSERTIONS.assertEqual(result["path"], "scripts\\tool.py")


def test_classify_changed_file_scope_malformed_forbidden_dict_is_forbidden():
    result = controller.classify_changed_file_scope("scripts/secret.py", ["scripts/"], {"files": ["scripts/secret.py"]})
    ASSERTIONS.assertEqual(result["classification"], "forbidden")


def test_classify_changed_file_scope_invalid_forbidden_entry_is_forbidden():
    result = controller.classify_changed_file_scope("scripts/secret.py", ["scripts/"], ["scripts/ secret.py"])
    ASSERTIONS.assertEqual(result["classification"], "forbidden")


def test_classify_changed_file_scope_malformed_allowed_dict_is_missing_allowlist():
    result = controller.classify_changed_file_scope("scripts/x.py", {"files": ["scripts/x.py"]}, [])
    ASSERTIONS.assertEqual(result["classification"], "missing_allowlist")


def test_classify_changed_file_scope_invalid_allowed_entry_is_missing_allowlist():
    result = controller.classify_changed_file_scope("scripts/x.py", ["scripts/ x.py"], [])
    ASSERTIONS.assertEqual(result["classification"], "missing_allowlist")


def test_audit_changed_files_against_scope_reports_offending_and_blocks_batch():
    audit = controller.audit_changed_files_against_scope(
        ["scripts/pr_automation_controller.py", "scripts/pr_flow_automation.py"],
        ["scripts/pr_automation_controller.py", "tests/scripts/test_pr_automation_controller.py"],
        ["scripts/pr_flow_automation.py"],
    )
    ASSERTIONS.assertFalse(audit["allowed"])
    ASSERTIONS.assertEqual(audit["offending_files"], ["scripts/pr_flow_automation.py"])


def test_audit_changed_files_against_scope_allows_directory_rule_match():
    audit = controller.audit_changed_files_against_scope(["scripts/x.py"], ["scripts/"], [])
    ASSERTIONS.assertTrue(audit["allowed"])
    ASSERTIONS.assertEqual(audit["offending_files"], [])


def test_audit_changed_files_against_scope_denies_nonmatching_directory_rule():
    audit = controller.audit_changed_files_against_scope(["scripts2/x.py"], ["scripts/"], [])
    ASSERTIONS.assertFalse(audit["allowed"])
    ASSERTIONS.assertEqual(audit["offending_files"], ["scripts2/x.py"])


def test_audit_changed_files_against_scope_string_input_preserves_leading_whitespace_offender():
    audit = controller.audit_changed_files_against_scope(" scripts/x.py", ["scripts/x.py"], [])
    ASSERTIONS.assertFalse(audit["allowed"])
    ASSERTIONS.assertIn(" scripts/x.py", audit["offending_files"])


def test_audit_changed_files_against_scope_string_input_preserves_trailing_whitespace_offender():
    audit = controller.audit_changed_files_against_scope("scripts/x.py ", ["scripts/x.py"], [])
    ASSERTIONS.assertFalse(audit["allowed"])
    ASSERTIONS.assertIn("scripts/x.py ", audit["offending_files"])


def test_audit_changed_files_against_scope_string_input_without_whitespace_matches_allowlist():
    audit = controller.audit_changed_files_against_scope("scripts/x.py", ["scripts/x.py"], [])
    ASSERTIONS.assertTrue(audit["allowed"])
    ASSERTIONS.assertEqual(audit["offending_files"], [])


def test_audit_changed_files_against_scope_csv_string_trims_separator_whitespace():
    audit = controller.audit_changed_files_against_scope("scripts/a.py, tests/b.py", ["scripts/", "tests/"], [])
    ASSERTIONS.assertTrue(audit["allowed"])
    ASSERTIONS.assertEqual(audit["offending_files"], [])


def test_audit_changed_files_against_scope_newline_string_trims_separator_whitespace():
    audit = controller.audit_changed_files_against_scope("scripts/a.py\n tests/b.py", ["scripts/", "tests/"], [])
    ASSERTIONS.assertTrue(audit["allowed"])
    ASSERTIONS.assertEqual(audit["offending_files"], [])


def test_path_matches_scope_rule_slash_star_requires_child_path():
    ASSERTIONS.assertFalse(controller.path_matches_scope_rule("scripts", "scripts/*"))
    ASSERTIONS.assertTrue(controller.path_matches_scope_rule("scripts/x.py", "scripts/*"))
    ASSERTIONS.assertTrue(controller.path_matches_scope_rule("scripts/sub/x.py", "scripts/*"))


def test_scope_allows_file_change_slash_star_does_not_allow_directory_itself():
    ASSERTIONS.assertFalse(controller.scope_allows_file_change("scripts", ["scripts/*"], []))


def test_audit_changed_files_against_scope_blocks_parent_escape_path():
    audit = controller.audit_changed_files_against_scope(
        ["../secrets/server.key"],
        ["scripts/pr_automation_controller.py"],
        [],
    )
    ASSERTIONS.assertFalse(audit["allowed"])
    ASSERTIONS.assertIn("../secrets/server.key", audit["offending_files"])


def test_audit_changed_files_against_scope_blocks_multi_parent_escape_path():
    audit = controller.audit_changed_files_against_scope(
        ["../../README.md"],
        ["scripts/pr_automation_controller.py"],
        [],
    )
    ASSERTIONS.assertFalse(audit["allowed"])
    ASSERTIONS.assertIn("../../README.md", audit["offending_files"])


def test_audit_changed_files_against_scope_blocks_absolute_path():
    audit = controller.audit_changed_files_against_scope(
        ["/absolute/path"],
        ["scripts/pr_automation_controller.py"],
        [],
    )
    ASSERTIONS.assertFalse(audit["allowed"])
    ASSERTIONS.assertIn("/absolute/path", audit["offending_files"])


def test_audit_changed_files_against_scope_blocks_windows_absolute_path():
    audit = controller.audit_changed_files_against_scope(
        ["C:\\repo\\scripts\\tool.py"],
        ["scripts/"],
        [],
    )
    ASSERTIONS.assertFalse(audit["allowed"])
    ASSERTIONS.assertIn("C:\\repo\\scripts\\tool.py", audit["offending_files"])


def test_audit_changed_files_against_scope_blocks_literal_backslash_path():
    audit = controller.audit_changed_files_against_scope(
        ["scripts\\tool.py"],
        ["scripts/"],
        [],
    )
    ASSERTIONS.assertFalse(audit["allowed"])
    ASSERTIONS.assertIn("scripts\\tool.py", audit["offending_files"])


def test_audit_changed_files_against_scope_malformed_forbidden_dict_blocks():
    audit = controller.audit_changed_files_against_scope(
        ["scripts/secret.py"],
        ["scripts/"],
        {"files": ["scripts/secret.py"]},
    )
    ASSERTIONS.assertFalse(audit["allowed"])
    ASSERTIONS.assertEqual(audit["offending_files"], ["scripts/secret.py"])


def test_audit_changed_files_against_scope_invalid_forbidden_entry_blocks():
    audit = controller.audit_changed_files_against_scope(
        ["scripts/secret.py"],
        ["scripts/"],
        ["scripts/ secret.py"],
    )
    ASSERTIONS.assertFalse(audit["allowed"])
    ASSERTIONS.assertEqual(audit["offending_files"], ["scripts/secret.py"])


def test_audit_changed_files_against_scope_malformed_allowed_dict_blocks():
    audit = controller.audit_changed_files_against_scope(
        ["scripts/x.py"],
        {"files": ["scripts/x.py"]},
        [],
    )
    ASSERTIONS.assertFalse(audit["allowed"])
    ASSERTIONS.assertEqual(audit["offending_files"], ["scripts/x.py"])


def test_audit_changed_files_against_scope_invalid_allowed_entry_blocks():
    audit = controller.audit_changed_files_against_scope(
        ["scripts/x.py"],
        ["scripts/ x.py"],
        [],
    )
    ASSERTIONS.assertFalse(audit["allowed"])
    ASSERTIONS.assertEqual(audit["offending_files"], ["scripts/x.py"])


def test_audit_changed_files_against_scope_invalid_changed_file_matrix_blocked():
    for path in _invalid_changed_paths():
        audit = controller.audit_changed_files_against_scope([path], ["scripts/"], [])
        ASSERTIONS.assertFalse(audit["allowed"])


def test_audit_changed_files_against_scope_blocks_blank_path_entry():
    audit = controller.audit_changed_files_against_scope(
        [""],
        ["scripts/pr_automation_controller.py"],
        [],
    )
    ASSERTIONS.assertFalse(audit["allowed"])
    ASSERTIONS.assertIn("<invalid:empty>", audit["offending_files"])


def test_audit_changed_files_against_scope_blocks_placeholder_entries():
    audit = controller.audit_changed_files_against_scope(
        ["none", "n/a", "tbd"],
        ["scripts/pr_automation_controller.py"],
        [],
    )
    ASSERTIONS.assertFalse(audit["allowed"])
    ASSERTIONS.assertEqual(audit["offending_files"], ["n/a", "none", "tbd"])


def test_normalize_file_scope_rules_drops_invalid_rules_before_normalization():
    rules = [
        "/scripts/x.py",
        "C:\\repo\\scripts\\x.py",
        "D:/repo/scripts/x.py",
        "../scripts/x.py",
        "../../scripts/x.py",
        " scripts/x.py",
        "scripts/x.py ",
        "scripts/ x.py",
        "scripts/",
        "scripts/pr_*",
    ]
    normalized = controller.normalize_file_scope_rules(rules)
    ASSERTIONS.assertEqual(normalized, ["scripts/", "scripts/pr_*"])


def test_invalid_allow_rules_do_not_broaden_scope_matching():
    invalid_rules = [
        "/scripts/x.py",
        "C:\\repo\\scripts\\x.py",
        "D:/repo/scripts/x.py",
        "../scripts/x.py",
        "../../scripts/x.py",
        " scripts/x.py",
        "scripts/x.py ",
        "scripts/ x.py",
    ]
    ASSERTIONS.assertFalse(controller.scope_allows_file_change("scripts/x.py", invalid_rules, []))
    ASSERTIONS.assertEqual(controller.normalize_file_scope_rules(invalid_rules), [])


def test_audit_changed_files_against_scope_non_list_non_string_input_hardened():
    audit = controller.audit_changed_files_against_scope(
        {"path": "scripts/pr_automation_controller.py"},
        ["scripts/pr_automation_controller.py"],
        [],
    )
    ASSERTIONS.assertFalse(audit["allowed"])
    ASSERTIONS.assertEqual(audit["offending_files"], [controller.INVALID_CHANGED_FILES_INPUT_MARKER])


def test_build_scope_violation_result_contract_shape():
    result = controller.build_scope_violation_result(
        ["scripts/pr_flow_automation.py"],
        ["scripts/pr_automation_controller.py"],
        ["scripts/pr_flow_automation.py"],
    )
    ASSERTIONS.assertEqual(result["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(result["next_action"], "needs_manual_scope_violation")
    ASSERTIONS.assertFalse(result["can_patch"])
    ASSERTIONS.assertFalse(result["can_commit"])
    ASSERTIONS.assertFalse(result["can_push"])
    ASSERTIONS.assertEqual(result["offending_files"], ["scripts/pr_flow_automation.py"])
    ASSERTIONS.assertEqual(result["reason"], "scope_violation")


def test_build_scope_violation_result_non_list_non_string_input_hardened():
    result = controller.build_scope_violation_result(
        ("scripts/pr_flow_automation.py",),
        ["scripts/pr_automation_controller.py"],
        ["scripts/pr_flow_automation.py"],
    )
    ASSERTIONS.assertEqual(result["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(result["next_action"], "needs_manual_scope_violation")
    ASSERTIONS.assertFalse(result["can_patch"])
    ASSERTIONS.assertFalse(result["can_commit"])
    ASSERTIONS.assertFalse(result["can_push"])
    ASSERTIONS.assertEqual(result["offending_files"], [controller.INVALID_OFFENDING_FILES_INPUT_MARKER])


def test_enforce_patch_file_scope_pass_and_violation():
    ok = controller.enforce_patch_file_scope(
        ["scripts/pr_automation_controller.py"],
        ["scripts/pr_automation_controller.py"],
        ["scripts/pr_flow_automation.py"],
    )
    ASSERTIONS.assertEqual(ok["status"], "PASS")
    ASSERTIONS.assertEqual(ok["next_action"], "allowed")
    ASSERTIONS.assertTrue(ok["can_patch"])
    ASSERTIONS.assertTrue(ok["can_commit"])
    ASSERTIONS.assertTrue(ok["can_push"])

    bad = controller.enforce_patch_file_scope(
        ["scripts/pr_automation_controller.py", "scripts/pr_flow_automation.py"],
        ["scripts/pr_automation_controller.py"],
        ["scripts/pr_flow_automation.py"],
    )
    ASSERTIONS.assertEqual(bad["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(bad["next_action"], "needs_manual_scope_violation")
    ASSERTIONS.assertFalse(bad["can_patch"])
    ASSERTIONS.assertFalse(bad["can_commit"])
    ASSERTIONS.assertFalse(bad["can_push"])
    ASSERTIONS.assertIn("scripts/pr_flow_automation.py", bad["offending_files"])


def test_enforce_patch_file_scope_csv_string_trims_separator_whitespace():
    ok = controller.enforce_patch_file_scope(
        "scripts/a.py, tests/b.py",
        ["scripts/", "tests/"],
        [],
    )
    ASSERTIONS.assertEqual(ok["status"], "PASS")
    ASSERTIONS.assertTrue(ok["can_patch"])


def test_enforce_patch_file_scope_invalid_path_blocks_with_needs_manual_scope_violation():
    bad = controller.enforce_patch_file_scope(
        ["../secrets/server.key"],
        ["scripts/pr_automation_controller.py"],
        [],
    )
    ASSERTIONS.assertEqual(bad["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(bad["next_action"], "needs_manual_scope_violation")
    ASSERTIONS.assertFalse(bad["can_patch"])
    ASSERTIONS.assertIn("../secrets/server.key", bad["offending_files"])


def test_enforce_patch_file_scope_malformed_forbidden_dict_blocks_with_scope_violation_contract():
    bad = controller.enforce_patch_file_scope(
        ["scripts/secret.py"],
        ["scripts/"],
        {"files": ["scripts/secret.py"]},
    )
    ASSERTIONS.assertEqual(bad["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(bad["next_action"], "needs_manual_scope_violation")
    ASSERTIONS.assertFalse(bad["can_patch"])
    ASSERTIONS.assertFalse(bad["can_commit"])
    ASSERTIONS.assertFalse(bad["can_push"])
    ASSERTIONS.assertIn("scripts/secret.py", bad["offending_files"])


def test_enforce_patch_file_scope_invalid_forbidden_entry_blocks_with_scope_violation_contract():
    bad = controller.enforce_patch_file_scope(
        ["scripts/secret.py"],
        ["scripts/"],
        ["scripts/ secret.py"],
    )
    ASSERTIONS.assertEqual(bad["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(bad["next_action"], "needs_manual_scope_violation")
    ASSERTIONS.assertFalse(bad["can_patch"])
    ASSERTIONS.assertFalse(bad["can_commit"])
    ASSERTIONS.assertFalse(bad["can_push"])
    ASSERTIONS.assertIn("scripts/secret.py", bad["offending_files"])


def test_enforce_patch_file_scope_malformed_allowed_dict_blocks_with_scope_violation_contract():
    bad = controller.enforce_patch_file_scope(
        ["scripts/x.py"],
        {"files": ["scripts/x.py"]},
        [],
    )
    ASSERTIONS.assertEqual(bad["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(bad["next_action"], "needs_manual_scope_violation")
    ASSERTIONS.assertFalse(bad["can_patch"])
    ASSERTIONS.assertFalse(bad["can_commit"])
    ASSERTIONS.assertFalse(bad["can_push"])
    ASSERTIONS.assertIn("scripts/x.py", bad["offending_files"])


def test_enforce_patch_file_scope_invalid_allowed_entry_blocks_with_scope_violation_contract():
    bad = controller.enforce_patch_file_scope(
        ["scripts/x.py"],
        ["scripts/ x.py"],
        [],
    )
    ASSERTIONS.assertEqual(bad["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(bad["next_action"], "needs_manual_scope_violation")
    ASSERTIONS.assertFalse(bad["can_patch"])
    ASSERTIONS.assertFalse(bad["can_commit"])
    ASSERTIONS.assertFalse(bad["can_push"])
    ASSERTIONS.assertIn("scripts/x.py", bad["offending_files"])


def test_enforce_commit_file_scope_matches_patch_enforcement():
    patch_result = controller.enforce_patch_file_scope(
        ["scripts/pr_flow_automation.py"],
        ["scripts/pr_automation_controller.py"],
        ["scripts/pr_flow_automation.py"],
    )
    commit_result = controller.enforce_commit_file_scope(
        ["scripts/pr_flow_automation.py"],
        ["scripts/pr_automation_controller.py"],
        ["scripts/pr_flow_automation.py"],
    )
    ASSERTIONS.assertEqual(commit_result, patch_result)


def test_enforce_commit_file_scope_invalid_path_blocks_commit_and_push():
    result = controller.enforce_commit_file_scope(
        ["/absolute/path"],
        ["scripts/pr_automation_controller.py"],
        [],
    )
    ASSERTIONS.assertEqual(result["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(result["next_action"], "needs_manual_scope_violation")
    ASSERTIONS.assertFalse(result["can_commit"])
    ASSERTIONS.assertFalse(result["can_push"])
    ASSERTIONS.assertIn("/absolute/path", result["offending_files"])


def test_build_patch_scope_snapshot_rejects_invalid_and_snapshots_repo_relative(tmp_path):
    (tmp_path / "allowed.txt").write_text("before", encoding="utf-8")
    snapshot = controller.build_patch_scope_snapshot(
        [
            "allowed.txt",
            "/absolute/path",
            "../escape.txt",
            "scripts\\tool.py",
            " ",
            "none",
        ],
        repo_root=tmp_path,
    )
    ASSERTIONS.assertEqual(snapshot["snapshot_paths"], ["allowed.txt"])
    ASSERTIONS.assertIn("/absolute/path", snapshot["invalid_paths"])
    ASSERTIONS.assertIn("../escape.txt", snapshot["invalid_paths"])
    ASSERTIONS.assertIn("scripts\\tool.py", snapshot["invalid_paths"])
    ASSERTIONS.assertIn("none", snapshot["invalid_paths"])
    ASSERTIONS.assertIn("<invalid:empty>", snapshot["invalid_paths"])
    ASSERTIONS.assertTrue(snapshot["files"]["allowed.txt"]["exists"])
    ASSERTIONS.assertEqual(snapshot["files"]["allowed.txt"]["text"], "before")


def test_build_patch_scope_snapshot_marks_directory_invalid_without_crashing(tmp_path):
    (tmp_path / "folder").mkdir()
    snapshot = controller.build_patch_scope_snapshot(["folder"], repo_root=tmp_path)
    ASSERTIONS.assertEqual(snapshot["snapshot_paths"], [])
    ASSERTIONS.assertIn("folder", snapshot["invalid_paths"])


def test_build_patch_scope_snapshot_unreadable_candidate_does_not_abort(monkeypatch, tmp_path):
    (tmp_path / "ok.txt").write_text("ok", encoding="utf-8")
    broken = tmp_path / "broken.txt"
    broken.write_text("broken", encoding="utf-8")
    original_read_bytes = Path.read_bytes

    def _fake_read_bytes(path_obj):
        if path_obj == broken:
            raise OSError("simulated unreadable")
        return original_read_bytes(path_obj)

    monkeypatch.setattr(Path, "read_bytes", _fake_read_bytes)
    snapshot = controller.build_patch_scope_snapshot(["ok.txt", "broken.txt"], repo_root=tmp_path)
    ASSERTIONS.assertEqual(snapshot["snapshot_paths"], ["ok.txt"])
    ASSERTIONS.assertIn("broken.txt", snapshot["invalid_paths"])
    ASSERTIONS.assertEqual(snapshot["files"]["ok.txt"]["text"], "ok")


def test_build_patch_scope_snapshot_keeps_binary_bytes_on_decode_failure(tmp_path):
    binary = tmp_path / "binary.bin"
    payload = b"\xff\xfe\x00\x81"
    binary.write_bytes(payload)
    snapshot = controller.build_patch_scope_snapshot(["binary.bin"], repo_root=tmp_path)
    ASSERTIONS.assertEqual(snapshot["snapshot_paths"], ["binary.bin"])
    ASSERTIONS.assertEqual(snapshot["invalid_paths"], [])
    record = snapshot["files"]["binary.bin"]
    ASSERTIONS.assertTrue(record["exists"])
    ASSERTIONS.assertEqual(record["bytes"], payload)
    ASSERTIONS.assertEqual(record["text"], None)
    ASSERTIONS.assertEqual(record["sha256"], hashlib.sha256(payload).hexdigest())


def test_normalize_snapshot_candidates_splits_csv_and_newline_inputs():
    csv_valid, csv_invalid = controller._normalize_snapshot_candidates("a.py,b.py")
    newline_valid, newline_invalid = controller._normalize_snapshot_candidates("a.py\nb.py")
    ASSERTIONS.assertEqual(csv_valid, ["a.py", "b.py"])
    ASSERTIONS.assertEqual(newline_valid, ["a.py", "b.py"])
    ASSERTIONS.assertEqual(csv_invalid, [])
    ASSERTIONS.assertEqual(newline_invalid, [])


def test_collect_patch_scope_changes_detects_created_modified_deleted(tmp_path):
    deleted = tmp_path / "deleted.txt"
    modified = tmp_path / "modified.txt"
    created = tmp_path / "created.txt"
    deleted.write_text("delete-me", encoding="utf-8")
    modified.write_text("before", encoding="utf-8")
    pre = controller.build_patch_scope_snapshot(["deleted.txt", "modified.txt", "created.txt"], repo_root=tmp_path)
    deleted.unlink()
    modified.write_text("after", encoding="utf-8")
    created.write_text("new", encoding="utf-8")
    post = controller.build_patch_scope_snapshot(["deleted.txt", "modified.txt", "created.txt"], repo_root=tmp_path)
    changes = controller.collect_patch_scope_changes(
        pre,
        post,
        changed_files=["scripts\\bad.py", "modified.txt"],
    )
    ASSERTIONS.assertEqual(changes["created_files"], ["created.txt"])
    ASSERTIONS.assertEqual(changes["deleted_files"], ["deleted.txt"])
    ASSERTIONS.assertEqual(changes["modified_files"], ["modified.txt"])
    ASSERTIONS.assertIn("scripts\\bad.py", changes["invalid_paths"])


def test_collect_patch_scope_changes_handles_non_dict_snapshots():
    changes = controller.collect_patch_scope_changes("not-a-dict", 42, changed_files=None)
    ASSERTIONS.assertEqual(changes["changed_files"], [])
    ASSERTIONS.assertEqual(changes["created_files"], [])
    ASSERTIONS.assertEqual(changes["modified_files"], [])
    ASSERTIONS.assertEqual(changes["deleted_files"], [])
    ASSERTIONS.assertEqual(changes["invalid_paths"], [])


def test_collect_patch_scope_changes_detects_file_replaced_by_symlink_same_bytes(tmp_path):
    path = tmp_path / "swap.txt"
    path.write_text("target.txt", encoding="utf-8")
    pre = controller.build_patch_scope_snapshot(["swap.txt"], repo_root=tmp_path)
    path.unlink()
    path.symlink_to("target.txt")
    post = controller.build_patch_scope_snapshot(["swap.txt"], repo_root=tmp_path)
    changes = controller.collect_patch_scope_changes(pre, post)
    ASSERTIONS.assertEqual(changes["modified_files"], ["swap.txt"])
    ASSERTIONS.assertIn("swap.txt", changes["changed_files"])


def test_collect_patch_scope_changes_detects_symlink_retarget_same_resolved_bytes(tmp_path):
    target_a = tmp_path / "target-a.txt"
    target_b = tmp_path / "target-b.txt"
    target_a.write_text("same-content", encoding="utf-8")
    target_b.write_text("same-content", encoding="utf-8")
    link = tmp_path / "link.txt"
    link.symlink_to("target-a.txt")
    pre = controller.build_patch_scope_snapshot(["link.txt"], repo_root=tmp_path)
    link.unlink()
    link.symlink_to("target-b.txt")
    post = controller.build_patch_scope_snapshot(["link.txt"], repo_root=tmp_path)
    changes = controller.collect_patch_scope_changes(pre, post)
    ASSERTIONS.assertEqual(changes["modified_files"], ["link.txt"])
    ASSERTIONS.assertIn("link.txt", changes["changed_files"])


def test_rollback_scope_violations_removes_forbidden_created_file(tmp_path):
    pre = controller.build_patch_scope_snapshot(["allowed.txt", "forbidden.txt"], repo_root=tmp_path)
    (tmp_path / "allowed.txt").write_text("ok", encoding="utf-8")
    (tmp_path / "forbidden.txt").write_text("forbidden", encoding="utf-8")
    result = controller.rollback_scope_violations(pre, ["forbidden.txt"], repo_root=tmp_path)
    ASSERTIONS.assertTrue(result["rollback_attempted"])
    ASSERTIONS.assertTrue(result["rollback_succeeded"])
    ASSERTIONS.assertFalse((tmp_path / "forbidden.txt").exists())
    ASSERTIONS.assertEqual(result["rolled_back_files"], ["forbidden.txt"])


def test_rollback_scope_violations_restores_forbidden_modified_file(tmp_path):
    blocked = tmp_path / "blocked.txt"
    blocked.write_text("before", encoding="utf-8")
    pre = controller.build_patch_scope_snapshot(["blocked.txt"], repo_root=tmp_path)
    blocked.write_text("after", encoding="utf-8")
    result = controller.rollback_scope_violations(pre, ["blocked.txt"], repo_root=tmp_path)
    ASSERTIONS.assertTrue(result["rollback_succeeded"])
    ASSERTIONS.assertEqual(blocked.read_text(encoding="utf-8"), "before")


def test_rollback_scope_violations_restores_forbidden_deleted_file(tmp_path):
    blocked = tmp_path / "blocked.txt"
    blocked.write_text("before", encoding="utf-8")
    pre = controller.build_patch_scope_snapshot(["blocked.txt"], repo_root=tmp_path)
    blocked.unlink()
    result = controller.rollback_scope_violations(pre, ["blocked.txt"], repo_root=tmp_path)
    ASSERTIONS.assertTrue(result["rollback_succeeded"])
    ASSERTIONS.assertEqual(blocked.read_text(encoding="utf-8"), "before")


def test_rollback_scope_violations_restores_symlink_path_without_mutating_target(tmp_path):
    source_a = tmp_path / "source-a.txt"
    source_b = tmp_path / "source-b.txt"
    source_a.write_text("A", encoding="utf-8")
    source_b.write_text("B", encoding="utf-8")
    link = tmp_path / "link.txt"
    link.symlink_to("source-a.txt")
    pre = controller.build_patch_scope_snapshot(["link.txt"], repo_root=tmp_path)
    link.unlink()
    link.symlink_to("source-b.txt")
    result = controller.rollback_scope_violations(pre, ["link.txt"], repo_root=tmp_path)
    ASSERTIONS.assertTrue(result["rollback_succeeded"])
    ASSERTIONS.assertTrue(link.is_symlink())
    ASSERTIONS.assertEqual(os.readlink(link), "source-a.txt")
    ASSERTIONS.assertEqual(source_a.read_text(encoding="utf-8"), "A")
    ASSERTIONS.assertEqual(source_b.read_text(encoding="utf-8"), "B")


def test_rollback_scope_violations_restores_binary_bytes_exactly(tmp_path):
    binary = tmp_path / "blocked.bin"
    before = b"\x00\x01\x02\xfe\xff"
    after = b"\x10\x20\x30"
    binary.write_bytes(before)
    pre = controller.build_patch_scope_snapshot(["blocked.bin"], repo_root=tmp_path)
    binary.write_bytes(after)
    result = controller.rollback_scope_violations(pre, ["blocked.bin"], repo_root=tmp_path)
    ASSERTIONS.assertTrue(result["rollback_succeeded"])
    ASSERTIONS.assertEqual(binary.read_bytes(), before)


def test_rollback_scope_violations_replaces_forbidden_directory_with_original_file(tmp_path):
    blocked = tmp_path / "blocked.txt"
    blocked.write_text("before", encoding="utf-8")
    pre = controller.build_patch_scope_snapshot(["blocked.txt"], repo_root=tmp_path)
    blocked.unlink()
    blocked.mkdir()
    (blocked / "nested.txt").write_text("nested", encoding="utf-8")
    result = controller.rollback_scope_violations(pre, ["blocked.txt"], repo_root=tmp_path)
    ASSERTIONS.assertTrue(result["rollback_succeeded"])
    ASSERTIONS.assertTrue(blocked.is_file())
    ASSERTIONS.assertEqual(blocked.read_text(encoding="utf-8"), "before")


def test_rollback_scope_violations_directory_cleanup_failure_fails_closed(tmp_path, monkeypatch):
    blocked = tmp_path / "blocked.txt"
    blocked.write_text("before", encoding="utf-8")
    pre = controller.build_patch_scope_snapshot(["blocked.txt"], repo_root=tmp_path)
    blocked.unlink()
    blocked.mkdir()

    def _fail_rmtree(_path):
        raise OSError("simulated rmtree failure")

    monkeypatch.setattr(controller.shutil, "rmtree", _fail_rmtree)
    result = controller.rollback_scope_violations(pre, ["blocked.txt"], repo_root=tmp_path)
    ASSERTIONS.assertTrue(result["rollback_attempted"])
    ASSERTIONS.assertFalse(result["rollback_succeeded"])
    ASSERTIONS.assertIn("blocked.txt:simulated rmtree failure", result["rollback_errors"][0])


def test_enforce_post_patch_scope_or_rollback_mixed_changes_rolls_back_forbidden_only(tmp_path, monkeypatch):
    allowed = tmp_path / "allowed.txt"
    forbidden = tmp_path / "forbidden.txt"
    allowed.write_text("old-allowed", encoding="utf-8")
    forbidden.write_text("old-forbidden", encoding="utf-8")
    pre = controller.build_patch_scope_snapshot(["allowed.txt", "forbidden.txt"], repo_root=tmp_path)
    allowed.write_text("new-allowed", encoding="utf-8")
    forbidden.write_text("new-forbidden", encoding="utf-8")
    post = controller.build_patch_scope_snapshot(["allowed.txt", "forbidden.txt"], repo_root=tmp_path)
    result = controller.enforce_post_patch_scope_or_rollback(
        candidate_paths=["allowed.txt", "forbidden.txt"],
        files_allowed=["allowed.txt"],
        files_forbidden=["forbidden.txt"],
        pre_snapshot=pre,
        post_snapshot=post,
        repo_root=tmp_path,
    )
    ASSERTIONS.assertEqual(result["post_fix_audit"], "FAIL")
    ASSERTIONS.assertEqual(result["next_action"], "needs_manual_scope_violation")
    ASSERTIONS.assertIn("forbidden.txt", result["offending_files"])
    ASSERTIONS.assertEqual(allowed.read_text(encoding="utf-8"), "new-allowed")
    ASSERTIONS.assertEqual(forbidden.read_text(encoding="utf-8"), "old-forbidden")


def test_enforce_post_patch_scope_or_rollback_rollback_failure_fails_closed(tmp_path, monkeypatch):
    blocked = tmp_path / "blocked.txt"
    blocked.write_text("before", encoding="utf-8")
    pre = controller.build_patch_scope_snapshot(["blocked.txt"], repo_root=tmp_path)
    blocked.write_text("after", encoding="utf-8")
    post = controller.build_patch_scope_snapshot(["blocked.txt"], repo_root=tmp_path)
    monkeypatch.setattr(
        controller,
        "rollback_scope_violations",
        lambda *_args, **_kwargs: {
            "rollback_attempted": True,
            "rollback_succeeded": False,
            "rolled_back_files": [],
            "rollback_errors": ["blocked.txt:simulated-failure"],
        },
    )
    result = controller.enforce_post_patch_scope_or_rollback(
        candidate_paths=["blocked.txt"],
        files_allowed=["allowed.txt"],
        files_forbidden=["blocked.txt"],
        pre_snapshot=pre,
        post_snapshot=post,
        repo_root=tmp_path,
    )
    ASSERTIONS.assertEqual(result["next_action"], "needs_manual_scope_violation_rollback_failed")
    ASSERTIONS.assertFalse(result["can_commit"])
    ASSERTIONS.assertFalse(result["can_push"])
    ASSERTIONS.assertTrue(result["rollback_attempted"])
    ASSERTIONS.assertFalse(result["rollback_succeeded"])


def test_enforce_post_patch_scope_or_rollback_ignores_live_dirty_state(tmp_path, monkeypatch):
    allowed = tmp_path / "allowed.txt"
    forbidden = tmp_path / "forbidden.txt"
    allowed.write_text("old-allowed", encoding="utf-8")
    forbidden.write_text("old-forbidden", encoding="utf-8")
    pre = controller.build_patch_scope_snapshot(["allowed.txt", "forbidden.txt"], repo_root=tmp_path)
    allowed.write_text("new-allowed", encoding="utf-8")
    forbidden.write_text("new-forbidden", encoding="utf-8")
    post = controller.build_patch_scope_snapshot(["allowed.txt", "forbidden.txt"], repo_root=tmp_path)
    result = controller.enforce_post_patch_scope_or_rollback(
        candidate_paths=["allowed.txt", "forbidden.txt"],
        files_allowed=["allowed.txt"],
        files_forbidden=["forbidden.txt"],
        pre_snapshot=pre,
        post_snapshot=post,
        repo_root=tmp_path,
    )
    ASSERTIONS.assertEqual(result["status"], "FAIL")
    ASSERTIONS.assertEqual(result["next_action"], "needs_manual_scope_violation")
    ASSERTIONS.assertTrue(result["rollback_attempted"])
    ASSERTIONS.assertTrue(result["rollback_succeeded"])
    ASSERTIONS.assertEqual(allowed.read_text(encoding="utf-8"), "new-allowed")
    ASSERTIONS.assertEqual(forbidden.read_text(encoding="utf-8"), "old-forbidden")


def test_detect_dirty_worktree_precondition_fails_closed(monkeypatch):
    monkeypatch.setattr(
        controller,
        "run",
        lambda *_args, **_kwargs: " M scripts/pr_automation_controller.py\n",
    )
    result = controller.detect_dirty_worktree_precondition()
    ASSERTIONS.assertTrue(result["dirty_worktree"])
    ASSERTIONS.assertEqual(result["post_fix_audit"], "FAIL")
    ASSERTIONS.assertEqual(result["next_action"], "needs_manual_scope_violation")
    ASSERTIONS.assertFalse(result["can_commit"])
    ASSERTIONS.assertFalse(result["can_push"])


def test_enforce_post_patch_scope_or_rollback_allowed_only_passes_without_rollback(tmp_path, monkeypatch):
    allowed = tmp_path / "allowed.txt"
    allowed.write_text("before", encoding="utf-8")
    pre = controller.build_patch_scope_snapshot(["allowed.txt"], repo_root=tmp_path)
    allowed.write_text("after", encoding="utf-8")
    post = controller.build_patch_scope_snapshot(["allowed.txt"], repo_root=tmp_path)
    result = controller.enforce_post_patch_scope_or_rollback(
        candidate_paths=["allowed.txt"],
        files_allowed=["allowed.txt"],
        files_forbidden=[],
        pre_snapshot=pre,
        post_snapshot=post,
        repo_root=tmp_path,
    )
    ASSERTIONS.assertEqual(result["status"], "PASS")
    ASSERTIONS.assertEqual(result["post_fix_audit"], "PASS")
    ASSERTIONS.assertEqual(result["next_action"], "allowed")
    ASSERTIONS.assertFalse(result["rollback_attempted"])
    ASSERTIONS.assertTrue(result["can_commit"])
    ASSERTIONS.assertTrue(result["can_push"])


def test_enforce_post_patch_scope_or_rollback_malformed_allowlist_defaults_deny(tmp_path, monkeypatch):
    blocked = tmp_path / "blocked.txt"
    blocked.write_text("before", encoding="utf-8")
    pre = controller.build_patch_scope_snapshot(["blocked.txt"], repo_root=tmp_path)
    blocked.write_text("after", encoding="utf-8")
    post = controller.build_patch_scope_snapshot(["blocked.txt"], repo_root=tmp_path)
    result = controller.enforce_post_patch_scope_or_rollback(
        candidate_paths=["blocked.txt"],
        files_allowed={"files": ["blocked.txt"]},
        files_forbidden=[],
        pre_snapshot=pre,
        post_snapshot=post,
        repo_root=tmp_path,
    )
    ASSERTIONS.assertEqual(result["post_fix_audit"], "FAIL")
    ASSERTIONS.assertEqual(result["next_action"], "needs_manual_scope_violation")
    ASSERTIONS.assertFalse(result["can_commit"])
    ASSERTIONS.assertFalse(result["can_push"])


def test_enforce_post_patch_scope_or_rollback_removes_created_forbidden_file(
    tmp_path,
    monkeypatch,
):
    allowed = tmp_path / "allowed.txt"
    forbidden = tmp_path / "forbidden.txt"
    allowed.write_text("before-allowed", encoding="utf-8")
    pre = controller.build_patch_scope_snapshot(["allowed.txt", "forbidden.txt"], repo_root=tmp_path)
    allowed.write_text("after-allowed", encoding="utf-8")
    forbidden.write_text("new-forbidden", encoding="utf-8")
    post = controller.build_patch_scope_snapshot(["allowed.txt", "forbidden.txt"], repo_root=tmp_path)
    result = controller.enforce_post_patch_scope_or_rollback(
        candidate_paths=["allowed.txt", "forbidden.txt"],
        files_allowed=["allowed.txt"],
        files_forbidden=["forbidden.txt"],
        pre_snapshot=pre,
        post_snapshot=post,
        repo_root=tmp_path,
    )
    ASSERTIONS.assertEqual(result["status"], "FAIL")
    ASSERTIONS.assertEqual(result["next_action"], "needs_manual_scope_violation")
    ASSERTIONS.assertTrue(result["rollback_attempted"])
    ASSERTIONS.assertTrue(result["rollback_succeeded"])
    ASSERTIONS.assertFalse(forbidden.exists())
    ASSERTIONS.assertEqual(allowed.read_text(encoding="utf-8"), "after-allowed")


def test_enforce_post_patch_scope_or_rollback_fails_closed_on_insufficient_evidence(tmp_path):
    result = controller.enforce_post_patch_scope_or_rollback(
        candidate_paths=["allowed.txt"],
        files_allowed=["allowed.txt"],
        files_forbidden=[],
        changed_files=None,
        pre_snapshot=None,
        post_snapshot=None,
        repo_root=tmp_path,
    )
    ASSERTIONS.assertEqual(result["post_fix_audit"], "FAIL")
    ASSERTIONS.assertFalse(result["can_commit"])
    ASSERTIONS.assertFalse(result["can_push"])
    ASSERTIONS.assertEqual(result["next_action"], "needs_manual_scope_violation")


def test_enforce_post_patch_scope_or_rollback_uses_generated_post_snapshot_when_pre_snapshot_provided(tmp_path):
    allowed = tmp_path / "allowed.txt"
    forbidden = tmp_path / "forbidden.txt"
    allowed.write_text("old-allowed", encoding="utf-8")
    forbidden.write_text("old-forbidden", encoding="utf-8")
    pre = controller.build_patch_scope_snapshot(["allowed.txt", "forbidden.txt"], repo_root=tmp_path)
    allowed.write_text("new-allowed", encoding="utf-8")
    forbidden.write_text("new-forbidden", encoding="utf-8")
    result = controller.enforce_post_patch_scope_or_rollback(
        candidate_paths=["allowed.txt", "forbidden.txt"],
        files_allowed=["allowed.txt"],
        files_forbidden=["forbidden.txt"],
        changed_files=None,
        pre_snapshot=pre,
        post_snapshot=None,
        repo_root=tmp_path,
    )
    ASSERTIONS.assertNotEqual(result["scope_audit"].get("reason"), "insufficient_scope_evidence")
    ASSERTIONS.assertTrue(result["scope_audit"].get("changed_files"))
    ASSERTIONS.assertTrue(result["rollback_attempted"])
    ASSERTIONS.assertTrue(result["rollback_succeeded"])
    ASSERTIONS.assertEqual(forbidden.read_text(encoding="utf-8"), "old-forbidden")
    ASSERTIONS.assertEqual(allowed.read_text(encoding="utf-8"), "new-allowed")


def test_enforce_post_patch_scope_or_rollback_changed_files_only_forbidden_modified_fails_closed(tmp_path):
    blocked = tmp_path / "blocked.txt"
    blocked.write_text("after", encoding="utf-8")
    result = controller.enforce_post_patch_scope_or_rollback(
        candidate_paths=["blocked.txt"],
        files_allowed=["allowed.txt"],
        files_forbidden=["blocked.txt"],
        changed_files=["blocked.txt"],
        pre_snapshot=None,
        post_snapshot=None,
        repo_root=tmp_path,
    )
    ASSERTIONS.assertEqual(result["post_fix_audit"], "FAIL")
    ASSERTIONS.assertEqual(result["next_action"], "needs_manual_scope_violation")
    ASSERTIONS.assertFalse(result["rollback_attempted"])
    ASSERTIONS.assertFalse(result["rollback_succeeded"])
    ASSERTIONS.assertFalse(result["can_commit"])
    ASSERTIONS.assertFalse(result["can_push"])
    ASSERTIONS.assertIn(
        result["scope_audit"].get("reason"),
        {"missing_trusted_pre_snapshot", "rollback_evidence_missing"},
    )
    ASSERTIONS.assertIn("rollback_evidence_missing", result["rollback_errors"])


def test_enforce_post_patch_scope_or_rollback_malformed_empty_dict_pre_snapshot_fails_closed(tmp_path):
    blocked = tmp_path / "blocked.txt"
    blocked.write_text("after", encoding="utf-8")
    result = controller.enforce_post_patch_scope_or_rollback(
        candidate_paths=["blocked.txt"],
        files_allowed=["allowed.txt"],
        files_forbidden=["blocked.txt"],
        changed_files=["blocked.txt"],
        pre_snapshot={},
        post_snapshot=None,
        repo_root=tmp_path,
    )
    ASSERTIONS.assertEqual(result["post_fix_audit"], "FAIL")
    ASSERTIONS.assertFalse(result["rollback_succeeded"])
    ASSERTIONS.assertIn("rollback_evidence_missing", result["rollback_errors"])
    ASSERTIONS.assertEqual(result["scope_audit"].get("reason"), "rollback_evidence_missing")


def test_enforce_post_patch_scope_or_rollback_malformed_non_dict_files_pre_snapshot_fails_closed(tmp_path):
    blocked = tmp_path / "blocked.txt"
    blocked.write_text("after", encoding="utf-8")
    result = controller.enforce_post_patch_scope_or_rollback(
        candidate_paths=["blocked.txt"],
        files_allowed=["allowed.txt"],
        files_forbidden=["blocked.txt"],
        changed_files=["blocked.txt"],
        pre_snapshot={"files": []},
        post_snapshot=None,
        repo_root=tmp_path,
    )
    ASSERTIONS.assertEqual(result["post_fix_audit"], "FAIL")
    ASSERTIONS.assertFalse(result["rollback_succeeded"])
    ASSERTIONS.assertIn("rollback_evidence_missing", result["rollback_errors"])
    ASSERTIONS.assertEqual(result["scope_audit"].get("reason"), "rollback_evidence_missing")


def test_build_scope_rollback_result_payload_shape():
    scope_audit = {
        "allowed": False,
        "offending_files": ["forbidden.txt"],
    }
    rollback = {
        "rollback_attempted": True,
        "rollback_succeeded": True,
        "rolled_back_files": ["forbidden.txt"],
        "rollback_errors": [],
    }
    result = controller.build_scope_rollback_result(
        scope_audit,
        rollback,
        ["allowed.txt", "forbidden.txt"],
    )
    expected_keys = {
        "status",
        "post_fix_audit",
        "next_action",
        "can_commit",
        "can_push",
        "rollback_attempted",
        "rollback_succeeded",
        "changed_files",
        "offending_files",
        "allowed_files",
        "rolled_back_files",
        "rollback_errors",
        "scope_audit",
    }
    ASSERTIONS.assertEqual(set(result.keys()), expected_keys)


def test_build_scope_rollback_result_next_action_contract():
    scope_audit = {"allowed": False, "offending_files": ["forbidden.txt"]}
    unattempted = controller.build_scope_rollback_result(
        scope_audit,
        {"rollback_attempted": False, "rollback_succeeded": False},
        ["forbidden.txt"],
    )
    succeeded = controller.build_scope_rollback_result(
        scope_audit,
        {"rollback_attempted": True, "rollback_succeeded": True},
        ["forbidden.txt"],
    )
    failed = controller.build_scope_rollback_result(
        scope_audit,
        {"rollback_attempted": True, "rollback_succeeded": False},
        ["forbidden.txt"],
    )
    ASSERTIONS.assertEqual(unattempted["next_action"], "needs_manual_scope_violation")
    ASSERTIONS.assertEqual(succeeded["next_action"], "needs_manual_scope_violation")
    ASSERTIONS.assertEqual(failed["next_action"], "needs_manual_scope_violation_rollback_failed")


def test_enforce_patch_and_commit_file_scope_windows_absolute_path_blocks_patch_commit_and_push():
    path = "D:/repo/scripts/tool.py"
    patch_result = controller.enforce_patch_file_scope(
        [path],
        ["scripts/"],
        [],
    )
    commit_result = controller.enforce_commit_file_scope(
        [path],
        ["scripts/"],
        [],
    )
    for result in (patch_result, commit_result):
        ASSERTIONS.assertEqual(result["status"], "NEEDS_MANUAL")
        ASSERTIONS.assertEqual(result["next_action"], "needs_manual_scope_violation")
        ASSERTIONS.assertFalse(result["can_patch"])
        ASSERTIONS.assertFalse(result["can_commit"])
        ASSERTIONS.assertFalse(result["can_push"])
        ASSERTIONS.assertTrue(result["offending_files"])
        ASSERTIONS.assertIn(path, result["offending_files"])


def test_enforce_patch_and_commit_file_scope_literal_backslash_path_blocks_patch_commit_and_push():
    path = "scripts\\tool.py"
    patch_result = controller.enforce_patch_file_scope(
        [path],
        ["scripts/"],
        [],
    )
    commit_result = controller.enforce_commit_file_scope(
        [path],
        ["scripts/"],
        [],
    )
    for result in (patch_result, commit_result):
        ASSERTIONS.assertEqual(result["status"], "NEEDS_MANUAL")
        ASSERTIONS.assertEqual(result["next_action"], "needs_manual_scope_violation")
        ASSERTIONS.assertFalse(result["can_patch"])
        ASSERTIONS.assertFalse(result["can_commit"])
        ASSERTIONS.assertFalse(result["can_push"])
        ASSERTIONS.assertTrue(result["offending_files"])
        ASSERTIONS.assertIn(path, result["offending_files"])


def test_classify_merge_conflict_contract_paths():
    """Merge conflict classification mirrors baseline auto/manual taxonomy contract."""
    dirty = controller.classify_merge_conflict(
        {"mergeable": "MERGEABLE", "mergeStateStatus": "DIRTY"},
        ["scripts/pr_flow_automation.py"],
        {"files": ["scripts/pr_automation_controller.py"]},
    )
    ASSERTIONS.assertEqual(dirty["category"], "merge_conflict")
    ASSERTIONS.assertFalse(dirty["auto_resolvable"])
    ASSERTIONS.assertEqual(dirty["resolution_strategy"], "needs_manual")
    ASSERTIONS.assertEqual(dirty["next_action"], "needs_manual_merge_conflict")
    conflicting = controller.classify_merge_conflict(
        {"mergeable": "CONFLICTING", "mergeStateStatus": "CLEAN"},
        ["order_manager.py"],
        {"files": ["scripts/pr_flow_automation.py"]},
    )
    ASSERTIONS.assertFalse(conflicting["auto_resolvable"])
    ASSERTIONS.assertEqual(conflicting["next_action"], "needs_manual_merge_conflict")


def test_classify_merge_conflict_clean_pr_is_not_conflict():
    """Clean PR metadata must not be labeled as merge_conflict."""
    clean = controller.classify_merge_conflict(
        {"mergeable": "MERGEABLE", "mergeStateStatus": "CLEAN"},
        ["scripts/pr_flow_automation.py"],
        {"files": ["scripts/pr_automation_controller.py"]},
    )
    ASSERTIONS.assertEqual(clean["category"], "none")
    ASSERTIONS.assertFalse(clean["auto_resolvable"])
    ASSERTIONS.assertEqual(clean["resolution_strategy"], "")
    ASSERTIONS.assertEqual(clean["next_action"], "")


def test_merge_conflict_default_deny_with_empty_allowlist(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "scripts").mkdir()
    path = tmp_path / "scripts" / "candidate.py"
    path.write_text("<<<<<<< ours\nx=1\n=======\nx=2\n>>>>>>> theirs\n", encoding="utf-8")
    result = controller.classify_merge_conflict(
        {"mergeable": "CONFLICTING", "mergeStateStatus": "DIRTY"},
        ["scripts/candidate.py"],
        {},
    )
    ASSERTIONS.assertTrue(result["denied"])
    ASSERTIONS.assertEqual(result["reason"], "path_default_deny_empty_allowlist")
    ASSERTIONS.assertEqual(result["next_action"], "needs_manual_merge_conflict")


def test_merge_conflict_allowlisted_automation_file_with_valid_markers_is_safe(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "scripts").mkdir()
    path = tmp_path / "scripts" / "candidate.py"
    path.write_text("<<<<<<< ours\nx=1\n=======\nx=2\n>>>>>>> theirs\n", encoding="utf-8")
    result = controller.classify_merge_conflict(
        {"mergeable": "CONFLICTING", "mergeStateStatus": "DIRTY"},
        ["scripts/candidate.py"],
        {"files": ["scripts/candidate.py"]},
    )
    ASSERTIONS.assertTrue(result["allowed"])
    ASSERTIONS.assertFalse(result["denied"])
    ASSERTIONS.assertTrue(result["auto_resolvable"])
    ASSERTIONS.assertEqual(result["action"], "auto_resolve_merge_conflict")
    ASSERTIONS.assertEqual(result["next_action"], "auto_resolve_merge_conflict")


def test_merge_conflict_result_payload_has_required_keys(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "scripts").mkdir()
    path = tmp_path / "scripts" / "candidate.py"
    path.write_text("<<<<<<< ours\nx=1\n=======\nx=2\n>>>>>>> theirs\n", encoding="utf-8")
    result = controller.classify_merge_conflict(
        {"mergeable": "CONFLICTING", "mergeStateStatus": "DIRTY"},
        ["scripts/candidate.py"],
        {"files": ["scripts/candidate.py"]},
    )
    required_keys = (
        "allowed",
        "denied",
        "action",
        "path",
        "reason",
        "next_action",
        "needs_manual",
        "conflict_class",
        "evidence",
    )
    for key in required_keys:
        ASSERTIONS.assertIn(key, result)


def test_merge_conflict_forbidden_paths_denied_even_when_allowlisted():
    for path in (
        ".github/workflows/pr-autofix-selfhosted.yml",
        "core/engine.py",
        "runtime/worker.py",
        "business/logic.py",
        "secrets/token.txt",
        "config/providers/openai.yml",
    ):
        result = controller.classify_merge_conflict(
            {"mergeable": "CONFLICTING", "mergeStateStatus": "DIRTY"},
            [path],
            {"files": [path]},
        )
        ASSERTIONS.assertTrue(result["denied"])
        ASSERTIONS.assertEqual(result["reason"], "path_forbidden")
        ASSERTIONS.assertEqual(result["next_action"], "needs_manual_merge_conflict")


def test_merge_conflict_non_allowlisted_paths_denied():
    result = controller.classify_merge_conflict(
        {"mergeable": "CONFLICTING", "mergeStateStatus": "DIRTY"},
        ["scripts/candidate.py"],
        {"files": ["scripts/other.py"]},
    )
    ASSERTIONS.assertTrue(result["denied"])
    ASSERTIONS.assertEqual(result["reason"], "path_not_allowlisted")


def test_merge_conflict_malformed_path_candidates_denied():
    bad_paths = [
        "../scripts/a.py",
        "/abs/path.py",
        "C:/repo/scripts/a.py",
        "scripts\\a.py",
        " scripts/a.py",
        "scripts/a.py ",
        "scripts /a.py",
        "",
        None,
        {"path": "scripts/a.py"},
        "<path>",
    ]
    for bad in bad_paths:
        result = controller.classify_merge_conflict(
            {"mergeable": "CONFLICTING", "mergeStateStatus": "DIRTY"},
            [bad],
            {"files": ["scripts/a.py"]},
        )
        ASSERTIONS.assertTrue(result["denied"])
        ASSERTIONS.assertTrue(result["needs_manual"])
        if not isinstance(bad, str):
            ASSERTIONS.assertEqual(result["reason"], "path_non_string")
        ASSERTIONS.assertEqual(result["next_action"], "needs_manual_merge_conflict")
        ASSERTIONS.assertNotEqual(result["action"], "auto_resolve_merge_conflict")


def test_merge_conflict_missing_binary_unreadable_and_symlink_denied(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "scripts").mkdir()
    missing = controller.classify_merge_conflict(
        {"mergeable": "CONFLICTING", "mergeStateStatus": "DIRTY"},
        ["scripts/missing.py"],
        {"files": ["scripts/missing.py"]},
    )
    ASSERTIONS.assertEqual(missing["reason"], "missing_file")

    binary_path = tmp_path / "scripts" / "binary.py"
    binary_path.write_bytes(b"\x00\x01\x02")
    binary = controller.classify_merge_conflict(
        {"mergeable": "CONFLICTING", "mergeStateStatus": "DIRTY"},
        ["scripts/binary.py"],
        {"files": ["scripts/binary.py"]},
    )
    ASSERTIONS.assertEqual(binary["reason"], "binary_file")

    monkeypatch.setattr(controller.os, "access", lambda _path, _mode: False)
    unreadable_path = tmp_path / "scripts" / "unreadable.py"
    unreadable_path.write_text("<<<<<<< ours\nx\n=======\ny\n>>>>>>> theirs\n", encoding="utf-8")
    unreadable = controller.classify_merge_conflict(
        {"mergeable": "CONFLICTING", "mergeStateStatus": "DIRTY"},
        ["scripts/unreadable.py"],
        {"files": ["scripts/unreadable.py"]},
    )
    ASSERTIONS.assertEqual(unreadable["reason"], "unreadable_file")
    ASSERTIONS.assertNotEqual(unreadable["reason"], "safe_auto_resolve_candidate")

    symlink_target = tmp_path / "scripts" / "target.py"
    symlink_target.write_text("<<<<<<< ours\nx\n=======\ny\n>>>>>>> theirs\n", encoding="utf-8")
    symlink_path = tmp_path / "scripts" / "link.py"
    symlink_path.symlink_to(symlink_target)
    symlink = controller.classify_merge_conflict(
        {"mergeable": "CONFLICTING", "mergeStateStatus": "DIRTY"},
        ["scripts/link.py"],
        {"files": ["scripts/link.py"]},
    )
    ASSERTIONS.assertEqual(symlink["reason"], "symlink_file")


def test_merge_conflict_marker_failures_denied(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "scripts").mkdir()
    cases = {
        "scripts/no_markers.py": "unknown_marker_shape",
        "scripts/malformed.py": "unbalanced_conflict_markers",
        "scripts/nested.py": "nested_conflict_markers",
        "scripts/duplicate.py": "duplicate_conflict_separator",
        "scripts/onesided.py": "one_sided_deletion_marker_shape",
        "scripts/multi.py": "safety_gate_conflict",
    }
    (tmp_path / "scripts" / "no_markers.py").write_text("print('x')\n", encoding="utf-8")
    (tmp_path / "scripts" / "malformed.py").write_text("<<<<<<< ours\nx\n=======\ny\n", encoding="utf-8")
    (tmp_path / "scripts" / "nested.py").write_text(
        "<<<<<<< ours\nx\n<<<<<<< ours2\ny\n=======\nz\n>>>>>>> theirs2\n>>>>>>> theirs\n", encoding="utf-8"
    )
    (tmp_path / "scripts" / "duplicate.py").write_text(
        "<<<<<<< ours\nx\n=======\ny\n=======\nz\n>>>>>>> theirs\n", encoding="utf-8"
    )
    (tmp_path / "scripts" / "onesided.py").write_text("<<<<<<< ours\nx\n>>>>>>> theirs\n", encoding="utf-8")
    (tmp_path / "scripts" / "multi.py").write_text(
        "<<<<<<< ours\nx\n=======\ny\n>>>>>>> theirs\n\n<<<<<<< ours2\na\n=======\nb\n>>>>>>> theirs2\n",
        encoding="utf-8",
    )
    for file_path, expected_reason in cases.items():
        result = controller.classify_merge_conflict(
            {"mergeable": "CONFLICTING", "mergeStateStatus": "DIRTY"},
            [file_path],
            {"files": [file_path]},
        )
        ASSERTIONS.assertTrue(result["denied"])
        ASSERTIONS.assertEqual(result["reason"], expected_reason)


def test_merge_conflict_mixed_safe_and_forbidden_denied(tmp_path, monkeypatch):
    """Mixed conflicted files must deny immediately when any path is forbidden."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts" / "candidate.py").write_text(
        "<<<<<<< ours\nx=1\n=======\nx=2\n>>>>>>> theirs\n",
        encoding="utf-8",
    )
    result = controller.classify_merge_conflict(
        {"mergeable": "CONFLICTING", "mergeStateStatus": "DIRTY"},
        ["scripts/candidate.py", "core/engine.py"],
        {"files": ["scripts/candidate.py", "core/engine.py"]},
    )
    ASSERTIONS.assertTrue(result["denied"])
    ASSERTIONS.assertEqual(result["reason"], "path_forbidden")
    ASSERTIONS.assertEqual(result["next_action"], "needs_manual_merge_conflict")
    evidence = cast(dict[str, Any], result.get("evidence") or {})
    ASSERTIONS.assertEqual(evidence.get("validated_paths"), ["scripts/candidate.py"])


def test_merge_conflict_all_safe_files_validate_before_allowing_auto_resolve(tmp_path, monkeypatch):
    """All conflicted files must validate before reporting safe auto-resolve evidence."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts" / "a.py").write_text(
        "<<<<<<< ours\nx=1\n=======\nx=2\n>>>>>>> theirs\n",
        encoding="utf-8",
    )
    (tmp_path / "scripts" / "b.py").write_text(
        "<<<<<<< ours\ny=1\n=======\ny=2\n>>>>>>> theirs\n",
        encoding="utf-8",
    )
    result = controller.classify_merge_conflict(
        {"mergeable": "CONFLICTING", "mergeStateStatus": "DIRTY"},
        ["scripts/a.py", "scripts/b.py"],
        {"files": ["scripts/a.py", "scripts/b.py"]},
    )
    ASSERTIONS.assertTrue(result["auto_resolvable"])
    ASSERTIONS.assertEqual(result["reason"], "safe_auto_resolve_candidate")
    evidence = result.get("evidence") if isinstance(result.get("evidence"), dict) else {}
    ASSERTIONS.assertEqual(
        cast(dict[str, Any], evidence).get("validated_paths"),
        ["scripts/a.py", "scripts/b.py"],
    )
    ASSERTIONS.assertEqual(result["path"], "multiple_files_validated")


def test_merge_conflict_multi_file_evidence_never_collapses_to_first_path(tmp_path, monkeypatch):
    """Success evidence must carry all validated files and avoid first-file-only path signals."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts" / "a.py").write_text(
        "<<<<<<< ours\nx=1\n=======\nx=2\n>>>>>>> theirs\n",
        encoding="utf-8",
    )
    (tmp_path / "scripts" / "b.py").write_text(
        "<<<<<<< ours\ny=1\n=======\ny=2\n>>>>>>> theirs\n",
        encoding="utf-8",
    )
    result = controller.classify_merge_conflict(
        {"mergeable": "CONFLICTING", "mergeStateStatus": "DIRTY"},
        ["scripts/a.py", "scripts/b.py"],
        {"files": ["scripts/a.py", "scripts/b.py"]},
    )
    ASSERTIONS.assertEqual(result["path"], "multiple_files_validated")
    ASSERTIONS.assertNotEqual(result["path"], "scripts/a.py")
    evidence = cast(dict[str, Any], result.get("evidence") or {})
    ASSERTIONS.assertEqual(evidence.get("validated_paths"), ["scripts/a.py", "scripts/b.py"])


def test_review_task_lines_include_only_unresolved_active_threads():
    """Resolved/outdated threads are excluded from active unresolved review task lines."""
    nodes = [
        _review_thread_node("active", overrides={"path": "a.py", "line": 10}),
        _review_thread_node("resolved", resolved=True, overrides={"path": "b.py", "line": 20}),
        _review_thread_node("outdated", outdated=True, overrides={"path": "c.py", "line": 30}),
    ]
    lines = "\n".join(controller._review_task_lines(nodes))  # pylint: disable=protected-access

    ASSERTIONS.assertIn("Thread active", lines)
    ASSERTIONS.assertNotIn("Thread resolved", lines)
    ASSERTIONS.assertNotIn("Thread outdated", lines)


def test_review_comment_summary_counts_active_and_ignored_threads():
    """Summary helper should count unresolved_active and ignored resolved/outdated threads."""
    if not hasattr(controller, "review_comments_summary"):
        raise NotImplementedError("review_comments_summary not implemented")
    summary = controller.review_comments_summary(
        [
            {"isResolved": False, "isOutdated": False},
            {"isResolved": True, "isOutdated": False},
            {"isResolved": False, "isOutdated": True},
        ]
    )
    ASSERTIONS.assertEqual(summary["unresolved_active"], 1)
    ASSERTIONS.assertEqual(summary["resolved_ignored"], 1)
    ASSERTIONS.assertEqual(summary["outdated_ignored"], 1)


def test_next_action_summary_contract():
    """A report helper should return exactly one final NEXT_ACTION from allowed values."""
    if not hasattr(controller, "summarize_next_action"):
        raise NotImplementedError("summarize_next_action not implemented")
    action = controller.summarize_next_action(_next_action_context())
    ASSERTIONS.assertIn(action, NEXT_ACTION_ALLOWED)


def _mock_codacy_evidence_helper(monkeypatch) -> None:
    monkeypatch.setattr(
        controller,
        "controller_codacy_blocking_evidence",
        lambda *_args: {
            "checks": [{"name": "Codacy Static Code Analysis", "state": "ACTION_REQUIRED"}],
            "check_blocking": True,
            "github_codacy_state": "ACTION_REQUIRED",
            "github_annotations": 0,
            "codacy_api_issues": 1,
            "issues": [{"filePath": "a.py", "patternId": "X"}],
            "api_available": True,
            "api_ok": True,
            "issues_returned": 1,
            "blocking": True,
            "ignored": False,
            "reason": "test",
        },
    )


def _mock_review_threads_helper(monkeypatch) -> None:
    monkeypatch.setattr(
        controller,
        "_review_threads_raw",
        lambda *_args: {
            "data": {
                "repository": {
                    "pullRequest": {
                        "reviewThreads": {"nodes": [{"isResolved": False, "isOutdated": False}]}
                    }
                }
            }
        },
    )


def _mock_next_action_helper(monkeypatch) -> None:
    _mock_codacy_evidence_helper(monkeypatch)
    _mock_review_threads_helper(monkeypatch)


def _mock_codacy_and_review_helpers(monkeypatch) -> None:
    _mock_next_action_helper(monkeypatch)


def _build_ctx_for_codacy_review_summary() -> controller.NextActionContext:
    decision = {"actions": [], "warnings": [], "errors": [], "pending_count": 0}
    pr = {
        "statusCheckRollup": [_check("Codacy Static Code Analysis", "ACTION_REQUIRED")],
        "headRefOid": "abc123",
        "mergeable": "MERGEABLE",
        "mergeStateStatus": "CLEAN",
    }
    return controller.build_next_action_context(_args(), decision, pr, ([], []))


def _assert_codacy_review_summary(ctx: controller.NextActionContext) -> None:
    ASSERTIONS.assertEqual(ctx.decision["codacy"]["classification"], "real_current_issues")
    ASSERTIONS.assertFalse(ctx.decision["codacy"]["ignored"])
    ASSERTIONS.assertEqual(ctx.decision["review"]["unresolved_active"], 1)
    ASSERTIONS.assertIn(ctx.decision["next_action_summary"], NEXT_ACTION_ALLOWED)


def test_build_next_action_context_wires_codacy_review_and_summary_helpers(monkeypatch):
    """Context builder should populate codacy classification, review summary, and next action summary."""
    _mock_codacy_and_review_helpers(monkeypatch)
    ctx = _build_ctx_for_codacy_review_summary()
    _assert_codacy_review_summary(ctx)


def test_build_next_action_context_passes_merge_readiness_fields_to_taxonomy(monkeypatch):
    """Taxonomy context should include merge readiness fields for empty-blocker routing."""
    _stub_codacy_head_preservation(monkeypatch)
    captured: dict[str, Any] = {}

    def _capture_summary(blockers: list[dict[str, Any]], context: dict[str, Any]) -> dict[str, Any]:
        captured["blockers"] = blockers
        captured["context"] = dict(context)
        return {
            "categories": [],
            "primary_category": "none",
            "next_action": "checks_green_or_no_action",
            "needs_manual": False,
            "safe_actions": [],
            "reasons": [],
        }

    monkeypatch.setattr(controller, "summarize_blocker_actions", _capture_summary)
    decision: dict[str, object] = {"actions": [], "warnings": [], "errors": []}
    pr = {
        "statusCheckRollup": [],
        "headRefOid": "pr-head-sha",
        "mergeable": "MERGEABLE",
        "mergeStateStatus": "CLEAN",
    }
    controller.build_next_action_context(_args(), decision, pr, ([], []))

    context = cast(dict[str, Any], captured["context"])
    ASSERTIONS.assertEqual(context["mergeable"], "MERGEABLE")
    ASSERTIONS.assertEqual(context["mergeStateStatus"], "CLEAN")
    ASSERTIONS.assertIn("can_merge", context)


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


def _review_thread_node(
    node_id: str,
    *,
    resolved: bool = False,
    outdated: bool = False,
    overrides: dict[str, object] | None = None,
) -> dict[str, object]:
    node: dict[str, object] = {
        "id": node_id,
        "isResolved": resolved,
        "isOutdated": outdated,
        "path": "a.py",
        "line": 1,
        "comments": {"nodes": []},
    }
    if overrides:
        node.update(overrides)
    return node


def test_automation_scope_safe_autofix_is_bounded_to_one_round():
    """Automation/controller/workflow PRs should remain bounded to one repair round."""
    args = _args()
    config = controller.safe_autofix_config_from_args(args)
    command = controller.safe_autofix_command(config)

    ASSERTIONS.assertIn("max_rounds=1", command)


def test_no_progress_repeated_blocker_signature_stops_with_needs_manual():
    """Repeated blocker signatures without improvement should stop with no_progress."""
    decision: dict = {
        "actions": [],
        "warnings": [],
        "errors": [],
        "previous_blocker_signature": controller.blocker_signature([_check("Unit tests", "FAILURE")]),
        "repeated_blocker_count": 1,
    }
    pr = {"statusCheckRollup": [_check("Unit tests", "FAILURE")]}
    ctx = controller.build_next_action_context(_args(), decision, pr, ([], []))
    controller.decide_next_action(ctx)

    ASSERTIONS.assertEqual(decision["next_action"], "needs_manual_no_progress")
    ASSERTIONS.assertEqual(decision["repeated_blocker_count"], 2)
    ASSERTIONS.assertEqual(decision["actions"], [])


def test_no_progress_non_repeated_blocker_signature_keeps_safe_autofix_launch(monkeypatch):
    """New blocker signature should preserve safe-autofix behavior."""
    _stub_codacy_evidence(monkeypatch, blocking=False, ignored=False, issues=0)
    monkeypatch.setattr(controller, "active_safe_autofix_runs", lambda _repo: [])

    decision: dict = {
        "actions": [],
        "warnings": [],
        "errors": [],
        "previous_blocker_signature": controller.blocker_signature([_check("Unit tests", "FAILURE")]),
        "repeated_blocker_count": 1,
    }
    pr = {"statusCheckRollup": [_check("Integration tests", "FAILURE")]}

    ctx = controller.build_next_action_context(_args(), decision, pr, ([], []))
    controller.decide_next_action(ctx)

    ASSERTIONS.assertEqual(decision["next_action"], "would_launch_safe_autofix")
    ASSERTIONS.assertEqual(decision["repeated_blocker_count"], 1)
    ASSERTIONS.assertTrue(decision["actions"])


def test_load_no_progress_state_reads_matching_file(tmp_path):
    """Load persisted no-progress state when repo and PR match."""
    path = tmp_path / "decision.json"
    payload = {
        "repo": "owner/repo",
        "pr": "225",
        "previous_blocker_signature": "sig-prev",
        "repeated_blocker_count": 2,
    }
    path.write_text(json.dumps(payload), encoding="utf-8")

    signature, count = controller._load_no_progress_state(  # pylint: disable=protected-access
        str(path),
        "owner/repo",
        "225",
    )

    ASSERTIONS.assertEqual(signature, "sig-prev")
    ASSERTIONS.assertEqual(count, 2)


def test_load_no_progress_state_ignores_repo_pr_mismatch(tmp_path):
    """Ignore persisted state when repo/PR do not match current context."""
    path = tmp_path / "decision.json"
    payload = {
        "repo": "owner/repo",
        "pr": "999",
        "blocker_signature": "sig",
        "repeated_blocker_count": 7,
    }
    path.write_text(json.dumps(payload), encoding="utf-8")

    signature, count = controller._load_no_progress_state(  # pylint: disable=protected-access
        str(path),
        "owner/repo",
        "225",
    )

    ASSERTIONS.assertEqual(signature, "")
    ASSERTIONS.assertEqual(count, 0)


def test_initial_decision_seeds_no_progress_state(tmp_path):
    """Seed initial decision from persisted no-progress blocker state."""
    path = tmp_path / "decision.json"
    payload = {
        "repo": "owner/repo",
        "pr": "225",
        "blocker_signature": "sig-current",
        "repeated_blocker_count": 3,
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    args = _args()
    args.output = str(path)

    decision = controller.initial_decision(args)

    ASSERTIONS.assertEqual(decision["previous_blocker_signature"], "sig-current")
    ASSERTIONS.assertEqual(decision["repeated_blocker_count"], 3)


def test_loading_missing_state_returns_safe_defaults(tmp_path):
    """Missing persisted automation state should normalize to safe defaults."""
    state = controller.load_pr_automation_state(str(tmp_path / "missing.json"), "owner/repo", "225")
    ASSERTIONS.assertEqual(state["repo"], "owner/repo")
    ASSERTIONS.assertEqual(state["pr"], "225")
    ASSERTIONS.assertEqual(state["controller_run_count"], 0)


def test_saving_loading_state_round_trips(tmp_path):
    """Saving/loading automation state should preserve normalized values."""
    path = tmp_path / "state.json"
    payload = controller.normalize_pr_automation_state({"head": "abc", "controller_run_count": 2}, "owner/repo", "225")
    controller.save_pr_automation_state(str(path), payload)
    loaded = controller.load_pr_automation_state(str(path), "owner/repo", "225")
    ASSERTIONS.assertEqual(loaded["head"], "abc")
    ASSERTIONS.assertEqual(loaded["controller_run_count"], 2)
    ASSERTIONS.assertEqual(loaded["active_final_micro_audit_path"], "")


def test_pr_budget_status_exhausts_with_default_limits():
    """Default budget limits must trigger exhausted state once thresholds are reached."""
    limits = controller._budget_limits()  # pylint: disable=protected-access
    status = controller.pr_budget_status({"autofix_commit_count": limits["max_autofix_commits_per_pr"]}, limits)

    ASSERTIONS.assertTrue(status["exhausted"])
    ASSERTIONS.assertEqual(status["next_action"], "needs_manual_budget_exhausted")


def test_pending_to_wait_pending():
    """Pending checks map to wait_pending action."""
    action = controller.summarize_next_action({"pending_count": 1})
    ASSERTIONS.assertEqual(action, "wait_pending")


def test_stale_checks_only_to_rerun_stale_checks():
    """Stale-only state should request stale check rerun."""
    action = controller.summarize_next_action({"has_stale_or_cancelled_rerun_state": True, "blockers_count": 0})
    ASSERTIONS.assertEqual(action, "rerun_stale_checks")


def test_codacy_real_issues_to_fix_codacy_current_issues():
    """Real Codacy issues should request Codacy-focused fixes."""
    action = controller.summarize_next_action({"codacy_classification": "real_current_issues"})
    ASSERTIONS.assertEqual(action, "fix_codacy_current_issues")


def test_api_zero_with_annotations_to_fix_github_codacy_annotations():
    """API/check mismatch should route to annotation fix action."""
    action = controller.summarize_next_action({"codacy_classification": "api_github_mismatch"})
    ASSERTIONS.assertEqual(action, "fix_github_codacy_annotations")


def test_same_blocker_repeated_to_needs_manual_no_progress():
    """Repeated blocker signals no-progress manual escalation."""
    action = controller.summarize_next_action({"same_blocker_repeated": True})
    ASSERTIONS.assertEqual(action, "needs_manual_no_progress")


def test_no_progress_to_needs_manual_no_progress():
    """Unchanged progress with blockers should escalate as no-progress."""
    action = controller.summarize_next_action({"progress": "unchanged", "blockers_count": 1})
    ASSERTIONS.assertEqual(action, "needs_manual_no_progress")


def test_regression_to_needs_manual_regression():
    """Regressed progress should escalate as manual regression."""
    action = controller.summarize_next_action({"progress": "regressed"})
    ASSERTIONS.assertEqual(action, "needs_manual_regression")


def test_budget_exhausted_to_needs_manual_budget_exhausted():
    """Exhausted budget should escalate with budget action."""
    action = controller.summarize_next_action({"budget_status": {"exhausted": True}})
    ASSERTIONS.assertEqual(action, "needs_manual_budget_exhausted")


def test_all_clean_plus_audit_present_to_run_final_micro_audit(tmp_path):
    """Clean decision with existing micro-audit file should trigger audit."""
    audit_path = tmp_path / "active-final-micro-audit.md"
    audit_path.write_text("audit", encoding="utf-8")
    decision = {
        "pending": [],
        "bad": [],
        "unresolved_active": 0,
        "codacy_classification": "none",
        "mergeable": "MERGEABLE",
        "mergeStateStatus": "CLEAN",
    }
    state = {"active_final_micro_audit_path": str(audit_path)}
    ASSERTIONS.assertTrue(controller.should_run_final_micro_audit(decision, state))
    action = controller.summarize_next_action({"run_final_micro_audit": True})
    ASSERTIONS.assertEqual(action, "run_final_micro_audit")


def test_audit_pass_state_to_ready_to_merge():
    """Audit pass should move to ready_to_merge action."""
    action = controller.summarize_next_action({"audit_passed": True})
    ASSERTIONS.assertEqual(action, "ready_to_merge")


def test_missing_active_final_micro_audit_path_disables_final_micro_audit():
    """Missing audit path in state should disable final micro-audit run."""
    decision = {
        "pending": [],
        "bad": [],
        "unresolved_active": 0,
        "codacy_classification": "none",
        "mergeable": "MERGEABLE",
        "mergeStateStatus": "CLEAN",
    }
    ASSERTIONS.assertFalse(controller.should_run_final_micro_audit(decision, {}))


def test_empty_active_final_micro_audit_path_disables_final_micro_audit():
    """Empty audit path in state should disable final micro-audit run."""
    decision = {
        "pending": [],
        "bad": [],
        "unresolved_active": 0,
        "codacy_classification": "none",
        "mergeable": "MERGEABLE",
        "mergeStateStatus": "CLEAN",
    }
    ASSERTIONS.assertFalse(
        controller.should_run_final_micro_audit(decision, {"active_final_micro_audit_path": "   "})
    )


def _active_input(task: str, audit: str, branch: str = "b1", pr: str = "225") -> controller.ActiveTaskContextInput:
    """Build active task context input for replacement tests."""
    return controller.ActiveTaskContextInput(task_text=task, audit_text=audit, branch=branch, pr=pr)


def test_new_task_id_archives_previous_active_context(tmp_path):
    """Replacing active task with a new ID should archive prior command context."""
    ctx = tmp_path / "context"
    controller.replace_active_task_context(str(ctx), _active_input("task-a", "audit-a"))
    controller.replace_active_task_context(str(ctx), _active_input("task-b", "audit-b"))
    history = list((ctx / "history").glob("*-task-command.md"))
    ASSERTIONS.assertTrue(history)
    ASSERTIONS.assertEqual((ctx / "active-task-command.md").read_text(encoding="utf-8"), "task-b")


def test_same_task_id_preserves_current_state(tmp_path):
    """Same task ID should keep active state and skip history archiving."""
    ctx = tmp_path / "context"
    controller.replace_active_task_context(str(ctx), _active_input("task-a", "audit-a"))
    first_state = json.loads((ctx / "active-task-state.json").read_text(encoding="utf-8"))
    second_state = controller.replace_active_task_context(str(ctx), _active_input("task-a", "audit-a"))
    ASSERTIONS.assertEqual(first_state["task_id"], second_state["task_id"])
    ASSERTIONS.assertFalse((ctx / "history").exists())


def test_active_micro_audit_always_matches_active_task(tmp_path):
    """Active task and micro-audit files should stay in sync."""
    ctx = tmp_path / "context"
    controller.replace_active_task_context(str(ctx), _active_input("task-c", "audit-c", branch="b2"))
    ASSERTIONS.assertEqual((ctx / "active-task-command.md").read_text(encoding="utf-8"), "task-c")
    ASSERTIONS.assertEqual((ctx / "active-final-micro-audit.md").read_text(encoding="utf-8"), "audit-c")


def test_replace_active_task_context_persists_active_final_micro_audit_path(tmp_path):
    """Active task state should persist the active final micro-audit path."""
    ctx = tmp_path / "context"
    state = controller.replace_active_task_context(str(ctx), _active_input("task-c", "audit-c", branch="b2"))

    ASSERTIONS.assertEqual(state["active_final_micro_audit_path"], str(ctx / "active-final-micro-audit.md"))
    persisted = json.loads((ctx / "active-task-state.json").read_text(encoding="utf-8"))
    ASSERTIONS.assertEqual(persisted["active_final_micro_audit_path"], str(ctx / "active-final-micro-audit.md"))


def test_should_run_final_micro_audit_with_state_from_replace_active_task_context(tmp_path):
    """Final micro-audit should activate from state produced by active task replacement."""
    ctx = tmp_path / "context"
    state = controller.replace_active_task_context(str(ctx), _active_input("task-c", "audit-c", branch="b2"))
    decision = {
        "pending": [],
        "bad": [],
        "unresolved_active": 0,
        "codacy_classification": "none",
        "mergeable": "MERGEABLE",
        "mergeStateStatus": "CLEAN",
    }

    ASSERTIONS.assertTrue(controller.should_run_final_micro_audit(decision, state))


def test_controller_does_not_read_history_for_active_decisions(tmp_path):
    """History files should not override the active-task state file."""
    ctx = tmp_path / "context"
    controller.replace_active_task_context(str(ctx), _active_input("task-a", "audit-a"))
    (ctx / "history").mkdir(parents=True, exist_ok=True)
    (ctx / "history" / "junk-task-state.json").write_text('{"task_id":"x"}', encoding="utf-8")
    state = controller.replace_active_task_context(str(ctx), _active_input("task-a", "audit-a"))
    active_state = json.loads((ctx / "active-task-state.json").read_text(encoding="utf-8"))
    ASSERTIONS.assertEqual(state["task_id"], active_state["task_id"])


def test_archived_active_task_state_is_written_once(tmp_path):
    """Archive should keep one task-state snapshot and not overwrite it twice."""
    ctx = tmp_path / "context"
    controller.replace_active_task_context(str(ctx), _active_input("task-a", "audit-a"))
    controller.replace_active_task_context(str(ctx), _active_input("task-b", "audit-b"))
    archived_states = list((ctx / "history").glob("*-task-state.json"))
    ASSERTIONS.assertEqual(len(archived_states), 1)
    archived = json.loads(archived_states[0].read_text(encoding="utf-8"))
    ASSERTIONS.assertEqual(archived["task_id"], controller.compute_task_id("task-a", "audit-a", "b1", "225"))


def test_budget_and_progress_helpers_feed_decision_summary(monkeypatch, tmp_path):
    """Controller flow should include progress and budget status in decision output."""
    decision, pr, ctx = _prepare_decision_tracking_test(monkeypatch, tmp_path)
    controller.update_decision_state_tracking(_args(), pr, ctx)
    ASSERTIONS.assertIn("budget_status", decision)
    ASSERTIONS.assertIn("progress", decision)
    ASSERTIONS.assertIn("pr_automation_state", decision)


def test_add_controller_core_args_supports_task_context_options():
    """Core parser should expose task/audit context options with expected defaults."""
    parser = argparse.ArgumentParser()
    controller.add_controller_core_args(parser)

    parsed = parser.parse_args(["--repo", "owner/repo", "--pr", "229"])

    ASSERTIONS.assertEqual(parsed.task_command_file, "")
    ASSERTIONS.assertEqual(parsed.final_micro_audit_file, "")
    ASSERTIONS.assertEqual(parsed.task_context_dir, ".autofix/context")


def test_merge_active_task_context_if_present_replaces_and_merges_state(tmp_path):
    """Providing both files should replace active context and merge returned state into decision."""
    args, decision, context_dir = _active_context_test_inputs(tmp_path)

    controller.merge_active_task_context_if_present(args, decision, {"headRefName": "feature/pr229"})

    _assert_active_context_merge(decision, context_dir)


def test_merge_active_task_context_if_present_without_inputs_is_noop(tmp_path):
    """Missing task/audit args should keep existing decision and avoid writes."""
    args = argparse.Namespace(
        pr="229",
        task_command_file="",
        final_micro_audit_file="",
        task_context_dir=str(tmp_path / "ctx"),
    )
    decision: dict[str, Any] = {"pr_automation_state": {"controller_run_count": 4}}
    original = copy.deepcopy(decision)

    controller.merge_active_task_context_if_present(args, decision, {"headRefName": "feature/pr229"})

    ASSERTIONS.assertEqual(decision, original)
    ASSERTIONS.assertFalse((tmp_path / "ctx").exists())


def test_set_no_launch_next_action_prefers_final_micro_audit_when_clean(tmp_path):
    """Clean non-blocked decision should schedule final micro-audit when active audit exists."""
    audit_path = tmp_path / "active-final-micro-audit.md"
    audit_path.write_text("audit", encoding="utf-8")
    decision: dict[str, Any] = {
        "pending": [],
        "bad": [],
        "pending_count": 0,
        "unresolved_active": 0,
        "codacy_classification": "none",
        "mergeable": "MERGEABLE",
        "mergeStateStatus": "CLEAN",
        "pr_automation_state": {"active_final_micro_audit_path": str(audit_path)},
    }

    controller.set_no_launch_next_action(decision, [])

    ASSERTIONS.assertEqual(decision["next_action"], "run_final_micro_audit")


def test_set_no_launch_next_action_missing_audit_path_keeps_green_fallback():
    """Missing/empty active audit path should not trigger final micro-audit action."""
    decision: dict[str, Any] = {
        "pending": [],
        "bad": [],
        "pending_count": 0,
        "unresolved_active": 0,
        "codacy_classification": "none",
        "mergeable": "MERGEABLE",
        "mergeStateStatus": "CLEAN",
        "pr_automation_state": {"active_final_micro_audit_path": "   "},
    }

    controller.set_no_launch_next_action(decision, [])

    ASSERTIONS.assertEqual(decision["next_action"], "checks_green_or_no_action")


def test_parse_phase0_preflight_result_json_pass_missing_plan_fails_closed():
    """JSON PASS without implementation_plan must fail closed."""
    payload = _phase0_pass_payload()
    payload.pop("implementation_plan")
    report = controller.parse_phase0_preflight_result(json.dumps(payload))
    ASSERTIONS.assertEqual(report["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(report["next_action"], "needs_manual_phase0_failed")


def test_parse_phase0_preflight_result_text_pass_missing_plan_fails_closed():
    """Text PASS without implementation_plan must fail closed."""
    report = controller.parse_phase0_preflight_result(
        "\n".join(
            [
                "PHASE_0_PREFLIGHT=PASS",
                "risk_level: low",
                "next_action: proceed_with_narrow_patch",
                "files_inspected: scripts/pr_automation_controller.py",
                "static_analysis_rules: ruff",
                "workflows_affected: pr-automation-controller-v2",
                "authoritative_modules: scripts/pr_automation_controller.py",
                "dangerous_gates: phase0 gate",
                "tests_to_run: pytest",
                "stop_conditions: scope violation",
            ]
        )
    )
    ASSERTIONS.assertEqual(report["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(report["next_action"], "needs_manual_phase0_failed")


def test_build_codex_patch_task_after_phase0_blocks_pass_without_plan():
    """Patch task generation must stay blocked when PASS lacks implementation_plan."""
    result = controller.build_codex_patch_task_after_phase0(
        "implement narrow fix",
        {"status": "PASS", "next_action": "generate_patch_prompt"},
    )
    ASSERTIONS.assertFalse(result["can_patch"])
    ASSERTIONS.assertTrue(result["blocked"])
    ASSERTIONS.assertEqual(result["task"], "")
    ASSERTIONS.assertEqual(result["patch_task"], "")


def test_parse_phase0_preflight_result_json_unknown_action_fails_closed():
    """JSON PASS with unsupported next_action must fail closed."""
    payload = _phase0_pass_payload()
    payload["next_action"] = "ship_it_now"
    report = controller.parse_phase0_preflight_result(json.dumps(payload))
    ASSERTIONS.assertEqual(report["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(report["next_action"], "needs_manual_phase0_failed")


def test_parse_phase0_preflight_result_text_unknown_action_fails_closed():
    """Text PASS with unsupported next_action must fail closed."""
    report = controller.parse_phase0_preflight_result(
        "\n".join(
            [
                "PHASE_0_PREFLIGHT=PASS",
                "risk_level: low",
                "next_action: ship_it_now",
                "files_inspected: scripts/pr_automation_controller.py",
                "static_analysis_rules: ruff",
                "workflows_affected: pr-automation-controller-v2",
                "authoritative_modules: scripts/pr_automation_controller.py",
                "dangerous_gates: phase0 gate",
                "implementation_plan: keep patch scoped",
                "tests_to_run: pytest",
                "stop_conditions: scope violation",
            ]
        )
    )
    ASSERTIONS.assertEqual(report["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(report["next_action"], "needs_manual_phase0_failed")


def test_parse_phase0_preflight_result_json_unknown_action_variant_fails_closed():
    """Unsupported next_action variants must fail closed after normalization."""
    payload = _phase0_pass_payload()
    payload["next_action"] = "Ship-It-Now"
    report = controller.parse_phase0_preflight_result(json.dumps(payload))
    ASSERTIONS.assertEqual(report["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(report["next_action"], "needs_manual_phase0_failed")


def test_parse_phase0_preflight_result_tries_later_fenced_json_candidate():
    """Invalid earlier fence should not block a later valid Phase 0 JSON fence."""
    payload = _phase0_pass_payload()
    raw = "\n".join(
        [
            "preflight result follows",
            "```",
            "{not-json}",
            "```",
            "```json",
            json.dumps(payload),
            "```",
        ]
    )
    report = controller.parse_phase0_preflight_result(raw)
    ASSERTIONS.assertEqual(report["status"], "PASS")
    ASSERTIONS.assertEqual(report["next_action"], "generate_patch_prompt")


def test_parse_phase0_preflight_result_tries_later_inline_json_candidate():
    """Invalid earlier brace block should not block a later valid inline JSON payload."""
    payload = _phase0_pass_payload()
    raw = "ignore this {not-json} and use this " + json.dumps(payload)
    report = controller.parse_phase0_preflight_result(raw)
    ASSERTIONS.assertEqual(report["status"], "PASS")
    ASSERTIONS.assertEqual(report["next_action"], "generate_patch_prompt")


def test_parse_phase0_preflight_result_skips_non_dict_json_candidate():
    """A valid non-dict JSON candidate should be skipped in favor of a later dict."""
    payload = _phase0_pass_payload()
    raw = "\n".join(["```json", "[1, 2, 3]", "```", "```json", json.dumps(payload), "```"])
    report = controller.parse_phase0_preflight_result(raw)
    ASSERTIONS.assertEqual(report["status"], "PASS")
    ASSERTIONS.assertEqual(report["next_action"], "generate_patch_prompt")


def test_parse_phase0_preflight_result_all_json_candidates_invalid_fails_closed():
    """All malformed JSON candidates should preserve fail-closed malformed behavior."""
    report = controller.parse_phase0_preflight_result("```json\n{not-json}\n```\nplain text")
    ASSERTIONS.assertEqual(report["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(report["next_action"], "needs_manual_phase0_malformed")


def test_parse_phase0_preflight_result_fenced_json_with_braces_in_string():
    """Fenced JSON should parse even when JSON string values contain braces."""
    payload = _phase0_pass_payload()
    payload["implementation_plan"] = ["handle string with braces {x} safely"]
    raw = "\n".join(
        [
            "preflight result follows",
            "```",
            "{not-json}",
            "```",
            "```json",
            json.dumps(payload),
            "```",
        ]
    )
    report = controller.parse_phase0_preflight_result(raw)
    ASSERTIONS.assertEqual(report["status"], "PASS")
    ASSERTIONS.assertEqual(report["next_action"], "generate_patch_prompt")
    ASSERTIONS.assertEqual(report["implementation_plan"], ["handle string with braces {x} safely"])


def test_decide_phase0_gate_returns_canonical_generate_patch_action():
    """PASS gate should return canonical generate_patch_prompt action."""
    payload = _phase0_pass_payload()
    payload["next_action"] = "Generate Patch Prompt"
    decision = controller.decide_phase0_gate(payload)
    ASSERTIONS.assertTrue(decision["can_patch"])
    ASSERTIONS.assertEqual(decision["next_action"], "generate_patch_prompt")


def test_decide_phase0_gate_returns_canonical_proceed_action():
    """PASS gate should return canonical proceed_with_narrow_patch action."""
    payload = _phase0_pass_payload()
    payload["next_action"] = "Proceed With Narrow Patch"
    decision = controller.decide_phase0_gate(payload)
    ASSERTIONS.assertTrue(decision["can_patch"])
    ASSERTIONS.assertEqual(decision["next_action"], "proceed_with_narrow_patch")


def test_build_codex_patch_task_after_phase0_returns_canonical_action():
    """Patch task wrapper should expose canonical Phase 0 next_action."""
    payload = _phase0_pass_payload()
    payload["next_action"] = "Generate Patch Prompt"
    result = controller.build_codex_patch_task_after_phase0("implement narrow fix", payload)
    ASSERTIONS.assertTrue(result["can_patch"])
    ASSERTIONS.assertEqual(result["next_action"], "generate_patch_prompt")
    ASSERTIONS.assertEqual(result["gate"]["next_action"], "generate_patch_prompt")


def test_parse_phase0_preflight_result_skips_wrapper_json_before_valid_report():
    """Parser should skip wrapper metadata JSON before a later valid Phase 0 report."""
    payload = _phase0_pass_payload()
    raw = "\n".join(
        [
            "wrapper metadata",
            '{"status": "ok", "kind": "metadata"}',
            "```json",
            json.dumps(payload),
            "```",
        ]
    )
    report = controller.parse_phase0_preflight_result(raw)
    ASSERTIONS.assertEqual(report["status"], "PASS")
    ASSERTIONS.assertEqual(report["next_action"], "generate_patch_prompt")


def test_decide_phase0_gate_blocked_manual_never_returns_patch_action():
    """Blocked Phase 0 gate must not return a patch-generation next_action."""
    decision = controller.decide_phase0_gate(
        {"status": "NEEDS_MANUAL", "next_action": "generate_patch_prompt"}
    )
    ASSERTIONS.assertFalse(decision["can_patch"])
    ASSERTIONS.assertTrue(decision["needs_manual"])
    ASSERTIONS.assertEqual(decision["next_action"], "needs_manual_phase0_failed")


def test_build_codex_patch_task_blocked_manual_uses_manual_next_action():
    """Blocked patch task wrapper should expose a manual next_action."""
    result = controller.build_codex_patch_task_after_phase0(
        "implement narrow fix",
        {"status": "NEEDS_MANUAL", "next_action": "generate_patch_prompt"},
    )
    ASSERTIONS.assertFalse(result["can_patch"])
    ASSERTIONS.assertTrue(result["blocked"])
    ASSERTIONS.assertEqual(result["next_action"], "needs_manual_phase0_failed")
    ASSERTIONS.assertEqual(result["task"], "")


def test_parse_phase0_preflight_result_skips_failed_wrapper_before_valid_pass():
    """Generic failed wrapper metadata should not block a later valid Phase 0 PASS."""
    payload = _phase0_pass_payload()
    raw = "\n".join(
        [
            '{"status": "failed", "kind": "metadata"}',
            "```json",
            json.dumps(payload),
            "```",
        ]
    )
    report = controller.parse_phase0_preflight_result(raw)
    ASSERTIONS.assertEqual(report["status"], "PASS")
    ASSERTIONS.assertEqual(report["next_action"], "generate_patch_prompt")


def test_parse_phase0_preflight_result_skips_generic_manual_wrapper_before_valid_pass():
    """Generic NEEDS_MANUAL wrapper metadata should not override later Phase 0 PASS."""
    payload = _phase0_pass_payload()
    raw = "\n".join(
        [
            '{"status": "NEEDS_MANUAL", "kind": "metadata"}',
            "```json",
            json.dumps(payload),
            "```",
        ]
    )
    report = controller.parse_phase0_preflight_result(raw)
    ASSERTIONS.assertEqual(report["status"], "PASS")
    ASSERTIONS.assertEqual(report["next_action"], "generate_patch_prompt")


def _review_thread(
    *,
    thread_id: str = "T1",
    author: str = "coderabbitai[bot]",
    body: str = "nit: style tweak",
    path: str = "scripts/pr_automation_controller.py",
    is_resolved: bool = False,
    is_outdated: bool = False,
) -> dict[str, Any]:
    return {
        "id": thread_id,
        "isResolved": is_resolved,
        "isOutdated": is_outdated,
        "path": path,
        "comments": {"nodes": [{"author": {"login": author}, "body": body}]},
    }


def test_review_provider_presence_missing_provider_does_not_block():
    status = controller.review_provider_presence_status([_review_thread(author="coderabbitai[bot]")])
    ASSERTIONS.assertFalse(status["blocking"])
    ASSERTIONS.assertEqual(status["next_action"], "continue_checks")
    ASSERTIONS.assertIn("greptile", status["missing_providers"])


def test_review_thread_author_handles_comments_nodes_with_none_and_non_dict():
    classified = controller.classify_review_thread(
        {
            "comments": {
                "nodes": [
                    None,
                    "bad",
                    {"author": {"login": "coderabbitai[bot]"}},
                ],
            }
        }
    )
    ASSERTIONS.assertEqual(classified["author"], "coderabbitai")
    ASSERTIONS.assertEqual(classified["provider"], "coderabbitai")


def test_review_thread_author_handles_comments_list_with_none_and_non_dict():
    classified = controller.classify_review_thread(
        {
            "comments": [
                None,
                "bad",
                {"author": {"login": "sourcery-ai[bot]"}},
            ]
        }
    )
    ASSERTIONS.assertEqual(classified["author"], "sourcery-ai")
    ASSERTIONS.assertEqual(classified["provider"], "sourcery-ai")


def test_review_provider_presence_no_comments_does_not_block():
    status = controller.review_provider_presence_status([])
    ASSERTIONS.assertFalse(status["blocking"])
    ASSERTIONS.assertEqual(status["present_providers"], [])
    ASSERTIONS.assertEqual(status["next_action"], "continue_checks")


def test_review_provider_presence_missing_specific_providers_never_block():
    status = controller.review_provider_presence_status([_review_thread(author="chatgpt-codex-connector[bot]")])
    ASSERTIONS.assertFalse(status["blocking"])
    for provider in ("greptile", "coderabbitai", "sourcery-ai", "qodo-code-review", "codacy-production"):
        ASSERTIONS.assertIn(provider, status["missing_providers"])


def test_review_classification_active_p1_and_high_are_blocking():
    p1 = controller.classify_review_thread(_review_thread(body="P1 bug correctness issue"))
    high = controller.classify_review_thread(_review_thread(body="HIGH security runtime risk", thread_id="T2"))
    fail_open = controller.classify_review_thread(
        _review_thread(body="FAIL-OPEN security path remains", thread_id="T3")
    )
    ASSERTIONS.assertEqual(p1["classification"], "blocking")
    ASSERTIONS.assertEqual(high["classification"], "blocking")
    ASSERTIONS.assertEqual(fail_open["classification"], "blocking")


def test_review_classification_uses_later_comment_when_first_is_irrelevant():
    thread = {
        "id": "T-later",
        "isResolved": False,
        "isOutdated": False,
        "path": "scripts/pr_automation_controller.py",
        "comments": {
            "nodes": [
                {"author": {"login": "coderabbitai[bot]"}, "body": "looks good"},
                {"author": {"login": "coderabbitai[bot]"}, "body": "action required: bug in runtime path"},
            ]
        },
    }
    classified = controller.classify_review_thread(thread)
    ASSERTIONS.assertEqual(classified["classification"], "blocking")
    ASSERTIONS.assertEqual(classified["actionability"], "fix_required")


def test_review_severity_medium_risk_and_standalone_medium_classify_medium():
    medium_risk = controller.classify_review_comment_severity(
        _review_thread(body="This is medium risk")
    )
    standalone_medium = controller.classify_review_comment_severity(
        _review_thread(body="severity: medium")
    )
    ASSERTIONS.assertEqual(medium_risk, "medium")
    ASSERTIONS.assertEqual(standalone_medium, "medium")


def test_review_actionability_failing_test_and_missing_test_are_blocking_test_required():
    failing = controller.classify_review_thread(_review_thread(body="failing test on this path"))
    missing = controller.classify_review_thread(_review_thread(body="missing test coverage"))
    ASSERTIONS.assertEqual(failing["classification"], "blocking")
    ASSERTIONS.assertEqual(failing["actionability"], "test_required")
    ASSERTIONS.assertEqual(missing["classification"], "blocking")
    ASSERTIONS.assertEqual(missing["actionability"], "test_required")


def test_review_high_word_boundaries_and_unknown_defaults():
    highly = controller.classify_review_thread(_review_thread(body="this is highly recommended"))
    highlight = controller.classify_review_thread(_review_thread(body="please highlight docs"))
    unknown = controller.classify_review_thread(_review_thread(body="consider revisiting this section"))
    ASSERTIONS.assertEqual(highly["severity"], "unknown")
    ASSERTIONS.assertEqual(highlight["severity"], "unknown")
    ASSERTIONS.assertEqual(unknown["severity"], "unknown")
    ASSERTIONS.assertEqual(unknown["actionability"], "needs_manual")


def test_review_keyword_boundaries_for_p1_p2_bug():
    p1 = controller.classify_review_comment_severity(_review_thread(body="P1 issue"))
    step1 = controller.classify_review_comment_severity(_review_thread(body="step1 update"))
    p2 = controller.classify_review_comment_severity(_review_thread(body="P2 polish"))
    step2 = controller.classify_review_comment_severity(_review_thread(body="step2 update"))
    bug = controller.classify_review_comment_actionability(_review_thread(body="bug in parser"))
    debug = controller.classify_review_comment_actionability(_review_thread(body="debug logging only"))
    ASSERTIONS.assertEqual(p1, "p1")
    ASSERTIONS.assertEqual(step1, "unknown")
    ASSERTIONS.assertEqual(p2, "p2")
    ASSERTIONS.assertEqual(step2, "unknown")
    ASSERTIONS.assertEqual(bug, "fix_required")
    ASSERTIONS.assertEqual(debug, "needs_manual")


def test_review_p2_style_suggestion_is_advisory_non_blocking():
    classified = controller.classify_review_thread(_review_thread(body="P2 style suggestion only"))
    ASSERTIONS.assertEqual(classified["severity"], "p2")
    ASSERTIONS.assertEqual(classified["actionability"], "advisory")
    ASSERTIONS.assertEqual(classified["classification"], "advisory")
    ASSERTIONS.assertFalse(classified["blocking"])


def test_review_p2_missing_test_stays_blocking_by_actionability():
    classified = controller.classify_review_thread(_review_thread(body="P2 missing test coverage"))
    ASSERTIONS.assertEqual(classified["severity"], "p2")
    ASSERTIONS.assertEqual(classified["actionability"], "test_required")
    ASSERTIONS.assertEqual(classified["classification"], "blocking")
    ASSERTIONS.assertTrue(classified["blocking"])


def test_review_classification_low_nit_is_advisory_and_not_safe_without_evidence():
    thread = _review_thread(body="low nitpick style suggestion docs")
    classified = controller.classify_review_thread(thread)
    ASSERTIONS.assertEqual(classified["classification"], "advisory")
    ASSERTIONS.assertFalse(controller.should_resolve_review_thread(thread, {}))


def test_review_stale_fixed_thread_with_evidence_is_safe_to_resolve():
    thread = _review_thread(body="stale style", thread_id="T3")
    evidence = {
        "safe_to_resolve": True,
        "issue_fixed_or_stale": True,
        "head_matches": True,
        "current_head_sha": "abc",
        "evidence_head_sha": "abc",
        "validation_passed": True,
        "checks_green": True,
        "pending_checks": False,
        "failing_checks": False,
        "tests": ["pytest"],
        "reply_body": "Fixed in latest patch.",
    }
    ASSERTIONS.assertTrue(controller.should_resolve_review_thread(thread, evidence))


def test_review_unknown_author_routes_needs_manual():
    classified = controller.classify_review_thread(_review_thread(author="mystery-user", body="please revisit"))
    ASSERTIONS.assertEqual(classified["classification"], "needs_manual")


def test_review_unknown_author_with_strict_generic_evidence_does_not_auto_resolve():
    thread = _review_thread(author="human-reviewer", body="already fixed stale")
    evidence = {
        "safe_to_resolve": True,
        "issue_fixed_or_stale": True,
        "head_matches": True,
        "current_head_sha": "abc",
        "evidence_head_sha": "abc",
        "validation_passed": True,
        "checks_green": True,
        "pending_checks": False,
        "failing_checks": False,
        "tests": ["pytest"],
    }
    triage = controller.triage_review_thread_contract(thread, evidence)
    ASSERTIONS.assertEqual(triage["decision"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(triage["reason"], "unknown_review_provider")
    ASSERTIONS.assertFalse(controller.should_resolve_review_thread(thread, evidence))


def test_review_unknown_author_active_failure_language_routes_patch_required():
    evidence = {
        "current_head_sha": "abc",
        "evidence_head_sha": "abc",
        "checks_green": True,
        "pending_checks": False,
        "failing_checks": False,
    }
    bodies = (
        "not already fixed; the failing behavior is still present",
        "this is failing on current head",
        "security bypass still present",
        "broken correctness failure in guard",
    )
    for body in bodies:
        triage = controller.triage_review_thread_contract(
            _review_thread(author="human-reviewer", body=body),
            evidence,
        )
        ASSERTIONS.assertEqual(triage["decision"], "PATCH_REQUIRED")
        ASSERTIONS.assertEqual(triage["next_action"], "patch_required")


def test_review_out_of_scope_routes_needs_manual():
    classified = controller.classify_review_thread(
        _review_thread(path="business/core/runtime.py", body="P2 fix"),
        {"files_allowed": ["scripts/pr_automation_controller.py"]},
    )
    ASSERTIONS.assertEqual(classified["classification"], "needs_manual")


def test_review_codacy_safe_resolve_requires_success_and_zero_annotations():
    thread = _review_thread(author="codacy-production[bot]", body="stale")
    bad = {
        "safe_to_resolve": True,
        "issue_fixed_or_stale": True,
        "head_matches": True,
        "current_head_sha": "abc",
        "evidence_head_sha": "abc",
        "validation_passed": True,
        "checks_green": True,
        "pending_checks": False,
        "failing_checks": False,
        "codacy_relevant": True,
        "codacy_state": "ACTION_REQUIRED",
        "codacy_annotations_count": 1,
        "tests": ["pytest"],
        "reply_body": "stale",
    }
    good = dict(bad) | {"codacy_state": "SUCCESS", "codacy_annotations_count": 0}
    ASSERTIONS.assertFalse(controller.should_resolve_review_thread(thread, bad))
    ASSERTIONS.assertTrue(controller.should_resolve_review_thread(thread, good))


def test_review_codacy_aliases_require_codacy_specific_green_evidence_for_triage():
    evidence = {
        "safe_to_resolve": True,
        "issue_fixed_or_stale": True,
        "head_matches": True,
        "current_head_sha": "abc",
        "evidence_head_sha": "abc",
        "validation_passed": True,
        "checks_green": True,
        "pending_checks": False,
        "failing_checks": False,
        "tests": ["pytest"],
    }
    for author in ("codacy", "codacy[bot]", "codacy-production", "codacy-production[bot]"):
        thread = _review_thread(author=author, body="already fixed stale")
        triage = controller.triage_review_thread_contract(thread, evidence)
        ASSERTIONS.assertEqual(triage["provider"], "codacy-production")
        ASSERTIONS.assertEqual(triage["decision"], "NEEDS_MANUAL")
        ASSERTIONS.assertEqual(triage["reason"], "missing_or_blocking_codacy_evidence")
        ASSERTIONS.assertFalse(controller.should_resolve_review_thread(thread, evidence))


def test_review_codacy_alias_action_required_or_annotations_blocks_evidence_resolve():
    thread = _review_thread(author="codacy", body="already fixed stale")
    base = {
        "safe_to_resolve": True,
        "issue_fixed_or_stale": True,
        "head_matches": True,
        "current_head_sha": "abc",
        "evidence_head_sha": "abc",
        "validation_passed": True,
        "checks_green": True,
        "pending_checks": False,
        "failing_checks": False,
        "tests": ["pytest"],
    }
    action_required = dict(base) | {"codacy_conclusion": "action_required", "annotations_count": 0}
    annotations = dict(base) | {"codacy_conclusion": "success", "annotations_count": 1}
    ASSERTIONS.assertNotEqual(
        controller.triage_review_thread_contract(thread, action_required)["decision"], "EVIDENCE_RESOLVE"
    )
    ASSERTIONS.assertNotEqual(
        controller.triage_review_thread_contract(thread, annotations)["decision"], "EVIDENCE_RESOLVE"
    )
    ASSERTIONS.assertFalse(controller.should_resolve_review_thread(thread, action_required))
    ASSERTIONS.assertFalse(controller.should_resolve_review_thread(thread, annotations))


def test_review_codacy_alias_success_zero_annotations_allows_evidence_resolve():
    thread = _review_thread(author="codacy", body="already fixed stale")
    evidence = {
        "safe_to_resolve": True,
        "issue_fixed_or_stale": True,
        "head_matches": True,
        "current_head_sha": "abc",
        "evidence_head_sha": "abc",
        "validation_passed": True,
        "checks_green": True,
        "pending_checks": False,
        "failing_checks": False,
        "codacy_conclusion": "success",
        "annotations_count": 0,
        "tests": ["pytest"],
    }
    ASSERTIONS.assertEqual(controller.triage_review_thread_contract(thread, evidence)["decision"], "EVIDENCE_RESOLVE")
    ASSERTIONS.assertTrue(controller.should_resolve_review_thread(thread, evidence))


def test_review_codacy_safe_resolve_non_numeric_annotations_fails_safe_without_exception():
    thread = _review_thread(author="codacy-production[bot]", body="stale")
    unsafe = {
        "safe_to_resolve": True,
        "issue_fixed_or_stale": True,
        "head_matches": True,
                        "validation_passed": True,
                        "checks_green": True,
                        "pending_checks": False,
                        "failing_checks": False,
        "codacy_relevant": True,
        "codacy_state": "SUCCESS",
        "codacy_annotations_count": "n/a",
    }
    ASSERTIONS.assertFalse(controller.should_resolve_review_thread(thread, unsafe))


def test_review_resolution_plan_includes_reply_body_and_missing_providers_not_blocking():
    thread = _review_thread(thread_id="T9", body="stale advisory style")
    plan = controller.build_review_thread_resolution_plan(
        [thread],
        {
            "resolution_evidence": {
                "T9": {
                    "safe_to_resolve": True,
                    "issue_fixed_or_stale": True,
                    "head_matches": True,
                    "current_head_sha": "abc",
                    "evidence_head_sha": "abc",
                    "validation_passed": True,
                    "checks_green": True,
                    "pending_checks": False,
                    "failing_checks": False,
                    "tests": ["pytest"],
                    "reply_body": "Addressed in current head.",
                }
            }
        },
    )
    ASSERTIONS.assertFalse(plan["missing_providers_blocking"])
    ASSERTIONS.assertEqual(plan["items"][0]["reply_body"], "Addressed in current head.")


def test_review_head_mismatch_pending_and_failing_checks_block_auto_resolve():
    thread = _review_thread(thread_id="T10", body="low style")
    base = {
        "safe_to_resolve": True,
        "issue_fixed_or_stale": True,
        "head_matches": True,
        "validation_passed": True,
        "pending_checks": False,
        "failing_checks": False,
        "reply_body": "fixed",
    }
    ASSERTIONS.assertFalse(controller.should_resolve_review_thread(thread, dict(base) | {"head_matches": False}))
    ASSERTIONS.assertFalse(controller.should_resolve_review_thread(thread, dict(base) | {"pending_checks": True}))
    ASSERTIONS.assertFalse(controller.should_resolve_review_thread(thread, dict(base) | {"failing_checks": True}))


def test_summarize_review_threads_mixed_blocking_advisory_and_manual():
    items = [
        _review_thread(thread_id="B", body="P1 bug in runtime"),
        _review_thread(thread_id="A", body="nit style suggestion"),
        _review_thread(thread_id="M", author="mystery-user", body="please re-check"),
    ]
    summary = controller.summarize_review_threads(items)
    ASSERTIONS.assertEqual(summary["blocking_count"], 1)
    ASSERTIONS.assertEqual(summary["advisory_count"], 1)
    ASSERTIONS.assertEqual(summary["needs_manual_count"], 1)
    ASSERTIONS.assertEqual(summary["next_action"], "fix_review_comments")


def test_summarize_review_threads_only_manual_needs_manual():
    summary = controller.summarize_review_threads([_review_thread(author="mystery-user", body="consider this")])
    ASSERTIONS.assertEqual(summary["blocking_count"], 0)
    ASSERTIONS.assertEqual(summary["advisory_count"], 0)
    ASSERTIONS.assertEqual(summary["needs_manual_count"], 1)
    ASSERTIONS.assertEqual(summary["next_action"], "needs_manual")


def test_summarize_review_threads_only_advisory_continue_checks_and_provider_presence():
    summary = controller.summarize_review_threads([_review_thread(author="coderabbitai[bot]", body="style suggestion")])
    ASSERTIONS.assertEqual(summary["blocking_count"], 0)
    ASSERTIONS.assertEqual(summary["advisory_count"], 1)
    ASSERTIONS.assertEqual(summary["needs_manual_count"], 0)
    ASSERTIONS.assertEqual(summary["next_action"], "continue_checks")
    ASSERTIONS.assertIn("coderabbitai", summary["present_providers"])


def test_review_thread_active_false_for_outdated_variants():
    camel = controller.classify_review_thread(_review_thread(thread_id="TO1", body="P1 bug", is_outdated=True))
    snake = controller.classify_review_thread(
        {
            **_review_thread(thread_id="TO2", body="P1 bug"),
            "isOutdated": False,
            "is_outdated": True,
        }
    )
    ASSERTIONS.assertFalse(camel["is_active"])
    ASSERTIONS.assertFalse(camel["blocking"])
    ASSERTIONS.assertFalse(snake["is_active"])
    ASSERTIONS.assertFalse(snake["blocking"])


def test_summarize_review_threads_ignores_outdated_unresolved_from_counts():
    summary = controller.summarize_review_threads([_review_thread(body="P1 bug", is_outdated=True)])
    ASSERTIONS.assertEqual(summary["blocking_count"], 0)
    ASSERTIONS.assertEqual(summary["advisory_count"], 0)
    ASSERTIONS.assertEqual(summary["needs_manual_count"], 0)
    ASSERTIONS.assertEqual(summary["next_action"], "continue_checks")


def test_summarize_review_threads_expected_providers_missing_never_block_or_change_next_action():
    summary = controller.summarize_review_threads(
        [_review_thread(author="coderabbitai[bot]", body="style suggestion")],
        {"expected_providers": ["coderabbitai", "greptile"]},
    )
    ASSERTIONS.assertIn("coderabbitai", summary["present_providers"])
    ASSERTIONS.assertIn("greptile", summary["missing_providers"])
    ASSERTIONS.assertFalse(summary["missing_providers_blocking"])
    ASSERTIONS.assertEqual(summary["blocking_count"], 0)
    ASSERTIONS.assertEqual(summary["advisory_count"], 1)
    ASSERTIONS.assertEqual(summary["next_action"], "continue_checks")


def test_summarize_review_threads_expected_providers_present_no_missing_and_blocking_drives_action():
    summary = controller.summarize_review_threads(
        [_review_thread(author="coderabbitai[bot]", body="P1 bug in runtime")],
        {"expected_providers": ["coderabbitai"]},
    )
    ASSERTIONS.assertEqual(summary["present_providers"], ["coderabbitai"])
    ASSERTIONS.assertEqual(summary["missing_providers"], [])
    ASSERTIONS.assertFalse(summary["missing_providers_blocking"])
    ASSERTIONS.assertEqual(summary["blocking_count"], 1)
    ASSERTIONS.assertEqual(summary["next_action"], "fix_review_comments")


def _pr_report_context(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "pr_number": 9,
        "head_sha": "abc123",
        "bad": [],
        "pending": [],
        "mergeStateStatus": "CLEAN",
        "unresolved_active": 0,
        "codacy": {"conclusion": "success", "annotations_count": 0},
        "blockers": [],
        "phase0_status": "PASS",
        "last_attempt": "attempt-1",
        "post_fix_audit_status": "pass",
        "validation_status": "pass",
        "next_action": "ready_to_merge",
        "ledger_summary": "none",
        "pr_url": "https://github.com/owner/repo/pull/9",
    }
    base.update(overrides)
    return base


def test_classify_pr_report_status_blocked_with_blockers():
    status = controller.classify_pr_report_status(_pr_report_context(blockers=["Codacy still failing"]))
    ASSERTIONS.assertEqual(status, "BLOCKED")


def test_classify_pr_report_status_blocked_with_bad():
    status = controller.classify_pr_report_status(_pr_report_context(bad=[{"name": "failing-check"}]))
    ASSERTIONS.assertEqual(status, "BLOCKED")


def test_classify_pr_report_status_blocked_with_nonempty_bad_list():
    status = controller.classify_pr_report_status(_pr_report_context(bad=["failing-check"]))
    ASSERTIONS.assertEqual(status, "BLOCKED")


def test_classify_pr_report_status_fixing_in_progress_context():
    status = controller.classify_pr_report_status(_pr_report_context(next_action="continue_checks"))
    ASSERTIONS.assertEqual(status, "FIXING")


def test_classify_pr_report_status_none_is_fixing():
    status = controller.classify_pr_report_status(None)
    ASSERTIONS.assertEqual(status, "FIXING")


def test_classify_pr_report_status_empty_dict_is_fixing():
    status = controller.classify_pr_report_status({})
    ASSERTIONS.assertEqual(status, "FIXING")


def test_classify_pr_report_status_needs_manual_phase0_and_manual_action():
    status = controller.classify_pr_report_status(
        _pr_report_context(phase0_status="NEEDS_MANUAL", next_action="needs_manual_phase0_failed")
    )
    ASSERTIONS.assertEqual(status, "NEEDS_MANUAL")


def test_classify_pr_report_status_ready_to_merge_strict_gate():
    status = controller.classify_pr_report_status(_pr_report_context())
    ASSERTIONS.assertEqual(status, "READY_TO_MERGE")


def test_not_ready_to_merge_when_codacy_not_success():
    status = controller.classify_pr_report_status(_pr_report_context(codacy={"conclusion": "action_required"}))
    ASSERTIONS.assertEqual(status, "FIXING")


def test_not_ready_to_merge_when_codacy_annotations_exist():
    status = controller.classify_pr_report_status(
        _pr_report_context(codacy={"conclusion": "success", "annotations_count": 2})
    )
    ASSERTIONS.assertEqual(status, "FIXING")


def test_not_ready_to_merge_when_pending_exists():
    status = controller.classify_pr_report_status(_pr_report_context(pending=[{"name": "merge readiness"}]))
    ASSERTIONS.assertEqual(status, "FIXING")


def test_classify_pr_report_status_noisy_scalar_bad_does_not_raise_and_not_ready():
    status = controller.classify_pr_report_status(_pr_report_context(bad=1))
    ASSERTIONS.assertNotEqual(status, "READY_TO_MERGE")


def test_classify_pr_report_status_noisy_scalar_pending_does_not_raise_and_not_ready():
    status = controller.classify_pr_report_status(_pr_report_context(pending=True))
    ASSERTIONS.assertNotEqual(status, "READY_TO_MERGE")


def test_classify_pr_report_status_noisy_scalar_blockers_does_not_raise_and_not_ready():
    status = controller.classify_pr_report_status(_pr_report_context(blockers=1))
    ASSERTIONS.assertNotEqual(status, "READY_TO_MERGE")


def test_classify_pr_report_status_noisy_object_bad_does_not_raise_and_not_ready():
    status = controller.classify_pr_report_status(_pr_report_context(bad=object()))
    ASSERTIONS.assertNotEqual(status, "READY_TO_MERGE")


def test_classify_pr_report_status_noisy_object_pending_does_not_raise_and_not_ready():
    status = controller.classify_pr_report_status(_pr_report_context(pending=object()))
    ASSERTIONS.assertNotEqual(status, "READY_TO_MERGE")


def test_classify_pr_report_status_noisy_object_blockers_does_not_raise_and_not_ready():
    status = controller.classify_pr_report_status(_pr_report_context(blockers=object()))
    ASSERTIONS.assertNotEqual(status, "READY_TO_MERGE")


def test_not_ready_to_merge_when_unresolved_active_exists():
    status = controller.classify_pr_report_status(_pr_report_context(unresolved_active=1))
    ASSERTIONS.assertEqual(status, "FIXING")


def test_not_ready_to_merge_when_unresolved_active_unknown_string():
    status = controller.classify_pr_report_status(_pr_report_context(unresolved_active="unknown"))
    ASSERTIONS.assertEqual(status, "FIXING")


def test_not_ready_to_merge_when_codacy_annotations_unknown_string():
    status = controller.classify_pr_report_status(
        _pr_report_context(codacy={"conclusion": "success", "annotations_count": "unknown"})
    )
    ASSERTIONS.assertEqual(status, "FIXING")


def test_classify_pr_report_status_phase0_fail_variants_need_manual():
    for phase0 in ("fail", "failed", "manual", "needs_manual"):
        status = controller.classify_pr_report_status(_pr_report_context(phase0_status=phase0))
        ASSERTIONS.assertEqual(status, "NEEDS_MANUAL")


def test_build_pr_status_report_fallbacks_do_not_crash():
    report = controller.build_pr_status_report({"pr_number": 9})
    ASSERTIONS.assertEqual(report["ledger_summary"], "unavailable")
    ASSERTIONS.assertEqual(report["post_fix_audit_status"], "unknown")
    ASSERTIONS.assertEqual(report["phase0_status"], "unknown")
    ASSERTIONS.assertEqual(report["validation_status"], "unknown")
    ASSERTIONS.assertEqual(report["blockers"], [])
    ASSERTIONS.assertEqual(report["pr_url"], "")


def test_build_pr_status_report_noisy_scalars_do_not_raise():
    report = controller.build_pr_status_report(_pr_report_context(bad=1, pending=True, blockers=1))
    ASSERTIONS.assertEqual(report["status"], "FIXING")
    ASSERTIONS.assertEqual(report["blockers"], [])


def test_build_telegram_pr_status_message_contains_required_fields():
    message = controller.build_telegram_pr_status_message(_pr_report_context())
    ASSERTIONS.assertIn("PR: #9", message)
    ASSERTIONS.assertIn("URL: https://github.com/owner/repo/pull/9", message)
    ASSERTIONS.assertIn("Head SHA: abc123", message)
    ASSERTIONS.assertIn("Status:", message)
    ASSERTIONS.assertIn("Blockers:", message)
    ASSERTIONS.assertIn("Phase 0:", message)
    ASSERTIONS.assertIn("Post-fix audit:", message)
    ASSERTIONS.assertIn("Validation:", message)
    ASSERTIONS.assertIn("Next action:", message)
    ASSERTIONS.assertIn("Ledger:", message)


def test_build_telegram_pr_status_message_truncates_long_blockers():
    long_blocker = "x" * 800
    message = controller.build_telegram_pr_status_message(_pr_report_context(blockers=[long_blocker]))
    ASSERTIONS.assertIn("...[", message)
    ASSERTIONS.assertLessEqual(len(message), 3500)


def test_build_telegram_pr_status_message_redacts_secret_like_tokens():
    message = controller.build_telegram_pr_status_message(
        _pr_report_context(blockers=["token " + ("ghp_" + "abcdefghijklmnopqrstuvwxyz123456")])
    )
    ASSERTIONS.assertNotIn(("ghp_" + "abcdefghijklmnopqrstuvwxyz123456"), message)
    ASSERTIONS.assertIn("[REDACTED]", message)


def test_build_telegram_pr_status_message_partial_dict_does_not_raise():
    message = controller.build_telegram_pr_status_message({"status": "READY_TO_MERGE"})
    ASSERTIONS.assertIn("PR Status Report", message)
    ASSERTIONS.assertIn("Status:", message)


def test_build_telegram_pr_status_message_noisy_scalars_do_not_raise():
    message = controller.build_telegram_pr_status_message(_pr_report_context(bad=1, pending=True, blockers=1))
    ASSERTIONS.assertIn("PR Status Report", message)
    ASSERTIONS.assertIn("Status: FIXING", message)


def test_build_telegram_audit_summary_message_contains_required_fields():
    message = controller.build_telegram_audit_summary_message(_pr_report_context())
    ASSERTIONS.assertIn("PR Audit Summary", message)
    ASSERTIONS.assertIn("PR: #9", message)
    ASSERTIONS.assertIn("URL: https://github.com/owner/repo/pull/9", message)
    ASSERTIONS.assertIn("Head SHA: abc123", message)
    ASSERTIONS.assertIn("Status: READY_TO_MERGE", message)
    ASSERTIONS.assertIn("Post-fix audit: pass", message)
    ASSERTIONS.assertIn("Validation: pass", message)
    ASSERTIONS.assertIn("Next action: ready_to_merge", message)


def test_build_telegram_ledger_summary_message_contains_required_fields():
    message = controller.build_telegram_ledger_summary_message(_pr_report_context())
    ASSERTIONS.assertIn("PR Ledger Summary", message)
    ASSERTIONS.assertIn("PR: #9", message)
    ASSERTIONS.assertIn("URL: https://github.com/owner/repo/pull/9", message)
    ASSERTIONS.assertIn("Head SHA: abc123", message)
    ASSERTIONS.assertIn("Status: READY_TO_MERGE", message)
    ASSERTIONS.assertIn("Ledger: none", message)
    ASSERTIONS.assertIn("Last attempt: attempt-1", message)
    ASSERTIONS.assertIn("Next action: ready_to_merge", message)


def test_build_telegram_audit_summary_message_respects_safe_limit():
    long_report = _pr_report_context(
        ledger_summary="L" * 20000,
        blockers=["b" * 20000],
        next_action="ready_to_merge",
    )
    audit_message = controller.build_telegram_audit_summary_message(long_report)
    ASSERTIONS.assertLessEqual(len(audit_message), controller._TELEGRAM_SAFE_MESSAGE_LIMIT)


def test_build_telegram_ledger_summary_message_respects_safe_limit():
    long_report = _pr_report_context(
        ledger_summary="L" * 20000,
        blockers=["b" * 20000],
        next_action="ready_to_merge",
    )
    ledger_message = controller.build_telegram_ledger_summary_message(long_report)
    ASSERTIONS.assertLessEqual(len(ledger_message), controller._TELEGRAM_SAFE_MESSAGE_LIMIT)


def test_truncate_for_telegram_under_limit_unchanged():
    text = "short text"
    ASSERTIONS.assertEqual(controller._truncate_for_telegram(text, 100), text)


def test_truncate_for_telegram_over_limit_has_digest_suffix():
    truncated = controller._truncate_for_telegram("x" * 200, 80)
    ASSERTIONS.assertRegex(truncated, r"\.\.\.\[[0-9a-f]{8}\]$")
    ASSERTIONS.assertLessEqual(len(truncated), 80)


def test_truncate_for_telegram_section_limit_behavior():
    truncated = controller._truncate_for_telegram("x" * 2000, controller._TELEGRAM_SECTION_LIMIT)
    ASSERTIONS.assertLessEqual(len(truncated), controller._TELEGRAM_SECTION_LIMIT)
    ASSERTIONS.assertRegex(truncated, r"\.\.\.\[[0-9a-f]{8}\]$")


def test_telegram_messages_redact_multiple_secret_patterns():
    secret_text = " ".join(
        [
            ("ghp_" + "abcdefghijklmnopqrstuvwxyz123456"),
            ("xo" + "xb-" + "1234567890-abcdefghijklmnopqrstuv"),
            ("sk-" + "abcdefghijklmnopqrstuvwxyz123456"),
            ("rk-" + "abcdefghijklmnopqrstuvwxyz123456"),
            ("AI" + "zaSyA12345678901234567890123456789012"),
        ]
    )
    message = controller.build_telegram_pr_status_message(_pr_report_context(blockers=[secret_text]))
    ASSERTIONS.assertNotIn(("ghp_" + "abcdefghijklmnopqrstuvwxyz123456"), message)
    ASSERTIONS.assertNotIn(("xo" + "xb-" + "1234567890-abcdefghijklmnopqrstuv"), message)
    ASSERTIONS.assertNotIn(("sk-" + "abcdefghijklmnopqrstuvwxyz123456"), message)
    ASSERTIONS.assertNotIn(("rk-" + "abcdefghijklmnopqrstuvwxyz123456"), message)
    ASSERTIONS.assertNotIn(("AI" + "zaSyA12345678901234567890123456789012"), message)
    ASSERTIONS.assertIn("[REDACTED]", message)


def test_render_next_action_summary_is_stable():
    summary = controller.render_next_action_summary(" Needs-Manual Phase0 Failed ")
    ASSERTIONS.assertEqual(summary, "needs_manual_phase0_failed")


def test_report_ready_to_merge_with_strict_clean_context():
    """Clean PR context renders READY_TO_MERGE, not FIXING."""
    context = {
        "pr_number": 241,
        "head_sha": "abc123",
        "bad": [],
        "pending": [],
        "blockers": [],
        "mergeStateStatus": "CLEAN",
        "unresolved_active": 0,
        "codacy_conclusion": "success",
        "codacy_annotations_count": 0,
        "pr_url": "https://github.com/example/repo/pull/241",
        "next_action": "ready",
        "phase0_status": "PASS",
        "post_fix_audit_status": "PASS",
        "validation_status": "PASS",
        "ledger_summary": "latest attempt ok",
    }
    report = controller.build_pr_status_report(context)
    ASSERTIONS.assertEqual(report["status"], "READY_TO_MERGE")
    message = controller.build_telegram_pr_status_message(context)
    ASSERTIONS.assertIn("READY_TO_MERGE", message)
    ASSERTIONS.assertIn("241", message)
    ASSERTIONS.assertIn("abc123", message)


def _matrix_phase0_output_complete() -> dict[str, Any]:
    return {
        "status": "PASS",
        "risk_level": "high",
        "next_action": "generate_patch_prompt",
        "files_inspected": ["scripts/pr_automation_controller.py"],
        "authoritative_modules": ["scripts/pr_automation_controller.py"],
        "dangerous_gates": ["path matching"],
        "files_allowed": ["scripts/pr_automation_controller.py", "tests/scripts/test_pr_automation_controller.py"],
        "files_forbidden": [".github/workflows/*"],
        "exact_patch_plan": ["add passive helpers only"],
        "test_matrix": ["unit coverage for matrix helpers"],
        "tests_to_run": ["python3 -m pytest tests/scripts/test_pr_automation_controller.py -q"],
        "stop_conditions": ["stop on scope drift"],
        "matcher_semantics": "exact path + normalized matching",
    }


def test_phase0_profile_detection_standard_vs_safety_critical():
    ASSERTIONS.assertEqual(controller.classify_phase0_profile({"task": "update prompt text"}), "standard")
    ASSERTIONS.assertEqual(
        controller.classify_phase0_profile({"task": "harden path matching and wildcard traversal gates"}),
        "matrix_required",
    )
    ASSERTIONS.assertFalse(controller.task_requires_matrix_phase0({"task": "docs only"}))
    ASSERTIONS.assertTrue(controller.task_requires_matrix_phase0({"task": "automation gate security controls"}))


def test_phase0_profile_detection_avoids_substring_false_positives():
    ASSERTIONS.assertEqual(controller.classify_phase0_profile({"task": "refresh global docs"}), "standard")
    ASSERTIONS.assertEqual(controller.classify_phase0_profile({"task": "prepare pushback response"}), "standard")
    ASSERTIONS.assertFalse(controller.task_requires_matrix_phase0({"task": "global docs wording"}))
    ASSERTIONS.assertFalse(controller.task_requires_matrix_phase0({"task": "pushback wording"}))


def test_phase0_profile_detection_matches_word_boundary_risk_keywords():
    ASSERTIONS.assertTrue(controller.task_requires_matrix_phase0({"task": "glob path matcher"}))
    ASSERTIONS.assertTrue(controller.task_requires_matrix_phase0({"task": "push permission gate"}))


def test_phase0_profile_detection_matches_space_separated_risk_phrases():
    ASSERTIONS.assertTrue(controller.task_requires_matrix_phase0({"task": "validate live action safety"}))
    ASSERTIONS.assertTrue(controller.task_requires_matrix_phase0({"task": "resolve merge conflict safely"}))


def test_phase0_profile_detection_includes_metadata_risk_keys():
    ASSERTIONS.assertTrue(controller.task_requires_matrix_phase0({"task_title": "resolve merge conflict safely"}))
    ASSERTIONS.assertTrue(controller.task_requires_matrix_phase0({"task_scope": "validate live action safety"}))


def test_phase0_profile_detection_matches_hyphenated_and_slash_risk_phrases():
    ASSERTIONS.assertTrue(controller.task_requires_matrix_phase0({"task": "validate live-action safety"}))
    ASSERTIONS.assertTrue(controller.task_requires_matrix_phase0({"task": "resolve merge-conflict safely"}))
    ASSERTIONS.assertTrue(controller.task_requires_matrix_phase0({"task": "enable/disable controls"}))
    ASSERTIONS.assertTrue(controller.task_requires_matrix_phase0({"task": "enable disable controls"}))


def test_phase0_profile_detection_uses_dangerous_and_authoritative_lists():
    ASSERTIONS.assertEqual(
        controller.classify_phase0_profile({"dangerous_gates": ["workflow execution gate"]}),
        "matrix_required",
    )
    ASSERTIONS.assertEqual(
        controller.classify_phase0_profile({"authoritative_modules": ["sandbox rollback controls"]}),
        "matrix_required",
    )
    ASSERTIONS.assertTrue(controller.task_requires_matrix_phase0({"dangerous_gates": ["live gate"]}))
    ASSERTIONS.assertTrue(
        controller.task_requires_matrix_phase0({"authoritative_modules": ["workflow execution safety"]})
    )


def test_matrix_phase0_requirements_pass_with_all_fields():
    requirements = controller.build_matrix_phase0_requirements(
        {"task": "path matching guardrails", "dangerous_gates": ["path matching"]}
    )
    validated = controller.validate_phase0_output_contract(_matrix_phase0_output_complete(), requirements)
    ASSERTIONS.assertEqual(validated["status"], "PASS")
    ASSERTIONS.assertEqual(validated["next_action"], "generate_patch_prompt")
    ASSERTIONS.assertTrue(validated["can_patch"])


def test_matrix_phase0_missing_fields_fails_closed_needs_manual():
    requirements = controller.build_matrix_phase0_requirements({"task": "wildcard traversal safety gate"})
    incomplete = {"status": "PASS", "risk_level": "high", "next_action": "generate_patch_prompt"}
    validated = controller.validate_phase0_output_contract(incomplete, requirements)
    ASSERTIONS.assertEqual(validated["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(validated["next_action"], "needs_manual_matrix_phase0_missing")
    ASSERTIONS.assertFalse(validated["can_patch"])
    ASSERTIONS.assertIn("files_inspected", validated["missing_fields"])


def test_matrix_phase0_incomplete_output_with_safety_task_context_fails_closed():
    safety_task_context = {"task": "harden wildcard path traversal safety controls"}
    incomplete = {"status": "PASS", "risk_level": "high", "next_action": "generate_patch_prompt"}
    validated = controller.validate_phase0_output_contract(incomplete, safety_task_context)
    ASSERTIONS.assertEqual(validated["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(validated["next_action"], "needs_manual_matrix_phase0_missing")
    ASSERTIONS.assertFalse(validated["can_patch"])
    ASSERTIONS.assertTrue(validated["phase0_required"])
    ASSERTIONS.assertNotEqual(validated["missing_fields"], [])


def test_matrix_phase0_complete_output_with_safety_task_context_can_patch():
    safety_task_context = {"task": "harden wildcard path traversal safety controls"}
    validated = controller.validate_phase0_output_contract(_matrix_phase0_output_complete(), safety_task_context)
    ASSERTIONS.assertEqual(validated["status"], "PASS")
    ASSERTIONS.assertTrue(validated["can_patch"])
    ASSERTIONS.assertTrue(validated["phase0_required"])


def test_standard_task_context_does_not_require_matrix_phase0_fields():
    standard_task_context = {"task": "update release docs text"}
    incomplete = {"status": "PASS", "risk_level": "medium", "next_action": "generate_patch_prompt"}
    validated = controller.validate_phase0_output_contract(incomplete, standard_task_context)
    ASSERTIONS.assertEqual(validated["status"], "PASS")
    ASSERTIONS.assertTrue(validated["can_patch"])
    ASSERTIONS.assertFalse(validated["phase0_required"])
    ASSERTIONS.assertEqual(validated["missing_fields"], [])


def test_matrix_phase0_requires_matcher_semantics_when_relevant():
    requirements = controller.build_matrix_phase0_requirements(
        {"task": "normalize glob matcher", "dangerous_gates": ["glob path matching"]}
    )
    payload = _matrix_phase0_output_complete()
    payload.pop("matcher_semantics")
    validated = controller.validate_phase0_output_contract(payload, requirements)
    ASSERTIONS.assertEqual(validated["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertIn("matcher_semantics", validated["missing_fields"])


def test_matrix_phase0_requires_matcher_semantics_from_task_text_only():
    requirements = controller.build_matrix_phase0_requirements({"task": "normalize glob matcher"})
    ASSERTIONS.assertTrue(requirements["matcher_semantics_required"])
    payload = _matrix_phase0_output_complete()
    payload.pop("matcher_semantics")
    validated = controller.validate_phase0_output_contract(payload, requirements)
    ASSERTIONS.assertEqual(validated["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(validated["next_action"], "needs_manual_matrix_phase0_missing")
    ASSERTIONS.assertFalse(validated["can_patch"])
    ASSERTIONS.assertIn("matcher_semantics", validated["missing_fields"])


def test_matrix_phase0_requires_matcher_semantics_from_metadata_task_title():
    requirements = controller.build_matrix_phase0_requirements({"task_title": "normalize allowlist traversal"})
    ASSERTIONS.assertTrue(requirements["matcher_semantics_required"])


def test_matrix_phase0_requires_matcher_semantics_for_allowlist_forbidden_normalization_traversal():
    for task_text in (
        "validate allowlist for patch scope",
        "respect forbidden files guardrails",
        "ensure path normalization safety",
        "block traversal path escapes",
    ):
        requirements = controller.build_matrix_phase0_requirements({"task": task_text})
        ASSERTIONS.assertTrue(requirements["matcher_semantics_required"])
        payload = _matrix_phase0_output_complete()
        payload.pop("matcher_semantics")
        validated = controller.validate_phase0_output_contract(payload, requirements)
        ASSERTIONS.assertEqual(validated["status"], "NEEDS_MANUAL")
        ASSERTIONS.assertIn("matcher_semantics", validated["missing_fields"])


def test_matrix_phase0_semantic_keyword_matching_is_case_insensitive():
    requirements = controller.build_matrix_phase0_requirements(
        {
            "task": "Review Path Matcher behavior",
            "dangerous_gates": ["Live Action controls"],
            "authoritative_modules": ["FORBIDDEN FILES policy"],
        }
    )
    ASSERTIONS.assertTrue(requirements["matcher_semantics_required"])
    ASSERTIONS.assertTrue(requirements["gate_semantics_required"])


def test_matrix_phase0_requires_gate_semantics_when_relevant():
    requirements = controller.build_matrix_phase0_requirements(
        {"task": "safety docs update", "dangerous_gates": ["live permission gate"]}
    )
    payload = _matrix_phase0_output_complete()
    payload.pop("gate_semantics", None)
    validated = controller.validate_phase0_output_contract(payload, requirements)
    ASSERTIONS.assertEqual(validated["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertIn("gate_semantics", validated["missing_fields"])
    ASSERTIONS.assertFalse(validated["can_patch"])


def test_matrix_phase0_requires_gate_semantics_from_task_text_only():
    requirements = controller.build_matrix_phase0_requirements({"task": "validate live action safety"})
    ASSERTIONS.assertTrue(requirements["gate_semantics_required"])
    payload = _matrix_phase0_output_complete()
    payload.pop("gate_semantics", None)
    validated = controller.validate_phase0_output_contract(payload, requirements)
    ASSERTIONS.assertEqual(validated["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(validated["next_action"], "needs_manual_matrix_phase0_missing")
    ASSERTIONS.assertFalse(validated["can_patch"])
    ASSERTIONS.assertIn("gate_semantics", validated["missing_fields"])


def test_matrix_phase0_requires_gate_semantics_from_metadata_task_scope():
    requirements = controller.build_matrix_phase0_requirements({"task_scope": "validate live action safety"})
    ASSERTIONS.assertTrue(requirements["gate_semantics_required"])


def test_matrix_phase0_requires_gate_semantics_for_live_gate_permission_tokens():
    for dangerous_gate in ("live action", "phase0 gate", "merge permission"):
        requirements = controller.build_matrix_phase0_requirements(
            {"task": "safety docs update", "dangerous_gates": [dangerous_gate]}
        )
        payload = _matrix_phase0_output_complete()
        payload.pop("gate_semantics", None)
        validated = controller.validate_phase0_output_contract(payload, requirements)
        ASSERTIONS.assertEqual(validated["status"], "NEEDS_MANUAL")
        ASSERTIONS.assertIn("gate_semantics", validated["missing_fields"])


def test_matrix_phase0_gate_semantics_required_and_present_passes():
    requirements = controller.build_matrix_phase0_requirements({"task": "validate Live Action permission gate"})
    ASSERTIONS.assertTrue(requirements["gate_semantics_required"])
    payload = _matrix_phase0_output_complete()
    payload["gate_semantics"] = "live action gate is constrained by explicit permission checks"
    validated = controller.validate_phase0_output_contract(payload, requirements)
    ASSERTIONS.assertEqual(validated["status"], "PASS")
    ASSERTIONS.assertTrue(validated["can_patch"])


def test_matrix_phase0_gate_semantics_fail_closed_from_raw_context_requirements():
    requirements = {"dangerous_gates": ["live gate"]}
    built = controller.build_matrix_phase0_requirements(requirements)
    ASSERTIONS.assertTrue(built["gate_semantics_required"])
    payload = _matrix_phase0_output_complete()
    payload.pop("gate_semantics", None)
    validated = controller.validate_phase0_output_contract(payload, requirements)
    ASSERTIONS.assertEqual(validated["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(validated["next_action"], "needs_manual_matrix_phase0_missing")
    ASSERTIONS.assertFalse(validated["can_patch"])
    ASSERTIONS.assertIn("gate_semantics", validated["missing_fields"])


def test_matrix_phase0_placeholder_required_values_fail_closed():
    requirements = controller.build_matrix_phase0_requirements({"task": "wildcard traversal safety gate"})
    payload = _matrix_phase0_output_complete()
    payload.update(
        {
            "exact_patch_plan": ["todo", " n/a ", "none"],
            "files_inspected": ["tbd"],
            "tests_to_run": "null",
        }
    )
    validated = controller.validate_phase0_output_contract(payload, requirements)
    ASSERTIONS.assertEqual(validated["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertIn("exact_patch_plan", validated["missing_fields"])
    ASSERTIONS.assertIn("files_inspected", validated["missing_fields"])
    ASSERTIONS.assertIn("tests_to_run", validated["missing_fields"])


def test_matrix_phase0_placeholder_object_and_quoted_values_fail_closed():
    requirements = controller.build_matrix_phase0_requirements({"task": "wildcard traversal safety gate"})
    payload = _matrix_phase0_output_complete()
    payload.update(
        {
            "files_inspected": [{}],
            "exact_patch_plan": ['"TBD"'],
            "test_matrix": [{"id": "todo"}],
            "tests_to_run": {"cmd": "pytest"},
        }
    )
    validated = controller.validate_phase0_output_contract(payload, requirements)
    ASSERTIONS.assertEqual(validated["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertIn("files_inspected", validated["missing_fields"])
    ASSERTIONS.assertIn("exact_patch_plan", validated["missing_fields"])
    ASSERTIONS.assertIn("test_matrix", validated["missing_fields"])
    ASSERTIONS.assertIn("tests_to_run", validated["missing_fields"])


def test_matrix_phase0_mixed_real_and_placeholder_list_counts_as_present():
    requirements = controller.build_matrix_phase0_requirements({"task": "wildcard traversal safety controls"})
    payload = _matrix_phase0_output_complete()
    payload["exact_patch_plan"] = ["todo", "add passive matcher check"]
    validated = controller.validate_phase0_output_contract(payload, requirements)
    ASSERTIONS.assertEqual(validated["status"], "PASS")
    ASSERTIONS.assertTrue(validated["can_patch"])


def test_matrix_phase0_placeholder_semantic_values_are_missing():
    requirements = {
        "phase0_required": False,
        "required_fields": [],
        "matcher_semantics_required": True,
        "gate_semantics_required": True,
        "next_action": "generate_patch_prompt",
    }
    validated = controller.validate_phase0_output_contract(
        {"status": "PASS", "matcher_semantics": "todo", "gate_semantics": "n/a"},
        requirements,
    )
    ASSERTIONS.assertEqual(validated["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertIn("matcher_semantics", validated["missing_fields"])
    ASSERTIONS.assertIn("gate_semantics", validated["missing_fields"])


def test_matrix_phase0_semantics_fail_closed_even_when_matrix_not_required():
    requirements = {
        "phase0_required": False,
        "required_fields": [],
        "matcher_semantics_required": True,
        "next_action": "generate_patch_prompt",
    }
    validated = controller.validate_phase0_output_contract({"status": "PASS"}, requirements)
    ASSERTIONS.assertEqual(validated["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertIn("matcher_semantics", validated["missing_fields"])
    ASSERTIONS.assertFalse(validated["can_patch"])


def test_matrix_phase0_required_false_does_not_override_derived_high_risk_context():
    requirements = {"task": "wildcard traversal safety gate", "phase0_required": False}
    validated = controller.validate_phase0_output_contract(
        {"status": "PASS", "risk_level": "high", "next_action": "generate_patch_prompt"},
        requirements,
    )
    ASSERTIONS.assertEqual(validated["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertTrue(validated["phase0_required"])
    ASSERTIONS.assertEqual(validated["next_action"], "needs_manual_matrix_phase0_missing")


def test_matrix_phase0_matcher_semantics_required_false_does_not_override_derived_context():
    requirements = {"task": "normalize glob matcher", "matcher_semantics_required": False}
    validated = controller.validate_phase0_output_contract(
        {"status": "PASS", "risk_level": "high", "next_action": "generate_patch_prompt"},
        requirements,
    )
    ASSERTIONS.assertEqual(validated["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(validated["next_action"], "needs_manual_matrix_phase0_missing")
    ASSERTIONS.assertIn("matcher_semantics", validated["missing_fields"])
    ASSERTIONS.assertFalse(validated["can_patch"])


def test_matrix_phase0_gate_semantics_required_false_does_not_override_derived_context():
    requirements = {"task": "validate live action safety", "gate_semantics_required": False}
    validated = controller.validate_phase0_output_contract(
        {"status": "PASS", "risk_level": "high", "next_action": "generate_patch_prompt"},
        requirements,
    )
    ASSERTIONS.assertEqual(validated["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(validated["next_action"], "needs_manual_matrix_phase0_missing")
    ASSERTIONS.assertIn("gate_semantics", validated["missing_fields"])
    ASSERTIONS.assertFalse(validated["can_patch"])


def test_matrix_phase0_missing_output_fails_closed():
    validated = controller.validate_phase0_output_contract(None, {"phase0_required": False})
    ASSERTIONS.assertEqual(validated["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(validated["next_action"], "needs_manual_phase0_missing_output")
    ASSERTIONS.assertFalse(validated["can_patch"])


def test_matrix_phase0_pass_missing_payload_next_action_fails_closed():
    validated = controller.validate_phase0_output_contract({"status": "PASS"}, {})
    ASSERTIONS.assertEqual(validated["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(validated["next_action"], "needs_manual_phase0_invalid_action")
    ASSERTIONS.assertFalse(validated["can_patch"])


def test_matrix_phase0_requirements_next_action_cannot_authorize_patch():
    validated = controller.validate_phase0_output_contract({"status": "PASS"}, {"next_action": "generate_patch_prompt"})
    ASSERTIONS.assertEqual(validated["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(validated["next_action"], "needs_manual_phase0_invalid_action")
    ASSERTIONS.assertFalse(validated["can_patch"])


def test_matrix_phase0_explicit_payload_generate_patch_prompt_can_patch():
    validated = controller.validate_phase0_output_contract(
        {"status": "PASS", "next_action": "generate_patch_prompt"},
        {"phase0_required": False, "next_action": "needs_manual_phase0_failed"},
    )
    ASSERTIONS.assertEqual(validated["status"], "PASS")
    ASSERTIONS.assertEqual(validated["next_action"], "generate_patch_prompt")
    ASSERTIONS.assertTrue(validated["can_patch"])


def test_matrix_phase0_explicit_payload_proceed_with_narrow_patch_can_patch():
    validated = controller.validate_phase0_output_contract(
        {"status": "PASS", "next_action": "proceed_with_narrow_patch"},
        {"phase0_required": False, "next_action": "generate_patch_prompt"},
    )
    ASSERTIONS.assertEqual(validated["status"], "PASS")
    ASSERTIONS.assertEqual(validated["next_action"], "proceed_with_narrow_patch")
    ASSERTIONS.assertTrue(validated["can_patch"])


def test_matrix_phase0_manual_or_blocked_output_cannot_patch():
    manual = controller.validate_phase0_output_contract({"status": "manual"}, {"phase0_required": False})
    blocked = controller.validate_phase0_output_contract({"status": "blocked"}, {"phase0_required": False})
    ASSERTIONS.assertEqual(manual["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(blocked["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertFalse(manual["can_patch"])
    ASSERTIONS.assertFalse(blocked["can_patch"])


def test_matrix_phase0_invalid_status_fails_closed():
    validated = controller.validate_phase0_output_contract({"status": "MAYBE"}, {"phase0_required": False})
    ASSERTIONS.assertEqual(validated["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(validated["next_action"], "needs_manual_phase0_invalid_status")
    ASSERTIONS.assertFalse(validated["can_patch"])


def test_matrix_phase0_required_fields_reject_non_string_scalar_values():
    requirements = controller.build_matrix_phase0_requirements({"task": "wildcard traversal safety gate"})
    for scalar in (True, False, 1, 0, 3.14):
        payload = _matrix_phase0_output_complete()
        payload["files_inspected"] = scalar
        validated = controller.validate_phase0_output_contract(payload, requirements)
        ASSERTIONS.assertEqual(validated["status"], "NEEDS_MANUAL")
        ASSERTIONS.assertFalse(validated["can_patch"])
        ASSERTIONS.assertIn("files_inspected", validated["missing_fields"])


def test_review_triage_active_reproducible_uncovered_bypass_patch_required():
    comment = {"body": "Active bypass failure in guard path", "active": True, "reproducible": True}
    result = controller.classify_review_triage_need(comment, {"checks_green": False})
    ASSERTIONS.assertEqual(result, "PATCH_REQUIRED")


def test_review_triage_covered_by_matrix_tests_evidence_resolve():
    comment = {"id": "t-1", "body": "already fixed by matrix", "active": True, "covered_by_matrix": True}
    triage = controller.triage_review_thread_contract(
        comment,
        {"current_head_sha": "abc", "evidence_head_sha": "abc", "checks_green": True, "tests": ["pytest"]},
    )
    ASSERTIONS.assertEqual(triage["decision"], "EVIDENCE_RESOLVE")


def test_review_triage_stale_advisory_with_green_checks_evidence_resolve():
    comment = {"id": "t-2", "body": "stale advisory nit", "active": True}
    triage = controller.triage_review_thread_contract(
        comment,
        {"current_head_sha": "abc", "evidence_head_sha": "abc", "checks_green": True, "tests": ["pytest"]},
    )
    ASSERTIONS.assertEqual(triage["decision"], "EVIDENCE_RESOLVE")


def test_review_triage_forbidden_scope_future_roadmap_ambiguous_needs_manual():
    comment = {"body": "future scope roadmap; ambiguous architecture", "active": True}
    result = controller.classify_review_triage_need(comment, {"checks_green": True})
    ASSERTIONS.assertEqual(result, "NEEDS_MANUAL")


def test_review_triage_outdated_without_evidence_fails_closed_needs_manual():
    comment = {"id": "t-o", "body": "stale advisory nit", "isOutdated": True}
    triage = controller.triage_review_thread_contract(comment, {"checks_green": True})
    ASSERTIONS.assertEqual(triage["decision"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(triage["reason"], "inactive_or_resolved_thread")


def test_review_triage_malformed_thread_payload_fails_closed_needs_manual():
    triage = controller.triage_review_thread_contract(cast(dict[str, Any], "invalid"), {"checks_green": True})
    ASSERTIONS.assertEqual(triage["decision"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(triage["reason"], "malformed_thread_payload")


def test_review_triage_outdated_with_head_mismatch_denies_evidence_resolve():
    comment = {"id": "t-m", "body": "already fixed stale", "isOutdated": True}
    triage = controller.triage_review_thread_contract(
        comment,
        {"current_head_sha": "abc", "evidence_head_sha": "def", "checks_green": True, "tests": ["pytest"]},
    )
    ASSERTIONS.assertEqual(triage["decision"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(triage["reason"], "inactive_or_resolved_thread")


def test_review_triage_is_active_false_and_isActive_false_follow_contract_semantics():
    snake = {"body": "bypass failure in guard path", "is_active": False, "reproducible": True}
    camel = {"body": "bypass failure in guard path", "isActive": False, "reproducible": True}
    snake_result = controller.classify_review_triage_need(snake, {"checks_green": False})
    camel_result = controller.classify_review_triage_need(camel, {"checks_green": False})
    ASSERTIONS.assertEqual(snake_result, "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(camel_result, "NEEDS_MANUAL")


def test_build_review_triage_matrix_patch_required_full_payload():
    comment = {"thread_id": "t-1", "body": "Active bypass failure in guard path", "active": True, "reproducible": True}
    matrix = controller.build_review_triage_matrix([comment], {"checks_green": False})
    item = matrix["items"][0]
    ASSERTIONS.assertEqual(item["thread_id"], "t-1")
    ASSERTIONS.assertEqual(item["review_thread_id"], "t-1")
    ASSERTIONS.assertEqual(item["decision"], "PATCH_REQUIRED")
    ASSERTIONS.assertEqual(item["next_action"], "patch_required")
    ASSERTIONS.assertFalse(item["can_patch"])
    ASSERTIONS.assertEqual(item["reason"], "active_safety_or_current_head_failure_or_contract_violation")
    ASSERTIONS.assertFalse(item["matrix_coverage"])
    ASSERTIONS.assertEqual(item["tests_covering_behavior"], [])
    ASSERTIONS.assertTrue(item["requires_patch"])
    ASSERTIONS.assertFalse(item["can_resolve_with_evidence"])
    ASSERTIONS.assertEqual(matrix["next_action"], "patch_required")


def test_build_review_triage_matrix_patch_required_authorized_can_patch_true():
    comment = {"thread_id": "t-1", "body": "Active bypass failure in guard path", "active": True, "reproducible": True}
    matrix = controller.build_review_triage_matrix(
        [comment],
        {"checks_green": False, "standing_owner_authorized": True},
    )
    item = matrix["items"][0]
    ASSERTIONS.assertTrue(item["requires_patch"])
    ASSERTIONS.assertTrue(item["can_patch"])
    ASSERTIONS.assertEqual(matrix["next_action"], "patch_required")


def test_build_review_triage_matrix_ignores_generic_can_patch_without_explicit_authorization():
    comment = {"thread_id": "t-1", "body": "Active bypass failure in guard path", "active": True, "reproducible": True}
    matrix = controller.build_review_triage_matrix([comment], {"checks_green": False, "can_patch": True})
    item = matrix["items"][0]
    ASSERTIONS.assertTrue(item["requires_patch"])
    ASSERTIONS.assertFalse(item["can_patch"])


def test_build_review_triage_matrix_patch_authorized_key_enables_can_patch():
    comment = {"thread_id": "t-1", "body": "Active bypass failure in guard path", "active": True, "reproducible": True}
    matrix = controller.build_review_triage_matrix([comment], {"checks_green": False, "patch_authorized": True})
    item = matrix["items"][0]
    ASSERTIONS.assertTrue(item["requires_patch"])
    ASSERTIONS.assertTrue(item["can_patch"])


def test_build_review_triage_matrix_truthy_string_authorization_flags_do_not_enable_patch():
    comment = {"thread_id": "t-1", "body": "Active bypass failure in guard path", "active": True, "reproducible": True}
    matrix = controller.build_review_triage_matrix(
        [comment], {"checks_green": False, "standing_owner_authorized": "yes", "patch_authorized": "1"}
    )
    item = matrix["items"][0]
    ASSERTIONS.assertTrue(item["requires_patch"])
    ASSERTIONS.assertFalse(item["can_patch"])


def test_build_review_triage_matrix_evidence_resolve_full_payload():
    comment = {
        "id": "22",
        "body": "already fixed by matrix",
        "active": True,
        "covered_by_matrix": True,
        "tests_covering_behavior": [
            "tests/scripts/test_pr_automation_controller.py::"
            "test_review_triage_covered_by_matrix_tests_evidence_resolve"
        ],
    }
    matrix = controller.build_review_triage_matrix(
        [comment],
        {"checks_green": True, "current_head_sha": "abc", "evidence_head_sha": "abc"},
    )
    item = matrix["items"][0]
    ASSERTIONS.assertEqual(item["thread_id"], "22")
    ASSERTIONS.assertEqual(item["decision"], "EVIDENCE_RESOLVE")
    ASSERTIONS.assertEqual(item["next_action"], "resolve_with_evidence")
    ASSERTIONS.assertFalse(item["can_patch"])
    ASSERTIONS.assertEqual(item["reason"], "stale_or_advisory_with_current_head_evidence")
    ASSERTIONS.assertTrue(item["matrix_coverage"])
    ASSERTIONS.assertEqual(len(item["tests_covering_behavior"]), 1)
    ASSERTIONS.assertFalse(item["requires_patch"])
    ASSERTIONS.assertTrue(item["can_resolve_with_evidence"])
    ASSERTIONS.assertEqual(matrix["next_action"], "resolve_with_evidence")


def test_build_review_triage_matrix_needs_manual_full_payload():
    comment = {"thread_id": "t-3", "body": "future scope roadmap; ambiguous architecture", "active": True}
    matrix = controller.build_review_triage_matrix([comment], {"checks_green": True})
    item = matrix["items"][0]
    ASSERTIONS.assertEqual(item["thread_id"], "t-3")
    ASSERTIONS.assertEqual(item["decision"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(item["next_action"], "needs_manual")
    ASSERTIONS.assertFalse(item["can_patch"])
    ASSERTIONS.assertEqual(item["reason"], "future_roadmap_scope")
    ASSERTIONS.assertFalse(item["matrix_coverage"])
    ASSERTIONS.assertEqual(item["tests_covering_behavior"], [])
    ASSERTIONS.assertFalse(item["requires_patch"])
    ASSERTIONS.assertFalse(item["can_resolve_with_evidence"])
    ASSERTIONS.assertEqual(matrix["next_action"], "needs_manual")


def test_review_triage_contract_forbidden_file_request_needs_manual():
    triage = controller.triage_review_thread_contract(
        {
            "id": "f1",
            "body": "please touch forbidden file .github/workflows/pr.yml",
            "path": ".github/workflows/pr.yml",
        },
        {"files_allowed": ["scripts/pr_automation_controller.py"]},
    )
    ASSERTIONS.assertEqual(triage["decision"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(triage["reason"], "forbidden_file_request")


def test_review_triage_contract_review_path_allowlist_exact_file_allows_patch_scope():
    triage = controller.triage_review_thread_contract(
        {
            "id": "review-allow-exact",
            "body": "security bypass still present",
            "path": "scripts/pr_automation_controller.py",
            "active": True,
        },
        {"files_allowed": ["scripts/pr_automation_controller.py"], "checks_green": False},
    )
    ASSERTIONS.assertEqual(triage["decision"], "PATCH_REQUIRED")
    ASSERTIONS.assertEqual(triage["reason"], "active_safety_or_current_head_failure_or_contract_violation")


def test_review_triage_contract_review_path_allowlist_directory_prefix_allows_patch_scope():
    triage = controller.triage_review_thread_contract(
        {
            "id": "review-allow-dir",
            "body": "security bypass still present",
            "path": "scripts/pr_automation_controller.py",
            "active": True,
        },
        {"files_allowed": ["scripts/"], "checks_green": False},
    )
    ASSERTIONS.assertEqual(triage["decision"], "PATCH_REQUIRED")
    ASSERTIONS.assertEqual(triage["reason"], "active_safety_or_current_head_failure_or_contract_violation")


def test_review_triage_contract_review_path_allowlist_simple_star_prefix_allows_patch_scope():
    triage = controller.triage_review_thread_contract(
        {
            "id": "review-allow-star",
            "body": "security bypass still present",
            "path": "scripts/pr_automation_controller.py",
            "active": True,
        },
        {"files_allowed": ["scripts/*"], "checks_green": False},
    )
    ASSERTIONS.assertEqual(triage["decision"], "PATCH_REQUIRED")
    ASSERTIONS.assertEqual(triage["reason"], "active_safety_or_current_head_failure_or_contract_violation")


def test_review_triage_contract_review_path_outside_allowlist_is_forbidden_file_request():
    triage = controller.triage_review_thread_contract(
        {
            "id": "review-outside-allow",
            "body": "security bypass still present",
            "path": "core/runtime.py",
            "active": True,
        },
        {"files_allowed": ["scripts/"], "checks_green": False},
    )
    ASSERTIONS.assertEqual(triage["decision"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(triage["reason"], "forbidden_file_request")


def test_review_triage_contract_review_path_forbidden_over_allowed_precedence():
    triage = controller.triage_review_thread_contract(
        {
            "id": "review-forbidden-precedence",
            "body": "security bypass still present",
            "path": "scripts/secret.py",
            "active": True,
        },
        {
            "files_allowed": ["scripts/"],
            "files_forbidden": ["scripts/secret.py"],
            "checks_green": False,
        },
    )
    ASSERTIONS.assertEqual(triage["decision"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(triage["reason"], "forbidden_file_request")


def test_review_triage_contract_review_path_missing_or_empty_allowlist_does_not_forbid_by_itself():
    for context in ({}, {"files_allowed": []}, {"files_allowed": ""}, {"files_allowed": None}):
        triage = controller.triage_review_thread_contract(
            {
                "id": f"review-empty-allow-{len(context)}",
                "body": "security bypass still present",
                "path": "scripts/pr_automation_controller.py",
                "active": True,
            },
            dict(context, checks_green=False),
        )
        ASSERTIONS.assertEqual(triage["decision"], "PATCH_REQUIRED")
        ASSERTIONS.assertEqual(triage["reason"], "active_safety_or_current_head_failure_or_contract_violation")


def test_review_triage_contract_review_path_malformed_allowlist_fails_closed():
    cases = [
        [" scripts/pr_automation_controller.py"],
        ["scripts/pr_automation_controller.py "],
        ["scripts/../core/x.py"],
        ["/repo/scripts/pr_automation_controller.py"],
        ["scripts\\pr_automation_controller.py"],
        ["C:\\repo\\scripts\\pr_automation_controller.py"],
    ]
    for files_allowed in cases:
        triage = controller.triage_review_thread_contract(
            {
                "id": f"review-malformed-allow-{files_allowed[0]}",
                "body": "security bypass still present",
                "path": "scripts/pr_automation_controller.py",
                "active": True,
            },
            {"files_allowed": files_allowed, "checks_green": False},
        )
        ASSERTIONS.assertEqual(triage["decision"], "NEEDS_MANUAL")
        ASSERTIONS.assertEqual(triage["reason"], "forbidden_file_request")


def test_review_triage_contract_review_path_unsafe_paths_fail_closed():
    unsafe_paths = [
        "../x",
        "scripts/../core/x.py",
        "/etc/passwd",
        "/home/pickfair/actions-runner/_work/Pickfair-nogui/Pickfair-nogui/scripts/pr_automation_controller.py",
        "scripts\\pr_automation_controller.py",
        "C:\\repo\\scripts\\pr_automation_controller.py",
        " scripts/pr_automation_controller.py",
        "scripts/pr_automation_controller.py ",
    ]
    for path in unsafe_paths:
        triage = controller.triage_review_thread_contract(
            {
                "id": f"review-unsafe-path-{path}",
                "body": "security bypass still present",
                "path": path,
                "active": True,
            },
            {"files_allowed": ["scripts/"], "checks_green": False},
        )
        ASSERTIONS.assertEqual(triage["decision"], "NEEDS_MANUAL")
        ASSERTIONS.assertEqual(triage["reason"], "forbidden_file_request")


def test_review_triage_contract_future_scope_needs_manual():
    triage = controller.triage_review_thread_contract({"id": "f2", "body": "future scope roadmap item"}, {})
    ASSERTIONS.assertEqual(triage["decision"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(triage["reason"], "future_roadmap_scope")


def test_review_triage_contract_later_in_the_flow_is_not_future_scope():
    triage = controller.triage_review_thread_contract(
        {"id": "f2b", "body": "this is handled later in the flow after validation"},
        {"reproducible": True},
    )
    ASSERTIONS.assertEqual(triage["decision"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(triage["reason"], "reproducible_without_patch_signal")


def test_review_triage_contract_style_suggestion_reproducible_is_not_patch_required():
    triage = controller.triage_review_thread_contract(
        {"id": "f2b-style", "body": "style suggestion: align spacing", "active": True, "reproducible": True},
        {"reproducible": True},
    )
    ASSERTIONS.assertEqual(triage["decision"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(triage["reason"], "reproducible_without_patch_signal")


def test_review_triage_contract_docs_suggestion_reproducible_is_not_patch_required():
    triage = controller.triage_review_thread_contract(
        {"id": "f2b-docs", "body": "docs suggestion: clarify this comment", "active": True, "reproducible": True},
        {"reproducible": True},
    )
    ASSERTIONS.assertEqual(triage["decision"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(triage["reason"], "reproducible_without_patch_signal")


def test_review_triage_contract_naming_suggestion_reproducible_is_not_patch_required():
    triage = controller.triage_review_thread_contract(
        {"id": "f2b-naming", "body": "please improve naming", "active": True, "reproducible": True},
        {"reproducible": True},
    )
    ASSERTIONS.assertEqual(triage["decision"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(triage["reason"], "reproducible_without_patch_signal")


def test_review_triage_contract_explicit_reproducible_false_blocks_patch_required():
    phrases = [
        "security regression broken",
        "broken correctness failure in guard",
        "security bypass still present",
    ]
    for index, phrase in enumerate(phrases):
        triage = controller.triage_review_thread_contract(
            {"id": f"explicit-not-repro-{index}", "body": phrase, "active": True, "reproducible": False},
            {"checks_green": False},
        )
        ASSERTIONS.assertEqual(triage["decision"], "NEEDS_MANUAL")
        ASSERTIONS.assertEqual(triage["reason"], "explicitly_not_reproducible")


def test_review_triage_contract_explicit_is_reproducible_false_blocks_patch_required():
    triage = controller.triage_review_thread_contract(
        {
            "id": "explicit-is-not-repro",
            "body": "security bypass still present",
            "active": True,
            "is_reproducible": False,
        },
        {"checks_green": False},
    )
    ASSERTIONS.assertEqual(triage["decision"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(triage["reason"], "explicitly_not_reproducible")


def test_review_triage_contract_context_explicit_reproducible_false_blocks_patch_required():
    triage = controller.triage_review_thread_contract(
        {"id": "context-explicit-not-repro", "body": "this is failing on current head", "active": True},
        {"checks_green": False, "reproducible": False},
    )
    ASSERTIONS.assertEqual(triage["decision"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(triage["reason"], "explicitly_not_reproducible")


def test_review_triage_contract_active_security_bypass_without_reproducible_is_patch_required():
    triage = controller.triage_review_thread_contract(
        {"id": "f2e", "body": "active security bypass in guard path", "active": True},
        {"checks_green": False},
    )
    ASSERTIONS.assertEqual(triage["decision"], "PATCH_REQUIRED")
    ASSERTIONS.assertEqual(triage["reason"], "active_safety_or_current_head_failure_or_contract_violation")


def test_review_triage_contract_active_fail_open_without_reproducible_is_patch_required():
    triage = controller.triage_review_thread_contract(
        {"id": "f2f", "body": "active fail-open route allows unsafe pass-through", "active": True},
        {"checks_green": False},
    )
    ASSERTIONS.assertEqual(triage["decision"], "PATCH_REQUIRED")
    ASSERTIONS.assertEqual(triage["reason"], "active_safety_or_current_head_failure_or_contract_violation")


def test_review_triage_contract_active_failing_current_head_without_reproducible_is_patch_required():
    triage = controller.triage_review_thread_contract(
        {"id": "f2h", "body": "this is failing on current head", "active": True},
        {"checks_green": False},
    )
    ASSERTIONS.assertEqual(triage["decision"], "PATCH_REQUIRED")
    ASSERTIONS.assertEqual(triage["reason"], "active_safety_or_current_head_failure_or_contract_violation")


def test_review_triage_contract_active_broken_correctness_without_reproducible_is_patch_required():
    triage = controller.triage_review_thread_contract(
        {"id": "f2i", "body": "broken correctness failure in guard", "active": True},
        {"checks_green": False},
    )
    ASSERTIONS.assertEqual(triage["decision"], "PATCH_REQUIRED")
    ASSERTIONS.assertEqual(triage["reason"], "active_safety_or_current_head_failure_or_contract_violation")


def test_review_triage_contract_missing_reproducible_metadata_allows_active_failure_patch_required():
    phrases = [
        "this is failing on current head",
        "broken correctness failure in guard",
        "security bypass still present",
    ]
    for index, phrase in enumerate(phrases):
        triage = controller.triage_review_thread_contract(
            {"id": f"missing-repro-active-{index}", "body": phrase, "active": True},
            {"checks_green": False},
        )
        ASSERTIONS.assertEqual(triage["decision"], "PATCH_REQUIRED")
        ASSERTIONS.assertEqual(triage["reason"], "active_safety_or_current_head_failure_or_contract_violation")


def test_review_triage_contract_active_regression_breaks_without_reproducible_is_patch_required():
    triage = controller.triage_review_thread_contract(
        {"id": "f2j", "body": "this regression breaks the resolver", "active": True},
        {"checks_green": False},
    )
    ASSERTIONS.assertEqual(triage["decision"], "PATCH_REQUIRED")
    ASSERTIONS.assertEqual(triage["reason"], "active_safety_or_current_head_failure_or_contract_violation")


def test_review_triage_contract_active_failure_wording_current_head_overrides_fixed_or_stale_context():
    triage = controller.triage_review_thread_contract(
        {
            "id": "safe-override-1",
            "body": "this is failing on current head",
            "active": True,
            "issue_fixed_or_stale": True,
        },
        {
            "issue_fixed_or_stale": True,
            "current_head_sha": "abc",
            "evidence_head_sha": "abc",
            "checks_green": True,
            "pending_checks": False,
            "failing_checks": False,
            "tests": ["pytest -q tests/scripts/test_pr_automation_controller.py -k review_triage"],
        },
    )
    ASSERTIONS.assertEqual(triage["decision"], "PATCH_REQUIRED")
    ASSERTIONS.assertEqual(triage["reason"], "active_safety_or_current_head_failure_or_contract_violation")


def test_review_triage_contract_active_failure_wording_remains_overrides_fixed_or_stale_context():
    triage = controller.triage_review_thread_contract(
        {
            "id": "safe-override-2",
            "body": "security bypass remains in guard path",
            "active": True,
            "issue_fixed_or_stale": True,
        },
        {
            "issue_fixed_or_stale": True,
            "current_head_sha": "abc",
            "evidence_head_sha": "abc",
            "checks_green": True,
            "pending_checks": False,
            "failing_checks": False,
            "tests": ["pytest -q tests/scripts/test_pr_automation_controller.py -k review_triage"],
        },
    )
    ASSERTIONS.assertEqual(triage["decision"], "PATCH_REQUIRED")
    ASSERTIONS.assertEqual(triage["reason"], "active_safety_or_current_head_failure_or_contract_violation")


def test_review_triage_contract_active_failure_wording_matrix_always_routes_patch_required():
    phrases = [
        "this is failing in the resolver",
        "broken state propagation remains",
        "correctness failure in guard evaluation",
        "this regression breaks the resolver",
        "security bypass still present",
        "security bypass remains",
        "this is failing on current head",
    ]
    for index, phrase in enumerate(phrases):
        triage = controller.triage_review_thread_contract(
            {
                "id": f"safe-override-matrix-{index}",
                "body": phrase,
                "active": True,
                "issue_fixed_or_stale": True,
            },
            {
                "issue_fixed_or_stale": True,
                "current_head_sha": "abc",
                "evidence_head_sha": "abc",
                "checks_green": True,
                "pending_checks": False,
                "failing_checks": False,
                "tests": ["pytest -q tests/scripts/test_pr_automation_controller.py -k review_triage"],
            },
        )
        ASSERTIONS.assertEqual(triage["decision"], "PATCH_REQUIRED")
        ASSERTIONS.assertEqual(triage["reason"], "active_safety_or_current_head_failure_or_contract_violation")


def test_review_triage_contract_negated_fixed_stale_failure_wording_routes_patch_required():
    phrases = [
        "not already fixed; the failing behavior is still present",
        "not fixed; broken behavior remains",
        "not resolved; correctness failure still present",
        "stale advisory: the failure remains on current head",
        "stale advisory: regression remains on current head",
        "already fixed? no, the failure remains",
        "already fixed on current head, but the failure remains",
        "already fixed on current head but regression remains",
    ]
    for index, phrase in enumerate(phrases):
        triage = controller.triage_review_thread_contract(
            {
                "id": f"negated-fixed-{index}",
                "body": phrase,
                "active": True,
                "issue_fixed_or_stale": True,
            },
            {
                "issue_fixed_or_stale": True,
                "current_head_sha": "abc",
                "evidence_head_sha": "abc",
                "checks_green": True,
                "pending_checks": False,
                "failing_checks": False,
                "tests": ["pytest -q tests/scripts/test_pr_automation_controller.py -k review_triage"],
            },
        )
        ASSERTIONS.assertEqual(triage["decision"], "PATCH_REQUIRED")
        ASSERTIONS.assertEqual(triage["reason"], "active_safety_or_current_head_failure_or_contract_violation")


def test_review_triage_contract_already_fixed_on_current_head_with_strict_evidence_resolves():
    triage = controller.triage_review_thread_contract(
        {"id": "safe-fixed-head-1", "body": "already fixed on current head", "active": True},
        {
            "current_head_sha": "abc",
            "evidence_head_sha": "abc",
            "checks_green": True,
            "tests": ["pytest"],
        },
    )
    ASSERTIONS.assertEqual(triage["decision"], "EVIDENCE_RESOLVE")


def test_review_triage_contract_already_fixed_no_failure_remains_with_strict_evidence_resolves():
    triage = controller.triage_review_thread_contract(
        {"id": "safe-fixed-head-2", "body": "already fixed; no failure remains", "active": True},
        {
            "current_head_sha": "abc",
            "evidence_head_sha": "abc",
            "checks_green": True,
            "tests": ["pytest"],
        },
    )
    ASSERTIONS.assertEqual(triage["decision"], "EVIDENCE_RESOLVE")


def test_review_triage_contract_already_covered_style_nit_with_strict_evidence_resolves():
    triage = controller.triage_review_thread_contract(
        {"id": "safe-covered-style", "body": "already covered style nit", "active": True},
        {
            "current_head_sha": "abc",
            "evidence_head_sha": "abc",
            "checks_green": True,
            "tests": ["pytest"],
        },
    )
    ASSERTIONS.assertEqual(triage["decision"], "EVIDENCE_RESOLVE")


def test_review_triage_contract_standalone_roadmap_is_future_scope_needs_manual():
    triage = controller.triage_review_thread_contract(
        {"id": "f2d", "body": "roadmap"},
        {"reproducible": True},
    )
    ASSERTIONS.assertEqual(triage["decision"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(triage["reason"], "future_roadmap_scope")


def test_review_triage_contract_roadmap_with_reproducible_metadata_still_needs_manual():
    triage = controller.triage_review_thread_contract(
        {"id": "f2g", "body": "roadmap", "active": True, "reproducible": True},
        {"reproducible": True, "checks_green": True},
    )
    ASSERTIONS.assertEqual(triage["decision"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(triage["reason"], "future_roadmap_scope")


def test_review_triage_contract_active_fixed_security_with_matching_evidence_resolves():
    triage = controller.triage_review_thread_contract(
        {
            "id": "safe-1",
            "body": "security regression previously reported and now already fixed",
            "active": True,
            "issue_fixed_or_stale": True,
        },
        {
            "current_head_sha": "abc",
            "evidence_head_sha": "abc",
            "checks_green": True,
            "tests": ["pytest -q tests/scripts/test_pr_automation_controller.py -k review"],
        },
    )
    ASSERTIONS.assertEqual(triage["decision"], "EVIDENCE_RESOLVE")
    ASSERTIONS.assertEqual(triage["reason"], "stale_or_advisory_with_current_head_evidence")


def test_review_triage_contract_safe_to_resolve_only_fixed_safety_with_matching_evidence_resolves():
    triage = controller.triage_review_thread_contract(
        {
            "id": "safe-safe-to-resolve-1",
            "body": "already fixed security regression",
            "active": True,
        },
        {
            "safe_to_resolve": True,
            "current_head_sha": "abc",
            "evidence_head_sha": "abc",
            "checks_green": True,
            "tests": ["pytest -q tests/scripts/test_pr_automation_controller.py -k review"],
        },
    )
    ASSERTIONS.assertEqual(triage["decision"], "EVIDENCE_RESOLVE")
    ASSERTIONS.assertEqual(triage["reason"], "stale_or_advisory_with_current_head_evidence")


def test_review_triage_contract_active_fixed_security_missing_tests_denies_evidence_resolve():
    triage = controller.triage_review_thread_contract(
        {
            "id": "safe-2",
            "body": "security regression already fixed",
            "active": True,
            "issue_fixed_or_stale": True,
        },
        {"current_head_sha": "abc", "evidence_head_sha": "abc", "checks_green": True},
    )
    ASSERTIONS.assertEqual(triage["decision"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(triage["reason"], "missing_tests_or_evidence")


def test_review_triage_contract_active_fixed_security_pending_checks_denies_evidence_resolve():
    triage = controller.triage_review_thread_contract(
        {
            "id": "safe-3",
            "body": "fail-open path fixed with tests",
            "active": True,
            "issue_fixed_or_stale": True,
        },
        {
            "current_head_sha": "abc",
            "evidence_head_sha": "abc",
            "checks_green": True,
            "tests": ["pytest"],
            "pending_checks": True,
        },
    )
    ASSERTIONS.assertEqual(triage["decision"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(triage["reason"], "checks_or_codacy_block_evidence_resolve")


def test_review_triage_contract_handle_this_later_is_future_scope():
    triage = controller.triage_review_thread_contract({"id": "f2c", "body": "please handle this later"}, {})
    ASSERTIONS.assertEqual(triage["decision"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(triage["reason"], "future_roadmap_scope")


def test_review_triage_contract_outdated_forbidden_workflow_file_still_needs_manual():
    triage = controller.triage_review_thread_contract(
        {"id": "f3", "body": "stale note: please update .github/workflows/ci.yml", "isOutdated": True},
        {"current_head_sha": "abc", "evidence_head_sha": "abc", "checks_green": True, "tests": ["pytest"]},
    )
    ASSERTIONS.assertEqual(triage["decision"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(triage["reason"], "forbidden_file_request")


def test_review_triage_contract_outdated_future_scope_still_needs_manual():
    triage = controller.triage_review_thread_contract(
        {"id": "f4", "body": "outdated thread; future roadmap for next quarter", "isOutdated": True},
        {"current_head_sha": "abc", "evidence_head_sha": "abc", "checks_green": True, "tests": ["pytest"]},
    )
    ASSERTIONS.assertEqual(triage["decision"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(triage["reason"], "future_roadmap_scope")


def test_review_triage_contract_outdated_fail_open_claim_still_patch_required():
    triage = controller.triage_review_thread_contract(
        {"id": "f5", "body": "stale advisory but fail open bypass remains", "isOutdated": True},
        {"current_head_sha": "abc", "evidence_head_sha": "abc", "checks_green": True, "tests": ["pytest"]},
    )
    ASSERTIONS.assertEqual(triage["decision"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(triage["reason"], "inactive_or_resolved_thread")


def test_review_triage_contract_resolved_fail_open_claim_is_not_patch_required():
    triage = controller.triage_review_thread_contract(
        {"id": "f5b", "body": "fail open bypass remains", "isResolved": True, "reproducible": True},
        {"checks_green": False},
    )
    ASSERTIONS.assertEqual(triage["decision"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(triage["reason"], "inactive_or_resolved_thread")


def test_review_triage_contract_inactive_security_bypass_is_not_patch_required():
    triage = controller.triage_review_thread_contract(
        {"id": "f5c", "body": "security bypass remains", "active": False},
        {"checks_green": False},
    )
    ASSERTIONS.assertEqual(triage["decision"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(triage["reason"], "inactive_or_resolved_thread")


def test_review_triage_contract_missing_tests_or_evidence_denies_evidence_resolve():
    triage = controller.triage_review_thread_contract(
        {"id": "e1", "body": "stale advisory"},
        {"current_head_sha": "abc", "evidence_head_sha": "abc", "checks_green": True},
    )
    ASSERTIONS.assertEqual(triage["decision"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(triage["reason"], "missing_tests_or_evidence")


def test_review_triage_contract_missing_checks_green_denies_evidence_resolve():
    triage = controller.triage_review_thread_contract(
        {"id": "e1b", "body": "stale advisory"},
        {"current_head_sha": "abc", "evidence_head_sha": "abc", "tests": ["pytest"]},
    )
    ASSERTIONS.assertEqual(triage["decision"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(triage["reason"], "checks_or_codacy_block_evidence_resolve")


def test_review_triage_contract_uses_author_extracted_from_comments_nodes():
    triage = controller.triage_review_thread_contract(
        {
            "id": "author-1",
            "body": "stale advisory",
            "comments": {"nodes": [{"author": {"login": "coderabbitai[bot]"}, "body": "nit"}]},
        },
        {"current_head_sha": "abc", "evidence_head_sha": "abc", "checks_green": True, "tests": ["pytest"]},
    )
    ASSERTIONS.assertEqual(triage["author"], "coderabbitai")


def test_review_triage_contract_checks_green_false_denies_evidence_resolve():
    triage = controller.triage_review_thread_contract(
        {"id": "e1c", "body": "stale advisory"},
        {"current_head_sha": "abc", "evidence_head_sha": "abc", "checks_green": False, "tests": ["pytest"]},
    )
    ASSERTIONS.assertEqual(triage["decision"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(triage["reason"], "checks_or_codacy_block_evidence_resolve")


def test_review_triage_contract_codacy_action_required_blocks_evidence_resolve():
    triage = controller.triage_review_thread_contract(
        {"id": "e2", "body": "already fixed stale"},
        {
            "current_head_sha": "abc",
            "evidence_head_sha": "abc",
            "checks_green": True,
            "tests": ["pytest"],
            "github_codacy_state": "ACTION_REQUIRED",
        },
    )
    ASSERTIONS.assertEqual(triage["decision"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(triage["reason"], "checks_or_codacy_block_evidence_resolve")


def test_review_triage_contract_context_evidence_present_without_tests_denies_evidence_resolve():
    triage = controller.triage_review_thread_contract(
        {"id": "e3", "body": "already fixed stale"},
        {
            "current_head_sha": "abc",
            "evidence_head_sha": "abc",
            "checks_green": True,
            "evidence_present": True,
        },
    )
    ASSERTIONS.assertEqual(triage["decision"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(triage["reason"], "missing_tests_or_evidence")


def test_review_triage_contract_non_scalar_tests_covering_behavior_denies_evidence_resolve():
    triage = controller.triage_review_thread_contract(
        {"id": "e4", "body": "already fixed stale"},
        {"current_head_sha": "abc", "evidence_head_sha": "abc", "checks_green": True, "tests_covering_behavior": [{}]},
    )
    ASSERTIONS.assertEqual(triage["tests_covering_behavior"], [])
    ASSERTIONS.assertEqual(triage["decision"], "NEEDS_MANUAL")
    ASSERTIONS.assertEqual(triage["reason"], "missing_tests_or_evidence")


def test_review_triage_contract_mixed_scalar_and_non_scalar_tests_covering_behavior_keeps_scalars_only():
    triage = controller.triage_review_thread_contract(
        {"id": "e4b", "body": "already fixed stale"},
        {
            "current_head_sha": "abc",
            "evidence_head_sha": "abc",
            "checks_green": True,
            "tests_covering_behavior": [None, {}, [], (), set(), "test_alpha", "  ", 42, 3.5, False],
        },
    )
    ASSERTIONS.assertEqual(triage["tests_covering_behavior"], ["test_alpha", "42", "3.5", "False"])
    ASSERTIONS.assertEqual(triage["decision"], "EVIDENCE_RESOLVE")
    ASSERTIONS.assertEqual(triage["reason"], "stale_or_advisory_with_current_head_evidence")


def test_review_triage_contract_scalar_tests_covering_behavior_allows_evidence_resolve():
    triage = controller.triage_review_thread_contract(
        {"id": "e5", "body": "already fixed stale"},
        {
            "current_head_sha": "abc",
            "evidence_head_sha": "abc",
            "checks_green": True,
            "tests_covering_behavior": ["tests/scripts/test_pr_automation_controller.py::test_example_behavior"],
        },
    )
    ASSERTIONS.assertEqual(
        triage["tests_covering_behavior"],
        ["tests/scripts/test_pr_automation_controller.py::test_example_behavior"],
    )
    ASSERTIONS.assertEqual(triage["decision"], "EVIDENCE_RESOLVE")
    ASSERTIONS.assertEqual(triage["reason"], "stale_or_advisory_with_current_head_evidence")


def test_should_resolve_review_thread_requires_deterministic_evidence_resolve_result():
    thread = {"id": "r1", "body": "stale advisory", "isResolved": False, "isOutdated": True}
    denied = controller.should_resolve_review_thread(
        thread,
        {"validation_passed": True, "current_head_sha": "abc", "evidence_head_sha": "def", "tests": ["pytest"]},
    )
    ASSERTIONS.assertFalse(denied)


def test_should_resolve_review_thread_fixed_safety_requires_deterministic_evidence():
    thread = {
        "id": "safe-r1",
        "body": "security bypass fixed now",
        "active": True,
        "issue_fixed_or_stale": True,
        "comments": {"nodes": [{"author": {"login": "coderabbitai[bot]"}, "body": "security bypass fixed"}]},
    }
    allowed = controller.should_resolve_review_thread(
        thread,
        {
            "validation_passed": True,
            "issue_fixed_or_stale": True,
            "current_head_sha": "abc",
            "evidence_head_sha": "abc",
            "checks_green": True,
            "tests": ["pytest"],
            "files_allowed": ["scripts/pr_automation_controller.py", "tests/scripts/test_pr_automation_controller.py"],
        },
    )
    ASSERTIONS.assertTrue(allowed)


def test_should_resolve_review_thread_fixed_safety_denied_on_head_mismatch_or_failing_checks():
    thread = {
        "id": "safe-r2",
        "body": "security bypass fixed now",
        "active": True,
        "issue_fixed_or_stale": True,
        "comments": {"nodes": [{"author": {"login": "coderabbitai[bot]"}, "body": "security bypass fixed"}]},
    }
    mismatch = controller.should_resolve_review_thread(
        thread,
        {
            "validation_passed": True,
            "issue_fixed_or_stale": True,
            "current_head_sha": "abc",
            "evidence_head_sha": "def",
            "checks_green": True,
            "tests": ["pytest"],
            "files_allowed": ["scripts/pr_automation_controller.py", "tests/scripts/test_pr_automation_controller.py"],
        },
    )
    failing = controller.should_resolve_review_thread(
        thread,
        {
            "validation_passed": True,
            "issue_fixed_or_stale": True,
            "current_head_sha": "abc",
            "evidence_head_sha": "abc",
            "checks_green": True,
            "tests": ["pytest"],
            "failing_checks": True,
            "files_allowed": ["scripts/pr_automation_controller.py", "tests/scripts/test_pr_automation_controller.py"],
        },
    )
    ASSERTIONS.assertFalse(mismatch)
    ASSERTIONS.assertFalse(failing)


def test_should_resolve_review_thread_fixed_safety_with_full_evidence_returns_true():
    thread = {
        "id": "safe-r3",
        "body": "already fixed security regression",
        "active": True,
        "issue_fixed_or_stale": True,
        "comments": {"nodes": [{"author": {"login": "coderabbitai[bot]"}, "body": "already fixed"}]},
    }
    evidence = {
        "validation_passed": True,
        "issue_fixed_or_stale": True,
        "current_head_sha": "abc",
        "evidence_head_sha": "abc",
        "checks_green": True,
        "pending_checks": False,
        "failing_checks": False,
        "tests": ["pytest -q tests/scripts/test_pr_automation_controller.py -k should_resolve_review_thread"],
    }
    ASSERTIONS.assertEqual(controller.triage_review_thread_contract(thread, evidence)["decision"], "EVIDENCE_RESOLVE")
    ASSERTIONS.assertTrue(controller.should_resolve_review_thread(thread, evidence))


def test_should_resolve_review_thread_fixed_safety_safe_to_resolve_only_with_full_evidence_returns_true():
    thread = {
        "id": "safe-r3-safe-to-resolve",
        "body": "already fixed security regression",
        "active": True,
        "comments": {"nodes": [{"author": {"login": "coderabbitai[bot]"}, "body": "already fixed"}]},
    }
    evidence = {
        "validation_passed": True,
        "safe_to_resolve": True,
        "current_head_sha": "abc",
        "evidence_head_sha": "abc",
        "checks_green": True,
        "pending_checks": False,
        "failing_checks": False,
        "tests": ["pytest -q tests/scripts/test_pr_automation_controller.py -k should_resolve_review_thread"],
    }
    ASSERTIONS.assertEqual(controller.triage_review_thread_contract(thread, evidence)["decision"], "EVIDENCE_RESOLVE")
    ASSERTIONS.assertTrue(controller.should_resolve_review_thread(thread, evidence))


def test_should_resolve_review_thread_already_covered_style_nit_with_full_evidence_returns_true():
    thread = {
        "id": "safe-r3-style",
        "body": "already covered style nit",
        "active": True,
        "issue_fixed_or_stale": True,
        "comments": {"nodes": [{"author": {"login": "coderabbitai[bot]"}, "body": "already covered"}]},
    }
    evidence = {
        "validation_passed": True,
        "issue_fixed_or_stale": True,
        "current_head_sha": "abc",
        "evidence_head_sha": "abc",
        "checks_green": True,
        "pending_checks": False,
        "failing_checks": False,
        "tests": ["pytest -q tests/scripts/test_pr_automation_controller.py -k should_resolve_review_thread"],
    }
    ASSERTIONS.assertEqual(controller.triage_review_thread_contract(thread, evidence)["decision"], "EVIDENCE_RESOLVE")
    ASSERTIONS.assertTrue(controller.should_resolve_review_thread(thread, evidence))


def test_should_resolve_review_thread_denies_when_validation_passed_missing():
    thread = {
        "id": "safe-r3b",
        "body": "already fixed security regression",
        "active": True,
        "issue_fixed_or_stale": True,
    }
    evidence = {
        "issue_fixed_or_stale": True,
        "current_head_sha": "abc",
        "evidence_head_sha": "abc",
        "checks_green": True,
        "pending_checks": False,
        "failing_checks": False,
        "tests": ["pytest"],
    }
    ASSERTIONS.assertEqual(controller.triage_review_thread_contract(thread, evidence)["decision"], "EVIDENCE_RESOLVE")
    ASSERTIONS.assertFalse(controller.should_resolve_review_thread(thread, evidence))


def test_should_resolve_review_thread_denies_when_validation_passed_false():
    thread = {
        "id": "safe-r3c",
        "body": "already fixed security regression",
        "active": True,
        "issue_fixed_or_stale": True,
    }
    evidence = {
        "validation_passed": False,
        "issue_fixed_or_stale": True,
        "current_head_sha": "abc",
        "evidence_head_sha": "abc",
        "checks_green": True,
        "pending_checks": False,
        "failing_checks": False,
        "tests": ["pytest"],
    }
    ASSERTIONS.assertEqual(controller.triage_review_thread_contract(thread, evidence)["decision"], "EVIDENCE_RESOLVE")
    ASSERTIONS.assertFalse(controller.should_resolve_review_thread(thread, evidence))


def test_should_resolve_review_thread_fixed_current_head_note_validation_gate_contract():
    thread = {
        "id": "safe-r3d",
        "body": "already fixed on current head",
        "active": True,
        "issue_fixed_or_stale": True,
        "comments": {"nodes": [{"author": {"login": "coderabbitai[bot]"}, "body": "already fixed"}]},
    }
    base = {
        "issue_fixed_or_stale": True,
        "current_head_sha": "abc",
        "evidence_head_sha": "abc",
        "checks_green": True,
        "pending_checks": False,
        "failing_checks": False,
        "tests": ["pytest"],
    }
    ASSERTIONS.assertEqual(controller.triage_review_thread_contract(thread, base)["decision"], "EVIDENCE_RESOLVE")
    ASSERTIONS.assertFalse(controller.should_resolve_review_thread(thread, base))
    ASSERTIONS.assertFalse(controller.should_resolve_review_thread(thread, dict(base) | {"validation_passed": False}))
    ASSERTIONS.assertTrue(controller.should_resolve_review_thread(thread, dict(base) | {"validation_passed": True}))


def test_should_resolve_review_thread_fixed_safety_with_pending_checks_returns_false():
    thread = {
        "id": "safe-r4",
        "body": "already fixed security regression",
        "active": True,
        "issue_fixed_or_stale": True,
    }
    evidence = {
        "issue_fixed_or_stale": True,
        "current_head_sha": "abc",
        "evidence_head_sha": "abc",
        "checks_green": True,
        "pending_checks": True,
        "failing_checks": False,
        "tests": ["pytest"],
    }
    ASSERTIONS.assertFalse(controller.should_resolve_review_thread(thread, evidence))


def test_should_resolve_review_thread_fixed_safety_with_head_mismatch_returns_false():
    thread = {
        "id": "safe-r5",
        "body": "already fixed security regression",
        "active": True,
        "issue_fixed_or_stale": True,
    }
    evidence = {
        "issue_fixed_or_stale": True,
        "current_head_sha": "abc",
        "evidence_head_sha": "def",
        "checks_green": True,
        "pending_checks": False,
        "failing_checks": False,
        "tests": ["pytest"],
    }
    ASSERTIONS.assertFalse(controller.should_resolve_review_thread(thread, evidence))


def test_should_resolve_review_thread_active_unresolved_bypass_returns_false():
    thread = {
        "id": "safe-r6",
        "body": "active unresolved bypass in guard path",
        "active": True,
    }
    evidence = {
        "current_head_sha": "abc",
        "evidence_head_sha": "abc",
        "checks_green": True,
        "pending_checks": False,
        "failing_checks": False,
        "tests": ["pytest"],
    }
    ASSERTIONS.assertEqual(controller.triage_review_thread_contract(thread, evidence)["decision"], "PATCH_REQUIRED")
    ASSERTIONS.assertFalse(controller.should_resolve_review_thread(thread, evidence))


def test_should_resolve_review_thread_active_security_bypass_still_present_ignores_safe_to_resolve():
    thread = {
        "id": "safe-r7",
        "body": "security bypass still present",
        "active": True,
    }
    evidence = {
        "safe_to_resolve": True,
        "issue_fixed_or_stale": False,
        "current_head_sha": "abc",
        "evidence_head_sha": "abc",
        "checks_green": True,
        "pending_checks": False,
        "failing_checks": False,
        "tests": ["pytest"],
    }
    ASSERTIONS.assertEqual(controller.triage_review_thread_contract(thread, evidence)["decision"], "PATCH_REQUIRED")
    ASSERTIONS.assertFalse(controller.should_resolve_review_thread(thread, evidence))


def test_should_resolve_review_thread_active_fail_open_still_present_ignores_safe_to_resolve():
    thread = {
        "id": "safe-r8",
        "body": "active fail-open still present",
        "active": True,
    }
    evidence = {
        "safe_to_resolve": True,
        "issue_fixed_or_stale": False,
        "current_head_sha": "abc",
        "evidence_head_sha": "abc",
        "checks_green": True,
        "pending_checks": False,
        "failing_checks": False,
        "tests": ["pytest"],
    }
    ASSERTIONS.assertEqual(controller.triage_review_thread_contract(thread, evidence)["decision"], "PATCH_REQUIRED")
    ASSERTIONS.assertFalse(controller.should_resolve_review_thread(thread, evidence))


def test_should_resolve_review_thread_roadmap_scope_ignores_safe_to_resolve():
    thread = {
        "id": "safe-r9",
        "body": "roadmap",
        "active": True,
    }
    evidence = {
        "safe_to_resolve": True,
        "current_head_sha": "abc",
        "evidence_head_sha": "abc",
        "checks_green": True,
        "pending_checks": False,
        "failing_checks": False,
        "tests": ["pytest"],
    }
    ASSERTIONS.assertEqual(controller.triage_review_thread_contract(thread, evidence)["decision"], "NEEDS_MANUAL")
    ASSERTIONS.assertFalse(controller.should_resolve_review_thread(thread, evidence))


def test_build_review_triage_matrix_mixed_decisions_patch_required_outranks_other_actions():
    comments = [
        {"thread_id": "p", "body": "Active bypass failure in guard path", "active": True, "reproducible": True},
        {"thread_id": "e", "body": "already fixed by matrix", "active": True, "covered_by_matrix": True},
        {"thread_id": "m", "body": "future scope roadmap; ambiguous architecture", "active": True},
    ]
    matrix = controller.build_review_triage_matrix(comments, {"checks_green": True, "standing_owner_authorized": True})
    ASSERTIONS.assertEqual(matrix["next_action"], "patch_required")


def test_standing_owner_authorization_narrow_in_scope_allowed():
    result = controller.evaluate_standing_owner_authorization(
        {
            "phase0_passed": True,
            "changed_files": ["scripts/pr_automation_controller.py"],
            "files_allowed": ["scripts/pr_automation_controller.py", "tests/scripts/test_pr_automation_controller.py"],
            "files_forbidden": [".github/workflows/*"],
            "tests_required": True,
            "tests_prove_behavior": True,
        }
    )
    ASSERTIONS.assertTrue(result["authorized"])
    ASSERTIONS.assertTrue(result["allowed"])
    ASSERTIONS.assertFalse(result["needs_manual"])
    ASSERTIONS.assertEqual(result["reasons"], [])


def test_standing_owner_authorization_blocks_for_forbidden_workflow_live_secrets_future_scope():
    result = controller.evaluate_standing_owner_authorization(
        {
            "changed_files": [".github/workflows/pr.yml", "scripts/pr_automation_controller.py"],
            "files_allowed": ["scripts/pr_automation_controller.py"],
            "files_forbidden": [".github/workflows/*"],
            "workflow_edits": True,
            "live_action": True,
            "external_api_activation": True,
            "secrets_or_provider_config": True,
            "future_scope": True,
            "tests_required": False,
            "phase0_passed": False,
        }
    )
    ASSERTIONS.assertFalse(result["authorized"])
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertTrue(result["needs_manual"])
    ASSERTIONS.assertIn("scope_violation", result["reasons"])
    ASSERTIONS.assertIn("forbidden_files_touched", result["reasons"])
    ASSERTIONS.assertIn("files_outside_allowed", result["reasons"])
    ASSERTIONS.assertIn("workflow_edits_without_explicit_scope", result["reasons"])
    ASSERTIONS.assertIn("live_action_or_automation_activation_blocked", result["reasons"])
    ASSERTIONS.assertIn("external_api_activation_blocked", result["reasons"])
    ASSERTIONS.assertIn("secrets_or_provider_config_blocked", result["reasons"])
    ASSERTIONS.assertIn("future_roadmap_scope_blocked", result["reasons"])
    ASSERTIONS.assertNotIn("tests_cannot_prove_behavior", result["reasons"])
    ASSERTIONS.assertIn("phase0_not_passed", result["reasons"])


def test_standing_owner_authorization_tests_not_required_does_not_block_by_itself():
    result = controller.evaluate_standing_owner_authorization(
        {
            "phase0_passed": True,
            "changed_files": ["scripts/pr_automation_controller.py"],
            "files_allowed": ["scripts/pr_automation_controller.py", "tests/scripts/test_pr_automation_controller.py"],
            "files_forbidden": [".github/workflows/*"],
            "tests_required": False,
        }
    )
    ASSERTIONS.assertTrue(result["authorized"])
    ASSERTIONS.assertNotIn("tests_cannot_prove_behavior", result["reasons"])


def test_standing_owner_authorization_tests_required_missing_proof_blocks():
    result = controller.evaluate_standing_owner_authorization(
        {
            "phase0_passed": True,
            "changed_files": ["scripts/pr_automation_controller.py"],
            "files_allowed": ["scripts/pr_automation_controller.py", "tests/scripts/test_pr_automation_controller.py"],
            "files_forbidden": [".github/workflows/*"],
            "tests_required": True,
            "tests_prove_behavior": None,
        }
    )
    ASSERTIONS.assertFalse(result["authorized"])
    ASSERTIONS.assertIn("tests_cannot_prove_behavior", result["reasons"])


def test_standing_owner_authorization_missing_phase0_signals_fails_closed():
    result = controller.evaluate_standing_owner_authorization(
        {
            "changed_files": ["scripts/pr_automation_controller.py"],
            "files_allowed": ["scripts/pr_automation_controller.py", "tests/scripts/test_pr_automation_controller.py"],
            "files_forbidden": [".github/workflows/*"],
            "tests_required": False,
        }
    )
    ASSERTIONS.assertFalse(result["authorized"])
    ASSERTIONS.assertIn("phase0_not_passed", result["reasons"])


def test_standing_owner_authorization_missing_changed_files_scope_fails_closed():
    result = controller.evaluate_standing_owner_authorization(
        {
            "phase0_passed": True,
            "files_allowed": ["scripts/pr_automation_controller.py", "tests/scripts/test_pr_automation_controller.py"],
            "files_forbidden": [".github/workflows/*"],
            "tests_required": False,
        }
    )
    ASSERTIONS.assertFalse(result["authorized"])
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertTrue(result["needs_manual"])
    ASSERTIONS.assertIn("changed_files_missing", result["reasons"])
    ASSERTIONS.assertIn("scope_unknown", result["reasons"])


def test_standing_owner_authorization_empty_changed_files_scope_fails_closed():
    for key in ("changed_files", "files_touched", "modified_files"):
        result = controller.evaluate_standing_owner_authorization(
            {
                "phase0_passed": True,
                key: [],
                "files_allowed": [
                    "scripts/pr_automation_controller.py",
                    "tests/scripts/test_pr_automation_controller.py",
                ],
                "files_forbidden": [".github/workflows/*"],
                "tests_required": False,
            }
        )
        ASSERTIONS.assertFalse(result["authorized"])
        ASSERTIONS.assertIn("changed_files_missing", result["reasons"])
        ASSERTIONS.assertIn("scope_unknown", result["reasons"])


def test_standing_owner_authorization_uses_modified_files_as_valid_scope_source():
    result = controller.evaluate_standing_owner_authorization(
        {
            "phase0_passed": True,
            "modified_files": ["scripts/pr_automation_controller.py"],
            "files_allowed": ["scripts/pr_automation_controller.py", "tests/scripts/test_pr_automation_controller.py"],
            "files_forbidden": [".github/workflows/*"],
            "tests_required": False,
        }
    )
    ASSERTIONS.assertTrue(result["authorized"])
    ASSERTIONS.assertTrue(result["allowed"])
    ASSERTIONS.assertFalse(result["needs_manual"])
    ASSERTIONS.assertNotIn("changed_files_missing", result["reasons"])
    ASSERTIONS.assertNotIn("scope_unknown", result["reasons"])


def test_standing_owner_authorization_non_boolean_phase0_values_fail_closed():
    for value in ("false", "0", "no", "yes", "1"):
        result = controller.evaluate_standing_owner_authorization(
            {
                "phase0_passed": value,
                "changed_files": ["scripts/pr_automation_controller.py"],
                "files_allowed": [
                    "scripts/pr_automation_controller.py",
                    "tests/scripts/test_pr_automation_controller.py",
                ],
                "files_forbidden": [".github/workflows/*"],
                "tests_required": False,
            }
        )
        ASSERTIONS.assertFalse(result["authorized"])
        ASSERTIONS.assertIn("phase0_not_passed", result["reasons"])


def test_manual_unblock_reason_classification_mapping_required_cases():
    cases = [
        ("current_head_verification_blocked", "current_head_verification_blocked"),
        ("matrix_phase0_missing_required_fields", "matrix_phase0_missing_required_fields"),
        ("matrix_phase0_needs_manual", "matrix_phase0_needs_manual"),
        ("review_triage_needs_manual", "review_triage_needs_manual"),
        ("forbidden_files_required", "forbidden_files_required"),
        ("workflow_edit_not_authorized", "workflow_edit_not_authorized"),
        ("live_action_requested_while_disabled", "live_action_requested_while_disabled"),
        ("tests_cannot_prove_behavior", "tests_cannot_prove_behavior"),
        ("ambiguous_architecture", "ambiguous_architecture"),
        ("future_roadmap_scope", "future_roadmap_scope"),
    ]
    for key, expected in cases:
        ASSERTIONS.assertEqual(controller.classify_manual_unblock_reason({"reason": key}), expected)


def test_manual_unblock_reason_classification_accepts_required_raw_string_inputs():
    cases = [
        ("current-head verification blocked", "current_head_verification_blocked"),
        ("matrix phase0 missing required fields", "matrix_phase0_missing_required_fields"),
        ("matrix phase0 needs_manual", "matrix_phase0_needs_manual"),
        ("review triage needs_manual", "review_triage_needs_manual"),
        ("forbidden files required", "forbidden_files_required"),
        ("workflow edit not authorized", "workflow_edit_not_authorized"),
        ("live action requested while disabled", "live_action_requested_while_disabled"),
        ("tests cannot prove behavior", "tests_cannot_prove_behavior"),
        ("ambiguous architecture", "ambiguous_architecture"),
        ("future roadmap scope", "future_roadmap_scope"),
    ]
    for raw_reason, expected in cases:
        ASSERTIONS.assertEqual(controller.classify_manual_unblock_reason(raw_reason), expected)


def test_manual_unblock_reason_classification_unknown_raw_string_falls_back_to_ambiguous_architecture():
    ASSERTIONS.assertEqual(
        controller.classify_manual_unblock_reason("some unknown reason text"),
        "ambiguous_architecture",
    )


def test_manual_unblock_reason_classification_boolean_flag_contexts_and_fallbacks():
    ASSERTIONS.assertEqual(
        controller.classify_manual_unblock_reason({"matrix_phase0_needs_manual": True}),
        "matrix_phase0_needs_manual",
    )
    ASSERTIONS.assertEqual(
        controller.classify_manual_unblock_reason({"review_triage_needs_manual": True}),
        "review_triage_needs_manual",
    )
    ASSERTIONS.assertEqual(
        controller.classify_manual_unblock_reason(
            {"matrix_phase0_needs_manual": True, "review_triage_needs_manual": True}
        ),
        "matrix_phase0_needs_manual",
    )
    for raw in (None, {}, 42, []):
        ASSERTIONS.assertEqual(controller.classify_manual_unblock_reason(raw), "ambiguous_architecture")


def test_manual_unblock_reason_classification_maps_standing_auth_reason_tokens_from_reasons_list():
    ASSERTIONS.assertEqual(
        controller.classify_manual_unblock_reason({"reasons": ["phase0_not_passed"]}),
        "matrix_phase0_needs_manual",
    )
    ASSERTIONS.assertEqual(
        controller.classify_manual_unblock_reason({"reasons": ["forbidden_files_touched"]}),
        "forbidden_files_required",
    )
    ASSERTIONS.assertEqual(
        controller.classify_manual_unblock_reason({"reasons": ["workflow_edits_without_explicit_scope"]}),
        "workflow_edit_not_authorized",
    )
    ASSERTIONS.assertEqual(
        controller.classify_manual_unblock_reason({"reasons": ["live_action_or_automation_activation_blocked"]}),
        "live_action_requested_while_disabled",
    )
    ASSERTIONS.assertEqual(
        controller.classify_manual_unblock_reason({"reasons": ["future_roadmap_scope_blocked"]}),
        "future_roadmap_scope",
    )


def test_manual_unblock_reason_classification_maps_standing_auth_reason_tokens_from_raw_reason_string():
    ASSERTIONS.assertEqual(
        controller.classify_manual_unblock_reason({"reasons": "forbidden_files_touched"}),
        "forbidden_files_required",
    )
    ASSERTIONS.assertEqual(
        controller.classify_manual_unblock_reason({"reasons": "future_roadmap_scope_blocked"}),
        "future_roadmap_scope",
    )


def test_manual_unblock_reason_classification_maps_phase0_manual_next_actions_to_matrix_phase0_needs_manual():
    for next_action in (
        "needs_manual_phase0_blocked",
        "needs_manual_phase0_failed",
        "needs_manual_phase0_malformed",
        "needs_manual_phase0_invalid_status",
        "needs_manual_phase0_invalid_action",
        "needs_manual_phase0_missing_output",
    ):
        ASSERTIONS.assertEqual(
            controller.classify_manual_unblock_reason({"status": "NEEDS_MANUAL", "next_action": next_action}),
            "matrix_phase0_needs_manual",
        )


def test_manual_unblock_reason_classification_preserves_matrix_phase0_missing_required_fields_priority():
    ASSERTIONS.assertEqual(
        controller.classify_manual_unblock_reason(
            {
                "status": "NEEDS_MANUAL",
                "next_action": "needs_manual_matrix_phase0_missing",
                "missing_fields": ["files_allowed"],
            }
        ),
        "matrix_phase0_missing_required_fields",
    )


def test_manual_unblock_authorization_text_exact_format():
    text = controller.build_manual_authorization_text("PR10E", "matrix_phase0_needs_manual")
    ASSERTIONS.assertEqual(text, "AUTORIZZO PATCH PR10E NEEDS_MANUAL per matrix_phase0_needs_manual")


def test_manual_unblock_authorization_text_fallbacks_keep_format():
    ASSERTIONS.assertEqual(
        controller.build_manual_authorization_text(None, None),
        "AUTORIZZO PATCH PR NEEDS_MANUAL per ambiguous_architecture",
    )
    ASSERTIONS.assertEqual(
        controller.build_manual_authorization_text("   ", "   "),
        "AUTORIZZO PATCH PR NEEDS_MANUAL per ambiguous_architecture",
    )
    ASSERTIONS.assertEqual(
        controller.build_manual_authorization_text(123, 999),
        "AUTORIZZO PATCH 123 NEEDS_MANUAL per 999",
    )


def test_manual_unblock_command_is_single_line_and_deterministic():
    first = controller.build_manual_unblock_command("PR10E", "225", "abc123", "matrix_phase0_needs_manual")
    second = controller.build_manual_unblock_command("PR10E", "225", "abc123", "matrix_phase0_needs_manual")
    ASSERTIONS.assertEqual(first, second)
    ASSERTIONS.assertNotIn("\n", first)
    ASSERTIONS.assertEqual(
        first,
        (
            'gh pr view -R zarbopiero963-droid/Pickfair-nogui '
            '--json number,headRefOid,mergeStateStatus,mergeable,url -- 225'
        ),
    )


def test_manual_unblock_command_uses_supported_contract_and_quotes_untrusted_pr_values():
    command = controller.build_manual_unblock_command(
        "PR10E",
        "-246 with space",
        "abc123",
        "matrix_phase0_needs_manual",
    )
    ASSERTIONS.assertNotIn("/tmp/check-pr-status.sh", command)
    ASSERTIONS.assertIn("gh pr view", command)
    ASSERTIONS.assertNotIn("--manual-unblock", command)
    ASSERTIONS.assertNotIn("--pr-name", command)
    ASSERTIONS.assertNotIn("--pr-number", command)
    ASSERTIONS.assertNotIn("--head-sha", command)
    ASSERTIONS.assertNotIn("--reason", command)
    ASSERTIONS.assertNotIn("--mode", command)
    for keyword in ("gh api", "gh run", "git push", "git commit", "resolveReviewThread"):
        ASSERTIONS.assertNotIn(keyword, command)
    ASSERTIONS.assertEqual(
        command,
        (
            'gh pr view -R zarbopiero963-droid/Pickfair-nogui '
            "--json number,headRefOid,mergeStateStatus,mergeable,url -- '-246 with space'"
        ),
    )
    ASSERTIONS.assertNotIn("\n", command)


def test_manual_unblock_command_neutralizes_dash_prefixed_pr_selectors():
    for selector in ("--json", "--live", "-246"):
        command = controller.build_manual_unblock_command("PR10E", selector, "abc123", "matrix_phase0_needs_manual")
        ASSERTIONS.assertEqual(
            command,
            "gh pr view -R zarbopiero963-droid/Pickfair-nogui "
            "--json number,headRefOid,mergeStateStatus,mergeable,url -- "
            + shlex.quote(selector),
        )
        ASSERTIONS.assertIn(" -- ", command)


def test_manual_unblock_command_supports_explicit_repo_override():
    command = controller.build_manual_unblock_command(
        "PR10E",
        "246",
        "abc123",
        "matrix_phase0_needs_manual",
        "custom/repo",
    )
    ASSERTIONS.assertEqual(
        command,
        "gh pr view -R custom/repo --json number,headRefOid,mergeStateStatus,mergeable,url -- 246",
    )


def test_manual_unblock_command_strips_newlines_from_pr_and_repo_to_remain_single_line():
    command = controller.build_manual_unblock_command(
        "PR10E",
        "246\n--json",
        "abc123",
        "matrix_phase0_needs_manual",
        "custom/repo\ninjected",
    )
    ASSERTIONS.assertNotIn("\n", command)
    ASSERTIONS.assertEqual(
        command,
        "gh pr view -R 'custom/repo injected' --json number,headRefOid,mergeStateStatus,mergeable,url -- '246 --json'",
    )


def test_manual_unblock_telegram_message_contains_pr_head_scope_reason():
    package = {
        "pr_number": "225",
        "head_sha": "abc123",
        "files_allowed": ["scripts/pr_automation_controller.py"],
        "files_forbidden": [".github/workflows/*"],
        "reason": "review_triage_needs_manual",
        "next_action": "needs_manual",
    }
    message = controller.build_manual_unblock_telegram_message(package)
    ASSERTIONS.assertIn("PR: 225", message)
    ASSERTIONS.assertIn("Head: abc123", message)
    ASSERTIONS.assertIn("Scope allowed: scripts/pr_automation_controller.py", message)
    ASSERTIONS.assertIn("Scope forbidden: .github/workflows/*", message)
    ASSERTIONS.assertIn("Reason: review_triage_needs_manual", message)


def test_manual_unblock_telegram_message_defaults_scope_to_none_for_empty_payload():
    message_none = controller.build_manual_unblock_telegram_message(None)
    ASSERTIONS.assertIn("Scope allowed: (none)", message_none)
    ASSERTIONS.assertIn("Scope forbidden: (none)", message_none)
    message_empty = controller.build_manual_unblock_telegram_message({})
    ASSERTIONS.assertIn("Scope allowed: (none)", message_empty)
    ASSERTIONS.assertIn("Scope forbidden: (none)", message_empty)


def test_manual_unblock_telegram_message_coerces_non_string_scope_entries():
    package = {
        "pr_number": "225",
        "head_sha": "abc123",
        "files_allowed": [None, 42, " scripts/pr_automation_controller.py "],
        "files_forbidden": ["", {"x": 1}, " .github/workflows/* "],
        "reason": "review_triage_needs_manual",
        "next_action": "needs_manual",
    }
    message = controller.build_manual_unblock_telegram_message(package)
    ASSERTIONS.assertIn("Scope allowed: 42, scripts/pr_automation_controller.py", message)
    ASSERTIONS.assertIn("Scope forbidden: {'x': 1}, .github/workflows/*", message)


def test_manual_unblock_telegram_message_treats_raw_string_scope_as_single_entry():
    package = {
        "files_allowed": " scripts/pr_automation_controller.py ",
        "files_forbidden": " .github/workflows/* ",
    }
    message = controller.build_manual_unblock_telegram_message(package)
    ASSERTIONS.assertIn("Scope allowed: scripts/pr_automation_controller.py", message)
    ASSERTIONS.assertIn("Scope forbidden: .github/workflows/*", message)


def test_manual_unblock_telegram_message_handles_none_empty_and_mixed_scope_safely():
    message_none = controller.build_manual_unblock_telegram_message(
        {"files_allowed": None, "files_forbidden": None}
    )
    ASSERTIONS.assertIn("Scope allowed: (none)", message_none)
    ASSERTIONS.assertIn("Scope forbidden: (none)", message_none)
    message_empty = controller.build_manual_unblock_telegram_message(
        {"files_allowed": "", "files_forbidden": "   "}
    )
    ASSERTIONS.assertIn("Scope allowed: (none)", message_empty)
    ASSERTIONS.assertIn("Scope forbidden: (none)", message_empty)
    message_mixed = controller.build_manual_unblock_telegram_message(
        {
            "files_allowed": [None, 7, " scripts/pr_automation_controller.py ", ""],
            "files_forbidden": [True, {"x": 1}, " .github/workflows/* "],
        }
    )
    ASSERTIONS.assertIn("Scope allowed: 7, scripts/pr_automation_controller.py", message_mixed)
    ASSERTIONS.assertIn("Scope forbidden: True, {'x': 1}, .github/workflows/*", message_mixed)


def test_manual_unblock_package_shape_and_fixed_status_flags():
    package = controller.build_manual_unblock_package(
        {
            "pr_name": "PR10E",
            "pr_number": "225",
            "head_sha": "abc123",
            "reason": "matrix_phase0_needs_manual",
            "next_action": "needs_manual",
            "files_allowed": ["scripts/pr_automation_controller.py", "tests/scripts/test_pr_automation_controller.py"],
            "files_forbidden": [".github/workflows/*", "scripts/pr_flow_automation.py"],
        }
    )
    ASSERTIONS.assertEqual(
        set(package.keys()),
        {
            "status",
            "next_action",
            "reason",
            "pr_number",
            "head_sha",
            "files_allowed",
            "files_forbidden",
            "copy_paste_command",
            "authorization_text",
            "telegram_message",
            "can_patch",
        },
    )
    ASSERTIONS.assertEqual(package["status"], "NEEDS_MANUAL")
    ASSERTIONS.assertFalse(package["can_patch"])
    ASSERTIONS.assertEqual(
        package["authorization_text"],
        "AUTORIZZO PATCH PR10E NEEDS_MANUAL per matrix_phase0_needs_manual",
    )


def test_manual_unblock_package_normalizes_string_scope_lists_and_keeps_can_patch_false():
    package = controller.build_manual_unblock_package(
        {
            "pr_name": "PR10E",
            "pr_number": "225",
            "head_sha": "abc123",
            "reason": "matrix_phase0_needs_manual",
            "files_allowed": "scripts/pr_automation_controller.py",
            "files_forbidden": ".github/workflows/*",
        }
    )
    ASSERTIONS.assertEqual(package["files_allowed"], ["scripts/pr_automation_controller.py"])
    ASSERTIONS.assertEqual(package["files_forbidden"], [".github/workflows/*"])
    ASSERTIONS.assertFalse(package["can_patch"])
    ASSERTIONS.assertEqual(
        package["authorization_text"],
        "AUTORIZZO PATCH PR10E NEEDS_MANUAL per matrix_phase0_needs_manual",
    )


def test_manual_unblock_package_accepts_tuple_and_set_scope_values():
    package_tuple = controller.build_manual_unblock_package(
        {
            "files_allowed": ("scripts/a.py", "tests/b.py"),
            "files_forbidden": ("scripts/c.py",),
        }
    )
    ASSERTIONS.assertEqual(package_tuple["files_allowed"], ["scripts/a.py", "tests/b.py"])
    ASSERTIONS.assertEqual(package_tuple["files_forbidden"], ["scripts/c.py"])

    package_set = controller.build_manual_unblock_package(
        {
            "files_allowed": {"scripts/a.py"},
            "files_forbidden": {"tests/b.py"},
        }
    )
    ASSERTIONS.assertEqual(package_set["files_allowed"], ["scripts/a.py"])
    ASSERTIONS.assertEqual(package_set["files_forbidden"], ["tests/b.py"])


def test_manual_unblock_package_handles_none_empty_and_mixed_scope_values():
    package_none = controller.build_manual_unblock_package({"files_allowed": None, "files_forbidden": None})
    ASSERTIONS.assertEqual(package_none["files_allowed"], [])
    ASSERTIONS.assertEqual(package_none["files_forbidden"], [])
    package_mixed = controller.build_manual_unblock_package(
        {
            "files_allowed": ["scripts/pr_automation_controller.py", None, 9, "  "],
            "files_forbidden": [".github/workflows/*", "", {"bad": True}],
        }
    )
    ASSERTIONS.assertEqual(package_mixed["files_allowed"], ["9", "scripts/pr_automation_controller.py"])
    ASSERTIONS.assertEqual(package_mixed["files_forbidden"], [".github/workflows/*"])


def test_manual_unblock_package_supports_phase0_manual_review_current_head_and_standing_auth_reasons():
    reason_contexts = [
        ({"matrix_phase0_needs_manual": True}, "matrix_phase0_needs_manual"),
        ({"reason": "review_triage_needs_manual"}, "review_triage_needs_manual"),
        ({"current_head_verification_blocked": True}, "current_head_verification_blocked"),
        ({"tests_cannot_prove_behavior": True}, "tests_cannot_prove_behavior"),
    ]
    for ctx, expected_reason in reason_contexts:
        package = controller.build_manual_unblock_package(
            {
                "pr_name": "PR10E",
                "pr_number": "225",
                "head_sha": "abc123",
                "files_allowed": ["scripts/pr_automation_controller.py"],
                "files_forbidden": [".github/workflows/*"],
                **ctx,
            }
        )
        ASSERTIONS.assertEqual(package["reason"], expected_reason)
        ASSERTIONS.assertEqual(package["status"], "NEEDS_MANUAL")
        ASSERTIONS.assertFalse(package["can_patch"])


def test_manual_unblock_package_maps_matrix_phase0_missing_next_action_to_required_fields_reason():
    package = controller.build_manual_unblock_package(
        {
            "pr_name": "PR10E",
            "pr_number": "246",
            "head_sha": "abc123",
            "status": "NEEDS_MANUAL",
            "next_action": "needs_manual_matrix_phase0_missing",
            "missing_fields": ["files_allowed", "test_matrix"],
            "files_allowed": ["scripts/pr_automation_controller.py"],
            "files_forbidden": ["tests/scripts/test_pr_automation_controller.py"],
        }
    )
    ASSERTIONS.assertEqual(package["reason"], "matrix_phase0_missing_required_fields")
    ASSERTIONS.assertEqual(
        package["authorization_text"],
        "AUTORIZZO PATCH PR10E NEEDS_MANUAL per matrix_phase0_missing_required_fields",
    )
    ASSERTIONS.assertFalse(package["can_patch"])


def test_manual_unblock_package_maps_future_roadmap_scope_blocked_reason_alias():
    package = controller.build_manual_unblock_package(
        {
            "pr_name": "PR10E",
            "pr_number": "246",
            "head_sha": "abc123",
            "reasons": ["future_roadmap_scope_blocked"],
            "files_allowed": ["scripts/pr_automation_controller.py"],
            "files_forbidden": ["tests/scripts/test_pr_automation_controller.py"],
        }
    )
    ASSERTIONS.assertEqual(package["reason"], "future_roadmap_scope")
    ASSERTIONS.assertEqual(
        package["authorization_text"],
        "AUTORIZZO PATCH PR10E NEEDS_MANUAL per future_roadmap_scope",
    )
    ASSERTIONS.assertFalse(package["can_patch"])


def test_manual_unblock_package_maps_phase0_not_passed_reason_alias_to_matrix_phase0_needs_manual():
    package = controller.build_manual_unblock_package(
        {
            "pr_name": "PR10E",
            "pr_number": "246",
            "head_sha": "abc123",
            "reasons": ["phase0_not_passed"],
            "files_allowed": ["scripts/pr_automation_controller.py"],
            "files_forbidden": ["tests/scripts/test_pr_automation_controller.py"],
        }
    )
    ASSERTIONS.assertEqual(package["reason"], "matrix_phase0_needs_manual")
    ASSERTIONS.assertFalse(package["can_patch"])
    ASSERTIONS.assertEqual(
        package["authorization_text"],
        "AUTORIZZO PATCH PR10E NEEDS_MANUAL per matrix_phase0_needs_manual",
    )


def test_manual_unblock_package_strips_newlines_from_pr_head_and_reason_outputs():
    package = controller.build_manual_unblock_package(
        {
            "pr_name": "PR10E\nMANUAL",
            "pr_number": "246\nx",
            "head_sha": "abc123\nzzz",
            "reason": "future_roadmap_scope\nblocked",
            "repo": "owner/repo\nalt",
        }
    )
    ASSERTIONS.assertNotIn("\n", str(package["pr_number"]))
    ASSERTIONS.assertNotIn("\n", str(package["head_sha"]))
    ASSERTIONS.assertNotIn("\n", str(package["reason"]))
    ASSERTIONS.assertNotIn("\n", str(package["copy_paste_command"]))
    ASSERTIONS.assertNotIn("\n", str(package["authorization_text"]))
    ASSERTIONS.assertEqual(package["reason"], "future_roadmap_scope")
    ASSERTIONS.assertEqual(
        package["authorization_text"],
        "AUTORIZZO PATCH PR10E MANUAL NEEDS_MANUAL per future_roadmap_scope",
    )


def test_manual_unblock_package_sanitizes_next_action_to_single_line_for_message_rendering():
    package = controller.build_manual_unblock_package(
        {
            "next_action": "needs_manual\r\ninjected_line",
        }
    )
    ASSERTIONS.assertEqual(package["next_action"], "needs_manual injected_line")
    ASSERTIONS.assertIn("Next action: needs_manual injected_line", package["telegram_message"])
    ASSERTIONS.assertNotIn("\ninjected_line", package["telegram_message"])


def test_manual_unblock_package_supports_pr_and_number_head_fallback_keys():
    package_pr_headref = controller.build_manual_unblock_package(
        {
            "pr": "246",
            "headRefOid": "abc123",
        }
    )
    ASSERTIONS.assertEqual(package_pr_headref["pr_number"], "246")
    ASSERTIONS.assertEqual(package_pr_headref["head_sha"], "abc123")

    package_number_head = controller.build_manual_unblock_package(
        {
            "number": "247",
            "head": "def456",
        }
    )
    ASSERTIONS.assertEqual(package_number_head["pr_number"], "247")
    ASSERTIONS.assertEqual(package_number_head["head_sha"], "def456")


def _automation_ctx(mode: str, **overrides: object) -> dict[str, object]:
    ctx: dict[str, object] = {
        "automation_mode": mode,
        "automation_flags": {
            "SAFE_AUTOFIX_ENABLED": True,
            "AUTO_RESOLVE_ENABLED": True,
            "AUTO_RERUN_ENABLED": True,
            "AUTO_PUSH_ENABLED": True,
            "AUTO_MERGE_ENABLED": True,
            "GITHUB_MUTATION_ENABLED": True,
            "EXTERNAL_SIDE_EFFECT_ENABLED": True,
            "REPORTING_ENABLED": True,
        },
        "task_no_commit_push": False,
        "mergeable": "MERGEABLE",
        "mergeStateStatus": "CLEAN",
        "bad": [],
        "pending": [],
        "unresolved_active": 0,
        "codacy_conclusion": "SUCCESS",
        "codacy_status": "SUCCESS",
        "annotations_count": 0,
        "current_head_matches": True,
        "explicit_merge_authorization": True,
    }
    ctx.update(overrides)
    return ctx


def test_normalize_automation_mode_fail_closed():
    ASSERTIONS.assertEqual(controller.normalize_automation_mode(None), "disabled")
    ASSERTIONS.assertEqual(controller.normalize_automation_mode(""), "disabled")
    ASSERTIONS.assertEqual(controller.normalize_automation_mode("   "), "disabled")
    ASSERTIONS.assertEqual(controller.normalize_automation_mode("unknown"), "disabled")
    ASSERTIONS.assertEqual(controller.normalize_automation_mode("LiVe"), "live")


def test_automation_flag_enabled_exact_true_only():
    ASSERTIONS.assertTrue(controller.automation_flag_enabled("true"))
    ASSERTIONS.assertTrue(controller.automation_flag_enabled(" TRUE "))
    for value in [None, "", "false", "1", "yes", "on", "random"]:
        ASSERTIONS.assertFalse(controller.automation_flag_enabled(value))


def test_build_automation_enablement_context_fails_closed_on_missing_and_malformed_env():
    context = controller.build_automation_enablement_context(
        {
            "AUTOMATION_MODE": "",
            "SAFE_AUTOFIX_ENABLED": "1",
            "AUTO_RESOLVE_ENABLED": "yes",
            "AUTO_RERUN_ENABLED": "on",
            "AUTO_PUSH_ENABLED": "false",
            "AUTO_MERGE_ENABLED": "random",
            "GITHUB_MUTATION_ENABLED": "1",
            "EXTERNAL_SIDE_EFFECT_ENABLED": "on",
            "REPORTING_ENABLED": None,
        }
    )
    ASSERTIONS.assertEqual(context["automation_mode"], "disabled")
    ASSERTIONS.assertEqual(
        context["automation_flags"],
        {
            "SAFE_AUTOFIX_ENABLED": False,
            "AUTO_RESOLVE_ENABLED": False,
            "AUTO_RERUN_ENABLED": False,
            "AUTO_PUSH_ENABLED": False,
            "AUTO_MERGE_ENABLED": False,
            "GITHUB_MUTATION_ENABLED": False,
            "EXTERNAL_SIDE_EFFECT_ENABLED": False,
            "REPORTING_ENABLED": False,
        },
    )


def test_automation_mode_action_matrix_core_modes():
    disabled = _automation_ctx("disabled")
    ASSERTIONS.assertFalse(controller.automation_mode_allows("report", disabled)["allowed"])

    report_only = _automation_ctx("report_only")
    ASSERTIONS.assertTrue(controller.automation_mode_allows("report", report_only)["allowed"])
    ASSERTIONS.assertFalse(controller.automation_mode_allows("plan", report_only)["allowed"])
    ASSERTIONS.assertFalse(controller.automation_mode_allows("push", report_only)["allowed"])

    plan_only = _automation_ctx("plan_only")
    ASSERTIONS.assertTrue(controller.automation_mode_allows("report", plan_only)["allowed"])
    ASSERTIONS.assertTrue(controller.automation_mode_allows("plan", plan_only)["allowed"])
    ASSERTIONS.assertFalse(controller.automation_mode_allows("push", plan_only)["allowed"])

    supervised = _automation_ctx("supervised")
    ASSERTIONS.assertFalse(controller.automation_mode_allows("push", supervised)["allowed"])
    ASSERTIONS.assertEqual(
        controller.automation_mode_allows("push", supervised)["next_action"],
        "manual_authorization_required",
    )

    live = _automation_ctx("live")
    ASSERTIONS.assertTrue(controller.automation_mode_allows("push", live)["allowed"])


def test_can_run_live_action_requires_mode_and_per_action_flag():
    report_only = _automation_ctx("report_only")
    blocked = controller.can_run_live_action("safe_autofix", report_only)
    ASSERTIONS.assertFalse(blocked["allowed"])

    live_ctx = _automation_ctx("live")
    ASSERTIONS.assertTrue(controller.can_run_safe_autofix(live_ctx)["allowed"])

    no_safe = _automation_ctx(
        "live",
        automation_flags={
            "SAFE_AUTOFIX_ENABLED": False,
            "AUTO_RESOLVE_ENABLED": True,
            "AUTO_RERUN_ENABLED": True,
            "AUTO_PUSH_ENABLED": True,
            "AUTO_MERGE_ENABLED": True,
            "GITHUB_MUTATION_ENABLED": True,
            "EXTERNAL_SIDE_EFFECT_ENABLED": True,
            "REPORTING_ENABLED": True,
        },
    )
    ASSERTIONS.assertFalse(controller.can_run_safe_autofix(no_safe)["allowed"])


def test_can_run_safe_autofix_accepts_raw_env_supervised_context():
    result = controller.can_run_safe_autofix(
        {
            "AUTOMATION_MODE": "supervised",
            "SAFE_AUTOFIX_ENABLED": "true",
        }
    )
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertTrue(result["needs_manual"])
    ASSERTIONS.assertEqual(result["mode"], "supervised")
    ASSERTIONS.assertEqual(result["reason"], "manual_authorization_required")


def test_can_run_safe_autofix_accepts_raw_env_live_with_flag_disabled():
    result = controller.can_run_safe_autofix(
        {
            "AUTOMATION_MODE": "live",
            "SAFE_AUTOFIX_ENABLED": "false",
        }
    )
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertTrue(result["needs_manual"])
    ASSERTIONS.assertEqual(result["mode"], "live")
    ASSERTIONS.assertEqual(result["reason"], "safe_autofix_enabled_disabled")


def test_can_run_safe_autofix_accepts_raw_env_live_with_flag_enabled():
    result = controller.can_run_safe_autofix(
        {
            "AUTOMATION_MODE": "live",
            "SAFE_AUTOFIX_ENABLED": "true",
        }
    )
    ASSERTIONS.assertTrue(result["allowed"])
    ASSERTIONS.assertFalse(result["needs_manual"])
    ASSERTIONS.assertEqual(result["mode"], "live")


def test_can_run_safe_autofix_explicit_disabled_context_beats_env_live_true():
    result = controller.can_run_safe_autofix(
        {
            "AUTOMATION_MODE": "live",
            "SAFE_AUTOFIX_ENABLED": "true",
            "automation_mode": "disabled",
            "automation_flags": {"SAFE_AUTOFIX_ENABLED": False},
        }
    )
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertEqual(result["mode"], "disabled")
    ASSERTIONS.assertEqual(result["reason"], "mode_disabled_blocks_safe_autofix")


def test_can_run_safe_autofix_nested_omitted_flag_does_not_inherit_env():
    result = controller.can_run_safe_autofix(
        {
            "AUTOMATION_MODE": "live",
            "SAFE_AUTOFIX_ENABLED": "true",
            "automation_mode": "live",
            "automation_flags": {"AUTO_MERGE_ENABLED": True},
        }
    )
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertEqual(result["mode"], "live")
    ASSERTIONS.assertEqual(result["reason"], "safe_autofix_enabled_disabled")


def test_can_run_safe_autofix_nested_present_flag_allows_action():
    result = controller.can_run_safe_autofix(
        {
            "AUTOMATION_MODE": "live",
            "SAFE_AUTOFIX_ENABLED": "false",
            "automation_mode": "live",
            "automation_flags": {"SAFE_AUTOFIX_ENABLED": True},
        }
    )
    ASSERTIONS.assertTrue(result["allowed"])
    ASSERTIONS.assertEqual(result["mode"], "live")
    ASSERTIONS.assertEqual(result["reason"], "live_action_allowed")


def test_task_no_commit_push_overrides_push_and_merge_even_live():
    ctx = _automation_ctx("live", task_no_commit_push=True)
    ASSERTIONS.assertFalse(controller.can_auto_push(ctx)["allowed"])
    ASSERTIONS.assertFalse(controller.can_auto_merge(ctx)["allowed"])


def test_can_auto_push_raw_env_live_with_task_no_commit_push_blocks_by_override():
    result = controller.can_auto_push(
        {
            "AUTOMATION_MODE": "live",
            "AUTO_PUSH_ENABLED": "true",
            "task_no_commit_push": True,
        }
    )
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertEqual(result["mode"], "live")
    ASSERTIONS.assertEqual(result["reason"], "task_no_commit_push")


def _post_fix_gate_ctx(**overrides: object) -> dict[str, object]:
    ctx: dict[str, object] = {
        "POST_FIX_AUDIT": "PASS",
        "validation_passed": True,
        "current_head_matches": True,
        "dirty_worktree": False,
        "scope_allowed": True,
        "rollback_attempted": False,
        "rollback_succeeded": True,
        "task_no_commit_push": False,
        "automation_mode": "live",
        "can_commit": True,
        "can_push": True,
    }
    ctx.update(overrides)
    return ctx


def test_post_fix_audit_gate_all_green_live_allows_commit_and_push():
    result = controller.evaluate_post_fix_audit_gate(_post_fix_gate_ctx())
    ASSERTIONS.assertTrue(result["allowed"])
    ASSERTIONS.assertTrue(result["can_commit"])
    ASSERTIONS.assertTrue(result["can_push"])
    ASSERTIONS.assertEqual(result["reason"], "allowed")


def test_post_fix_audit_gate_fail_status_denies():
    result = controller.evaluate_post_fix_audit_gate(_post_fix_gate_ctx(POST_FIX_AUDIT="FAIL"))
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "post_fix_audit_not_pass")


def test_post_fix_audit_gate_accepts_normalized_status_sources_and_report_status_fail():
    via_post_fix = controller.evaluate_post_fix_audit_gate(
        _post_fix_gate_ctx(POST_FIX_AUDIT=None, post_fix_audit="PASS")
    )
    via_report = controller.evaluate_post_fix_audit_gate(
        _post_fix_gate_ctx(POST_FIX_AUDIT=None, post_fix_audit=None, post_fix_audit_report={"status": "PASS"})
    )
    fail_via_report_status = controller.evaluate_post_fix_audit_gate(
        _post_fix_gate_ctx(POST_FIX_AUDIT=None, post_fix_audit=None, report={"status": "FAIL"})
    )
    ASSERTIONS.assertTrue(via_post_fix["allowed"])
    ASSERTIONS.assertTrue(via_report["allowed"])
    ASSERTIONS.assertEqual(fail_via_report_status["reason"], "post_fix_audit_not_pass")


def test_post_fix_audit_gate_conflicting_or_malformed_status_sources_fail_closed():
    conflict = controller.evaluate_post_fix_audit_gate(
        _post_fix_gate_ctx(POST_FIX_AUDIT="PASS", post_fix_audit="FAIL")
    )
    malformed = controller.evaluate_post_fix_audit_gate(_post_fix_gate_ctx(POST_FIX_AUDIT="banana"))
    ASSERTIONS.assertFalse(conflict["allowed"])
    ASSERTIONS.assertEqual(conflict["reason"], "post_fix_audit_malformed")
    ASSERTIONS.assertFalse(malformed["allowed"])
    ASSERTIONS.assertEqual(malformed["reason"], "post_fix_audit_malformed")


def test_post_fix_audit_gate_missing_empty_malformed_unknown_or_needs_manual_denies():
    missing_payload = {k: v for k, v in _post_fix_gate_ctx().items() if k != "POST_FIX_AUDIT"}
    result = controller.evaluate_post_fix_audit_gate(missing_payload)
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "post_fix_audit_missing")

    cases = [
        ("", "post_fix_audit_missing"),
        ("garbage", "post_fix_audit_malformed"),
        ("UNKNOWN", "post_fix_audit_not_pass"),
        ("NEEDS_MANUAL", "post_fix_audit_not_pass"),
    ]
    for status, reason in cases:
        case_result = controller.evaluate_post_fix_audit_gate(_post_fix_gate_ctx(POST_FIX_AUDIT=status))
        ASSERTIONS.assertFalse(case_result["allowed"])
        ASSERTIONS.assertEqual(case_result["reason"], reason)


def test_post_fix_audit_gate_validation_failed_or_missing_denies():
    failed = controller.evaluate_post_fix_audit_gate(_post_fix_gate_ctx(validation_passed=False))
    missing2 = controller.evaluate_post_fix_audit_gate(
        {k: v for k, v in _post_fix_gate_ctx().items() if k != "validation_passed"}
    )
    ASSERTIONS.assertEqual(failed["reason"], "validation_failed")
    ASSERTIONS.assertEqual(missing2["reason"], "evidence_missing")


def test_post_fix_audit_gate_current_head_mismatch_or_missing_denies():
    mismatch = controller.evaluate_post_fix_audit_gate(_post_fix_gate_ctx(current_head_matches=False))
    missing = controller.evaluate_post_fix_audit_gate(
        {k: v for k, v in _post_fix_gate_ctx().items() if k != "current_head_matches"}
    )
    ASSERTIONS.assertEqual(mismatch["reason"], "current_head_mismatch")
    ASSERTIONS.assertEqual(missing["reason"], "evidence_missing")


def test_post_fix_audit_gate_dirty_worktree_denies():
    result = controller.evaluate_post_fix_audit_gate(_post_fix_gate_ctx(dirty_worktree=True))
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "dirty_worktree")


def test_post_fix_audit_gate_missing_dirty_worktree_uses_distinct_reason():
    payload = {k: v for k, v in _post_fix_gate_ctx().items() if k != "dirty_worktree"}
    result = controller.evaluate_post_fix_audit_gate(payload)
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "dirty_worktree_missing")


def test_post_fix_audit_gate_task_no_commit_push_denies():
    result = controller.evaluate_post_fix_audit_gate(_post_fix_gate_ctx(task_no_commit_push=True))
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "task_no_commit_push")


def test_post_fix_audit_gate_non_live_modes_deny():
    for mode in ["disabled", "report_only", "plan_only", "supervised"]:
        result = controller.evaluate_post_fix_audit_gate(_post_fix_gate_ctx(automation_mode=mode))
        ASSERTIONS.assertFalse(result["allowed"])
        ASSERTIONS.assertEqual(result["reason"], "passive_mode_blocks_commit_push")


def test_post_fix_audit_gate_env_style_live_mode_allows_with_full_evidence():
    payload = _post_fix_gate_ctx()
    payload.pop("automation_mode", None)
    payload["AUTOMATION_MODE"] = "live"
    result = controller.evaluate_post_fix_audit_gate(payload)
    ASSERTIONS.assertTrue(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "allowed")


def test_post_fix_audit_gate_env_style_unknown_mode_denies_fail_closed():
    result = controller.evaluate_post_fix_audit_gate(
        _post_fix_gate_ctx(automation_mode=None, AUTOMATION_MODE="banana")
    )
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "passive_mode_blocks_commit_push")


def test_post_fix_audit_gate_env_style_live_still_denies_missing_evidence():
    payload = {k: v for k, v in _post_fix_gate_ctx().items() if k != "validation_passed"}
    payload.pop("automation_mode", None)
    payload["AUTOMATION_MODE"] = "live"
    result = controller.evaluate_post_fix_audit_gate(payload)
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "evidence_missing")


def test_post_fix_audit_gate_explicit_status_overrides_ambient_env_lowercase_source(monkeypatch):
    monkeypatch.setenv("POST_FIX_AUDIT", "FAIL")
    result = controller.evaluate_post_fix_audit_gate(
        _post_fix_gate_ctx(POST_FIX_AUDIT=None, post_fix_audit="PASS")
    )
    ASSERTIONS.assertTrue(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "allowed")


def test_post_fix_audit_gate_explicit_status_overrides_ambient_env_uppercase_source(monkeypatch):
    monkeypatch.setenv("POST_FIX_AUDIT", "FAIL")
    result = controller.evaluate_post_fix_audit_gate(
        _post_fix_gate_ctx(POST_FIX_AUDIT="PASS", post_fix_audit=None)
    )
    ASSERTIONS.assertTrue(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "allowed")


def test_post_fix_audit_gate_rollback_failure_denies():
    result = controller.evaluate_post_fix_audit_gate(
        _post_fix_gate_ctx(rollback_attempted=True, rollback_succeeded=False)
    )
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "rollback_failed")


def test_post_fix_audit_gate_explicit_false_false_rollback_tuple_denies():
    result = controller.evaluate_post_fix_audit_gate(
        _post_fix_gate_ctx(rollback_attempted=False, rollback_succeeded=False)
    )
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertFalse(result["can_commit"])
    ASSERTIONS.assertFalse(result["can_push"])
    ASSERTIONS.assertEqual(result["reason"], "evidence_malformed")


def test_post_fix_audit_gate_nested_rollback_attempted_failed_denies():
    result = controller.evaluate_post_fix_audit_gate(
        _post_fix_gate_ctx(
            rollback_attempted=False,
            rollback_succeeded=True,
            rollback={"rollback_attempted": True, "rollback_succeeded": False},
        )
    )
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertFalse(result["can_commit"])
    ASSERTIONS.assertFalse(result["can_push"])
    ASSERTIONS.assertEqual(result["reason"], "evidence_malformed")


def test_post_fix_audit_gate_nested_rollback_attempted_succeeded_stays_green():
    result = controller.evaluate_post_fix_audit_gate(
        _post_fix_gate_ctx(
            rollback_attempted=False,
            rollback_succeeded=False,
            rollback={"rollback_attempted": True, "rollback_succeeded": True},
        )
    )
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertFalse(result["can_commit"])
    ASSERTIONS.assertFalse(result["can_push"])
    ASSERTIONS.assertEqual(result["reason"], "evidence_malformed")


def test_post_fix_audit_gate_pr10b_scope_rollback_failed_shape_denies():
    result = controller.evaluate_post_fix_audit_gate(
        _post_fix_gate_ctx(
            POST_FIX_AUDIT="FAIL",
            post_fix_audit="FAIL",
            status="FAIL",
            next_action="needs_manual_scope_violation_rollback_failed",
            rollback_attempted=False,
            rollback_succeeded=True,
            rollback={"rollback_attempted": True, "rollback_succeeded": False},
        )
    )
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertFalse(result["can_commit"])
    ASSERTIONS.assertFalse(result["can_push"])
    ASSERTIONS.assertEqual(result["reason"], "post_fix_audit_not_pass")


def test_post_fix_audit_gate_scope_violation_denies():
    result = controller.evaluate_post_fix_audit_gate(_post_fix_gate_ctx(scope_allowed=False))
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "scope_violation")


def test_post_fix_audit_gate_scope_audit_allowed_true_derives_scope_allowed():
    result = controller.evaluate_post_fix_audit_gate(
        _post_fix_gate_ctx(scope_allowed=None, scope_audit={"allowed": True})
    )
    ASSERTIONS.assertTrue(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "allowed")


def test_post_fix_audit_gate_scope_audit_allowed_false_denies_scope_violation():
    result = controller.evaluate_post_fix_audit_gate(
        _post_fix_gate_ctx(scope_allowed=None, scope_audit={"allowed": False})
    )
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "scope_violation")


def test_post_fix_audit_gate_scope_audit_allowed_string_fails_closed():
    result = controller.evaluate_post_fix_audit_gate(
        _post_fix_gate_ctx(scope_allowed=None, scope_audit={"allowed": "false"})
    )
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "evidence_malformed")


def test_post_fix_audit_gate_explicit_commit_or_push_false_denies_respective_action():
    commit_block = controller.evaluate_post_fix_audit_gate(_post_fix_gate_ctx(can_commit=False))
    push_block = controller.evaluate_post_fix_audit_gate(_post_fix_gate_ctx(can_push=False))
    ASSERTIONS.assertFalse(commit_block["can_commit"])
    ASSERTIONS.assertEqual(commit_block["reason"], "commit_not_allowed")
    ASSERTIONS.assertFalse(push_block["can_push"])
    ASSERTIONS.assertEqual(push_block["reason"], "push_not_allowed")


def test_post_fix_audit_gate_explicit_authorization_cannot_override_explicit_false():
    result = controller.evaluate_post_fix_audit_gate(
        _post_fix_gate_ctx(
            can_commit=False,
            can_push=False,
            explicit_commit_authorization=True,
            explicit_push_authorization=True,
        )
    )
    ASSERTIONS.assertFalse(result["can_commit"])
    ASSERTIONS.assertFalse(result["can_push"])
    ASSERTIONS.assertEqual(result["reason"], "commit_not_allowed")


def test_post_fix_audit_gate_commit_and_push_false_keeps_deterministic_commit_reason():
    result = controller.evaluate_post_fix_audit_gate(_post_fix_gate_ctx(can_commit=False, can_push=False))
    ASSERTIONS.assertFalse(result["can_commit"])
    ASSERTIONS.assertFalse(result["can_push"])
    ASSERTIONS.assertEqual(result["reason"], "commit_not_allowed")


def test_clean_rebuild_command_includes_post_fix_gate_context():
    rules = controller.CleanScopeRules(allowlist=("a.py",), forbidden=("b.py",), commit_limit=3)
    config = controller.CleanRebuildConfig(
        repo="owner/repo",
        pr_number="249",
        head_branch="feature/branch",
        dry_run=True,
        rules=rules,
        post_fix_gate_context={"post_fix_audit": "PASS", "can_push": True},
    )
    cmd = controller.clean_rebuild_command(config, "out.json")
    ASSERTIONS.assertIn("--post-fix-gate-context", cmd)
    context_index = cmd.index("--post-fix-gate-context") + 1
    parsed = json.loads(cmd[context_index])
    ASSERTIONS.assertEqual(parsed["post_fix_audit"], "PASS")


def test_clean_rebuild_command_skips_post_fix_gate_context_when_missing():
    rules = controller.CleanScopeRules(allowlist=("a.py",), forbidden=("b.py",), commit_limit=3)
    config = controller.CleanRebuildConfig(
        repo="owner/repo",
        pr_number="249",
        head_branch="feature/branch",
        dry_run=True,
        rules=rules,
        post_fix_gate_context=None,
    )
    cmd = controller.clean_rebuild_command(config, "out.json")
    ASSERTIONS.assertNotIn("--post-fix-gate-context", cmd)


def test_maybe_launch_clean_rebuild_populates_decision_gate_context_and_launches(monkeypatch):
    args = _args()
    args.clean_scope_rebuild = True
    args.clean_scope_rebuild_mode = "execute"
    decision: dict[str, Any] = {
        "actions": [],
        "warnings": [],
        "post_fix_audit_gate_context": _post_fix_gate_ctx(),
    }
    pr = {"headRefName": "feature/branch", "headRefOid": "abc123"}
    ctx = controller.NextActionContext(args, pr, [], [], [], [], decision)
    rules = controller.CleanScopeRules(allowlist=("a.py",), forbidden=("b.py",), commit_limit=3)
    signals = {
        "forbidden_files": ["b.py"],
        "has_allowlisted_file": True,
        "autofix_commit_count": 4,
        "autofix_commit_limit_exceeded": True,
        "possible_autofix_oscillation": False,
    }
    monkeypatch.setattr(
        controller,
        "run_clean_scope_rebuild",
        lambda _config: {
            "action": "would_launch_clean_scope_rebuild",
            "cmd": ["python3", "scripts/pr_clean_scope_rebuild.py"],
        },
    )
    launched = controller.maybe_launch_clean_rebuild(ctx, rules, signals)
    ASSERTIONS.assertTrue(launched)
    ASSERTIONS.assertEqual(decision["next_action"], "would_launch_clean_scope_rebuild")
    ASSERTIONS.assertEqual(decision["post_fix_audit_gate_context"]["repo"], "owner/repo")
    ASSERTIONS.assertEqual(decision["post_fix_audit_gate_context"]["pr"], "225")
    ASSERTIONS.assertEqual(decision["post_fix_audit_gate_context"]["headRefOid"], "abc123")


def test_maybe_launch_clean_rebuild_fails_closed_without_gate_context(monkeypatch):
    args = _args()
    args.clean_scope_rebuild = True
    args.clean_scope_rebuild_mode = "execute"
    decision: dict[str, Any] = {"actions": [], "warnings": []}
    pr = {"headRefName": "feature/branch", "headRefOid": "abc123"}
    ctx = controller.NextActionContext(args, pr, [], [], [], [], decision)
    rules = controller.CleanScopeRules(allowlist=("a.py",), forbidden=("b.py",), commit_limit=3)
    signals = {
        "forbidden_files": ["b.py"],
        "has_allowlisted_file": True,
        "autofix_commit_count": 4,
        "autofix_commit_limit_exceeded": True,
        "possible_autofix_oscillation": False,
    }
    called = {"value": False}

    def _unexpected_launch(_config):
        called["value"] = True
        return {"action": "would_launch_clean_scope_rebuild"}

    monkeypatch.setattr(controller, "run_clean_scope_rebuild", _unexpected_launch)
    launched = controller.maybe_launch_clean_rebuild(ctx, rules, signals)
    ASSERTIONS.assertTrue(launched)
    ASSERTIONS.assertFalse(called["value"])
    ASSERTIONS.assertEqual(decision["next_action"], "needs_manual_clean_scope_gate_context_missing_or_invalid")


def test_post_fix_audit_gate_missing_scope_rollback_task_or_action_evidence_denies():
    missing_scope = controller.evaluate_post_fix_audit_gate(
        {k: v for k, v in _post_fix_gate_ctx().items() if k != "scope_allowed"}
    )
    missing_rollback = controller.evaluate_post_fix_audit_gate(
        {k: v for k, v in _post_fix_gate_ctx().items() if k != "rollback_succeeded"}
    )
    missing_task = controller.evaluate_post_fix_audit_gate(
        {k: v for k, v in _post_fix_gate_ctx().items() if k != "task_no_commit_push"}
    )
    ASSERTIONS.assertEqual(missing_scope["reason"], "evidence_missing")
    ASSERTIONS.assertEqual(missing_rollback["reason"], "evidence_missing")
    ASSERTIONS.assertEqual(missing_task["reason"], "evidence_missing")


def test_post_fix_audit_gate_boolean_like_string_and_malformed_evidence_fail_closed():
    false_string = controller.evaluate_post_fix_audit_gate(_post_fix_gate_ctx(task_no_commit_push="false"))
    malformed_string = controller.evaluate_post_fix_audit_gate(_post_fix_gate_ctx(validation_passed="yes"))
    ASSERTIONS.assertTrue(false_string["allowed"])
    ASSERTIONS.assertEqual(malformed_string["reason"], "evidence_malformed")


def test_post_fix_audit_gate_scope_and_rollback_reason_precedence_over_audit_not_pass():
    scope_first = controller.evaluate_post_fix_audit_gate(
        _post_fix_gate_ctx(POST_FIX_AUDIT="FAIL", scope_allowed=False)
    )
    rollback_first = controller.evaluate_post_fix_audit_gate(
        _post_fix_gate_ctx(POST_FIX_AUDIT="FAIL", rollback_attempted=True, rollback_succeeded=False)
    )
    ASSERTIONS.assertEqual(scope_first["reason"], "scope_violation")
    ASSERTIONS.assertEqual(rollback_first["reason"], "rollback_failed")


def test_can_auto_push_live_flag_only_denies_without_pr3h_evidence():
    denied = controller.can_auto_push({"AUTOMATION_MODE": "live", "AUTO_PUSH_ENABLED": "true"})
    ASSERTIONS.assertFalse(denied["allowed"])
    ASSERTIONS.assertEqual(denied["reason"], "post_fix_audit_missing")


def test_can_auto_push_env_style_live_enabled_and_full_pr3h_evidence_allows():
    payload = _post_fix_gate_ctx()
    payload.pop("automation_mode", None)
    payload["AUTOMATION_MODE"] = "live"
    payload["AUTO_PUSH_ENABLED"] = "true"
    result = controller.can_auto_push(payload)
    ASSERTIONS.assertTrue(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "live_action_allowed")


def test_post_fix_audit_gate_env_only_run_evidence_denies(monkeypatch):
    monkeypatch.setenv("POST_FIX_AUDIT", "PASS")
    monkeypatch.setenv("VALIDATION_PASSED", "true")
    monkeypatch.setenv("CURRENT_HEAD_MATCHES", "true")
    monkeypatch.setenv("DIRTY_WORKTREE", "false")
    monkeypatch.setenv("SCOPE_ALLOWED", "true")
    monkeypatch.setenv("ROLLBACK_ATTEMPTED", "false")
    monkeypatch.setenv("ROLLBACK_SUCCEEDED", "true")
    monkeypatch.setenv("TASK_NO_COMMIT_PUSH", "false")
    payload = {"automation_mode": "live", "can_commit": True, "can_push": True}
    result = controller.evaluate_post_fix_audit_gate(payload)
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "dirty_worktree_missing")


def test_can_auto_push_env_mode_flag_with_explicit_run_evidence_allows(monkeypatch):
    monkeypatch.setenv("AUTOMATION_MODE", "live")
    monkeypatch.setenv("AUTO_PUSH_ENABLED", "true")
    payload = _post_fix_gate_ctx(automation_mode=None)
    payload.pop("AUTOMATION_MODE", None)
    payload.pop("automation_mode", None)
    result = controller.can_auto_push(payload)
    ASSERTIONS.assertTrue(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "live_action_allowed")


def test_can_auto_push_task_no_commit_push_string_false_does_not_block():
    result = controller.can_auto_push(
        {
            "AUTOMATION_MODE": "live",
            "AUTO_PUSH_ENABLED": "true",
            "POST_FIX_AUDIT": "PASS",
            "validation_passed": True,
            "current_head_matches": True,
            "dirty_worktree": False,
            "scope_allowed": True,
            "rollback_attempted": False,
            "rollback_succeeded": True,
            "task_no_commit_push": "false",
            "can_commit": True,
            "can_push": True,
        }
    )
    ASSERTIONS.assertTrue(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "live_action_allowed")


def test_can_auto_push_task_no_commit_push_string_true_blocks():
    result = controller.can_auto_push(
        {
            "AUTOMATION_MODE": "live",
            "AUTO_PUSH_ENABLED": "true",
            "POST_FIX_AUDIT": "PASS",
            "validation_passed": True,
            "current_head_matches": True,
            "dirty_worktree": False,
            "scope_allowed": True,
            "rollback_attempted": False,
            "rollback_succeeded": True,
            "task_no_commit_push": "true",
            "can_commit": True,
            "can_push": True,
        }
    )
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "task_no_commit_push")


def test_can_auto_push_task_no_commit_push_malformed_denies_fail_closed():
    result = controller.can_auto_push(
        {
            "AUTOMATION_MODE": "live",
            "AUTO_PUSH_ENABLED": "true",
            "task_no_commit_push": "banana",
        }
    )
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "task_no_commit_push")


def test_post_fix_audit_gate_conflicting_boolean_sources_deny_fail_closed():
    result = controller.evaluate_post_fix_audit_gate(
        _post_fix_gate_ctx(rollback_succeeded=False, rollback={"rollback_succeeded": True})
    )
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "evidence_malformed")


def test_post_fix_audit_gate_missing_can_commit_and_can_push_denies():
    payload = _post_fix_gate_ctx()
    payload.pop("can_commit")
    payload.pop("can_push")
    result = controller.evaluate_post_fix_audit_gate(payload)
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "commit_not_allowed")


def test_post_fix_audit_gate_missing_can_push_denies_push():
    payload = _post_fix_gate_ctx()
    payload.pop("can_push")
    result = controller.evaluate_post_fix_audit_gate(payload)
    ASSERTIONS.assertFalse(result["can_push"])
    ASSERTIONS.assertEqual(result["reason"], "push_not_allowed")


def test_post_fix_audit_gate_missing_can_commit_denies_commit():
    payload = _post_fix_gate_ctx()
    payload.pop("can_commit")
    result = controller.evaluate_post_fix_audit_gate(payload)
    ASSERTIONS.assertFalse(result["can_commit"])
    ASSERTIONS.assertEqual(result["reason"], "commit_not_allowed")


def test_assert_live_action_allowed_push_denies_without_pr3h_evidence():
    with ASSERTIONS.assertRaises(PermissionError):
        controller.assert_live_action_allowed(
            "push",
            {"AUTOMATION_MODE": "live", "AUTO_PUSH_ENABLED": "true"},
        )


def test_post_fix_audit_wrappers_return_action_consistent_payloads():
    commit_allowed = controller.can_commit_after_post_fix_audit(_post_fix_gate_ctx())
    push_denied = controller.can_push_after_post_fix_audit(_post_fix_gate_ctx(can_push=False))
    ASSERTIONS.assertEqual(commit_allowed["allowed"], commit_allowed["can_commit"])
    ASSERTIONS.assertEqual(push_denied["allowed"], push_denied["can_push"])
    ASSERTIONS.assertEqual(commit_allowed["status"], "PASS")
    ASSERTIONS.assertEqual(push_denied["status"], "FAIL")
    ASSERTIONS.assertFalse(commit_allowed["needs_manual"])
    ASSERTIONS.assertTrue(push_denied["needs_manual"])
    ASSERTIONS.assertEqual(commit_allowed["next_action"], "proceed")
    ASSERTIONS.assertEqual(push_denied["next_action"], "needs_manual")


def test_post_fix_audit_gate_contract_shape():
    result = controller.evaluate_post_fix_audit_gate(_post_fix_gate_ctx())
    keys = {
        "allowed",
        "can_commit",
        "can_push",
        "status",
        "post_fix_audit",
        "reason",
        "next_action",
        "needs_manual",
        "missing_fields",
    }
    ASSERTIONS.assertEqual(set(result.keys()), keys)


def test_assert_post_fix_audit_gate_or_block_raises_on_deny():
    with ASSERTIONS.assertRaises(RuntimeError):
        controller.assert_post_fix_audit_gate_or_block(_post_fix_gate_ctx(POST_FIX_AUDIT="FAIL"))
    with ASSERTIONS.assertRaises(PermissionError):
        controller.assert_post_fix_audit_gate_or_block(_post_fix_gate_ctx(task_no_commit_push=True))


def test_can_auto_merge_denies_each_guard_condition():
    base = _automation_ctx("live")
    cases = [
        ("mergeable_not_mergeable", {"mergeable": "CONFLICTING"}),
        ("merge_state_not_clean", {"mergeStateStatus": "DIRTY"}),
        ("bad_checks_present", {"bad": ["x"]}),
        ("pending_checks_present", {"pending": ["x"]}),
        ("bad_checks_missing", {"bad": "not-a-list", "blockers": "not-a-list"}),
        ("pending_checks_missing", {"pending": "not-a-list"}),
        ("unresolved_active_missing", {"unresolved_active": "bad-value"}),
        ("unresolved_reviews_present", {"unresolved_active": 1}),
        (
            "codacy_failure_state_present",
            {"codacy_conclusion": "SUCCESS", "codacy_status": "FAILURE"},
        ),
        ("codacy_not_success", {"codacy_conclusion": "NEUTRAL", "codacy_status": "NEUTRAL"}),
        ("codacy_not_success", {"codacy_conclusion": "", "codacy_status": ""}),
        ("annotations_data_missing", {"annotations_count": "unknown"}),
        ("annotations_present", {"annotations_count": 1}),
        ("current_head_mismatch", {"current_head_matches": False}),
        ("explicit_merge_authorization_required", {"explicit_merge_authorization": False}),
    ]
    for expected_reason, patch in cases:
        ctx = dict(base)
        ctx.update(patch)
        result = controller.can_auto_merge(ctx)
        ASSERTIONS.assertFalse(result["allowed"])
        ASSERTIONS.assertTrue(result["needs_manual"])
        ASSERTIONS.assertEqual(result["reason"], expected_reason)


def test_can_auto_merge_bad_missing_with_non_empty_blockers_denies_bad_checks_present():
    result = controller.can_auto_merge(
        _automation_ctx("live", bad="not-a-list", blockers=[{"name": "failing check"}])
    )
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "bad_checks_present")


def test_can_auto_merge_bad_missing_with_empty_blockers_and_all_green_allows():
    result = controller.can_auto_merge(_automation_ctx("live", bad="not-a-list", blockers=[]))
    ASSERTIONS.assertTrue(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "enabled")


def test_can_auto_merge_bad_empty_and_blockers_non_empty_denies_bad_checks_present():
    result = controller.can_auto_merge(_automation_ctx("live", bad=[], blockers=[{"name": "failing check"}]))
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "bad_checks_present")


def test_can_auto_merge_bad_and_blockers_both_empty_with_all_green_allows():
    result = controller.can_auto_merge(_automation_ctx("live", bad=[], blockers=[]))
    ASSERTIONS.assertTrue(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "enabled")


def test_can_auto_merge_bad_missing_and_blockers_missing_denies_bad_checks_missing():
    result = controller.can_auto_merge(
        _automation_ctx("live", bad="not-a-list", blockers="not-a-list")
    )
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "bad_checks_missing")


def test_can_auto_merge_mixed_codacy_success_failure_denies():
    result = controller.can_auto_merge(
        _automation_ctx(
            "live",
            codacy_conclusion="SUCCESS",
            codacy_status="FAILURE",
        )
    )
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertIn("codacy_failure_state_present", result["reason"])


def test_can_auto_merge_codacy_failure_without_success_does_not_add_not_success_reason():
    result = controller.can_auto_merge(
        _automation_ctx(
            "live",
            codacy_conclusion="FAILURE",
            codacy_status="",
            github_codacy_state="",
        )
    )
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "codacy_failure_state_present")


def test_can_auto_merge_codacy_missing_state_denies_with_not_success_only():
    result = controller.can_auto_merge(
        _automation_ctx(
            "live",
            codacy_conclusion="",
            codacy_status="",
            github_codacy_state="",
            codacy_check_conclusion="",
            codacy_check_status="",
        )
    )
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "codacy_not_success")


def test_can_auto_merge_uses_controller_codacy_keys():
    allowed = controller.can_auto_merge(
        _automation_ctx(
            "live",
            codacy_conclusion="",
            codacy_status="",
            codacy_check_conclusion="",
            codacy_check_status="",
            github_codacy_state="SUCCESS",
        )
    )
    ASSERTIONS.assertTrue(allowed["allowed"])

    denied = controller.can_auto_merge(
        _automation_ctx(
            "live",
            codacy_conclusion="",
            codacy_status="",
            codacy_check_conclusion="",
            codacy_check_status="",
            github_codacy_state="FAILURE",
        )
    )
    ASSERTIONS.assertFalse(denied["allowed"])
    ASSERTIONS.assertIn("codacy_failure_state_present", denied["reason"])


def test_can_auto_merge_annotation_count_aliases_and_missing_reason():
    from_codacy_nested = controller.can_auto_merge(
        _automation_ctx(
            "live",
            annotations_count=None,
            codacy={"github_annotations_count": 0},
        )
    )
    ASSERTIONS.assertTrue(from_codacy_nested["allowed"])

    missing = controller.can_auto_merge(_automation_ctx("live", annotations_count="not-int"))
    ASSERTIONS.assertFalse(missing["allowed"])
    ASSERTIONS.assertIn("annotations_data_missing", missing["reason"])


def test_can_auto_merge_codacy_annotations_count_zero_allows_when_other_guards_green():
    result = controller.can_auto_merge(
        _automation_ctx("live", annotations_count=None, codacy_annotations_count=0)
    )
    ASSERTIONS.assertTrue(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "enabled")


def test_can_auto_merge_codacy_annotations_count_gt_zero_denies_annotations_present():
    result = controller.can_auto_merge(
        _automation_ctx("live", annotations_count=None, codacy_annotations_count=2)
    )
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "annotations_present")


def test_can_auto_merge_codacy_annotations_count_malformed_denies_data_missing():
    result = controller.can_auto_merge(
        _automation_ctx("live", annotations_count=None, codacy_annotations_count="n/a")
    )
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "annotations_data_missing")


def test_can_auto_merge_codacy_annotations_empty_list_allows_when_other_guards_green():
    result = controller.can_auto_merge(
        _automation_ctx(
            "live",
            annotations_count=None,
            codacy={"conclusion": "SUCCESS", "annotations": []},
        )
    )
    ASSERTIONS.assertTrue(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "enabled")


def test_can_auto_merge_codacy_annotations_list_with_dict_denies_present():
    result = controller.can_auto_merge(
        _automation_ctx(
            "live",
            annotations_count=None,
            codacy={"conclusion": "SUCCESS", "annotations": [{"x": 1}]},
        )
    )
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "annotations_present")


def test_can_auto_merge_codacy_annotations_list_with_scalar_denies_data_missing():
    result = controller.can_auto_merge(
        _automation_ctx(
            "live",
            annotations_count=None,
            codacy={"conclusion": "SUCCESS", "annotations": ["bad"]},
        )
    )
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertEqual(result["reason"], "annotations_data_missing")


def test_can_auto_merge_all_green_with_explicit_auth_allows():
    ctx = _automation_ctx("live")
    result = controller.can_auto_merge(ctx)
    ASSERTIONS.assertTrue(result["allowed"])
    ASSERTIONS.assertFalse(result["needs_manual"])


def test_can_auto_merge_raw_env_live_all_green_allows():
    result = controller.can_auto_merge(
        {
            "AUTOMATION_MODE": "live",
            "AUTO_MERGE_ENABLED": "true",
            "mergeable": "MERGEABLE",
            "mergeStateStatus": "CLEAN",
            "bad": [],
            "pending": [],
            "unresolved_active": 0,
            "codacy_conclusion": "SUCCESS",
            "codacy_status": "SUCCESS",
            "annotations_count": 0,
            "current_head_matches": True,
            "explicit_merge_authorization": True,
        }
    )
    ASSERTIONS.assertTrue(result["allowed"])
    ASSERTIONS.assertEqual(result["mode"], "live")
    ASSERTIONS.assertEqual(result["reason"], "enabled")


def test_can_auto_merge_raw_env_live_with_codacy_conclusion_success_allows():
    result = controller.can_auto_merge(
        {
            "AUTOMATION_MODE": "live",
            "AUTO_MERGE_ENABLED": "true",
            "mergeable": "MERGEABLE",
            "mergeStateStatus": "CLEAN",
            "bad": [],
            "pending": [],
            "unresolved_active": 0,
            "codacy_conclusion": "success",
            "annotations_count": 0,
            "current_head_matches": True,
            "explicit_merge_authorization": True,
        }
    )
    ASSERTIONS.assertTrue(result["allowed"])
    ASSERTIONS.assertEqual(result["mode"], "live")
    ASSERTIONS.assertEqual(result["reason"], "enabled")


def test_can_auto_merge_raw_env_live_with_codacy_status_success_allows():
    result = controller.can_auto_merge(
        {
            "AUTOMATION_MODE": "live",
            "AUTO_MERGE_ENABLED": "true",
            "mergeable": "MERGEABLE",
            "mergeStateStatus": "CLEAN",
            "bad": [],
            "pending": [],
            "unresolved_active": 0,
            "codacy_status": "success",
            "annotations_count": 0,
            "current_head_matches": True,
            "explicit_merge_authorization": True,
        }
    )
    ASSERTIONS.assertTrue(result["allowed"])
    ASSERTIONS.assertEqual(result["mode"], "live")
    ASSERTIONS.assertEqual(result["reason"], "enabled")


def test_can_auto_merge_raw_env_live_with_codacy_check_status_success_allows():
    result = controller.can_auto_merge(
        {
            "AUTOMATION_MODE": "live",
            "AUTO_MERGE_ENABLED": "true",
            "mergeable": "MERGEABLE",
            "mergeStateStatus": "CLEAN",
            "bad": [],
            "pending": [],
            "unresolved_active": 0,
            "codacy_check_status": "success",
            "annotations_count": 0,
            "current_head_matches": True,
            "explicit_merge_authorization": True,
        }
    )
    ASSERTIONS.assertTrue(result["allowed"])
    ASSERTIONS.assertEqual(result["mode"], "live")
    ASSERTIONS.assertEqual(result["reason"], "enabled")


def test_can_auto_merge_raw_env_live_with_bad_checks_denies_and_keeps_live_mode():
    result = controller.can_auto_merge(
        {
            "AUTOMATION_MODE": "live",
            "AUTO_MERGE_ENABLED": "true",
            "mergeable": "MERGEABLE",
            "mergeStateStatus": "CLEAN",
            "bad": ["failing check"],
            "pending": [],
            "unresolved_active": 0,
            "codacy_conclusion": "SUCCESS",
            "annotations_count": 0,
            "current_head_matches": True,
            "explicit_merge_authorization": True,
        }
    )
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertEqual(result["mode"], "live")
    ASSERTIONS.assertIn("bad_checks_present", result["reason"])


def test_can_auto_merge_raw_env_live_missing_current_head_matches_denies():
    result = controller.can_auto_merge(
        {
            "AUTOMATION_MODE": "live",
            "AUTO_MERGE_ENABLED": "true",
            "mergeable": "MERGEABLE",
            "mergeStateStatus": "CLEAN",
            "bad": [],
            "pending": [],
            "unresolved_active": 0,
            "codacy_conclusion": "SUCCESS",
            "annotations_count": 0,
            "explicit_merge_authorization": True,
        }
    )
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertEqual(result["mode"], "live")
    ASSERTIONS.assertIn("current_head_mismatch", result["reason"])


def test_can_auto_merge_raw_env_live_missing_explicit_merge_authorization_denies():
    result = controller.can_auto_merge(
        {
            "AUTOMATION_MODE": "live",
            "AUTO_MERGE_ENABLED": "true",
            "mergeable": "MERGEABLE",
            "mergeStateStatus": "CLEAN",
            "bad": [],
            "pending": [],
            "unresolved_active": 0,
            "codacy_conclusion": "SUCCESS",
            "annotations_count": 0,
            "current_head_matches": True,
        }
    )
    ASSERTIONS.assertFalse(result["allowed"])
    ASSERTIONS.assertEqual(result["mode"], "live")
    ASSERTIONS.assertIn("explicit_merge_authorization_required", result["reason"])


def test_wrapper_helpers_default_to_disabled_when_mode_missing():
    safe = controller.can_run_safe_autofix({"SAFE_AUTOFIX_ENABLED": "true"})
    push = controller.can_auto_push({"AUTO_PUSH_ENABLED": "true"})
    merge = controller.can_auto_merge({"AUTO_MERGE_ENABLED": "true"})
    ASSERTIONS.assertEqual(safe["mode"], "disabled")
    ASSERTIONS.assertEqual(push["mode"], "disabled")
    ASSERTIONS.assertEqual(merge["mode"], "disabled")
    ASSERTIONS.assertFalse(safe["allowed"])
    ASSERTIONS.assertFalse(push["allowed"])
    ASSERTIONS.assertFalse(merge["allowed"])


def test_can_run_live_action_github_mutation_requires_explicit_flag():
    denied = controller.can_run_live_action("github_mutation", {"AUTOMATION_MODE": "live"})
    ASSERTIONS.assertFalse(denied["allowed"])
    ASSERTIONS.assertEqual(denied["reason"], "github_mutation_enabled_disabled")

    allowed = controller.can_run_live_action(
        "github_mutation",
        {"AUTOMATION_MODE": "live", "GITHUB_MUTATION_ENABLED": "true"},
    )
    ASSERTIONS.assertTrue(allowed["allowed"])


def test_can_run_live_action_external_side_effect_requires_explicit_flag():
    denied = controller.can_run_live_action("external_side_effect", {"AUTOMATION_MODE": "live"})
    ASSERTIONS.assertFalse(denied["allowed"])
    ASSERTIONS.assertEqual(denied["reason"], "external_side_effect_enabled_disabled")

    allowed = controller.can_run_live_action(
        "external_side_effect",
        {"AUTOMATION_MODE": "live", "EXTERNAL_SIDE_EFFECT_ENABLED": "true"},
    )
    ASSERTIONS.assertTrue(allowed["allowed"])


def test_can_auto_resolve_and_rerun_checks_require_explicit_true_flags():
    resolve_denied = controller.can_auto_resolve_review_threads(
        {"AUTOMATION_MODE": "live", "AUTO_RESOLVE_ENABLED": "false"}
    )
    rerun_denied = controller.can_auto_rerun_checks(
        {"AUTOMATION_MODE": "live", "AUTO_RERUN_ENABLED": "1"}
    )
    ASSERTIONS.assertFalse(resolve_denied["allowed"])
    ASSERTIONS.assertFalse(rerun_denied["allowed"])

    resolve_allowed = controller.can_auto_resolve_review_threads(
        {"AUTOMATION_MODE": "live", "AUTO_RESOLVE_ENABLED": "true"}
    )
    rerun_allowed = controller.can_auto_rerun_checks(
        {"AUTOMATION_MODE": "live", "AUTO_RERUN_ENABLED": "true"}
    )
    ASSERTIONS.assertTrue(resolve_allowed["allowed"])
    ASSERTIONS.assertTrue(rerun_allowed["allowed"])


def test_can_auto_resolve_review_threads_modes_block_live_actions_fail_closed():
    for mode in ("disabled", "report_only", "plan_only", "supervised", "unknown"):
        result = controller.can_auto_resolve_review_threads(
            {"AUTOMATION_MODE": mode, "AUTO_RESOLVE_ENABLED": "true"}
        )
        ASSERTIONS.assertFalse(result["allowed"])


def test_can_auto_resolve_review_threads_malformed_flag_fails_closed():
    malformed = controller.can_auto_resolve_review_threads(
        {"AUTOMATION_MODE": "live", "AUTO_RESOLVE_ENABLED": "yes"}
    )
    ASSERTIONS.assertFalse(malformed["allowed"])


def test_build_automation_enablement_context_runtime_mode_and_flags_override_env():
    context = controller.build_automation_enablement_context(
        {
            "AUTOMATION_MODE": "live",
            "AUTO_MERGE_ENABLED": "false",
        },
        {
            "automation_mode": "report_only",
            "automation_flags": {"AUTO_MERGE_ENABLED": "true"},
            "mergeable": "MERGEABLE",
        },
    )
    ASSERTIONS.assertEqual(context["automation_mode"], "report_only")
    ASSERTIONS.assertTrue(context["automation_flags"]["AUTO_MERGE_ENABLED"])
    ASSERTIONS.assertEqual(context["mergeable"], "MERGEABLE")


def test_can_auto_merge_reason_codes_for_missing_merge_evidence():
    bad = controller.can_auto_merge(_automation_ctx("live", bad="bad-type", blockers="bad-type"))
    pending = controller.can_auto_merge(_automation_ctx("live", pending="bad-type"))
    unresolved = controller.can_auto_merge(_automation_ctx("live", unresolved_active="bad-type"))
    ASSERTIONS.assertEqual(bad["reason"], "bad_checks_missing")
    ASSERTIONS.assertEqual(pending["reason"], "pending_checks_missing")
    ASSERTIONS.assertEqual(unresolved["reason"], "unresolved_active_missing")


def test_assert_live_action_allowed_raises_when_blocked():
    with ASSERTIONS.assertRaises(PermissionError):
        controller.assert_live_action_allowed("push", _automation_ctx("disabled"))

    allowed = controller.assert_live_action_allowed("safe_autofix", _automation_ctx("live"))
    ASSERTIONS.assertTrue(allowed["allowed"])
