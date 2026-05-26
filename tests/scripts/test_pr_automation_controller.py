"""Tests for PR automation controller decisions."""
# pylint: disable=invalid-name,duplicate-code

import argparse
import copy
import json
import re
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


def test_classify_merge_conflict_contract_paths():
    """Merge conflict classification mirrors baseline auto/manual taxonomy contract."""
    dirty = controller.classify_merge_conflict(
        {"mergeable": "MERGEABLE", "mergeStateStatus": "DIRTY"},
        ["scripts/pr_flow_automation.py"],
        {"files": ["scripts/pr_automation_controller.py"]},
    )
    ASSERTIONS.assertEqual(dirty["category"], "merge_conflict")
    ASSERTIONS.assertTrue(dirty["auto_resolvable"])
    ASSERTIONS.assertEqual(dirty["resolution_strategy"], "take_main_for_out_of_scope_automation")
    ASSERTIONS.assertEqual(dirty["next_action"], "auto_resolve_merge_conflict")
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
    ASSERTIONS.assertEqual(p1["classification"], "blocking")
    ASSERTIONS.assertEqual(high["classification"], "blocking")


def test_review_classification_low_nit_is_advisory_and_not_safe_without_evidence():
    thread = _review_thread(body="low nitpick style suggestion docs")
    classified = controller.classify_review_thread(thread)
    ASSERTIONS.assertEqual(classified["classification"], "advisory")
    ASSERTIONS.assertFalse(controller.should_resolve_review_thread(thread, {}))


def test_review_stale_fixed_thread_with_evidence_is_safe_to_resolve():
    thread = _review_thread(body="style", thread_id="T3")
    evidence = {
        "safe_to_resolve": True,
        "issue_fixed_or_stale": True,
        "head_matches": True,
        "validation_passed": True,
        "pending_checks": False,
        "failing_checks": False,
        "reply_body": "Fixed in latest patch.",
    }
    ASSERTIONS.assertTrue(controller.should_resolve_review_thread(thread, evidence))


def test_review_unknown_author_routes_needs_manual():
    classified = controller.classify_review_thread(_review_thread(author="mystery-user", body="please revisit"))
    ASSERTIONS.assertEqual(classified["classification"], "needs_manual")


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
        "validation_passed": True,
        "pending_checks": False,
        "failing_checks": False,
        "codacy_relevant": True,
        "codacy_state": "ACTION_REQUIRED",
        "codacy_annotations_count": 1,
        "reply_body": "stale",
    }
    good = dict(bad) | {"codacy_state": "SUCCESS", "codacy_annotations_count": 0}
    ASSERTIONS.assertFalse(controller.should_resolve_review_thread(thread, bad))
    ASSERTIONS.assertTrue(controller.should_resolve_review_thread(thread, good))


def test_review_resolution_plan_includes_reply_body_and_missing_providers_not_blocking():
    thread = _review_thread(thread_id="T9", body="low style")
    plan = controller.build_review_thread_resolution_plan(
        [thread],
        {
            "resolution_evidence": {
                "T9": {
                    "safe_to_resolve": True,
                    "issue_fixed_or_stale": True,
                    "head_matches": True,
                    "validation_passed": True,
                    "pending_checks": False,
                    "failing_checks": False,
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
