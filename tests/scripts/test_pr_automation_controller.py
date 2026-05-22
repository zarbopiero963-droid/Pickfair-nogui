"""Tests for PR automation controller decisions."""
# pylint: disable=invalid-name,duplicate-code

import argparse
import copy
import json
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
    prompt = controller.build_codex_task_prompt({"task": "workflow hygiene", "objective": "contracted prompts"})
    for section in controller.CODEX_PROMPT_REQUIRED_SECTIONS:
        ASSERTIONS.assertIn(f"{section}:", prompt)
    ASSERTIONS.assertIn("PHASE 0 PRE-FLIGHT (READ-ONLY)", prompt)
    ASSERTIONS.assertTrue(controller.validate_codex_prompt_contract(prompt)["valid"])


def test_ensure_codex_prompt_contract_appends_missing_sections():
    """Contract-enforcer appends missing sections and keeps required audit/preflight headers."""
    prompt = controller.ensure_codex_prompt_contract("TASK:\nFix this")
    ASSERTIONS.assertEqual(controller.codex_prompt_contract_missing_sections(prompt), [])
    ASSERTIONS.assertIn("POST-FIX MICRO-AUDIT BEFORE COMMIT", prompt)
    ASSERTIONS.assertIn("PHASE 0 PRE-FLIGHT (READ-ONLY)", prompt)


def test_validate_codex_prompt_contract_reports_missing_sections_for_incomplete_prompt():
    """Incomplete prompt fails contract validation and reports required missing sections."""
    result = controller.validate_codex_prompt_contract("TASK:\nfix this")
    ASSERTIONS.assertIn("OBJECTIVE", result["missing_sections"])
    ASSERTIONS.assertIn("CONTEXT", result["missing_sections"])
    ASSERTIONS.assertFalse(result["valid"])


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
    report = controller.parse_phase0_preflight_result(
        json.dumps({"status": "PASS", "risk_level": "low", "next_action": "generate_patch_prompt"})
    )
    ASSERTIONS.assertEqual(report["status"], "PASS")
    ASSERTIONS.assertEqual(report["risk_level"], "low")
    ASSERTIONS.assertEqual(report["next_action"], "generate_patch_prompt")
    ASSERTIONS.assertFalse(controller.phase0_preflight_failed(report))


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
    ASSERTIONS.assertEqual(report["next_action"], "needs_manual_phase0_failed")


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
            controller.build_codex_task_prompt({"task": "Fix lint", "objective": "Address blockers"}),
            "ALLOW_COMMIT_PUSH: yes",
            "METHOD:\nRun git commit and git push origin branch",
        ]
    )
    result = controller.validate_codex_prompt_contract(prompt)
    ASSERTIONS.assertTrue(result["valid"])


def test_validate_codex_prompt_contract_does_not_reject_descriptive_committing_word():
    """Descriptive words like committing do not trigger unsafe commit/push detection."""
    prompt = "\n".join(
        [
            controller.build_codex_task_prompt({"task": "Fix lint", "objective": "Address blockers"}),
            "METHOD:\nDocument expected steps before committing anything.",
        ]
    )
    result = controller.validate_codex_prompt_contract(prompt)
    ASSERTIONS.assertTrue(result["valid"])


def test_validate_codex_prompt_contract_does_not_reject_post_fix_micro_audit_header():
    """POST-FIX MICRO-AUDIT BEFORE COMMIT header is mandatory and must remain valid."""
    prompt = controller.ensure_codex_prompt_contract("TASK:\nFix lint")
    result = controller.validate_codex_prompt_contract(prompt)
    ASSERTIONS.assertTrue(result["valid"])


def test_validate_codex_prompt_contract_does_not_reject_do_not_commit_or_push():
    """Negative guardrails about commit/push must not be treated as unsafe instructions."""
    prompt = "\n".join(
        [
            controller.build_codex_task_prompt({"task": "Fix lint", "objective": "Address blockers"}),
            "METHOD:\nDo not commit. Do not push. no commit/push.",
        ]
    )
    result = controller.validate_codex_prompt_contract(prompt)
    ASSERTIONS.assertTrue(result["valid"])


def test_section_has_value_rejects_tbd_list_placeholder():
    """Section content with only list placeholder values like - TBD remains invalid."""
    prompt = "\n".join(
        [
            "TASK:\nFix lint",
            "OBJECTIVE:\n- TBD",
            "CONTEXT:\nReady",
            "VALIDATION:\n- python3 -m pytest -q",
        ]
    )
    ASSERTIONS.assertFalse(controller._section_has_value(prompt, "OBJECTIVE"))


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


def test_codacy_rule_conflict_symbol_less_different_lines_are_not_conflict():
    """Symbol-less D203/D211 on different lines must not be treated as a conflict."""
    result = controller.classify_codacy_evidence(
        _codacy_state_payload(
            api_issues=2,
            issues=[
                {"filePath": "a.py", "patternId": "D203", "lineNumber": 10},
                {"filePath": "a.py", "patternId": "D211", "lineNumber": 20},
            ],
        )
    )
    ASSERTIONS.assertNotEqual(result["classification"], "rule_conflict")


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
                "next_action": "auto_resolve_merge_conflict",
                "reason": "merge conflict can be auto-resolved",
            }
        ],
        {},
    )

    ASSERTIONS.assertEqual(summary["primary_category"], "merge_conflict")
    ASSERTIONS.assertEqual(summary["next_action"], "auto_resolve_merge_conflict")


def test_scope_paths_skips_empty_list_and_continues():
    """Scope parsing should continue scanning keys after an empty list value."""
    scope_paths = controller._scope_paths(  # pylint: disable=protected-access
        {"files": [], "allowlist": ["scripts/x.py"]}
    )

    ASSERTIONS.assertEqual(scope_paths, {"scripts/x.py"})


def test_classify_merge_conflict_contract_paths():
    """Merge conflict classification differentiates safe out-of-scope automation from manual critical files."""
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
