from pathlib import Path
import pytest
from scripts.guardrail_check import resolve_task, validate_task_selection


_ALLOWED = {
    "observability_phase2_eventbus_contract",
    "observability_phase3_contention_ambiguity",
    "audit_runtime_cto_final_control",
    "ci_pr_guard_task_case_normalization",
}


def _validate_callable_methods(owner, required_methods):
    for method_name in required_methods:
        if not hasattr(owner, method_name):
            return False
        if not callable(getattr(owner, method_name)):
            return False
    return True


def test_callable_validation_rejects_hasattr_only_false_green():
    class BrokenEntrypoints:
        submit_quick_bet = object()
        recover_after_restart = None

    assert _validate_callable_methods(
        BrokenEntrypoints,
        ("submit_quick_bet", "recover_after_restart"),
    ) is False


def test_pr_guard_workflow_uses_fail_closed_markers_only():
    workflow = Path(".github/workflows/pr-guard.yml").read_text(encoding="utf-8")

    assert "guard_inputs" not in workflow
    assert "should_run_guard" not in workflow
    assert "Skipping scope guard for unknown task" not in workflow
    assert "python scripts/guardrail_check.py" in workflow
    assert "pr_meta.json" in workflow
    assert "pr_files_raw.json" in workflow


def test_pr_check_workflow_keeps_pull_request_and_full_pytest_gate():
    workflow = Path(".github/workflows/pr-check.yml").read_text(encoding="utf-8")
    assert "pull_request:" in workflow
    assert "branches: [main]" in workflow
    assert "pytest -q" in workflow


def test_merge_simulation_hard_keeps_merge_validation_and_fail_closed_pytest():
    workflow = Path(".github/workflows/merge-simulation-hard.yml").read_text(encoding="utf-8")
    assert "workflow_call:" in workflow
    assert "pull_request:" in workflow
    assert "git fetch origin main" in workflow
    assert "git merge --no-ff --no-edit origin/main" in workflow
    assert "pytest -q -x" in workflow


def test_pr_guard_workflow_keeps_required_pr_metadata_and_shell_safety():
    workflow = Path(".github/workflows/pr-guard.yml").read_text(encoding="utf-8")
    assert "pull_request:" in workflow
    assert "types: [opened, edited, synchronize, reopened, ready_for_review]" in workflow
    assert "set -euo pipefail" in workflow
    assert "PR_TITLE" in workflow
    assert "PR_BODY" in workflow
    assert "jq -R -s 'split(\"\\n\") | map(select(length > 0))'" in workflow
    assert "python scripts/guardrail_check.py" in workflow


def test_observability_runtime_workflow_keeps_critical_path_filters():
    workflow_path = Path(".github/workflows/observability-runtime.yml")
    assert workflow_path.exists()
    workflow = workflow_path.read_text(encoding="utf-8")
    assert "observability/**" in workflow
    assert "services/telegram_alerts_service.py" in workflow
    assert "tests/observability/**" in workflow
    assert "tests/smoke/test_observability_full_flow.py" in workflow
    assert "tests/services/test_telegram_alert_pipeline.py" in workflow


def test_ci_dynamic_intelligent_and_changed_modules_cover_critical_routing():
    workflow = Path(".github/workflows/ci-dynamic-intelligent.yml").read_text(encoding="utf-8")
    router = Path("scripts/ci_changed_modules.py").read_text(encoding="utf-8")

    assert "python scripts/ci_changed_modules.py origin/main > changed_modules.json" in workflow
    assert "merge-simulation-hard:" in workflow

    assert '"name": "runtime-controller"' in router
    assert '"name": "chaos-critical"' in router
    assert '"name": "trading-engine"' in router
    assert '"name": "order-manager"' in router
    assert '"name": "simulation-broker"' in router
    assert '"core/runtime_controller.py"' in router
    assert '"core/reconciliation_engine.py"' in router
    assert '"core/trading_engine.py"' in router
    assert '"order_manager.py"' in router
    assert '"tests/chaos/"' in router


def test_pr_title_task_is_accepted():
    task, source, unknown, ignored = resolve_task(
        {
            "title": "[TASK: observability_phase2_eventbus_contract]",
            "body": "",
            "branch": "feature/no-marker",
            "latest_commit_message": "no marker here",
        },
        [],
        _ALLOWED,
    )
    assert task == "observability_phase2_eventbus_contract"
    assert source == "pr_title"
    assert unknown == []
    assert ignored == []


def test_pr_body_task_is_accepted():
    task, source, unknown, ignored = resolve_task(
        {
            "title": "normal title",
            "body": "details [TASK: observability_phase3_contention_ambiguity]",
            "branch": "feature/no-marker",
            "latest_commit_message": "no marker here",
        },
        [],
        _ALLOWED,
    )
    assert task == "observability_phase3_contention_ambiguity"
    assert source == "pr_body"
    assert unknown == []
    assert ignored == []


def test_branch_task_is_accepted():
    task, source, unknown, ignored = resolve_task(
        {
            "title": "normal title",
            "body": "",
            "branch": "feature/[TASK: audit_runtime_cto_final_control]",
            "latest_commit_message": "no marker here",
        },
        [],
        _ALLOWED,
    )
    assert task == "audit_runtime_cto_final_control"
    assert source == "branch"
    assert unknown == []
    assert ignored == []


def test_latest_commit_task_is_accepted():
    task, source, unknown, ignored = resolve_task(
        {
            "title": "normal title",
            "body": "",
            "branch": "feature/no-marker",
            "latest_commit_message": "commit subject [TASK: ci_pr_guard_task_case_normalization]",
        },
        [],
        _ALLOWED,
    )
    assert task == "ci_pr_guard_task_case_normalization"
    assert source == "latest_commit_message"
    assert unknown == []
    assert ignored == []


def test_task_marker_can_come_from_commit_messages_list():
    task, source, _, _ = resolve_task(
        {
            "title": "normal title",
            "body": "",
            "branch": "feature/no-marker",
            "latest_commit_message": "no marker",
            "commit_messages": ["misc", "[TASK: observability_phase2_eventbus_contract] extra"],
        },
        [],
        _ALLOWED,
    )
    assert task == "observability_phase2_eventbus_contract"
    assert source == "commit_messages"


def test_task_file_change_is_detected_from_ops_tasks_paths():
    task, source, _, _ = resolve_task(
        {"title": "", "body": "", "branch": "", "latest_commit_message": ""},
        ["ops/tasks/123.md"],
        _ALLOWED,
    )
    assert task == "task_file_change"
    assert source == "changed_task_files"


def test_task_file_change_cannot_bypass_critical_file_protection():
    with pytest.raises(SystemExit):
        validate_task_selection("task_file_change", ["core/trading_engine.py"], _ALLOWED, [])


def test_task_guard_fails_when_no_source_exists_anywhere():
    task, source, unknown, ignored = resolve_task(
        {
            "title": "normal title",
            "body": "",
            "branch": "feature/no-marker",
            "latest_commit_message": "no task marker",
        },
        ["src/file.py"],
        _ALLOWED,
    )
    assert task is None
    assert source is None
    assert unknown == []
    assert ignored == []


def test_placeholder_is_ignored_when_allowlisted_commit_message_exists():
    task, source, unknown, ignored = resolve_task(
        {
            "title": "[TASK: ....]",
            "body": "",
            "branch": "work",
            "latest_commit_message": "",
            "commit_messages": ["note", "[TASK: ci_pr_guard_task_case_normalization]"],
        },
        [],
        _ALLOWED,
    )
    assert task == "ci_pr_guard_task_case_normalization"
    assert source == "commit_messages"
    assert unknown == []
    assert ignored


def test_placeholder_alone_does_not_pass():
    task, source, unknown, ignored = resolve_task(
        {"title": "[TASK: ....]", "body": "", "branch": "work", "latest_commit_message": ""},
        [],
        _ALLOWED,
    )
    assert task is None
    assert source is None
    assert unknown == []
    assert ignored


def test_unknown_valid_format_task_fails_allowlist_validation():
    task, source, unknown, ignored = resolve_task(
        {"title": "[TASK: unknown_new_task]", "body": "", "branch": "", "latest_commit_message": ""},
        [],
        _ALLOWED,
    )
    assert task is None
    assert source is None
    assert unknown == [("pr_title", "unknown_new_task")]
    assert ignored == []
    with pytest.raises(SystemExit):
        validate_task_selection(task, [], _ALLOWED, unknown)


def test_mixed_case_allowlisted_task_normalizes_and_passes():
    task, source, unknown, ignored = resolve_task(
        {"title": "[TASK: Ci_Pr_Guard_Task_Case_Normalization]", "body": "", "branch": "", "latest_commit_message": ""},
        [],
        _ALLOWED,
    )
    assert task == "ci_pr_guard_task_case_normalization"
    assert source == "pr_title"
    assert unknown == []
    assert ignored == []


def test_mixed_case_unknown_task_is_recorded_normalized_and_fails():
    task, source, unknown, ignored = resolve_task(
        {"title": "[TASK: UnKnown_New_Task]", "body": "", "branch": "", "latest_commit_message": ""},
        [],
        _ALLOWED,
    )
    assert task is None
    assert source is None
    assert unknown == [("pr_title", "unknown_new_task")]
    assert ignored == []
    with pytest.raises(SystemExit):
        validate_task_selection(task, [], _ALLOWED, unknown)


def test_legacy_pr_guard_task_passes():
    task, source, unknown, ignored = resolve_task(
        {"title": "[TASK: pr_guard]", "body": "", "branch": "", "latest_commit_message": ""},
        [],
        _ALLOWED,
    )
    assert task == "pr_guard"
    assert source == "pr_title"
    assert unknown == []
    assert ignored == []


def test_legacy_pr_guard_mixed_case_normalizes_and_passes():
    task, source, unknown, ignored = resolve_task(
        {"title": "[TASK: PR_GUARD]", "body": "", "branch": "", "latest_commit_message": ""},
        [],
        _ALLOWED,
    )
    assert task == "pr_guard"
    assert source == "pr_title"
    assert unknown == []
    assert ignored == []


def test_pr_random_still_fails():
    task, source, unknown, ignored = resolve_task(
        {"title": "[TASK: pr_random]", "body": "", "branch": "", "latest_commit_message": ""},
        [],
        _ALLOWED,
    )
    assert task is None
    assert source is None
    assert unknown == [("pr_title", "pr_random")]
    assert ignored == []
    with pytest.raises(SystemExit):
        validate_task_selection(task, [], _ALLOWED, unknown)


def test_explicit_allowlisted_task_outside_prefix_passes():
    custom_allowed = set(_ALLOWED) | {"deploy_hotfix"}
    task, source, unknown, ignored = resolve_task(
        {"title": "[TASK: deploy_hotfix]", "body": "", "branch": "", "latest_commit_message": ""},
        [],
        custom_allowed,
    )
    assert task == "deploy_hotfix"
    assert source == "pr_title"
    assert unknown == []
    assert ignored == []


def test_explicit_allowlisted_task_mixed_case_normalizes_and_passes():
    custom_allowed = set(_ALLOWED) | {"fix_regression"}
    task, source, unknown, ignored = resolve_task(
        {"title": "[TASK: FIX_REGRESSION]", "body": "", "branch": "", "latest_commit_message": ""},
        [],
        custom_allowed,
    )
    assert task == "fix_regression"
    assert source == "pr_title"
    assert unknown == []
    assert ignored == []


def test_observability_phase2_eventbus_contract_passes():
    task, source, _, _ = resolve_task(
        {"title": "[TASK: observability_phase2_eventbus_contract]", "body": "", "branch": "", "latest_commit_message": ""},
        [],
        _ALLOWED,
    )
    assert task == "observability_phase2_eventbus_contract"
    assert source == "pr_title"


def test_observability_phase3_contention_ambiguity_passes():
    task, source, _, _ = resolve_task(
        {"title": "[TASK: observability_phase3_contention_ambiguity]", "body": "", "branch": "", "latest_commit_message": ""},
        [],
        _ALLOWED,
    )
    assert task == "observability_phase3_contention_ambiguity"
    assert source == "pr_title"


def test_audit_runtime_cto_final_control_passes():
    task, source, _, _ = resolve_task(
        {"title": "[TASK: audit_runtime_cto_final_control]", "body": "", "branch": "", "latest_commit_message": ""},
        [],
        _ALLOWED,
    )
    assert task == "audit_runtime_cto_final_control"
    assert source == "pr_title"
