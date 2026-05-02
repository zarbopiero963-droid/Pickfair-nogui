from pathlib import Path
import pytest
from scripts.guardrail_check import resolve_task, validate_task_selection


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


def test_task_marker_can_come_from_branch_or_commit_when_pr_text_missing():
    task, source = resolve_task(
        {
            "title": "normal title",
            "body": "",
            "branch": "feature/[TASK: observability_phase1_review_fixes]",
            "latest_commit_message": "no marker here",
        },
        [],
    )
    assert task == "observability_phase1_review_fixes"
    assert source == "branch"

    task2, source2 = resolve_task(
        {
            "title": "normal title",
            "body": "",
            "branch": "feature/no-marker",
            "latest_commit_message": "commit subject [TASK: restore_pr_guard_workflow]",
        },
        [],
    )
    assert task2 == "restore_pr_guard_workflow"
    assert source2 == "latest_commit_message"


def test_task_marker_can_come_from_commit_messages_list():
    task, source = resolve_task(
        {
            "title": "normal title",
            "body": "",
            "branch": "feature/no-marker",
            "latest_commit_message": "no marker",
            "commit_messages": ["misc", "[TASK: ci_pr_guard_task_source_hardening] extra"],
        },
        [],
    )
    assert task == "ci_pr_guard_task_source_hardening"
    assert source == "commit_messages"


def test_task_file_change_is_detected_from_ops_tasks_paths():
    task, source = resolve_task(
        {"title": "", "body": "", "branch": "", "latest_commit_message": ""},
        ["ops/tasks/123.md"],
    )
    assert task == "task_file_change"
    assert source == "changed_task_files"


def test_task_file_change_cannot_bypass_critical_file_protection():
    with pytest.raises(SystemExit):
        validate_task_selection("task_file_change", ["core/trading_engine.py"], set())


def test_task_guard_fails_when_no_source_exists_anywhere():
    task, source = resolve_task(
        {
            "title": "normal title",
            "body": "",
            "branch": "feature/no-marker",
            "latest_commit_message": "no task marker",
        },
        ["src/file.py"],
    )
    assert task is None
    assert source is None
