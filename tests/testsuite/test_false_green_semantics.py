from pathlib import Path


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
