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


def test_pr_guard_workflow_skips_unknown_task_instead_of_static_blocking():
    workflow = Path('.github/workflows/pr-guard.yml').read_text(encoding='utf-8')

    assert "Install gh CLI" not in workflow
    assert "Authenticate gh" not in workflow
    assert "if: steps.guard_inputs.outputs.should_run_guard == 'true'" in workflow
    assert "Skipping scope guard for unknown task" in workflow
