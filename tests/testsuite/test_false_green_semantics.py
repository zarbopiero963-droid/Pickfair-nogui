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


def test_pr_guard_workflow_is_fail_closed_for_unknown_or_missing_task():
    workflow = Path(".github/workflows/pr-guard.yml").read_text(encoding="utf-8")

    assert "Install gh CLI" not in workflow
    assert "Authenticate gh" not in workflow

    # Old skip-path logic must be gone.
    assert "if: steps.guard_inputs.outputs.should_run_guard == 'true'" not in workflow
    assert "Skipping scope guard for unknown task" not in workflow
    assert "should_run_guard" not in workflow
    assert "guard_inputs" not in workflow

    # New fail-closed workflow should always fetch PR data and run guardrail.
    assert "Fetch PR metadata and files" in workflow
    assert 'pr_meta.json' in workflow
    assert 'pr_files_raw.json' in workflow
    assert "Run fail-closed guardrail" in workflow
    assert "python scripts/guardrail_check.py" in workflow

    # Workflow must read PR metadata/files through GitHub API before checking.
    assert 'gh api "repos/${{ github.repository }}/pulls/$PR_NUMBER" > pr_meta.json' in workflow
    assert 'gh api "repos/${{ github.repository }}/pulls/$PR_NUMBER/files?per_page=100" > pr_files_raw.json' in workflow