from pathlib import Path
import re
import importlib.util
import pytest
from scripts.guardrail_check import resolve_task, validate_task_selection


_ALLOWED = {
    "observability_phase2_eventbus_contract",
    "observability_phase3_contention_ambiguity",
    "audit_runtime_cto_final_control",
    "ci_pr_guard_task_case_normalization",
}

def _read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def _workflow(path: str):
    text = _read(path)
    try:
        import yaml  # type: ignore
    except Exception:
        return {"_text": text}
    data = yaml.safe_load(text)
    if isinstance(data, dict) and True in data and "on" not in data:
        data["on"] = data[True]
    return data if isinstance(data, dict) else {"_text": text}


def _run_text(workflow_obj) -> str:
    if not isinstance(workflow_obj, dict) or "_text" in workflow_obj:
        return str((workflow_obj or {}).get("_text", ""))
    runs = []
    jobs = workflow_obj.get("jobs") or {}
    if isinstance(jobs, dict):
        for _, job in jobs.items():
            if not isinstance(job, dict):
                continue
            steps = job.get("steps") or []
            if isinstance(steps, list):
                for step in steps:
                    if isinstance(step, dict) and isinstance(step.get("run"), str):
                        runs.append(step["run"])
    return "\n".join(runs)


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
    wf = _workflow(".github/workflows/pr-check.yml")
    run_blocks = _run_text(wf)
    trigger_block = (wf.get("on") if isinstance(wf, dict) and "_text" not in wf else None) or {}

    if isinstance(trigger_block, dict) and trigger_block:
        assert "pull_request" in trigger_block
        pull = trigger_block.get("pull_request")
        if isinstance(pull, dict):
            branches = pull.get("branches")
            assert isinstance(branches, list)
            assert "main" in [str(x) for x in branches]
    else:
        raw = _read(".github/workflows/pr-check.yml")
        assert re.search(r"(?m)^\s*pull_request\s*:", raw)
        assert (
            re.search(r"(?ms)^\s*pull_request\s*:\s*\n(?:[ \t]+.*\n)*?[ \t]+branches\s*:\s*\[\s*main\s*\]", raw)
            or re.search(r"(?ms)^\s*pull_request\s*:\s*\n(?:[ \t]+.*\n)*?[ \t]+branches\s*:\s*\n(?:[ \t]+-\s*main\s*\n)", raw)
        )

    assert re.search(r"pytest\s+(-q\s+)?(?:-x\s+)?(?:tests?[\w/\s\.-]*)?$", run_blocks, flags=re.MULTILINE)


def test_merge_simulation_hard_keeps_merge_validation_and_fail_closed_pytest():
    wf = _workflow(".github/workflows/merge-simulation-hard.yml")
    run_blocks = _run_text(wf)
    trigger_block = (wf.get("on") if isinstance(wf, dict) and "_text" not in wf else None) or {}
    raw = _read(".github/workflows/merge-simulation-hard.yml")

    if isinstance(trigger_block, dict) and trigger_block:
        assert "workflow_call" in trigger_block
        assert "pull_request" in trigger_block
    else:
        assert re.search(r"(?m)^\s*workflow_call\s*:", raw)
        assert re.search(r"(?m)^\s*pull_request\s*:", raw)

    assert re.search(r"git\s+fetch\s+origin\s+main", run_blocks)
    assert re.search(r"git\s+merge\b[^\n]*origin/main", run_blocks)
    assert re.search(r"pytest\s+-q(?:\s+-x|\s+.*\s-x|\s+-x\s+.*)", run_blocks)


def test_pr_guard_workflow_keeps_required_pr_metadata_and_shell_safety():
    wf = _workflow(".github/workflows/pr-guard.yml")
    raw = _read(".github/workflows/pr-guard.yml")
    run_blocks = _run_text(wf)
    trigger_block = (wf.get("on") if isinstance(wf, dict) and "_text" not in wf else None) or {}

    if isinstance(trigger_block, dict) and trigger_block:
        assert "pull_request" in trigger_block
        pull = trigger_block.get("pull_request")
        if isinstance(pull, dict) and isinstance(pull.get("types"), list):
            types = {str(x) for x in pull.get("types")}
            for required in {"opened", "edited", "synchronize", "reopened"}:
                assert required in types
    else:
        assert re.search(r"(?m)^\s*pull_request\s*:", raw)

    assert "set -euo pipefail" in run_blocks
    assert re.search(r"python\s+scripts/guardrail_check\.py", run_blocks)
    for marker in ["PR_TITLE", "PR_BODY", "PR_HEAD_REF", "LATEST_COMMIT_MESSAGE", "commit_messages", "pr_files_raw.json", "pr_meta.json"]:
        assert marker in raw


def test_observability_runtime_workflow_keeps_critical_path_filters():
    workflow_path = ".github/workflows/observability-runtime.yml"
    assert Path(workflow_path).exists()
    workflow = _read(workflow_path)
    assert "observability/**" in workflow
    assert "services/telegram_alerts_service.py" in workflow
    assert "tests/observability/**" in workflow
    assert "tests/smoke/test_observability_full_flow.py" in workflow
    assert "tests/services/test_telegram_alert_pipeline.py" in workflow


def test_ci_dynamic_intelligent_and_changed_modules_cover_critical_routing():
    workflow = _read(".github/workflows/ci-dynamic-intelligent.yml")
    assert re.search(r"python\s+scripts/ci_changed_modules\.py\s+origin/main\s*>\s*changed_modules\.json", workflow)
    assert "merge-simulation-hard:" in workflow

    spec = importlib.util.spec_from_file_location("ci_changed_modules", "scripts/ci_changed_modules.py")
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    rules = getattr(mod, "MODULE_RULES")
    assert isinstance(rules, list)
    by_name = {r["name"]: set(r["paths"]) for r in rules if isinstance(r, dict) and "name" in r and "paths" in r}

    required = {
        "runtime-controller": {"core/runtime_controller.py", "tests/integration/test_runtime_controller"},
        "chaos-critical": {"core/reconciliation_engine.py", "order_manager.py", "tests/chaos/"},
        "trading-engine": {"core/trading_engine.py", "tests/integration/test_trading_engine"},
        "order-manager": {"order_manager.py", "tests/integration/test_order_manager"},
        "simulation-broker": {"simulation_broker.py", "tests/integration/test_simulation_broker"},
    }
    for rule_name, required_paths in required.items():
        assert rule_name in by_name
        assert required_paths.issubset(by_name[rule_name])
        for path in required_paths:
            owners = {name for name, paths in by_name.items() if path in paths}
            assert rule_name in owners, f"{path} must be routed by {rule_name}, owners={sorted(owners)}"


def test_module_ultra_check_uses_validation_only_guardrail_runner_fail_closed():
    wf = _workflow(".github/workflows/_module-ultra-check.yml")
    raw = _read(".github/workflows/_module-ultra-check.yml")

    target_steps = None
    if isinstance(wf, dict) and "_text" not in wf:
        jobs = wf.get("jobs")
        assert isinstance(jobs, dict)
        for job in jobs.values():
            if not isinstance(job, dict):
                continue
            steps = job.get("steps")
            if not isinstance(steps, list):
                continue
            if any(isinstance(s, dict) and "python scripts/guardrail_runner.py --module" in str(s.get("run", "")) for s in steps):
                target_steps = steps
                break
    else:
        runner_pos = raw.find("python scripts/guardrail_runner.py --module")
        json_pos = raw.find("- name: Validate guardrail JSON syntax")
        focused_pos = raw.find("- name: Run module-focused tests")
        full_pos = raw.find("- name: Run full test suite (backstop)")
        assert json_pos != -1
        assert runner_pos != -1
        assert focused_pos != -1
        assert full_pos != -1
        assert json_pos < runner_pos < focused_pos
        assert json_pos < runner_pos < full_pos
        assert "set -euo pipefail" in raw
        assert "--run-mutations" not in raw
        assert "--mutation-timeout-sec" not in raw
        assert "inputs.module_path" in raw
        return

    assert isinstance(target_steps, list)

    json_idx = None
    runner_idx = None
    focused_idx = None
    full_idx = None
    runner_run = ""

    for idx, step in enumerate(target_steps):
        if not isinstance(step, dict):
            continue
        step_name = str(step.get("name", ""))
        step_run = str(step.get("run", ""))
        if "Validate guardrail JSON syntax" in step_name:
            json_idx = idx
        if "python scripts/guardrail_runner.py --module" in step_run:
            runner_idx = idx
            runner_run = step_run
        if "Run module-focused tests" in step_name:
            focused_idx = idx
        if "Run full test suite (backstop)" in step_name:
            full_idx = idx

    assert json_idx is not None
    assert runner_idx is not None
    assert runner_idx == json_idx + 1
    assert focused_idx is not None and runner_idx < focused_idx
    assert full_idx is not None and runner_idx < full_idx
    assert "set -euo pipefail" in runner_run
    assert "--run-mutations" not in runner_run
    assert "--mutation-timeout-sec" not in runner_run
    assert "inputs.module_path" in raw


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


def _resolve_repo_root_from_test_file() -> Path:
    anchors = (
        lambda root: (root / ".git").exists(),
        lambda root: (root / "pyproject.toml").is_file(),
        lambda root: (root / ".github").is_dir() and (root / "guardrails").is_dir(),
    )

    for candidate in (Path(__file__).resolve(), *Path(__file__).resolve().parents):
        if not candidate.is_dir():
            continue
        if any(check(candidate) for check in anchors):
            return candidate

    raise AssertionError("Could not locate repository root from test file path")


def test_guardrail_json_covers_real_hardening_authority_modules_with_existing_files_only():
    import json

    authorities = {
        "services/telegram_signal_processor.py": "telegram_signal_processor",
        "services/telegram_bet_resolver.py": "telegram_bet_resolver",
        "telegram_sender.py": "telegram_sender",
        "shutdown_manager.py": "shutdown_manager",
        "core/risk_desk.py": "core.risk_desk",
        "core/safety_layer.py": "core.safety_layer",
        "core/order_router.py": "core.order_router",
    }
    kinds = ("specs", "contracts", "state_models", "mutations")
    repo_root = _resolve_repo_root_from_test_file()

    for module_path, module_name in authorities.items():
        assert (repo_root / module_path).is_file(), f"{module_path} missing"

        for kind in kinds:
            guardrail_path = repo_root / "guardrails" / kind / f"{module_name}.json"
            assert guardrail_path.is_file(), f"{guardrail_path} missing"

            payload = json.loads(guardrail_path.read_text(encoding="utf-8"))
            assert payload.get("module") == module_name

            mapped_paths = payload.get("module_paths")
            assert isinstance(mapped_paths, list), f"{guardrail_path}: module_paths must be a list"
            assert module_path in mapped_paths, f"{guardrail_path}: {module_path} not mapped"

            for mapped in mapped_paths:
                assert (repo_root / mapped).is_file(), f"{guardrail_path}: mapped path missing: {mapped}"

            focused_tests = payload.get("focused_tests", [])
            assert isinstance(focused_tests, list), f"{guardrail_path}: focused_tests must be a list"
            for test_path in focused_tests:
                assert (repo_root / test_path).is_file(), f"{guardrail_path}: focused test missing: {test_path}"

            if kind == "mutations":
                mutations = payload.get("mutations")
                assert isinstance(mutations, list), f"{guardrail_path}: mutations must be a list"
                assert mutations, f"{guardrail_path}: mutations must not be empty"
                for mutation in mutations:
                    assert isinstance(mutation, dict), f"{guardrail_path}: mutation must be an object"
                    assert mutation.get("id"), f"{guardrail_path}: mutation id missing"
                    assert "expected_failure" in mutation, f"{guardrail_path}: expected_failure missing"


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


def test_hardening_phase1_telegram_ingestion_tests_passes():
    task, source, unknown, ignored = resolve_task(
        {"title": "[TASK: hardening_phase1_telegram_ingestion_tests]", "body": "", "branch": "", "latest_commit_message": ""},
        [],
        _ALLOWED,
    )
    assert task == "hardening_phase1_telegram_ingestion_tests"
    assert source == "pr_title"
    assert unknown == []
    assert ignored == []


def test_hardening_phase0_5_reality_remap_passes():
    task, source, unknown, ignored = resolve_task(
        {"title": "[TASK: hardening_phase0_5_reality_remap]", "body": "", "branch": "", "latest_commit_message": ""},
        [],
        _ALLOWED,
    )
    assert task == "hardening_phase0_5_reality_remap"
    assert source == "pr_title"
    assert unknown == []
    assert ignored == []


def test_hardening_mixed_case_normalizes_and_passes():
    task, source, unknown, ignored = resolve_task(
        {"title": "[TASK: HARDENING_PHASE1_TELEGRAM_INGESTION_TESTS]", "body": "", "branch": "", "latest_commit_message": ""},
        [],
        _ALLOWED,
    )
    assert task == "hardening_phase1_telegram_ingestion_tests"
    assert source == "pr_title"
    assert unknown == []
    assert ignored == []
