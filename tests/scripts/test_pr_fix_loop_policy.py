"""Executable owner policy: bounded repair cycles and evidence-based triage."""
import importlib
import argparse
from pathlib import Path

import pytest

from scripts import pr_automation_controller as controller
from scripts import pr_flow_automation as flow
from scripts import pr_clean_scope_rebuild as rebuild


def test_existing_controller_uses_owner_cap_five():
    assert controller._budget_limits()["max_autofix_commits_per_pr"] == 5


def test_current_correct_contract_mutation_does_not_trigger_existing_patch_route():
    thread = {"id": "T1", "body": "Security bypass: mutation allows two engines", "author": {"login": "codex"}}
    context = {
        "current_head_sha": "head", "evidence_head_sha": "head",
        "review_assessments": {"T1": assessment("THEORETICAL_MUTATION")},
    }
    assert controller.triage_review_thread_contract(thread, context)["decision"] != "PATCH_REQUIRED"


@pytest.fixture
def policy():
    return importlib.import_module("scripts.pr_fix_loop_policy")


def assessment(kind, **extra):
    return {
        "class": kind, "thread_id": "T1", "current_head_sha": "head",
        "evidence": "local contract inspection and reproducible test",
        "current_head_correct": kind != "CURRENT_DEFECT",
        **extra,
    }


@pytest.fixture
def ledger(policy, tmp_path):
    result = policy.FixLoopLedger(tmp_path / "budget.sqlite", "owner/repo", 495)
    result.initialize(0, "new PR; no review repair pushes")
    return result


def consume(ledger, name):
    result = ledger.reserve(name, assessment("CURRENT_DEFECT"))
    assert result["allowed"]
    assert ledger.complete(name, f"head-{name}")["completed_count"] >= 1


def test_cycles_one_through_five_then_six_stops(ledger, policy):
    assert policy.MAX_FIX_LOOP_ITERATIONS_PER_PR == 5
    for number in range(1, 6):
        consume(ledger, str(number))
    assert ledger.status()["completed_count"] == 5
    stopped = ledger.reserve("6", assessment("CURRENT_DEFECT"))
    assert not stopped["allowed"]
    assert stopped["AUTO_PR_FLOW_STATUS"] == "NEEDS_MANUAL"
    assert stopped["REASON"] == "fix_loop_budget_exhausted"
    assert not ledger.authorize("6")["allowed"]


@pytest.mark.parametrize("change", [
    "new_commit", "new_head", "reviewer", "resolved_threads", "green_ci",
    "partial_progress", "new_finding", "cluster", "review_rerun",
])
def test_budget_survives_every_progress_change(ledger, policy, change):
    for number in range(5):
        consume(ledger, str(number))
    reopened = policy.FixLoopLedger(ledger.path, "owner/repo", 495)
    reopened.initialize(0, change)  # initialization cannot overwrite history
    assert reopened.status()["completed_count"] == 5
    assert not reopened.reserve(change, assessment("CURRENT_DEFECT"))["allowed"]


def test_single_owner_override_is_bounded_and_not_replayed(ledger):
    for number in range(5):
        consume(ledger, str(number))
    decision = {
        "repo": "owner/repo", "pr": 495, "author": "owner", "id": "42",
        "html_url": "https://github.com/owner/repo/pull/495#issuecomment-42",
        "body": "OWNER_FIX_LOOP_OVERRIDE PR=495 ADDITIONAL=1 AT_COUNT=5",
    }
    ledger.grant_override(decision)
    consume(ledger, "six")
    ledger.grant_override(decision)
    assert ledger.status()["ceiling"] == 6
    assert not ledger.reserve("seven", assessment("CURRENT_DEFECT"))["allowed"]


def test_override_rejects_wrong_owner_pr_and_infinite_flag(ledger):
    for decision in (
        {"author": "reviewer", "repo": "owner/repo", "pr": 495},
        {"author": "owner", "repo": "owner/repo", "pr": 496},
        {"author": "owner", "repo": "owner/repo", "pr": 495,
         "body": "IGNORE_FIX_LOOP_LIMIT=true"},
    ):
        with pytest.raises(ValueError):
            ledger.grant_override(decision)


def test_missing_state_and_pending_cycle_fail_closed(policy, tmp_path, ledger):
    missing = policy.FixLoopLedger(tmp_path / "absent.sqlite", "owner/repo", 495)
    assert not missing.reserve("x", assessment("CURRENT_DEFECT"))["allowed"]
    assert ledger.reserve("one", assessment("CURRENT_DEFECT"))["allowed"]
    assert not ledger.reserve("two", assessment("CURRENT_DEFECT"))["allowed"]
    assert ledger.authorize("one")["allowed"]
    ledger.complete("one", "h1")
    ledger.complete("one", "h1")
    assert ledger.status()["completed_count"] == 1
    assert not ledger.authorize("one")["allowed"]


@pytest.mark.parametrize("kind,extra,expected", [
    ("CURRENT_DEFECT", {"type": "runtime_bug"}, "PATCH_REQUIRED"),
    ("CURRENT_DEFECT", {"type": "contract_contradiction"}, "PATCH_REQUIRED"),
    ("CURRENT_DEFECT", {"safety": True, "fail_open": True}, "PATCH_REQUIRED"),
    ("GUARDRAIL_GAP", {"contract_critical": True, "material": True}, "PATCH_REQUIRED"),
    ("GUARDRAIL_GAP", {"material": False}, "NEEDS_MANUAL"),
    ("THEORETICAL_MUTATION", {}, "NEEDS_MANUAL"),
    ("STALE", {}, "EVIDENCE_RESOLVE"),
    ("DUPLICATE", {}, "EVIDENCE_RESOLVE"),
])
def test_triage_matrix(policy, kind, extra, expected):
    assert policy.triage(assessment(kind, **extra))["decision"] == expected


def test_equivalent_mutations_never_consume_push_budget(ledger):
    for number, text in enumerate([
        "due motori", "un motore per modalità", "motori separati",
        "engine distinti", "SIM engine + LIVE engine",
    ]):
        result = ledger.reserve(str(number), assessment(
            "THEORETICAL_MUTATION", area="engine_contract", mutation=text,
        ))
        assert not result["allowed"]
        if number:
            assert result["REASON"] == "review_churn"
    assert ledger.status()["completed_count"] == 0


def test_accepted_limitation_requires_owner_and_never_says_fixed(policy):
    current = assessment("THEORETICAL_MUTATION")
    assert not policy.accept_limitation(current, {}, "owner/repo", 495)["allowed"]
    decision = {
        "repo": "owner/repo", "pr": 495, "author": "owner", "id": "99",
        "html_url": "https://github.com/owner/repo/pull/495#issuecomment-99",
        "body": "KNOWN_LIMITATION_ACCEPTED_BY_OWNER PR=495 THREAD=T1 HEAD=head",
    }
    result = policy.accept_limitation(current, decision, "owner/repo", 495)
    assert result["allowed"]
    assert result["decision"] == "KNOWN_LIMITATION_ACCEPTED_BY_OWNER"
    assert decision["html_url"] in result["reply_body"]
    assert "FIXED" not in result["reply_body"]
    assert not policy.accept_limitation(
        {**current, "safety": True}, decision, "owner/repo", 495,
    )["allowed"]


def repair_context(ledger, cycle):
    return {
        "repo": "owner/repo", "pr": 495, "POST_FIX_AUDIT": "PASS",
        "validation_passed": True, "current_head_matches": True,
        "dirty_worktree": False, "scope_allowed": True,
        "rollback_attempted": False, "rollback_succeeded": True,
        "task_no_commit_push": False, "automation_mode": "live",
        "can_commit": True, "can_push": True,
        "automation_flags": {"SAFE_AUTOFIX_ENABLED": True},
        "fix_loop": {"path": str(ledger.path), "repo": "owner/repo",
                     "pr": 495, "cycle_id": cycle},
    }


def test_real_push_persists_once_and_sixth_never_runs(ledger):
    calls = []
    def runner(command, **_kwargs):
        calls.append(command)
        return "new-head" if command[1] == "rev-parse" else ""
    for number in range(5):
        cycle = str(number)
        assert ledger.reserve(cycle, assessment("CURRENT_DEFECT"))["allowed"]
        context = repair_context(ledger, cycle)
        assert controller.can_run_safe_autofix(context)["allowed"]
        assert controller.can_commit_after_post_fix_audit(context)["allowed"]
        assert flow.push_with_retry_once(runner, "owner/repo", "branch", context=context)["ok"]
    assert ledger.status()["completed_count"] == 5
    count_before = len(calls)
    context = repair_context(ledger, "six")
    assert not controller.can_run_safe_autofix(context)["allowed"]
    assert not controller.can_commit_after_post_fix_audit(context)["allowed"]
    result = flow.push_with_retry_once(runner, "owner/repo", "branch", context=context)
    assert not result["ok"]
    assert result["AUTO_PR_FLOW_STATUS"] == "NEEDS_MANUAL"
    assert result["REASON"] == "fix_loop_budget_exhausted"
    assert len(calls) == count_before


def test_patch_task_is_blocked_before_phase_zero_can_grant_repair(ledger, monkeypatch):
    monkeypatch.setattr(controller, "decide_phase0_gate", lambda _report: {"can_patch": True})
    result = controller.build_codex_patch_task_after_phase0("patch", {}, repair_context(ledger, "missing"))
    assert result["can_patch"] is False


def test_missing_budget_flag_only_cannot_launch_autofix():
    result = controller.can_run_safe_autofix({
        "AUTOMATION_MODE": "live", "SAFE_AUTOFIX_ENABLED": "true",
    })
    assert not result["allowed"]
    assert result["reason"] == "fix_loop_reservation_missing"


@pytest.mark.parametrize("field,value", [
    ("checks_green", False), ("validation_passed", False),
    ("evidence_head_sha", "old-head"), ("pending_checks_count", 1),
    ("failing_checks_count", 1), ("head_matches", False),
    ("failing_current_head_checks", True), ("current_head_check_failure", True),
])
def test_owner_limitation_cannot_bypass_resolution_gates(policy, monkeypatch, field, value):
    thread, context = accepted_context()
    context[field] = value
    monkeypatch.setattr(policy, "fetch_owner_comment", lambda *_args: owner_acceptance())
    assert not controller.should_resolve_review_thread(thread, context)


def owner_acceptance():
    return {
        "repo": "owner/repo", "pr": 495, "author": "owner", "id": "99",
        "html_url": "https://github.com/owner/repo/pull/495#issuecomment-99",
        "body": "KNOWN_LIMITATION_ACCEPTED_BY_OWNER PR=495 THREAD=T1 HEAD=head",
    }


def accepted_context():
    return {"id": "T1", "body": "another theoretical mutation", "author": "any-reviewer"}, {
        "repo": "owner/repo", "pr": 495, "current_head_sha": "head", "evidence_head_sha": "head",
        "validation_passed": True, "checks_green": True,
        "tests_covering_behavior": ["current contract proof"],
        "pending_checks_count": 0, "failing_checks_count": 0,
        "review_assessments": {"T1": assessment("THEORETICAL_MUTATION")},
        "owner_decision_ids": {"T1": "99"},
    }


def test_owner_acceptance_is_fetched_and_resolution_plan_never_fakes_fixed(policy, monkeypatch):
    thread, context = accepted_context()
    fetched = []
    def api(repo, pr, comment):
        fetched.append((repo, pr, comment))
        return owner_acceptance()
    monkeypatch.setattr(policy, "fetch_owner_comment", api)
    assert controller.should_resolve_review_thread(thread, context)
    plan = controller.build_review_thread_resolution_plan([thread], context)
    item = plan["items"][0]
    assert item["safe_to_resolve"]
    assert item["disposition"] == "KNOWN_LIMITATION_ACCEPTED_BY_OWNER"
    assert "FIXED" not in item["reply_body"]
    assert fetched
    assert flow.eligible_review_comments_for_auto_resolve([thread], {**context, "evidence_only": True}) == [thread]


def test_corrupt_ledger_never_becomes_zero(policy, tmp_path):
    path = tmp_path / "corrupt.sqlite"
    path.write_bytes(b"not a database")
    ledger = policy.FixLoopLedger(path, "owner/repo", 495)
    assert not ledger.status()["allowed"]
    assert not ledger.reserve("one", assessment("CURRENT_DEFECT"))["allowed"]


def test_other_pr_and_completed_reservation_cannot_authorize(ledger, policy):
    assert ledger.reserve("one", assessment("CURRENT_DEFECT"))["allowed"]
    context = repair_context(ledger, "one")
    assert not policy.reservation_gate({**context, "pr": 496})["allowed"]
    ledger.complete("one", "head")
    assert not policy.reservation_gate(context)["allowed"]


def test_clean_rebuild_records_real_repair_push_in_same_ledger(ledger, monkeypatch):
    assert ledger.reserve("clean", assessment("CURRENT_DEFECT"))["allowed"]
    args = rebuild.RebuildArgs("owner/repo", "495", "branch", ["scripts/a.py"], [],
                               "unused.json", repair_context(ledger, "clean"), False)
    monkeypatch.setattr(rebuild, "run", lambda command, **_kwargs:
                        (0, "new-head" if command[:2] == ["git", "rev-parse"] else ""))
    decision = rebuild.initial_decision(args)
    rebuild.commit_and_push(args, decision, ["scripts/a.py"])
    assert decision["final_status"] == "success"
    assert ledger.status()["completed_count"] == 1


def test_verified_mutation_does_not_reenter_legacy_blocking_classification():
    thread = {"id": "T1", "body": "Missing security coverage for another grammar", "author": {"login": "codex"}}
    context = {"current_head_sha": "head", "review_assessments": {"T1": assessment("THEORETICAL_MUTATION")}}
    assert not controller.classify_review_thread(thread, context)["blocking"]
    assert not controller.review_comment_requires_patch(thread, context)


def test_controller_reports_durable_count_on_new_head_and_resolved_review(ledger, monkeypatch, tmp_path):
    for number in range(5):
        consume(ledger, str(number))
    monkeypatch.setenv("PR_FIX_LOOP_LEDGER", str(ledger.path))
    args = argparse.Namespace(repo="owner/repo", pr="495", output=str(tmp_path / "report.json"))
    for head in ("first", "new", "green-ci", "reviewer-changed", "threads-resolved"):
        decision = {"review": {"unresolved_active": 0}}
        context = controller.NextActionContext(args, {}, [], [], [], [], decision)
        controller.update_decision_state_tracking(args, {"headRefOid": head}, context)
        assert decision["pr_automation_state"]["autofix_commit_count"] == 5
        assert decision["fix_loop_budget"]["completed_count"] == 5
        # No sixth repair requested: exhausted repair budget does not fabricate
        # a failed final readiness for a genuinely clean PR.
        assert not decision["budget_status"]["exhausted"]


def test_normative_docs_keep_policy_classes_and_global_cap_five():
    root = Path(__file__).resolve().parents[2]
    required = (
        "MAX_FIX_LOOP_ITERATIONS_PER_PR = 5", "CURRENT_DEFECT", "GUARDRAIL_GAP",
        "THEORETICAL_MUTATION", "KNOWN_LIMITATION_ACCEPTED_BY_OWNER", "REVIEW_CHURN",
    )
    for name in ("AGENTS.md", "CLAUDE.md", "docs/auto_pr_flow_spec.md"):
        text = (root / name).read_text(encoding="utf-8")
        for requirement in required:
            assert requirement in text
        assert "Maximum automatic attempts per PR/check state: 3" not in text


@pytest.mark.parametrize("kind", ["CURRENT_DEFECT", "THEORETICAL_MUTATION"])
@pytest.mark.parametrize("reviewer", ["Sol", "Grok", "Codex", "unknown"])
def test_reviewer_identity_does_not_choose_verified_finding_class(kind, reviewer):
    thread = {"id": "T1", "body": "reported finding", "author": reviewer}
    context = {"current_head_sha": "head", "review_assessments": {"T1": assessment(kind)}}
    result = controller.triage_review_thread_contract(thread, context)
    expected = "PATCH_REQUIRED" if kind == "CURRENT_DEFECT" else "NEEDS_MANUAL"
    assert result["decision"] == expected


def test_quoted_owner_example_is_not_an_authorization(ledger, policy):
    decision = owner_acceptance()
    decision["body"] = "Example only:\n" + decision["body"]
    assert not policy.accept_limitation(assessment("THEORETICAL_MUTATION"), decision, "owner/repo", 495)["allowed"]


def test_actual_github_pr_comment_url_can_authorize_known_limit(policy):
    comment = owner_acceptance()
    comment["html_url"] = "https://github.com/owner/repo/pull/495#issuecomment-99"
    assert policy.accept_limitation(assessment("THEORETICAL_MUTATION"), comment, "owner/repo", 495)["allowed"]


def test_cli_budget_status_does_not_infer_zero_from_absent_state(policy, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["policy", "status", "--ledger", str(tmp_path / "missing.sqlite"),
                                    "--repo", "owner/repo", "--pr", "495"])
    assert policy.main() == 1
    assert '"AUTO_PR_FLOW_STATUS": "NEEDS_MANUAL"' in capsys.readouterr().out


def test_response_lost_cannot_reuse_reservation_for_another_push(ledger):
    assert ledger.reserve("lost", assessment("CURRENT_DEFECT"))["allowed"]
    context = repair_context(ledger, "lost")
    calls = []
    def lost(command, **_kwargs):
        calls.append(command)
        raise RuntimeError("connection lost after sending push")
    assert not flow.push_with_retry_once(lost, "owner/repo", "branch", context=context)["ok"]
    assert not ledger.authorize("lost")["allowed"]
    previous = len(calls)
    assert not flow.push_with_retry_once(lost, "owner/repo", "branch", context=context)["ok"]
    assert len(calls) == previous
