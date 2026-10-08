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
        "evidence": "local contract inspection and reproducible test", "pr_branch": "branch",
        "current_head_correct": kind != "CURRENT_DEFECT",
        "material": False, "contract_critical": False,
        **{risk: False for risk in controller.fix_policy.PROTECTED_RISKS},
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
    assert begin_push(ledger, name)["allowed"]
    assert ledger.complete(name, f"head-{name}", branch="branch")["completed_count"] >= 1


def test_cycles_one_through_five_then_six_stops(ledger, policy):
    assert policy.MAX_FIX_LOOP_ITERATIONS_PER_PR == 5
    for number in range(1, 6):
        consume(ledger, str(number))
    assert ledger.status()["completed_count"] == 5
    stopped = ledger.reserve("6", assessment("CURRENT_DEFECT"))
    assert not stopped["allowed"]
    assert stopped["AUTO_PR_FLOW_STATUS"] == "NEEDS_MANUAL"
    assert stopped["REASON"] == "fix_loop_budget_exhausted"
    assert not ledger.authorize("6", current_branch="branch")["allowed"]


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
    assert ledger.authorize("one", "head", current_branch="branch")["allowed"]
    begin_push(ledger, "one")
    ledger.complete("one", "h1", branch="branch")
    ledger.complete("one", "h1", branch="branch")
    assert ledger.status()["completed_count"] == 1
    assert not ledger.authorize("one", "head", current_branch="branch")["allowed"]


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


def repair_context(ledger, cycle, *, claim=True, branch="branch"):
    context = {
        "repo": "owner/repo", "pr": 495, "pr_branch": branch, "current_head_sha": "head", "POST_FIX_AUDIT": "PASS",
        "validation_passed": True, "current_head_matches": True,
        "dirty_worktree": False, "scope_allowed": True,
        "rollback_attempted": False, "rollback_succeeded": True,
        "task_no_commit_push": False, "automation_mode": "live",
        "can_commit": True, "can_push": True,
        "automation_flags": {"SAFE_AUTOFIX_ENABLED": True},
        "fix_loop": {"path": str(ledger.path), "repo": "owner/repo",
                     "pr": 495, "cycle_id": cycle},
    }
    tokens = getattr(ledger, '_test_tokens', {})
    if claim and cycle in tokens:
        context['fix_loop']['claim_token'] = tokens[cycle]
    elif claim and ledger.authorize(cycle, "head", current_branch=branch)['allowed']:
        result = controller.fix_policy.claim_patch_gate(context)
        if result['allowed']:
            tokens[cycle] = context['fix_loop']['claim_token']
            ledger._test_tokens = tokens
    return context


def begin_push(ledger, cycle):
    return controller.fix_policy.start_push_gate(repair_context(ledger, cycle))


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
    begin_push(ledger, "one")
    ledger.complete("one", "head", branch="branch")
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
        if command[1] == "push":
            raise RuntimeError("connection lost after sending push")
        return "pinned-head"
    assert not flow.push_with_retry_once(lost, "owner/repo", "branch", context=context)["ok"]
    assert not ledger.authorize("lost", current_branch="branch")["allowed"]
    previous = len(calls)
    assert not flow.push_with_retry_once(lost, "owner/repo", "branch", context=context)["ok"]
    assert len(calls) == previous


def test_clean_commit_cannot_follow_a_competing_fifth_completion(ledger, monkeypatch):
    for number in range(4):
        consume(ledger, str(number))
    assert ledger.reserve("fifth", assessment("CURRENT_DEFECT"))["allowed"]
    args = rebuild.RebuildArgs("owner/repo", "495", "branch", ["scripts/a.py"], [],
                               "unused.json", repair_context(ledger, "fifth"), False)
    def competing_attempt():
        if begin_push(ledger, "fifth")["allowed"]:
            ledger.complete("fifth", "competing-head", branch="branch")
    monkeypatch.setattr(rebuild, "ensure_git_identity", competing_attempt)
    commits = []
    def runner(command, **_kwargs):
        if command[:2] == ["git", "commit"]:
            commits.append(ledger.status()["completed_count"])
        return 0, "new-head" if command[:2] == ["git", "rev-parse"] else ""
    monkeypatch.setattr(rebuild, "run", runner)
    decision = rebuild.initial_decision(args)
    rebuild.commit_and_push(args, decision, ["scripts/a.py"])
    assert all(count < 5 for count in commits), "commit after budget exhaustion"


def test_clean_context_copies_preserve_the_same_reservation(ledger):
    assert ledger.reserve("clean", assessment("CURRENT_DEFECT"))["allowed"]
    context = repair_context(ledger, "clean")
    args = rebuild.RebuildArgs("owner/repo", "495", "branch", [], [], "unused.json", context, False)
    first = rebuild.build_clean_gate_ctx(args)
    assert rebuild.controller.fix_policy.start_push_gate(first)["allowed"]
    second = rebuild.build_clean_gate_ctx(args)
    assert first["fix_loop"] == second["fix_loop"] == context["fix_loop"]
    assert ledger.complete(second["fix_loop"]["cycle_id"], "new-head", branch="branch")["completed_count"] == 1


@pytest.mark.parametrize('state', ['unset', 'missing', 'corrupt'])
def test_controller_unavailable_ledger_stops_manual(monkeypatch, tmp_path, state):
    monkeypatch.delenv('PR_FIX_LOOP_LEDGER', raising=False)
    if state != 'unset':
        path = tmp_path / 'unavailable.sqlite'
        if state == 'corrupt':
            path.write_text('invalid database')
        monkeypatch.setenv('PR_FIX_LOOP_LEDGER', str(path))
    args = argparse.Namespace(repo='owner/repo', pr='495', output=str(tmp_path / 'report.json'))
    decision = {'review': {'unresolved_active': 0}}
    ctx = controller.NextActionContext(args, {}, [], [], [], [], decision)
    controller.update_decision_state_tracking(args, {'headRefOid': 'head'}, ctx)
    assert decision['budget_status']['exhausted']
    assert decision['budget_status']['next_action'].startswith('needs_manual')


def test_clean_preflight_at_five_does_not_request_sixth(ledger, monkeypatch, tmp_path):
    for number in range(5):
        consume(ledger, str(number))
    monkeypatch.setenv('PR_FIX_LOOP_LEDGER', str(ledger.path))
    monkeypatch.setattr(flow, 'pr_view', lambda *_: {'headRefName':'branch', 'headRefOid':'head'})
    monkeypatch.setattr(flow, 'split_checks', lambda *_a, **_k: {})
    monkeypatch.setattr(flow, 'sh', lambda *_a, **_k: '')
    monkeypatch.setattr(flow, 'safe_autofix_commits', lambda *_: [])
    args = argparse.Namespace(repo='owner/repo', pr='495', output='', max_safe_autofix_commits=5,
                              oscillation_touch_limit=3, comment=False, no_fail=False)
    assert flow.cmd_preflight(args) == 0


def test_verified_assessment_cannot_bypass_file_scope(ledger):
    ledger.reserve('scope', assessment('CURRENT_DEFECT'))
    context = {**repair_context(ledger, 'scope'), 'current_head_sha':'head',
               'files_allowed':['scripts/pr_fix_loop_policy.py'],
               'review_assessments':{'T1': assessment('CURRENT_DEFECT')}}
    thread = {'id':'T1', 'path':'.github/workflows/suspended.yml', 'body':'current bug'}
    assert controller.triage_review_thread_contract(thread, context)['decision'] == 'NEEDS_MANUAL'


def test_reservation_rejects_changed_assessed_head(ledger, policy):
    ledger.reserve('head-bound', assessment('CURRENT_DEFECT'))
    context = {**repair_context(ledger, 'head-bound'), 'current_head_sha':'collaborator-head'}
    assert not policy.reservation_gate(context)['allowed']


def test_completion_records_pushed_branch_not_checkout_head(ledger):
    ledger.reserve('branch', assessment('CURRENT_DEFECT', pr_branch='other-branch'))
    refs = []
    def runner(command, **_kwargs):
        if command[:2] == ['git', 'rev-parse']:
            refs.append(command[-1])
            return 'pushed-head' if command[-1] == 'refs/heads/other-branch' else 'unrelated-head'
        return ''
    assert flow.push_with_retry_once(runner, 'owner/repo', 'other-branch',
                                    context=repair_context(ledger, 'branch',branch='other-branch'))['ok']
    assert refs == ['refs/heads/other-branch']


def test_rejected_retry_without_refresh_cannot_replay_reservation(ledger):
    ledger.reserve('rejection', assessment('CURRENT_DEFECT'))
    pushes = []
    def runner(command, **_kwargs):
        if command[:2] == ['git', 'push']:
            pushes.append(command)
            raise RuntimeError('non-fast-forward')
        return 'pinned-head'
    context = repair_context(ledger, 'rejection')
    assert not flow.push_with_retry_once(runner, 'owner/repo', 'branch', context=context)['ok']
    assert not flow.push_with_retry_once(runner, 'owner/repo', 'branch', context=context)['ok']
    assert len(pushes) == 1


def test_retry_audit_never_exposes_reservation_to_competitor(ledger, monkeypatch, policy):
    ledger.reserve('concurrent-retry', assessment('CURRENT_DEFECT'))
    context = repair_context(ledger, 'concurrent-retry')
    context['retry_push_gate_context'] = {'explicitly_refreshed':True, 'gate_context':dict(context)}
    original = flow.ensure_post_fix_audit_gate_before_push
    checks = []
    competing = []
    def audit(ctx, **kwargs):
        checks.append(ctx)
        if len(checks) > 1:
            competing.append(policy.start_push_gate(context)['allowed'])
            return {'allowed':False, 'reason':'refreshed_audit_denied'}
        return original(ctx, **kwargs)
    monkeypatch.setattr(flow, 'ensure_post_fix_audit_gate_before_push', audit)
    def runner(command, **_kwargs):
        if command[:2] == ['git','push']:
            raise RuntimeError('non-fast-forward')
        return 'pinned-head'
    assert not flow.push_with_retry_once(runner, 'owner/repo', 'branch', context=context)['ok']
    assert competing == [False]


def test_push_and_completion_use_one_immutable_sha(ledger):
    ledger.reserve('pinned', assessment('CURRENT_DEFECT'))
    sent = []
    def runner(command, **_kwargs):
        if command[:2] == ['git','push']:
            sent.append(command)
        if command[:2] == ['git','rev-parse']:
            return 'already-advanced-head' if sent else 'pinned-head'
        return ''
    result = flow.push_with_retry_once(runner, 'owner/repo', 'branch', context=repair_context(ledger,'pinned'))
    assert result['ok']
    assert 'pinned-head:refs/heads/branch' in sent[0]
    assert ledger.complete('pinned','pinned-head', branch="branch")['allowed']


@pytest.mark.parametrize('context', [None, {}, {'fix_loop':None}])
def test_retry_missing_identity_returns_manual(context):
    retry = flow.NonFastForwardRetryContext('owner/repo','branch',RuntimeError('non-fast-forward'))
    result = flow._retry_non_fast_forward_push(lambda *_a, **_k:'', retry, 'origin', context)
    assert not result['ok'] and result['needs_manual']


@pytest.mark.parametrize('missing', [*controller.fix_policy.PROTECTED_RISKS, 'material'])
def test_accepted_limitation_unknown_risk_is_not_clearance(policy, missing):
    current = assessment('THEORETICAL_MUTATION', material=False,
                         **{risk:False for risk in policy.PROTECTED_RISKS})
    current.pop(missing)
    assert not policy.accept_limitation(current, owner_acceptance(), 'owner/repo',495)['allowed']


def test_controller_five_with_nonpatch_thread_is_not_sixth(ledger, monkeypatch, tmp_path):
    for number in range(5): consume(ledger, str(number))
    monkeypatch.setenv('PR_FIX_LOOP_LEDGER',str(ledger.path))
    args=argparse.Namespace(repo='owner/repo',pr='495',output=str(tmp_path/'report.json'))
    decision={'review':{'unresolved_active':1},'review_triage':{'decision':'EVIDENCE_RESOLVE'}}
    ctx=controller.NextActionContext(args,{},[],[],[],[],decision)
    controller.update_decision_state_tracking(args,{'headRefOid':'head'},ctx)
    assert not decision['budget_status']['exhausted']


def test_complete_requires_actual_push_intent(ledger):
    ledger.reserve('not-pushed', assessment('CURRENT_DEFECT'))
    assert not ledger.complete('not-pushed','invented-head', branch="branch")['allowed']
    assert ledger.status()['reserved_count'] == 1
    assert ledger.status()['completed_count'] == 0


@pytest.mark.parametrize('branch', ['feature', 'main'])
def test_clean_fetch_uses_branch_ref_despite_older_same_name_tag(tmp_path, monkeypatch, branch):
    import subprocess
    remote, seed, worker = (tmp_path / name for name in ('remote.git', 'seed', 'worker'))

    def git(at, *args):
        result = subprocess.run(['git', '-C', str(at), *args], text=True, capture_output=True)
        if result.returncode:
            raise RuntimeError(result.stderr)
        return result.stdout.strip()

    subprocess.run(['git', 'init', '--bare', str(remote)], check=True, capture_output=True)
    subprocess.run(['git', 'clone', str(remote), str(seed)], check=True, capture_output=True)
    git(seed, 'config', 'user.name', 'Test')
    git(seed, 'config', 'user.email', 'test@example.invalid')
    (seed / 'base').write_text('old assessed head')
    git(seed, 'add', 'base')
    git(seed, 'commit', '-m', 'base')
    git(seed, 'branch', '-M', 'main')
    if branch != 'main':
        git(seed, 'checkout', '-b', branch)
    git(seed, 'tag', branch)
    refs = list(dict.fromkeys(['refs/heads/main', f'refs/heads/{branch}', f'refs/tags/{branch}']))
    git(seed, 'push', 'origin', *refs)
    subprocess.run(['git', 'clone', '--branch', branch, str(remote), str(worker)], check=True, capture_output=True)
    (seed / 'advance').write_text('new unassessed head')
    git(seed, 'add', 'advance')
    git(seed, 'commit', '-m', 'advance branch but leave tag old')
    advanced = git(seed, 'rev-parse', 'HEAD')
    git(seed, 'push', 'origin', f'refs/heads/{branch}')
    monkeypatch.setattr(rebuild, 'run', lambda cmd, check=True: (0, git(worker, *cmd[1:])))
    args = rebuild.RebuildArgs('owner/repo', '495', branch, [], [], '', None, False)
    assert rebuild.fetch_heads(args, {}).old_head == advanced


def test_clean_rebuild_stops_before_restore_after_competing_completion(ledger, monkeypatch, tmp_path):
    rebuild=importlib.import_module('scripts.pr_clean_scope_rebuild')
    for number in range(4): consume(ledger,str(number))
    ledger.reserve('restore',assessment('CURRENT_DEFECT'))
    context=repair_context(ledger,'restore',claim=False)
    args=rebuild.RebuildArgs('owner/repo','495','branch',['scripts/a.py'],[],str(tmp_path/'report.json'),context,False)
    decision={}
    touched=[]
    monkeypatch.setattr(rebuild,'prepare_scope',lambda *_:(['scripts/a.py'],[]))
    monkeypatch.setattr(rebuild,'stop_for_scope_blockers',lambda *_:False)
    monkeypatch.setattr(rebuild,'fetch_heads',lambda *_:rebuild.CleanBranches('head','backup','clean'))
    original=rebuild.ensure_clean_push_gate
    def initial_gate(*params):
        allowed=original(*params)
        if begin_push(ledger, 'restore')['allowed']: ledger.complete('restore','competing-head', branch="branch")
        return allowed
    monkeypatch.setattr(rebuild,'ensure_clean_push_gate',initial_gate)
    monkeypatch.setattr(rebuild,'create_backup_and_clean_branch',lambda *_:touched.append('branch'))
    monkeypatch.setattr(rebuild,'restore_files',lambda *_:touched.append('restore'))
    for name in ['run_diff_guard','run_compile_guard','run_focused_tests']:
        monkeypatch.setattr(rebuild,name,lambda *_:None)
    monkeypatch.setattr(rebuild,'stop_if_forbidden_remains',lambda *_:False)
    monkeypatch.setattr(rebuild,'write_decision',lambda *_:0)
    rebuild.execute_rebuild(args,decision,tmp_path/'report.json')
    assert touched == []


def test_exhaustion_template_has_machine_readable_stop():
    agents=(Path(__file__).resolve().parents[2]/'AGENTS.md').read_text()
    tail=agents.split('If another PATCH_REQUIRED cycle would exceed the allowed cumulative budget:')[-1]
    assert 'AUTO_PR_FLOW_STATUS=NEEDS_MANUAL' in tail
    assert 'REASON=fix_loop_budget_exhausted' in tail
    assert 'PARTIAL' not in tail


def test_patch_claim_is_exclusive_and_required_by_other_worker(ledger, policy):
    ledger.reserve('exclusive', assessment('CURRENT_DEFECT'))
    first=repair_context(ledger,'exclusive',claim=False)
    other=repair_context(ledger,'exclusive',claim=False)
    assert policy.claim_patch_gate(first)['allowed']
    assert not policy.claim_patch_gate(other)['allowed']
    assert not policy.start_push_gate(other)['allowed']
    assert policy.reservation_gate(first)['allowed']
    assert policy.start_push_gate(first)['allowed']
    assert ledger.complete('exclusive','new-head', branch="branch")['allowed']


def test_real_git_same_named_tag_cannot_replace_audited_branch(ledger,tmp_path):
    import subprocess
    repo=tmp_path/'git-repo'
    repo.mkdir()
    def git(*args):
        return subprocess.check_output(['git',*args],cwd=repo,text=True,stderr=subprocess.DEVNULL).strip()
    git('init','-b','main')
    git('config','user.name','Policy Test')
    git('config','user.email','policy@example.test')
    git('commit','--allow-empty','-m','first')
    tag_head=git('rev-parse','HEAD')
    git('commit','--allow-empty','-m','branch')
    branch_head=git('rev-parse','HEAD')
    git('branch','collision')
    git('tag','collision',tag_head)
    assert tag_head != branch_head
    ledger.reserve('tag-collision',assessment('CURRENT_DEFECT',pr_branch='collision',current_head_sha=tag_head))
    sent=[]
    def runner(command,**_kwargs):
        if command[1]=='push':
            sent.append(command)
            return ''
        return git(*command[1:])
    context=repair_context(ledger,'tag-collision',branch='collision',claim=False)
    context['current_head_sha']=tag_head
    assert controller.fix_policy.claim_patch_gate(context)['allowed']
    result=flow.push_with_retry_once(runner,'owner/repo','collision',context=context)
    assert result['ok']
    assert f'{branch_head}:refs/heads/collision' in sent[0]


def test_claim_twice_keeps_original_worker_capability(ledger,policy):
    ledger.reserve('idempotent-claim',assessment('CURRENT_DEFECT'))
    context=repair_context(ledger,'idempotent-claim')
    assert policy.claim_patch_gate(context)['allowed']
    token=context['fix_loop']['claim_token']
    assert policy.claim_patch_gate(context)['allowed']
    assert context['fix_loop']['claim_token'] == token


def test_simultaneous_claims_have_exactly_one_winner(ledger,policy):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    ledger.reserve('concurrent-claim',assessment('CURRENT_DEFECT'))
    barrier=Barrier(2)
    def claim():
        barrier.wait(timeout=5)
        return policy.FixLoopLedger(ledger.path,'owner/repo',495).claim_patch('concurrent-claim','head', branch="branch")
    with ThreadPoolExecutor(max_workers=2) as pool:
        results=list(pool.map(lambda _:claim(),range(2)))
    assert sum(r['allowed'] for r in results) == 1


def test_phase_zero_handoff_preserves_claim_in_worker_context(ledger,monkeypatch,policy):
    ledger.reserve('handoff',assessment('CURRENT_DEFECT'))
    context=repair_context(ledger,'handoff',claim=False)
    monkeypatch.setattr(controller,'decide_phase0_gate',lambda _: {'can_patch':True})
    assert controller.build_codex_patch_task_after_phase0('patch',{},context)['can_patch']
    assert context['fix_loop']['claim_token']
    assert policy.reservation_gate(context)['allowed']
    assert policy.start_push_gate(context)['allowed']


@pytest.mark.parametrize('scope', [{}, {'files_allowed':[]}, {'files_forbidden':['core/']}])
def test_verified_forbidden_path_without_allowlist_is_manual(scope):
    path='core/engine.py' if scope.get('files_forbidden') else '.github/workflows/policy.yml'
    context={'current_head_sha':'head','review_assessments':{'T1':assessment('CURRENT_DEFECT')},**scope}
    assert controller.triage_review_thread_contract({'id':'T1','path':path},context)['decision']=='NEEDS_MANUAL'


def test_ambiguous_post_intent_failure_reports_manual(ledger):
    ledger.reserve('ambiguous-report',assessment('CURRENT_DEFECT'))
    def runner(command,**_kwargs):
        if command[1]=='push': raise RuntimeError('response lost after push')
        return 'pinned-head'
    result=flow.push_with_retry_once(runner,'owner/repo','branch',context=repair_context(ledger,'ambiguous-report'))
    assert result['needs_manual'] and result['status']=='needs_manual'


def test_clean_rebuild_rejects_fetched_unassessed_head(ledger,monkeypatch,tmp_path):
    rebuild=importlib.import_module('scripts.pr_clean_scope_rebuild')
    ledger.reserve('fetched-head',assessment('CURRENT_DEFECT'))
    context=repair_context(ledger,'fetched-head')
    args=rebuild.RebuildArgs('owner/repo','495','branch',['scripts/a.py'],[],str(tmp_path/'report.json'),context,False)
    touched=[]
    monkeypatch.setattr(rebuild,'prepare_scope',lambda *_:(['scripts/a.py'],[]))
    monkeypatch.setattr(rebuild,'stop_for_scope_blockers',lambda *_:False)
    monkeypatch.setattr(rebuild,'fetch_heads',lambda *_:rebuild.CleanBranches('collaborator-head','backup','clean'))
    monkeypatch.setattr(rebuild,'create_backup_and_clean_branch',lambda *_:touched.append('branch'))
    monkeypatch.setattr(rebuild,'restore_files',lambda *_:touched.append('restore'))
    for name in ['run_diff_guard','run_compile_guard','run_focused_tests']:
        monkeypatch.setattr(rebuild,name,lambda *_:None)
    monkeypatch.setattr(rebuild,'stop_if_forbidden_remains',lambda *_:False)
    monkeypatch.setattr(rebuild,'commit_and_push',lambda *_:None)
    monkeypatch.setattr(rebuild,'write_decision',lambda *_:0)
    rebuild.execute_rebuild(args,{},tmp_path/'report.json')
    assert not touched


def test_direct_push_cannot_skip_exclusive_claim(ledger):
    ledger.reserve('unclaimed',assessment('CURRENT_DEFECT'))
    calls=[]
    result=flow.push_with_retry_once(lambda cmd,**_:(calls.append(cmd) or 'head'),'owner/repo','branch',
                                    context=repair_context(ledger,'unclaimed',claim=False))
    assert not result['ok'] and result['needs_manual']
    assert not calls


@pytest.mark.parametrize('value',[-100,'not-a-counter',1.5])
def test_invalid_persisted_history_fails_closed(ledger,value):
    import sqlite3
    with sqlite3.connect(ledger.path) as connection:
        connection.execute('UPDATE prs SET historical_count=?',(value,))
    assert not ledger.status()['allowed']
    assert not ledger.reserve('corrupt',assessment('CURRENT_DEFECT'))['allowed']


@pytest.mark.parametrize('stage',['reserved','working','pushing','retry_ready'])
def test_unfinished_cycle_blocks_general_preflight(ledger,monkeypatch,stage):
    import sqlite3
    ledger.reserve('unfinished',assessment('CURRENT_DEFECT'))
    context=repair_context(ledger,'unfinished',claim=False)
    if stage != 'reserved': controller.fix_policy.claim_patch_gate(context)
    if stage in {'pushing','retry_ready'}: controller.fix_policy.start_push_gate(context)
    if stage == 'retry_ready': controller.fix_policy.retry_after_confirmed_rejection(context)
    monkeypatch.setenv('PR_FIX_LOOP_LEDGER',str(ledger.path))
    monkeypatch.setattr(flow,'pr_view',lambda *_:{'headRefName':'branch','headRefOid':'head'})
    monkeypatch.setattr(flow,'split_checks',lambda *_a,**_k:{})
    monkeypatch.setattr(flow,'sh',lambda *_a,**_k:'')
    monkeypatch.setattr(flow,'safe_autofix_commits',lambda *_:[])
    args=argparse.Namespace(repo='owner/repo',pr='495',output='',max_safe_autofix_commits=5,
                            oscillation_touch_limit=3,comment=False,no_fail=False)
    assert flow.cmd_preflight(args)==1


def test_clean_push_lease_is_bound_to_assessed_head(ledger,monkeypatch):
    rebuild=importlib.import_module('scripts.pr_clean_scope_rebuild')
    ledger.reserve('lease',assessment('CURRENT_DEFECT'))
    args=rebuild.RebuildArgs('owner/repo','495','branch',['scripts/a.py'],[],'unused.json',repair_context(ledger,'lease'),False)
    commands=[]
    def runner(command):
        commands.append(command)
        return 0,'new-head' if command[1]=='rev-parse' else ''
    monkeypatch.setattr(rebuild,'run',runner)
    monkeypatch.setattr(rebuild,'ensure_git_identity',lambda:None)
    rebuild.commit_and_push(args,{},['scripts/a.py'])
    pushed=next(command for command in commands if command[1]=='push')
    assert '--force-with-lease=refs/heads/branch:head' in pushed


def test_clean_restore_uses_fetched_immutable_source(ledger,monkeypatch,tmp_path):
    rebuild=importlib.import_module('scripts.pr_clean_scope_rebuild')
    ledger.reserve('restore-source',assessment('CURRENT_DEFECT'))
    args=rebuild.RebuildArgs('owner/repo','495','branch',['scripts/a.py'],[],str(tmp_path/'report.json'),repair_context(ledger,'restore-source'),False)
    sources=[]
    monkeypatch.setattr(rebuild,'prepare_scope',lambda *_:(['scripts/a.py'],[]))
    monkeypatch.setattr(rebuild,'stop_for_scope_blockers',lambda *_:False)
    monkeypatch.setattr(rebuild,'fetch_heads',lambda *_:rebuild.CleanBranches('head','backup','clean'))
    monkeypatch.setattr(rebuild,'create_backup_and_clean_branch',lambda *_:None)
    monkeypatch.setattr(rebuild,'restore_files',lambda _args,_files,source=None:sources.append(source))
    for name in ['run_diff_guard','run_compile_guard','run_focused_tests','commit_and_push']:
        monkeypatch.setattr(rebuild,name,lambda *_:None)
    monkeypatch.setattr(rebuild,'stop_if_forbidden_remains',lambda *_:False)
    monkeypatch.setattr(rebuild,'write_decision',lambda *_:0)
    rebuild.execute_rebuild(args,{},tmp_path/'report.json')
    assert sources==['head']


def test_general_controller_stops_on_unfinished_cycle(ledger,monkeypatch,tmp_path):
    ledger.reserve('active-worker',assessment('CURRENT_DEFECT'))
    controller.fix_policy.claim_patch_gate(repair_context(ledger,'active-worker'))
    monkeypatch.setenv('PR_FIX_LOOP_LEDGER',str(ledger.path))
    args=argparse.Namespace(repo='owner/repo',pr='495',output=str(tmp_path/'report.json'))
    decision={'review':{'unresolved_active':0}}
    ctx=controller.NextActionContext(args,{},[],[],[],[],decision)
    controller.update_decision_state_tracking(args,{'headRefOid':'head'},ctx)
    assert decision['budget_status']['exhausted']
    assert decision['budget_status']['next_action'].startswith('needs_manual')


@pytest.mark.parametrize('mutation',[
    "UPDATE cycles SET assessment=NULL",
    "UPDATE cycles SET assessment='[]'",
    "UPDATE cycles SET stage='completed'",
    "UPDATE cycles SET head='invented-head'",
    "INSERT INTO grants VALUES ('owner/repo',495,'1','bad-ceiling','{}')",
])
def test_inconsistent_persisted_rows_fail_closed(ledger,mutation):
    import sqlite3
    ledger.reserve('invalid-row',assessment('CURRENT_DEFECT'))
    with sqlite3.connect(ledger.path) as connection: connection.execute(mutation)
    assert not ledger.status()['allowed']
    assert not ledger.reserve('next',assessment('CURRENT_DEFECT'))['allowed']


def test_retry_preserves_new_unassessed_remote_head(policy, tmp_path):
    """Real Git: fetching a collaborator commit must not authorize overwriting it."""
    import subprocess
    remote, seed, worker, other = (tmp_path / name for name in ('remote.git', 'seed', 'worker', 'other'))

    def git(at, *args):
        result = subprocess.run(['git', '-C', str(at), *args], text=True, capture_output=True)
        if result.returncode:
            raise RuntimeError(result.stderr)
        return result.stdout.strip()

    subprocess.run(['git', 'init', '--bare', str(remote)], check=True, capture_output=True)
    subprocess.run(['git', 'clone', str(remote), str(seed)], check=True, capture_output=True)
    git(seed, 'config', 'user.name', 'Test')
    git(seed, 'config', 'user.email', 'test@example.invalid')
    (seed / 'base').write_text('base')
    git(seed, 'add', 'base')
    git(seed, 'commit', '-m', 'base')
    git(seed, 'branch', '-M', 'feature')
    git(seed, 'push', 'origin', 'feature')
    assessed = git(seed, 'rev-parse', 'HEAD')
    for clone in (worker, other):
        subprocess.run(['git', 'clone', '--branch', 'feature', str(remote), str(clone)], check=True, capture_output=True)
        git(clone, 'config', 'user.name', 'Test')
        git(clone, 'config', 'user.email', 'test@example.invalid')
    (worker / 'patch').write_text('reviewed patch')
    git(worker, 'add', 'patch')
    git(worker, 'commit', '-m', 'reviewed patch')
    (other / 'collaborator').write_text('unassessed advance')
    git(other, 'add', 'collaborator')
    git(other, 'commit', '-m', 'new collaborator head')
    advanced = git(other, 'rev-parse', 'HEAD')
    git(other, 'push', 'origin', 'feature')
    ledger = policy.FixLoopLedger(tmp_path / 'lease-budget.sqlite', 'owner/repo', 495)
    ledger.initialize(0, 'new PR')
    assert ledger.reserve('retry', assessment('CURRENT_DEFECT', current_head_sha=assessed,pr_branch='feature'))['allowed']
    context = {
        'repo': 'owner/repo', 'pr': 495, 'pr_branch':'feature', 'current_head_sha': assessed,
        'fix_loop': {'path': str(ledger.path), 'repo': 'owner/repo', 'pr': 495, 'cycle_id': 'retry'},
        'automation_mode': 'live', 'post_fix_audit': 'PASS', 'validation_passed': True,
        'current_head_matches': True, 'dirty_worktree': False, 'scope_allowed': True,
        'rollback_attempted': False, 'task_no_commit_push': False,
        'rollback_succeeded': True, 'can_push': True, 'can_commit': True,
    }
    assert policy.claim_patch_gate(context)['allowed']
    refreshed = dict(context)
    context['retry_push_gate_context'] = {'explicitly_refreshed': True, 'gate_context': refreshed}
    result = flow.push_with_retry_once(lambda cmd, **kwargs: git(worker, *cmd[1:]),
                                       'owner/repo', 'feature', context=context)
    assert result['retried'], str(result)
    assert not result['ok']
    assert result['needs_manual']
    assert git(remote, 'rev-parse', 'refs/heads/feature') == advanced
    assert ledger.status()['completed_count'] == 0
    assert ledger.status()['reserved_count'] == 1


@pytest.mark.parametrize('state', [{'isResolved': True}, {'is_resolved': True}, {'isOutdated': True}, {'isActive': False}, {'active': False}])
def test_cached_defect_cannot_repatch_inactive_thread(state):
    thread = {'id': 'T1', 'body': 'runtime bug', **state}
    context = {'current_head_sha': 'head', 'review_assessments': {'T1': assessment('CURRENT_DEFECT')}}
    assert controller.triage_review_thread_contract(thread, context)['decision'] != 'PATCH_REQUIRED'
    assert not controller.classify_review_thread(thread, context)['blocking']


def test_push_destination_must_be_the_assessed_pr_branch(ledger):
    assert ledger.reserve('wrong-branch', assessment('CURRENT_DEFECT', pr_branch='pr-branch'))['allowed']
    context = repair_context(ledger, 'wrong-branch')
    calls = []
    def runner(cmd, **kwargs):
        calls.append(cmd)
        return 'unrelated-head' if cmd[1] == 'rev-parse' else ''
    result = flow.push_with_retry_once(runner, 'owner/repo', 'unrelated-branch', context=context)
    assert not result['ok']
    assert result['needs_manual']
    assert calls == []
    assert ledger.status()['completed_count'] == 0


@pytest.mark.parametrize('stage', ['reserved', 'completed'])
def test_invalid_claim_token_stage_is_corrupt(ledger, stage):
    import sqlite3, json
    ledger.reserve('invalid-claim', assessment('CURRENT_DEFECT'))
    if stage == 'completed':
        begin_push(ledger, 'invalid-claim')
        ledger.complete('invalid-claim', 'pushed', branch="branch")
    with sqlite3.connect(ledger.path) as connection:
        payload = json.loads(connection.execute('SELECT assessment FROM cycles').fetchone()[0])
        if stage == 'reserved': payload['claim_token'] = 'impossible-existing-token'
        else: payload.pop('claim_token')
        connection.execute('UPDATE cycles SET assessment=?', (json.dumps(payload),))
    assert not ledger.status()['allowed']


@pytest.mark.parametrize('budget', [
    {'fix_loop_budget': {'allowed': True, 'reserved_count': 1}},
    {'fix_loop_budget': {'allowed': False, 'reason': 'fix_loop_state_missing'}},
    {'budget_status': {'exhausted': True, 'reason': 'unfinished_fix_loop_cycle'}},
])
def test_auto_merge_cannot_bypass_durable_manual_stop(budget):
    context = {'automation_mode':'live', 'automation_flags':{'AUTO_MERGE_ENABLED':True,
        'GITHUB_MUTATION_ENABLED':True,'EXTERNAL_SIDE_EFFECT_ENABLED':True},
        'task_no_commit_push':False,'mergeable':'MERGEABLE','mergeStateStatus':'CLEAN',
        'bad':[],'pending':[],'unresolved_active':0,'current_head_matches':True,
        'explicit_merge_authorization':True, **budget}
    assert not controller.can_auto_merge(context)['allowed']


# Prove del contratto di autonomia (§0.9) per un merge automatico pulito: dal
# #499 `can_auto_merge` richiede anche `merge_decision == READY_FOR_AUTO_MERGE`.
AUTONOMY_READY_STATE = {
    "evaluated_head": "abc123", "current_head": "abc123",
    "scope_valid": True, "acceptance_complete": True, "suite_pass": True,
    "hard_verify_pass": True, "codex_triaged": True, "fix_loop_valid": True,
    "dependencies_satisfied": True, "pertinent_owner_decision_open": False,
    "manual_stop": False, "manual_label_present": False,
    "preexisting_activated_or_aggravated": False, "unresolved_threads": 0,
    "introduced_p0_p1_open": 0, "reviewer_gate": "PASS", "merge_readiness": "PASS",
    "full_diff": {"status": "PASS", "owner_manual_merge_required": False,
                  "classification": {"safety_critical": []}},
    "fix_loop_exhausted": False,
}


def merge_context(**extra):
    return {'automation_mode':'live','automation_flags':{'AUTO_MERGE_ENABLED':True,
        'GITHUB_MUTATION_ENABLED':True,'EXTERNAL_SIDE_EFFECT_ENABLED':True},
        'task_no_commit_push':False,'mergeable':'MERGEABLE','mergeStateStatus':'CLEAN',
        'bad':[],'pending':[],'unresolved_active':0,'current_head_matches':True,
        'explicit_merge_authorization':True,'autonomy_merge_state':AUTONOMY_READY_STATE,'headRefOid':'abc123', **extra}


@pytest.mark.parametrize('stage', ['reserved','working','pushing','retry_ready'])
def test_merge_rechecks_actual_ledger_despite_green_cached_snapshot(ledger, stage):
    ledger.reserve('held',assessment('CURRENT_DEFECT'))
    context=repair_context(ledger,'held',claim=stage!='reserved')
    if stage in {'pushing','retry_ready'}: assert controller.fix_policy.start_push_gate(context)['allowed']
    if stage=='retry_ready': assert controller.fix_policy.retry_after_confirmed_rejection(context)['allowed']
    descriptor={'path':str(ledger.path),'repo':'owner/repo','pr':495}
    result=controller.can_auto_merge(merge_context(fix_loop=descriptor,
        fix_loop_budget={'allowed':True,'reserved_count':0}))
    assert not result['allowed']
    assert 'unfinished_fix_loop_cycle' in result['reason']


def test_real_pr_merge_without_ledger_is_not_authorized(monkeypatch):
    monkeypatch.delenv('PR_FIX_LOOP_LEDGER',raising=False)
    assert not controller.can_auto_merge(merge_context(repo='owner/repo',pr=495,
        fix_loop_budget={'allowed':True,'reserved_count':0}))['allowed']


def test_clean_merge_at_five_keeps_existing_guards(ledger):
    for number in range(5): consume(ledger,str(number))
    context=merge_context(fix_loop={'path':str(ledger.path),'repo':'owner/repo','pr':495})
    assert controller.can_auto_merge(context)['allowed']
    assert not controller.can_auto_merge({**context,'pending':['CI']})['allowed']
    assert not controller.can_auto_merge({**context,'unresolved_active':1})['allowed']
    assert ledger.status()['completed_count']==5


def test_completion_rejects_wrong_branch_and_corrupt_assessment(ledger):
    import sqlite3
    ledger.reserve('completion-branch',assessment('CURRENT_DEFECT'))
    begin_push(ledger,'completion-branch')
    assert not ledger.complete('completion-branch','pushed',branch='other')['allowed']
    with sqlite3.connect(ledger.path) as connection:
        connection.execute("UPDATE cycles SET assessment='[]'")
    assert not ledger.complete('completion-branch','pushed',branch='branch')['allowed']


@pytest.mark.parametrize('snapshot', [{'allowed':False,'reason':'fix_loop_budget_exhausted'},
                                      {'allowed':True,'reserved_count':1}])
def test_fresh_clean_ledger_does_not_clear_explicit_manual_merge_stop(ledger,snapshot):
    descriptor={'path':str(ledger.path),'repo':'owner/repo','pr':495}
    assert not controller.can_auto_merge(merge_context(fix_loop=descriptor,fix_loop_budget=snapshot))['allowed']


@pytest.mark.parametrize('metadata', [{}, {'pr_branch':None},
    {'pr_branch':None,'branch':'other'}, {'pr_branch':'branch','branch':'other'},
    {'pr_branch':'branch','headRefName':'other'}])
def test_branch_metadata_is_mandatory_and_consistent(ledger,policy,metadata):
    ledger.reserve('branch-required',assessment('CURRENT_DEFECT'))
    ctx=repair_context(ledger,'branch-required',claim=False)
    for key in ('pr_branch','branch','headRefName'): ctx.pop(key,None)
    ctx.update(metadata)
    assert not policy.claim_patch_gate(ctx)['allowed']
    assert ledger.status()['completed_count']==0


def test_direct_authorization_and_completion_require_branch(ledger):
    ledger.reserve('branch-required',assessment('CURRENT_DEFECT'))
    assert not ledger.authorize('branch-required','head')['allowed']
    assert begin_push(ledger,'branch-required')['allowed']
    assert not ledger.complete('branch-required','pushed')['allowed']
    assert ledger.status()['reserved_count']==1


def test_auto_merge_without_pr_identity_is_denied(policy,monkeypatch):
    monkeypatch.delenv('PR_FIX_LOOP_LEDGER',raising=False)
    assert not controller.can_auto_merge(merge_context())['allowed']
    assert not policy.merge_budget_gate({'fix_loop_budget':{'allowed':True,'reserved_count':0}})['allowed']


@pytest.mark.parametrize('path',['core/engine.py','services/foo.py'])
def test_verified_review_respects_repository_default_scope(path):
    thread={'id':'T1','path':path,'body':'current bug'}
    ctx={'current_head_sha':'head','review_assessments':{'T1':assessment('CURRENT_DEFECT')}}
    assert controller.triage_review_thread_contract(thread,ctx)['decision']=='NEEDS_MANUAL'
    ctx['files_allowed']=[path]
    assert controller.triage_review_thread_contract(thread,ctx)['decision']=='PATCH_REQUIRED'
    ctx['files_forbidden']=[path]
    assert controller.triage_review_thread_contract(thread,ctx)['decision']=='NEEDS_MANUAL'


@pytest.mark.parametrize('critical',[True,None])
def test_critical_or_unknown_guardrail_cannot_be_accepted(critical,policy):
    a=assessment('GUARDRAIL_GAP')
    if critical is not None: a['contract_critical']=critical
    else: a.pop('contract_critical')
    c={'repo':'owner/repo','pr':495,'author':'owner','id':'99',
       'html_url':'https://github.com/owner/repo/pull/495#issuecomment-99',
       'body':'KNOWN_LIMITATION_ACCEPTED_BY_OWNER PR=495 THREAD=T1 HEAD=head'}
    assert not policy.accept_limitation(a,c,'owner/repo',495)['allowed']
    if critical is True: assert policy.triage(a)['reason']!='noncritical_guardrail_gap'


def test_direct_ledger_cannot_skip_current_head_evidence(ledger):
    ledger.reserve('direct-head',assessment('CURRENT_DEFECT'))
    assert not ledger.authorize('direct-head',current_branch='branch')['allowed']
    assert not ledger.authorize('direct-head','other',current_branch='branch')['allowed']


def test_direct_claim_and_push_cannot_skip_branch(ledger):
    ledger.reserve('direct-claim',assessment('CURRENT_DEFECT'))
    assert not ledger.claim_patch('direct-claim','head')['allowed']
    result=ledger.claim_patch('direct-claim','head',branch='branch')
    assert result['allowed']
    assert not ledger.start_push('direct-claim',result['claim_token'])['allowed']


@pytest.mark.parametrize('path',['betfair_client.py','order_manager.py','telegram_bot.py',
    'telegram_listener.py','telegram_bot_runtime.py','simulation_broker.py',
    'cashout_wiring.py','core/risk_gate.py','services/betfair_service.py',
    'future_package/exchange_adapter.py','config.json','unknown_package/source.rs'])
def test_all_product_python_requires_explicit_task_scope(path):
    t={'id':'T1','path':path,'body':'current bug'}
    ctx={'current_head_sha':'head','review_assessments':{'T1':assessment('CURRENT_DEFECT')}}
    assert controller.triage_review_thread_contract(t,ctx)['decision']=='NEEDS_MANUAL'
    ctx['files_allowed']=[path]
    assert controller.triage_review_thread_contract(t,ctx)['decision']=='PATCH_REQUIRED'
    ctx['files_forbidden']=[path]
    assert controller.triage_review_thread_contract(t,ctx)['decision']=='NEEDS_MANUAL'


def test_initial_push_refuses_incorporated_unassessed_remote_head(tmp_path,policy):
    import subprocess
    remote=tmp_path/'remote.git'; worker=tmp_path/'worker'; other=tmp_path/'other'
    def git(where,*args):
        result=subprocess.run(['git',*args],cwd=where,text=True,capture_output=True)
        if result.returncode: raise RuntimeError(result.stderr)
        return result.stdout.strip()
    git(tmp_path,'init','--bare',str(remote));git(tmp_path,'clone',str(remote),str(worker))
    git(worker,'config','user.name','test');git(worker,'config','user.email','test@example.test')
    git(worker,'checkout','-b','branch');git(worker,'commit','--allow-empty','-m','assessed')
    assessed=git(worker,'rev-parse','HEAD');git(worker,'push','origin','branch')
    ledger=policy.FixLoopLedger(tmp_path/'initial.sqlite','owner/repo',495)
    ledger.initialize(0,'new PR');ledger.reserve('initial',assessment('CURRENT_DEFECT',current_head_sha=assessed))
    ctx=repair_context(ledger,'initial',claim=False);ctx['current_head_sha']=assessed
    assert policy.claim_patch_gate(ctx)['allowed']
    git(tmp_path,'clone',str(remote),str(other));git(other,'checkout','branch')
    git(other,'config','user.name','test');git(other,'config','user.email','test@example.test')
    git(other,'commit','--allow-empty','-m','unassessed');advanced=git(other,'rev-parse','HEAD')
    git(other,'push','origin','branch');git(worker,'fetch','origin');git(worker,'merge','--ff-only','origin/branch')
    git(worker,'commit','--allow-empty','-m','repair')
    result=flow.push_with_retry_once(lambda cmd,**kwargs:git(worker,*cmd[1:]),'owner/repo','branch',context=ctx)
    assert not result['ok']
    assert git(remote,'rev-parse','refs/heads/branch')==advanced
    assert ledger.status()['completed_count']==0
    assert ledger.status()['reserved_count']==1
