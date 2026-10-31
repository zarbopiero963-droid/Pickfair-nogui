"""OWNER DECISION 31/10/2026 — GitHub Actions primario, Buildkite differito (#586).

Gli status della lane Buildkite sospesa (`full-suite`, `overlap-check`, `pr-merge-readiness` come StatusContext
su buildkite.com) non sono gate ordinari: ne' blocker ne' pending, ma restano fra gli ignorati. BLOCK: il CheckRun
`overlap-check` di GitHub Actions (required del ruleset) e ogni altro check continuano a bloccare; un'identita'
anche solo parzialmente diversa (tipo, nome, host) NON viene esclusa (fail-closed).
"""
from __future__ import annotations

import pytest

from scripts import pr_automation_controller as controller
from scripts import pr_flow_automation as flow

pytestmark = pytest.mark.unit

BK = "https://buildkite.com/zarbopiero963-droid/pf-dedicated-ci/builds/51"


def _status(context, state="FAILURE", url=BK):
    return {"__typename": "StatusContext", "context": context, "state": state, "targetUrl": url}


def _run(name, conclusion="FAILURE", url="https://github.com/o/r/actions/runs/1/job/2"):
    return {"__typename": "CheckRun", "name": name, "status": "COMPLETED", "conclusion": conclusion,
            "detailsUrl": url}


def _split(*checks):
    return flow.split_checks({"statusCheckRollup": list(checks)})


@pytest.mark.parametrize("context", ["full-suite", "overlap-check", "pr-merge-readiness"])
@pytest.mark.parametrize("state", ["FAILURE", "PENDING", "ERROR"])
def test_deferred_buildkite_statuses_neither_block_nor_wait(context, state):
    out = _split(_status(context, state))
    assert out["blockers"] == [] and out["pending"] == []
    assert [i["name"] for i in out["ignored"]] == [context]


def test_github_actions_overlap_check_still_blocks():
    out = _split(_run("overlap-check"), _status("overlap-check"))
    assert [b["name"] for b in out["blockers"]] == ["overlap-check"]
    assert out["blockers"][0]["rollup_typename"] == "CheckRun"


@pytest.mark.parametrize("check", [
    _status("full-suite", url="https://github.com/o/r/actions/runs/9"),          # non Buildkite
    _status("full-suite", url="https://buildkite.com.evil.example/x"),          # host contraffatto
    _status("full-suite", url=""),                                               # nessun target
    _status("tests"),                                                            # context non differito
    _run("full-suite", url=BK),                                                  # CheckRun, non status
    {"context": "full-suite", "state": "FAILURE", "targetUrl": BK},              # tipo sconosciuto
])
def test_anything_not_exactly_a_deferred_buildkite_status_still_blocks(check):
    assert len(_split(check)["blockers"]) == 1


def test_buildkite_subdomain_is_recognised():
    assert controller.is_deferred_buildkite_status(_status("full-suite", url="https://api.buildkite.com/v2/x"))


def test_single_source_and_controller_path_agree():
    assert flow.DEFERRED_BUILDKITE_STATUS_CONTEXTS is controller.DEFERRED_BUILDKITE_STATUS_CONTEXTS
    checks = [_status("full-suite"), _run("overlap-check"), _status("tests")]
    kept = controller.filter_ignored_codacy_checks(checks, {})
    assert [c.get("context") or c.get("name") for c in kept] == ["overlap-check", "tests"]
    for c in checks:
        assert flow.is_deferred_buildkite_status(c) == controller.is_deferred_buildkite_status(c)


def test_controller_pending_wait_ignores_deferred_buildkite_statuses():
    """Review Claude A su 2cdfe8a: anche l'attesa del controller (`wait_for_pending_checks`) non deve aspettare la
    lane sospesa; un pending reale (CheckRun di Actions) resta in attesa."""
    pr = {"statusCheckRollup": [_status("full-suite", "PENDING"), _status("pr-merge-readiness", "PENDING"),
                                _run("tests", conclusion=None) | {"status": "IN_PROGRESS"}]}
    names = [c.get("name") or c.get("context") for c in controller.pending_checks_from_pr(pr)]
    assert names == ["tests"]
