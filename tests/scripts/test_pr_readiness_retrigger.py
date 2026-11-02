"""PF-CI-PERFORMANCE-FINAL-01 — riattivazione event-driven della Merge readiness (casi 16, 17 e anti-loop)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import pr_readiness_retrigger as R  # noqa: E402

REPO = "zarbopiero963-droid/Pickfair-nogui"
SHA, OLD = "d" * 40, "c" * 40


def event(**over):
    cr = {"name": R.TRUSTED_CHECK, "app": {"slug": R.TRUSTED_APP}, "status": "completed", "conclusion": "success",
          "head_sha": SHA, "pull_requests": [{"number": 602}]}
    cr.update(over)
    return {"action": "completed", "check_run": cr}


def pr(head=SHA, state="open", repo=REPO):
    return {"state": state, "head": {"sha": head, "ref": "feat", "repo": {"full_name": repo}}}


def run_(rid=10, status="completed", conclusion="failure", attempt=1, sha=SHA, event_="pull_request"):
    return {"id": rid, "status": status, "conclusion": conclusion, "run_attempt": attempt, "head_sha": sha,
            "event": event_, "path": ".github/workflows/pr-merge-readiness.yml", "pull_requests": [{"number": 602}]}


@pytest.mark.parametrize("ev,reason", [
    (dict(event(), action="created"), "NOT_COMPLETED"),
    (event(name="pf-betfair-live-certification"), "NOT_THE_PRELIVE_CHECK"),
    (event(app={"slug": "evil-app"}), "UNTRUSTED_APP"),
    (event(status="in_progress"), "CHECK_NOT_COMPLETED"),
    (event(head_sha="xyz"), "MALFORMED_SHA"),
])
def test_only_trusted_prelive_completions_are_accepted(ev, reason):
    with pytest.raises(R.RetriggerError, match=reason):
        R.validate_event(ev)


def test_16_a_concluded_prelive_reruns_the_failed_readiness_of_that_head():
    plan = R.plan_reruns(REPO, SHA, {602: pr()}, [run_(rid=9, conclusion="failure"), run_(rid=10, attempt=1)])
    assert plan == [{"pr": 602, "action": "RERUN", "reason": "PRELIVE_COMPLETED", "run_id": 10}]


def test_17_an_old_result_is_never_attributed_to_a_new_head():
    plan = R.plan_reruns(REPO, OLD, {602: pr(head=SHA)}, [run_(sha=OLD)])
    assert plan[0]["action"] == "SKIP" and plan[0]["reason"] == "STALE_SHA"


@pytest.mark.parametrize("runs,reason", [
    ([run_(status="in_progress", conclusion=None)], "READINESS_RUNNING"),       # nessuna doppia pubblicazione
    ([run_(conclusion="success")], "ALREADY_READY"),
    ([run_(attempt=R.MAX_ATTEMPTS)], "ATTEMPTS_EXHAUSTED"),                    # anti-loop / anti-replay
    ([run_(event_="pull_request_target")], "NO_READINESS_RUN"),
    ([], "NO_READINESS_RUN"),
])
def test_no_rerun_when_it_would_duplicate_loop_or_has_no_target(runs, reason):
    assert R.plan_reruns(REPO, SHA, {602: pr()}, runs)[0]["reason"] == reason


@pytest.mark.parametrize("p,reason", [(pr(state="closed"), "PR_NOT_OPEN"), (pr(repo="fork/x"), "FORK_PR")])
def test_closed_or_fork_prs_are_skipped(p, reason):
    assert R.plan_reruns(REPO, SHA, {602: p}, [run_()])[0]["reason"] == reason


def test_replayed_event_does_not_rerun_twice():
    state = {"runs": [run_()]}
    posts = []

    def http(method, url):
        path = url.split("api.github.com", 1)[1]
        if method == "POST":
            posts.append(path)
            state["runs"] = [run_(status="queued", conclusion=None, attempt=2)]
            return 201, b"{}"
        if "/pulls/602" in path:
            return 200, json.dumps(pr()).encode()
        if "/actions/workflows/" in path:
            return 200, json.dumps({"workflow_runs": state["runs"]}).encode()
        return 404, b""
    gh = R.GitHub(REPO, "t", http=http)
    assert R.run(event(), gh)["actions"][0]["action"] == "RERUN"
    assert R.run(event(), gh)["actions"][0]["reason"] == "READINESS_RUNNING"
    assert posts == [f"/repos/{REPO}/actions/runs/10/rerun"]


@pytest.mark.parametrize("method,path", [("POST", f"/repos/{REPO}/pulls/602/merge"),
                                         ("PUT", f"/repos/{REPO}/actions/runs/10/rerun"),
                                         ("POST", f"/repos/{REPO}/actions/runs/10/cancel"),
                                         ("GET", "/repos/other/repo/pulls/1")])
def test_any_other_write_is_refused_before_the_request(method, path):
    calls = []
    gh = R.GitHub(REPO, "t", http=lambda *a: calls.append(a) or (200, b"{}"))
    with pytest.raises(R.RetriggerError, match="REFUSED"):
        gh.call(method, path)
    assert calls == []


def test_api_errors_produce_nothing():
    gh = R.GitHub(REPO, "t", http=lambda m, u: (503, b""))
    with pytest.raises(R.RetriggerError, match="HTTP_503"):
        R.run(event(), gh)


def test_workflow_runs_from_default_branch_with_minimal_permissions():
    import yaml
    w = yaml.safe_load((Path(__file__).resolve().parents[2] / ".github/workflows/pr-readiness-retrigger.yml")
                       .read_text())
    on = w.get("on", w.get(True))
    assert list(on) == ["check_run"] and on["check_run"]["types"] == ["completed"]
    assert w["permissions"] == {"actions": "write", "contents": "read", "pull-requests": "read", "checks": "read"}
    job = w["jobs"]["retrigger"]
    assert "pickfair-dedicated" in job["if"] and "pf-betfair-certification" in job["if"]
    assert job["timeout-minutes"] <= 10
    assert all("ref" not in (s.get("with") or {}) for s in job["steps"])      # niente checkout del codice della PR
