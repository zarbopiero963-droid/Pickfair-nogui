"""PF-DISPATCHER-01 — test della coda Roadmap V2 (scheda Grok #586 c.6583997783: RED, GREEN, mutation)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import roadmap_dispatcher as D  # noqa: E402

OWNER = "zarbopiero963-droid"
REPO = D.REPO


def card(cid, status="READY_FOR_CLAUDE", prio="P1", deps=(), order=10_000):
    return D.Card(id=cid, priority=prio, status=status, deps=tuple(deps), order=order)


def cards(*cs):
    return {c.id: c for c in cs}


def ev(assignment, state, pr=None, card_id=None):
    return D.Event(assignment=assignment, card=card_id or assignment.split("#")[0], state=state, pr=pr)


def comment(body, login=OWNER, cid=1):
    return {"id": cid, "user": {"login": login}, "body": body}


def marker(**kw):
    return f"<!-- pf-dispatch:v1 {json.dumps(kw)} -->"


TASK_PR = {"number": 700, "state": "open", "title": "[TASK: x] qualcosa"}
GOV_PR = {"number": 602, "state": "open", "title": "[TASK: gov_review_attestation] Governance"}


# ── selezione ───────────────────────────────────────────────────────────────────────────────────────────────────────

def test_selects_by_priority_then_order():
    d = D.plan(cards(card("F-18", prio="P1", order=8), card("CB-586-FI-08B", prio="P1", order=2),
                     card("F-28", prio="P3", order=1), card("F-05", prio="P0", order=50)), [], [])
    assert d.action == "SELECT" and d.card == "F-05" and d.assignment == "F-05#1"


def test_unmet_dependency_is_not_selected():
    d = D.plan(cards(card("F-02", deps=["F-01"]), card("F-01", status="PR_OPEN")), [], [])
    assert d.action == "NONE" and d.reason == "NOTHING_READY"
    assert d.detail["waiting_on_deps"] == {"F-02": ["F-01"]}


def test_dependency_satisfied_by_merged_label_or_event():
    assert D.plan(cards(card("F-02", deps=["F-01"]), card("F-01", status="MERGED")), [], []).card == "F-02"
    evs = [ev("F-01#1", "ASSIGNED"), ev("F-01#1", "PR_OPEN", 610), ev("F-01#1", "MERGED", 610),
           ev("F-01#1", "VERIFIED", 610)]
    assert D.plan(cards(card("F-02", deps=["F-01"]), card("F-01", status="PR_OPEN")), evs, []).card == "F-02"


def test_unknown_dependency_is_never_satisfied():
    d = D.plan(cards(card("F-02", deps=["F-99"])), [], [])
    assert d.action == "NONE" and d.detail["waiting_on_deps"] == {"F-02": ["F-99"]}


def test_only_ready_for_claude_cards_are_eligible():
    for st in ("BACKLOG", "IN_PROGRESS", "BLOCKED", "MERGED", "VERIFIED", "PR_OPEN"):
        assert D.plan(cards(card("F-18", status=st)), [], []).action == "NONE"


# ── una PR alla volta ───────────────────────────────────────────────────────────────────────────────────────────────

def test_nothing_selected_while_602_is_open():
    d = D.plan(cards(card("F-13b")), [], [GOV_PR], blocking_prs=[602])
    assert d.action == "NONE" and d.reason == "ONE_PR_AT_A_TIME" and d.detail["open_prs"] == [602]


def test_blocking_pr_counts_even_without_task_marker():
    d = D.plan(cards(card("F-13b")), [], [{"number": 602, "state": "open", "title": "governance"}],
               blocking_prs=[602])
    assert d.action == "NONE" and d.reason == "ONE_PR_AT_A_TIME"


def test_two_open_prs_select_nothing():
    d = D.plan(cards(card("F-13b")), [], [TASK_PR, GOV_PR])
    assert d.action == "NONE" and d.detail["open_prs"] == [602, 700]


def test_non_task_prs_do_not_block():
    assert D.plan(cards(card("F-13b")), [], [{"number": 9, "state": "open", "title": "docs: typo"}]).action == "SELECT"


def test_active_assignment_with_a_foreign_pr_waits():
    evs = [ev("F-13b#1", "ASSIGNED"), ev("F-13b#1", "PR_OPEN", 610)]
    d = D.plan(cards(card("F-13b", status="PR_OPEN")), evs, [{"number": 610, "state": "open", "title": "[TASK: a]"},
                                                             TASK_PR])
    assert d.action == "NONE" and d.reason == "ONE_PR_AT_A_TIME" and d.detail["open_prs"] == [700]


# ── idempotenza, riavvio, lease ─────────────────────────────────────────────────────────────────────────────────────

def test_no_double_assignment_after_restart():
    """Dopo un riavvio lo stato si ricostruisce dagli eventi: la scheda assegnata si riprende, non si riassegna."""
    evs = D.events_from_comments([comment(marker(assignment="F-13b#1", card="F-13b", state="ASSIGNED"))], [OWNER])
    d = D.plan(cards(card("F-13b"), card("F-18", prio="P0")), evs, [])
    assert d.action == "RESUME" and d.assignment == "F-13b#1"


def test_same_snapshot_same_decision_and_fingerprint():
    cs, evs = cards(card("F-13b"), card("F-18")), [ev("F-02#1", "ASSIGNED"), ev("F-02#1", "RELEASED")]
    a, b = D.plan(cs, evs, []), D.plan(cs, evs, [])
    assert a.to_dict() == b.to_dict() and D.fingerprint(cs, evs, []) == D.fingerprint(cs, evs, [])


def test_new_assignment_ids_never_repeat():
    evs = [ev("F-13b#1", "ASSIGNED"), ev("F-13b#1", "RELEASED")]
    assert D.plan(cards(card("F-13b")), evs, []).assignment == "F-13b#2"


def test_two_active_assignments_fail_closed():
    with pytest.raises(D.DispatcherError, match="TWO_ACTIVE"):
        D.plan(cards(card("F-13b")), [ev("F-13b#1", "ASSIGNED"), ev("F-18#1", "ASSIGNED")], [])


@pytest.mark.parametrize("seq", [["PR_OPEN"], ["ASSIGNED", "VERIFIED"], ["ASSIGNED", "PR_OPEN", "VERIFIED"],
                                 ["ASSIGNED", "RELEASED", "PR_OPEN"]])
def test_invalid_transitions_fail_closed(seq):
    with pytest.raises(D.DispatcherError, match="TRANSITION_INVALID"):
        D.assignments([ev("F-13b#1", s, 610) for s in seq])


def test_merged_is_not_the_end_until_verified():
    evs = [ev("F-13b#1", "ASSIGNED"), ev("F-13b#1", "PR_OPEN", 610), ev("F-13b#1", "MERGED", 610)]
    d = D.plan(cards(card("F-13b", status="MERGED"), card("F-18")), evs, [])
    assert d.action == "RESUME" and d.state == "MERGED"          # verifica di main prima della scheda successiva


def test_events_from_untrusted_logins_are_ignored_and_malformed_trusted_fail():
    body = marker(assignment="F-13b#1", card="F-13b", state="ASSIGNED")
    assert D.events_from_comments([comment(body, login="mallory")], [OWNER]) == []
    with pytest.raises(D.DispatcherError, match="EVENT_MALFORMED"):
        D.events_from_comments([comment("<!-- pf-dispatch:v1 {not json} -->")], [OWNER])
    with pytest.raises(D.DispatcherError, match="EVENT_MALFORMED"):
        D.events_from_comments([comment(marker(assignment="F-18#1", card="F-13b", state="ASSIGNED"))], [OWNER])


def test_local_lease_one_holder_and_fencing(tmp_path):
    t = [1000.0]
    lease = D.LocalLease(str(tmp_path / "lease.json"), clock=lambda: t[0])
    a = lease.acquire("F-13b#1", "worker-a", ttl=60)
    assert a["fencing"] == 1 and lease.check("F-13b#1", "worker-a", 1)
    with pytest.raises(D.LeaseHeld):
        lease.acquire("F-13b#1", "worker-b", ttl=60)
    with pytest.raises(D.LeaseHeld):
        lease.acquire("F-18#1", "worker-a", ttl=60)                # un lavoro alla volta anche per lo stesso worker
    assert lease.acquire("F-13b#1", "worker-a", ttl=60)["fencing"] == 1   # rinnovo idempotente
    t[0] += 61
    b = lease.acquire("F-13b#1", "worker-b", ttl=60)                # scaduto: lo riprende un altro, fencing+1
    assert b["fencing"] == 2 and not lease.check("F-13b#1", "worker-a", 1)


def test_corrupted_lease_fails_closed(tmp_path):
    p = tmp_path / "lease.json"
    p.write_text("{oops")
    with pytest.raises(D.DispatcherError, match="LEASE_CORRUPTED"):
        D.LocalLease(str(p)).acquire("F-13b#1", "w", 60)


# ── fonti: issue e registro ─────────────────────────────────────────────────────────────────────────────────────────

def test_card_from_issue_labels_and_marker():
    c = D.card_from_issue({"number": 640, "title": "[CB-586-FI-03Z] SIM size-0", "state": "open",
                           "body": "x <!-- pf-card:priority=P1 deps=F-07,F-05 order=3 --> y",
                           "labels": [{"name": "roadmap-v2"}, {"name": "READY_FOR_CLAUDE"}]})
    assert (c.id, c.priority, c.status, c.deps, c.order, c.issue) == ("CB-586-FI-03Z", "P1", "READY_FOR_CLAUDE",
                                                                      ("F-05", "F-07"), 3, 640)


def test_issue_without_card_id_is_not_a_card_and_two_statuses_fail():
    assert D.card_from_issue({"number": 1, "title": "Registro", "labels": []}) is None
    with pytest.raises(D.DispatcherError, match="AMBIGUOUS"):
        D.card_from_issue({"number": 2, "title": "[F-18] x", "labels": ["READY_FOR_CLAUDE", "BLOCKED"]})


def test_registry_parsing_and_issue_precedence():
    body = (D.REGISTRY_MARKER + "\n```json\n" + json.dumps({"cards": [
        {"id": "F-13b", "priority": "P1", "status": "BACKLOG", "deps": ["F-13a"], "order": 1},
        {"id": "F-13a", "status": "MERGED"}]}) + "\n```")
    reg = D.cards_from_registry(body)
    issue = D.Card(id="F-13b", status="READY_FOR_CLAUDE", issue=650)
    merged = D.merge_cards(reg, [issue])
    assert merged["F-13b"].status == "READY_FOR_CLAUDE" and merged["F-13b"].deps == ("F-13a",)
    assert D.plan(merged, [], []).card == "F-13b"
    with pytest.raises(D.DispatcherError, match="DUPLICATED"):
        D.merge_cards(reg, [issue, issue])


@pytest.mark.parametrize("bad", ["no json", "```json\n{bad}\n```", "```json\n" + json.dumps(
    {"cards": [{"id": "f13", "status": "BACKLOG"}]}) + "\n```", "```json\n" + json.dumps(
    {"cards": [{"id": "F-13", "status": "DONE"}]}) + "\n```"])
def test_malformed_registry_fails_closed(bad):
    with pytest.raises(D.DispatcherError):
        D.cards_from_registry(D.REGISTRY_MARKER + "\n" + bad)


# ── classificazione: nessun errore diventa PASS ─────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("check,outcome", [
    ({"status": "completed", "conclusion": "success"}, "PASS"),
    ({"status": "in_progress", "conclusion": None}, "PENDING"),
    ({"status": "queued"}, "PENDING"),
    ({"status": "completed", "conclusion": "failure", "name": "tests"}, "APP_FAILURE"),
    ({"status": "completed", "conclusion": "failure", "summary": "dial tcp 1.2.3.4:443: i/o timeout"},
     "INFRA_FAILURE"),
    ({"status": "completed", "conclusion": "timed_out"}, "INFRA_FAILURE"),
    ({"status": "completed", "conclusion": "action_required"}, "OWNER_APPROVAL"),
    ({"status": "completed", "conclusion": "neutral", "name": "pf-betfair-live-certification"}, "LIVE_NOT_RUN"),
    ({"status": "completed", "conclusion": "neutral", "name": "tests"}, "MISSING_EVIDENCE"),
    ({"status": "completed", "conclusion": "skipped"}, "MISSING_EVIDENCE"),
    ({"status": "completed", "conclusion": "stale"}, "MISSING_EVIDENCE"),
])
def test_classify_check(check, outcome):
    assert D.classify_check(check) == outcome


def _ok(name):
    return {"name": name, "status": "completed", "conclusion": "success"}


def test_pr_gate_precedence_and_required_checks():
    good = [_ok("tests"), _ok("pf-betfair-certification"),
            {"name": "pf-betfair-live-certification", "status": "completed", "conclusion": "neutral"}]
    assert D.pr_gate(good, 0, False) == "PASS"
    assert D.pr_gate(good, 1, False) == "REVIEW_BLOCKERS"
    assert D.pr_gate(good, 0, True) == "OWNER_APPROVAL"
    assert D.pr_gate([_ok("tests")], 0, False) == "MISSING_EVIDENCE"            # PRE-LIVE assente
    assert D.pr_gate([], 0, False) == "MISSING_EVIDENCE"
    assert D.pr_gate(good + [{"name": "x", "status": "queued"}], 0, False) == "PENDING"
    assert D.pr_gate(good + [{"name": "tests2", "status": "completed", "conclusion": "failure"}], 0,
                     False) == "APP_FAILURE"


# ── nessuna scrittura sulle PR ──────────────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("method,path", [
    ("POST", f"/repos/{REPO}/pulls"),
    ("PUT", f"/repos/{REPO}/pulls/602/merge"),
    ("PATCH", f"/repos/{REPO}/pulls/602"),
    ("POST", f"/repos/{REPO}/pulls/602/reviews"),
    ("POST", f"/repos/{REPO}/issues/602/comments"),
    ("POST", f"/repos/{REPO}/issues/586/labels"),
    ("PATCH", f"/repos/{REPO}/issues/comments/1"),
    ("PUT", f"/repos/{REPO}/contents/x.py"),
    ("DELETE", f"/repos/{REPO}/issues/comments/1"),
    ("GET", "/repos/other/repo/issues"),
    ("GET", f"/repos/{REPO}/../x"),
])
def test_any_write_other_than_the_roadmap_event_is_refused_before_the_request(method, path):
    calls = []
    gh = D.GitHub(http=lambda *a: calls.append(a) or (200, b"{}"))
    with pytest.raises(D.WriteRefused):
        gh.request(method, path, {})
    assert calls == []


def test_the_only_write_is_an_event_comment_on_586():
    calls = []
    gh = D.GitHub(http=lambda m, u, b: calls.append((m, u, json.loads(b))) or (201, b"{}"))
    gh.post_event({"assignment": "F-13b#1", "card": "F-13b", "state": "ASSIGNED", "reason": "dispatcher"})
    (m, u, payload), = calls
    assert m == "POST" and u.endswith(f"/repos/{REPO}/issues/586/comments")
    evs = D.events_from_comments([comment(payload["body"])], [OWNER])
    assert [(e.assignment, e.state) for e in evs] == [("F-13b#1", "ASSIGNED")]


def _fake_github(comments, issues, prs):
    def http(method, url, body):
        assert method == "GET", url
        path = url.split("api.github.com", 1)[1]
        if "/issues/586/comments" in path:
            rows = comments
        elif "/issues?" in path:
            rows = issues
        elif "/pulls?" in path:
            rows = prs
        else:
            return 404, b""
        return 200, json.dumps(rows if "page=1" in path else []).encode()
    return D.GitHub(http=http)


def test_snapshot_end_to_end_dry_run():
    reg = comment(D.REGISTRY_MARKER + "\n```json\n" + json.dumps({"cards": [
        {"id": "F-13a", "status": "MERGED"}, {"id": "F-13b", "priority": "P1", "status": "READY_FOR_CLAUDE",
                                              "deps": ["F-13a"], "order": 2},
        {"id": "CB-586-FI-08B", "priority": "P1", "status": "READY_FOR_CLAUDE", "order": 1}]}) + "\n```")
    gh = _fake_github([reg], [], [GOV_PR])
    cs, evs, prs = gh.snapshot([OWNER])
    assert D.plan(cs, evs, prs, blocking_prs=[602]).reason == "ONE_PR_AT_A_TIME"
    gh = _fake_github([reg], [], [])
    cs, evs, prs = gh.snapshot([OWNER])
    assert D.plan(cs, evs, prs).card == "CB-586-FI-08B"


def test_cli_refuses_live_without_the_environment_switch(monkeypatch, capsys):
    reg = comment(D.REGISTRY_MARKER + "\n```json\n" + json.dumps({"cards": [
        {"id": "F-18", "priority": "P1", "status": "READY_FOR_CLAUDE"}]}) + "\n```")
    fake = _fake_github([reg], [], [])
    monkeypatch.setattr(D, "GitHub", lambda *a, **k: fake)
    monkeypatch.delenv("PF_DISPATCHER_LIVE", raising=False)
    assert D.main(["plan", "--live"]) == 2
    assert "LIVE_NOT_ENABLED" in capsys.readouterr().out
    assert D.main(["plan"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["action"] == "SELECT" and out["card"] == "F-18" and out["dry_run"] is True
