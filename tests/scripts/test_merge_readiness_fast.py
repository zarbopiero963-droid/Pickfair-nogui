"""PF-MERGE-READINESS-FAST-01 (#586): Merge readiness veloce, con scadenze esplicite e fail-closed.

Nessun caso qui puo' produrre un falso SUCCESS: GitHub che non risponde, timeout GraphQL, rate limit, 5xx, rete
interrotta, PRE-LIVE in corso o fallito, review con bloccanti, check autoreferenziale, nuovo SHA durante la verifica,
ripresa dopo un errore e scadenza complessiva superata.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import pr_flow_automation as flow  # noqa: E402


class Clock:
    def __init__(self):
        self.now = 1_000_000.0
        self.sleeps = []

    def time(self):
        return self.now

    def sleep(self, s):
        self.sleeps.append(s)
        self.now += s


@pytest.fixture
def clock(monkeypatch):
    c = Clock()
    monkeypatch.setattr(flow.time, "time", c.time)
    monkeypatch.setattr(flow.time, "sleep", c.sleep)
    monkeypatch.setattr(flow, "ai_review_gate", lambda *a, **k: {"gate": {"status": "PASS"}})
    monkeypatch.setattr(flow, "live_readiness", lambda *a, **k: {"live_certification": "NOT_RUN",
                                                                   "live_readiness": "UNKNOWN"})
    return c


def _args(**kw):
    base = dict(repo="o/r", pr="602", ignore_safe_autofix=True, wait_unknown_seconds=0, wait_pending_seconds=600,
                poll_seconds=15, max_poll_seconds=60, backoff_after_seconds=300, deadline_seconds=0, output="",
                no_fail=False)
    base.update(kw)
    return argparse.Namespace(**base)


def _decision(pending=(), can_merge=True, head="a" * 40, seen=40, reasons=()):
    return {"already_merged": False, "can_merge": can_merge and not pending, "mergeable": "MERGEABLE",
            "mergeStateStatus": "CLEAN", "pending": [{"name": p} for p in pending], "checks_seen": seen,
            "headRefOid": head, "reasons": list(reasons), "next_action": "merge"}


def _sequence(monkeypatch, items):
    """``_readiness_decision`` finto: restituisce gli elementi in ordine (eccezioni sollevate), poi l'ultimo."""
    calls = {"n": 0}

    def fake(*_a):
        i = min(calls["n"], len(items) - 1)
        calls["n"] += 1
        item = items[i]
        if isinstance(item, BaseException):
            raise item
        return dict(item)
    monkeypatch.setattr(flow, "_readiness_decision", fake)
    return calls


def _run(args, capsys):
    rc = flow.cmd_readiness(args)
    import json
    out = capsys.readouterr().out
    return rc, json.loads(out[out.index("{"):])


# ── 1-5: GitHub che non risponde, timeout, rate limit, 5xx, rete interrotta ───────────────────────────────────────

def test_1_a_hanging_gh_call_is_bounded_and_reported(monkeypatch):
    def hang(*a, **k):
        assert k.get("timeout") == flow.GH_CALL_TIMEOUT_SECONDS
        raise subprocess.TimeoutExpired(a[0], k["timeout"])
    monkeypatch.setattr(flow.subprocess, "run", hang)
    with pytest.raises(flow.TransientGitHubError, match="timed out"):
        flow.sh(["gh", "pr", "view", "602"])


@pytest.mark.parametrize("stderr", [
    'Post "https://api.github.com/graphql": dial tcp 20.29.134.17:443: i/o timeout',   # 2: osservato su #602
    "API rate limit exceeded for installation",                                           # 3
    "HTTP 502: Bad Gateway",                                                              # 4
    "read: connection reset by peer",                                                     # 5
])
def test_2_to_5_transient_errors_are_retried_a_bounded_number_of_times(monkeypatch, stderr):
    calls, sleeps = [], []
    monkeypatch.setattr(flow, "sh", lambda cmd: calls.append(cmd) or (_ for _ in ()).throw(RuntimeError(stderr)))
    monkeypatch.setattr(flow.time, "sleep", sleeps.append)
    with pytest.raises(RuntimeError):
        flow.gh_json(["gh", "api", "x"])
    assert len(calls) == flow.GH_RETRIES and len(sleeps) == flow.GH_RETRIES - 1
    assert sum(sleeps) <= flow.GH_RETRY_BACKOFF_SECONDS * (2 ** flow.GH_RETRIES)


def test_transient_error_then_success_returns_the_data(monkeypatch):
    out = iter([RuntimeError("HTTP 503"), '{"ok": true}'])

    def sh(cmd):
        v = next(out)
        if isinstance(v, BaseException):
            raise v
        return v
    monkeypatch.setattr(flow, "sh", sh)
    monkeypatch.setattr(flow.time, "sleep", lambda s: None)
    assert flow.gh_json(["gh", "api", "x"]) == {"ok": True}


def test_non_transient_errors_are_not_retried(monkeypatch):
    calls = []
    monkeypatch.setattr(flow, "sh", lambda cmd: calls.append(cmd) or (_ for _ in ()).throw(
        RuntimeError("HTTP 404: Not Found")))
    with pytest.raises(RuntimeError, match="404"):
        flow.gh_json(["gh", "api", "x"])
    assert len(calls) == 1


def test_github_unavailable_from_the_start_is_never_ready(monkeypatch, clock, capsys):
    _sequence(monkeypatch, [flow.TransientGitHubError("dial tcp: i/o timeout")])
    rc, d = _run(_args(wait_pending_seconds=60, deadline_seconds=120), capsys)
    assert rc == 1 and d["can_merge"] is False
    assert any(r.startswith("github_api_unavailable") for r in d["reasons"])


# ── 8-10: PRE-LIVE in corso o fallito, review con bloccanti ──────────────────────────────────────────────────────

def test_8_prelive_still_running_at_the_deadline_is_pending_not_pass(monkeypatch, clock, capsys):
    _sequence(monkeypatch, [_decision(pending=["pf-betfair-certification"])])
    rc, d = _run(_args(wait_pending_seconds=900, deadline_seconds=600), capsys)
    assert rc == 1 and d["can_merge"] is False
    assert [p["name"] for p in d["pending"]] == ["pf-betfair-certification"]
    assert any("deadline_exceeded" in r for r in d["reasons"])


def test_9_prelive_failed_is_not_ready(monkeypatch, clock, capsys):
    _sequence(monkeypatch, [_decision(can_merge=False, reasons=["pf-betfair-certification: FAILURE"])])
    rc, d = _run(_args(), capsys)
    assert rc == 1 and d["can_merge"] is False


def test_10_review_blockers_are_not_ready(monkeypatch, clock, capsys):
    _sequence(monkeypatch, [_decision(can_merge=False, reasons=["active unresolved review thread"])])
    rc, d = _run(_args(), capsys)
    assert rc == 1 and d["can_merge"] is False


# ── 11: check autoreferenziale ───────────────────────────────────────────────────────────────────────────────────

def test_11_merge_readiness_never_waits_for_itself():
    pr = {"statusCheckRollup": [
        {"__typename": "CheckRun", "name": "Merge readiness", "status": "IN_PROGRESS", "conclusion": None},
        {"__typename": "CheckRun", "name": "tests", "status": "COMPLETED", "conclusion": "SUCCESS"}]}
    split = flow.split_checks(pr)
    assert [c["name"] for c in split["pending"]] == []


# ── 12-14: nuovo SHA, ripresa dopo un errore, scadenza complessiva ───────────────────────────────────────────────

def test_12_new_head_during_the_check_is_never_ready(monkeypatch, clock, capsys):
    _sequence(monkeypatch, [_decision(pending=["tests"], head="a" * 40), _decision(head="b" * 40)])
    rc, d = _run(_args(), capsys)
    assert rc == 1 and d["can_merge"] is False
    assert any(r.startswith("head_changed_during_readiness") for r in d["reasons"])


def test_13_a_transient_error_mid_wait_recovers_and_still_judges_the_real_state(monkeypatch, clock, capsys):
    _sequence(monkeypatch, [_decision(pending=["pf-betfair-certification"]),
                            flow.TransientGitHubError('Post "https://api.github.com/graphql": i/o timeout'),
                            _decision()])
    rc, d = _run(_args(), capsys)
    assert rc == 0 and d["can_merge"] is True
    assert d["github_api_errors"] and "i/o timeout" in d["github_api_errors"][0]


def test_13b_a_non_transient_error_is_not_swallowed(monkeypatch, clock):
    _sequence(monkeypatch, [_decision(pending=["tests"]), ValueError("schema changed")])
    with pytest.raises(ValueError):
        flow.cmd_readiness(_args())


def test_14_overall_deadline_caps_the_wait_and_is_never_ready(monkeypatch, clock, capsys):
    _sequence(monkeypatch, [_decision(pending=["slow"])])
    rc, d = _run(_args(wait_pending_seconds=6600, deadline_seconds=900), capsys)
    assert rc == 1 and d["can_merge"] is False
    assert clock.now - 1_000_000.0 <= 900 + 60                      # al massimo un intervallo oltre la scadenza
    assert any("readiness_deadline_exceeded" in r for r in d["reasons"])


def test_green_checks_are_recognised_quickly(monkeypatch, clock, capsys):
    _sequence(monkeypatch, [_decision()])
    rc, d = _run(_args(), capsys)
    assert rc == 0 and d["can_merge"] is True
    assert clock.now - 1_000_000.0 <= 15 * flow.INTERVALLI_STABILI_RICHIESTI     # solo i giri di stabilita'


def test_polling_backs_off_while_a_slow_check_is_pending_and_not_while_counting_stability(monkeypatch, clock,
                                                                                         capsys):
    _sequence(monkeypatch, [_decision(pending=["pf-betfair-certification"])] * 40 + [_decision()])
    _run(_args(wait_pending_seconds=6600), capsys)
    assert clock.sleeps[0] == 15 and 60 in clock.sleeps
    assert clock.sleeps[-1] == 15            # a pending vuoto la stabilita' si conta all'intervallo base


def test_review_threads_are_not_paginated_while_checks_are_pending(monkeypatch):
    threads = []
    monkeypatch.setattr(flow, "fetch_all_review_threads", lambda *a: threads.append(a) or [])
    monkeypatch.setattr(flow, "build_decision", lambda *a, review_threads=None, **k: _decision(
        pending=["tests"] if review_threads is None else []))
    d = flow._readiness_decision("o/r", "602", True)
    assert d["can_merge"] is False and d["review_threads_deferred"] is True and threads == []
    monkeypatch.setattr(flow, "build_decision", lambda *a, review_threads=None, **k: _decision())
    assert flow._readiness_decision("o/r", "602", True)["can_merge"] is True and len(threads) == 1


def test_workflow_sets_explicit_limits():
    text = (Path(__file__).resolve().parents[2] / ".github/workflows/pr-merge-readiness.yml").read_text()
    for needle in ("--deadline-seconds", "--max-poll-seconds", "PF_GH_CALL_TIMEOUT_SECONDS"):
        assert needle in text, needle


# ── PF-CI-PERFORMANCE-FINAL-01: stati, PENDING_EXTERNAL, revoca, restart, lettura finale ─────────────────────────

def test_status_mapping_only_ready_is_mergeable():
    assert flow.readiness_status(_decision()) == "READY"
    assert flow.readiness_status(_decision(pending=["pf-betfair-certification"])) == "PENDING_EXTERNAL"
    assert flow.readiness_status(_decision(pending=["tests"])) == "BLOCKED"
    assert flow.readiness_status(dict(_decision(can_merge=False), blockers=[{"name": "tests"}])) == "FAILED"
    assert flow.readiness_status(_decision(can_merge=False, reasons=["github_api_unavailable: x"])) == "TIMEOUT"
    assert flow.readiness_status(_decision(pending=["pf-betfair-certification"]), deadline_hit=True) == "TIMEOUT"
    mixed = dict(_decision(pending=["pf-betfair-certification"]), blockers=[{"name": "tests"}])
    assert flow.readiness_status(mixed) == "FAILED"


def test_6_slow_prelive_exits_early_as_pending_external_and_is_not_ready(monkeypatch, clock, capsys):
    _sequence(monkeypatch, [_decision(pending=["tests", "pf-betfair-certification"])] * 3
              + [_decision(pending=["pf-betfair-certification"])])
    rc, d = _run(_args(wait_pending_seconds=6600, pending_external_exit_after=300), capsys)
    assert rc == 1 and d["can_merge"] is False and d["readiness_status"] == "PENDING_EXTERNAL"
    assert clock.now - 1_000_000.0 < 400                          # non resta un'ora sul runner


def test_pending_external_never_exits_while_other_checks_are_pending(monkeypatch, clock, capsys):
    _sequence(monkeypatch, [_decision(pending=["tests", "pf-betfair-certification"])])
    rc, d = _run(_args(wait_pending_seconds=1200, pending_external_exit_after=300), capsys)
    assert rc == 1 and d["readiness_status"] == "BLOCKED"           # attende fino al budget, poi non pronta
    assert clock.now - 1_000_000.0 >= 1200


def test_7_prelive_success_is_ready(monkeypatch, clock, capsys):
    _sequence(monkeypatch, [_decision(pending=["pf-betfair-certification"]), _decision()])
    rc, d = _run(_args(), capsys)
    assert rc == 0 and d["readiness_status"] == "READY"


def test_9_prelive_revoked_after_success_is_not_ready(monkeypatch, clock, capsys):
    """SUCCESS letto e poi revocato (nuova run FAILURE sullo stesso head) prima del giudizio: non pronta."""
    _sequence(monkeypatch, [_decision(pending=["pf-betfair-certification"]), _decision(),
                            dict(_decision(can_merge=False, reasons=["pf-betfair-certification: FAILURE"]),
                                 blockers=[{"name": "pf-betfair-certification"}])])
    rc, d = _run(_args(), capsys)
    assert rc == 1 and d["can_merge"] is False and d["readiness_status"] == "FAILED"


def test_15_restart_gives_the_same_judgement(monkeypatch, clock, capsys):
    seq = [_decision(pending=["pf-betfair-certification"]), _decision()]
    _sequence(monkeypatch, seq)
    rc1, d1 = _run(_args(), capsys)
    _sequence(monkeypatch, seq)
    rc2, d2 = _run(_args(), capsys)
    assert (rc1, d1["readiness_status"], d1["can_merge"]) == (rc2, d2["readiness_status"], d2["can_merge"])


def test_18_checks_changing_between_the_last_read_and_the_verdict_are_seen(monkeypatch, clock, capsys):
    """Il giudizio si da' sull'ULTIMA lettura completa: un check che compare durante la finestra di stabilita'
    blocca anche se la lettura precedente era verde."""
    _sequence(monkeypatch, [_decision(), _decision(seen=41, pending=["late"]), _decision(seen=41, pending=["late"])])
    rc, d = _run(_args(wait_pending_seconds=60), capsys)
    assert rc == 1 and d["can_merge"] is False


# ── PF-DUAL-CERTIFICATION-01: LIVE_READINESS visibile, mai requisito, mai atteso ─────────────────────────────────

H = "a" * 40


def _live(conclusion="neutral", status="completed", title="LIVE_NOT_RUN_NO_LIVE_OBLIGATIONS", summary="",
          app="pickfair-dedicated", head=H, rid=5):
    return {"id": rid, "name": flow.LIVE_CHECK, "app": {"slug": app}, "head_sha": head, "status": status,
            "conclusion": conclusion, "output": {"title": title, "summary": summary}}


@pytest.mark.parametrize("runs,cert,ready", [
    ([_live()], "NOT_RUN", "UNKNOWN"),
    ([_live(summary="LIVE_READINESS=PASS (read-only)")], "NOT_RUN", "PASS"),
    ([_live(title="LIVE_NOT_RUN | LIVE_READINESS_BLOCKED")], "NOT_RUN", "BLOCKED"),
    ([_live(status="in_progress", conclusion=None)], "PENDING", "UNKNOWN"),
    ([_live(conclusion="failure")], "FAILED", "UNKNOWN"),
    ([_live(conclusion="success", summary="LIVE_READINESS_PASS")], "CERTIFIED", "PASS"),
    ([_live(app="evil", summary="LIVE_READINESS_PASS")], "MISSING", "UNKNOWN"),
    ([_live(head="b" * 40, summary="LIVE_READINESS_PASS")], "MISSING", "UNKNOWN"),
    ([], "MISSING", "UNKNOWN"),
])
def test_live_readiness_is_parsed_from_the_trusted_live_check_of_the_exact_head(runs, cert, ready):
    out = flow.live_readiness_from_checks(runs, H)
    assert (out["live_certification"], out["live_readiness"]) == (cert, ready)


def test_live_readiness_never_changes_the_merge_verdict(monkeypatch, clock, capsys):
    monkeypatch.setattr(flow, "live_readiness", lambda *a: {"live_certification": "FAILED", "live_readiness":
                                                             "BLOCKED"})
    _sequence(monkeypatch, [_decision()])
    rc, d = _run(_args(), capsys)
    assert rc == 0 and d["can_merge"] is True and d["live_readiness"] == "BLOCKED"
    monkeypatch.setattr(flow, "live_readiness", lambda *a: {"live_certification": "CERTIFIED", "live_readiness":
                                                             "PASS"})
    _sequence(monkeypatch, [_decision(can_merge=False, reasons=["tests: FAILURE"])])
    rc, d = _run(_args(), capsys)
    assert rc == 1 and d["can_merge"] is False                     # un LIVE verde non rende pronta una PR rossa


def test_live_lookup_errors_are_unknown_not_pass(monkeypatch):
    monkeypatch.setattr(flow, "gh_json", lambda cmd: (_ for _ in ()).throw(flow.TransientGitHubError("timeout")))
    out = flow.live_readiness("o/r", H)
    assert out["live_certification"] == "UNKNOWN" and out["live_readiness"] == "UNKNOWN"


# ── Sol su #603 (range 5a1e01c...1e2d6ac): una lettura fallita non e' una verifica ────────────────────────────────
def test_kept_reads_after_a_green_read_never_count_as_stable_intervals(monkeypatch, clock, capsys):
    """Dopo una lettura verde, fetch falliti tenevano can_merge=True e contavano come intervalli stabili: due errori
    transitori bastavano per READY senza alcuna verifica finale riuscita."""
    _sequence(monkeypatch, [_decision(pending=["tests"]), _decision(),
                            flow.TransientGitHubError("i/o timeout")])
    rc, d = _run(_args(), capsys)
    assert rc == 1 and d["can_merge"] is False and d["readiness_status"] == "TIMEOUT"
    assert any(r.startswith("github_api_unavailable") for r in d["reasons"])


def test_a_failed_read_resets_stability_and_a_fresh_green_read_still_decides(monkeypatch, clock, capsys):
    calls = _sequence(monkeypatch, [_decision(pending=["tests"]), _decision(),
                                    flow.TransientGitHubError("i/o timeout"), _decision(), _decision(), _decision()])
    rc, d = _run(_args(), capsys)
    assert rc == 0 and d["can_merge"] is True
    assert calls["n"] >= 5                    # l'errore non conta: servono due intervalli stabili su letture vere


def test_the_unknown_wait_never_ends_on_a_kept_read(monkeypatch, clock, capsys):
    unknown = dict(_decision(), mergeable="UNKNOWN")
    _sequence(monkeypatch, [unknown, flow.TransientGitHubError("502 Bad Gateway")])
    rc, d = _run(_args(wait_unknown_seconds=60, wait_pending_seconds=0), capsys)
    assert rc == 1 and d["can_merge"] is False


# ── Codex su #603: dopo la scadenza complessiva nessuna chiamata in piu' prima di scrivere il giudizio ─────────────
def test_past_the_overall_deadline_informational_lookups_are_skipped(monkeypatch, clock, capsys):
    called = []
    monkeypatch.setattr(flow, "live_readiness", lambda *a, **k: called.append("live") or {})
    monkeypatch.setattr(flow, "ai_review_gate", lambda *a, **k: called.append("ai") or {})
    _sequence(monkeypatch, [_decision(pending=["slow"])])
    rc, d = _run(_args(wait_pending_seconds=6600, deadline_seconds=900), capsys)
    assert rc == 1 and called == []
    assert d["live_certification"] == "UNKNOWN" and d["live_readiness"] == "UNKNOWN"
    assert d["ai_review_gate_status"] == "UNKNOWN"


def test_poll_sleep_never_overshoots_the_overall_deadline(monkeypatch, clock, capsys):
    _sequence(monkeypatch, [_decision(pending=["slow"])])
    _run(_args(wait_pending_seconds=6600, deadline_seconds=1000, poll_seconds=15, max_poll_seconds=60,
               backoff_after_seconds=0), capsys)
    assert clock.now - 1_000_000.0 <= 1000
