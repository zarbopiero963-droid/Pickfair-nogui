"""PF-DISPATCHER-01 — worker di Claude B in dry-run: nessuna esecuzione, lease esclusivo, nessun segreto."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import claude_worker as W  # noqa: E402
import roadmap_dispatcher as D  # noqa: E402

SELECT = {"action": "SELECT", "reason": "READY", "card": "CB-586-FI-08B", "assignment": "CB-586-FI-08B#1",
          "state": "ASSIGNED"}
RESUME = {"action": "RESUME", "reason": "ACTIVE_PR_OPEN", "card": "F-13b", "assignment": "F-13b#1",
          "state": "PR_OPEN", "pr": 610}


def _run(tmp_path, decision, **kw):
    return W.run_once(decision, repo=D.REPO, issue=586, workdir=str(tmp_path / "work"),
                      state_dir=str(tmp_path / "state"), **kw)


def test_dry_run_plans_without_executing_anything(tmp_path, monkeypatch):
    import subprocess
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: pytest.fail("eseguito un comando in dry-run"))
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **k: pytest.fail("eseguito un comando in dry-run"))
    plan = _run(tmp_path, SELECT, holder="w1")
    assert plan["dry_run"] is True and plan["fencing"] == 1
    assert plan["branch"] == "auto/cb-586-fi-08b"
    assert plan["commands"][1][:4] == ["git", "-C", str(tmp_path / "work" / "_repo"), "worktree"]
    assert plan["commands"][2][0] == "claude" and "--output-format" in plan["commands"][2]
    assert not (tmp_path / "work").exists()                          # nessun worktree creato
    audit = [json.loads(x) for x in (tmp_path / "state" / "audit.jsonl").read_text().splitlines()]
    assert audit[-1]["event"] == "PLANNED" and audit[-1]["assignment"] == "CB-586-FI-08B#1"


def test_prompt_carries_the_rules_and_the_stop_conditions():
    p = W.prompt_for(SELECT, D.REPO, 586)
    for needle in ("CLAUDE.md", "AGENTS.md", "RED", "Una sola PR", "10 cicli", "OWNER_REQUIRED", "denaro reale"):
        assert needle in p
    r = W.prompt_for(RESUME, D.REPO, 586)
    assert "RIPRESA" in r and "PR #610" in r and "non aprire una seconda PR" in r


def test_resume_reuses_the_worktree_instead_of_creating_one(tmp_path):
    plan = _run(tmp_path, RESUME, holder="w1")
    assert plan["commands"][1][-2:] == ["status", "--short"]


def test_second_worker_cannot_take_the_same_or_another_job(tmp_path):
    _run(tmp_path, SELECT, holder="w1")
    with pytest.raises(D.LeaseHeld):
        _run(tmp_path, SELECT, holder="w2")
    with pytest.raises(D.LeaseHeld):
        _run(tmp_path, RESUME, holder="w1")                         # un lavoro alla volta


def test_restart_of_the_same_worker_resumes_with_the_same_fencing(tmp_path):
    assert _run(tmp_path, SELECT, holder="w1")["fencing"] == 1
    assert _run(tmp_path, SELECT, holder="w1")["fencing"] == 1


def test_idle_when_the_dispatcher_selects_nothing(tmp_path):
    plan = _run(tmp_path, {"action": "NONE", "reason": "ONE_PR_AT_A_TIME"})
    assert plan["action"] == "IDLE" and plan["commands"] == []
    assert not (tmp_path / "state" / "lease.json").exists()


def test_activation_is_always_refused(tmp_path, monkeypatch):
    monkeypatch.setenv("PF_WORKER_ACTIVATED", "1")
    with pytest.raises(W.ActivationRefused, match="OWNER_GO_REQUIRED"):
        _run(tmp_path, SELECT, holder="w1", activate=True)
    assert json.loads((tmp_path / "state" / "audit.jsonl").read_text().splitlines()[-1])["event"] == \
        "ACTIVATION_REFUSED"


def test_cli_activate_fails_closed(tmp_path, capsys, monkeypatch):
    monkeypatch.setenv("PF_WORKER_ACTIVATED", "1")
    f = tmp_path / "d.json"
    f.write_text(json.dumps(SELECT))
    rc = W.main(["--decision-file", str(f), "--workdir", str(tmp_path / "w"), "--state-dir", str(tmp_path / "s"),
                 "--activate"])
    assert rc == 2 and "OWNER_GO_REQUIRED" in capsys.readouterr().out
    assert W.main(["--decision-file", str(f), "--workdir", str(tmp_path / "w"), "--state-dir", str(tmp_path / "s")]) == 0


def test_a_secret_in_the_plan_or_audit_fails_closed(tmp_path):
    leaky = dict(SELECT, card="CB-586-FI-08B", assignment="CB-586-FI-08B#1")
    with pytest.raises(D.DispatcherError, match="SECRET"):
        W.build_plan(leaky, repo=D.REPO, issue=586, workdir="/w/ghp_" + "a" * 36, holder="w")
    with pytest.raises(D.DispatcherError, match="SECRET"):
        W.audit(str(tmp_path), {"note": "sk-ant-" + "x" * 20})
