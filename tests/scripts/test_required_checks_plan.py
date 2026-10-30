"""Test del piano dei required check (#562 PR-C2, `scripts/ci/required_checks_plan.py`, `ops/ci/required_checks.json`).

Fissano che il piano: descriva il ruleset reale di oggi; non apra mai una finestra senza un controllo; identifichi
ogni check per contesto E app; e che un required check fallito, pendente, mancante, di un'altra app o di un altro
SHA blocchi il merge. Coerenza con chi pubblica e valuta davvero: contesti del dispatcher, self-check delle due
readiness (nessuna dipendenza circolare).
"""
from __future__ import annotations

import copy
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
PLAN_PATH = ROOT / "ops" / "ci" / "required_checks.json"
RULESET_PATH = ROOT / "tests" / "fixtures" / "ruleset_25459867_20261028.json"
PR579_PATH = ROOT / "tests" / "fixtures" / "checks_pr579_8b088ea.json"
SCRIPT = ROOT / "scripts" / "ci" / "required_checks_plan.py"


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


rcp = _load("required_checks_plan", SCRIPT)
td = _load("pr_trusted_dispatcher", ROOT / "scripts" / "pr_trusted_dispatcher.py")
SHA = "a" * 40
OTHER = "b" * 40


def _plan():
    return json.loads(PLAN_PATH.read_text(encoding="utf-8"))


def _ruleset():
    return json.loads(RULESET_PATH.read_text(encoding="utf-8"))


def _green(plan, phase, sha=SHA):
    """Check run e status tutti `success` per i required check della fase."""
    runs, statuses = [], []
    pubs = {p["integration_id"]: p for p in plan["publishers"].values()}
    for req in rcp.required_after(plan, phase):
        pub = pubs[req["integration_id"]]
        if pub["kind"] == "check_run":
            runs.append({"name": req["context"], "status": "completed", "conclusion": "success", "head_sha": sha,
                         "app": {"id": req["integration_id"]}})
        else:
            statuses.append({"context": req["context"], "state": "success", "creator": {"login": pub["status_creator"]},
                             "url": f"https://api.github.com/repos/o/r/statuses/{sha}"})
    return runs, statuses


# -------------------------------------------------------------------------------------------------- piano e ruleset
def test_repository_plan_is_valid_and_ends_with_every_control_required():
    states = rcp.validate(_plan())
    plan = _plan()
    assert {plan["checks"][k]["control"] for k in states[-1]} == set(rcp.CONTROLS)
    assert states[0] == {"actions-overlap"}


def test_plan_current_state_matches_the_live_ruleset():
    assert rcp.diff(_plan(), _ruleset()) == []


@pytest.mark.parametrize("mutate,expected", [
    (lambda r: r.update(enforcement="evaluate"), "non attivo"),
    (lambda r: r.update(bypass_actors=[{"actor_id": 5, "actor_type": "RepositoryRole"}]), "bypass_actors"),
    (lambda r: r.update(id=1), "id"),
    (lambda r: _rsc(r)["required_status_checks"].append({"context": "x", "integration_id": 1}), "diversi"),
    (lambda r: _rsc(r)["required_status_checks"][0].update(integration_id=5492626), "diversi"),
    (lambda r: _rsc(r).update(strict_required_status_checks_policy=True), "strict"),
])
def test_drift_of_the_live_ruleset_is_reported(mutate, expected):
    live = _ruleset()
    mutate(live)
    problems = rcp.diff(_plan(), live)
    assert any(expected in p for p in problems), problems


def _rsc(ruleset):
    return next(r for r in ruleset["rules"] if r["type"] == "required_status_checks")["parameters"]


def test_a_phase_that_removes_a_control_before_its_replacement_is_refused():
    plan = _plan()
    plan["phases"].insert(0, {"name": "ritiro prematuro", "add": [], "remove": ["actions-overlap"], "strict": False,
                              "precondition": "nessuna, e' proprio l'errore da fermare"})
    with pytest.raises(rcp.PlanError, match="copertura"):
        rcp.validate(plan)


@pytest.mark.parametrize("mutate,expected", [
    (lambda p: p["phases"][0]["add"].append("actions-overlap"), "gia' richiesto"),
    (lambda p: p["phases"][0]["remove"].append("lane-readiness"), "non richiesto"),
    (lambda p: p["phases"][0]["add"].append("sconosciuto"), "sconosciuti"),
    (lambda p: p["publishers"]["github-actions"].update(integration_id="15368"), "integration_id"),
    (lambda p: p["publishers"]["github-actions"].update(integration_id=True), "integration_id"),
    (lambda p: p["checks"]["lane-overlap"].update(publisher="github-actions"), "duplicato"),
    (lambda p: p["checks"]["lane-full-suite"].update(control="tests"), "control"),
    (lambda p: p["phases"][1].update(precondition=""), "precondizione"),
    (lambda p: p["phases"].__delitem__(slice(1, None)), "stato finale"),
])
def test_malformed_or_incomplete_plans_are_refused(mutate, expected):
    plan = _plan()
    mutate(plan)
    with pytest.raises(rcp.PlanError, match=expected):
        rcp.validate(plan)


def _unblocked():
    """Il piano con i prerequisiti tecnici aperti (`blocked_by`) dichiarati risolti: per i test sul payload in se'."""
    plan = _plan()
    for ph in plan["phases"]:
        ph.pop("blocked_by", None)
    return plan


def test_payload_identifies_each_check_by_context_and_app():
    out = rcp.payload(_unblocked(), len(_plan()["phases"]))
    assert out["type"] == "required_status_checks"
    req = out["parameters"]["required_status_checks"]
    assert all(isinstance(r["integration_id"], int) and r["integration_id"] > 0 for r in req)
    assert {"context": "full-suite", "integration_id": 5492626} in req
    assert out["parameters"]["strict_required_status_checks_policy"] is True
    with pytest.raises(rcp.PlanError):
        rcp.payload(_plan(), 99)


# ---------------------------------------------------------------------------------- un required check che fallisce
def test_all_required_checks_success_on_the_exact_sha_is_the_only_pass():
    plan = _plan()
    for phase in range(len(plan["phases"]) + 1):
        runs, statuses = _green(plan, phase)
        assert rcp.evaluate(plan, phase, SHA, runs, statuses) == [], phase


@pytest.mark.parametrize("state,expected", [("failure", "failure"), ("error", "error"), ("pending", "pending")])
def test_a_failed_errored_or_pending_required_status_blocks(state, expected):
    plan, phase = _plan(), 1
    runs, statuses = _green(plan, phase)
    statuses = [dict(s, state=state) if s["context"] == "full-suite" else s for s in statuses]
    reasons = rcp.evaluate(plan, phase, SHA, runs, statuses)
    assert reasons and all("full-suite" in r for r in reasons) and expected in reasons[0]


def test_the_latest_status_wins_so_an_old_success_does_not_hide_a_new_failure():
    plan, phase = _plan(), 1
    runs, statuses = _green(plan, phase)
    newest = [{"context": "full-suite", "state": "failure", "creator": {"login": "pickfair-trusted-status[bot]"},
               "url": f"https://api.github.com/repos/o/r/statuses/{SHA}"}]
    assert rcp.evaluate(plan, phase, SHA, runs, newest + statuses)


@pytest.mark.parametrize("conclusion", ["failure", "cancelled", "timed_out", "action_required", "skipped",
                                        "neutral", None, "stale"])
def test_a_required_check_run_that_is_not_success_blocks_even_skipped_or_neutral(conclusion):
    plan, phase = _plan(), 2
    runs, statuses = _green(plan, phase)
    runs = [dict(r, conclusion=conclusion) if r["name"] == "overlap-check" else r for r in runs]
    assert any("overlap-check (app 15368)" in r for r in rcp.evaluate(plan, phase, SHA, runs, statuses))


def test_a_missing_required_check_blocks():
    plan, phase = _plan(), 1
    runs, statuses = _green(plan, phase)
    statuses = [s for s in statuses if s["context"] != "full-suite"]
    assert any("full-suite" in r and "assente" in r for r in rcp.evaluate(plan, phase, SHA, runs, statuses))


def test_the_same_context_from_another_app_does_not_count():
    plan, phase = _plan(), 1
    runs, statuses = _green(plan, phase)
    # overlap-check della lane sostituito da uno status omonimo di un altro autore
    statuses = [dict(s, creator={"login": "github-actions[bot]"}) if s["context"] == "overlap-check" else s
                for s in statuses]
    runs = runs + [{"name": "full-suite", "status": "completed", "conclusion": "success", "head_sha": SHA,
                    "app": {"id": 999}}]
    reasons = rcp.evaluate(plan, phase, SHA, runs, statuses)
    assert any("overlap-check (app 5492626)" in r for r in reasons)


def test_a_required_check_run_from_another_app_does_not_count():
    plan, phase = _plan(), 2
    runs, statuses = _green(plan, phase)
    runs = [dict(r, app={"id": 999}) if r["name"] == "overlap-check" else r for r in runs]
    assert any("overlap-check (app 15368): assente" in r for r in rcp.evaluate(plan, phase, SHA, runs, statuses))


def test_a_check_run_of_another_sha_blocks():
    plan, phase = _plan(), 0
    runs, statuses = _green(plan, phase, sha=OTHER)
    assert any("altro SHA" in r for r in rcp.evaluate(plan, phase, SHA, runs, statuses))


def test_real_head_of_pr579_would_be_blocked_by_the_planned_rules():
    """Dati reali dell'head di #579: con i required check di oggi passa (solo overlap di Actions); con la fase F1
    sarebbe bloccato, perche' la full-suite non e' mai stata pubblicata su quell'head."""
    data = json.loads(PR579_PATH.read_text(encoding="utf-8"))
    plan = _plan()
    assert rcp.evaluate(plan, 0, data["sha"], data["check_runs"], data["statuses"]) == []
    reasons = rcp.evaluate(plan, 1, data["sha"], data["check_runs"], data["statuses"])
    assert reasons == ["full-suite (app 5492626): assente sull'head"]


# ------------------------------------------------------------------------- coerenza con chi pubblica e chi valuta
def test_lane_contexts_are_the_ones_the_dispatcher_publishes():
    plan = _plan()
    lane = {c["context"] for c in plan["checks"].values() if c["publisher"] == "pickfair-trusted-status"}
    published = {g["context"] for g in td.GUARDS.values()} | {td.SUITE_TARGET["context"]}
    assert lane == published


def test_no_readiness_waits_for_itself():
    """Dipendenze circolari: ciascuna readiness esclude se stessa (e l'altra) dai check che attende."""
    plan = _plan()
    readiness = [c for c in plan["checks"].values() if c["control"] == "readiness"]
    for c in readiness:
        if c["publisher"] == "pickfair-trusted-status":
            assert c["context"] in td.READINESS_SELF_STATUS_CONTEXTS
        else:
            assert td._is_readiness_self_check(c["context"])
    flow = _load("pr_flow_automation_for_c2", ROOT / "scripts" / "pr_flow_automation.py")
    assert flow.is_self_check({"name": "Merge readiness"})
    assert flow.is_self_check({"context": "pr-merge-readiness",
                               "targetUrl": "https://buildkite.com/o/pr-merge-readiness/builds/1"})


def test_full_suite_is_waited_for_by_the_readiness_not_excluded():
    """La readiness deve attendere la full-suite: se la escludesse, potrebbe dare PASS prima che la suite finisca."""
    assert td.SUITE_TARGET["context"] not in td.READINESS_SELF_STATUS_CONTEXTS
    assert not td._is_readiness_self_check(td.SUITE_TARGET["context"])


# ----------------------------------------------------------------------------------------------------------- CLI
def _cli(*args):
    return subprocess.run([sys.executable, str(SCRIPT), *args], capture_output=True, text=True)


def test_cli_exit_codes(tmp_path):
    assert _cli("validate", "--plan", str(PLAN_PATH)).returncode == 0
    assert _cli("diff", "--plan", str(PLAN_PATH), "--ruleset", str(RULESET_PATH)).returncode == 0
    drift = _ruleset()
    _rsc(drift)["required_status_checks"] = []
    (tmp_path / "r.json").write_text(json.dumps(drift))
    assert _cli("diff", "--plan", str(PLAN_PATH), "--ruleset", str(tmp_path / "r.json")).returncode == 1
    data = json.loads(PR579_PATH.read_text(encoding="utf-8"))
    (tmp_path / "c.json").write_text(json.dumps({"check_runs": data["check_runs"]}))
    (tmp_path / "s.json").write_text(json.dumps(data["statuses"]))
    common = ("--plan", str(PLAN_PATH), "--sha", data["sha"], "--check-runs", str(tmp_path / "c.json"),
              "--statuses", str(tmp_path / "s.json"))
    assert _cli("evaluate", "--phase", "0", *common).returncode == 0
    res = _cli("evaluate", "--phase", "1", *common)
    assert res.returncode == 1 and "BLOCK: full-suite" in res.stdout
    bad = copy.deepcopy(_plan())
    bad["phases"][0]["remove"] = ["actions-overlap"]
    bad["phases"][0]["add"] = []
    (tmp_path / "p.json").write_text(json.dumps(bad))
    assert _cli("validate", "--plan", str(tmp_path / "p.json")).returncode == 1
    (tmp_path / "x.json").write_text("{")
    assert _cli("validate", "--plan", str(tmp_path / "x.json")).returncode != 0


def test_document_matrix_is_in_sync():
    doc = (ROOT / "docs" / "ci_required_checks.md").read_text(encoding="utf-8")
    start = doc.index(rcp.MATRIX_BEGIN) + len(rcp.MATRIX_BEGIN) + 1
    assert doc[start:doc.index(rcp.MATRIX_END)] == rcp.matrix_markdown()


def test_document_states_every_phase_payload_context():
    doc = (ROOT / "docs" / "ci_required_checks.md").read_text(encoding="utf-8")
    plan = _plan()
    for phase in plan["phases"]:
        assert phase["name"].split(" ")[0] in doc
    for c in plan["checks"].values():
        assert c["context"] in doc


# ------------------------------------------------------------------- opzione B dell'owner: readiness separata
def test_lane_technical_readiness_is_required_from_f2_and_auto_merge_never():
    plan = _plan()
    states = rcp.validate(plan)
    assert "lane-readiness" in states[2] and "lane-readiness" in states[-1]
    contexts = {(plan["checks"][k]["context"], plan["checks"][k]["publisher"]) for st in states for k in st}
    assert ("pr-auto-merge", "pickfair-trusted-status") not in contexts
    assert {n["context"] for n in plan["never_required"]} == {td.AUTO_MERGE_CONTEXT}


def test_a_plan_that_requires_the_auto_merge_status_is_refused():
    plan = _plan()
    plan["checks"]["lane-auto-merge"] = {"context": "pr-auto-merge", "publisher": "pickfair-trusted-status",
                                         "control": "readiness"}
    plan["phases"][1]["add"].append("lane-auto-merge")
    with pytest.raises(rcp.PlanError, match="mai essere un required check"):
        rcp.validate(plan)


def test_owner_merge_pr_with_green_gates_satisfies_the_planned_required_checks():
    """Opzione B: una PR a merge owner con i gate verdi ha `pr-merge-readiness` success e `pr-auto-merge` failure;
    i required check sono soddisfatti, quindi l'owner la mergia a mano senza bypass."""
    plan, phase = _plan(), len(_plan()["phases"])
    runs, statuses = _green(plan, phase)
    statuses = [{"context": "pr-auto-merge", "state": "failure",
                 "creator": {"login": "pickfair-trusted-status[bot]"}}] + statuses
    assert rcp.evaluate(plan, phase, SHA, runs, statuses) == []


# --- Sol su #587: status di un altro SHA, ruleset che non protegge main

@pytest.mark.parametrize("phase", [1, 2, 3])
def test_green_lane_statuses_of_another_sha_do_not_count(phase):
    plan = _plan()
    runs, statuses = _green(plan, phase, sha=OTHER)          # tutti verdi, ma esportati per il commit OTHER
    runs = [dict(r, head_sha=SHA) for r in runs]
    reasons = rcp.evaluate(plan, phase, SHA, runs, statuses)
    assert reasons and all("assente sull'head" in r for r in reasons)


def test_status_without_url_does_not_count():
    plan = _plan()
    runs, statuses = _green(plan, 1)
    statuses = [{k: v for k, v in s.items() if k != "url"} for s in statuses]
    assert rcp.evaluate(plan, 1, SHA, runs, statuses)


@pytest.mark.parametrize("mutate, ok", [
    (lambda r: None, True),
    (lambda r: r["conditions"]["ref_name"]["exclude"].append("refs/heads/main"), False),
    (lambda r: r["conditions"]["ref_name"]["exclude"].append("refs/heads/*"), False),
    (lambda r: r["conditions"]["ref_name"]["exclude"].append("~DEFAULT_BRANCH"), False),
    (lambda r: r["conditions"]["ref_name"].update(include=["refs/heads/release"]), False),
    (lambda r: r["conditions"]["ref_name"].update(include=["refs/heads/main"]), True),
    (lambda r: r.update(target="tag"), False),
    (lambda r: r.pop("conditions"), False),
])
def test_diff_requires_the_ruleset_to_apply_to_main(mutate, ok):
    ruleset = _ruleset()
    mutate(ruleset)
    problems = rcp.diff(_plan(), ruleset)
    assert (not any("refs/heads/main" in p for p in problems)) is ok


# ------------------------------------------------------- Codex su #587: readiness required che si blocca da sola
def test_f2_payload_is_refused_while_the_readiness_self_block_is_open():
    """Con `pr-merge-readiness` required, il suo stesso `pending` rende la PR `BLOCKED` e `effective_merge_state_ok()`
    rifiuta `BLOCKED`: la readiness pubblicherebbe sempre failure. Finche' non e' gestito, il payload di F2 (e delle
    fasi successive, che lo includono) non si genera; F1 resta applicabile."""
    plan = _plan()
    assert any("BLOCKED" in b for b in plan["phases"][1]["blocked_by"])
    assert rcp.payload(plan, 1)["parameters"]["required_status_checks"]
    for phase in (2, 3):
        with pytest.raises(rcp.PlanError, match="bloccata"):
            rcp.payload(plan, phase)
    res = subprocess.run([sys.executable, str(SCRIPT), "payload", "--plan", str(PLAN_PATH), "--phase", "2"],
                         capture_output=True, text=True)
    assert res.returncode == 1 and "bloccata" in res.stderr


@pytest.mark.parametrize("bad", ["", "x", 3, [""], [3]])
def test_malformed_blocked_by_is_refused(bad):
    plan = _plan()
    plan["phases"][0]["blocked_by"] = bad
    with pytest.raises(rcp.PlanError, match="blocked_by"):
        rcp.validate(plan)
