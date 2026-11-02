"""PF-REVIEW-ATTESTATION v2 — test positivi e negativi del verificatore condiviso (Merge readiness e VPS).

Ogni caso negativo del contratto (#586 c.6582617372) deve dare "non PASS" con un motivo esplicito.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import pr_review_attestation as A  # noqa: E402

REPO = "zarbopiero963-droid/Pickfair-nogui"
PR, AUTHOR = 601, "zarbopiero963-droid"
BASE, C1, C2, HEAD = "b" * 40, "1" * 40, "2" * 40, "3" * 40
COMMITS = [C1, C2, HEAD]


def _att(reviewer="gpt-6.1-sol", frm=BASE, to=HEAD, verdict="PASS", run=11, attempt=1, **over):
    body = {"schema": A.SCHEMA, "repo": REPO, "pr": PR, "head_sha": to, "base_sha": BASE,
            "range": {"from": frm, "to": to}, "pr_author": AUTHOR, "reviewer": reviewer,
            "provider": "openrouter", "workflow_path": A.TRUSTED_REVIEW_WORKFLOWS.get(reviewer, "x"),
            "run_id": run, "run_attempt": attempt, "verdict": verdict, "blockers": [], "files_truncated": 0,
            "files_sent": 3, "diff_sha256": _diff(frm, to)}
    body.update(over)
    return json.dumps(body).encode()


MAIN_WF = "a" * 40           # commit di main da cui gira il workflow di review


def _diff(frm, to):
    return A.sha256_hex(f"{frm}...{to}".encode())


def _sig(raw, reviewer="gpt-6.1-sol", run=11, attempt=1, **over):
    s = dict(ok=True, job_workflow_ref=f"{REPO}/{A.TRUSTED_REVIEW_WORKFLOWS.get(reviewer, 'x')}@refs/heads/main",
             event_name="pull_request_target", run_id=str(run), run_attempt=str(attempt), issuer=A.OIDC_ISSUER,
             digest=A.sha256_hex(raw), workflow_sha=MAIN_WF)
    s.update(over)
    return A.Signed(**s)


def _pair(reviewer="gpt-6.1-sol", frm=BASE, to=HEAD, verdict="PASS", run=11, sig=None, **over):
    raw = _att(reviewer, frm, to, verdict, run, **over)
    return raw, _sig(raw, reviewer, run, **(sig or {}))


def _decide(pairs, **kw):
    kw.setdefault("is_trusted_workflow_commit", lambda sha: sha == MAIN_WF)
    kw.setdefault("range_diff_sha256", _diff)
    return A.decide(pairs, repository=REPO, pr=PR, pr_author=AUTHOR, head_sha=HEAD, base_sha=BASE,
                    commits=COMMITS, **kw)


def _both(**kw):
    return [_pair("gpt-6.1-sol", run=1, **kw), _pair("grok-4.7", run=2, **kw)]


def test_full_range_pass_by_both_reviewers_passes():
    d = _decide(_both())
    assert d.passed and d.reasons == [] and len(d.covered) == 2


def test_push_ranges_chained_to_the_head_pass():
    pairs = []
    for rev, run0 in (("gpt-6.1-sol", 10), ("grok-4.7", 20)):
        pairs += [_pair(rev, BASE, C1, run=run0), _pair(rev, C1, C2, run=run0 + 1), _pair(rev, C2, HEAD, run=run0 + 2)]
    assert _decide(pairs).passed


def test_blockers_on_a_range_later_fixed_by_a_pass_range_pass():
    pairs = [_pair("gpt-6.1-sol", BASE, C2, run=1), _pair("gpt-6.1-sol", C1, C2, "BLOCKERS", run=2),
             _pair("gpt-6.1-sol", C2, HEAD, run=3), _pair("grok-4.7", run=4)]
    assert _decide(pairs).passed


@pytest.mark.parametrize("verdict", ["BLOCKERS", "INCONCLUSIVE", "ERROR"])
def test_later_non_pass_on_the_same_head_overrides_an_earlier_pass(verdict):
    # Sol su #602: PASS e poi BLOCKERS sullo stesso range (run successivo) => il PASS vecchio non copre piu' l'head
    pairs = [_pair("gpt-6.1-sol", run=1), _pair("gpt-6.1-sol", verdict=verdict, run=2), _pair("grok-4.7", run=3)]
    d = _decide(pairs)
    assert not d.passed and "OPEN_BLOCKERS:gpt-6.1-sol" in d.reasons


def test_later_attempt_blockers_overrides_an_earlier_attempt_pass_of_the_same_run():
    pairs = [_pair("gpt-6.1-sol", run=5), _pair("gpt-6.1-sol", verdict="BLOCKERS", run=5, attempt=2,
                                                  sig={"run_attempt": "2"}), _pair("grok-4.7", run=6)]
    d = _decide(pairs)
    assert not d.passed and "OPEN_BLOCKERS:gpt-6.1-sol" in d.reasons


def test_pass_after_blockers_on_the_same_head_clears_them():
    pairs = [_pair("gpt-6.1-sol", verdict="BLOCKERS", run=1), _pair("gpt-6.1-sol", run=2), _pair("grok-4.7", run=3)]
    assert _decide(pairs).passed


def test_blockers_with_unreadable_run_order_never_clear():
    pairs = [_pair("gpt-6.1-sol", verdict="BLOCKERS", run="x", sig={"run_id": "x"}), _pair("gpt-6.1-sol", run=2),
             _pair("grok-4.7", run=3)]
    d = _decide(pairs)
    assert not d.passed and "OPEN_BLOCKERS:gpt-6.1-sol" in d.reasons


@pytest.mark.parametrize("verdict", ["BLOCKERS", "INCONCLUSIVE", "ERROR"])
def test_non_pass_verdict_on_the_last_range_never_passes(verdict):
    pairs = [_pair("gpt-6.1-sol", BASE, C2, run=1), _pair("gpt-6.1-sol", C2, HEAD, verdict, run=2),
             _pair("grok-4.7", run=3)]
    d = _decide(pairs)
    assert not d.passed and "NOT_COVERED_TO_HEAD:gpt-6.1-sol" in d.reasons


def test_old_sha_after_a_new_push_does_not_cover_the_head():
    pairs = [_pair("gpt-6.1-sol", BASE, C2, run=1), _pair("grok-4.7", BASE, C2, run=2)]
    d = _decide(pairs)
    assert not d.passed and "NOT_COVERED_TO_HEAD:gpt-6.1-sol" in d.reasons


def test_gap_in_the_chain_is_not_coverage():
    pairs = [_pair("gpt-6.1-sol", BASE, C1, run=1), _pair("gpt-6.1-sol", C2, HEAD, run=2), _pair("grok-4.7", run=3)]
    assert not _decide(pairs).passed


def test_missing_required_reviewer_never_passes():
    d = _decide([_pair("gpt-6.1-sol")])
    assert not d.passed and "NOT_COVERED_TO_HEAD:grok-4.7" in d.reasons


@pytest.mark.parametrize("sig,reason", [
    ({"ok": False, "reason": "no attestation"}, "SIGNATURE_INVALID"),
    ({"issuer": "https://evil"}, "SIGNATURE_WRONG_ISSUER"),
    ({"digest": "0" * 64}, "DIGEST_MISMATCH"),
    ({"event_name": "pull_request"}, "UNTRUSTED_EVENT"),
    ({"job_workflow_ref": f"{REPO}/.github/workflows/pr-review-openrouter-gpt56-sol.yml@refs/pull/601/merge"},
     "UNTRUSTED_WORKFLOW"),
    ({"job_workflow_ref": f"{REPO}/.github/workflows/evil.yml@refs/heads/main"}, "UNTRUSTED_WORKFLOW"),
    ({"job_workflow_ref": f"someone/fork/.github/workflows/pr-review-openrouter-gpt56-sol.yml@refs/heads/main"},
     "UNTRUSTED_WORKFLOW"),
    ({"run_id": "999"}, "RUN_MISMATCH"),
])
def test_forged_or_untrusted_signatures_never_count(sig, reason):
    pairs = [_pair("gpt-6.1-sol", run=1, sig=sig), _pair("grok-4.7", run=2)]
    d = _decide(pairs)
    assert not d.passed and any(r.startswith(reason) for r in d.reasons), d.reasons


@pytest.mark.parametrize("over,reason", [
    ({"pr": 999}, "WRONG_PR"),                                # replay su un'altra PR
    ({"repo": "x/y"}, "WRONG_PR"),
    ({"pr_author": "someone-else"}, "AUTHOR_MISMATCH"),
    ({"verdict": "MAYBE"}, "VERDICT_UNKNOWN"),
    ({"schema": "v0"}, "MALFORMED"),
    ({"reviewer": "self-review"}, "REVIEWER_NOT_TRUSTED"),   # autore o reviewer non autorizzato
    ({"workflow_path": ".github/workflows/other.yml"}, "WORKFLOW_PATH_MISMATCH"),
    ({"head_sha": "abc"}, "MALFORMED_RANGE"),
])
def test_tampered_content_never_counts(over, reason):
    over = dict(over)
    reviewer = over.pop("reviewer", "gpt-6.1-sol")
    raw = _att(reviewer, run=1, **over)
    pairs = [(raw, _sig(raw, "gpt-6.1-sol", 1)), _pair("grok-4.7", run=2)]
    d = _decide(pairs)
    assert not d.passed and reason in d.reasons, d.reasons


def test_marker_without_attestation_or_garbage_bytes_never_count():
    raw = b'<!-- gpt56sol-pr-review-done:pickfair-nogui:aaa...bbb --> VALID_PASS'
    d = _decide([(raw, _sig(raw, run=1)), _pair("grok-4.7", run=2)])
    assert not d.passed and "MALFORMED" in d.reasons


def test_range_outside_the_pr_commits_never_counts():
    other = "9" * 40
    d = _decide([_pair("gpt-6.1-sol", other, HEAD, run=1), _pair("grok-4.7", run=2)])
    assert not d.passed and "RANGE_NOT_IN_PR:gpt-6.1-sol" in d.reasons


def test_head_not_matching_the_commit_chain_never_passes():
    d = A.decide(_both(), repository=REPO, pr=PR, pr_author=AUTHOR, head_sha="4" * 40, base_sha=BASE, commits=COMMITS,
                 is_trusted_workflow_commit=lambda sha: True, range_diff_sha256=_diff)
    assert not d.passed and d.reasons == ["HEAD_NOT_IN_COMMITS"]


def test_re_evaluation_of_the_same_attestations_is_idempotent():
    pairs = _both()
    assert _decide(pairs).passed and _decide(pairs).passed


# ── adattatore gh ───────────────────────────────────────────────────────────────────────────────────────────────────

def _gh_json(digest, *, trigger="pull_request_target", signer=None):
    signer = signer or f"https://github.com/{REPO}/.github/workflows/pr-review-openrouter-gpt56-sol.yml@refs/heads/main"
    return json.dumps([{"verificationResult": {
        "signature": {"certificate": {"buildSignerURI": signer, "githubWorkflowTrigger": trigger,
                                      "buildSignerDigest": MAIN_WF,
                                      "runInvocationURI": f"https://github.com/{REPO}/actions/runs/11/attempts/1",
                                      "issuer": A.OIDC_ISSUER}},
        "statement": {"subject": [{"digest": {"sha256": digest}}]}}}])


def test_gh_adapter_builds_the_strict_command_and_parses_the_certificate():
    calls = []
    raw = _att(run=11)

    def run(argv):
        calls.append(argv)
        return 0, _gh_json(A.sha256_hex(raw))
    signed = A.gh_signature_verifier(run, repository=REPO)("f.json", ".github/workflows/pr-review-openrouter-gpt56-sol.yml")
    argv = calls[0]
    assert argv[:3] == ["gh", "attestation", "verify"]
    assert "--signer-workflow" in argv and argv[argv.index("--source-ref") + 1] == "refs/heads/main"
    assert signed.ok and signed.run_id == "11" and signed.run_attempt == "1" and signed.event_name == "pull_request_target"
    att, why = A.check_attestation(raw, signed, repository=REPO, pr=PR, pr_author=AUTHOR,
                                   is_trusted_workflow_commit=lambda sha: sha == MAIN_WF, range_diff_sha256=_diff)
    assert why == "" and att["verdict"] == "PASS"


@pytest.mark.parametrize("result", [(1, ""), (0, "not json"), (0, "[]"), (0, json.dumps([{"x": 1}]))])
def test_gh_adapter_failures_are_unverified(result):
    signed = A.gh_signature_verifier(lambda argv: result, repository=REPO)("f.json", "w.yml")
    assert signed.ok is False


def test_gh_adapter_exception_is_unverified():
    def boom(argv):
        raise OSError("gh missing")
    assert A.gh_signature_verifier(boom, repository=REPO)("f.json", "w.yml").ok is False


# ── owner #586: firma autentica != review corretta o indipendente ───────────────────────────────────────────────────

def test_valid_signature_from_a_workflow_version_not_on_main_never_counts():
    d = _decide(_both(), is_trusted_workflow_commit=lambda sha: False)
    assert not d.passed and "WORKFLOW_COMMIT_NOT_ON_MAIN" in d.reasons


@pytest.mark.parametrize("kw", [{"is_trusted_workflow_commit": None}, {"range_diff_sha256": None},
                                {"range_diff_sha256": lambda a, b: None}])
def test_unverifiable_workflow_commit_or_diff_never_counts(kw):
    d = _decide(_both(), **kw)
    assert not d.passed and any(r in d.reasons for r in ("WORKFLOW_COMMIT_UNVERIFIABLE", "DIFF_UNVERIFIABLE"))


def test_workflow_commit_lookup_error_never_counts():
    def boom(sha):
        raise RuntimeError("API down")
    assert not _decide(_both(), is_trusted_workflow_commit=boom).passed


@pytest.mark.parametrize("over,reason", [({"files_sent": 0}, "NOTHING_REVIEWED"),
                                         ({"files_truncated": 2}, "DIFF_TRUNCATED"),
                                         ({"diff_sha256": "f" * 64}, "DIFF_MISMATCH")])
def test_reviewer_that_did_not_see_the_whole_diff_never_counts(over, reason):
    raw = _att("gpt-6.1-sol", run=1, **over)
    d = _decide([(raw, _sig(raw, "gpt-6.1-sol", 1)), _pair("grok-4.7", run=2)])
    assert not d.passed and reason in d.reasons, d.reasons


def test_missing_workflow_sha_in_the_certificate_never_counts():
    raw = _att("gpt-6.1-sol", run=1)
    d = _decide([(raw, _sig(raw, "gpt-6.1-sol", 1, workflow_sha="")), _pair("grok-4.7", run=2)])
    assert not d.passed and "WORKFLOW_COMMIT_UNVERIFIABLE" in d.reasons


# ── i workflow di review producono e firmano l'attestazione (parita' con la libreria) ─────────────────────────────

ROOT = Path(__file__).resolve().parents[2]


def _workflow_python(path):
    import yaml
    w = yaml.safe_load((ROOT / path).read_text(encoding="utf-8"))
    run = w["jobs"]["review"]["steps"][0]["run"]
    return w, run.split("python3 <<'PY'\n", 1)[1].rsplit("\nPY", 1)[0]


@pytest.mark.parametrize("reviewer,path", sorted(A.TRUSTED_REVIEW_WORKFLOWS.items()))
def test_review_workflow_signs_and_uploads_the_attestation(reviewer, path):
    w, py = _workflow_python(path)
    assert w["permissions"]["id-token"] == "write" and w["permissions"]["attestations"] == "write"
    names = [s.get("name") for s in w["jobs"]["review"]["steps"]]
    assert names[1:] == ["Sign review attestation", "Upload review attestation"]
    sign = w["jobs"]["review"]["steps"][1]
    assert sign["uses"].startswith("actions/attest-build-provenance@") and sign["with"]["subject-path"] == \
        "review-attestation.json"
    assert f'"reviewer": "{reviewer}"' in py and f'"workflow_path": "{path}"' in py
    assert '"verdict": _verdicts.get(attempt_state, "ERROR")' in py, "verdetto non dalla classificazione"
    trigger = w.get("on", w.get(True))
    assert list(trigger) == ["pull_request_target"], "il workflow firmatario deve girare dal base"


@pytest.mark.parametrize("path", sorted(A.TRUSTED_REVIEW_WORKFLOWS.values()))
def test_workflow_digest_function_matches_the_library(path):
    import ast
    _w, py = _workflow_python(path)
    fn = next(n for n in ast.parse(py).body if isinstance(n, ast.FunctionDef) and n.name == "canonical_range_digest")
    ns = {"json": json, "_hl": __import__("hashlib")}
    exec(compile(ast.Module(body=[fn], type_ignores=[]), path, "exec"), ns)
    files = [{"filename": "b.py", "patch": "@@ -1 +1 @@\n-x\n+y"}, {"filename": "a.py", "patch": "+é"},
             {"filename": "bin.png"}, "garbage"]
    assert ns["canonical_range_digest"](files) == A.canonical_range_digest(files)


# ── integrazione GitHub (HTTP finto) ────────────────────────────────────────────────────────────────────────────────

import io
import zipfile


def _zip(raw):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(A.ATTESTATION_FILE, raw)
    return buf.getvalue()


class _API:
    def __init__(self, *, fork=False, on_main=True, digest_ok=True, prelive=("completed", "success"), fail=None):
        self.raws = {1: _att("gpt-6.1-sol", run=1), 2: _att("grok-4.7", run=2)}
        self.fork, self.on_main, self.digest_ok, self.prelive, self.fail = fork, on_main, digest_ok, prelive, fail

    def __call__(self, url):
        if self.fail and self.fail in url:
            return 500, b""
        p = url.split(f"/repos/{REPO}", 1)[1]
        if p == f"/pulls/{PR}":
            body = {"head": {"sha": HEAD, "repo": {"full_name": "fork/x" if self.fork else REPO}},
                    "base": {"sha": BASE}, "user": {"login": AUTHOR}}
        elif p.startswith(f"/pulls/{PR}/commits"):
            body = [{"sha": s} for s in COMMITS]
        elif p.startswith("/actions/workflows/"):
            file = p.split("/")[3]
            run = 1 if "gpt56" in file else 2
            body = {"workflow_runs": [{"id": run, "path": f".github/workflows/{file}", "status": "completed",
                                       "pull_requests": [{"number": PR}]},
                                      {"id": 99, "path": f".github/workflows/{file}", "status": "completed",
                                       "pull_requests": [{"number": 7}]}]}
        elif p.startswith("/actions/runs/") and p.endswith("/artifacts?per_page=100"):
            rid = int(p.split("/")[3])
            body = {"artifacts": [{"id": 1000 + rid, "name": "review-attestation-1", "expired": False}]}
        elif p.startswith("/actions/artifacts/"):
            return 200, _zip(self.raws[int(p.split("/")[3]) - 1000])
        elif p.startswith(f"/compare/{MAIN_WF}...main"):
            body = {"status": "ahead" if self.on_main else "diverged"}
        elif p.startswith("/compare/"):
            frm, to = p.split("/compare/")[1].split("...")
            body = {"files": [{"filename": "x", "patch": "+1"}]}
            self.last_range = (frm, to)
        elif p.startswith(f"/commits/{HEAD}/check-runs"):
            st, concl = self.prelive
            body = {"check_runs": [{"id": 5, "name": A.PRELIVE_CHECK, "app": {"slug": A.PRELIVE_APP}, "head_sha": HEAD,
                                    "status": st, "conclusion": concl, "completed_at": "2026-11-02T17:00:00Z"},
                                   {"id": 6, "name": "pf-betfair-live-certification", "app": {"slug": A.PRELIVE_APP},
                                    "head_sha": HEAD, "status": "completed", "conclusion": "neutral"}]}
        else:
            return 404, b""
        return 200, json.dumps(body).encode()


def _verify(api):
    gh = A.GitHub(REPO, "t", http=api)
    # firma "valida" dello stub; il digest canonico del range finto e' quello di [["x","+1"]]
    canon = A.canonical_range_digest([{"filename": "x", "patch": "+1"}])
    for k, raw in list(api.raws.items()):
        body = json.loads(raw)
        body["diff_sha256"] = canon if api.digest_ok else "0" * 64
        api.raws[k] = json.dumps(body).encode()
    reviewer_of = {1: "gpt-6.1-sol", 2: "grok-4.7"}
    return A.verify_pr(gh, PR, lambda raw, path: _sig(raw, json.loads(raw)["reviewer"], json.loads(raw)["run_id"]))


def test_verify_pr_collects_signed_attestations_of_this_pr_only():
    out = _verify(_API())
    assert out["passed"] and out["attestations"] == 2, out


def test_verify_pr_refuses_a_fork():
    assert _verify(_API(fork=True))["reasons"] == ["FORK_PR"]


def test_verify_pr_refuses_a_workflow_commit_not_on_main():
    out = _verify(_API(on_main=False))
    assert not out["passed"] and "WORKFLOW_COMMIT_NOT_ON_MAIN" in out["reasons"]


def test_verify_pr_refuses_a_diff_the_reviewer_did_not_see():
    out = _verify(_API(digest_ok=False))
    assert not out["passed"] and "DIFF_MISMATCH" in out["reasons"]


@pytest.mark.parametrize("fail", ["/pulls/601", "/actions/workflows/", "/artifacts"])
def test_verify_pr_api_errors_are_never_pass(fail):
    out = _verify(_API(fail=fail))
    assert not out["passed"]


def test_prelive_success_on_the_exact_head_passes_and_live_neutral_is_ignored():
    out = A.verify_prelive(A.GitHub(REPO, "t", http=_API()), HEAD)
    assert out["passed"] and out["check_run_id"] == 5


@pytest.mark.parametrize("state,reason", [(("completed", "failure"), "PRELIVE_FAILURE"),
                                          (("completed", "action_required"), "PRELIVE_ACTION_REQUIRED"),
                                          (("completed", "neutral"), "PRELIVE_NEUTRAL"),
                                          (("in_progress", None), "PRELIVE_PENDING")])
def test_prelive_not_success_never_passes(state, reason):
    out = A.verify_prelive(A.GitHub(REPO, "t", http=_API(prelive=state)), HEAD)
    assert not out["passed"] and out["reason"] == reason


def test_prelive_missing_or_from_another_app_never_passes():
    class Other(_API):
        def __call__(self, url):
            status, body = super().__call__(url)
            if "/check-runs" in url:
                d = json.loads(body)
                for r in d["check_runs"]:
                    r["app"]["slug"] = "evil-app"
                body = json.dumps(d).encode()
            return status, body
    assert A.verify_prelive(A.GitHub(REPO, "t", http=Other()), HEAD)["reason"] == "PRELIVE_MISSING"


def test_live_neutral_alone_can_never_satisfy_prelive():
    # PRE-LIVE assente, solo il LIVE neutral sull'head: mai PASS
    class OnlyLive(_API):
        def __call__(self, url):
            status, body = super().__call__(url)
            if "/check-runs" in url:
                d = json.loads(body)
                d["check_runs"] = [r for r in d["check_runs"] if r["name"] != A.PRELIVE_CHECK]
                body = json.dumps(d).encode()
            return status, body
    assert A.verify_prelive(A.GitHub(REPO, "t", http=OnlyLive()), HEAD)["reason"] == "PRELIVE_MISSING"
