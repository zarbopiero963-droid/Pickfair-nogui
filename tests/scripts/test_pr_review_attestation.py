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
            "files_sent": 3, "diff_sha256": _diff(frm, to), "generated_at": _ts(run, attempt)}
    body.update(over)
    return json.dumps(body).encode()


def _ts(run, attempt=1):
    """Istante di emissione di default: cresce con run e tentativo (i test che incrociano i run lo fissano a mano)."""
    try:
        sec = int(run) * 60 + int(attempt)
    except (TypeError, ValueError):
        return "non-una-data"
    return f"2026-11-02T{sec // 3600:02d}:{sec % 3600 // 60:02d}:{sec % 60:02d}Z"


MAIN_WF = "a" * 40           # commit di main da cui gira il workflow di review


def _diff(frm, to, base=None):
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


def test_rerun_of_an_older_run_with_blockers_after_a_newer_pass_is_still_open():
    # Sol su #602 (185058f): il rerun conserva il run_id; conta quando la review e' stata EMESSA, non il numero del run
    pairs = [_pair("gpt-6.1-sol", run=11, generated_at="2026-11-02T10:00:00Z"),
             _pair("gpt-6.1-sol", verdict="BLOCKERS", run=10, attempt=2, sig={"run_attempt": "2"},
                   generated_at="2026-11-02T10:30:00Z"),
             _pair("grok-4.7", run=12)]
    d = _decide(pairs)
    assert not d.passed and "OPEN_BLOCKERS:gpt-6.1-sol" in d.reasons


def test_pass_emitted_in_the_same_second_as_blockers_does_not_clear_them():
    t = "2026-11-02T10:00:00Z"
    pairs = [_pair("gpt-6.1-sol", verdict="BLOCKERS", run=1, generated_at=t), _pair("gpt-6.1-sol", run=2, generated_at=t),
             _pair("grok-4.7", run=3)]
    assert not _decide(pairs).passed


def test_later_partial_pass_on_the_same_head_does_not_clear_blockers_on_the_whole_range():
    # Grok su #602 (185058f): PASS intero, poi BLOCKERS intero, poi PASS solo sull'ultimo pezzo dello STESSO head
    pairs = [_pair("gpt-6.1-sol", BASE, HEAD, run=1), _pair("gpt-6.1-sol", BASE, HEAD, "BLOCKERS", run=2),
             _pair("gpt-6.1-sol", C2, HEAD, run=3), _pair("grok-4.7", run=4)]
    d = _decide(pairs)
    assert not d.passed and "OPEN_BLOCKERS:gpt-6.1-sol" in d.reasons


def test_later_pass_that_re_covers_the_blocked_range_clears_it():
    pairs = [_pair("gpt-6.1-sol", BASE, HEAD, "BLOCKERS", run=1), _pair("gpt-6.1-sol", C1, HEAD, run=2),
             _pair("gpt-6.1-sol", BASE, C1, run=3), _pair("gpt-6.1-sol", BASE, HEAD, run=4), _pair("grok-4.7", run=5)]
    assert _decide(pairs).passed


@pytest.mark.parametrize("over", [{"files_truncated": 1}, {"files_sent": 0}, {"diff_sha256": "0" * 64}])
@pytest.mark.parametrize("verdict", ["BLOCKERS", "INCONCLUSIVE", "ERROR"])
def test_authentic_non_pass_counts_even_when_its_coverage_would_not_qualify_for_pass(verdict, over):
    # Codex su #602 (7f3e48f): un BLOCKERS autentico con diff troncato non deve sparire davanti a una copertura vecchia
    pairs = [_pair("gpt-6.1-sol", run=1), _pair("gpt-6.1-sol", verdict=verdict, run=2, **over), _pair("grok-4.7", run=3)]
    d = _decide(pairs)
    assert not d.passed and "OPEN_BLOCKERS:gpt-6.1-sol" in d.reasons


@pytest.mark.parametrize("over", [{"files_truncated": 1}, {"files_sent": 0}, {"diff_sha256": "0" * 64}])
def test_a_pass_with_incomplete_coverage_still_never_counts(over):
    d = _decide([_pair("gpt-6.1-sol", run=1, **over), _pair("grok-4.7", run=2)])
    assert not d.passed and "NOT_COVERED_TO_HEAD:gpt-6.1-sol" in d.reasons


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
                                {"range_diff_sha256": lambda a, b, c: None}])
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
    assert '"verdict": _verdicts.get(state, "ERROR")' in py, "verdetto non dalla classificazione"
    assert 'write_attestation(attempt_state, time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))' in py, \
        "formato letto da _emitted_at"
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


# ── rilievi Codex/Sol su #602 ────────────────────────────────────────────────────────────────────────────────────────

OLD_BASE = "c" * 40


def test_base_advanced_after_the_review_keeps_the_signed_initial_range():
    # main avanza senza toccare la PR: la base corrente e' NEW, i range firmati partono dalla base di allora
    pairs = [_pair("gpt-6.1-sol", OLD_BASE, HEAD, run=1, base_sha=OLD_BASE),
             _pair("grok-4.7", OLD_BASE, HEAD, run=2, base_sha=OLD_BASE)]
    assert _decide(pairs).passed


def test_a_range_from_an_unknown_sha_that_is_not_its_signed_base_never_counts():
    pairs = [_pair("gpt-6.1-sol", OLD_BASE, HEAD, run=1), _pair("grok-4.7", run=2)]   # base_sha firmata = BASE
    d = _decide(pairs)
    assert not d.passed and "RANGE_NOT_IN_PR:gpt-6.1-sol" in d.reasons


def test_digest_is_recomputed_on_the_signed_base():
    seen = []
    pairs = [_pair("gpt-6.1-sol", C2, HEAD, run=1, base_sha=OLD_BASE)]
    A.decide(pairs, repository=REPO, pr=PR, pr_author=AUTHOR, head_sha=HEAD, base_sha=BASE, commits=COMMITS,
             is_trusted_workflow_commit=lambda sha: True,
             range_diff_sha256=lambda f, t, b: seen.append(b) or _diff(f, t))
    assert seen == [OLD_BASE]


def _workflow_filter(path):
    import ast
    _w, py = _workflow_python(path)
    fn = next(n for n in ast.parse(py).body if isinstance(n, ast.FunctionDef) and n.name == "solo_file_della_pr")
    return fn


def _run_workflow_filter(path, push, own, frm, base, own_error=False):
    import ast
    import re as _re
    ns = {"BASE_SHA": base, "REVIEW_ID": "t", "redact": str, "print": lambda *a, **k: None,
          "good_sha": lambda x: bool(_re.fullmatch(r"[0-9a-f]{40}", x or ""))}

    def compare_range(b, h):
        if own_error:
            raise RuntimeError("api")
        return {"files": own}
    ns["compare_range"] = compare_range
    exec(compile(ast.Module(body=[_workflow_filter(path)], type_ignores=[]), path, "exec"), ns)
    return ns["solo_file_della_pr"](push, frm, HEAD)[0]


_PUSH_MERGE = [{"filename": "mine.py", "patch": "+mine and main lines"}, {"filename": "from_main.py", "patch": "+m"},
               {"filename": "renamed.py", "previous_filename": "old.py", "patch": "+r"}]
_OWN = [{"filename": "mine.py", "patch": "+mine only"}, {"filename": "renamed.py", "previous_filename": "old.py"}]


@pytest.mark.parametrize("path", sorted(A.TRUSTED_REVIEW_WORKFLOWS.values()))
@pytest.mark.parametrize("push,own,frm,own_error", [
    (_PUSH_MERGE, _OWN, C1, False),                                    # push che porta dentro main: filtro + patch vs base
    (_PUSH_MERGE[:1], _OWN, C1, False),                                # push normale: invariato
    (_PUSH_MERGE, _OWN, BASE, False),                                  # range dalla base: invariato
    (_PUSH_MERGE, None, C1, True),                                     # file propri non calcolabili: invariato
    ([{"filename": f"f{i}", "patch": "+"} for i in range(300)], _OWN, C1, False),   # cap 300: invariato
    (_PUSH_MERGE, [{"filename": f"f{i}"} for i in range(300)], C1, False),          # propri al cap: invariato
])
def test_range_normalization_matches_the_review_workflow(path, push, own, frm, own_error):
    expected = _run_workflow_filter(path, push, own, frm, BASE, own_error)
    assert A.pr_range_files(push, None if own_error else own, frm, BASE) == expected


def test_merge_push_digest_on_the_verifier_matches_what_the_reviewer_signed():
    class Merge(_API):
        def __call__(self, url):
            if f"/compare/{C1}...{HEAD}" in url:
                return 200, json.dumps({"files": _PUSH_MERGE}).encode()
            if f"/compare/{BASE}...{HEAD}" in url:
                return 200, json.dumps({"files": _OWN}).encode()
            return super().__call__(url)
    gh = A.GitHub(REPO, "t", http=Merge())
    signed_by_workflow = A.canonical_range_digest(
        _run_workflow_filter(A.TRUSTED_REVIEW_WORKFLOWS["gpt-6.1-sol"], _PUSH_MERGE, _OWN, C1, BASE))
    assert gh.range_digest(C1, HEAD, BASE) == signed_by_workflow
    assert signed_by_workflow != A.canonical_range_digest(_PUSH_MERGE)   # senza normalizzazione: DIFF_MISMATCH


def _runs_api(pages, pr_page):
    def http(url):
        page = int(url.rsplit("page=", 1)[1])
        rows = [{"id": page * 1000 + i, "path": "x", "status": "completed", "pull_requests": [{"number": 7}],
                 "created_at": f"2026-11-{30 - page:02d}T00:00:00Z"} for i in range(100 if page < pages else 3)]
        if page == pr_page:
            rows[0] = {"id": 42, "path": "x", "status": "completed", "pull_requests": [{"number": PR}],
                       "created_at": rows[0]["created_at"]}
        return 200, json.dumps({"workflow_runs": rows}).encode()
    return A.GitHub(REPO, "t", http=http)


def test_review_runs_paginate_past_the_first_hundred():
    runs = _runs_api(pages=3, pr_page=2).review_runs("x", PR)
    assert [r["id"] for r in runs] == [42]


def test_review_runs_stop_at_runs_older_than_the_pr():
    calls = []
    gh = _runs_api(pages=10, pr_page=1)
    http = gh.http
    gh.http = lambda url: calls.append(url) or http(url)
    gh.review_runs("x", PR, since="2026-11-28T12:00:00Z")      # pagina 2 = 28/11 00:00, gia' prima della PR
    assert len(calls) == 2


def test_review_runs_beyond_the_page_cap_are_unverifiable_not_partial():
    with pytest.raises(RuntimeError):
        _runs_api(pages=50, pr_page=1).review_runs("x", PR, max_pages=3)


class _Seq:
    def __init__(self, results):
        self.results, self.calls, self.now = list(results), 0, 0.0

    def verify(self, *_a):
        self.calls += 1
        return self.results.pop(0) if len(self.results) > 1 else self.results[0]

    def sleep(self, s):
        self.now += s


@pytest.fixture
def seq(monkeypatch):
    def make(results):
        q = _Seq(results)
        monkeypatch.setattr(A, "verify_pr", q.verify)
        return q
    return make


NOT_COV = {"passed": False, "reasons": ["NOT_COVERED_TO_HEAD:grok-4.7"]}


def test_readiness_waits_while_reviews_are_still_running(seq):
    q = seq([NOT_COV, NOT_COV, {"passed": True, "reasons": []}])
    out = A.verify_pr_waiting(None, PR, None, wait_seconds=600, interval=60, sleep=q.sleep, clock=lambda: q.now)
    assert out["passed"] and q.calls == 3


@pytest.mark.parametrize("bad", [["OPEN_BLOCKERS:gpt-6.1-sol"], ["GITHUB_API_UNAVAILABLE:RuntimeError"], ["FORK_PR"],
                                 ["NOT_COVERED_TO_HEAD:grok-4.7", "OPEN_BLOCKERS:gpt-6.1-sol"]])
def test_readiness_does_not_wait_out_blockers_or_errors(seq, bad):
    q = seq([{"passed": False, "reasons": bad}])
    out = A.verify_pr_waiting(None, PR, None, wait_seconds=600, sleep=q.sleep, clock=lambda: q.now)
    assert not out["passed"] and q.calls == 1


def test_readiness_wait_is_bounded(seq):
    q = seq([NOT_COV])
    out = A.verify_pr_waiting(None, PR, None, wait_seconds=300, interval=60, sleep=q.sleep, clock=lambda: q.now)
    assert not out["passed"] and q.calls == 6 and q.now <= 300


def test_readiness_workflow_waits_for_the_reviews():
    text = (Path(__file__).resolve().parents[2] / ".github/workflows/pr-merge-readiness.yml").read_text()
    assert "verify-pr --repo \"$REPO\" --pr \"$PR\" --wait-seconds 1800" in text


def test_review_runs_keep_earlier_ranges_after_github_empties_pull_requests():
    # osservato su #601: dopo i push successivi i run vecchi hanno pull_requests == []
    rows = [{"id": 1, "path": "x", "status": "completed", "pull_requests": [], "head_branch": "feat"},
            {"id": 2, "path": "x", "status": "completed", "pull_requests": [], "head_branch": "other"},
            {"id": 3, "path": "x", "status": "in_progress", "pull_requests": [], "head_branch": "feat"}]
    gh = A.GitHub(REPO, "t", http=lambda url: (200, json.dumps({"workflow_runs": rows}).encode()))
    assert [r["id"] for r in gh.review_runs("x", PR, head_ref="feat")] == [1]
    assert gh.review_runs("x", PR) == []


def test_an_attestation_of_another_pr_on_the_same_branch_never_counts():
    pairs = [_pair("gpt-6.1-sol", run=1, pr=PR + 1), _pair("grok-4.7", run=2)]
    d = _decide(pairs)
    assert not d.passed and "WRONG_PR" in d.reasons


@pytest.mark.parametrize("sig", [{"ok": False, "reason": "VERIFIER_ERROR:TimeoutExpired"}, {"digest": "0" * 64},
                                 {"event_name": "pull_request"}, {"workflow_sha": "f" * 40}])
def test_an_unverifiable_artifact_blocks_even_with_valid_older_coverage(sig):
    # Codex P1 su #602 (238f78f): un artefatto dei workflow trusted che non si verifica potrebbe essere un BLOCKERS
    pairs = [_pair("gpt-6.1-sol", run=1), _pair("gpt-6.1-sol", verdict="BLOCKERS", run=2, sig=sig),
             _pair("grok-4.7", run=3)]
    d = _decide(pairs)
    assert not d.passed and "UNVERIFIED_ATTESTATION" in d.reasons, d.reasons


@pytest.mark.parametrize("over", [{"pr": PR + 1}, {"files_truncated": 1}, {"diff_sha256": "0" * 64}])
def test_authentic_artifacts_that_simply_do_not_apply_do_not_block(over):
    # di un'altra PR sullo stesso branch, o un PASS incompleto che semplicemente non copre
    pairs = [_pair("gpt-6.1-sol", run=1), _pair("gpt-6.1-sol", run=2, **over), _pair("grok-4.7", run=3)]
    assert _decide(pairs).passed


def test_readiness_does_not_wait_out_an_unverified_attestation():
    assert not A._still_waiting({"passed": False, "reasons": ["NOT_COVERED_TO_HEAD:x", "UNVERIFIED_ATTESTATION"]})


# ── Codex P2 su #602: un rerun dopo firma/upload falliti ri-emette l'attestazione senza ripagare il modello ─────────

def _attestation_fns(path, tmp_path, comments, states_header=None, run_artifacts=()):
    import ast
    import re as _re
    _w, py = _workflow_python(path)
    tree = ast.parse(py)
    names = {"canonical_range_digest", "write_attestation", "reemit_from_done_review", "rr_attempts", "rr_coverage_gaps",
             "dedup_exit", "run_has_attestation"}
    fns = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    assigns = [n for n in tree.body if isinstance(n, ast.Assign) and any(
        isinstance(t, ast.Name) and (t.id.startswith("RR_") or t.id.startswith("_RR_")) for t in n.targets)]
    import hashlib
    import os as _os
    ns = {"json": json, "re": _re, "os": _os, "_hl": hashlib, "sys": sys, "REPO": REPO, "PR_NUMBER": str(PR), "REVIEW_ID": "x",
          "range_base": C2, "range_head": HEAD, "BASE_SHA": BASE, "marker": "<!-- m -->",
          "done_marker": "<!-- done -->", "files": [{"filename": "a.py", "patch": "+1"}],
          "diff_text": "\n\n### FILE: a.py\n+1", "compare_truncated": False, "print": lambda *a, **k: None,
          "gh_request": lambda method, url: ({"artifacts": [{"name": n} for n in run_artifacts]}
                                             if "/artifacts" in url else comments)}
    exec(compile(ast.Module(body=assigns + fns, type_ignores=[]), path, "exec"), ns)
    return ns


@pytest.mark.parametrize("reviewer,path", sorted(A.TRUSTED_REVIEW_WORKFLOWS.items()))
@pytest.mark.parametrize("state,verdict", [("VALID_BLOCKERS", "BLOCKERS"), ("VALID_INCONCLUSIVE", "INCONCLUSIVE")])
def test_rerun_of_a_reviewed_range_re_emits_the_signed_verdict_without_a_model_call(reviewer, path, tmp_path,
                                                                                     monkeypatch, state, verdict):
    rid = "gpt56sol" if "sol" in path else "grok46"
    head = (f"<!-- {rid}-pr-review:pickfair-nogui:{C2[:12]}...{HEAD[:12]} --><!-- done -->"
            f"<!-- {rid}-pr-review-attempt:pickfair-nogui:{C2[:12]}...{HEAD[:12]}:1:{state} -->\n# review")
    comments = [{"user": {"login": "someone"}, "body": "<!-- done --> forged", "updated_at": "2026-11-02T09:00:00Z"},
                {"user": {"login": "github-actions[bot]"}, "body": head, "updated_at": "2026-11-02T10:11:12Z"}]
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_RUN_ID", "77")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "2")
    monkeypatch.setenv("GITHUB_WORKSPACE", str(tmp_path))
    monkeypatch.setenv("PR_AUTHOR", AUTHOR)
    ns = _attestation_fns(path, tmp_path, comments)
    ns["REVIEW_ID"] = rid
    ns["reemit_from_done_review"]()
    att = json.loads((tmp_path / "review-attestation.json").read_text())
    assert att["verdict"] == verdict and att["reemitted"] is True and att["reviewer"] == reviewer
    assert att["generated_at"] == "2026-11-02T10:11:12Z" and att["run_attempt"] == 2
    assert att["range"] == {"from": C2, "to": HEAD} and att["diff_sha256"] == A.canonical_range_digest(ns["files"])


@pytest.mark.parametrize("path", sorted(A.TRUSTED_REVIEW_WORKFLOWS.values()))
@pytest.mark.parametrize("comments", [
    [],                                                                                     # commento assente
    [{"user": {"login": "someone"}, "body": "<!-- done -->", "updated_at": "2026-11-02T10:11:12Z"}],   # non del bot
    [{"user": {"login": "github-actions[bot]"}, "body": "<!-- done -->\n", "updated_at": "2026-11-02T10:11:12Z"}],
])
def test_no_reliable_done_review_means_no_re_emitted_attestation(path, comments, tmp_path, monkeypatch):
    for k, v in (("GITHUB_ACTIONS", "true"), ("GITHUB_RUN_ID", "77"), ("GITHUB_RUN_ATTEMPT", "2"),
                 ("GITHUB_WORKSPACE", str(tmp_path)), ("PR_AUTHOR", AUTHOR)):
        monkeypatch.setenv(k, v)
    _attestation_fns(path, tmp_path, comments)["reemit_from_done_review"]()
    assert not (tmp_path / "review-attestation.json").exists()


@pytest.mark.parametrize("path", sorted(A.TRUSTED_REVIEW_WORKFLOWS.values()))
def test_both_dedup_exits_re_emit_before_leaving(path):
    _w, py = _workflow_python(path)
    assert py.count("\n    dedup_exit()\n") == 2
    assert py.index("def write_attestation") < py.index("_already_reviewed = comment_exists(done_marker)")


@pytest.mark.parametrize("path", sorted(A.TRUSTED_REVIEW_WORKFLOWS.values()))
def test_a_pass_is_never_re_emitted_from_an_editable_comment(path, tmp_path, monkeypatch):
    # Sol su #602 (05f6290): chi ha write puo' modificare il commento del bot (VALID_BLOCKERS -> VALID_PASS)
    rid = "gpt56sol" if "sol" in path else "grok46"
    head = (f"<!-- {rid}-pr-review:pickfair-nogui:{C2[:12]}...{HEAD[:12]} --><!-- done -->"
            f"<!-- {rid}-pr-review-attempt:pickfair-nogui:{C2[:12]}...{HEAD[:12]}:1:VALID_PASS -->\n# review")
    comments = [{"user": {"login": "github-actions[bot]"}, "body": head, "updated_at": "2026-11-02T10:11:12Z"}]
    for k, v in (("GITHUB_ACTIONS", "true"), ("GITHUB_RUN_ID", "77"), ("GITHUB_RUN_ATTEMPT", "2"),
                 ("GITHUB_WORKSPACE", str(tmp_path)), ("PR_AUTHOR", AUTHOR)):
        monkeypatch.setenv(k, v)   # ambiente completo: l'assenza del file non deve dipendere da un errore di scrittura
    ns = _attestation_fns(path, tmp_path, comments)
    ns["REVIEW_ID"] = rid
    ns["reemit_from_done_review"]()
    assert not (tmp_path / "review-attestation.json").exists()


# ── Codex P1 su #602 (05f6290): un run trusted fallito senza attestazione puo' nascondere un BLOCKERS ───────────────

class _RunsAPI(_API):
    def __init__(self, extra_run, extra_artifacts, attempt_conclusions=None, **kw):
        super().__init__(**kw)
        self.extra_run, self.extra_artifacts = extra_run, extra_artifacts
        self.attempt_conclusions = attempt_conclusions or {}

    def __call__(self, url):
        p = url.split(f"/repos/{REPO}", 1)[1]
        if p.startswith("/actions/workflows/") and "gpt56" in p:
            status, body = super().__call__(url)
            d = json.loads(body)
            d["workflow_runs"].append(dict(self.extra_run, path=".github/workflows/" + p.split("/")[3]))
            return 200, json.dumps(d).encode()
        if p.startswith("/actions/runs/500/artifacts"):
            return 200, json.dumps({"artifacts": self.extra_artifacts}).encode()
        if p.startswith("/actions/artifacts/5001/"):
            return 200, _zip(self.raws[1])
        if p.startswith("/actions/runs/500/attempts/"):
            n = int(p.rsplit("/", 1)[1])
            return 200, json.dumps({"conclusion": self.attempt_conclusions.get(n, "success")}).encode()
        return super().__call__(url)


@pytest.mark.parametrize("conclusion,attempt,artifacts,blocked", [
    ("failure", 1, [], True),                                # review conclusa, firma/upload falliti: esito sconosciuto
    ("cancelled", 1, [], True),
    ("success", 1, [], False),                               # salto legittimo (dedup, costo, range vuoto)
    # Sol su #602 (e76f2e4): un PASS del tentativo 1 non copre un rerun (tentativo 2) fallito senza attestazione
    ("failure", 2, [{"id": 5001, "name": "review-attestation-1", "expired": False}], True),
])
def test_trusted_run_without_attestation(conclusion, attempt, artifacts, blocked):
    run = {"id": 500, "status": "completed", "conclusion": conclusion, "run_attempt": attempt,
           "pull_requests": [{"number": PR}]}
    out = _verify(_RunsAPI(run, artifacts))
    assert out["passed"] is (not blocked), out
    assert ("UNVERIFIED_ATTESTATION" in out["reasons"]) is blocked


@pytest.mark.parametrize("path", sorted(A.TRUSTED_REVIEW_WORKFLOWS.values()))
@pytest.mark.parametrize("state,attempt,prior,code", [
    ("VALID_PASS", "2", (), 1),                          # rerun senza attestazioni precedenti, non ri-emettibile: rosso
    ("VALID_PASS", "2", ("review-attestation-1",), 0),   # il primo tentativo l'aveva pubblicata: resta valida, verde
    ("VALID_PASS", "1", (), 0),          # primo tentativo che trova il range gia' fatto da un altro run: salto legittimo
    ("VALID_BLOCKERS", "2", (), 0),      # rerun che ri-emette il bloccante: verde, con attestazione
])
def test_dedup_exit_turns_red_when_a_rerun_cannot_re_emit(path, state, attempt, prior, code, tmp_path, monkeypatch):
    rid = "gpt56sol" if "sol" in path else "grok46"
    head = (f"<!-- {rid}-pr-review:pickfair-nogui:{C2[:12]}...{HEAD[:12]} --><!-- done -->"
            f"<!-- {rid}-pr-review-attempt:pickfair-nogui:{C2[:12]}...{HEAD[:12]}:1:{state} -->\n# review")
    comments = [{"user": {"login": "github-actions[bot]"}, "body": head, "updated_at": "2026-11-02T10:11:12Z"}]
    for k, v in (("GITHUB_ACTIONS", "true"), ("GITHUB_RUN_ID", "77"), ("GITHUB_RUN_ATTEMPT", attempt),
                 ("GITHUB_WORKSPACE", str(tmp_path)), ("PR_AUTHOR", AUTHOR)):
        monkeypatch.setenv(k, v)
    ns = _attestation_fns(path, tmp_path, comments, run_artifacts=prior)
    ns["REVIEW_ID"] = rid
    with pytest.raises(SystemExit) as exc:
        ns["dedup_exit"]()
    assert exc.value.code == code


@pytest.mark.parametrize("path", sorted(A.TRUSTED_REVIEW_WORKFLOWS.values()))
def test_a_published_review_that_cannot_be_attested_turns_the_job_red(path):
    _w, py = _workflow_python(path)
    tail = py[py.rindex("write_attestation(attempt_state"):]
    assert "sys.exit(1)" in tail.split("\n")[1], tail[:300]


@pytest.mark.parametrize("path", sorted(A.TRUSTED_REVIEW_WORKFLOWS.values()))
def test_no_attestation_is_written_for_a_review_that_was_not_published(path):
    # Codex P1 su #602 (48ad2e6): upsert_comment fallito => niente PASS firmato, job rosso
    _w, py = _workflow_python(path)
    pub = py.rindex("_published = upsert_comment(marker, body[:64000])")
    guard = py.index("if not _published:", pub)
    assert guard < py.index("write_attestation(attempt_state", pub)
    assert "sys.exit(1)" in py[guard:py.index("write_attestation(attempt_state", pub)]


# ── Codex su #602 (e76f2e4): review in corso e branch riusati ────────────────────────────────────────────────────

class _PendingAPI(_API):
    def __init__(self, pending_status, **kw):
        super().__init__(**kw)
        self.pending_status = pending_status

    def __call__(self, url):
        p = url.split(f"/repos/{REPO}", 1)[1]
        if p.startswith("/actions/workflows/") and "gpt56" in p and "&status=" in p:
            st = p.split("&status=")[1].split("&")[0]
            rows = [{"id": 900, "path": ".github/workflows/" + p.split("/")[3], "status": st,
                     "pull_requests": [{"number": PR}]}] if st == self.pending_status else []
            return 200, json.dumps({"workflow_runs": rows}).encode()
        return super().__call__(url)


@pytest.mark.parametrize("status", ["queued", "in_progress", "waiting"])
def test_a_review_still_running_denies_pass_even_with_full_older_coverage(status):
    out = _verify(_PendingAPI(status))
    assert not out["passed"] and f"REVIEW_PENDING:gpt-6.1-sol:900" in out["reasons"]
    assert A._still_waiting(out), "una review in corso si aspetta (entro il limite)"


def test_no_pending_review_keeps_the_pass():
    assert _verify(_PendingAPI("nessuno"))["passed"]


@pytest.mark.parametrize("run,belongs", [
    ({"head_branch": "feat", "created_at": "2026-11-01T00:00:00Z", "pull_requests": []}, False),   # PR precedente
    ({"head_branch": "feat", "created_at": "2026-11-02T12:00:00Z", "pull_requests": []}, True),
    ({"head_branch": "feat", "pull_requests": []}, False),                                         # data assente
    ({"head_branch": "other", "created_at": "2026-11-01T00:00:00Z", "pull_requests": [{"number": PR}]}, True),
])
def test_runs_on_a_reused_branch_before_the_pr_was_opened_do_not_belong_to_it(run, belongs):
    assert A._run_belongs(run, PR, "2026-11-02T10:00:00Z", "feat") is belongs


@pytest.mark.parametrize("path", sorted(A.TRUSTED_REVIEW_WORKFLOWS.values()))
def test_review_workflows_can_read_their_own_run_artifacts(path):
    w, _py = _workflow_python(path)
    assert w["permissions"].get("actions") == "read"


def test_a_failed_earlier_attempt_without_attestation_blocks_even_if_the_rerun_is_green():
    # Grok su #602 (2864ace): tentativo 2 fallito senza attestazione, tentativo 3 (dedup) verde
    run = {"id": 500, "status": "completed", "conclusion": "success", "run_attempt": 3,
           "pull_requests": [{"number": PR}]}
    arts = [{"id": 5001, "name": "review-attestation-1", "expired": False}]
    out = _verify(_RunsAPI(run, arts, attempt_conclusions={2: "failure"}))
    assert not out["passed"] and "RUN_WITHOUT_ATTESTATION:gpt-6.1-sol:500:2" in out["reasons"]
    assert _verify(_RunsAPI(run, arts, attempt_conclusions={2: "success"}))["passed"]


@pytest.mark.parametrize("verdict", ["BLOCKERS", "INCONCLUSIVE", "ERROR"])
def test_current_head_blockers_from_an_out_of_chain_start_are_kept(verdict):
    # Codex P1 su #602 (2864ace): force-push che torna a un head gia' revisionato da un tip divergente
    stray = "9" * 40
    pairs = [_pair("gpt-6.1-sol", run=1), _pair("gpt-6.1-sol", stray, HEAD, verdict, run=2), _pair("grok-4.7", run=3)]
    d = _decide(pairs)
    assert not d.passed and "OPEN_BLOCKERS:gpt-6.1-sol" in d.reasons


def test_an_out_of_chain_pass_still_never_counts():
    stray = "9" * 40
    d = _decide([_pair("gpt-6.1-sol", stray, HEAD, run=1), _pair("grok-4.7", run=2)])
    assert not d.passed and "RANGE_NOT_IN_PR:gpt-6.1-sol" in d.reasons
