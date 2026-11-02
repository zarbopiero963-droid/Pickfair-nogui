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
            "run_id": run, "run_attempt": attempt, "verdict": verdict, "blockers": [], "files_truncated": 0}
    body.update(over)
    return json.dumps(body).encode()


def _sig(raw, reviewer="gpt-6.1-sol", run=11, attempt=1, **over):
    s = dict(ok=True, job_workflow_ref=f"{REPO}/{A.TRUSTED_REVIEW_WORKFLOWS.get(reviewer, 'x')}@refs/heads/main",
             event_name="pull_request_target", run_id=str(run), run_attempt=str(attempt), issuer=A.OIDC_ISSUER,
             digest=A.sha256_hex(raw))
    s.update(over)
    return A.Signed(**s)


def _pair(reviewer="gpt-6.1-sol", frm=BASE, to=HEAD, verdict="PASS", run=11, sig=None, **over):
    raw = _att(reviewer, frm, to, verdict, run, **over)
    return raw, _sig(raw, reviewer, run, **(sig or {}))


def _decide(pairs, **kw):
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
    d = A.decide(_both(), repository=REPO, pr=PR, pr_author=AUTHOR, head_sha="4" * 40, base_sha=BASE, commits=COMMITS)
    assert not d.passed and d.reasons == ["HEAD_NOT_IN_COMMITS"]


def test_re_evaluation_of_the_same_attestations_is_idempotent():
    pairs = _both()
    assert _decide(pairs).passed and _decide(pairs).passed


# ── adattatore gh ───────────────────────────────────────────────────────────────────────────────────────────────────

def _gh_json(digest, *, trigger="pull_request_target", signer=None):
    signer = signer or f"https://github.com/{REPO}/.github/workflows/pr-review-openrouter-gpt56-sol.yml@refs/heads/main"
    return json.dumps([{"verificationResult": {
        "signature": {"certificate": {"buildSignerURI": signer, "githubWorkflowTrigger": trigger,
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
    att, why = A.check_attestation(raw, signed, repository=REPO, pr=PR, pr_author=AUTHOR)
    assert why == "" and att["verdict"] == "PASS"


@pytest.mark.parametrize("result", [(1, ""), (0, "not json"), (0, "[]"), (0, json.dumps([{"x": 1}]))])
def test_gh_adapter_failures_are_unverified(result):
    signed = A.gh_signature_verifier(lambda argv: result, repository=REPO)("f.json", "w.yml")
    assert signed.ok is False


def test_gh_adapter_exception_is_unverified():
    def boom(argv):
        raise OSError("gh missing")
    assert A.gh_signature_verifier(boom, repository=REPO)("f.json", "w.yml").ok is False
