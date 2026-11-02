"""PF-BETFAIR-GITHUB-APP-ACTIVATION-01 — tooling della certificazione Betfair delle PR (tools/pf_cert).

Verifiche crittografiche con vettori ufficiali (RFC 8032) e con chiavi generate da openssl nel test (nessuna chiave
reale, nessuna rete). Ogni caso negativo del mandato (richiesta non autorizzata, SHA contraffatto, replay, firma non
valida, risultato di un altro SHA, dataset non verificato, prove mancanti) deve fallire in modo sicuro.
"""
from __future__ import annotations

import base64
import json
import re
import shutil
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from tools.pf_cert import classify as C
from tools.pf_cert import crypto_verify as X
from tools.pf_cert import request as R
from tools.pf_cert import result as V

pytestmark = pytest.mark.skipif(shutil.which("openssl") is None, reason="openssl richiesto per generare chiavi")

REPO = "zarbopiero963-droid/Pickfair-nogui"
REPO_ID = "1072600000"
HEAD = "d5e7f544424ad94861b0d2d34306b8a37072b905"
NOW = 1_800_000_000


def _openssl(*args, data: bytes = b"") -> bytes:
    return subprocess.run(["openssl", *args], input=data, capture_output=True, check=True).stdout


def _b64u(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


# ------------------------------------------------------------------------------------------------------- Ed25519
RFC8032 = [  # (public, message, signature) — RFC 8032 §7.1, TEST 1/2/3
    ("d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a", "",
     "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e065224901555fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b"),
    ("3d4017c3e843895a92b70aa74d1b7ebc9c982ccf2ec4968cc0cd55f12af4660c", "72",
     "92a009a9f0d4cab8720e820b5f642540a2b27b5416503f8fb3762223ebdb69da085ac1e43e15996e458f3613d0f11d8c387b2eaeb4302aeeb00d291612bb0c00"),
    ("fc51cd8e6218a1a38da47ed00230f0580816ed13ba3303ac5deb911548908025", "af82",
     "6291d657deec24024827e69c3abe01a30ce548a284743a445e3680d7db5ac3ac18ff9b538d16f290ae67f760984dc6594a7c15e9716ed28dc027beceea1ec40a"),
]


@pytest.mark.parametrize("pub,msg,sig", RFC8032)
def test_ed25519_rfc8032_vectors(pub, msg, sig):
    assert X.ed25519_verify(bytes.fromhex(pub), bytes.fromhex(msg), bytes.fromhex(sig))


@pytest.mark.parametrize("pub,msg,sig", RFC8032)
def test_ed25519_rejects_tampering(pub, msg, sig):
    p, m, s = bytes.fromhex(pub), bytes.fromhex(msg), bytearray.fromhex(sig)
    assert not X.ed25519_verify(p, m + b"x", bytes(s))
    s[0] ^= 1
    assert not X.ed25519_verify(p, m, bytes(s))
    s[0] ^= 1
    s[40] ^= 1
    assert not X.ed25519_verify(p, m, bytes(s))


def test_ed25519_rejects_malformed_inputs_and_non_canonical_s():
    pub, msg, sig = (bytes.fromhex(v) for v in RFC8032[0])
    assert not X.ed25519_verify(pub[:31], msg, sig)
    assert not X.ed25519_verify(pub, msg, sig[:63])
    big_s = sig[:32] + (int.from_bytes(sig[32:], "little") + X._Q).to_bytes(32, "little")
    assert not X.ed25519_verify(pub, msg, big_s), "s >= q (firma malleabile) accettata"
    assert not X.ed25519_verify("pub", msg, sig)            # type: ignore[arg-type]


@pytest.fixture(scope="module")
def ed_key(tmp_path_factory):
    d = tmp_path_factory.mktemp("ed")
    key = d / "k.pem"
    _openssl("genpkey", "-algorithm", "ed25519", "-out", str(key))
    spki = base64.b64encode(_openssl("pkey", "-in", str(key), "-pubout", "-outform", "DER")).decode()

    def sign(message: bytes) -> bytes:
        msg = d / "m.bin"
        msg.write_bytes(message)
        return _openssl("pkeyutl", "-sign", "-inkey", str(key), "-rawin", "-in", str(msg))
    return spki, sign


def test_ed25519_interoperates_with_openssl_and_spki(ed_key):
    spki, sign = ed_key
    pub = X.ed25519_public_key_from_spki_b64(spki)
    msg = b'{"result":"PASS"}'
    assert pub is not None and X.ed25519_verify(pub, msg, sign(msg))
    assert X.ed25519_public_key_from_spki_b64(V.ORCHESTRATOR_RESULT_KEY_SPKI_B64) is not None, "chiave pinnata"
    for bad in ("", "not-base64!", base64.b64encode(b"\x00" * 44).decode()):
        assert X.ed25519_public_key_from_spki_b64(bad) is None


# --------------------------------------------------------------------------------------------------------- RS256
@pytest.fixture(scope="module")
def rsa_key(tmp_path_factory):
    d = tmp_path_factory.mktemp("rsa")
    key = d / "k.pem"
    _openssl("genpkey", "-algorithm", "RSA", "-pkeyopt", "rsa_keygen_bits:2048", "-out", str(key))
    mod = _openssl("rsa", "-in", str(key), "-noout", "-modulus").decode().strip().split("=", 1)[1]
    n = int(mod, 16)

    def sign(message: bytes) -> bytes:
        return _openssl("dgst", "-sha256", "-sign", str(key), data=message)
    jwk = {"kty": "RSA", "kid": "k1", "use": "sig", "alg": "RS256",
           "n": _b64u(n.to_bytes((n.bit_length() + 7) // 8, "big")), "e": _b64u((65537).to_bytes(3, "big"))}
    return n, sign, {"keys": [jwk]}


def test_rs256_interoperates_with_openssl(rsa_key):
    n, sign, _ = rsa_key
    assert X.rs256_verify(n, 65537, b"abc", sign(b"abc"))
    assert not X.rs256_verify(n, 65537, b"abd", sign(b"abc"))
    sig = bytearray(sign(b"abc"))
    sig[-1] ^= 1
    assert not X.rs256_verify(n, 65537, b"abc", bytes(sig))
    assert not X.rs256_verify(n, 65537, b"abc", sign(b"abc")[1:]), "lunghezza della firma errata"
    assert not X.rs256_verify(n, 3, b"abc", sign(b"abc")), "esponente diverso"


def test_rs256_refuses_small_moduli():
    assert not X.rs256_verify((1 << 1023) | 1, 65537, b"abc", b"\x01" * 128)


# --------------------------------------------------------------------------------------------------- classificatore
@pytest.mark.parametrize("files,expected", [
    (["docs/x.md", "tests/a.py", "CHANGELOG.md", ".github/workflows/x.yml"], []),
    (["simulation_broker.py"], ["pytest-microstake-recovery", "sim-replay-microstake"]),
    (["core/reconciliation_engine.py"], ["pytest-reconciliation"]),
    (["core/brand_new_module.py"], list(C.FULL_MANDATORY)),
    (["config.json"], list(C.FULL_MANDATORY)),
    (["../simulation_broker.py"], list(C.FULL_MANDATORY)),
    (["/etc/passwd"], list(C.FULL_MANDATORY)),
    ([None], list(C.FULL_MANDATORY)),
])
def test_classifier_mandatory_scenarios(files, expected):
    assert C.classify(files)["mandatory"] == sorted(expected)


def test_classifier_unreadable_list_is_the_full_set():
    assert C.classify(None)["mandatory"] == list(C.FULL_MANDATORY)


def test_live_scenario_is_never_mandatory():
    out = C.classify(["betfair_client.py"])
    assert "live-real-order" in out["informational"] and "live-real-order" not in out["mandatory"]
    assert "sim-replay-microstake" in out["mandatory"]


def test_classifier_cli_and_summary(tmp_path, capsys):
    files, summary = tmp_path / "f.json", tmp_path / "s.md"
    files.write_text(json.dumps(["simulation_broker.py"]))
    assert C.main(["x", str(files), str(summary)]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["mandatory"] == ["pytest-microstake-recovery", "sim-replay-microstake"]
    assert re.fullmatch(r"[0-9a-f]{64}", out["classifier_sha256"])
    assert "sim-replay-microstake | si" in summary.read_text()
    files.write_text("{not json")
    assert C.main(["x", str(files)]) == 0
    assert json.loads(capsys.readouterr().out)["mandatory"] == list(C.FULL_MANDATORY)


# ------------------------------------------------------------------------------------------------ richiesta trusted
def _claims(**over):
    c = {"iss": R.OIDC_ISSUER, "aud": f"pf-cert:{REPO}", "iat": NOW - 30, "nbf": NOW - 30, "exp": NOW + 270,
         "jti": "jti-1", "repository": REPO, "repository_id": REPO_ID,
         "job_workflow_ref": f"{REPO}/{R.WORKFLOW_PATH}@{R.TRUSTED_REF}", "event_name": R.TRUSTED_EVENT,
         "ref": R.TRUSTED_REF, "run_id": "123456789", "run_attempt": "1", "sha": "basesha"}
    c.update(over)
    return c


def _token(sign, claims, header=None):
    h = _b64u(json.dumps(header or {"alg": "RS256", "typ": "JWT", "kid": "k1"}).encode())
    p = _b64u(json.dumps(claims).encode())
    return f"{h}.{p}.{_b64u(sign(f'{h}.{p}'.encode()))}"


def _line(token, sha=HEAD, run="123456789.1", scenarios="sim-replay-microstake"):
    return f"/pf-cert v2 pr=601 sha={sha} run={run} catalog=1.0.0 scenarios={scenarios} oidc={token}"


class _GitHub:
    def __init__(self, head=HEAD, head_repo=REPO, state="open", run_path=R.WORKFLOW_PATH,
                 run_event=R.TRUSTED_EVENT, run_prs=(601,), files=("simulation_broker.py",), fail=False):
        self.head, self.head_repo, self.state = head, head_repo, state
        self.run = {"path": run_path, "event": run_event, "pull_requests": [{"number": n} for n in run_prs]}
        self.files, self.fail = files, fail

    def pr(self, n):
        if self.fail:
            raise RuntimeError("api down")
        return {"state": self.state, "head": {"sha": self.head, "repo": {"full_name": self.head_repo}}}

    def run_lookup(self, run_id):
        return self.run

    def changed(self, n):
        return None if self.files is None else list(self.files)


def _verify(text, jwks, gh=None, seen=None, now=NOW):
    gh = gh or _GitHub()
    return R.verify_request(text, jwks=jwks, repository=REPO, repository_id=REPO_ID, now=now,
                            seen=seen if seen is not None else set(), pr_lookup=gh.pr, run_lookup=gh.run_lookup,
                            classify=C.classify, changed_files=gh.changed)


def test_trusted_request_is_accepted_and_scenarios_come_from_the_classifier(rsa_key):
    _, sign, jwks = rsa_key
    # la richiesta dichiara uno scenario solo: la VPS ricalcola dai file cambiati (qui due obbligatori)
    out = _verify(_line(_token(sign, _claims())), jwks)
    assert out["sha"] == HEAD and out["mandatory"] == ["pytest-microstake-recovery", "sim-replay-microstake"]


@pytest.mark.parametrize("claims_over,code", [
    ({"iss": "https://evil.example"}, "TOKEN_WRONG_ISSUER"),
    ({"aud": "pf-cert:other/repo"}, "TOKEN_WRONG_AUDIENCE"),
    ({"exp": NOW - 120}, "TOKEN_EXPIRED"),
    ({"iat": NOW - 3600, "nbf": NOW - 3600}, "TOKEN_EXPIRED"),
    ({"nbf": NOW + 600}, "TOKEN_EXPIRED"),
    ({"repository": "attacker/Pickfair-nogui"}, "TOKEN_CLAIM_MISMATCH:repository"),
    ({"repository_id": "1"}, "TOKEN_CLAIM_MISMATCH:repository_id"),
    # workflow eseguito dal branch della PR (pull_request) o da un altro file: non e' il percorso trusted
    ({"job_workflow_ref": f"{REPO}/{R.WORKFLOW_PATH}@refs/pull/601/merge"}, "TOKEN_CLAIM_MISMATCH:job_workflow_ref"),
    ({"job_workflow_ref": f"{REPO}/.github/workflows/evil.yml@refs/heads/main"},
     "TOKEN_CLAIM_MISMATCH:job_workflow_ref"),
    ({"event_name": "pull_request"}, "TOKEN_CLAIM_MISMATCH:event_name"),
    ({"ref": "refs/heads/f13-customer-order-ref"}, "TOKEN_CLAIM_MISMATCH:ref"),
    ({"run_id": "999"}, "TOKEN_CLAIM_MISMATCH:run_id"),
    ({"run_attempt": "2"}, "TOKEN_CLAIM_MISMATCH:run_attempt"),
    ({"jti": ""}, "TOKEN_MALFORMED"),
])
def test_forged_or_foreign_tokens_are_rejected(rsa_key, claims_over, code):
    _, sign, jwks = rsa_key
    with pytest.raises(R.Rejected) as exc:
        _verify(_line(_token(sign, _claims(**claims_over))), jwks)
    assert exc.value.code == code


def test_token_signature_header_and_key_attacks_are_rejected(rsa_key, tmp_path):
    _, sign, jwks = rsa_key
    good = _token(sign, _claims())
    h, p, s = good.split(".")
    forged_payload = _b64u(json.dumps(_claims(repository="x/y")).encode())
    cases = {
        "TOKEN_BAD_SIGNATURE": f"{h}.{forged_payload}.{s}",
        "TOKEN_ALG_NOT_ALLOWED": _token(sign, _claims(), {"alg": "none", "kid": "k1"}),
        "TOKEN_KEY_UNKNOWN": _token(sign, _claims(), {"alg": "RS256", "kid": "other"}),
    }
    for code, token in cases.items():
        with pytest.raises(R.Rejected) as exc:
            _verify(_line(token), jwks)
        assert exc.value.code == code, code
    # chiave di un attaccante con lo stesso kid ma non nel JWKS di GitHub
    other = tmp_path / "o.pem"
    _openssl("genpkey", "-algorithm", "RSA", "-pkeyopt", "rsa_keygen_bits:2048", "-out", str(other))
    evil = _token(lambda m: _openssl("dgst", "-sha256", "-sign", str(other), data=m), _claims())
    with pytest.raises(R.Rejected) as exc:
        _verify(_line(evil), jwks)
    assert exc.value.code == "TOKEN_BAD_SIGNATURE"


def test_replay_of_token_or_run_is_rejected(rsa_key):
    _, sign, jwks = rsa_key
    seen = set()
    line = _line(_token(sign, _claims()))
    _verify(line, jwks, seen=seen)
    with pytest.raises(R.Rejected) as exc:
        _verify(line, jwks, seen=seen)
    assert exc.value.code == "REPLAY"
    with pytest.raises(R.Rejected) as exc:                    # stesso run, token nuovo
        _verify(_line(_token(sign, _claims(jti="jti-2"))), jwks, seen=seen)
    assert exc.value.code == "REPLAY"


@pytest.mark.parametrize("gh,code", [
    (_GitHub(head="0" * 40), "SHA_NOT_PR_HEAD"),                     # nuovo push: la richiesta e' superata
    (_GitHub(head_repo="attacker/fork"), "FORK_PR_REFUSED"),
    (_GitHub(state="closed"), "PR_NOT_OPEN"),
    (_GitHub(run_prs=(602,)), "RUN_NOT_FOR_THIS_PR"),
    (_GitHub(run_path=".github/workflows/other.yml"), "RUN_NOT_TRUSTED_WORKFLOW"),
    (_GitHub(run_event="pull_request"), "RUN_NOT_TRUSTED_WORKFLOW"),
    (_GitHub(fail=True), "GITHUB_API_UNAVAILABLE"),
    (_GitHub(files=None), "GITHUB_API_UNAVAILABLE"),
])
def test_github_state_must_confirm_the_request(rsa_key, gh, code):
    _, sign, jwks = rsa_key
    with pytest.raises(R.Rejected) as exc:
        _verify(_line(_token(sign, _claims())), jwks, gh=gh)
    assert exc.value.code == code


def test_rejected_request_does_not_consume_the_replay_registry(rsa_key):
    _, sign, jwks = rsa_key
    seen = set()
    with pytest.raises(R.Rejected):
        _verify(_line(_token(sign, _claims())), jwks, gh=_GitHub(head="0" * 40), seen=seen)
    assert seen == set()


@pytest.mark.parametrize("text", [
    None, "", "/pf-cert v1 scenario=x sha=" + "a" * 40,
    _line("a" * 20 + "." + "b" * 20 + "." + "c" * 20, sha="A" * 40),
    _line("a" * 20 + "." + "b" * 20 + "." + "c" * 20, sha="a" * 39),
    _line("a" * 20 + "." + "b" * 20 + "." + "c" * 20) + "; rm -rf /",
    _line("a" * 20 + "." + "b" * 20 + "." + "c" * 20) + "\n/pf-cert v2 extra",
    "x " + _line("a" * 20 + "." + "b" * 20 + "." + "c" * 20),
    _line("a" * 20 + "." + "b" * 20 + "." + "c" * 20, scenarios="sim-replay-microstake,sim-replay-microstake"),
])
def test_malformed_request_lines_are_rejected(text):
    with pytest.raises(R.Rejected) as exc:
        R.parse_request(text)
    assert exc.value.code == "MALFORMED"


def test_format_request_roundtrip_and_cli(tmp_path, capsys):
    token = "a" * 20 + "." + "b" * 20 + "." + "c" * 20
    line = R.format_request(pr=601, sha=HEAD, run_id=7, run_attempt=1, catalog="1.0.0", scenarios=[], oidc=token)
    req = R.parse_request(line)
    assert (req.pr, req.sha, req.run_id, req.scenarios) == (601, HEAD, 7, ())
    scen = tmp_path / "s.json"
    scen.write_text(json.dumps({"catalog_version": "1.0.0", "scenarios": ["sim-replay-microstake"]}))
    env = {"PR": "601", "HEAD_SHA": HEAD, "RUN_ID": "7", "RUN_ATTEMPT": "1", "PF_CERT_OIDC": token}
    assert R.main(["x", str(scen)], env) == 0
    assert R.parse_request(capsys.readouterr().out.strip()).scenarios == ("sim-replay-microstake",)
    assert R.main(["x", str(scen)], {**env, "HEAD_SHA": "bad"}) == 1
    assert R.main(["x", str(scen)], {k: v for k, v in env.items() if k != "PF_CERT_OIDC"}) == 1


# ------------------------------------------------------------------------------------------------ risultati firmati
T0 = datetime(2026, 11, 2, 12, 0, tzinfo=timezone.utc)


def _manifest(**over):
    m = {"repo": REPO, "sha": HEAD, "scenario": "sim-replay-microstake", "catalog_version": "1.0.0",
         "result": "PASS", "issued_at": "2026-11-02T11:00:00Z", "expires_at": "2026-11-09T11:00:00Z",
         "dataset": {"id": "bfsim-x", "signature": "VERIFIED"}}
    m.update(over)
    return json.dumps(m, sort_keys=True).encode()


def _evidence(sign, **over):
    raw = _manifest(**over)
    return {"scenario": json.loads(raw)["scenario"], "manifest": raw, "signature": sign(raw)}


def _verdict(ed_key, evidence, mandatory=("sim-replay-microstake",)):
    spki, _ = ed_key
    return V.certification_verdict(evidence, mandatory=mandatory, repository=REPO, head_sha=HEAD, now=T0,
                                   public_key_spki_b64=spki)


def test_all_mandatory_pass_is_certified(ed_key):
    _, sign = ed_key
    ev = [_evidence(sign), _evidence(sign, scenario="pytest-microstake-recovery")]
    assert _verdict(ed_key, ev, ("sim-replay-microstake", "pytest-microstake-recovery"))["verdict"] == "CERTIFIED"


@pytest.mark.parametrize("over,reason", [
    ({"result": "FAIL"}, "FAIL"),
    ({"result": "BLOCKED"}, "BLOCKED"),
    ({"result": "NOT_RUN"}, "NOT_RUN"),
    ({"sha": "0" * 40}, "SHA_MISMATCH"),
    ({"repo": "other/repo"}, "WRONG_REPOSITORY"),
    ({"expires_at": "2026-11-02T11:30:00Z"}, "EXPIRED_OR_NOT_YET_VALID"),
    ({"issued_at": "2026-11-03T00:00:00Z"}, "EXPIRED_OR_NOT_YET_VALID"),
    ({"issued_at": "not a date"}, "EXPIRED_OR_NOT_YET_VALID"),
    ({"dataset": {"signature": "BAD_SIGNATURE"}}, "DATASET_NOT_VERIFIED"),
    ({"catalog_version": "0.9.0"}, "CATALOG_VERSION_MISMATCH"),
    ({"result": "GREEN"}, "RESULT_UNKNOWN"),
])
def test_non_pass_or_invalid_manifest_is_never_certified(ed_key, over, reason):
    _, sign = ed_key
    out = _verdict(ed_key, [_evidence(sign, **over)])
    assert out["verdict"] == "NOT_CERTIFIED" and out["reasons"] == [f"sim-replay-microstake:{reason}"]


def test_missing_mandatory_scenario_is_not_run(ed_key):
    _, sign = ed_key
    out = _verdict(ed_key, [_evidence(sign)], ("sim-replay-microstake", "pytest-reconciliation"))
    assert out["verdict"] == "NOT_CERTIFIED" and out["reasons"] == ["pytest-reconciliation:NOT_RUN"]


def test_forged_or_tampered_pass_is_rejected(ed_key, tmp_path):
    _, sign = ed_key
    ev = _evidence(sign, result="FAIL")
    tampered = dict(ev, manifest=ev["manifest"].replace(b'"FAIL"', b'"PASS"'))
    assert _verdict(ed_key, [tampered])["reasons"] == ["sim-replay-microstake:BAD_SIGNATURE"]
    other = tmp_path / "e.pem"
    _openssl("genpkey", "-algorithm", "ed25519", "-out", str(other))
    msg = tmp_path / "m"
    msg.write_bytes(_manifest())
    fake = _openssl("pkeyutl", "-sign", "-inkey", str(other), "-rawin", "-in", str(msg))
    forged = {"scenario": "sim-replay-microstake", "manifest": _manifest(), "signature": fake}
    assert _verdict(ed_key, [forged])["verdict"] == "NOT_CERTIFIED"


def test_contradictory_evidence_keeps_the_worst(ed_key):
    _, sign = ed_key
    ev = [_evidence(sign), _evidence(sign, result="FAIL")]
    assert _verdict(ed_key, ev)["verdict"] == "NOT_CERTIFIED"
    assert _verdict(ed_key, list(reversed(ev)))["verdict"] == "NOT_CERTIFIED"


def test_scenario_label_must_match_the_signed_manifest(ed_key):
    _, sign = ed_key
    ev = _evidence(sign, scenario="pytest-microstake-recovery")
    ev["scenario"] = "sim-replay-microstake"
    assert _verdict(ed_key, [ev])["verdict"] == "NOT_CERTIFIED"


def test_the_pinned_orchestrator_key_rejects_test_signatures(ed_key):
    _, sign = ed_key
    out = V.certification_verdict([_evidence(sign)], mandatory=["sim-replay-microstake"], repository=REPO,
                                  head_sha=HEAD, now=T0)
    assert out["reasons"] == ["sim-replay-microstake:BAD_SIGNATURE"]


def test_evidence_lines_roundtrip(ed_key):
    _, sign = ed_key
    ev = _evidence(sign)
    text = ("## pf-betfair-certification\n"
            f"pf-cert-evidence {ev['scenario']} {base64.b64encode(ev['manifest']).decode()} "
            f"{base64.b64encode(ev['signature']).decode()}\n")
    assert _verdict(ed_key, V.parse_evidence(text))["verdict"] == "CERTIFIED"
    assert V.parse_evidence(None) == [] and V.parse_evidence("nothing") == []


# --------------------------------------------------------------------------------------------- workflow (statico)
WF = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "pf-cert-request.yml"


def test_workflow_runs_only_trusted_base_code_with_minimal_permissions():
    yaml = pytest.importorskip("yaml")
    doc = yaml.safe_load(WF.read_text())
    on = doc.get(True, doc.get("on"))
    assert set(on) == {"pull_request_target"}, "solo pull_request_target (workflow del branch base)"
    assert doc["permissions"] == {"contents": "read"}
    job = doc["jobs"]["request"]
    assert job["permissions"] == {"contents": "read", "pull-requests": "write", "id-token": "write"}
    assert "head.repo.full_name == github.repository" in job["if"], "fork esclusi"
    checkout = next(s for s in job["steps"] if str(s.get("uses", "")).startswith("actions/checkout"))
    assert checkout["with"] == {"ref": "${{ github.sha }}", "persist-credentials": False}
    text = WF.read_text()
    for unsafe in ("pull_request.head.ref", "pull_request.title", "pull_request.body", "head.sha }}\n          run",
                   "refs/pull/"):
        assert unsafe not in text
    for step in job["steps"]:
        assert "${{" not in str(step.get("run", "")), "espressioni nel corpo di run: rischio injection"
