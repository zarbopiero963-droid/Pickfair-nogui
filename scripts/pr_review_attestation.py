"""PF-REVIEW-ATTESTATION v2 (#586 c.6582617372): verifica condivisa delle review AI per Merge readiness e VPS.

Una review conta per l'head ``H`` di una PR solo se e' dimostrata da un'**attestazione firmata** (GitHub Artifact
Attestation, Sigstore) prodotta da un workflow di review trusted che gira da ``pull_request_target`` sul branch base.
La firma lega il file ``review-attestation.json`` all'identita' OIDC del workflow
(``job_workflow_ref = <repo>/.github/workflows/<review>.yml@refs/heads/main``); un workflow aggiunto o modificato da
una PR ha un'altra identita' (``refs/pull/N/merge``) e non entra nell'allowlist. Il marker nei commenti non conta: e'
solo presentazione per gli umani.

Questo modulo NON fa crittografia: la verifica della firma e' iniettata (``signature_verifier``). In produzione e'
``gh attestation verify <file> --repo <repo> --signer-workflow <path>@refs/heads/main --predicate-type ...``
(strumento ufficiale GitHub), nei test uno stub. Tutto il resto (contenuto, SHA, copertura dei range, indipendenza,
replay) e' qui, identico per Merge readiness e per l'orchestratore VPS.

Fail-closed: ogni condizione non dimostrata => motivo esplicito, mai PASS.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

SCHEMA = "pf-review-attestation/1"
PREDICATE_TYPE = "https://pickfair/review-attestation/v1"
OIDC_ISSUER = "https://token.actions.githubusercontent.com"
TRUSTED_REF = "refs/heads/main"
TRUSTED_EVENT = "pull_request_target"
VERDICTS = ("PASS", "BLOCKERS", "INCONCLUSIVE", "ERROR")

#: workflow di review autorizzati dalla governance (#426/P41): reviewer -> path del workflow su main
TRUSTED_REVIEW_WORKFLOWS: Dict[str, str] = {
    "gpt-6.1-sol": ".github/workflows/pr-review-openrouter-gpt56-sol.yml",
    "grok-4.7": ".github/workflows/pr-review-xai-grok46.yml",
}
#: reviewer che la governance richiede per ogni PR (tutti devono coprire l'head)
REQUIRED_REVIEWERS: Tuple[str, ...] = ("gpt-6.1-sol", "grok-4.7")

_SHA = re.compile(r"[0-9a-f]{40}")


@dataclass(frozen=True)
class Signed:
    """Esito della verifica della firma (Sigstore) di un file di attestazione."""

    ok: bool
    job_workflow_ref: str = ""
    event_name: str = ""
    run_id: str = ""
    run_attempt: str = ""
    issuer: str = ""
    digest: str = ""
    reason: str = ""


@dataclass
class Decision:
    passed: bool
    reasons: List[str] = field(default_factory=list)
    covered: List[Tuple[str, str, str]] = field(default_factory=list)   # (reviewer, from, to)

    def to_dict(self) -> Dict[str, Any]:
        return {"passed": self.passed, "reasons": list(self.reasons),
                "covered": [{"reviewer": r, "from": a, "to": b} for r, a, b in self.covered]}


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _bad(reason: str) -> Tuple[Optional[Dict[str, Any]], str]:
    return None, reason


def check_attestation(raw: bytes, signed: Signed, *, repository: str, pr: int, pr_author: str,
                      trusted: Mapping[str, str] = TRUSTED_REVIEW_WORKFLOWS) -> Tuple[Optional[Dict[str, Any]], str]:
    """``(attestazione, "")`` se autentica e ben formata per questa PR, altrimenti ``(None, motivo)``.

    Non decide sull'head ne' sulla copertura (vedi ``decide``)."""
    if not signed.ok:
        return _bad(f"SIGNATURE_INVALID:{signed.reason or 'unverified'}")
    if signed.issuer != OIDC_ISSUER:
        return _bad("SIGNATURE_WRONG_ISSUER")
    if signed.digest != sha256_hex(raw):
        return _bad("DIGEST_MISMATCH")
    if signed.event_name != TRUSTED_EVENT:
        return _bad("UNTRUSTED_EVENT")
    try:
        att = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return _bad("MALFORMED")
    if not isinstance(att, dict) or att.get("schema") != SCHEMA:
        return _bad("MALFORMED")
    reviewer = att.get("reviewer")
    path = trusted.get(reviewer) if isinstance(reviewer, str) else None
    if path is None:
        return _bad("REVIEWER_NOT_TRUSTED")
    if signed.job_workflow_ref != f"{repository}/{path}@{TRUSTED_REF}":
        return _bad("UNTRUSTED_WORKFLOW")
    if att.get("workflow_path") != path:
        return _bad("WORKFLOW_PATH_MISMATCH")
    if att.get("repo") != repository or att.get("pr") != pr:
        return _bad("WRONG_PR")
    if str(att.get("run_id")) != str(signed.run_id) or str(att.get("run_attempt")) != str(signed.run_attempt):
        return _bad("RUN_MISMATCH")
    rng = att.get("range") if isinstance(att.get("range"), dict) else {}
    head, frm, to = att.get("head_sha"), rng.get("from"), rng.get("to")
    if not all(isinstance(x, str) and _SHA.fullmatch(x) for x in (head, frm, to)) or to != head:
        return _bad("MALFORMED_RANGE")
    if not pr_author or att.get("pr_author") != pr_author:
        return _bad("AUTHOR_MISMATCH")
    if att.get("verdict") not in VERDICTS:
        return _bad("VERDICT_UNKNOWN")
    return att, ""


def decide(attestations: Iterable[Tuple[bytes, Signed]], *, repository: str, pr: int, pr_author: str, head_sha: str,
           base_sha: str, commits: Sequence[str],
           required: Sequence[str] = REQUIRED_REVIEWERS,
           trusted: Mapping[str, str] = TRUSTED_REVIEW_WORKFLOWS) -> Decision:
    """Verdetto delle review per l'head ``head_sha``.

    ``commits`` = SHA della PR dal piu' vecchio all'head (la catena ``base -> ... -> head``). Per ogni reviewer
    richiesto le attestazioni PASS devono coprire la catena senza buchi fino all'head: ogni range ``from...to`` va da
    un commit della catena (o dal base) a un commit successivo, e l'ultimo termina sull'head. Un range con verdetto
    diverso da PASS che termina su un commit lo rende "non coperto" finche' un range PASS successivo non lo supera.
    Anti-replay: repo, PR e range sono nel contenuto FIRMATO, quindi un'attestazione non vale per un'altra PR; la
    stessa attestazione riletta per la stessa PR (giri successivi della VPS) e' idempotente."""
    chain = [base_sha, *commits]
    if not commits or commits[-1] != head_sha:
        return Decision(False, ["HEAD_NOT_IN_COMMITS"])
    index = {sha: i for i, sha in enumerate(chain)}
    reasons: List[str] = []
    by_reviewer: Dict[str, List[Tuple[int, int, str, str]]] = {}
    for raw, signed in attestations:
        att, why = check_attestation(raw, signed, repository=repository, pr=pr, pr_author=pr_author, trusted=trusted)
        if att is None:
            reasons.append(why)
            continue
        frm, to = att["range"]["from"], att["range"]["to"]
        if frm not in index or to not in index or index[frm] >= index[to]:
            reasons.append(f"RANGE_NOT_IN_PR:{att['reviewer']}")
            continue
        by_reviewer.setdefault(att["reviewer"], []).append((index[frm], index[to], att["verdict"], str(signed.run_id)))
    covered: List[Tuple[str, str, str]] = []
    for reviewer in required:
        if reviewer not in trusted:
            return Decision(False, reasons + [f"REQUIRED_REVIEWER_UNKNOWN:{reviewer}"])
        ranges = sorted(by_reviewer.get(reviewer, []), key=lambda r: (r[1], r[0]))
        # copertura: si avanza dal base (0) fino all'head (len(chain)-1) con range PASS contigui
        reached = 0
        last_bad = -1
        for a, b, verdict, _run in ranges:
            if verdict != "PASS":
                last_bad = max(last_bad, b)
                continue
            if a <= reached < b:
                reached = b
                covered.append((reviewer, chain[a], chain[b]))
        if reached != len(chain) - 1:
            reasons.append(f"NOT_COVERED_TO_HEAD:{reviewer}")
        elif last_bad > reached:
            reasons.append(f"OPEN_BLOCKERS:{reviewer}")
    passed = not [r for r in reasons if r.startswith(("NOT_COVERED", "OPEN_BLOCKERS", "REQUIRED_", "HEAD_"))]
    return Decision(passed, reasons, covered)


def gh_signature_verifier(run: Callable[[List[str]], Tuple[int, str]], *, repository: str,
                          predicate_type: str = PREDICATE_TYPE) -> Callable[[str, str], Signed]:
    """Adattatore di produzione: ``gh attestation verify --format json`` sul file, per il workflow atteso.

    ``run(argv) -> (exit_code, stdout)`` e' iniettato (subprocess in produzione). Exit diverso da 0, output non JSON
    o certificato senza i campi attesi => ``Signed(ok=False)``."""

    def verify(path: str, workflow_path: str) -> Signed:
        argv = ["gh", "attestation", "verify", path, "--repo", repository, "--predicate-type", predicate_type,
                "--signer-workflow", f"{repository}/{workflow_path}", "--source-ref", TRUSTED_REF,
                "--cert-oidc-issuer", OIDC_ISSUER, "--format", "json"]
        try:
            code, out = run(argv)
        except Exception as exc:  # noqa: BLE001 - verificatore non eseguibile => non verificato
            return Signed(False, reason=f"VERIFIER_ERROR:{type(exc).__name__}")
        if code != 0:
            return Signed(False, reason=f"GH_EXIT_{code}")
        try:
            results = json.loads(out)
            first = results[0]
            cert = first["verificationResult"]["signature"]["certificate"]
            digest = first["verificationResult"]["statement"]["subject"][0]["digest"]["sha256"]
        except (ValueError, KeyError, IndexError, TypeError):
            return Signed(False, reason="GH_OUTPUT_MALFORMED")
        return Signed(True, job_workflow_ref=str(cert.get("buildSignerURI", "")).replace("https://github.com/", ""),
                      event_name=str(cert.get("githubWorkflowTrigger", "")),
                      run_id=str(cert.get("runInvocationURI", "")).rstrip("/").split("/attempts/")[0].rsplit("/", 1)[-1],
                      run_attempt=str(cert.get("runInvocationURI", "")).rstrip("/").rsplit("/attempts/", 1)[-1]
                      if "/attempts/" in str(cert.get("runInvocationURI", "")) else "",
                      issuer=str(cert.get("issuer", "")), digest=str(digest))

    return verify
