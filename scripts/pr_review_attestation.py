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
#: provenance SLSA standard di actions/attest-build-provenance: lega il digest del file all'identita' del workflow
PREDICATE_TYPE = "https://slsa.dev/provenance/v1"
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
    workflow_sha: str = ""      # commit da cui e' stato eseguito il workflow firmatario (buildSignerDigest)
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


def canonical_range_digest(files: Iterable[Mapping[str, Any]]) -> str:
    """Impronta canonica del codice di un range (Compare API ``files``): sha256 del JSON compatto e ordinato di
    ``[nome_file, patch]`` per ogni file con patch. Il workflow di review la scrive nell'attestazione, il verificatore
    la ricalcola dalla stessa API: coincide solo se il reviewer ha ricevuto le stesse patch. La STESSA funzione e'
    copiata nei workflow (test di parita' in tests/scripts/test_pr_review_attestation.py)."""
    rows = sorted([str(f.get("filename") or ""), str(f.get("patch") or "")]
                  for f in files if isinstance(f, Mapping) and f.get("patch"))
    return hashlib.sha256(json.dumps(rows, separators=(",", ":"), ensure_ascii=False).encode("utf-8")).hexdigest()


def _bad(reason: str) -> Tuple[Optional[Dict[str, Any]], str]:
    return None, reason


def check_attestation(raw: bytes, signed: Signed, *, repository: str, pr: int, pr_author: str,
                      trusted: Mapping[str, str] = TRUSTED_REVIEW_WORKFLOWS,
                      is_trusted_workflow_commit: Optional[Callable[[str], bool]] = None,
                      range_diff_sha256: Optional[Callable[[str, str, str], Optional[str]]] = None,
                      ) -> Tuple[Optional[Dict[str, Any]], str]:
    """``(attestazione, "")`` se autentica e ben formata per questa PR, altrimenti ``(None, motivo)``.

    Oltre alla firma (owner #586: «una firma autentica non dimostra che la review sia corretta o indipendente»):
    - ``is_trusted_workflow_commit(sha)``: il commit del workflow firmatario e' su ``main`` (versione non controllabile
      dall'autore della PR). Obbligatorio: assente => non verificabile;
    - ``range_diff_sha256(from, to, base)``: ``canonical_range_digest`` del range ricalcolato dal verificatore (Compare
      API) con la STESSA normalizzazione del workflow (``pr_range_files``, sulla ``base_sha`` firmata); deve
      coincidere con ``diff_sha256`` dichiarato, con ``files_sent > 0`` e ``files_truncated == 0`` (il reviewer ha
      ricevuto TUTTO il codice del range). Obbligatorio: assente => non verificabile.
    I tre requisiti di copertura valgono solo per un PASS: un verdetto non PASS autentico e' accettato comunque, cosi'
    ``decide`` lo conta come bloccante. Non decide sull'head ne' sulla copertura (vedi ``decide``)."""
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
    head, frm, to, base = att.get("head_sha"), rng.get("from"), rng.get("to"), att.get("base_sha")
    if not all(isinstance(x, str) and _SHA.fullmatch(x) for x in (head, frm, to, base)) or to != head:
        return _bad("MALFORMED_RANGE")
    if not pr_author or att.get("pr_author") != pr_author:
        return _bad("AUTHOR_MISMATCH")
    if att.get("verdict") not in VERDICTS:
        return _bad("VERDICT_UNKNOWN")
    if is_trusted_workflow_commit is None or not _SHA.fullmatch(signed.workflow_sha or ""):
        return _bad("WORKFLOW_COMMIT_UNVERIFIABLE")
    try:
        trusted_commit = bool(is_trusted_workflow_commit(signed.workflow_sha))
    except Exception:  # noqa: BLE001 - API non leggibile => non verificabile
        return _bad("WORKFLOW_COMMIT_UNVERIFIABLE")
    if not trusted_commit:
        return _bad("WORKFLOW_COMMIT_NOT_ON_MAIN")
    if att["verdict"] != "PASS":
        # verdetto negativo autentico: conta SEMPRE, anche se la sua review era incompleta (troncata, nulla inviato, diff
        # non ricalcolabile). I requisiti di copertura servono a meritare un PASS, non a far sparire un bloccante
        # davanti a una copertura piu' vecchia (rilievo Codex su #602; AGENTS.md: review troncata mai PASS).
        return att, ""
    sent, truncated = att.get("files_sent"), att.get("files_truncated")
    if not isinstance(sent, int) or isinstance(sent, bool) or sent <= 0:
        return _bad("NOTHING_REVIEWED")
    if truncated != 0:
        return _bad("DIFF_TRUNCATED")
    if range_diff_sha256 is None:
        return _bad("DIFF_UNVERIFIABLE")
    try:
        expected = range_diff_sha256(frm, to, base)
    except Exception:  # noqa: BLE001
        expected = None
    if not expected:
        return _bad("DIFF_UNVERIFIABLE")
    if att.get("diff_sha256") != expected:
        return _bad("DIFF_MISMATCH")
    return att, ""


def decide(attestations: Iterable[Tuple[bytes, Signed]], *, repository: str, pr: int, pr_author: str, head_sha: str,
           base_sha: str, commits: Sequence[str],
           required: Sequence[str] = REQUIRED_REVIEWERS,
           trusted: Mapping[str, str] = TRUSTED_REVIEW_WORKFLOWS,
           is_trusted_workflow_commit: Optional[Callable[[str], bool]] = None,
           range_diff_sha256: Optional[Callable[[str, str, str], Optional[str]]] = None) -> Decision:
    """Verdetto delle review per l'head ``head_sha``.

    ``commits`` = SHA della PR dal piu' vecchio all'head (la catena ``base -> ... -> head``). Per ogni reviewer
    richiesto le attestazioni PASS devono coprire la catena senza buchi fino all'head: ogni range ``from...to`` va da
    un commit della catena (o dal base) a un commit successivo, e l'ultimo termina sull'head. Un range con verdetto
    diverso da PASS che termina su un commit lo rende "non coperto" finche' un range PASS successivo non lo supera.
    "Successivo" e' nell'istante di emissione firmato (``generated_at``, strettamente dopo), non nella posizione del
    range ne' nel numero del run (i rerun lo conservano): un BLOCKERS emesso DOPO un PASS sullo stesso head lo annulla
    (rilievi di Sol su #602). Il PASS che lo supera deve portare commit nuovi oltre il non-PASS o ri-coprirne tutto il
    range; un PASS successivo solo su una parte dello stesso head non basta (Grok su #602). Un istante illeggibile conta come l'ultimo se non e' PASS (blocca) e come il primo se e'
    PASS (non sblocca nulla); a parita' di secondo il PASS non sblocca.
    Anti-replay: repo, PR e range sono nel contenuto FIRMATO, quindi un'attestazione non vale per un'altra PR; la
    stessa attestazione riletta per la stessa PR (giri successivi della VPS) e' idempotente."""
    chain = [base_sha, *commits]
    if not commits or commits[-1] != head_sha:
        return Decision(False, ["HEAD_NOT_IN_COMMITS"])
    index = {sha: i for i, sha in enumerate(chain)}
    reasons: List[str] = []
    by_reviewer: Dict[str, List[Tuple[int, int, str, str]]] = {}
    for raw, signed in attestations:
        att, why = check_attestation(raw, signed, repository=repository, pr=pr, pr_author=pr_author, trusted=trusted,
                                     is_trusted_workflow_commit=is_trusted_workflow_commit,
                                     range_diff_sha256=range_diff_sha256)
        if att is None:
            reasons.append(why)
            # Gli artefatti vengono SOLO dai run dei workflow trusted: se uno non si verifica (firma, identita',
            # verificatore in timeout, contenuto malformato) potrebbe essere un BLOCKERS, quindi la copertura piu'
            # vecchia non basta (rilievo Codex P1 su #602). Non bloccano solo gli artefatti autentici che non si
            # applicano: di un'altra PR, o un PASS incompleto che semplicemente non copre.
            if not why.startswith(_NOT_APPLICABLE) and "UNVERIFIED_ATTESTATION" not in reasons:
                reasons.append("UNVERIFIED_ATTESTATION")
            continue
        frm, to = att["range"]["from"], att["range"]["to"]
        # base avanzato senza cambiare la PR: il range iniziale parte dalla base FIRMATA di allora, che conta come
        # l'inizio della catena (il digest del diff e' gia' stato ricalcolato su quella base)
        start = 0 if frm == att["base_sha"] and frm not in index else index.get(frm)
        if start is None or to not in index or start >= index[to]:
            reasons.append(f"RANGE_NOT_IN_PR:{att['reviewer']}")
            continue
        by_reviewer.setdefault(att["reviewer"], []).append(
            (start, index[to], att["verdict"], _emitted_at(att, att["verdict"] == "PASS")))
    covered: List[Tuple[str, str, str]] = []
    for reviewer in required:
        if reviewer not in trusted:
            return Decision(False, reasons + [f"REQUIRED_REVIEWER_UNKNOWN:{reviewer}"])
        ranges = sorted(by_reviewer.get(reviewer, []), key=lambda r: (r[1], r[0]))
        # copertura: si avanza dal base (0) fino all'head (len(chain)-1) con range PASS contigui
        reached = 0
        for a, b, verdict, _order in ranges:
            if verdict == "PASS" and a <= reached < b:
                reached = b
                covered.append((reviewer, chain[a], chain[b]))
        # un non-PASS ``a...b`` resta aperto finche' un PASS emesso DOPO di lui non lo supera: o porta commit nuovi
        # oltre ``b`` (la correzione, revisionata nella push successiva), o ri-copre tutto ``a...b``. Un PASS successivo
        # solo su una parte dello STESSO head non basta (rilievo di Grok su #602).
        open_bad = [
            (b, order) for a, b, verdict, order in ranges if verdict != "PASS"
            and not any(v2 == "PASS" and o2 > order and (b2 > b or (b2 >= b and a2 <= a))
                        for a2, b2, v2, o2 in ranges)
        ]
        if reached != len(chain) - 1:
            reasons.append(f"NOT_COVERED_TO_HEAD:{reviewer}")
        elif open_bad:
            reasons.append(f"OPEN_BLOCKERS:{reviewer}")
    passed = not [r for r in reasons if r.startswith(_GATING)]
    return Decision(passed, reasons, covered)


#: motivi di ``check_attestation`` per artefatti autentici che semplicemente non si applicano a questa decisione
_NOT_APPLICABLE = ("WRONG_PR", "NOTHING_REVIEWED", "DIFF_TRUNCATED", "DIFF_MISMATCH", "DIFF_UNVERIFIABLE")
#: motivi che negano il PASS
_GATING = ("NOT_COVERED", "OPEN_BLOCKERS", "REQUIRED_", "HEAD_", "UNVERIFIED_ATTESTATION", "REVIEW_PENDING")


def _emitted_at(att: Mapping[str, Any], is_pass: bool) -> float:
    """Istante di emissione della review: ``generated_at`` del contenuto FIRMATO (scritto dal workflow trusted).

    Non ``run_id``: un rerun conserva il numero del run, quindi un BLOCKERS del run 10 rilanciato dopo un PASS del run 11
    risulterebbe "precedente" (rilievo di Sol su #602, 185058f). Illeggibile => fail-closed: +inf per un non-PASS
    (resta sempre l'ultimo, quindi blocca), -inf per un PASS (non supera mai nulla)."""
    import datetime as _dt
    text = att.get("generated_at")
    try:
        if not isinstance(text, str) or not re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", text):
            raise ValueError(text)
        return _dt.datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=_dt.timezone.utc).timestamp()
    except ValueError:
        return float("-inf") if is_pass else float("inf")


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
                      issuer=str(cert.get("issuer", "")), digest=str(digest),
                      workflow_sha=str(cert.get("buildSignerDigest", "")))

    return verify


def pr_range_files(push_files: List[Dict[str, Any]], own_files: Optional[List[Dict[str, Any]]], range_from: str,
                   base_sha: str) -> List[Dict[str, Any]]:
    """I file del range come li ha ricevuti il reviewer: stessa regola di ``solo_file_della_pr`` dei workflow Sol/Grok
    (test di parita'). ``own_files`` = Compare ``base_sha...to`` (``None`` se non calcolabile).

    Range che parte dalla base, liste al cap di 300 o file propri non calcolabili => range intero. Altrimenti si tengono
    solo i file che la PR cambia; se qualcosa e' stato escluso (push che porta dentro main), per i file tenuti si usa il
    patch calcolato sulla base, quando GitHub lo fornisce."""
    if range_from == base_sha or not _SHA.fullmatch(base_sha or "") or len(push_files) >= 300:
        return push_files
    if own_files is None or len(own_files) >= 300:
        return push_files
    names = {f.get("filename") for f in own_files} | {f.get("previous_filename") for f in own_files}
    names.discard(None)
    kept = [f for f in push_files if f.get("filename") in names or f.get("previous_filename") in names]
    excluded = {f.get("filename") for f in push_files} - {f.get("filename") for f in kept} - {None}
    if not excluded:
        return kept
    by_name: Dict[Any, Dict[str, Any]] = {}
    for f in own_files:
        if f.get("previous_filename"):
            by_name[f["previous_filename"]] = f
    for f in own_files:
        if f.get("filename"):
            by_name[f["filename"]] = f

    def from_base(f: Dict[str, Any]) -> Dict[str, Any]:
        own = by_name.get(f.get("filename")) or by_name.get(f.get("previous_filename"))
        return own if own and own.get("patch") else f

    return [from_base(f) for f in kept]


# ── integrazione GitHub (Merge readiness / VPS) ─────────────────────────────────────────────────────────────────────

PRELIVE_CHECK = "pf-betfair-certification"
PRELIVE_APP = "pickfair-dedicated"
ATTESTATION_FILE = "review-attestation.json"


class GitHub:
    """Letture REST minime. ``http(url) -> (status, bytes)`` iniettabile; il token non segue i redirect."""

    def __init__(self, repository: str, token: str, http: Optional[Callable[[str], Tuple[int, bytes]]] = None):
        self.repository, self.token = repository, token
        self.http = http or self._urllib

    def _urllib(self, url: str) -> Tuple[int, bytes]:
        import urllib.request
        req = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json"})
        if self.token:
            req.add_unredirected_header("Authorization", f"Bearer {self.token}")
        with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310 - solo api.github.com e i suoi redirect
            return resp.status, resp.read()

    def json(self, path: str) -> Any:
        status, body = self.http(f"https://api.github.com/repos/{self.repository}{path}")
        if status != 200:
            raise RuntimeError(f"HTTP {status} {path}")
        return json.loads(body.decode("utf-8"))

    def pr(self, number: int) -> Dict[str, Any]:
        return self.json(f"/pulls/{number}")

    def pr_commits(self, number: int) -> List[str]:
        out: List[str] = []
        page = 1
        while True:
            rows = self.json(f"/pulls/{number}/commits?per_page=100&page={page}")
            out += [r["sha"] for r in rows]
            if len(rows) < 100:
                return out
            page += 1

    def review_runs(self, workflow_path: str, number: int, since: str = "", max_pages: int = 20,
                    head_ref: str = "") -> List[Dict[str, Any]]:
        """Run completati del workflow per la PR, paginando fino ai run creati prima di ``since`` (apertura della PR).
        Oltre ``max_pages`` senza arrivarci => errore (non verificabile), mai una lista parziale in silenzio.

        Il campo ``pull_requests`` di un run e' calcolato da GitHub al momento della lettura e si svuota quando l'head
        della PR cambia (run di #601 dopo i push successivi: ``[]``), quindi da solo perderebbe i range gia' firmati.
        Vale anche il branch della PR (``head_branch`` dei run ``pull_request_target``); l'appartenenza vera alla PR
        la decide comunque il contenuto FIRMATO (``pr``), controllato in ``check_attestation``."""
        file = workflow_path.rsplit("/", 1)[-1]
        runs: List[Dict[str, Any]] = []
        for page in range(1, max_pages + 1):
            rows = self.json(f"/actions/workflows/{file}/runs?event={TRUSTED_EVENT}&per_page=100&page={page}"
                             ).get("workflow_runs", [])
            runs += rows
            if len(rows) < 100 or (since and min(str(r.get("created_at") or "") for r in rows) < since):
                break
        else:
            raise RuntimeError(f"review runs oltre {max_pages} pagine")
        return [r for r in runs if r.get("path") == workflow_path and r.get("status") == "completed"
                and _run_belongs(r, number, since, head_ref)]

    def pending_review_runs(self, workflow_path: str, number: int, since: str = "",
                            head_ref: str = "") -> List[Dict[str, Any]]:
        """Run del workflow per la PR non ancora conclusi (queued / in_progress / waiting / ...). Una review in corso
        puo' ancora pubblicare un BLOCKERS: finche' c'e', nessun PASS (rilievo Codex P1 su #602)."""
        file = workflow_path.rsplit("/", 1)[-1]
        out: List[Dict[str, Any]] = []
        for status in ("queued", "in_progress", "waiting", "requested", "pending"):
            rows = self.json(f"/actions/workflows/{file}/runs?event={TRUSTED_EVENT}&status={status}&per_page=100"
                             ).get("workflow_runs", [])
            if len(rows) >= 100:
                raise RuntimeError(f"run {status} oltre una pagina: non verificabile")
            out += [r for r in rows if r.get("path") == workflow_path and r.get("status") != "completed"
                    and _run_belongs(r, number, since, head_ref)]
        return out

    def attestation_files(self, run_id: int) -> List[bytes]:
        return [raw for _name, raw in self.attestation_artifacts(run_id)]

    def attestation_artifacts(self, run_id: int) -> List[Tuple[str, bytes]]:
        import io
        import zipfile
        arts = self.json(f"/actions/runs/{run_id}/artifacts?per_page=100").get("artifacts", [])
        out: List[Tuple[str, bytes]] = []
        for art in arts:
            if not str(art.get("name", "")).startswith("review-attestation-") or art.get("expired"):
                continue
            status, blob = self.http(f"https://api.github.com/repos/{self.repository}/actions/artifacts/{art['id']}/zip")
            if status != 200:
                raise RuntimeError(f"HTTP {status} artifact {art['id']}")
            with zipfile.ZipFile(io.BytesIO(blob)) as zf:
                out.append((str(art.get("name")), zf.read(ATTESTATION_FILE)))
        return out

    def is_on_main(self, sha: str) -> bool:
        cmp = self.json(f"/compare/{sha}...main")
        return cmp.get("status") in ("identical", "ahead")      # sha antenato (o uguale) di main

    def range_digest(self, frm: str, to: str, base: str) -> Optional[str]:
        files = self.json(f"/compare/{frm}...{to}").get("files")
        if not isinstance(files, list) or len(files) >= 300:
            return None                                          # Compare API troncata: non verificabile
        own: Optional[List[Dict[str, Any]]] = None
        if frm != base:
            try:
                own = self.json(f"/compare/{base}...{to}").get("files")
            except Exception:  # noqa: BLE001 - come il workflow: file propri non calcolabili => range intero
                own = None
            own = own if isinstance(own, list) else None
        return canonical_range_digest(pr_range_files(files, own, frm, base))

    def check_runs(self, sha: str) -> List[Dict[str, Any]]:
        return self.json(f"/commits/{sha}/check-runs?per_page=100").get("check_runs", [])


def _run_belongs(run: Mapping[str, Any], number: int, since: str, head_ref: str) -> bool:
    """Run della PR: elencato da GitHub in ``pull_requests``, oppure sul branch della PR e creato DOPO la sua apertura
    (un branch riusato non porta dentro i run di una PR precedente, rilievo Codex P2 su #602)."""
    if any(p.get("number") == number for p in run.get("pull_requests") or []):
        return True
    created = str(run.get("created_at") or "")
    return bool(head_ref) and run.get("head_branch") == head_ref and (not since or (bool(created) and created >= since))


def verify_pr(gh: GitHub, number: int, verify_signature: Callable[[bytes, str], Signed]) -> Dict[str, Any]:
    """Raccoglie dal repository le attestazioni delle review della PR e applica ``decide``. Ogni errore => non PASS."""
    try:
        pr = gh.pr(number)
        head, base = pr["head"]["sha"], pr["base"]["sha"]
        if pr["head"]["repo"]["full_name"] != gh.repository:
            return {"passed": False, "reasons": ["FORK_PR"], "head_sha": head}
        commits = gh.pr_commits(number)
        pairs = []
        unpublished: List[str] = []
        pending: List[str] = []
        since, head_ref = str(pr.get("created_at") or ""), str(pr["head"].get("ref") or "")
        for reviewer, path in TRUSTED_REVIEW_WORKFLOWS.items():
            pending += [f"REVIEW_PENDING:{reviewer}:{r['id']}"
                        for r in gh.pending_review_runs(path, number, since=since, head_ref=head_ref)]
            for run in gh.review_runs(path, number, since=since, head_ref=head_ref):
                artifacts = gh.attestation_artifacts(int(run["id"]))
                for _name, raw in artifacts:
                    pairs.append((raw, verify_signature(raw, path)))
                # Un run il cui ULTIMO tentativo non chiude `success` e non ha la propria attestazione puo' aver
                # concluso una review (anche BLOCKERS) senza riuscire a pubblicarla: esito sconosciuto => niente PASS
                # (Codex P1 e Sol su #602). Conta il tentativo, non il run: un artefatto PASS di un tentativo precedente
                # non copre un rerun fallito. I workflow chiudono rosso quando scrittura, firma o upload falliscono e sul
                # rerun che non puo' ri-emettere senza attestazioni precedenti; i salti legittimi chiudono `success`.
                latest = f"review-attestation-{run.get('run_attempt') or 1}"
                if run.get("conclusion") != "success" and latest not in {name for name, _raw in artifacts}:
                    unpublished.append(f"RUN_WITHOUT_ATTESTATION:{reviewer}:{run['id']}")
    except Exception as exc:  # noqa: BLE001 - API non leggibile => non verificabile
        return {"passed": False, "reasons": [f"GITHUB_API_UNAVAILABLE:{type(exc).__name__}"]}
    decision = decide(pairs, repository=gh.repository, pr=number, pr_author=pr["user"]["login"], head_sha=head,
                      base_sha=base, commits=commits, is_trusted_workflow_commit=gh.is_on_main,
                      range_diff_sha256=gh.range_digest)
    out = dict(decision.to_dict(), head_sha=head, attestations=len(pairs))
    if unpublished:
        out["passed"] = False
        out["reasons"] = list(out.get("reasons", [])) + unpublished + (
            [] if "UNVERIFIED_ATTESTATION" in out.get("reasons", []) else ["UNVERIFIED_ATTESTATION"])
    if pending:
        out["passed"] = False
        out["reasons"] = list(out.get("reasons", [])) + pending
    return out


def _still_waiting(result: Mapping[str, Any]) -> bool:
    """Vale la pena riprovare solo se manca la copertura (review ancora in corso): bloccanti, firme o API no."""
    gating = [r for r in result.get("reasons", []) if r.startswith(_GATING + ("GITHUB_API", "FORK"))]
    return not result.get("passed") and bool(gating) and all(
        r.startswith(("NOT_COVERED_TO_HEAD:", "REVIEW_PENDING:")) for r in gating)


def verify_pr_waiting(gh: GitHub, number: int, verify_signature: Callable[[bytes, str], Signed], *,
                      wait_seconds: float = 0, interval: float = 60, sleep: Callable[[float], None] = None,
                      clock: Callable[[], float] = None) -> Dict[str, Any]:
    """``verify_pr`` con attesa limitata: Merge readiness puo' partire prima che Sol/Grok abbiano finito. Si riprova
    finche' l'unico motivo e' ``NOT_COVERED_TO_HEAD`` e non e' scaduto ``wait_seconds``; poi vale l'ultimo esito."""
    import time
    sleep = sleep or time.sleep
    clock = clock or time.monotonic
    deadline = clock() + max(0.0, wait_seconds)
    while True:
        result = verify_pr(gh, number, verify_signature)
        if not _still_waiting(result) or clock() + interval > deadline:
            return result
        sleep(interval)


def verify_prelive(gh: GitHub, head_sha: str) -> Dict[str, Any]:
    """Il check PRE-LIVE di Pickfair Dedicated in SUCCESS sull'head esatto. Il check LIVE non e' un gate qui."""
    try:
        runs = gh.check_runs(head_sha)
    except Exception as exc:  # noqa: BLE001
        return {"passed": False, "reason": f"GITHUB_API_UNAVAILABLE:{type(exc).__name__}"}
    mine = [r for r in runs if r.get("name") == PRELIVE_CHECK and (r.get("app") or {}).get("slug") == PRELIVE_APP
            and r.get("head_sha") == head_sha]
    if not mine:
        return {"passed": False, "reason": "PRELIVE_MISSING"}
    latest = max(mine, key=lambda r: str(r.get("completed_at") or r.get("started_at") or ""))
    if latest.get("status") != "completed":
        return {"passed": False, "reason": "PRELIVE_PENDING"}
    if latest.get("conclusion") != "success":
        return {"passed": False, "reason": f"PRELIVE_{str(latest.get('conclusion')).upper()}"}
    return {"passed": True, "reason": "", "check_run_id": latest.get("id")}


def main(argv: Optional[Sequence[str]] = None) -> int:
    import argparse
    import os
    import subprocess
    import tempfile
    parser = argparse.ArgumentParser(description="PF-REVIEW-ATTESTATION v2 / gate PRE-LIVE")
    sub = parser.add_subparsers(dest="cmd", required=True)
    for name in ("verify-pr", "verify-prelive"):
        p = sub.add_parser(name)
        p.add_argument("--repo", required=True)
        p.add_argument("--pr", type=int, required=True)
        p.add_argument("--output", required=True)
        p.add_argument("--wait-seconds", type=float, default=0)
    args = parser.parse_args(argv)
    gh = GitHub(args.repo, os.environ.get("GH_TOKEN", ""))

    def run(cmd: List[str]) -> Tuple[int, str]:
        done = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        return done.returncode, done.stdout

    if args.cmd == "verify-pr":
        verifier = gh_signature_verifier(run, repository=args.repo)

        def verify_signature(raw: bytes, workflow_path: str) -> Signed:
            with tempfile.NamedTemporaryFile("wb", suffix=".json", delete=False) as fh:
                fh.write(raw)
            try:
                return verifier(fh.name, workflow_path)
            finally:
                os.unlink(fh.name)

        result = verify_pr_waiting(gh, args.pr, verify_signature, wait_seconds=args.wait_seconds)
    else:
        try:
            head = gh.pr(args.pr)["head"]["sha"]
        except Exception as exc:  # noqa: BLE001
            result = {"passed": False, "reason": f"GITHUB_API_UNAVAILABLE:{type(exc).__name__}"}
        else:
            result = verify_prelive(gh, head)
    with open(args.output, "w", encoding="utf-8") as fh:
        json.dump(result, fh, indent=2, sort_keys=True)
    print(json.dumps(result, sort_keys=True))
    return 0 if result.get("passed") else 1


if __name__ == "__main__":
    raise SystemExit(main())
