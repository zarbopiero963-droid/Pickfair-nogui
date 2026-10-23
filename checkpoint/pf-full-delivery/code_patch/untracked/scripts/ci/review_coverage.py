#!/usr/bin/env python3
"""Copertura attestata delle review AI (PFC-002, requisito M159).

Il problema che chiude. I reviewer Sol e Grok tagliavano ogni file oltre un
tetto di caratteri (7000 e 5000) e saltavano i file oltre il budget totale del
prompt. Il job finiva comunque verde: una parte del diff poteva non essere mai
passata davanti a un revisore indipendente, e niente lo diceva al gate.

Cosa fa questo modulo, senza dipendenze esterne:

1. **Segmentazione deterministica.** Ogni patch si divide ai confini degli hunk
   (``@@``); un hunk piu' lungo del tetto si divide per righe, una riga piu'
   lunga per caratteri. Nessun carattere si perde: la concatenazione dei
   segmenti e' il patch originale, e ogni segmento ha il proprio sha256.
2. **Lotti.** I segmenti si impacchettano in lotti entro il budget del prompt,
   nell'ordine del diff. Un lotto = una chiamata al modello. Oltre
   ``max_calls`` lotti non si chiama nessun modello: si dichiara
   ``budget_exceeded``, invece di spendere su una review che resterebbe
   comunque incompleta.
3. **Riscontro del modello.** Il modello deve chiudere con la riga
   ``SEGMENTI ESAMINATI: <id>, <id>, ...``. Un lotto conta come revisionato solo
   se la risposta e' completa (niente troncamento, niente errore) e cita tutti i
   suoi segmenti.
4. **Attestazione.** Il risultato di un range (revisore, PR, range, run, file,
   segmenti con sha256, lotti, motivi di incompletezza) diventa un JSON che il
   workflow carica come artifact del proprio run. Un artifact non si modifica:
   si puo' solo cancellare, e allora manca, cioe' fail-closed.
5. **Verifica della catena.** Il verdetto finale guarda l'intera PR, non solo
   l'ultima push: dai run UFFICIALI del workflow del revisore (evento
   ``pull_request_target``, quindi codice del branch base, mai della PR) si
   raccolgono le attestazioni complete e si cerca una catena di range che parte
   da un commit del branch base e arriva al head corrente. Un'attestazione che
   arriva da un altro workflow, da un altro evento o da un altro head non conta.

Uso da riga di comando (nel workflow del revisore)::

    review_coverage.py verify --repo O/R --pr N --reviewer gpt56sol \
        --workflow-file pr-review-openrouter-gpt56-sol.yml \
        --base-sha <sha> --head-sha <sha> [--local-attestation f.json] \
        --output esito.json

Esce con 0 solo se la catena e' completa. Ogni errore di rete, di formato o di
autorizzazione e' un esito negativo, mai un verde.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import sys
import urllib.error
import urllib.request
import zipfile
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

SCHEMA = "pickfair-review-coverage/v1"
ARTIFACT_PREFIX = "review-coverage"
ACK_PREFIX = "SEGMENTI ESAMINATI:"
_ACK_RE = re.compile(r"^\s*\**\s*SEGMENTI ESAMINATI\s*\**\s*:\s*(.*)$", re.IGNORECASE | re.MULTILINE)
_SEG_ID_RE = re.compile(r"\bF\d{3}S\d{2,}\b")

# File senza patch testuale che nessun revisore diff-only puo' leggere. Sono
# dichiarati nell'attestazione e non bloccano: non contengono codice. Un file di
# TESTO senza patch (GitHub omette il patch oltre una certa dimensione) invece
# blocca: li' dentro c'e' codice che nessuno ha visto.
BINARY_SUFFIXES = (
    ".png", ".jpg", ".jpeg", ".gif", ".bmp", ".ico", ".webp", ".svgz",
    ".pdf", ".zip", ".gz", ".tgz", ".xz", ".bz2", ".7z",
    ".woff", ".woff2", ".ttf", ".otf", ".eot",
    ".mp3", ".mp4", ".wav", ".ogg", ".webm",
    ".exe", ".dll", ".so", ".dylib", ".bin", ".pyc",
    ".db", ".sqlite", ".sqlite3", ".xlsx", ".docx", ".pptx",
)


@dataclass(frozen=True)
class Segment:
    seg_id: str
    filename: str
    part: int
    parts: int
    text: str

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.text.encode("utf-8")).hexdigest()

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.seg_id,
            "file": self.filename,
            "part": self.part,
            "parts": self.parts,
            "chars": len(self.text),
            "sha256": self.sha256,
        }


@dataclass
class Plan:
    segments: list[Segment] = field(default_factory=list)
    batches: list[list[str]] = field(default_factory=list)
    unreviewable: list[str] = field(default_factory=list)
    binary: list[str] = field(default_factory=list)
    unchanged: list[str] = field(default_factory=list)
    budget_exceeded: bool = False
    max_calls: int = 0

    def by_id(self) -> dict[str, Segment]:
        return {s.seg_id: s for s in self.segments}


# --------------------------------------------------------------------------
# Segmentazione
# --------------------------------------------------------------------------

def _split_hunks(patch: str) -> list[str]:
    """Divide il patch ai confini degli hunk, senza perdere caratteri."""
    righe = patch.splitlines(keepends=True)
    blocchi: list[str] = []
    corrente: list[str] = []
    for riga in righe:
        if riga.startswith("@@") and corrente:
            blocchi.append("".join(corrente))
            corrente = []
        corrente.append(riga)
    if corrente:
        blocchi.append("".join(corrente))
    return blocchi


def _split_oversized(block: str, max_chars: int) -> list[str]:
    """Divide un blocco troppo lungo per righe, e una riga troppo lunga per caratteri."""
    pezzi: list[str] = []
    corrente = ""
    for riga in block.splitlines(keepends=True):
        while len(riga) > max_chars:
            if corrente:
                pezzi.append(corrente)
                corrente = ""
            pezzi.append(riga[:max_chars])
            riga = riga[max_chars:]
        if len(corrente) + len(riga) > max_chars and corrente:
            pezzi.append(corrente)
            corrente = ""
        corrente += riga
    if corrente:
        pezzi.append(corrente)
    return pezzi


def segment_patch(file_index: int, filename: str, patch: str, max_chars: int) -> list[Segment]:
    """Segmenti deterministici di un patch: stessa entrata, stessi segmenti."""
    if max_chars <= 0:
        raise ValueError("max_chars deve essere positivo")
    pezzi: list[str] = []
    corrente = ""
    for hunk in _split_hunks(patch):
        if len(hunk) > max_chars:
            if corrente:
                pezzi.append(corrente)
                corrente = ""
            pezzi.extend(_split_oversized(hunk, max_chars))
            continue
        if len(corrente) + len(hunk) > max_chars and corrente:
            pezzi.append(corrente)
            corrente = ""
        corrente += hunk
    if corrente:
        pezzi.append(corrente)
    totale = len(pezzi)
    return [
        Segment(f"F{file_index:03d}S{n:02d}", filename, n, totale, testo)
        for n, testo in enumerate(pezzi, start=1)
    ]


def render_segment(seg: Segment) -> str:
    return (
        f"\n\n### SEGMENTO {seg.seg_id} — FILE: {seg.filename} (parte {seg.part}/{seg.parts})\n"
        f"```diff\n{seg.text}\n```"
    )


def plan_review(
    files: Iterable[dict[str, Any]],
    *,
    max_segment_chars: int,
    max_batch_chars: int,
    max_calls: int,
) -> Plan:
    """Segmenti e lotti di un range. `files` porta `filename`, `patch` (gia'
    redatto e neutralizzato) e opzionalmente `changes`.

    Il budget di un lotto conta il testo esatto che finira' nel prompt
    (`render_segment`), cosi' nessun lotto supera il tetto dopo l'impaginazione.
    """
    if max_segment_chars <= 0 or max_batch_chars <= 0 or max_calls <= 0:
        raise ValueError("i tetti devono essere positivi")
    plan = Plan(max_calls=max_calls)
    for index, f in enumerate(files, start=1):
        filename = str(f.get("filename") or "")
        patch = f.get("patch")
        if not patch:
            if int(f.get("changes") or 0) == 0:
                plan.unchanged.append(filename)  # rename o permessi: nessuna riga
            elif filename.lower().endswith(BINARY_SUFFIXES):
                plan.binary.append(filename)
            else:
                plan.unreviewable.append(filename)
            continue
        plan.segments.extend(segment_patch(index, filename, str(patch), max_segment_chars))

    corrente: list[str] = []
    usato = 0
    for seg in plan.segments:
        dimensione = len(render_segment(seg))
        if dimensione > max_batch_chars:
            raise ValueError(
                f"segmento {seg.seg_id} ({dimensione} caratteri) oltre il budget del lotto "
                f"({max_batch_chars}): max_segment_chars deve stare sotto max_batch_chars"
            )
        if corrente and usato + dimensione > max_batch_chars:
            plan.batches.append(corrente)
            corrente, usato = [], 0
        corrente.append(seg.seg_id)
        usato += dimensione
    if corrente:
        plan.batches.append(corrente)
    plan.budget_exceeded = len(plan.batches) > max_calls
    return plan


def render_batch(plan: Plan, batch_index: int) -> str:
    segmenti = plan.by_id()
    return "".join(render_segment(segmenti[sid]) for sid in plan.batches[batch_index])


def ack_instruction(batch_ids: list[str]) -> str:
    return (
        "OBBLIGATORIO, come ULTIMA riga della risposta, esattamente:\n"
        f"{ACK_PREFIX} " + ", ".join(batch_ids) + "\n"
        "elencando SOLO i segmenti che hai davvero letto per intero. Un segmento che non "
        "hai letto NON va elencato: la review di quel segmento verra' rifatta."
    )


def parse_ack(text: str) -> set[str]:
    """Id dei segmenti che il modello dichiara di aver letto (ultima riga di riscontro)."""
    trovati = _ACK_RE.findall(text or "")
    if not trovati:
        return set()
    return set(_SEG_ID_RE.findall(trovati[-1]))


def batch_outcome(text: str, *, completed: bool, expected: list[str]) -> dict[str, Any]:
    """Esito di un lotto: revisionato solo se la risposta e' completa e cita tutto."""
    citati = parse_ack(text) if completed else set()
    mancanti = [sid for sid in expected if sid not in citati]
    return {
        "segments": list(expected),
        "response_complete": bool(completed),
        "acknowledged": sorted(citati & set(expected)),
        "missing": mancanti,
        "reviewed": bool(completed) and not mancanti,
    }


# --------------------------------------------------------------------------
# Attestazione
# --------------------------------------------------------------------------

def build_attestation(
    *,
    reviewer: str,
    model: str,
    repo: str,
    pr: int,
    range_base: str,
    range_head: str,
    run_id: int,
    run_attempt: int,
    workflow_file: str,
    plan: Plan | None,
    outcomes: list[dict[str, Any]] | None,
    compare_truncated: bool = False,
    excluded_from_base: list[str] | None = None,
    reasons: list[str] | None = None,
) -> dict[str, Any]:
    motivi = list(reasons or [])
    outcomes = list(outcomes or [])
    if plan is None:
        motivi.append("nessun piano di copertura: review non eseguita")
        plan = Plan()
    if compare_truncated:
        motivi.append("Compare API al tetto di 300 file: il range non e' interamente visibile")
    if plan.budget_exceeded:
        motivi.append(
            f"servono {len(plan.batches)} chiamate, tetto {plan.max_calls}: nessun modello chiamato"
        )
    if plan.unreviewable:
        motivi.append(
            "file di testo senza patch (GitHub non lo fornisce): " + ", ".join(sorted(plan.unreviewable)[:20])
        )
    if not plan.budget_exceeded and plan.segments:
        if len(outcomes) != len(plan.batches):
            motivi.append(f"lotti revisionati {len(outcomes)} su {len(plan.batches)}")
        for numero, esito in enumerate(outcomes, start=1):
            if not esito.get("reviewed"):
                if not esito.get("response_complete"):
                    motivi.append(f"lotto {numero}: risposta incompleta (troncata, vuota o errore)")
                else:
                    motivi.append(
                        f"lotto {numero}: segmenti non riscontrati dal modello: "
                        + ", ".join(esito.get("missing") or [])
                    )
    revisionati = sorted({sid for e in outcomes if e.get("reviewed") for sid in e.get("segments") or []})
    tutti = [s.seg_id for s in plan.segments]
    non_revisionati = [sid for sid in tutti if sid not in set(revisionati)]
    if non_revisionati and not motivi:
        motivi.append("segmenti senza review: " + ", ".join(non_revisionati[:20]))
    return {
        "schema": SCHEMA,
        "reviewer": reviewer,
        "model": model,
        "repo": repo,
        "pr": int(pr),
        "range_base": range_base,
        "range_head": range_head,
        "run_id": int(run_id),
        "run_attempt": int(run_attempt),
        "workflow_file": workflow_file,
        "segments": [s.as_dict() for s in plan.segments],
        "batches": [list(b) for b in plan.batches],
        "outcomes": outcomes,
        "reviewed_segments": revisionati,
        "unreviewed_segments": non_revisionati,
        "binary_files": sorted(plan.binary),
        "unchanged_files": sorted(plan.unchanged),
        "unreviewable_files": sorted(plan.unreviewable),
        "excluded_from_base": sorted(excluded_from_base or []),
        "complete": not motivi,
        "reasons": motivi,
    }


def artifact_name(reviewer: str, range_base: str, range_head: str, run_attempt: int) -> str:
    return f"{ARTIFACT_PREFIX}-{reviewer}-{range_base[:12]}-{range_head[:12]}-a{int(run_attempt)}"


def attestation_valid(att: Any, *, reviewer: str, pr: int, repo: str) -> list[str]:
    """Motivi per cui un'attestazione non si puo' usare (lista vuota = usabile)."""
    if not isinstance(att, dict):
        return ["attestazione non e' un oggetto JSON"]
    errori = []
    if att.get("schema") != SCHEMA:
        errori.append(f"schema {att.get('schema')!r} diverso da {SCHEMA!r}")
    if att.get("reviewer") != reviewer:
        errori.append(f"revisore {att.get('reviewer')!r}, atteso {reviewer!r}")
    if att.get("repo") != repo:
        errori.append(f"repository {att.get('repo')!r}, atteso {repo!r}")
    try:
        if int(att.get("pr")) != int(pr):
            errori.append(f"PR {att.get('pr')!r}, attesa {pr}")
    except (TypeError, ValueError):
        errori.append("numero PR assente o malformato")
    for chiave in ("range_base", "range_head"):
        if not re.fullmatch(r"[0-9a-f]{40}", str(att.get(chiave) or "")):
            errori.append(f"{chiave} non e' uno SHA completo")
    if att.get("complete") is not True:
        errori.append("attestazione dichiarata incompleta: " + "; ".join(att.get("reasons") or []))
    elif att.get("reasons"):
        errori.append("attestazione completa ma con motivi di incompletezza: incoerente")
    elif att.get("unreviewed_segments"):
        errori.append("attestazione completa ma con segmenti non revisionati: incoerente")
    return errori


# --------------------------------------------------------------------------
# Verifica della catena sull'intera PR
# --------------------------------------------------------------------------

def verify_chain(
    attestations: list[dict[str, Any]],
    *,
    head_sha: str,
    is_root: Callable[[str], bool],
) -> dict[str, Any]:
    """Cerca una catena di range completi da un commit del branch base al head.

    Ogni range `a...b` revisionato copre le righe cambiate fra a e b. Una catena
    `r0 -> ... -> head` con `r0` nel branch base copre ogni riga del diff della
    PR: una riga del diff finale e' stata scritta da una push, e quella push e'
    un anello della catena.
    """
    archi: dict[str, list[dict[str, Any]]] = {}
    for att in attestations:
        archi.setdefault(att["range_head"], []).append(att)
    # visita all'indietro dal head: da ogni nodo si va al range_base dei range che vi arrivano
    visti: set[str] = set()
    frontiera = [(head_sha, [])]
    while frontiera:
        nodo, cammino = frontiera.pop(0)
        if nodo in visti:
            continue
        visti.add(nodo)
        for att in archi.get(nodo, []):
            nuovo = [att] + cammino
            if is_root(att["range_base"]):
                return {
                    "ok": True,
                    "chain": [f"{a['range_base'][:12]}...{a['range_head'][:12]}" for a in nuovo],
                    "reasons": [],
                }
            frontiera.append((att["range_base"], nuovo))
    if head_sha not in archi:
        motivo = f"nessuna review completa che termini sul head corrente {head_sha[:12]}"
    else:
        motivo = (
            "le review complete non formano una catena dal branch base al head: c'e' almeno "
            "una push senza review completa"
        )
    return {"ok": False, "chain": [], "reasons": [motivo]}


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: D401
        return None


class GitHub:
    """Client minimo per l'API GitHub: lettura sola, token del workflow."""

    def __init__(self, repo: str, token: str, api: str = "https://api.github.com"):
        self.repo = repo
        self.token = token.strip()
        self.api = api.rstrip("/")

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "pickfair-review-coverage",
        }

    def get(self, path: str) -> Any:
        req = urllib.request.Request(f"{self.api}{path}", headers=self._headers())
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8")
        return json.loads(raw) if raw else None

    def download(self, url: str) -> bytes:
        """Scarica un artifact. GitHub risponde 302 verso uno storage firmato: il
        token NON va inoltrato li' (lo storage rifiuta le richieste con due firme)."""
        opener = urllib.request.build_opener(_NoRedirect)
        req = urllib.request.Request(url, headers=self._headers())
        try:
            with opener.open(req, timeout=60) as resp:
                return resp.read()
        except urllib.error.HTTPError as exc:
            if exc.code not in (301, 302, 303, 307, 308):
                raise
            destinazione = exc.headers.get("Location")
            if not destinazione:
                raise
        with urllib.request.urlopen(destinazione, timeout=120) as resp:
            return resp.read()


def _pr_commit_shas(gh: GitHub, pr: int) -> set[str]:
    shas: set[str] = set()
    for page in range(1, 4):
        commits = gh.get(f"/repos/{gh.repo}/pulls/{pr}/commits?per_page=100&page={page}") or []
        shas.update(c.get("sha") for c in commits if c.get("sha"))
        if len(commits) < 100:
            return shas
    raise RuntimeError("PR con oltre 300 commit: la lista non e' completa, verifica impossibile")


def official_attestations(
    gh: GitHub,
    *,
    pr: int,
    reviewer: str,
    workflow_file: str,
    pr_commits: set[str],
) -> tuple[list[dict[str, Any]], list[str]]:
    """Attestazioni prodotte dai run UFFICIALI del workflow del revisore.

    Un run conta solo se: e' del file workflow atteso, l'evento e'
    `pull_request_target` (codice del branch base, non della PR) e il suo head e'
    un commit della PR. L'attestazione dentro l'artifact deve poi nominare lo
    stesso run e lo stesso head. Un workflow aggiunto dalla PR, un evento
    `pull_request` o un artifact con lo stesso nome caricato altrove non entrano.
    """
    trovate: list[dict[str, Any]] = []
    scarti: list[str] = []
    percorso = f".github/workflows/{workflow_file}"
    for page in range(1, 11):
        dati = gh.get(
            f"/repos/{gh.repo}/actions/workflows/{workflow_file}/runs"
            f"?event=pull_request_target&per_page=100&page={page}"
        ) or {}
        runs = dati.get("workflow_runs") or []
        for run in runs:
            if run.get("head_sha") not in pr_commits:
                continue
            if run.get("event") != "pull_request_target" or str(run.get("path") or "").split("@")[0] != percorso:
                scarti.append(f"run {run.get('id')}: evento o percorso non ufficiale")
                continue
            artifacts = (gh.get(f"/repos/{gh.repo}/actions/runs/{run['id']}/artifacts?per_page=100") or {}).get(
                "artifacts"
            ) or []
            for art in artifacts:
                if not str(art.get("name") or "").startswith(f"{ARTIFACT_PREFIX}-{reviewer}-"):
                    continue
                if art.get("expired"):
                    scarti.append(f"run {run['id']}: artifact {art.get('name')} scaduto")
                    continue
                try:
                    contenuto = gh.download(art["archive_download_url"])
                    with zipfile.ZipFile(io.BytesIO(contenuto)) as zf:
                        nomi = [n for n in zf.namelist() if n.endswith(".json")]
                        if len(nomi) != 1:
                            raise ValueError(f"{len(nomi)} file JSON nell'artifact, atteso 1")
                        att = json.loads(zf.read(nomi[0]).decode("utf-8"))
                except Exception as exc:  # artifact illeggibile = non conta
                    scarti.append(f"run {run['id']}: artifact illeggibile ({str(exc)[:120]})")
                    continue
                errori = attestation_valid(att, reviewer=reviewer, pr=pr, repo=gh.repo)
                if int(att.get("run_id") or 0) != int(run["id"]):
                    errori.append("l'attestazione nomina un altro run")
                if att.get("range_head") != run.get("head_sha"):
                    errori.append("l'attestazione nomina un head diverso da quello del run")
                if errori:
                    scarti.append(f"run {run['id']}: " + "; ".join(errori))
                    continue
                trovate.append(att)
        if len(runs) < 100:
            break
    return trovate, scarti


def _root_checker(gh: GitHub, base_sha: str, pr_commits: set[str]) -> Callable[[str], bool]:
    cache: dict[str, bool] = {}

    def is_root(sha: str) -> bool:
        if sha == base_sha:
            return True
        if sha in pr_commits:
            return False
        if sha not in cache:
            try:
                stato = (gh.get(f"/repos/{gh.repo}/compare/{sha}...{base_sha}") or {}).get("status")
            except Exception:
                stato = None
            # `ahead`/`identical`: il base contiene `sha`, quindi `sha` e' un commit del branch base
            cache[sha] = stato in ("ahead", "identical")
        return cache[sha]

    return is_root


def cmd_verify(args: argparse.Namespace) -> int:
    esito: dict[str, Any] = {
        "schema": SCHEMA,
        "reviewer": args.reviewer,
        "pr": args.pr,
        "head_sha": args.head_sha,
        "base_sha": args.base_sha,
        "ok": False,
    }
    try:
        token = os.environ.get("GITHUB_TOKEN", "")
        if not token:
            raise RuntimeError("GITHUB_TOKEN assente")
        gh = GitHub(args.repo, token)
        commits = _pr_commit_shas(gh, args.pr)
        if args.head_sha not in commits:
            raise RuntimeError("il head indicato non e' un commit della PR")
        atts, scarti = official_attestations(
            gh, pr=args.pr, reviewer=args.reviewer, workflow_file=args.workflow_file, pr_commits=commits
        )
        if args.local_attestation and os.path.exists(args.local_attestation):
            with open(args.local_attestation, encoding="utf-8") as fh:
                locale = json.load(fh)
            errori = attestation_valid(locale, reviewer=args.reviewer, pr=args.pr, repo=args.repo)
            if not errori and locale.get("range_head") == args.head_sha:
                atts.append(locale)
            elif errori:
                scarti.append("attestazione di questo run: " + "; ".join(errori))
        catena = verify_chain(atts, head_sha=args.head_sha, is_root=_root_checker(gh, args.base_sha, commits))
        esito.update(catena)
        esito["attestations_used"] = len(atts)
        esito["discarded"] = scarti
    except Exception as exc:
        esito["reasons"] = [f"verifica non eseguibile: {str(exc)[:300]}"]
    if args.output:
        with open(args.output, "w", encoding="utf-8") as fh:
            json.dump(esito, fh, indent=2, ensure_ascii=False)
    print(json.dumps(esito, indent=2, ensure_ascii=False))
    return 0 if esito.get("ok") is True else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="cmd", required=True)
    v = sub.add_parser("verify", help="verifica la copertura dell'intera PR")
    v.add_argument("--repo", required=True)
    v.add_argument("--pr", type=int, required=True)
    v.add_argument("--reviewer", required=True)
    v.add_argument("--workflow-file", required=True)
    v.add_argument("--base-sha", required=True)
    v.add_argument("--head-sha", required=True)
    v.add_argument("--local-attestation", default="")
    v.add_argument("--output", default="")
    args = parser.parse_args(argv)
    if args.cmd == "verify":
        return cmd_verify(args)
    return 2


if __name__ == "__main__":
    sys.exit(main())
