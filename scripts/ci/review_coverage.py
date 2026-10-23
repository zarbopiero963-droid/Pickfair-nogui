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
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

SCHEMA = "pickfair-review-coverage/v1"
ARTIFACT_PREFIX = "review-coverage"
ACK_PREFIX = "SEGMENTI ESAMINATI:"
_ACK_RE = re.compile(r"^\s*\**\s*SEGMENTI ESAMINATI\s*\**\s*:\s*(.*?)\s*\**\s*$", re.IGNORECASE)
_SEG_ID_RE = re.compile(r"\bF\d{3}S\d{2,}\b")

# File senza patch testuale che nessun revisore diff-only puo' leggere. Rendono
# INCOMPLETA l'attestazione, come un file di testo senza patch: un eseguibile, un
# database o un archivio non sono stati visti da nessuno, e AGENTS.md vieta i
# binari salvo richiesta (rilievo Codex su #539). Si distinguono solo nel motivo,
# cosi' l'owner sa che cosa decidere. Restano fuori solo i rename senza modifiche.
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


MAX_SEGMENTS_PER_BATCH = 40


def plan_review(
    files: Iterable[dict[str, Any]],
    *,
    max_segment_chars: int,
    max_batch_chars: int,
    max_calls: int,
    max_segments_per_batch: int = MAX_SEGMENTS_PER_BATCH,
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
        # Anche un tetto sul NUMERO di segmenti: con molti file piccoli un lotto
        # porterebbe centinaia di id da riscontrare, e il riscontro non starebbe
        # nella risposta corta richiesta al modello (rilievo Codex su #539).
        if corrente and (usato + dimensione > max_batch_chars or len(corrente) >= max_segments_per_batch):
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


_SEZIONE = {titolo: re.compile(rf"^##\s*{titolo}\s*$(.*?)(?=^##\s|\Z)", re.MULTILINE | re.DOTALL)
            for titolo in ("Bloccanti", "Verdetto finale")}
_NESSUN_BLOCCANTE = re.compile(
    r"[-*]?\s*nessun(?:o)?\s+bloccante(?:\s+(?:evidente|rilevato|rilevante|trovato|individuato))?\s*[.!]?",
    re.IGNORECASE)


def aggregate_reviews(lotti: list[dict[str, Any]]) -> dict[str, str]:
    """Una sola coppia canonica `## Bloccanti` / `## Verdetto finale` per range.

    Con piu' lotti ogni risposta porta le sue due sezioni: incollate una dopo
    l'altra, il commento avrebbe sezioni duplicate, e il lettore delle review
    (`pr_autonomy_policy._section`) tratta una sezione duplicata come schema
    inatteso, UNKNOWN (rilievo Codex su #539). Qui i bloccanti di tutti i lotti
    finiscono in un'unica sezione, ciascuno etichettato col suo lotto; un lotto
    senza una sezione Bloccanti unica, o senza un Verdetto finale unico e non
    vuoto, e' esso stesso un bloccante (non si legge, quindi non e' pulito). Il testo integrale dei lotti resta nei dettagli, con i
    titoli abbassati a `####`, cosi' non crea sezioni di primo livello.

    `lotti`: dict con `indice` (1-based), `totale`, `text` e, facoltativo,
    `completo` (False = risposta incompleta: niente marcatori sintetici).
    """
    bloccanti: list[str] = []
    verdetti: list[str] = []
    dettagli: list[str] = []
    for lotto in lotti:
        tag = f"[lotto {lotto['indice']}/{lotto['totale']}]"
        testo = str(lotto.get("text") or "")
        # la riga di riscontro chiude la risposta e cade dentro l'ultima sezione:
        # non e' contenuto della review, quindi non conta per nessuna delle due
        corpo = re.sub(r"(?im)^\s*\**\s*SEGMENTI ESAMINATI\s*\**\s*:.*$", "", testo)
        # Risposta incompleta (errore del provider, troncamento): la copertura
        # e' gia' incompleta per questo, e un marcatore sintetico diventerebbe
        # un bloccante persistente da triagiare dopo ogni errore transitorio
        # (terzo audit su #539). I bloccanti reali del testo parziale restano.
        completo = lotto.get("completo", True) is not False
        sezioni = _SEZIONE["Bloccanti"].findall(corpo)
        contenuto = sezioni[0].strip() if len(sezioni) == 1 else None
        if not contenuto:
            if completo:
                bloccanti.append(f"- {tag} sezione Bloccanti assente, vuota o duplicata: lotto non leggibile")
        elif not _NESSUN_BLOCCANTE.fullmatch(contenuto):
            for riga in contenuto.splitlines():
                riga = riga.strip().lstrip("-*").strip()
                if riga:
                    bloccanti.append(f"- {tag} {riga}")
        ver = _SEZIONE["Verdetto finale"].findall(corpo)
        if len(ver) == 1 and ver[0].strip():
            verdetti.append(f"- {tag} {' '.join(ver[0].split())}")
        elif not completo:
            verdetti.append(f"- {tag} (risposta incompleta: lotto non revisionato)")
        else:
            # Verdetto assente, vuoto o duplicato = lotto malformato, quindi non
            # pulito: normalizzarlo in silenzio farebbe sembrare VALID_CLEAN una
            # review che il gate tratterebbe come UNKNOWN (rilievo GPT-6.1 Sol su #539).
            bloccanti.append(f"- {tag} sezione Verdetto finale assente, vuota o duplicata: lotto non leggibile")
            verdetti.append(f"- {tag} (verdetto non leggibile)")
        dettagli.append(re.sub(r"(?m)^#+[ \t]*", "#### ", testo))
    return {
        "bloccanti": "\n".join(bloccanti) if bloccanti else "Nessun bloccante evidente",
        # gli stessi bloccanti, senza il trattino: vanno nell'attestazione e
        # rendono rossa la verifica (rilievo Codex su #539)
        "lista_bloccanti": [b[2:] for b in bloccanti],
        "verdetto": "\n".join(verdetti) if verdetti else "(nessun lotto)",
        "dettagli": dettagli,
    }


def ack_instruction(batch_ids: list[str]) -> str:
    return (
        "OBBLIGATORIO, come ULTIMA riga della risposta (NON conta nel limite di parole), esattamente:\n"
        f"{ACK_PREFIX} " + ", ".join(batch_ids) + "\n"
        "elencando SOLO i segmenti che hai davvero letto per intero. Un segmento che non "
        "hai letto NON va elencato: la review di quel segmento verra' rifatta."
    )


def parse_ack(text: str) -> set[str]:
    """Id dei segmenti che il modello dichiara di aver letto.

    Vale solo l'ULTIMA riga non vuota della risposta (rilievo Codex su #539): un
    riscontro scritto prima di un rifiuto, di un errore o di altro testo non
    chiude la review, e quindi non conta.
    """
    righe = [r for r in (text or "").splitlines() if r.strip()]
    if not righe:
        return set()
    trovato = _ACK_RE.match(righe[-1])
    return set(_SEG_ID_RE.findall(trovato.group(1))) if trovato else set()


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
    blocking_findings: list[str] | None = None,
) -> dict[str, Any]:
    """`blocking_findings`: i bloccanti della review aggregata (lista vuota =
    review pulita). `None` = esito non registrato: la verifica lo tratta come
    rosso, mai come pulito."""
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
    if plan.binary:
        motivi.append(
            "file binari che nessun revisore diff-only puo' leggere (decisione owner): "
            + ", ".join(sorted(plan.binary)[:20])
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
        "blocking_findings": None if blocking_findings is None else [str(b) for b in blocking_findings],
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


def _numeri_pr(run: dict[str, Any]) -> set[int]:
    numeri = set()
    for voce in run.get("pull_requests") or []:
        valore = voce.get("number") if isinstance(voce, dict) else voce
        try:
            numeri.add(int(valore))
        except (TypeError, ValueError):
            continue
    return numeri


def _run_della_pr(run: dict[str, Any], pr: int, pr_commits: set[str]) -> bool:
    return run.get("head_sha") in pr_commits or int(pr) in _numeri_pr(run)


_ACCODATO = ("queued", "waiting", "pending", "requested")


def runs_in_corso(gh: "GitHub", *, pr: int, workflow_file: str, pr_commits: set[str],
                  escludi_run: int = 0, head_ref: str = "", head_sha: str = "") -> list[int]:
    """Run ufficiali della PR non ancora conclusi (escluso quello corrente).

    Stessa associazione dei bloccanti (`_run_associato`): conta anche il run
    legato alla PR solo dal branch, dopo un force-push (terzo audit su #539).
    Un run ACCODATO sullo stesso head non si aspetta (quarto audit): e' nella
    stessa concurrency di questo, parte solo dopo e vedra' tutto; aspettarlo
    dava un rosso a vuoto.
    """
    dati = gh.get(
        f"/repos/{gh.repo}/actions/workflows/{workflow_file}/runs?event=pull_request_target&per_page=100&page=1"
    ) or {}
    return [int(r["id"]) for r in dati.get("workflow_runs") or []
            if _run_associato(r, pr, pr_commits, head_ref) and r.get("status") != "completed"
            and not (head_sha and r.get("head_sha") == head_sha and r.get("status") in _ACCODATO)
            and int(r.get("id") or 0) != int(escludi_run)]


def range_attested(gh: "GitHub", *, pr: int, reviewer: str, workflow_file: str,
                   range_base: str, range_head: str) -> bool:
    """Esiste gia' un'attestazione ufficiale e completa per questo range?

    Serve alla dedup a pagamento: il done_marker del commento non basta,
    perche' l'artifact puo' mancare (upload fallito, scaduto, cancellato) e
    allora saltare la review lascerebbe il check rosso per sempre (rilievo
    Codex su #539). Ogni errore => False: nel dubbio si rifa' la review.
    """
    try:
        commits = _pr_commit_shas(gh, pr)
        atts, _ = official_attestations(gh, pr=pr, reviewer=reviewer, workflow_file=workflow_file,
                                        pr_commits=commits)
    except Exception:
        return False
    return any(a["range_base"] == range_base and a["range_head"] == range_head for a in atts)


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
    `pull_request_target` (codice del branch base, non della PR) ed e' associato
    alla PR (head fra i commit della PR, oppure la PR fra le `pull_requests` del
    run: rilievo Codex su #539, che per `pull_request_target` vede l'head del
    run sul branch base; misurato su #538/#539 l'API riporta l'head della PR,
    ma l'associazione non deve dipendere da questo). L'attestazione dentro
    l'artifact deve nominare lo stesso run e un head che e' commit della PR. Un
    workflow aggiunto dalla PR, un evento `pull_request` o un artifact con lo
    stesso nome caricato altrove non entrano.
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
            if not _run_della_pr(run, pr, pr_commits):
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
                if att.get("range_head") not in pr_commits:
                    errori.append("l'attestazione nomina un head che non e' un commit della PR")
                elif run.get("head_sha") in pr_commits and att.get("range_head") != run.get("head_sha"):
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


def esito_non_registrato_sul_head(attestations: list[dict[str, Any]], head_sha: str) -> list[str]:
    """La review sul head deve aver registrato il proprio esito (lista, anche vuota)."""
    return [
        f"range {str(att.get('range_base'))[:12]}...{head_sha[:12]}: esito della review non registrato "
        "nell'attestazione"
        for att in attestations
        if att.get("range_head") == head_sha and not isinstance(att.get("blocking_findings"), list)
    ]


# --------------------------------------------------------------------------
# Persistenza dei bloccanti (decisione owner D1-D4 su #539)
# --------------------------------------------------------------------------
# Un bloccante trovato da un revisore resta aperto per tutta la vita della PR:
# attraverso rerun, push successivi, commit vuoti, merge di main, modifiche di
# titolo/descrizione e force-push. Si chiude SOLO con il triage dell'owner.
# La chiusura automatica "da review" non c'e': una review successiva pulita che
# non cita il bloccante non prova che sia stato corretto (decisione owner:
# senza una chiusura automatica dimostrabile vale la regola fail-closed).

TRIAGE_MARKER = re.compile(r"<!--\s*pfc-triage-bloccante:([a-z0-9]+):([0-9a-f]{12})\s*-->")
MIN_MOTIVAZIONE = 20


def blocker_id(reviewer: str, testo: str, dove: str) -> str:
    """Id di un bloccante: revisore, testo e DOVE e' stato trovato.

    Non dipende dagli spazi o dalle maiuscole. Dipende dal range (secondo
    audit su #539) e dall'etichetta del lotto (quarto audit): senza, un triage
    dell'owner chiudeva ogni ripetizione dello stesso testo, compreso il
    bloccante sintetico «lotto non leggibile» di un ALTRO lotto, che nasconde il
    contenuto vero di quel lotto. Il piano dei lotti e' deterministico per lo
    stesso range, quindi l'id resta lo stesso fra i rerun.
    """
    corpo = " ".join(str(testo).split()).lower()
    return hashlib.sha256(f"{reviewer}\n{dove}\n{corpo}".encode("utf-8")).hexdigest()[:12]


def _run_associato(run: dict[str, Any], pr: int, pr_commits: set[str], head_ref: str) -> bool:
    return _run_della_pr(run, pr, pr_commits) or (bool(head_ref) and run.get("head_branch") == head_ref)


def attestazioni_con_bloccanti(
    gh: GitHub,
    *,
    pr: int,
    reviewer: str,
    workflow_file: str,
    pr_commits: set[str],
    head_ref: str,
    created_since: str,
    collegate: Iterable[dict[str, Any]] = (),
    max_pages: int = 10,
) -> tuple[list[dict[str, Any]], list[str], list[str], list[dict[str, Any]]]:
    """TUTTE le attestazioni ufficiali della PR, complete o no, di ogni tentativo.

    Differenze da `official_attestations`, che serve alla catena di copertura:
    - le incomplete contano (D1: un lotto troncato non cancella il bloccante
      trovato da un altro lotto, e il rerun non lo cancella);
    - contano anche i range sui commit tolti da un force-push, purche' il run
      sia della PR (`pull_requests` o branch della PR);
    - contano le attestazioni di QUALUNQUE PR che terminano su un commit di
      questa (decisione owner sul terzo audit di #539): chiudere la PR e
      riaprirla con gli stessi commit non azzera i bloccanti. I run si cercano
      anche per SHA, perche' quelli della PR chiusa sono nati prima di questa;
    - cio' che impedisce di leggerle tutte e' rosso, non uno scarto
      silenzioso: un'attestazione non letta puo' contenere bloccanti. Un
      artifact scaduto o illeggibile diventa un bloccante con il suo id, che
      l'owner puo' triagiare (altrimenti la PR resterebbe rossa per sempre);
      lo storico dei run oltre il limite di pagine e' rosso senza id.

    Ritorna (attestazioni, motivi_di_rosso, note, bloccanti_illeggibili).
    """
    trovate: list[dict[str, Any]] = []
    motivi: list[str] = []
    note: list[str] = []
    illeggibili: list[dict[str, Any]] = []

    def illeggibile(run_id: Any, nome: Any, testo: str, dettaglio: str = "") -> None:
        # l'id non dipende dal dettaglio (messaggio d'errore), che puo' cambiare fra due verifiche
        dove = f"run {run_id} artifact {nome}"
        illeggibili.append({"id": blocker_id(reviewer, testo, dove), "range": dove,
                            "text": f"{testo} ({dettaglio})" if dettaglio else testo})

    percorso = f".github/workflows/{workflow_file}"
    base_url = f"/repos/{gh.repo}/actions/workflows/{workflow_file}/runs?event=pull_request_target&per_page=100"
    candidati: dict[str, dict[str, Any]] = {}
    # 1) i run della PR dalla sua creazione (anche dopo un force-push)
    filtro = f"&created=%3E%3D{created_since}" if created_since else ""
    letto_per_intero = False
    for page in range(1, max_pages + 1):
        runs = (gh.get(f"{base_url}&page={page}{filtro}") or {}).get("workflow_runs") or []
        for run in runs:
            if _run_associato(run, pr, pr_commits, head_ref):
                candidati.setdefault(str(run.get("id")), run)
        if len(runs) < 100:
            letto_per_intero = True
            break
    if not letto_per_intero:
        motivi.append(f"storico dei run non letto per intero (oltre {max_pages} pagine): bloccanti non verificabili")
    # 2) i run di qualunque PR su un commit di questa, anche precedenti alla sua creazione
    for sha in sorted(pr_commits):
        runs = (gh.get(f"{base_url}&head_sha={sha}&page=1") or {}).get("workflow_runs") or []
        if len(runs) >= 100:
            motivi.append(f"oltre 100 run sul commit {sha[:12]}: bloccanti non verificabili")
        for run in runs:
            if run.get("head_sha") == sha:
                candidati.setdefault(str(run.get("id")), run)
    # 3) i run delle PR storiche collegate (`pr_collegate`), dal LORO branch e solo
    #    nella finestra in cui erano aperte: un force-push le ha tolto i commit
    #    (ciclo 11 su #539), e un branch riusato in un'altra epoca non e' loro.
    numeri_collegati = set()
    for altra in collegate:
        numeri_collegati.add(str(altra.get("number")))
        ref = str((altra.get("head") or {}).get("ref") or "")
        finestra = f"{altra.get('created_at') or '*'}..{altra.get('closed_at') or '*'}"
        letta = False
        for page in range(1, max_pages + 1):
            runs = (gh.get(f"{base_url}&branch={urllib.parse.quote(ref, safe='')}&created={finestra}&page={page}")
                    or {}).get("workflow_runs") or []
            for run in runs:
                if run.get("head_branch") == ref:
                    candidati.setdefault(str(run.get("id")), run)
            if len(runs) < 100:
                letta = True
                break
        if not letta:
            motivi.append(f"storico dei run della PR #{altra.get('number')} non letto per intero: "
                          "bloccanti non verificabili")
    for run in candidati.values():
        if run.get("event") != "pull_request_target" or str(run.get("path") or "").split("@")[0] != percorso:
            continue
        artifacts = (gh.get(f"/repos/{gh.repo}/actions/runs/{run['id']}/artifacts?per_page=100") or {}).get(
            "artifacts"
        ) or []
        for art in artifacts:
            if not str(art.get("name") or "").startswith(f"{ARTIFACT_PREFIX}-{reviewer}-"):
                continue
            if art.get("expired"):
                illeggibile(run["id"], art.get("name"), "artifact scaduto: i suoi bloccanti non sono leggibili")
                continue
            try:
                contenuto = gh.download(art["archive_download_url"])
                with zipfile.ZipFile(io.BytesIO(contenuto)) as zf:
                    nomi = [n for n in zf.namelist() if n.endswith(".json")]
                    if len(nomi) != 1:
                        raise ValueError(f"{len(nomi)} file JSON nell'artifact, atteso 1")
                    att = json.loads(zf.read(nomi[0]).decode("utf-8"))
            except Exception as exc:  # non letto = bloccanti non verificabili
                illeggibile(run["id"], art.get("name"), "artifact illeggibile, bloccanti non verificabili",
                            str(exc)[:120])
                continue
            della_pr = isinstance(att, dict) and (str(att.get("pr")) == str(pr)
                                                  or str(att.get("pr")) in numeri_collegati
                                                  or att.get("range_head") in pr_commits)
            if (not della_pr or att.get("schema") != SCHEMA or att.get("reviewer") != reviewer
                    or att.get("repo") != gh.repo or str(att.get("run_id")) != str(run["id"])):
                note.append(f"run {run['id']}: artifact {art.get('name')} non e' un'attestazione di questa PR "
                            "ne' dei suoi commit")
                continue
            trovate.append(att)
    return trovate, motivi, note, illeggibili


def pr_collegate(gh: GitHub, *, pr: int, pr_commits: set[str], head_ref: str,
                 max_pr: int = 10) -> tuple[list[dict[str, Any]], list[str]]:
    """PR storiche pertinenti: NON mergiate, collegate a questa per branch o per commit.

    Ciclo 11 su #539 (deroga owner). Un bloccante non deve sparire chiudendo
    una PR e aprendone un'altra dopo un force-push. Legami verificabili, in
    modo transitivo:
    - stesso branch (`/pulls?state=closed&head=<owner>:<branch>`): copre la
      nuova PR sullo stesso branch, anche con commit riscritti (rebase);
    - un commit in comune (`/commits/<sha>/pulls`, che elenca anche le PR
      chiuse): copre la nuova PR su un branch diverso che riusa un commit.
    Le PR mergiate non trasmettono bloccanti: sono passate dal merge owner.
    NON e' collegabile una PR nuova su un branch diverso SENZA commit in
    comune (cherry-pick, riscrittura): per costruzione e' indistinguibile da
    una PR indipendente. Limite dichiarato, coperto dalla policy.
    Oltre `max_pr` PR collegate: rosso (motivo), non una lettura parziale.
    """
    owner = gh.repo.split("/")[0]
    trovate: dict[int, dict[str, Any]] = {}
    motivi: list[str] = []
    refs, shas = [head_ref] if head_ref else [], list(pr_commits)
    visti_ref: set[str] = set()
    visti_sha: set[str] = set()

    def aggiungi(altra: dict[str, Any]) -> None:
        try:
            numero = int(altra.get("number"))
        except (TypeError, ValueError):
            return
        if numero == int(pr) or numero in trovate or altra.get("merged_at"):
            return
        trovate[numero] = altra
        refs.append(str((altra.get("head") or {}).get("ref") or ""))
        for page in range(1, 4):
            commits = gh.get(f"/repos/{gh.repo}/pulls/{numero}/commits?per_page=100&page={page}") or []
            shas.extend(c.get("sha") for c in commits if c.get("sha"))
            if len(commits) < 100:
                break

    while (refs or shas) and len(trovate) <= max_pr:
        if refs:
            ref = refs.pop()
            if ref and ref not in visti_ref:
                visti_ref.add(ref)
                testa = urllib.parse.quote(f"{owner}:{ref}", safe="")
                for altra in gh.get(f"/repos/{gh.repo}/pulls?state=closed&head={testa}&per_page=100") or []:
                    aggiungi(altra)
            continue
        sha = shas.pop()
        if sha and sha not in visti_sha:
            visti_sha.add(sha)
            for altra in gh.get(f"/repos/{gh.repo}/commits/{sha}/pulls?per_page=100") or []:
                aggiungi(altra)
    if len(trovate) > max_pr:
        motivi.append(f"oltre {max_pr} PR collegate per branch o commit: bloccanti non verificabili")
    return [trovate[n] for n in sorted(trovate)], motivi


def triage_dell_owner(gh: GitHub, *, pr: int, reviewer: str) -> set[str]:
    """Id dei bloccanti che l'owner ha triagiato con un commento verificabile.

    Vale solo un commento che:
    - e' dell'owner del repository (login e `author_association` OWNER);
    - NON e' stato pubblicato tramite un'app (`performed_via_github_app`
      vuoto): l'agente scrive con l'account dell'owner ma sempre attraverso
      un'app, quindi non puo' assolversi da solo;
    - non e' mai stato modificato: un'app potrebbe modificare un commento
      dell'owner senza cambiarne l'autore;
    - porta il marker `<!-- pfc-triage-bloccante:<revisore>:<id> -->` e una
      motivazione di almeno MIN_MOTIVAZIONE caratteri.
    """
    owner = gh.repo.split("/")[0]
    ids: set[str] = set()
    for page in range(1, 11):
        commenti = gh.get(f"/repos/{gh.repo}/issues/{pr}/comments?per_page=100&page={page}") or []
        for c in commenti:
            if (c.get("user") or {}).get("login") != owner or c.get("author_association") != "OWNER":
                continue
            if c.get("performed_via_github_app"):
                continue
            if not c.get("created_at") or c.get("created_at") != c.get("updated_at"):
                continue
            corpo = str(c.get("body") or "")
            if len(TRIAGE_MARKER.sub("", corpo).strip()) < MIN_MOTIVAZIONE:
                continue
            ids.update(bid for rev, bid in TRIAGE_MARKER.findall(corpo) if rev == reviewer)
        if len(commenti) < 100:
            break
    return ids


def bloccanti_aperti(attestations: list[dict[str, Any]], *, reviewer: str,
                     triaged: set[str]) -> tuple[list[dict[str, Any]], list[str]]:
    """(bloccanti aperti, note). Attestazione senza il campo = versione precedente del workflow."""
    aperti: dict[str, dict[str, Any]] = {}
    note: list[str] = []
    for att in attestations:
        rng = f"{str(att.get('range_base'))[:12]}...{str(att.get('range_head'))[:12]}"
        dove = f"{att.get('range_base')}...{att.get('range_head')}"
        if "blocking_findings" not in att:
            note.append(f"range {rng}: attestazione di una versione precedente, senza esito registrato")
            continue
        trovati = att.get("blocking_findings")
        if not isinstance(trovati, list):
            trovati = ["esito della review non registrato nell'attestazione"]
        for testo in trovati:
            bid = blocker_id(reviewer, testo, dove)
            if bid not in triaged:
                aperti.setdefault(bid, {"id": bid, "range": rng, "text": str(testo)})
    return list(aperti.values()), note


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
        is_root = _root_checker(gh, args.base_sha, commits)
        info = gh.get(f"/repos/{gh.repo}/pulls/{args.pr}") or {}
        head_ref = str((info.get("head") or {}).get("ref") or "")
        scadenza = time.monotonic() + max(0, args.wait_seconds)
        while True:
            atts, scarti = official_attestations(
                gh, pr=args.pr, reviewer=args.reviewer, workflow_file=args.workflow_file, pr_commits=commits
            )
            atts = atts + _attestazione_locale(args, scarti)
            catena = verify_chain(atts, head_sha=args.head_sha, is_root=is_root)
            # Push ravvicinati: il run del push precedente puo' non aver ancora
            # caricato la sua attestazione (rilievo Codex su #539). Finche' ci
            # sono run ufficiali della PR in corso si aspetta e si rilegge,
            # ANCHE con la catena gia' completa (secondo audit su #539): con un
            # range dal base la catena si chiude con la sola attestazione locale,
            # e i bloccanti del run in corso andrebbero persi. Scaduta l'attesa
            # vale lo stato reale, e un run ancora in corso e' rosso.
            in_corso = runs_in_corso(gh, pr=args.pr, workflow_file=args.workflow_file, pr_commits=commits,
                                     escludi_run=int(os.environ.get("GITHUB_RUN_ID") or 0), head_ref=head_ref,
                                     head_sha=args.head_sha)
            if not in_corso or time.monotonic() >= scadenza:
                break
            esito["waited_for_runs"] = in_corso
            time.sleep(max(1, args.poll_seconds))
        esito.update(catena)
        motivi = list(catena.get("reasons") or []) + esito_non_registrato_sul_head(atts, args.head_sha)
        if in_corso:
            motivi.append(f"run della PR ancora in corso {sorted(in_corso)}: i loro bloccanti non sono ancora "
                          "leggibili, si rilancia la verifica quando finiscono")
        # Bloccanti persistenti: tutte le attestazioni della PR, di ogni tentativo
        # e di ogni range, piu' quella di questo run (non ancora fra gli artifact).
        collegate, motivi_collegate = pr_collegate(gh, pr=args.pr, pr_commits=commits, head_ref=head_ref)
        tutte, motivi_scan, note, illeggibili = attestazioni_con_bloccanti(
            gh, pr=args.pr, reviewer=args.reviewer, workflow_file=args.workflow_file, pr_commits=commits,
            head_ref=head_ref, created_since=str(info.get("created_at") or ""), collegate=collegate,
        )
        motivi_scan = motivi_collegate + motivi_scan
        # Quella di questo run anche se incompleta: il suo artifact puo' non
        # essere ancora elencato (terzo audit su #539).
        visti = {(str(a.get("run_id")), str(a.get("run_attempt"))) for a in tutte}
        tutte += [a for a in atts + _attestazione_locale_grezza(args)
                  if (str(a.get("run_id")), str(a.get("run_attempt"))) not in visti]
        # il triage dell'owner vale anche se scritto sulla PR storica che ha trovato il bloccante
        triaged = set()
        for numero in [args.pr] + [int(a["number"]) for a in collegate]:
            triaged |= triage_dell_owner(gh, pr=numero, reviewer=args.reviewer)
        aperti, note_b = bloccanti_aperti(tutte, reviewer=args.reviewer, triaged=triaged)
        aperti += [b for b in illeggibili if b["id"] not in triaged]
        motivi += motivi_scan
        motivi += [f"bloccante aperto {b['id'] or '(senza id)'} (range {b['range']}): {b['text']}" for b in aperti]
        trovati = {b["id"] for b in bloccanti_aperti(tutte, reviewer=args.reviewer, triaged=set())[0]}
        trovati |= {b["id"] for b in illeggibili}
        esito["open_blockers"] = aperti
        esito["linked_prs"] = [int(a["number"]) for a in collegate]
        esito["triaged_blockers"] = sorted(triaged & trovati)
        esito["notes"] = note + note_b
        esito["reasons"] = motivi
        esito["ok"] = bool(catena["ok"]) and not motivi
        esito["attestations_used"] = len(atts)
        esito["discarded"] = scarti
    except Exception as exc:
        esito["reasons"] = [f"verifica non eseguibile: {str(exc)[:300]}"]
    if args.output:
        with open(args.output, "w", encoding="utf-8") as fh:
            json.dump(esito, fh, indent=2, ensure_ascii=False)
    print(json.dumps(esito, indent=2, ensure_ascii=False))
    return 0 if esito.get("ok") is True else 1


def _attestazione_locale_grezza(args: argparse.Namespace) -> list[dict[str, Any]]:
    """L'attestazione di QUESTO run per i bloccanti: completa o no, purche' sia sua."""
    if not (args.local_attestation and os.path.exists(args.local_attestation)):
        return []
    with open(args.local_attestation, encoding="utf-8") as fh:
        locale = json.load(fh)
    if (isinstance(locale, dict) and locale.get("schema") == SCHEMA and locale.get("reviewer") == args.reviewer
            and locale.get("repo") == args.repo and str(locale.get("pr")) == str(args.pr)):
        return [locale]
    return []


def _attestazione_locale(args: argparse.Namespace, scarti: list[str]) -> list[dict[str, Any]]:
    """L'attestazione scritta da QUESTO run, se valida e sul head richiesto."""
    if not (args.local_attestation and os.path.exists(args.local_attestation)):
        return []
    with open(args.local_attestation, encoding="utf-8") as fh:
        locale = json.load(fh)
    errori = attestation_valid(locale, reviewer=args.reviewer, pr=args.pr, repo=args.repo)
    if errori:
        scarti.append("attestazione di questo run: " + "; ".join(errori))
        return []
    return [locale] if locale.get("range_head") == args.head_sha else []


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
    v.add_argument("--wait-seconds", type=int, default=0,
                   help="attesa massima dei run ufficiali ancora in corso (push ravvicinati)")
    v.add_argument("--poll-seconds", type=int, default=30)
    v.add_argument("--output", default="")
    args = parser.parse_args(argv)
    if args.cmd == "verify":
        return cmd_verify(args)
    return 2


if __name__ == "__main__":
    sys.exit(main())
