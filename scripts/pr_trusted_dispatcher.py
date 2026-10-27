#!/usr/bin/env python3
"""Dispatcher trusted della lane Buildkite (issue #562, PF-TRUSTED-DISPATCHER-IMPLEMENTATION-01).

Contratto tecnico definitivo di Grok (#562 c.6438092317, §5), applicato senza aggiunte:

1. **Intake.** Webhook GitHub `pull_request` (opened, synchronize, reopened, ready_for_review) con firma
   `X-Hub-Signature-256` verificata in tempo costante PRIMA di leggere il JSON. Repository
   `zarbopiero963-droid/Pickfair-nogui`, base `main`, head nello stesso repository (niente fork), PR aperta e non in
   bozza. Altrimenti nessuna build.
2. **Replay e ordine.** `X-GitHub-Delivery` unico in un archivio SQLite persistente (conservazione >= 24 h);
   deduplica su (PR, head SHA) per le build in corso; un nuovo head della stessa PR annulla e sostituisce le build
   del vecchio. Un evento con `updated_at` piu' vecchio dell'ultimo visto per la PR e' fuori ordine e si scarta.
3. **Build.** API Buildkite solo sulle pipeline `pr-overlap-guard` e `pr-merge-readiness`, `branch=main`,
   `commit=HEAD`, env ESATTAMENTE `PF_PR_NUMBER`, `PF_PR_HEAD_SHA`, `PF_PR_BASE_REF=main`, `PF_EVENT`,
   `PF_DELIVERY_ID`.
4. **Esito.** Il guard (`scripts/ci/trusted_pr_guard.py`) scrive `<guard>.json`; lo SHA analizzato deve essere
   `PF_PR_HEAD_SHA` (e PR, repository e guard quelli della richiesta), altrimenti esito `error`.
5. **Pubblicazione.** Solo tramite il comando di pubblicazione configurato (sul VPS l'helper root
   `pf-publish-status`, che ricontrolla la PR e lo SHA prima di scrivere). Build annullata, bloccata, ambigua o in
   timeout => `failure`/`error`, mai `success`. Contesti `overlap-check` e `pr-merge-readiness`.
6. **Segreti.** Il dispatcher legge solo il secret del webhook e il token Buildkite, da file 0600/0400; non li
   passa mai alle build, non li stampa, non li scrive nell'archivio.
7. **Fail closed** su ogni errore di API, rete, validazione o configurazione.

Configurazione: un file JSON con TUTTE le chiavi obbligatorie e nessun default per i parametri infrastrutturali
(endpoint, organizzazione, percorsi dei segreti, artifact, comando di pubblicazione, timeout). Chiave mancante,
sconosciuta o fuori formato => `NOT CONFIGURED`, il servizio non parte. Dettagli in `docs/trusted_dispatcher.md`.

Solo libreria standard. Nessun ordine Betfair, nessun accesso al codice delle PR.
"""
from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import re
import sqlite3
import stat
import subprocess  # nosec B404 - solo il comando di pubblicazione configurato, argv senza shell
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Mapping, Optional

TRUSTED_REPO = "zarbopiero963-droid/Pickfair-nogui"
BASE_REF = "main"
ALLOWED_ACTIONS = frozenset({"opened", "synchronize", "reopened", "ready_for_review"})
#: Unica fonte delle pipeline avviabili: la configurazione non puo' aggiungerne altre.
GUARDS: dict[str, dict[str, str]] = {
    "overlap": {"pipeline": "pr-overlap-guard", "context": "overlap-check"},
    "readiness": {"pipeline": "pr-merge-readiness", "context": "pr-merge-readiness"},
}
BUILDKITE_API = "https://api.buildkite.com/v2"
BUILDKITE_WEB = "https://buildkite.com"
MAX_BODY_BYTES = 2 * 1024 * 1024
MAX_ARTIFACT_BYTES = 64 * 1024
MAX_API_BYTES = 4 * 1024 * 1024
MIN_RETENTION_HOURS = 24
DESCRIPTION_MAX = 140

_PR_RE = re.compile(r"[1-9][0-9]{0,6}", re.ASCII)
_SHA_RE = re.compile(r"[0-9a-f]{40}", re.ASCII)
_DELIVERY_RE = re.compile(r"[A-Za-z0-9-]{8,64}", re.ASCII)   # stesso formato del gate pre-command
_SIGNATURE_RE = re.compile(r"sha256=([0-9a-f]{64})", re.ASCII)
_TIMESTAMP_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z", re.ASCII)
_SLUG_RE = re.compile(r"[a-z0-9][a-z0-9-]{0,99}", re.ASCII)
_HOST_RE = re.compile(r"[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)+", re.ASCII)
_ARTIFACT_RE = re.compile(r"[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*", re.ASCII)
_TOKEN_RE = re.compile(r"[!-~]{20,8192}", re.ASCII)
_SECRET_RE = re.compile(r"[!-~]{16,4096}", re.ASCII)

# Stati Buildkite di una build (API REST v2). Sconosciuto => error, mai attesa infinita.
BUILD_RUNNING = frozenset({"creating", "scheduled", "running", "failing", "canceling", "waiting", "waiting_failed"})
BUILD_TERMINAL = frozenset({"passed", "failed", "canceled", "skipped", "not_run", "blocked"})

# Stati del job nell'archivio.
ACTIVE_STATES = ("queued", "creating", "dispatched", "publishing")
FINAL_STATES = ("done", "superseded", "publish_failed")

EXIT_OK = 0
EXIT_FAIL = 1
EXIT_NOT_CONFIGURED = 3


class ConfigError(Exception):
    """Configurazione assente, incompleta o fuori formato: il servizio resta NOT CONFIGURED."""

    def __init__(self, problems: list[str]) -> None:
        super().__init__("; ".join(problems))
        self.problems = problems


class ApiError(Exception):
    """Risposta HTTP definita ma non quella attesa (4xx/5xx, JSON malformato)."""

    def __init__(self, message: str, status: Optional[int] = None) -> None:
        super().__init__(message)
        self.status = status


class TransportError(Exception):
    """Esito della richiesta ignoto (rete, timeout): per una creazione di build e' AMBIGUO."""


# --------------------------------------------------------------------------------------------- configurazione
CONFIG_SCHEMA_KEYS = {
    "listen": {"host", "port", "path"},
    "webhook_secret_file": None,
    "buildkite": {"organization", "token_file", "result_artifacts", "artifact_download_hosts"},
    "publisher": {"command"},
    "state_db": None,
    "delivery_retention_hours": None,
    "poll_interval_seconds": None,
    "build_timeout_seconds": set(GUARDS),
    "http_timeout_seconds": None,
}


@dataclass(frozen=True)
class Config:
    listen_host: str
    listen_port: int
    listen_path: str
    webhook_secret_file: str
    organization: str
    buildkite_token_file: str
    result_artifacts: dict
    artifact_download_hosts: tuple
    publisher_command: tuple
    state_db: str
    delivery_retention_hours: int
    poll_interval_seconds: int
    build_timeout_seconds: dict
    http_timeout_seconds: int


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _abs_path(value: Any) -> bool:
    return isinstance(value, str) and value.startswith("/") and "\x00" not in value and ".." not in value.split("/")


def validate_config(raw: Any) -> Config:
    """Tutte le chiavi sono obbligatorie e nessuna ha un default: un parametro infrastrutturale mancante non viene
    mai indovinato. Chiavi sconosciute => errore (un refuso non deve diventare un default silenzioso)."""
    problems: list[str] = []
    if not isinstance(raw, dict):
        raise ConfigError(["la configurazione deve essere un oggetto JSON"])
    for key, sub in CONFIG_SCHEMA_KEYS.items():
        if key not in raw:
            problems.append(f"{key}: MISSING")
        elif sub is not None:
            if not isinstance(raw[key], dict):
                problems.append(f"{key}: deve essere un oggetto")
                continue
            for k in sorted(sub - set(raw[key])):
                problems.append(f"{key}.{k}: MISSING")
            for k in sorted(set(raw[key]) - sub):
                problems.append(f"{key}.{k}: chiave sconosciuta")
    for k in sorted(set(raw) - set(CONFIG_SCHEMA_KEYS)):
        problems.append(f"{k}: chiave sconosciuta")
    if problems:
        raise ConfigError(problems)

    listen, bk = raw["listen"], raw["buildkite"]
    host = listen["host"]
    if not isinstance(host, str) or not host or len(host) > 255 or any(c.isspace() for c in host):
        problems.append("listen.host: stringa non vuota senza spazi")
    port = listen["port"]
    if not _is_int(port) or not 1 <= port <= 65535:
        problems.append("listen.port: intero 1..65535")
    path = listen["path"]
    if not isinstance(path, str) or not re.fullmatch(r"/[A-Za-z0-9._/-]{0,200}", path, re.ASCII) or ".." in path:
        problems.append("listen.path: percorso che inizia con '/' (caratteri [A-Za-z0-9._/-])")
    for key in ("webhook_secret_file", "state_db"):
        if not _abs_path(raw[key]):
            problems.append(f"{key}: percorso assoluto senza '..'")
    if not _abs_path(bk["token_file"]):
        problems.append("buildkite.token_file: percorso assoluto senza '..'")
    org = bk["organization"]
    if not isinstance(org, str) or not _SLUG_RE.fullmatch(org):
        problems.append("buildkite.organization: slug Buildkite [a-z0-9-]")
    artifacts = bk["result_artifacts"]
    if not isinstance(artifacts, dict) or set(artifacts) != set(GUARDS):
        problems.append(f"buildkite.result_artifacts: oggetto con esattamente {sorted(GUARDS)}")
    else:
        for guard, value in artifacts.items():
            # il guard scrive `<guard>.json` (scripts/ci/trusted_pr_guard.py): un altro nome non e' il suo risultato
            if (not isinstance(value, str) or not _ARTIFACT_RE.fullmatch(value) or ".." in value.split("/")
                    or value.rsplit("/", 1)[-1] != f"{guard}.json"):
                problems.append(f"buildkite.result_artifacts.{guard}: percorso relativo che finisce con {guard}.json")
    hosts = bk["artifact_download_hosts"]
    if (not isinstance(hosts, list) or not hosts
            or not all(isinstance(h, str) and _HOST_RE.fullmatch(h) for h in hosts)):
        problems.append("buildkite.artifact_download_hosts: elenco non vuoto di host esatti (minuscoli)")
    command = raw["publisher"]["command"]
    if (not isinstance(command, list) or not command or not all(isinstance(c, str) and c and "\x00" not in c
                                                                 for c in command)
            or not _abs_path(command[0])):
        problems.append("publisher.command: argv non vuoto con eseguibile a percorso assoluto")
    retention = raw["delivery_retention_hours"]
    if not _is_int(retention) or not MIN_RETENTION_HOURS <= retention <= 24 * 90:
        problems.append(f"delivery_retention_hours: intero {MIN_RETENTION_HOURS}..{24 * 90}")
    poll = raw["poll_interval_seconds"]
    if not _is_int(poll) or not 1 <= poll <= 300:
        problems.append("poll_interval_seconds: intero 1..300")
    timeouts = raw["build_timeout_seconds"]
    for guard in GUARDS:
        value = timeouts[guard]
        if not _is_int(value) or not 60 <= value <= 6 * 3600:
            problems.append(f"build_timeout_seconds.{guard}: intero 60..21600")
    http_timeout = raw["http_timeout_seconds"]
    if not _is_int(http_timeout) or not 5 <= http_timeout <= 120:
        problems.append("http_timeout_seconds: intero 5..120")
    if problems:
        raise ConfigError(problems)
    return Config(
        listen_host=host, listen_port=port, listen_path=path, webhook_secret_file=raw["webhook_secret_file"],
        organization=org, buildkite_token_file=bk["token_file"], result_artifacts=dict(artifacts),
        artifact_download_hosts=tuple(hosts), publisher_command=tuple(command), state_db=raw["state_db"],
        delivery_retention_hours=retention, poll_interval_seconds=poll, build_timeout_seconds=dict(timeouts),
        http_timeout_seconds=http_timeout)


def load_config(path: Optional[str]) -> Config:
    if not path:
        raise ConfigError(["--config: MISSING (nessun file di configurazione predefinito)"])
    try:
        with open(path, encoding="utf-8") as fh:
            raw = json.load(fh)
    except FileNotFoundError:
        raise ConfigError([f"file di configurazione assente: {path}"]) from None
    except (OSError, ValueError) as exc:
        raise ConfigError([f"file di configurazione illeggibile: {type(exc).__name__}"]) from None
    return validate_config(raw)


def read_secret_file(path: str, pattern: re.Pattern, label: str) -> str:
    """File regolare, non link simbolico, 0600/0400 (nessun permesso a gruppo o altri), contenuto nel formato atteso.
    Il valore non compare mai nei messaggi d'errore."""
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except OSError as exc:
        raise ConfigError([f"{label}: file non apribile ({type(exc).__name__})"]) from None
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode):
            raise ConfigError([f"{label}: non e' un file regolare"])
        if st.st_mode & 0o077:
            raise ConfigError([f"{label}: file accessibile a gruppo o altri (servono 0600/0400)"])
        if st.st_size > 16 * 1024:
            raise ConfigError([f"{label}: file troppo grande"])
        data = os.read(fd, 16 * 1024 + 1)
    finally:
        os.close(fd)
    try:
        value = data.decode("ascii")
    except UnicodeDecodeError:
        raise ConfigError([f"{label}: contenuto non ASCII"]) from None
    if value.endswith("\n"):
        value = value[:-1]
    if not pattern.fullmatch(value):
        raise ConfigError([f"{label}: contenuto fuori formato"])
    return value


# --------------------------------------------------------------------------------------------- intake
def verify_signature(secret: bytes, body: bytes, header: Optional[str]) -> bool:
    """HMAC SHA-256 di GitHub sul corpo grezzo, confronto in tempo costante. Header assente o malformato => False."""
    if not isinstance(header, str):
        return False
    m = _SIGNATURE_RE.fullmatch(header)
    if not m:
        return False
    expected = hmac.new(secret, body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, m.group(1))


@dataclass(frozen=True)
class Intake:
    delivery_id: str
    action: str
    pr: int
    head_sha: str
    updated_at: str


class Rejected(Exception):
    """Evento firmato ma non ammesso. `ignored` = escluso per contratto (azione, bozza, fork, PR chiusa): HTTP 200;
    altrimenti dato incoerente o malformato: HTTP 422. In entrambi i casi nessuna build."""

    def __init__(self, reason: str, *, ignored: bool = False) -> None:
        super().__init__(reason)
        self.ignored = ignored


def _get(obj: Any, *keys: str) -> Any:
    for key in keys:
        if not isinstance(obj, dict):
            return None
        obj = obj.get(key)
    return obj


def parse_event(event: Optional[str], delivery: Optional[str], payload: Any) -> Intake:
    """Validazione del contratto §5.1 su un evento GIA' autenticato. Ogni campo con tipo esatto: un booleano non e'
    un numero di PR, una stringa non e' un booleano."""
    if not isinstance(delivery, str) or not _DELIVERY_RE.fullmatch(delivery):
        raise Rejected("X-GitHub-Delivery assente o fuori formato")
    if event != "pull_request":
        raise Rejected(f"evento non gestito: {event!r}", ignored=True)
    if not isinstance(payload, dict):
        raise Rejected("payload non e' un oggetto JSON")
    action = payload.get("action")
    if action not in ALLOWED_ACTIONS:
        raise Rejected(f"azione non gestita: {action!r}", ignored=True)
    if _get(payload, "repository", "full_name") != TRUSTED_REPO:
        raise Rejected("repository dell'evento diverso da quello atteso")
    pr = payload.get("pull_request")
    if not isinstance(pr, dict):
        raise Rejected("pull_request assente")
    number = pr.get("number")
    if not _is_int(number) or not _PR_RE.fullmatch(str(number)) or payload.get("number") not in (None, number):
        raise Rejected("numero di PR assente o fuori formato")
    if _get(pr, "base", "repo", "full_name") != TRUSTED_REPO:
        raise Rejected("repository base della PR diverso da quello atteso")
    head_sha = _get(pr, "head", "sha")
    if not isinstance(head_sha, str) or not _SHA_RE.fullmatch(head_sha):
        raise Rejected("head SHA assente o fuori formato")
    updated_at = pr.get("updated_at")
    if not isinstance(updated_at, str) or not _TIMESTAMP_RE.fullmatch(updated_at):
        raise Rejected("updated_at della PR assente o fuori formato")
    state, draft = pr.get("state"), pr.get("draft")
    if state not in ("open", "closed") or not isinstance(draft, bool):
        raise Rejected("stato o bozza della PR fuori formato")
    if state != "open":
        raise Rejected("PR non aperta", ignored=True)
    if draft:
        raise Rejected("PR in bozza", ignored=True)
    if _get(pr, "base", "ref") != BASE_REF:
        raise Rejected(f"base diversa da {BASE_REF}", ignored=True)
    if _get(pr, "head", "repo", "full_name") != TRUSTED_REPO:
        raise Rejected("head da fork o da repository cancellato: non supportato", ignored=True)
    return Intake(delivery_id=delivery, action=action, pr=number, head_sha=head_sha, updated_at=updated_at)


# --------------------------------------------------------------------------------------------- archivio
SCHEMA = """
CREATE TABLE IF NOT EXISTS deliveries (
  delivery_id TEXT PRIMARY KEY,
  received_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS pr_heads (
  pr INTEGER PRIMARY KEY,
  head_sha TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  delivery_id TEXT NOT NULL,
  event TEXT NOT NULL,
  pr INTEGER NOT NULL,
  head_sha TEXT NOT NULL,
  guard TEXT NOT NULL,
  state TEXT NOT NULL,
  superseded INTEGER NOT NULL DEFAULT 0,
  build_number INTEGER,
  build_url TEXT,
  outcome TEXT,
  description TEXT,
  created_at REAL NOT NULL,
  dispatched_at REAL,
  updated_at REAL NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS jobs_active ON jobs(pr, head_sha, guard)
  WHERE state IN ('queued', 'creating', 'dispatched', 'publishing');
"""


class Store:
    """SQLite con transazioni `BEGIN IMMEDIATE`: delivery, ordine e job di un evento si scrivono insieme o niente."""

    def __init__(self, path: str) -> None:
        self._lock = threading.Lock()
        self._db = sqlite3.connect(path, isolation_level=None, check_same_thread=False, timeout=30)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._db.close()

    def _tx(self, fn: Callable[[sqlite3.Connection], Any]) -> Any:
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                result = fn(self._db)
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
            self._db.execute("COMMIT")
            return result

    def record_event(self, intake: Intake, now: float) -> tuple[str, list[int]]:
        """Esiti: ("duplicate", []) delivery gia' vista; ("out_of_order", []) evento piu' vecchio dell'ultimo noto
        per la PR; ("accepted", [id dei job nuovi]) altrimenti (lista vuota se i job del head sono gia' in corso)."""

        def fn(db: sqlite3.Connection) -> tuple[str, list[int]]:
            try:
                db.execute("INSERT INTO deliveries(delivery_id, received_at) VALUES (?, ?)",
                           (intake.delivery_id, now))
            except sqlite3.IntegrityError:
                return "duplicate", []
            row = db.execute("SELECT head_sha, updated_at FROM pr_heads WHERE pr = ?", (intake.pr,)).fetchone()
            if row is not None and intake.updated_at < row["updated_at"]:
                return "out_of_order", []
            db.execute("INSERT INTO pr_heads(pr, head_sha, updated_at) VALUES (?, ?, ?) "
                       "ON CONFLICT(pr) DO UPDATE SET head_sha = excluded.head_sha, updated_at = excluded.updated_at",
                       (intake.pr, intake.head_sha, intake.updated_at))
            placeholders = ",".join("?" * len(ACTIVE_STATES))
            db.execute(f"UPDATE jobs SET superseded = 1, updated_at = ? WHERE pr = ? AND head_sha != ? "  # nosec B608
                       f"AND state IN ({placeholders})", (now, intake.pr, intake.head_sha, *ACTIVE_STATES))
            created: list[int] = []
            for guard in GUARDS:
                cur = db.execute(
                    "INSERT OR IGNORE INTO jobs(delivery_id, event, pr, head_sha, guard, state, created_at, updated_at)"
                    " VALUES (?, ?, ?, ?, ?, 'queued', ?, ?)",
                    (intake.delivery_id, intake.action, intake.pr, intake.head_sha, guard, now, now))
                if cur.rowcount == 1:
                    created.append(int(cur.lastrowid))
            return "accepted", created

        return self._tx(fn)

    def active_jobs(self) -> list[dict]:
        placeholders = ",".join("?" * len(ACTIVE_STATES))
        with self._lock:
            rows = self._db.execute(f"SELECT * FROM jobs WHERE state IN ({placeholders}) ORDER BY id",  # nosec B608
                                    ACTIVE_STATES).fetchall()
        return [dict(r) for r in rows]

    def get_job(self, job_id: int) -> dict:
        with self._lock:
            row = self._db.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return dict(row) if row is not None else {}

    def recent_jobs(self, limit: int = 50) -> list[dict]:
        with self._lock:
            rows = self._db.execute("SELECT * FROM jobs ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]

    def update_job(self, job_id: int, now: float, **fields: Any) -> None:
        allowed = {"state", "build_number", "build_url", "outcome", "description", "dispatched_at"}
        if not fields or set(fields) - allowed:
            raise ValueError("campi non ammessi")
        cols = ", ".join(f"{k} = ?" for k in fields)
        self._tx(lambda db: db.execute(f"UPDATE jobs SET {cols}, updated_at = ? WHERE id = ?",  # nosec B608
                                       (*fields.values(), now, job_id)))

    def purge(self, now: float, retention_hours: int) -> None:
        cutoff = now - retention_hours * 3600
        placeholders = ",".join("?" * len(FINAL_STATES))

        def fn(db: sqlite3.Connection) -> None:
            db.execute("DELETE FROM deliveries WHERE received_at < ?", (cutoff,))
            db.execute(f"DELETE FROM jobs WHERE updated_at < ? AND state IN ({placeholders})",  # nosec B608
                       (cutoff, *FINAL_STATES))

        self._tx(fn)


# --------------------------------------------------------------------------------------------- HTTP / Buildkite
Transport = Callable[[str, str, Mapping[str, str], Optional[bytes], int],
                     "tuple[int, Mapping[str, str], bytes]"]


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: D401 - firma di urllib
        return None   # un redirect seguito ripeterebbe l'Authorization verso un altro host


_OPENER = urllib.request.build_opener(_NoRedirect)


def urllib_transport(method: str, url: str, headers: Mapping[str, str], body: Optional[bytes],
                     timeout: int) -> "tuple[int, Mapping[str, str], bytes]":
    if not url.startswith("https://"):
        raise TransportError("solo https")
    req = urllib.request.Request(url, data=body, headers=dict(headers), method=method)
    try:
        with _OPENER.open(req, timeout=timeout) as resp:  # nosec B310 - schema https verificato sopra
            return resp.status, dict(resp.headers), resp.read(MAX_API_BYTES + 1)
    except urllib.error.HTTPError as exc:
        return exc.code, dict(exc.headers or {}), exc.read(MAX_API_BYTES + 1) if exc.fp else b""
    except (urllib.error.URLError, OSError, ValueError) as exc:
        raise TransportError(type(exc).__name__) from None


class Buildkite:
    """Client minimo: crea, legge, annulla build delle DUE pipeline ammesse e scarica l'artifact del risultato."""

    def __init__(self, cfg: Config, token: str, transport: Transport = urllib_transport) -> None:
        self._cfg = cfg
        self._token = token
        self._transport = transport

    def _pipeline_url(self, guard: str) -> str:
        pipeline = GUARDS[guard]["pipeline"]     # KeyError su un guard non ammesso: mai una pipeline libera
        return f"{BUILDKITE_API}/organizations/{self._cfg.organization}/pipelines/{pipeline}"

    def _request(self, method: str, url: str, payload: Any = None) -> "tuple[int, Any]":
        headers = {"Authorization": f"Bearer {self._token}", "Accept": "application/json"}
        body = None
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        status, _, data = self._transport(method, url, headers, body, self._cfg.http_timeout_seconds)
        if len(data) > MAX_API_BYTES:
            raise ApiError("risposta Buildkite troppo grande", status)
        try:
            return status, (json.loads(data) if data else None)
        except ValueError:
            raise ApiError("risposta Buildkite non JSON", status) from None

    def create_build(self, guard: str, env: Mapping[str, str], message: str) -> dict:
        status, data = self._request("POST", f"{self._pipeline_url(guard)}/builds",
                                     {"commit": "HEAD", "branch": BASE_REF, "message": message, "env": dict(env)})
        if status >= 500:
            raise TransportError(f"HTTP {status}")    # il server puo' aver creato la build: ambiguo
        if status != 201 or not isinstance(data, dict):
            raise ApiError(f"creazione build rifiutata (HTTP {status})", status)
        return data

    def get_build(self, guard: str, number: int) -> dict:
        status, data = self._request("GET", f"{self._pipeline_url(guard)}/builds/{int(number)}")
        if status != 200 or not isinstance(data, dict):
            raise ApiError(f"lettura build fallita (HTTP {status})", status)
        return data

    def list_builds(self, guard: str, created_from: str) -> list:
        query = urllib.parse.urlencode({"branch": BASE_REF, "created_from": created_from, "per_page": "100"})
        status, data = self._request("GET", f"{self._pipeline_url(guard)}/builds?{query}")
        if status != 200 or not isinstance(data, list):
            raise ApiError(f"elenco build fallito (HTTP {status})", status)
        return data

    def cancel_build(self, guard: str, number: int) -> None:
        status, _ = self._request("PUT", f"{self._pipeline_url(guard)}/builds/{int(number)}/cancel")
        if status not in (200, 422):           # 422: build gia' terminata, niente da annullare
            raise ApiError(f"annullamento fallito (HTTP {status})", status)

    def fetch_result(self, guard: str, number: int) -> bytes:
        """L'unico artifact `result_artifacts[guard]` della build, in stato `finished`. Il download segue UN
        redirect verso un host ammesso, SENZA Authorization."""
        status, data = self._request("GET", f"{self._pipeline_url(guard)}/builds/{int(number)}/artifacts")
        if status != 200 or not isinstance(data, list):
            raise ApiError(f"elenco artifact fallito (HTTP {status})", status)
        wanted = self._cfg.result_artifacts[guard]
        matches = [a for a in data if isinstance(a, dict) and a.get("path") == wanted]
        if len(matches) != 1:
            raise ApiError(f"artifact {wanted}: {len(matches)} trovati, atteso 1")
        art = matches[0]
        size = art.get("file_size")
        if art.get("state") != "finished" or not _is_int(size) or not 0 < size <= MAX_ARTIFACT_BYTES:
            raise ApiError("artifact non completo o di dimensione non ammessa")
        url = art.get("download_url")
        prefix = f"{BUILDKITE_API}/organizations/{self._cfg.organization}/pipelines/{GUARDS[guard]['pipeline']}/"
        if not isinstance(url, str) or not url.startswith(prefix):
            raise ApiError("download_url fuori dall'API Buildkite attesa")
        headers = {"Authorization": f"Bearer {self._token}"}
        status, resp_headers, _ = self._transport("GET", url, headers, None, self._cfg.http_timeout_seconds)
        location = {k.lower(): v for k, v in dict(resp_headers).items()}.get("location")
        if status not in (301, 302, 303, 307) or not isinstance(location, str):
            raise ApiError(f"download artifact senza redirect (HTTP {status})", status)
        parsed = urllib.parse.urlsplit(location)
        if parsed.scheme != "https" or parsed.hostname not in self._cfg.artifact_download_hosts:
            raise ApiError("redirect dell'artifact verso un host non ammesso")
        status, _, body = self._transport("GET", location, {}, None, self._cfg.http_timeout_seconds)
        if status != 200 or len(body) > MAX_ARTIFACT_BYTES:
            raise ApiError(f"download artifact fallito (HTTP {status})", status)
        return body


# --------------------------------------------------------------------------------------------- esito
def build_env(job: Mapping[str, Any]) -> dict[str, str]:
    """ESATTAMENTE le cinque variabili del contratto §5.3; nessun segreto, nessun'altra variabile."""
    return {
        "PF_PR_NUMBER": str(job["pr"]),
        "PF_PR_HEAD_SHA": job["head_sha"],
        "PF_PR_BASE_REF": BASE_REF,
        "PF_EVENT": job["event"],
        "PF_DELIVERY_ID": job["delivery_id"],
    }


def build_matches_job(build: Any, guard: str, job: Mapping[str, Any], org: str) -> bool:
    """La build letta e' quella chiesta: branch main, env identico a quello inviato, URL della pipeline giusta."""
    if not isinstance(build, dict):
        return False
    number, url = build.get("number"), build.get("web_url")
    prefix = f"{BUILDKITE_WEB}/{org}/{GUARDS[guard]['pipeline']}/builds/"
    return (_is_int(number) and number > 0 and build.get("branch") == BASE_REF
            and build.get("env") == build_env(job)
            and isinstance(url, str) and url == f"{prefix}{number}")


def decide_outcome(build: Mapping[str, Any], artifact: Optional[bytes], job: Mapping[str, Any]) -> tuple[str, str]:
    """(stato GitHub, descrizione). `success` SOLO con build `passed` E risultato del guard PASS sullo stesso
    repository, PR, guard e head SHA. Tutto il resto e' `failure` (verdetto FAIL coerente) o `error`."""
    state = build.get("state")
    if state not in ("passed", "failed"):
        return "error", f"build {state}: nessun verdetto"
    if artifact is None:
        return "error", "risultato del guard assente"
    try:
        result = json.loads(artifact.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return "error", "risultato del guard illeggibile"
    if not isinstance(result, dict):
        return "error", "risultato del guard illeggibile"
    pr = result.get("pr")
    if (result.get("guard") != job["guard"] or result.get("repo") != TRUSTED_REPO
            or not _is_int(pr) or pr != job["pr"]):
        return "error", "risultato di un'altra PR o di un altro controllo"
    if result.get("head_sha") != job["head_sha"]:
        return "error", "stale SHA: il guard ha valutato un head diverso"
    verdict = result.get("verdict")
    if state == "passed" and verdict == "PASS":
        return "success", "PASS"
    if state == "failed" and verdict == "FAIL":
        reasons = result.get("reasons")
        first = reasons[0] if isinstance(reasons, list) and reasons and isinstance(reasons[0], str) else "FAIL"
        return "failure", f"FAIL: {first}"
    return "error", f"build {state} incoerente con verdetto {verdict!r}"


def clean_description(text: str) -> str:
    text = "".join(c if c.isprintable() else " " for c in text)
    return text[:DESCRIPTION_MAX]


def default_publisher(command: tuple, timeout: int) -> Callable[..., int]:
    def publish(pr: int, sha: str, state: str, context: str, url: str, description: str) -> int:
        argv = [*command, str(pr), sha, state, context, url, description]
        try:
            proc = subprocess.run(argv, env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "LANG": "C.UTF-8"},  # nosec B603
                                  stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                  timeout=timeout, check=False)
        except (OSError, subprocess.SubprocessError) as exc:
            log(f"pubblicazione non eseguita: {type(exc).__name__}")
            return 1
        if proc.returncode != 0:
            log(f"pubblicazione rifiutata (exit {proc.returncode}): "
                f"{proc.stdout.decode('utf-8', 'replace')[-500:]!r}")
        return proc.returncode
    return publish


def log(message: str) -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"{stamp} [pr-trusted-dispatcher] {message}", file=sys.stderr, flush=True)


# --------------------------------------------------------------------------------------------- dispatcher
class Dispatcher:
    def __init__(self, cfg: Config, store: Store, buildkite: Buildkite, secret: bytes,
                 publisher: Callable[..., int], clock: Callable[[], float] = time.time) -> None:
        self.cfg = cfg
        self.store = store
        self.bk = buildkite
        self.secret = secret
        self.publish = publisher
        self.clock = clock

    # ---- intake (thread HTTP)
    def accept(self, headers: Mapping[str, str], body: bytes) -> tuple[int, str]:
        h = {k.lower(): v for k, v in headers.items()}
        if not verify_signature(self.secret, body, h.get("x-hub-signature-256")):
            return 401, "firma assente o non valida"
        event = h.get("x-github-event")
        if event == "ping":
            return 200, "pong"
        try:
            payload = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            return 400, "payload non JSON"
        try:
            intake = parse_event(event, h.get("x-github-delivery"), payload)
        except Rejected as exc:
            log(f"evento scartato: {exc}")
            return (200 if exc.ignored else 422), str(exc)
        outcome, created = self.store.record_event(intake, self.clock())
        log(f"delivery {intake.delivery_id} PR #{intake.pr} {intake.head_sha[:12]}: {outcome}, job {created}")
        if outcome == "duplicate":
            return 200, "delivery gia' ricevuta: replay ignorato"
        if outcome == "out_of_order":
            return 200, "evento piu' vecchio dell'ultimo noto per la PR: ignorato"
        return 202, f"accettato: job {created}" if created else "build gia' in corso per questo head"

    # ---- worker
    def tick(self) -> None:
        for job in self.store.active_jobs():
            try:
                self.advance(job)
            except Exception as exc:  # un job non deve fermare gli altri; lo stato resta quello salvato
                log(f"job {job['id']}: errore inatteso {type(exc).__name__}")
        self.store.purge(self.clock(), self.cfg.delivery_retention_hours)

    def advance(self, job: dict) -> None:
        if job["superseded"]:
            return self._supersede(job)
        state = job["state"]
        if state == "queued":
            return self._create(job)
        if state == "creating":
            return self._recover_creation(job)
        if state == "dispatched":
            return self._poll(job)
        if state == "publishing":
            return self._publish_final(job)
        return None

    def _set(self, job: dict, **fields: Any) -> None:
        self.store.update_job(job["id"], self.clock(), **fields)
        job.update(fields)

    def _supersede(self, job: dict) -> None:
        if job["build_number"] is not None:
            try:
                self.bk.cancel_build(job["guard"], job["build_number"])
            except (ApiError, TransportError) as exc:
                log(f"job {job['id']}: annullamento build {job['build_number']} non confermato: {exc}")
        self._set(job, state="superseded", description="sostituito da un head piu' recente")

    def _fail(self, job: dict, state: str, description: str) -> None:
        self._set(job, state="publishing", outcome=state, description=clean_description(description))
        self._publish_final(job)

    def _create(self, job: dict) -> None:
        self._set(job, state="creating")          # da qui in poi un crash lascia l'esito ambiguo, mai un retry cieco
        message = f"PR #{job['pr']} {job['head_sha'][:12]} ({job['guard']}, delivery {job['delivery_id']})"
        try:
            build = self.bk.create_build(job["guard"], build_env(job), message)
        except TransportError as exc:
            log(f"job {job['id']}: creazione build con esito ignoto ({exc}): riconciliazione")
            return
        except ApiError as exc:
            return self._fail(job, "error", str(exc))
        self._adopt(job, build)

    def _adopt(self, job: dict, build: Any) -> None:
        if not build_matches_job(build, job["guard"], job, self.cfg.organization):
            if isinstance(build, dict) and _is_int(build.get("number")):
                self._set(job, build_number=build["number"])
                self._cancel_quietly(job)
            return self._fail(job, "error", "build creata non corrisponde alla richiesta")
        self._set(job, state="dispatched", build_number=build["number"], build_url=build["web_url"],
                  dispatched_at=self.clock())
        self.publish(job["pr"], job["head_sha"], "pending", GUARDS[job["guard"]]["context"], build["web_url"],
                     "build in corso")

    def _cancel_quietly(self, job: dict) -> None:
        try:
            self.bk.cancel_build(job["guard"], job["build_number"])
        except (ApiError, TransportError) as exc:
            log(f"job {job['id']}: annullamento non confermato: {exc}")

    def _recover_creation(self, job: dict) -> None:
        """Creazione con esito ignoto (timeout, 5xx, crash): si cerca la build per delivery e head. Una sola =>
        adottata; nessuna (elenco letto con successo) => error, mai una seconda creazione; piu' di una =>
        annullate tutte, error. Elenco illeggibile => si riprova fino al timeout della build, poi error."""
        created_from = datetime.fromtimestamp(job["created_at"] - 300, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        try:
            builds = self.bk.list_builds(job["guard"], created_from)
        except (ApiError, TransportError) as exc:
            if self.clock() - job["created_at"] > self.cfg.build_timeout_seconds[job["guard"]]:
                return self._fail(job, "error", f"creazione build non verificabile: {exc}")
            return None
        expected = build_env(job)
        found = [b for b in builds if isinstance(b, dict) and b.get("env") == expected]
        if len(found) == 1:
            return self._adopt(job, found[0])
        for b in found:
            if _is_int(b.get("number")):
                try:
                    self.bk.cancel_build(job["guard"], b["number"])
                except (ApiError, TransportError):
                    pass
        return self._fail(job, "error", "creazione build non confermata" if not found else
                          f"{len(found)} build per la stessa richiesta: annullate")

    def _poll(self, job: dict) -> None:
        guard = job["guard"]
        try:
            build = self.bk.get_build(guard, job["build_number"])
        except (ApiError, TransportError) as exc:
            build = None
            log(f"job {job['id']}: lettura build fallita: {exc}")
        timed_out = self.clock() - (job["dispatched_at"] or job["created_at"]) > self.cfg.build_timeout_seconds[guard]
        if build is not None and not build_matches_job(build, guard, job, self.cfg.organization):
            self._cancel_quietly(job)
            return self._fail(job, "error", "build letta non corrisponde alla richiesta")
        state = build.get("state") if build is not None else None
        if build is not None and state not in BUILD_RUNNING | BUILD_TERMINAL:
            self._cancel_quietly(job)
            return self._fail(job, "error", f"stato build sconosciuto: {state!r}")
        if build is None or state in BUILD_RUNNING:
            if timed_out:
                self._cancel_quietly(job)
                return self._fail(job, "error", "timeout della build")
            return None
        if state == "blocked":
            self._cancel_quietly(job)
        artifact: Optional[bytes] = None
        if state in ("passed", "failed"):
            try:
                artifact = self.bk.fetch_result(guard, job["build_number"])
            except (ApiError, TransportError) as exc:
                log(f"job {job['id']}: risultato non scaricato: {exc}")
        outcome, description = decide_outcome(build, artifact, job)
        self._set(job, state="publishing", outcome=outcome, description=clean_description(description))
        self._publish_final(job)

    def _publish_final(self, job: dict) -> None:
        fresh = self.store.get_job(job["id"])
        if fresh.get("superseded"):
            return self._supersede(job)
        outcome = job["outcome"]
        if outcome not in ("success", "failure", "error"):
            outcome, job["description"] = "error", "esito interno non valido"
        url = job["build_url"] or f"{BUILDKITE_WEB}/{self.cfg.organization}/{GUARDS[job['guard']]['pipeline']}"
        rc = self.publish(job["pr"], job["head_sha"], outcome, GUARDS[job["guard"]]["context"], url,
                          job["description"] or outcome)
        self._set(job, state="done" if rc == 0 else "publish_failed")


# --------------------------------------------------------------------------------------------- server
def make_handler(dispatcher: Dispatcher) -> type:
    class Handler(BaseHTTPRequestHandler):
        server_version = "pf-trusted-dispatcher"
        sys_version = ""
        timeout = 30                    # un client lento non tiene occupato un thread all'infinito

        def log_message(self, fmt: str, *args: Any) -> None:  # niente header nei log
            log(f"http {self.address_string()} {fmt % args}")

        def _reply(self, code: int, text: str) -> None:
            data = (text + "\n").encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self) -> None:  # noqa: N802
            self._reply(405, "solo POST")

        def do_POST(self) -> None:  # noqa: N802
            if urllib.parse.urlsplit(self.path).path != dispatcher.cfg.listen_path:
                return self._reply(404, "percorso sconosciuto")
            raw_len = self.headers.get("Content-Length")
            if raw_len is None or not raw_len.isdigit():
                return self._reply(411, "Content-Length obbligatorio")
            length = int(raw_len)
            if length > MAX_BODY_BYTES:
                return self._reply(413, "payload troppo grande")
            body = self.rfile.read(length)
            try:
                code, text = dispatcher.accept(dict(self.headers.items()), body)
            except Exception as exc:  # archivio non scrivibile o simili: 500, GitHub registra il fallimento
                log(f"intake fallito: {type(exc).__name__}")
                code, text = 500, "errore interno: evento non registrato"
            self._reply(code, text)

    return Handler


def build_dispatcher(cfg: Config, transport: Transport = urllib_transport,
                     publisher: Optional[Callable[..., int]] = None) -> Dispatcher:
    secret = read_secret_file(cfg.webhook_secret_file, _SECRET_RE, "webhook_secret_file").encode("ascii")
    token = read_secret_file(cfg.buildkite_token_file, _TOKEN_RE, "buildkite.token_file")
    store = Store(cfg.state_db)
    return Dispatcher(cfg, store, Buildkite(cfg, token, transport), secret,
                      publisher or default_publisher(cfg.publisher_command, 60))


def serve(cfg: Config) -> int:
    dispatcher = build_dispatcher(cfg)
    stop = threading.Event()

    def worker() -> None:
        while not stop.is_set():
            try:
                dispatcher.tick()
            except Exception as exc:
                log(f"tick fallito: {type(exc).__name__}")
            stop.wait(cfg.poll_interval_seconds)

    thread = threading.Thread(target=worker, name="pf-dispatch-worker", daemon=True)
    thread.start()
    server = ThreadingHTTPServer((cfg.listen_host, cfg.listen_port), make_handler(dispatcher))
    log(f"in ascolto su {cfg.listen_host}:{cfg.listen_port}{cfg.listen_path}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        server.server_close()
        thread.join(timeout=10)
        dispatcher.store.close()
    return EXIT_OK


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="pr_trusted_dispatcher.py", description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("check-config", "serve", "jobs"))
    parser.add_argument("--config", required=False)
    parser.add_argument("--check-secrets", action="store_true",
                        help="check-config: legge anche i file dei segreti (permessi e formato)")
    args = parser.parse_args(argv)
    try:
        cfg = load_config(args.config)
        if args.command == "check-config":
            if args.check_secrets:
                read_secret_file(cfg.webhook_secret_file, _SECRET_RE, "webhook_secret_file")
                read_secret_file(cfg.buildkite_token_file, _TOKEN_RE, "buildkite.token_file")
            print("CONFIGURED")
            return EXIT_OK
        if args.command == "jobs":
            store = Store(cfg.state_db)
            try:
                print(json.dumps(store.recent_jobs(), indent=1))
            finally:
                store.close()
            return EXIT_OK
        return serve(cfg)
    except ConfigError as exc:
        print("NOT CONFIGURED", file=sys.stderr)
        for problem in exc.problems:
            print(f"  - {problem}", file=sys.stderr)
        return EXIT_NOT_CONFIGURED


if __name__ == "__main__":
    raise SystemExit(main())
