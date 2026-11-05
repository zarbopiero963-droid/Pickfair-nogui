#!/usr/bin/env python3
"""PF-VPS-UNIVERSAL-TEST-AUTOMATION-01 (#586) — test di Pickfair richiedibili da Claude B, lato GitHub e contratto VPS.

Modello a **pull**, senza endpoint in ingresso sulla VPS e senza segreti su GitHub:

1. ``tests/catalog/pf-test-catalog.json`` e' il catalogo versionato delle suite: per ognuna un runner e un argv FISSI
   (``validate_catalog``), ambiente, rischio, risorse, timeout, dataset, evidenze e criteri. Nuove suite solo via PR,
   review e CI; la VPS lo legge **da main**, mai dalla PR.
2. ``.github/workflows/pf-test-request.yml`` (``workflow_dispatch``, solo da ``main``) chiama ``build``: richiedente,
   permessi, PR e head si leggono dall'API di GitHub; la suite (o la campagna) deve esistere nel catalogo; nasce una
   richiesta a **schema chiuso** legata a SHA, impronta del catalogo e identita'. Il workflow la firma con Sigstore e
   la carica come artefatto.
3. La VPS (Grok, orchestratore esistente, sandbox) preleva l'artefatto, verifica la firma
   (``gh attestation verify --signer-workflow …@refs/heads/main``), chiama ``verify_request`` contro lo stato ATTUALE
   della PR e contro il catalogo di main, poi ``Ledger.admit`` (idempotenza). Esegue SOLO l'argv del catalogo.
4. L'esito torna come check run di Pickfair Dedicated sull'head esatto; ``status`` lo traduce negli stati del mandato.
   Un errore d'infrastruttura e' ``INFRA_ERROR``: mai ``FAIL`` del codice, mai ``PASS``.

Due corsie (PF-UNIVERSAL-TEST-AUTONOMY-365-01):

- ``certification``: ``suite:<id>`` o ``campaign:<nome>`` del catalogo di main; verdetto ufficiale, idempotenza per
  SHA; e' l'unica corsia che puo' dare un esito certificativo;
- ``development``: ``dev:<percorso>[,<percorso>…]``, test NUOVI scritti nella PR (codice non fidato). Solo ``pytest``
  su percorsi ``tests/…`` che esistono allo SHA richiesto, nella sandbox di sviluppo (``DEV_SANDBOX``: nessuna rete,
  nessun segreto, filesystem di sola lettura salvo la directory di lavoro, limiti fissi). Ogni dispatch e' una nuova
  esecuzione (si puo' ripetere un test fallito); l'esito e' marcato ``certification: false`` e non conta MAI per
  campagne, gate o certificazioni.

Fail-closed ovunque. Modalita' ``dry-run`` fissata nel workflow: l'esecuzione reale richiede GO owner e una PR a merge
owner (gate di attivazione §11).
"""
from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = "tests/catalog/pf-test-catalog.json"
CATALOG_SCHEMA = "pf-test-catalog/1"
REQUEST_SCHEMA = "pf-test-request/1"
REQUEST_WORKFLOW = ".github/workflows/pf-test-request.yml"
TRUSTED_APP = "pickfair-dedicated"
#: chi puo' chiedere test: l'account owner (le sessioni Claude B agiscono con la sua identita' GitHub)
TRUSTED_REQUESTERS = frozenset({"zarbopiero963-droid"})
REQUESTER_PERMISSIONS = frozenset({"admin", "write"})
MODES = ("dry-run",)                         # l'esecuzione reale arrivera' solo con GO owner (PR a merge owner)
MAX_AGE_SECONDS = 24 * 3600
REQUEST_FIELDS = frozenset({"schema", "lane", "target", "suites", "dev_paths", "catalog_sha256", "catalog_version",
                            "repo", "pr", "head_sha", "base_sha", "requested_by", "run_id", "run_attempt",
                            "created_at", "mode", "idempotency_key"})
LANES = ("certification", "development")
#: sandbox della corsia development: codice di test NON fidato (scritto nella PR), mai privilegi
DEV_SANDBOX = {"runner": "pytest", "environment": "DEV_SANDBOX", "network": "none", "secrets": "none",
               "filesystem": "read-only except the ephemeral workdir", "timeout_seconds": 1800,
               "resources": {"cpu": 2, "memory_mb": 2048}, "certification": False}
MAX_DEV_PATHS = 20

# ── catalogo ─────────────────────────────────────────────────────────────────────────────────────────────────────
ENVIRONMENTS = frozenset({"OFFLINE", "SIM", "REPLAY_RECORDED", "SANDBOX_EPHEMERAL"})
RISKS = frozenset({"LOW", "MEDIUM", "HIGH"})
#: ambienti in cui un test puo' toccare processi, disco o rete (sempre effimeri, mai il DB operativo)
DESTRUCTIVE_ENVS = frozenset({"SANDBOX_EPHEMERAL"})
DESTRUCTIVE_CATEGORIES = frozenset({"chaos", "crash-restart"})
#: runner ammessi e cosa possono ricevere: nessuna shell, nessun flag, solo percorsi del repository o nomi fissi
PYTHON_SCRIPTS = frozenset({"scripts/run_mutation_guardrails.py"})
ORCHESTRATOR_JOBS = frozenset({"PRELIVE_CERTIFY", "SIM_MULTITABLE_CERTIFY"})
MAX_TIMEOUT_SECONDS = 7200
MAX_CPU, MAX_MEMORY_MB = 4, 8192
SUITE_FIELDS = frozenset({"id", "component", "behaviour", "category", "runner", "args", "environment", "risk",
                          "timeout_seconds", "resources", "datasets", "evidence", "requires", "criteria"})
_ID = re.compile(r"[a-z0-9][a-z0-9-]{0,62}")
_SAFE_ARG = re.compile(r"[A-Za-z0-9_./-]+")


def _canonical(path: str) -> bool:
    """Percorso gia' in forma canonica: nessun segmento vuoto, `.` o `..` (quindi niente barra finale ne' doppia).
    `tests/.` o `tests/./chaos` sfuggirebbero al confronto testuale di _covers (rilievo di Sol su #606)."""
    return all(seg not in ("", ".", "..") for seg in path.split("/"))
_SHA = re.compile(r"[0-9a-f]{40}")
_TS = "%Y-%m-%dT%H:%M:%SZ"

#: stati del mandato (§6) e traduzione dei check run
STATES = ("QUEUED", "RUNNING", "PASS", "FAIL", "BLOCKED", "TIMEOUT", "CANCELLED", "INFRA_ERROR")
_CONCLUSION_STATE = {"success": "PASS", "failure": "FAIL", "action_required": "BLOCKED", "timed_out": "TIMEOUT",
                     "cancelled": "CANCELLED"}


class Refused(ValueError):
    """Richiesta o catalogo rifiutati: il messaggio e' il motivo, mai un dettaglio sensibile."""


def catalog_digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _check_args(suite: Mapping[str, Any], root: Path) -> None:
    runner, args = suite["runner"], suite["args"]
    if not isinstance(args, list) or not args or not all(isinstance(a, str) for a in args):
        raise Refused(f"BAD_ARGS:{suite['id']}")
    for a in args:
        if not _SAFE_ARG.fullmatch(a) or ".." in a or a.startswith(("/", "-")) or not _canonical(a):
            raise Refused(f"UNSAFE_ARG:{suite['id']}")              # niente shell, flag, percorsi assoluti o risalite
    if runner == "pytest":
        for a in args:
            if not (a == "tests" or a.startswith("tests/")) or not (root / a).exists():
                raise Refused(f"PYTEST_PATH:{suite['id']}")
    elif runner == "python":
        if len(args) != 1 or args[0] not in PYTHON_SCRIPTS or not (root / args[0]).is_file():
            raise Refused(f"PYTHON_SCRIPT:{suite['id']}")
    elif runner == "orchestrator":
        if len(args) != 1 or args[0] not in ORCHESTRATOR_JOBS:
            raise Refused(f"ORCHESTRATOR_JOB:{suite['id']}")
    else:
        raise Refused(f"RUNNER:{suite['id']}")


def _covers(outer: str, inner: str) -> bool:
    """``outer`` (cartella o file pytest) comprende ``inner``."""
    return inner == outer or inner.startswith(outer.rstrip("/") + "/")


def _check_isolation_coverage(suites: List[Mapping[str, Any]]) -> None:
    """Una suite fuori dalla sandbox effimera non esegue percorsi che il catalogo isola (rilievo di Sol su #606):
    ne' aggregandoli (``pytest tests`` comprende ``tests/chaos``) ne' prendendone una parte (``tests/chaos/x.py``).
    Isolati = argomenti pytest delle suite in ambiente distruttivo, esclusa la radice ``tests``: l'aggregato
    dell'intero albero e' esso stesso una suite isolata, e il primo controllo vieta di eseguirlo altrove."""
    isolated = [a for s in suites if s["runner"] == "pytest" and s["environment"] in DESTRUCTIVE_ENVS
                for a in s["args"]]
    roots = [a for a in isolated if a.rstrip("/") != "tests"]
    for s in suites:
        if s["runner"] != "pytest" or s["environment"] in DESTRUCTIVE_ENVS:
            continue
        for a in s["args"]:
            if any(_covers(a, d) for d in isolated) or any(_covers(d, a) for d in roots):
                raise Refused(f"AGGREGATE_INCLUDES_ISOLATED:{s['id']}")


def validate_catalog(raw: bytes, root: Path = ROOT) -> Dict[str, Any]:
    """Catalogo valido o ``Refused``: schema chiuso, id stabili e unici, argv sicuro, limiti di risorse."""
    try:
        cat = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        raise Refused("CATALOG_MALFORMED") from None
    if not isinstance(cat, dict) or cat.get("schema") != CATALOG_SCHEMA or not isinstance(cat.get("version"), int):
        raise Refused("CATALOG_SCHEMA")
    suites = cat.get("suites")
    if not isinstance(suites, list) or not suites:
        raise Refused("CATALOG_EMPTY")
    seen = set()
    for s in suites:
        if not isinstance(s, dict) or set(s) != SUITE_FIELDS:
            raise Refused(f"SUITE_FIELDS:{s.get('id') if isinstance(s, dict) else '?'}")
        if not isinstance(s["id"], str) or not _ID.fullmatch(s["id"]) or s["id"] in seen:
            raise Refused(f"SUITE_ID:{s['id']}")
        seen.add(s["id"])
        if s["environment"] not in ENVIRONMENTS or s["risk"] not in RISKS:
            raise Refused(f"SUITE_ENV_OR_RISK:{s['id']}")
        # Chaos e crash-restart toccano processi, disco e rete: solo nella sandbox effimera, a QUALUNQUE rischio
        # dichiarato (rilievo di Grok su #606: prima un chaos "LOW" passava in OFFLINE).
        if s["category"] in DESTRUCTIVE_CATEGORIES and s["environment"] not in DESTRUCTIVE_ENVS:
            raise Refused(f"DESTRUCTIVE_NOT_ISOLATED:{s['id']}")
        t = s["timeout_seconds"]
        if not isinstance(t, int) or isinstance(t, bool) or not 0 < t <= MAX_TIMEOUT_SECONDS:
            raise Refused(f"SUITE_TIMEOUT:{s['id']}")
        r = s["resources"]
        if not isinstance(r, dict) or set(r) != {"cpu", "memory_mb"} or not (
                isinstance(r["cpu"], int) and 0 < r["cpu"] <= MAX_CPU and isinstance(r["memory_mb"], int)
                and 0 < r["memory_mb"] <= MAX_MEMORY_MB):
            raise Refused(f"SUITE_RESOURCES:{s['id']}")
        if set(s["criteria"]) != {"PASS", "FAIL", "BLOCKED"} or not s["evidence"]:
            raise Refused(f"SUITE_CRITERIA:{s['id']}")
        _check_args(s, root)
    _check_isolation_coverage(suites)
    for name, ids in (cat.get("campaigns") or {}).items():
        if not _ID.fullmatch(str(name)) or not isinstance(ids, list) or not ids or not set(ids) <= seen:
            raise Refused(f"CAMPAIGN:{name}")
    return cat


def resolve_target(cat: Mapping[str, Any], target: str) -> List[str]:
    """``suite:<id>`` o ``campaign:<nome>`` -> elenco ordinato di suite del catalogo; altro => ``Refused``."""
    ids = {s["id"] for s in cat["suites"]}
    kind, _, name = str(target).partition(":")
    if kind == "suite" and name in ids:
        return [name]
    if kind == "campaign" and name in (cat.get("campaigns") or {}):
        return sorted(cat["campaigns"][name])
    raise Refused("TARGET_NOT_IN_CATALOG")


def dev_paths_from_target(target: str) -> List[str]:
    """``dev:a,b`` -> percorsi pytest sotto ``tests/`` (sintassi soltanto: l'esistenza si verifica allo SHA)."""
    kind, _, rest = str(target).partition(":")
    if kind != "dev" or not rest:
        raise Refused("TARGET_NOT_IN_CATALOG")
    paths = [p for p in rest.split(",")]
    if not 0 < len(paths) <= MAX_DEV_PATHS or len(set(paths)) != len(paths):
        raise Refused("DEV_PATHS")
    for p in paths:
        if (not _SAFE_ARG.fullmatch(p) or ".." in p or p.startswith(("/", "-")) or "//" in p or not _canonical(p)
                or not p.startswith("tests/") or not (p.endswith(".py") or "." not in p.rsplit("/", 1)[-1])):
            raise Refused("DEV_PATH_UNSAFE")
    return paths


def lane_of(target: str) -> str:
    return "development" if str(target).startswith("dev:") else "certification"


# ── richiesta ────────────────────────────────────────────────────────────────────────────────────────────────────
def idempotency_key(target: str, catalog_sha: str, repo: str, pr: int, sha: str, run_id: Optional[int] = None) -> str:
    """Certificazione: una sola esecuzione per (target, catalogo, PR, SHA). Sviluppo: una per dispatch (``run_id``),
    cosi' un test sperimentale fallito si puo' ripetere; lo stesso run non viene mai eseguito due volte."""
    tail = f"|run:{run_id}" if run_id is not None else ""
    return hashlib.sha256(f"{target}|{catalog_sha}|{repo}|{pr}|{sha}{tail}".encode()).hexdigest()


def _check_pr(pr_data: Mapping[str, Any], *, repo: str, sha: str) -> None:
    if pr_data.get("state") != "open":
        raise Refused("PR_NOT_OPEN")
    head = pr_data.get("head") or {}
    if ((head.get("repo") or {}).get("full_name")) != repo:
        raise Refused("FORK_PR")                       # codice di terzi: mai eseguito in automatico
    if head.get("sha") != sha:
        raise Refused("STALE_SHA")                     # si testa solo l'head attuale


def build_request(*, target: str, pr: Any, sha: str, repo: str, actor: str, actor_permission: str,
                  pr_data: Mapping[str, Any], catalog_raw: bytes, run_id: Any, run_attempt: Any, mode: str,
                  now: Optional[_dt.datetime] = None, root: Path = ROOT,
                  path_exists_at_sha: Optional[Any] = None) -> Dict[str, Any]:
    """Richiesta a schema chiuso, oppure ``Refused``. I dati di GitHub e il catalogo arrivano gia' letti.

    ``path_exists_at_sha(path) -> bool`` (corsia development) controlla i percorsi nel codice della PR allo SHA."""
    if mode not in MODES:
        raise Refused("MODE_NOT_ALLOWED")
    cat = validate_catalog(catalog_raw, root)
    lane = lane_of(target)
    if lane == "development":
        suites, dev_paths = [], dev_paths_from_target(target)
    else:
        suites, dev_paths = resolve_target(cat, target), []
    try:
        pr_number = int(str(pr).strip())
    except ValueError:
        raise Refused("MALFORMED_PR") from None
    if pr_number <= 0 or str(pr).strip() != str(pr_number):
        raise Refused("MALFORMED_PR")
    if not isinstance(sha, str) or not _SHA.fullmatch(sha):
        raise Refused("MALFORMED_SHA")
    if actor not in TRUSTED_REQUESTERS:
        raise Refused("REQUESTER_NOT_TRUSTED")
    if actor_permission not in REQUESTER_PERMISSIONS:
        raise Refused("REQUESTER_PERMISSION")
    _check_pr(pr_data, repo=repo, sha=sha)
    if dev_paths and (path_exists_at_sha is None or not all(path_exists_at_sha(p) for p in dev_paths)):
        raise Refused("DEV_PATH_MISSING")             # percorsi verificati sul codice della PR allo SHA
    digest = catalog_digest(catalog_raw)
    run = int(run_id)
    return {"schema": REQUEST_SCHEMA, "lane": lane, "target": target, "suites": suites, "dev_paths": dev_paths,
            "catalog_sha256": digest,
            "catalog_version": cat["version"], "repo": repo, "pr": pr_number, "head_sha": sha,
            "base_sha": str((pr_data.get("base") or {}).get("sha") or ""), "requested_by": actor,
            "run_id": run, "run_attempt": int(run_attempt),
            "created_at": (now or _dt.datetime.now(_dt.timezone.utc)).strftime(_TS), "mode": mode,
            "idempotency_key": idempotency_key(target, digest, repo, pr_number, sha,
                                               run if lane == "development" else None)}


def verify_request(raw: bytes, *, repo: str, pr_data: Mapping[str, Any], signer_run_id: str, catalog_raw: bytes,
                   now: Optional[_dt.datetime] = None, max_age: int = MAX_AGE_SECONDS,
                   root: Path = ROOT) -> Dict[str, Any]:
    """Lato VPS, DOPO la verifica della firma. ``pr_data`` e ``catalog_raw`` (di main) la VPS li legge da se'.

    ``signer_run_id`` e' il run del certificato Sigstore: la richiesta deve venire proprio da quel run."""
    try:
        req = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        raise Refused("MALFORMED_REQUEST") from None
    if not isinstance(req, dict) or set(req) != REQUEST_FIELDS or req.get("schema") != REQUEST_SCHEMA:
        raise Refused("SCHEMA_MISMATCH")               # niente campi in piu': nessun parametro libero passa
    if req["mode"] not in MODES:
        raise Refused("MODE_NOT_ALLOWED")
    if req["repo"] != repo:
        raise Refused("WRONG_REPO")
    if req["requested_by"] not in TRUSTED_REQUESTERS:
        raise Refused("REQUESTER_NOT_TRUSTED")
    if not isinstance(req["pr"], int) or isinstance(req["pr"], bool) or req["pr"] <= 0:
        raise Refused("MALFORMED_PR")
    if not isinstance(req["head_sha"], str) or not _SHA.fullmatch(req["head_sha"]):
        raise Refused("MALFORMED_SHA")
    if str(req["run_id"]) != str(signer_run_id):
        raise Refused("RUN_MISMATCH")
    if req["catalog_sha256"] != catalog_digest(catalog_raw):
        raise Refused("CATALOG_MISMATCH")              # catalogo alterato o diverso da quello di main
    cat = validate_catalog(catalog_raw, root)
    if req["lane"] not in LANES or req["lane"] != lane_of(req["target"]):
        raise Refused("LANE_MISMATCH")
    if req["lane"] == "development":
        if req["suites"] != [] or req["dev_paths"] != dev_paths_from_target(req["target"]):
            raise Refused("SUITES_MISMATCH")
    elif req["dev_paths"] != [] or req["suites"] != resolve_target(cat, req["target"]):
        raise Refused("SUITES_MISMATCH")
    if req["catalog_version"] != cat["version"]:
        raise Refused("SUITES_MISMATCH")
    if req["idempotency_key"] != idempotency_key(req["target"], req["catalog_sha256"], req["repo"], req["pr"],
                                                 req["head_sha"],
                                                 req["run_id"] if req["lane"] == "development" else None):
        raise Refused("KEY_MISMATCH")
    try:
        created = _dt.datetime.strptime(str(req["created_at"]), _TS).replace(tzinfo=_dt.timezone.utc)
    except ValueError:
        raise Refused("MALFORMED_TIMESTAMP") from None
    age = ((now or _dt.datetime.now(_dt.timezone.utc)) - created).total_seconds()
    if age < -300 or age > max_age:
        raise Refused("EXPIRED")
    if pr_data.get("number") != req["pr"]:            # la PR riletta dalla VPS deve essere proprio quella
        raise Refused("WRONG_PR")
    _check_pr(pr_data, repo=repo, sha=req["head_sha"])
    return req


def _lock_file(fh: Any) -> None:
    """Lock esclusivo interprocesso (POSIX ``flock``, la VPS e' Linux). Senza lock disponibile => eccezione."""
    import fcntl
    fcntl.flock(fh.fileno(), fcntl.LOCK_EX)


def _fsync_dir(path: Path) -> None:
    dfd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)


def _durable_makedirs(path: Path) -> None:
    """Crea ``path`` e i genitori mancanti rendendo durevole ogni creazione: dopo ogni ``mkdir`` si sincronizza la
    directory genitore (rilievo di Sol su #606). Senza, un crash dell'host puo' perdere l'albero appena creato insieme
    al registro e riammettere richieste gia' confermate. Una directory gia' esistente non viene toccata; un errore si
    propaga (nessuna ammissione dichiarata)."""
    path = Path(path)
    missing = []
    cur = path
    while not cur.exists():
        missing.append(cur)
        if cur.parent == cur:
            break
        cur = cur.parent
    for d in reversed(missing):
        try:
            os.mkdir(str(d))
        except FileExistsError:
            pass                                       # creata in concorrenza: la si sincronizza comunque
        _fsync_dir(d.parent)


class Ledger:
    """Registro persistente delle richieste ammesse (idempotenza, lato VPS). Lettura, controllo e scrittura stanno
    sotto un lock esclusivo interprocesso (``<registro>.lock``): due consumer concorrenti non ammettono mai la stessa
    richiesta e non si cancellano le ammissioni a vicenda (rilievo di Sol e Grok su #606). Scrittura atomica; file
    illeggibile o lock non disponibile => ``Refused`` (mai ripartire da vuoto, che riammetterebbe tutto)."""

    def __init__(self, path: Path):
        self.path = Path(path)

    def _load(self) -> Dict[str, Any]:
        if not self.path.exists():
            return {"admitted": {}}
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            raise Refused("LEDGER_UNREADABLE") from None
        if not isinstance(data, dict) or not isinstance(data.get("admitted"), dict):
            raise Refused("LEDGER_UNREADABLE")
        return data

    def admit(self, req: Mapping[str, Any]) -> None:
        _durable_makedirs(self.path.parent)
        with open(f"{self.path}.lock", "a+", encoding="utf-8") as lock:
            try:
                _lock_file(lock)
            except (OSError, ImportError):
                raise Refused("LEDGER_LOCK_UNAVAILABLE") from None
            self._admit_locked(req)                    # il lock si rilascia alla chiusura del file

    def _admit_locked(self, req: Mapping[str, Any]) -> None:
        data = self._load()
        key = str(req["idempotency_key"])
        if key in data["admitted"]:
            raise Refused("DUPLICATE_REQUEST")
        data["admitted"][key] = {"lane": req["lane"], "target": req["target"], "pr": req["pr"],
                                 "head_sha": req["head_sha"], "run_id": req["run_id"]}
        _durable_makedirs(self.path.parent)
        fd, tmp = tempfile.mkstemp(dir=str(self.path.parent), prefix=".ledger-")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(data, fh, sort_keys=True)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.path)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise
        # la rinomina diventa durevole solo con la directory sincronizzata (rilievo di Sol su #606): senza, un crash
        # dell'host puo' perdere un'ammissione gia' confermata e riammettere la stessa richiesta. Un errore qui si
        # propaga: l'ammissione non viene dichiarata (la riga, se scritta, rifiuta comunque un secondo tentativo).
        _fsync_dir(self.path.parent)


# ── stato ────────────────────────────────────────────────────────────────────────────────────────────────────────
def check_name(suite_id: str) -> str:
    """Check run con cui la VPS pubblica l'esito di una suite (le due certificazioni storiche tengono il loro nome)."""
    return {"prelive-certify": "pf-betfair-certification",
            "sim-multitable-certify": "pf-sim-multitable-certification"}.get(suite_id, f"pf-test/{suite_id}")


def dev_check_name(idempotency_key_: str) -> str:
    """Esito di un'esecuzione di sviluppo: nome distinto, mai confondibile con una suite certificativa."""
    return f"pf-dev-test/{str(idempotency_key_)[:12]}"


def dev_status(idempotency_key_: str, sha: str, check_runs: list) -> Dict[str, Any]:
    """Esito sperimentale: stessi stati, ma ``certification: false`` e nessun peso su campagne o gate."""
    name = dev_check_name(idempotency_key_)
    mine = [c for c in check_runs if c.get("name") == name and c.get("head_sha") == sha
            and ((c.get("app") or {}).get("slug")) == TRUSTED_APP]
    if not mine:
        return {"lane": "development", "sha": sha, "state": "NOT_REQUESTED", "certification": False}
    open_runs = [c for c in mine if c.get("status") != "completed"]
    last = max(open_runs or mine, key=lambda c: int(c.get("id") or 0))
    return {"lane": "development", "sha": sha, "state": state_from_check_run(last), "certification": False,
            "check_run_id": last.get("id")}


def state_from_check_run(cr: Mapping[str, Any]) -> str:
    if cr.get("status") == "queued":
        return "QUEUED"
    if cr.get("status") != "completed":
        return "RUNNING"
    title = str((cr.get("output") or {}).get("title") or "")
    if title.startswith("INFRA_ERROR"):
        return "INFRA_ERROR"                           # infrastruttura: mai un FAIL del codice, mai un PASS
    return _CONCLUSION_STATE.get(str(cr.get("conclusion")), "INFRA_ERROR")   # sconosciuto => mai PASS


def suite_status(suite_id: str, sha: str, check_runs: list, catalog_sha: str) -> Dict[str, Any]:
    """Esito di una suite: solo l'app fidata, solo lo SHA esatto, solo il catalogo vigente; se un run e' ancora
    aperto, conta quello.

    Il check run deve portare ``external_id`` che inizia con ``<catalog_sha256>:`` del catalogo letto da main
    (rilievo di Sol su #606): se la definizione di una suite cambia senza cambiarne l'id, un PASS ottenuto con il
    catalogo precedente non certifica la definizione nuova, e la suite torna ``NOT_REQUESTED``."""
    if not re.fullmatch(r"[0-9a-f]{64}", str(catalog_sha)):
        raise Refused("MALFORMED_CATALOG_SHA")
    prefix = f"{catalog_sha}:"
    mine = [c for c in check_runs if c.get("name") == check_name(suite_id) and c.get("head_sha") == sha
            and ((c.get("app") or {}).get("slug")) == TRUSTED_APP
            and str(c.get("external_id") or "").startswith(prefix)]
    if not mine:
        return {"suite": suite_id, "sha": sha, "state": "NOT_REQUESTED"}
    open_runs = [c for c in mine if c.get("status") != "completed"]
    last = max(open_runs or mine, key=lambda c: int(c.get("id") or 0))
    return {"suite": suite_id, "sha": sha, "state": state_from_check_run(last), "check_run_id": last.get("id")}


def campaign_state(states: List[str]) -> str:
    """Verdetto complessivo: PASS solo se TUTTE le suite sono PASS."""
    if not states:
        return "BLOCKED"
    for s in ("INFRA_ERROR", "FAIL", "TIMEOUT", "CANCELLED", "BLOCKED", "NOT_REQUESTED", "RUNNING", "QUEUED"):
        if s in states:
            return s
    return "PASS" if all(s == "PASS" for s in states) else "BLOCKED"


# ── CLI ──────────────────────────────────────────────────────────────────────────────────────────────────────────
def _gh(path: str) -> Any:
    out = subprocess.run(["gh", "api", path], capture_output=True, text=True, timeout=60, check=False)
    if out.returncode != 0:
        raise Refused("GITHUB_API_UNAVAILABLE")
    return json.loads(out.stdout or "null")


MAX_CHECK_RUN_PAGES = 20


def _gh_pages(path: str) -> Any:
    """Tutte le pagine di un elenco, con ``page=N`` esplicito sullo stesso percorso ``repos/<owner>/<repo>/…``.

    Non si usa ``gh --paginate``: segue i link ``next`` di GitHub, che puntano a ``repositories/<id>/…`` (non sempre
    raggiungibile, per esempio dietro un proxy). Si chiede la pagina successiva finche' l'ultima e' piena; oltre
    ``MAX_CHECK_RUN_PAGES`` il risultato non e' verificabile (``Refused``), mai un elenco parziale."""
    sep = "&" if "?" in path else "?"
    pages = []
    for n in range(1, MAX_CHECK_RUN_PAGES + 1):
        page = _gh(f"{path}{sep}page={n}")
        pages.append(page)
        items = page.get("check_runs") if isinstance(page, dict) else None
        if not isinstance(items, list) or len(items) < 100:
            return pages
    raise Refused("GITHUB_API_TOO_MANY_PAGES")


def all_check_runs(repo: str, sha: str) -> list:
    """Check run del commit su tutte le pagine. Forma inattesa => ``Refused``, mai un elenco vuoto (che direbbe
    ``NOT_REQUESTED`` per suite magari concluse)."""
    # filter=all: con il default (latest) GitHub nasconde i run piu' vecchi per nome, e un rerun aperto potrebbe non
    # comparire mentre resta visibile un PASS (rilievo di Sol su #606). suite_status preferisce comunque i run aperti.
    pages = _gh_pages(f"repos/{repo}/commits/{sha}/check-runs?filter=all&per_page=100")
    if not isinstance(pages, list) or not all(isinstance(p, dict) and isinstance(p.get("check_runs"), list)
                                              for p in pages):
        raise Refused("GITHUB_API_MALFORMED")
    return [c for p in pages for c in p["check_runs"]]


def main(argv: Optional[list] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build", help="nel workflow: costruisce la richiesta (input da variabili d'ambiente)")
    b.add_argument("--output", required=True)
    sub.add_parser("validate-catalog", help="valida tests/catalog/pf-test-catalog.json")
    s = sub.add_parser("status", help="stato delle suite di un target per uno SHA")
    s.add_argument("--repo", required=True)
    s.add_argument("--target", required=True)
    s.add_argument("--sha", required=True)
    s.add_argument("--dev-key", default="", help="corsia development: idempotency_key della richiesta")
    args = ap.parse_args(argv)
    catalog_raw = (ROOT / CATALOG_PATH).read_bytes()
    try:
        if args.cmd == "validate-catalog":
            cat = validate_catalog(catalog_raw)
            print(json.dumps({"valid": True, "version": cat["version"], "suites": len(cat["suites"]),
                              "catalog_sha256": catalog_digest(catalog_raw)}))
            return 0
        if args.cmd == "build":
            env = os.environ.get
            repo, actor, pr = env("GITHUB_REPOSITORY", ""), env("GITHUB_ACTOR", ""), env("PF_REQ_PR", "").strip()
            if not re.fullmatch(r"[1-9][0-9]{0,6}", pr) or not re.fullmatch(r"[A-Za-z0-9-]{1,39}", actor):
                raise Refused("MALFORMED_INPUT")         # validato PRIMA di comporre qualunque URL
            pr_data = _gh(f"repos/{repo}/pulls/{pr}")
            permission = str((_gh(f"repos/{repo}/collaborators/{actor}/permission") or {}).get("permission") or "")
            sha = env("PF_REQ_SHA", "").strip()

            def exists(path: str) -> bool:
                if not _SHA.fullmatch(sha):
                    return False
                try:
                    return _gh(f"repos/{repo}/contents/{path}?ref={sha}") is not None
                except Refused:
                    return False                       # assente, o API non leggibile: mai eseguito

            req = build_request(target=env("PF_REQ_TARGET", "").strip(), pr=pr, sha=sha,
                                repo=repo, actor=actor, actor_permission=permission, pr_data=pr_data,
                                catalog_raw=catalog_raw, run_id=env("GITHUB_RUN_ID", "0"),
                                run_attempt=env("GITHUB_RUN_ATTEMPT", "0"), mode=env("PF_TEST_DISPATCH_MODE", ""),
                                path_exists_at_sha=exists)
            Path(args.output).write_text(json.dumps(req, sort_keys=True), encoding="utf-8")
            print(json.dumps({"accepted": True, "lane": req["lane"], "target": req["target"],
                              "suites": req["suites"], "dev_paths": req["dev_paths"], "pr": req["pr"],
                              "head_sha": req["head_sha"], "mode": req["mode"],
                              "idempotency_key": req["idempotency_key"]}))
            return 0
        if not _SHA.fullmatch(args.sha):
            raise Refused("MALFORMED_SHA")
        if args.dev_key:
            if not re.fullmatch(r"[0-9a-f]{64}", args.dev_key):
                raise Refused("MALFORMED_KEY")
            runs = all_check_runs(args.repo, args.sha)
            print(json.dumps(dev_status(args.dev_key, args.sha, runs), sort_keys=True))
            return 0
        suites = resolve_target(validate_catalog(catalog_raw), args.target)
        runs = all_check_runs(args.repo, args.sha)
        digest = catalog_digest(catalog_raw)
        per_suite = [suite_status(s, args.sha, runs, digest) for s in suites]
        print(json.dumps({"target": args.target, "sha": args.sha, "catalog_sha256": digest,
                          "state": campaign_state([p["state"] for p in per_suite]), "suites": per_suite},
                         sort_keys=True))
        return 0
    except Refused as exc:
        print(json.dumps({"accepted": False, "refused": str(exc)}))
        return 1


if __name__ == "__main__":
    sys.exit(main())
