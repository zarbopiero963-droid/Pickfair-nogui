#!/usr/bin/env python3
"""pfckpt — checkpoint universale e portabile per il lavoro multi-agente su Pickfair.

PF-UNIVERSAL-CHECKPOINT-V1 (schema pf-checkpoint/3) + PF-AGENT-CONTINUITY-01 (lease/fencing, puntatori CURRENT e
TASK_STATE, cattura della patch del codice, recover al checkpoint valido precedente, divergenza di baseline).

Fonte di verita': un branch GitHub di stato. Un checkpoint e' UN commit (atomico) che contiene la directory di stato:
STATE.json (schema versionato), MANIFEST.sha256 (hash di ogni altro file), CURRENT.json e TASK_STATE.json (puntatori
derivati da STATE, verificati), AGENT_LEASE.json (ownership con scadenza e fencing token), HANDOFF.md, RUNBOOK.md,
artefatti e, se catturata, la patch del codice non committato (code_patch/). Solo Python standard library e git.
Nessuna dipendenza dalla memoria della conversazione o da file locali.

Comandi:
  verify    --repo R --ref REF [--dir D]                verifica il checkpoint DENTRO il tree committato di REF
  save      --repo R --branch B --remote O --expected SHA --message M --agent A [--ttl-min N] [--release]
            [--code-repo PATH] [--code-branch C] [--dir D]
                                                        pubblica in modo atomico e verificato, ripresa idempotente;
                                                        rinnova (o rilascia) il lease; cattura la patch del codice
  bootstrap --url URL --branch B --dest DEST [--code-branch C] [--agent A] [--dir D]
                                                        ripresa da zero in un ambiente pulito (START/RESUME)
  recover   --url URL --branch B --dest DEST [--dir D]   materializza l'ULTIMO checkpoint valido, senza toccare il remoto
Esiti: una riga JSON; exit 0 = OK, 2 = checkpoint non valido, 3 = conflitto di concorrenza, 4 = readback fallito,
5 = errore git/ambiente, 6 = lease tenuto da un altro agente / scrittore stale.
"""
import argparse
import datetime as _dt
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile

SCHEMA = "pf-checkpoint/3"
DEFAULT_DIR = "checkpoint/pf-full-delivery"
STATUSES = {"PASS", "FAIL", "BLOCKED", "NOT VERIFIED", "MISSING"}
REQUIRED = {
    "schema": str, "repository": str, "seq": int, "mandate_id": str, "previous_checkpoint_sha": (str, type(None)),
    "code": dict, "lotto_corrente": str, "lotti_completati": list, "next_atomic_action": str, "dependencies": dict,
    "blocks": list, "owner_authorizations": dict, "tests_executed": list, "tests_missing": list,
    "missing_historical_evidence": list, "gate_a": str, "pr_505": str, "certification_verdict": str,
}
MANIFEST = "MANIFEST.sha256"
CURRENT = "CURRENT.json"
TASK_STATE = "TASK_STATE.json"
LEASE = "AGENT_LEASE.json"
CODE_PATCH_DIR = "code_patch"
DEFAULT_LEASE_MIN = 120
SECRET_PATTERNS = [re.compile(p) for p in (
    r"(?i)(api[_-]?key|app[_-]?key|password|secret|session[_-]?token)\s*[:=]\s*['\"][^'\"\s]{12,}",
    r"-----BEGIN [A-Z ]*PRIVATE KEY-----", r"ghp_[A-Za-z0-9]{30,}", r"sk-[A-Za-z0-9]{30,}")]
# L'unico file esente dallo scan: la copia dello scanner nel checkpoint, al suo percorso esatto (relativo alla
# directory del checkpoint). Un file con lo stesso nome altrove (es. results/pfckpt.py) viene scansionato.
SECRET_SCAN_EXEMPT = frozenset({"tools/pfckpt.py"})


class CheckpointError(Exception):
    def __init__(self, code, reason, **detail):
        super().__init__(reason)
        self.code, self.reason, self.detail = code, reason, detail


def git(repo, *args, check=True, input_bytes=None):
    p = subprocess.run(["git", "-C", repo, *args], capture_output=True, input=input_bytes)
    if check and p.returncode != 0:
        raise CheckpointError(5, "GIT_ERROR", cmd=" ".join(args), stderr=p.stderr.decode(errors="replace")[-400:])
    return p


def out(repo, *args):
    return git(repo, *args).stdout.decode().strip()


def utcnow():
    return _dt.datetime.now(_dt.timezone.utc)


def iso(ts):
    return ts.replace(microsecond=0).isoformat()


def parse_iso(text):
    try:
        ts = _dt.datetime.fromisoformat(str(text).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return ts if ts.tzinfo else ts.replace(tzinfo=_dt.timezone.utc)


# ------------------------------------------------------------------ lettura dal TREE committato (mai dal disco)
def tree_files(repo, ref, d):
    names = out(repo, "ls-tree", "-r", "--name-only", ref, "--", d).splitlines()
    prefix = d.rstrip("/") + "/"
    return {n[len(prefix):]: n for n in names if n.startswith(prefix)}


def blob(repo, ref, path):
    return git(repo, "show", f"{ref}:{path}").stdout


def tree_json(repo, ref, d, name):
    """JSON di un file del checkpoint al commit REF, o None se assente; ValueError se corrotto."""
    files = tree_files(repo, ref, d)
    if name not in files:
        return None
    return json.loads(blob(repo, ref, files[name]))


def parse_manifest(text):
    entries = {}
    for i, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        m = re.fullmatch(r"([0-9a-f]{64}) [ *]\.?/?(.+)", line.strip())
        if not m:
            raise CheckpointError(2, "MANIFEST_MALFORMED", line=i)
        entries[m.group(2)] = m.group(1)
    return entries


def validate_state(state):
    errors = []
    for k, t in REQUIRED.items():
        if k not in state:
            errors.append(f"campo mancante: {k}")
        elif not isinstance(state[k], t):
            errors.append(f"tipo errato: {k}")
    if state.get("schema") != SCHEMA:
        errors.append(f"schema {state.get('schema')!r} != {SCHEMA}")
    code = state.get("code") or {}
    if not re.fullmatch(r"[0-9a-f]{40}", str(code.get("sha", ""))):
        errors.append("code.sha deve essere uno SHA completo di 40 caratteri")
    for t in state.get("tests_executed") or []:
        if not isinstance(t, dict) or t.get("status") not in STATUSES:
            errors.append(f"test con stato non ammesso: {t!r:.120}")
        elif t.get("status") in ("PASS", "FAIL") and not re.fullmatch(r"[0-9a-f]{7,40}", str(t.get("sha", ""))):
            errors.append(f"test {t.get('id')!r} PASS/FAIL senza SHA del codice")
    if state.get("certification_verdict") not in {"PASS", "PARTIAL", "NO-GO", "BLOCKED", "NOT VERIFIED"}:
        errors.append("certification_verdict non ammesso")
    return errors


def derived_current(state, commit=None):
    """CURRENT.json: puntatore minimo, leggibile da qualunque agente con un solo `git show`."""
    return {"schema": "pf-current/1", "seq": state["seq"], "previous_checkpoint_sha": state.get("previous_checkpoint_sha"),
            "code": {"branch": (state.get("code") or {}).get("branch"), "sha": (state.get("code") or {}).get("sha")},
            "mandate_id": state["mandate_id"], "next_atomic_action": state["next_atomic_action"],
            "certification_verdict": state["certification_verdict"], "checkpoint_commit": commit}


def derived_task_state(state):
    return {"schema": "pf-task-state/1", "mandate_id": state["mandate_id"], "lotto_corrente": state["lotto_corrente"],
            "lotti_completati": state["lotti_completati"], "next_atomic_action": state["next_atomic_action"],
            "blocks": state["blocks"], "dependencies": state["dependencies"], "tests_missing": state["tests_missing"],
            "gate_a": state["gate_a"], "pr_505": state["pr_505"]}


def validate_derived(state, current, task_state):
    """I puntatori derivati devono coincidere con STATE.json: un puntatore diverso e' un checkpoint incoerente."""
    problems = []
    if current is not None and not isinstance(current, dict):
        return [f"{CURRENT} non e' un oggetto JSON"]
    if task_state is not None and not isinstance(task_state, dict):
        return [f"{TASK_STATE} non e' un oggetto JSON"]
    if current is not None:
        expected = derived_current(state, current.get("checkpoint_commit"))
        for k, v in expected.items():
            if current.get(k) != v:
                problems.append(f"{CURRENT}.{k} != STATE.json")
    if task_state is not None:
        expected = derived_task_state(state)
        for k, v in expected.items():
            if task_state.get(k) != v:
                problems.append(f"{TASK_STATE}.{k} != STATE.json")
    return problems


def validate_lease(lease, seq):
    problems = []
    if lease is None:
        return problems
    if not isinstance(lease, dict) or lease.get("schema") != "pf-lease/1":
        return [f"{LEASE} malformato"]
    if lease.get("owner") is not None:
        if not isinstance(lease.get("fencing_token"), int) or lease["fencing_token"] != seq:
            problems.append(f"{LEASE}.fencing_token {lease.get('fencing_token')!r} != seq {seq}")
        if parse_iso(lease.get("acquired_at")) is None or parse_iso(lease.get("expires_at")) is None:
            problems.append(f"{LEASE} senza acquired_at/expires_at validi")
    return problems


def verify(repo, ref, d=DEFAULT_DIR, code_repo=None):
    """Verifica completa del checkpoint al commit REF. Ritorna un dict di report; solleva CheckpointError(2) se invalido."""
    commit = out(repo, "rev-parse", "--verify", f"{ref}^{{commit}}")
    files = tree_files(repo, commit, d)
    problems = []
    for req in ("STATE.json", MANIFEST):
        if req not in files:
            raise CheckpointError(2, "FILE_REQUIRED_MISSING", file=req, commit=commit)
    try:
        state = json.loads(blob(repo, commit, files["STATE.json"]))
    except ValueError as exc:
        raise CheckpointError(2, "STATE_JSON_INVALID", error=str(exc)[:200], commit=commit)
    if not isinstance(state, dict):
        # JSON valido ma non oggetto: checkpoint invalido strutturato, mai un AttributeError che fermerebbe
        # `last_valid_checkpoint`/`recover` (rilievo Codex r4308704074)
        raise CheckpointError(2, "STATE_JSON_INVALID", error=f"non un oggetto: {type(state).__name__}", commit=commit)
    problems += validate_state(state)
    manifest = parse_manifest(blob(repo, commit, files[MANIFEST]).decode(errors="replace"))
    listed, present = set(manifest), set(files) - {MANIFEST}
    for f in sorted(listed - present):
        problems.append(f"artefatto nel manifest ma assente dal commit: {f}")
    for f in sorted(present - listed):
        problems.append(f"file nel commit ma non nel manifest: {f}")
    for f in sorted(listed & present):
        data = blob(repo, commit, files[f])
        if hashlib.sha256(data).hexdigest() != manifest[f]:
            problems.append(f"hash errato: {f}")
        if f not in SECRET_SCAN_EXEMPT:
            # esente SOLO lo scanner stesso al suo percorso autorizzato (contiene i pattern): ogni altro
            # file, sorgenti Python del code_patch e un artifact chiamato pfckpt.py altrove compresi, viene
            # scansionato (rilievi Codex e Sol #512)
            for pat in SECRET_PATTERNS:
                if pat.search(data.decode(errors="ignore")):
                    problems.append(f"possibile segreto in {f}")
                    break
    parents = out(repo, "rev-list", "--parents", "-n", "1", commit).split()[1:]
    prev = state.get("previous_checkpoint_sha")
    if parents:
        parent = parents[0]
        if not prev or not parent.startswith(str(prev)):
            problems.append(f"previous_checkpoint_sha {prev!r} != parent {parent[:12]}")
        pfiles = tree_files(repo, parent, d)
        if "STATE.json" in pfiles:
            try:
                pstate = json.loads(blob(repo, parent, pfiles["STATE.json"]))
                pseq = pstate.get("seq") if isinstance(pstate, dict) else None
            except ValueError:
                pseq = None
            if isinstance(pseq, int) and state.get("seq") != pseq + 1:
                problems.append(f"seq {state.get('seq')} non monotono (precedente {pseq})")
    code_sha = (state.get("code") or {}).get("sha", "")
    lookup = code_repo or repo
    if code_sha and git(lookup, "cat-file", "-e", f"{code_sha}^{{commit}}", check=False).returncode != 0:
        problems.append(f"SHA del codice inesistente nel repository: {code_sha}")
    for t in state.get("tests_executed") or []:
        if isinstance(t, dict) and t.get("status") in ("PASS", "FAIL"):
            # rilievo Codex #512 su 034af59: lo SHA di un test eseguito deve risolversi a un commit reale,
            # con la stessa lookup di code.sha (altrimenti un esito e' attribuibile a codice inesistente)
            tsha = str(t.get("sha", ""))
            # Un'evidenza su un albero MAI pubblicato (esperimento scratch) e' ammessa solo se
            # dichiarata esplicitamente (`unpublished_tree: true` + `sha_note`): resta verificabile
            # come dichiarazione, mai inferita da uno SHA che semplicemente non si risolve.
            # Lo SHA dichiarato deve comunque essere un id di commit ben formato (riesame indipendente c9):
            # la dichiarazione esonera solo dalla risoluzione, mai dalla forma.
            # Vale per OGNI voce, dichiarata o no: un riferimento mobile (`HEAD`, `main`, `main~1`) si
            # risolverebbe ma legherebbe l'esito a un commit diverso nel tempo (secondo riesame c9).
            well_formed = re.fullmatch(r"[0-9a-f]{7,40}", tsha) is not None
            declared_unpublished = (t.get("unpublished_tree") is True and bool(str(t.get("sha_note", "")).strip()))
            if not well_formed or (not declared_unpublished
                                   and git(lookup, "cat-file", "-e", f"{tsha}^{{commit}}", check=False).returncode != 0):
                problems.append(f"test {t.get('id')!r}: test sha unresolved: {tsha}")
        art = t.get("artifact") if isinstance(t, dict) else None
        for a in ([art] if isinstance(art, str) else (art or [])):
            if a not in files:
                problems.append(f"test {t.get('id')!r}: artefatto dichiarato assente: {a}")
    # puntatori derivati e lease (PF-AGENT-CONTINUITY-01): coerenti con STATE, altrimenti checkpoint incoerente
    derived = {}
    for name in (CURRENT, TASK_STATE, LEASE):
        if name in files:
            try:
                derived[name] = json.loads(blob(repo, commit, files[name]))
            except ValueError:
                problems.append(f"{name} non e' JSON valido")
                derived[name] = None
    if not isinstance(state.get("seq"), int):
        pass
    else:
        problems += validate_derived(state, derived.get(CURRENT), derived.get(TASK_STATE))
        problems += validate_lease(derived.get(LEASE), state["seq"])
    patch_meta = (state.get("code") or {}).get("patch")
    if isinstance(patch_meta, dict):
        for f in patch_meta.get("files") or []:
            if f not in files:
                problems.append(f"code.patch: file dichiarato assente: {f}")
    if problems:
        raise CheckpointError(2, "CHECKPOINT_INVALID", commit=commit, problems=problems)
    lease = derived.get(LEASE) or {}
    return {"commit": commit, "seq": state["seq"], "code_sha": code_sha, "files": len(files),
            "next_atomic_action": state["next_atomic_action"], "verdict": state["certification_verdict"],
            "lease_owner": lease.get("owner"), "lease_expires_at": lease.get("expires_at")}


# ------------------------------------------------------------------ lease / fencing
def lease_status(lease, agent, now=None):
    """('FREE'|'MINE'|'HELD'|'EXPIRED', lease). Letto dal checkpoint REMOTO atteso, mai dal disco locale."""
    now = now or utcnow()
    if not isinstance(lease, dict) or lease.get("owner") in (None, ""):
        return "FREE", lease
    if lease.get("owner") == agent:
        return "MINE", lease
    exp = parse_iso(lease.get("expires_at"))
    if exp is None or exp <= now:
        return "EXPIRED", lease
    return "HELD", lease


def new_lease(agent, seq, ttl_min, now=None, released=False):
    now = now or utcnow()
    if released:
        return {"schema": "pf-lease/1", "owner": None, "fencing_token": None, "acquired_at": None, "expires_at": None,
                "released_by": agent, "released_at": iso(now)}
    return {"schema": "pf-lease/1", "owner": agent, "fencing_token": seq, "acquired_at": iso(now),
            "expires_at": iso(now + _dt.timedelta(minutes=ttl_min))}


# ------------------------------------------------------------------ cattura della patch del codice non committato
def capture_code_patch(code_repo, worktree, d):
    """Scrive in <d>/code_patch la patch tracciata e i file non tracciati del repo di codice; ritorna i metadati.
    Un checkpoint senza questa cattura non trasporta il lavoro non committato: con la cattura, un agente che
    riparte dal `code.sha` + patch riottiene lo stesso albero di lavoro (ignorati e __pycache__ esclusi)."""
    root = os.path.join(worktree, d, CODE_PATCH_DIR)
    shutil.rmtree(root, ignore_errors=True)
    os.makedirs(root, exist_ok=True)
    head = out(code_repo, "rev-parse", "HEAD")
    tree = out(code_repo, "rev-parse", "HEAD^{tree}")
    tracked = git(code_repo, "diff", "--binary", "HEAD").stdout
    # porcelain non va strip-ato: il primo carattere di stato puo' essere uno spazio
    # -z: percorsi NUL-terminati e mai quotati (rilievo Codex #512: `"space name.py"` quotato veniva
    # saltato da isfile e il checkpoint risultava durevole senza quel file)
    status = [l for l in git(code_repo, "status", "--porcelain", "-z", "--untracked-files=all").stdout.decode().split("\0") if l.strip()]
    modified = sorted(l[3:] for l in status if not l.startswith("??"))
    untracked = sorted(l[3:] for l in status if l.startswith("??") and "__pycache__" not in l and not l.endswith(".pyc"))
    files = []
    if tracked:
        with open(os.path.join(root, "tracked.patch"), "wb") as fh:
            fh.write(tracked)
        files.append(f"{CODE_PATCH_DIR}/tracked.patch")
    for rel in untracked:
        src = os.path.join(code_repo, rel)
        if not os.path.isfile(src):
            continue
        dst = os.path.join(root, "untracked", rel)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copyfile(src, dst)
        files.append(f"{CODE_PATCH_DIR}/untracked/{rel}")
    meta = {"head": head, "tree": tree, "captured_at": iso(utcnow()), "modified": modified, "untracked": untracked,
            "files": files}
    with open(os.path.join(root, "BASELINE.json"), "w") as fh:
        json.dump(meta, fh, indent=1, sort_keys=True)
    files.append(f"{CODE_PATCH_DIR}/BASELINE.json")
    meta["files"] = files
    return meta


def restore_code_patch(ckpt_repo, ref, d, code_repo):
    """Applica al repo di codice (che DEVE essere pulito e sullo SHA di baseline) la patch catturata nel checkpoint."""
    meta = tree_json(ckpt_repo, ref, d, f"{CODE_PATCH_DIR}/BASELINE.json")
    if meta is None:
        raise CheckpointError(2, "NO_CODE_PATCH", ref=ref)
    if out(code_repo, "rev-parse", "HEAD") != meta["head"]:
        raise CheckpointError(3, "CODE_BASELINE_MISMATCH", head=out(code_repo, "rev-parse", "HEAD"), expected=meta["head"])
    if out(code_repo, "status", "--porcelain"):
        raise CheckpointError(3, "CODE_REPO_DIRTY")
    files = tree_files(ckpt_repo, ref, d)
    if f"{CODE_PATCH_DIR}/tracked.patch" in files:
        patch = blob(ckpt_repo, ref, files[f"{CODE_PATCH_DIR}/tracked.patch"])
        git(code_repo, "apply", "--binary", "--index", "-", input_bytes=patch)
        git(code_repo, "reset", "-q")
    for rel in meta.get("untracked") or []:
        key = f"{CODE_PATCH_DIR}/untracked/{rel}"
        if key not in files:
            raise CheckpointError(2, "CODE_PATCH_FILE_MISSING", file=key)
        dst = os.path.join(code_repo, rel)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        with open(dst, "wb") as fh:
            fh.write(blob(ckpt_repo, ref, files[key]))
    status = sorted(l[3:] for l in git(code_repo, "status", "--porcelain", "--untracked-files=all").stdout.decode().splitlines() if l.strip())
    expected = sorted(set(meta.get("modified") or []) | set(meta.get("untracked") or []))
    if status != expected:
        raise CheckpointError(4, "CODE_PATCH_RESTORE_MISMATCH", restored=status, expected=expected)
    return {"status": "CODE_RESTORED", "head": meta["head"], "modified": meta["modified"], "untracked": meta["untracked"]}


# ------------------------------------------------------------------ salvataggio atomico, concorrenza, readback
def write_manifest(worktree, d):
    root = os.path.join(worktree, d)
    lines = []
    for base, _dirs, names in os.walk(root):
        for n in names:
            rel = os.path.relpath(os.path.join(base, n), root).replace(os.sep, "/")
            if rel == MANIFEST or "/__pycache__/" in f"/{rel}" or rel.endswith(".pyc"):
                continue
            with open(os.path.join(base, n), "rb") as fh:
                lines.append(f"{hashlib.sha256(fh.read()).hexdigest()}  ./{rel}")
    with open(os.path.join(root, MANIFEST), "w") as fh:
        fh.write("\n".join(sorted(lines, key=lambda s: s.split("  ", 1)[1])) + "\n")


def write_derived(worktree, d, state, lease):
    root = os.path.join(worktree, d)
    with open(os.path.join(root, CURRENT), "w") as fh:
        json.dump(derived_current(state), fh, indent=1, sort_keys=True)
    with open(os.path.join(root, TASK_STATE), "w") as fh:
        json.dump(derived_task_state(state), fh, indent=1, sort_keys=True)
    with open(os.path.join(root, LEASE), "w") as fh:
        json.dump(lease, fh, indent=1, sort_keys=True)


def remote_head(repo, remote, branch):
    p = git(repo, "ls-remote", remote, f"refs/heads/{branch}", check=False)
    if p.returncode != 0:
        raise CheckpointError(5, "REMOTE_UNREACHABLE", stderr=p.stderr.decode(errors="replace")[-300:])
    line = p.stdout.decode().strip()
    return line.split()[0] if line else None


def readback(remote_url, branch, d, code_branch=None):
    """Clone pulito dal remoto e verifica completa li': l'unica prova di durabilita'."""
    tmp = tempfile.mkdtemp(prefix="pfckpt_readback_")
    try:
        p = subprocess.run(["git", "clone", "-q", "--no-local", "--branch", branch, remote_url, tmp], capture_output=True)
        if p.returncode != 0:
            raise CheckpointError(4, "READBACK_CLONE_FAILED", stderr=p.stderr.decode(errors="replace")[-300:])
        if code_branch:
            git(tmp, "fetch", "-q", "origin", f"{code_branch}:refs/remotes/origin/{code_branch}", check=False)
        # Gli SHA di `tests_executed` possono stare su qualunque branch pubblicato (ciclo 8 #512: la
        # risoluzione e' obbligatoria): il readback scarica tutti i branch remoti, non solo quello di codice.
        git(tmp, "fetch", "-q", "origin", "+refs/heads/*:refs/remotes/origin/*", check=False)
        try:
            return verify(tmp, "HEAD", d)
        except CheckpointError as exc:
            raise CheckpointError(4, "READBACK_VERIFY_FAILED", inner=exc.reason, **exc.detail)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def save(repo, branch, remote, expected, message, d=DEFAULT_DIR, code_branch=None, agent=None,
         ttl_min=DEFAULT_LEASE_MIN, release=False, code_repo=None, now=None):
    """Pubblicazione verificata. Ripresa idempotente: rieseguire save dopo un crash in qualunque punto e' sicuro.
    expected = SHA remoto da cui l'agente e' partito (controllo ottimistico); mai force-push, mai merge di stato.
    agent = identita' dello scrittore: il lease si legge dal checkpoint remoto atteso; tenuto da un altro agente e
    non scaduto => exit 6, nessuna scrittura."""
    url = out(repo, "remote", "get-url", remote)
    current = remote_head(repo, remote, branch)
    head = out(repo, "rev-parse", "HEAD")
    parent = out(repo, "rev-parse", "HEAD^") if git(repo, "rev-parse", "--verify", "HEAD^", check=False).returncode == 0 else None
    if current == head and head != expected and parent == expected:
        # crash dopo il push (prima del readback): il remoto ha gia' il NOSTRO checkpoint figlio di expected
        report = readback(url, branch, d, code_branch)
        if report["commit"] != head:
            raise CheckpointError(4, "READBACK_SHA_MISMATCH", remote=report["commit"])
        return {"status": "DURABLE", "resumed": "after_push", **report}
    if current != expected:
        raise CheckpointError(3, "CONCURRENT_UPDATE", expected=expected, remote=current,
                              action="leggere il nuovo checkpoint, riconciliare, ripartire da quello SHA")
    resumed = None
    if parent == expected and head != expected and git(repo, "diff", "--quiet", "HEAD", "--", d, check=False).returncode == 0 \
            and not out(repo, "status", "--porcelain", "--", d):
        resumed = "after_commit"            # crash dopo il commit locale e prima del push: si riusa il commit
    else:
        if head != expected:
            raise CheckpointError(3, "LOCAL_BASE_MISMATCH", head=head, expected=expected,
                                  action="il worktree deve partire dallo SHA remoto atteso")
        # lease: si legge dal commit atteso (quello che il remoto ha davvero), non dal disco
        try:
            remote_lease = tree_json(repo, expected, d, LEASE)
        except ValueError:
            raise CheckpointError(2, "LEASE_CORRUPT", commit=expected)
        status, _ = lease_status(remote_lease, agent, now)
        if status == "HELD":
            raise CheckpointError(6, "LEASE_HELD", owner=remote_lease.get("owner"), expires_at=remote_lease.get("expires_at"),
                                  action="attendere la scadenza o il rilascio; nessuna scrittura")
        if agent in (None, ""):
            raise CheckpointError(6, "AGENT_REQUIRED", action="ogni scrittura dichiara l'agente (--agent)")
        state_path = os.path.join(repo, d, "STATE.json")
        st = json.load(open(state_path))
        if not str(expected).startswith(str(st.get("previous_checkpoint_sha") or "")) or not st.get("previous_checkpoint_sha"):
            raise CheckpointError(2, "PREVIOUS_SHA_MISMATCH", declared=st.get("previous_checkpoint_sha"), expected=expected)
        errors = validate_state(st)
        if errors:
            raise CheckpointError(2, "STATE_INVALID", problems=errors)
        st.setdefault("agent", {})
        if isinstance(st["agent"], dict):
            st["agent"]["last_writer"] = agent
            st["agent"]["lease_status_at_write"] = status
        if code_repo:
            st.setdefault("code", {})["patch"] = capture_code_patch(code_repo, repo, d)
        with open(state_path, "w") as fh:
            json.dump(st, fh, indent=1, ensure_ascii=False)
        write_derived(repo, d, st, new_lease(agent, st["seq"], ttl_min, now, released=release))
        write_manifest(repo, d)
        git(repo, "add", "-A", "-f", "--", d)          # -f: un .gitignore non puo' escludere artefatti in silenzio
        git(repo, "commit", "-q", "-m", message)
    verify(repo, "HEAD", d)                         # verifica del TREE committato prima di pubblicare
    untracked = [l for l in out(repo, "status", "--porcelain", "--ignored", "--", d).splitlines()
                 if not l.endswith(".pyc") and "__pycache__" not in l]
    if untracked:
        raise CheckpointError(2, "LOCAL_ONLY_FILES", files=untracked[:20])
    p = git(repo, "push", "-q", remote, f"HEAD:refs/heads/{branch}", check=False)   # senza force: non-FF rifiutato
    if p.returncode != 0:
        now_remote = remote_head(repo, remote, branch)
        if now_remote == out(repo, "rev-parse", "HEAD"):
            resumed = "push_reported_error_but_landed"
        else:
            raise CheckpointError(3, "PUSH_REJECTED", remote=now_remote, stderr=p.stderr.decode(errors="replace")[-300:])
    report = readback(url, branch, d, code_branch)
    if report["commit"] != out(repo, "rev-parse", "HEAD"):
        raise CheckpointError(4, "READBACK_SHA_MISMATCH", remote=report["commit"])
    return {"status": "DURABLE", "resumed": resumed, **report}


# ------------------------------------------------------------------ ripresa da zero, recover, divergenza
def last_valid_checkpoint(repo, d, start="HEAD", limit=200):
    for c in out(repo, "rev-list", "--first-parent", start).splitlines()[:limit]:
        try:
            verify(repo, c, d)
            return c
        except CheckpointError:
            continue
    return None


def code_divergence(repo, code_branch, code_sha):
    """Il branch di codice e' avanzato (o divergente) rispetto allo SHA del checkpoint?"""
    ref = f"refs/remotes/origin/{code_branch}"
    if git(repo, "rev-parse", "--verify", ref, check=False).returncode != 0:
        return {"status": "CODE_BRANCH_UNKNOWN"}
    head = out(repo, "rev-parse", ref)
    if head == code_sha:
        return {"status": "ALIGNED", "head": head}
    ahead = out(repo, "rev-list", "--count", f"{code_sha}..{head}") if git(repo, "cat-file", "-e", f"{code_sha}^{{commit}}", check=False).returncode == 0 else "?"
    behind = out(repo, "rev-list", "--count", f"{head}..{code_sha}") if ahead != "?" else "?"
    return {"status": "DIVERGED", "head": head, "ahead": ahead, "behind": behind,
            "action": "non completare un lotto con risultati di un'altra versione: riconciliare esplicitamente"}


def bootstrap(url, branch, dest, d=DEFAULT_DIR, code_branch=None, agent=None):
    p = subprocess.run(["git", "clone", "-q", "--no-local", "--branch", branch, url, dest], capture_output=True)
    if p.returncode != 0:
        raise CheckpointError(5, "CLONE_FAILED", stderr=p.stderr.decode(errors="replace")[-300:])
    if code_branch:
        git(dest, "fetch", "-q", "origin", f"{code_branch}:refs/remotes/origin/{code_branch}", check=False)
    try:
        report = verify(dest, "HEAD", d)
    except CheckpointError as exc:
        last_valid = last_valid_checkpoint(dest, d, "HEAD~1") if out(dest, "rev-list", "--count", "HEAD") != "1" else None
        raise CheckpointError(2, "LATEST_CHECKPOINT_CORRUPT", inner=exc.reason, problems=exc.detail.get("problems"),
                              last_valid_checkpoint=last_valid, action="nessun rollback automatico: decidere esplicitamente")
    state = json.loads(blob(dest, report["commit"], f"{d}/STATE.json"))
    lease = tree_json(dest, report["commit"], d, LEASE)
    status, _ = lease_status(lease, agent)
    result = {"status": "READY", **report, "mandate_id": state["mandate_id"],
              "owner_authorizations": state["owner_authorizations"], "blocks": state["blocks"],
              "code_branch": (state.get("code") or {}).get("branch"), "lease": status,
              "code_patch": bool((state.get("code") or {}).get("patch"))}
    if code_branch:
        result["code_divergence"] = code_divergence(dest, code_branch, report["code_sha"])
    if status == "HELD":
        result["status"] = "READY_READ_ONLY"
        result["action"] = f"lease tenuto da {lease.get('owner')} fino a {lease.get('expires_at')}: nessuna scrittura"
    return result


def recover(url, branch, dest, d=DEFAULT_DIR):
    """Materializza in DEST l'ultimo checkpoint VALIDO della catena, senza toccare il remoto."""
    p = subprocess.run(["git", "clone", "-q", "--no-local", "--branch", branch, url, dest], capture_output=True)
    if p.returncode != 0:
        raise CheckpointError(5, "CLONE_FAILED", stderr=p.stderr.decode(errors="replace")[-300:])
    head = out(dest, "rev-parse", "HEAD")
    valid = last_valid_checkpoint(dest, d, "HEAD")
    if valid is None:
        raise CheckpointError(2, "NO_VALID_CHECKPOINT", head=head)
    git(dest, "checkout", "-q", "--detach", valid)
    report = verify(dest, "HEAD", d)
    return {"status": "RECOVERED", "remote_head": head, "skipped_invalid": out(dest, "rev-list", "--count", f"{valid}..{head}"),
            **report}


def main(argv=None):
    ap = argparse.ArgumentParser(prog="pfckpt")
    sub = ap.add_subparsers(dest="cmd", required=True)
    v = sub.add_parser("verify"); v.add_argument("--repo", default="."); v.add_argument("--ref", default="HEAD")
    v.add_argument("--dir", default=DEFAULT_DIR); v.add_argument("--code-repo")
    s = sub.add_parser("save"); s.add_argument("--repo", default="."); s.add_argument("--branch", required=True)
    s.add_argument("--remote", default="origin"); s.add_argument("--expected", required=True)
    s.add_argument("--message", required=True); s.add_argument("--dir", default=DEFAULT_DIR); s.add_argument("--code-branch")
    s.add_argument("--agent", required=True); s.add_argument("--ttl-min", type=int, default=DEFAULT_LEASE_MIN)
    s.add_argument("--release", action="store_true"); s.add_argument("--code-repo")
    b = sub.add_parser("bootstrap"); b.add_argument("--url", required=True); b.add_argument("--branch", required=True)
    b.add_argument("--dest", required=True); b.add_argument("--dir", default=DEFAULT_DIR); b.add_argument("--code-branch")
    b.add_argument("--agent")
    r = sub.add_parser("recover"); r.add_argument("--url", required=True); r.add_argument("--branch", required=True)
    r.add_argument("--dest", required=True); r.add_argument("--dir", default=DEFAULT_DIR)
    rc = sub.add_parser("restore-code"); rc.add_argument("--repo", default="."); rc.add_argument("--ref", default="HEAD")
    rc.add_argument("--dir", default=DEFAULT_DIR); rc.add_argument("--code-repo", required=True)
    a = ap.parse_args(argv)
    try:
        if a.cmd == "verify":
            res = {"status": "VALID", **verify(a.repo, a.ref, a.dir, a.code_repo)}
        elif a.cmd == "save":
            res = save(a.repo, a.branch, a.remote, a.expected, a.message, a.dir, a.code_branch, agent=a.agent,
                       ttl_min=a.ttl_min, release=a.release, code_repo=a.code_repo)
        elif a.cmd == "bootstrap":
            res = bootstrap(a.url, a.branch, a.dest, a.dir, a.code_branch, agent=a.agent)
        elif a.cmd == "recover":
            res = recover(a.url, a.branch, a.dest, a.dir)
        else:
            res = restore_code_patch(a.repo, a.ref, a.dir, a.code_repo)
        print(json.dumps(res, ensure_ascii=False))
        return 0
    except CheckpointError as exc:
        print(json.dumps({"status": "ERROR", "reason": exc.reason, **exc.detail}, ensure_ascii=False, default=str))
        return exc.code


if __name__ == "__main__":
    sys.exit(main())
