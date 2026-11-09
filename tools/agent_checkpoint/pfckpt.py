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
            [--code-repo PATH] [--code-branch C] [--dir D] [--keep-refs] [--allow-branch-only] [--durable-branch B]
                                                        pubblica in modo atomico e verificato, ripresa idempotente;
                                                        rinnova (o rilascia) il lease; cattura la patch del codice
  bootstrap --url URL --branch B --dest DEST [--code-branch C] [--agent A] [--dir D]
                                                        ripresa da zero in un ambiente pulito (START/RESUME)
  recover   --url URL --branch B --dest DEST [--dir D]   materializza l'ULTIMO checkpoint valido, senza toccare il remoto
  integrate --repo R --original SHA --integration SHA [--original-tree T]
                                                        calcola il record `integration` (squash merge -> main) dai tree
  handoff-verify --repo R --source SHA --patch FILE --base SHA --integration SHA
  (verify/save/bootstrap/recover) --repo-path NOME=PERCORSO   repository citati dagli handoff, ripetibile
                                                        prova (ricalcolata) che la patch consegnata su base = integrazione
SHA durevoli (pfckpt_squash_durable_refs): al save ogni SHA di codice citato deve essere raggiungibile sul remoto da
un branch durevole (default `main`), da `refs/pull/<N>/head` (dichiarato in `pr` o scoperto) o, tramite `integration`
TREE_EQUAL, dal commit di integrazione su un branch durevole; altrimenti CODE_SHA_NOT_DURABLE e nulla e' pubblicato.
Il readback lo riverifica da un clone pulito. I ref permanenti `refs/agent-state/keep/<sha>` sono OPZIONALI
(`--keep-refs`, disattivati di default: il proxy delle sessioni rifiuta ref fuori da refs/heads e refs/tags).
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
# Ref permanenti degli SHA citati (opt-in `--keep-refs`): fuori da refs/heads e refs/tags, uno per SHA, pubblicati
# senza force nello stesso push atomico del checkpoint. Disattivati di default: il proxy delle sessioni rifiuta i ref
# fuori da refs/heads e refs/tags, e un branch/tag per SHA farebbe partire la CI on-push a ogni checkpoint.
KEEP_REF_PREFIX = "refs/agent-state/keep/"
DEFAULT_DURABLE_BRANCHES = ("main",)
DURABILITY_NS = "refs/pfckpt-durability/"          # namespace SOLO locale (mai pubblicato) per le punte remote
KEEP_REFSPEC = f"+{KEEP_REF_PREFIX}*:{KEEP_REF_PREFIX}*"
EQUIVALENCES = ("TREE_EQUAL", "NOT_PROVEN")
INTEGRATION_FIELDS = ("original_sha", "original_tree", "integration_sha", "integration_tree", "equivalence")
_SHA40 = re.compile(r"[0-9a-f]{40}")
HANDOFF_STATUSES = ("DELIVERED", "DURABLE", "INTEGRATED_VERIFIED", "INTEGRATED_NOT_PROVEN")
HANDOFF_TEXT_FIELDS = ("source_repo", "source_branch", "target_repo", "handoff_ref")
HANDOFF_REQUIRED_TEXT = ("checkpoint_repo", "source_repo", "target_repo", "handoff_ref")
HANDOFF_SHA_FIELDS = ("source_sha", "base_sha", "handoff_sha", "handoff_tree", "integration_sha", "integration_tree")


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


def _code_dict(state):
    c = state.get("code")
    return c if isinstance(c, dict) else {}


def _tests_list(state):
    """`tests_executed` come lista; un tipo errato (gia' segnalato da validate_state) non diventa mai un TypeError che
    fermerebbe verify/recover/bootstrap (rilievo Sol #615)."""
    t = state.get("tests_executed")
    return t if isinstance(t, list) else []


def validate_state(state):
    errors = []
    for k, t in REQUIRED.items():
        if k not in state:
            errors.append(f"campo mancante: {k}")
        elif not isinstance(state[k], t):
            errors.append(f"tipo errato: {k}")
    if state.get("schema") != SCHEMA:
        errors.append(f"schema {state.get('schema')!r} != {SCHEMA}")
    code = state.get("code") if isinstance(state.get("code"), dict) else {}
    if not re.fullmatch(r"[0-9a-f]{40}", str(code.get("sha", ""))):
        errors.append("code.sha deve essere uno SHA completo di 40 caratteri")
    for t in _tests_list(state):
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
            "code": {"branch": _code_dict(state).get("branch"), "sha": _code_dict(state).get("sha")},
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


def resolve_commit(repo, sha):
    """SHA completo del commit, o None. Solo id esadecimali: mai un riferimento mobile (HEAD, main, main~1)."""
    if not re.fullmatch(r"[0-9a-f]{7,40}", str(sha or "")):
        return None
    p = git(repo, "rev-parse", "-q", "--verify", f"{sha}^{{commit}}", check=False)
    return p.stdout.decode().strip() if p.returncode == 0 else None


def tree_of(repo, commit):
    return out(repo, "rev-parse", f"{commit}^{{tree}}")


def check_code_reference(repo, label, sha, integration, anchors=(), unresolved_msg=None):
    """Risolvibilita' di uno SHA di codice citato (code.sha o tests_executed[].sha).

    Senza `integration`: lo SHA deve risolversi. Con `integration` (squash merge verso main): se l'originale si risolve
    deve valere tree(original_sha) == original_tree; se non si risolve e' accettato SOLO con equivalence TREE_EQUAL e
    tree(integration_sha) == original_tree, confrontando i tree SHA (mai file per file) del commit di integrazione
    SPECIFICO (SHA completo, mai l'ultimo main). Ritorna (problems, keep_shas, info)."""
    problems, keep = [], []
    unresolved_msg = unresolved_msg or f"{label}: SHA inesistente nel repository: {sha}"
    full = resolve_commit(repo, sha)
    info = {"sha": sha, "original_reachable": bool(full), "via": None, "effective_sha": full}
    if full:
        for t in anchors:
            if t and tree_of(repo, full) != t:
                problems.append(f"{label}: tree({sha[:12]}) != code.tree registrato {t[:12]}")
    if integration is None:
        if full:
            keep.append(full)
            info["via"] = "ORIGINAL"
        else:
            problems.append(unresolved_msg)
        return problems, keep, info
    bad = [] if isinstance(integration, dict) else ["non un oggetto"]
    if not bad:
        for f in ("original_sha", "original_tree", "integration_sha", "integration_tree"):
            if not _SHA40.fullmatch(str(integration.get(f, ""))):
                bad.append(f)
        if integration.get("equivalence") not in EQUIVALENCES:
            bad.append("equivalence")
        if "original_sha" not in bad and not integration["original_sha"].startswith(str(sha)):
            bad.append("original_sha != SHA citato")
    if bad:
        problems.append(f"{label}: integration malformata: {', '.join(bad)}")
        if not full:
            problems.append(unresolved_msg)
        else:
            keep.append(full)
        return problems, keep, info
    otree, itree = integration["original_tree"], integration["integration_tree"]
    for t in anchors:
        if t and t != otree:
            problems.append(f"{label}: integration.original_tree != code.tree registrato {t[:12]}")
    if full:
        keep.append(full)
        if tree_of(repo, full) != otree:
            problems.append(f"{label}: tree(original_sha) != integration.original_tree")
    ifull = resolve_commit(repo, integration["integration_sha"])
    if ifull and tree_of(repo, ifull) != itree:
        problems.append(f"{label}: tree(integration_sha) != integration.integration_tree")
    if integration["equivalence"] == "TREE_EQUAL":
        if not ifull:
            problems.append(f"{label}: integration_sha irrisolto: {integration['integration_sha']}")
        elif itree != otree or tree_of(repo, ifull) != otree:
            problems.append(f"{label}: tree di integrazione diverso dall'originale: equivalenza non provata")
        else:
            info["integration_sha"] = ifull            # transizione provata: il commit di integrazione e' citabile
    if ifull:
        keep.append(ifull)
    if not full and integration["equivalence"] != "TREE_EQUAL":
        problems.append(f"{label}: originale irraggiungibile ed equivalenza {integration['equivalence']}")
    if not full and not any(anchors):
        # rilievo Sol #615: senza l'oggetto originale l'original_tree DICHIARATO e' un'autodichiarazione (potrebbe
        # essere il tree di main): vale solo se coincide con un code.tree registrato da un save quando l'oggetto c'era
        problems.append(f"{label}: original_tree non ancorato a un code.tree registrato da un checkpoint; recuperare "
                        "l'oggetto originale (es. refs/pull/<N>/head)")
    if not full and not problems:
        info.update(via="INTEGRATION_TREE_EQUAL", effective_sha=ifull)
    elif full and not problems:
        info["via"] = "ORIGINAL"
    if not full and problems:
        problems.append(unresolved_msg)
    return problems, keep, info


def build_integration(repo, original_sha, integration_sha, original_tree=None):
    """Record `integration` calcolato SOLO dagli oggetti git: tree dell'originale (dall'oggetto se presente, altrimenti
    dal tree gia' registrato e passato esplicitamente) e tree del commit di integrazione specifico. Mai per file."""
    ifull = resolve_commit(repo, integration_sha)
    if not ifull or not _SHA40.fullmatch(str(integration_sha)):
        raise CheckpointError(2, "INTEGRATION_UNRESOLVED", integration_sha=integration_sha,
                              action="serve lo SHA completo del commit di integrazione, risolvibile")
    if not _SHA40.fullmatch(str(original_sha)):
        raise CheckpointError(2, "ORIGINAL_SHA_MALFORMED", original_sha=original_sha)
    ofull = resolve_commit(repo, original_sha)
    if ofull:
        otree, source = tree_of(repo, ofull), "OBJECT"
        if original_tree and original_tree != otree:
            raise CheckpointError(2, "ORIGINAL_TREE_MISMATCH", declared=original_tree, actual=otree)
    elif original_tree and _SHA40.fullmatch(str(original_tree)):
        otree, source = original_tree, "RECORDED"
    else:
        raise CheckpointError(2, "ORIGINAL_TREE_UNKNOWN", original_sha=original_sha,
                              action="recuperare l'oggetto originale (es. refs/pull/<n>/head) o un tree registrato")
    itree = tree_of(repo, ifull)
    return {"original_sha": original_sha, "original_tree": otree, "integration_sha": ifull, "integration_tree": itree,
            "equivalence": "TREE_EQUAL" if itree == otree else "NOT_PROVEN", "original_tree_source": source}


def apply_patch_tree(repo, base_sha, patch):
    """Tree ottenuto applicando `patch` su `base_sha`, in un indice TEMPORANEO (nessun checkout, nessun effetto sul
    repository): `read-tree base` + `git apply --cached` + `write-tree`, equivalente a `git apply --index` in un
    worktree temporaneo. None se la patch non si applica."""
    fd, idx = tempfile.mkstemp(prefix="pfckpt_handoff_idx_")
    os.close(fd)
    os.unlink(idx)
    env = dict(os.environ, GIT_INDEX_FILE=idx)
    try:
        def run(*args, data=None):
            return subprocess.run(["git", "-C", repo, *args], capture_output=True, input=data, env=env)
        if run("read-tree", base_sha).returncode != 0:
            return None
        if run("apply", "--cached", "--binary", "-", data=patch).returncode != 0:
            return None
        p = run("write-tree")
        return p.stdout.decode().strip() if p.returncode == 0 else None
    finally:
        if os.path.exists(idx):
            os.unlink(idx)


def handoff_verify(repo, source, patch, base, integration, **meta):
    """Record di handoff con la prova ricalcolata: patch applicata su `base` == tree(`integration`)
    => INTEGRATED_VERIFIED, altrimenti INTEGRATED_NOT_PROVEN. Mai inferito da main o dai nomi dei branch."""
    for name, sha in (("source", source), ("base", base), ("integration", integration)):
        if not _SHA40.fullmatch(str(sha)) or not resolve_commit(repo, sha):
            raise CheckpointError(2, "HANDOFF_SHA_UNRESOLVED", field=name, sha=sha)
    applied = apply_patch_tree(repo, base, patch)
    itree = tree_of(repo, integration)
    rec = {**{k: v for k, v in meta.items() if v is not None}, "source_sha": source,
           "patch_sha256": hashlib.sha256(patch).hexdigest(), "base_sha": base, "integration_sha": integration,
           "integration_tree": itree, "applied_tree": applied,
           "status": "INTEGRATED_VERIFIED" if applied and applied == itree else "INTEGRATED_NOT_PROVEN"}
    return rec


# Forma canonica del digest di una patch di handoff: `git diff --binary --full-index base head` con la configurazione
# di default di git resa esplicita (rename detection attiva, prefissi a/ b/, algoritmo myers, 3 righe di contesto,
# nessun interHunkContext, nessun orderFile, sottomoduli sempre inclusi, niente colore, niente diff esterni o textconv), cosi' il risultato non
# dipende dalla config locale. Una forma diversa => digest diverso
# => prova fallita (fail-closed), mai una prova accettata per errore.
_CANONICAL_DIFF_CFG = ("-c", "diff.renames=true", "-c", "diff.noprefix=false", "-c", "diff.mnemonicPrefix=false",
                       "-c", "diff.algorithm=myers", "-c", "diff.relative=false", "-c", "diff.indentHeuristic=true",
                       "-c", "core.quotePath=true", "-c", "diff.context=3", "-c", "diff.interHunkContext=0",
                       "-c", "diff.suppressBlankEmpty=false")


def canonical_diff_sha256(repo, base_sha, head_sha):
    """sha256 di `git diff --binary --full-index base_sha head_sha` in forma canonica; None se git fallisce."""
    p = subprocess.run(["git", "-C", repo, *_CANONICAL_DIFF_CFG, "diff", "--no-color", "--no-ext-diff",
                        "--no-textconv", "--ignore-submodules=none", "--unified=3", "-O/dev/null", "--binary", "--full-index", base_sha, head_sha, "--"], capture_output=True)
    return hashlib.sha256(p.stdout).hexdigest() if p.returncode == 0 else None


def parse_repo_paths(items):
    """`--repo-path NAME=PATH` (ripetibile) -> {NAME: PATH}. Un NAME ripetuto o senza PATH e' un errore."""
    paths = {}
    for it in items or []:
        name, sep, path = str(it).partition("=")
        if not sep or not name.strip() or not path.strip() or name.strip() in paths:
            raise CheckpointError(5, "REPO_PATH_MALFORMED", item=it, action="usare --repo-path NOME=PERCORSO, una volta per nome")
        paths[name.strip()] = path.strip()
    return paths


def _published_on(repo, sha, branch, remote="origin"):
    """`sha` e' contenuto nel branch pubblicato `branch` di `repo`: via il remote-tracking `refs/remotes/<remote>/<branch>`
    del SOLO remote del repository (mai un fork o un altro remote aggiunto, rilievo Sol #615) o, se `repo` e' esso stesso
    il remoto (bare), via `refs/heads/<branch>`."""
    if not _valid_branch_name(repo, branch):
        return False                # mai una sintassi di revisione (`main@{1}`, `main~1`): solo un nome di branch
    ref = f"refs/heads/{branch}" if out(repo, "rev-parse", "--is-bare-repository") == "true" \
        else f"refs/remotes/{remote}/{branch}"
    p = git(repo, "show-ref", "--verify", "-s", ref, check=False)   # ref ESATTO, niente reflog (rilievo Sol #615)
    tip = p.stdout.decode().strip() if p.returncode == 0 else ""
    return bool(tip) and git(repo, "merge-base", "--is-ancestor", sha, tip, check=False).returncode == 0


def _valid_branch_name(repo, branch):
    b = str(branch or "")
    return bool(b) and "@{" not in b and not b.startswith("-") \
        and git(repo, "check-ref-format", f"refs/heads/{b}", check=False).returncode == 0


def _published_anywhere(repo, sha, remote="origin"):
    """`sha` raggiungibile da un ref PUBBLICATO di `repo`: un remote-tracking `refs/remotes/<remote>/*` (branch o
    `pull/*`) del solo remote del repository o, se `repo` e' esso stesso il remoto (bare), `refs/heads/*`. Un commit solo
    locale, o presente solo su un altro remote (fork), no."""
    bare = out(repo, "rev-parse", "--is-bare-repository") == "true"
    ns = "refs/heads/" if bare else f"refs/remotes/{remote}/"
    p = git(repo, "for-each-ref", "--contains", sha, "--format=%(refname)", ns, check=False)
    refs = [r for r in p.stdout.decode().splitlines() if r and not r.endswith("/HEAD")] if p.returncode == 0 else []
    return bool(refs)


def _norm_repo(name):
    n = str(name).strip().replace("\\", "/").rstrip("/").lower()
    return n[:-4] if n.endswith(".git") else n


def _repo_identity_ok(path, name):
    """Il percorso passato con --repo-path NOME=PERCORSO e' davvero NOME: l'URL del suo `origin` (o, per un bare, il
    percorso stesso) termina con `owner/name` (rilievo Sol #615: la prova di pubblicazione e' vincolata al remoto del
    target, non a un fork)."""
    if out(path, "rev-parse", "--is-bare-repository") == "true":
        where = os.path.abspath(path)
    else:
        p = git(path, "remote", "get-url", "origin", check=False)
        if p.returncode != 0:
            return False
        where = p.stdout.decode().strip()
    w, n = _norm_repo(where), _norm_repo(name)
    return w == n or w.endswith("/" + n) or w.endswith(":" + n)


def refresh_published_refs(path, remote="origin"):
    """Remote-tracking dei branch aggiornati E potati prima di usarli come prova di pubblicazione (rilievo Sol #615): un
    branch cancellato sul remoto non resta «pubblicato» per un ref locale obsoleto. Prima il fetch, poi l'elenco dei
    branch vivi: un branch cancellato nel mezzo viene comunque potato. I `refs/remotes/<remote>/pull/<N>` del fallback
    si riallineano a `refs/pull/<N>/head` del remoto o si tolgono. Remoto irraggiungibile => errore, mai una prova su
    ref vecchi."""
    if out(path, "rev-parse", "--is-bare-repository") == "true" \
            or git(path, "remote", "get-url", remote, check=False).returncode != 0:
        return                  # nessun remote: nessuna prova possibile, lo rifiuta il controllo d'identita'/pubblicazione
    p = git(path, "fetch", "-q", remote, f"+refs/heads/*:refs/remotes/{remote}/*", check=False)
    q = git(path, "ls-remote", "--heads", remote, check=False) if p.returncode == 0 else p
    if q.returncode != 0:
        raise CheckpointError(5, "PUBLISHED_REFS_REFRESH_FAILED", repo=path, remote=remote,
                              stderr=q.stderr.decode(errors="replace")[-300:])
    live = {ln.split("\t", 1)[1][len("refs/heads/"):] for ln in q.stdout.decode().splitlines() if "\trefs/heads/" in ln}
    pref = f"refs/remotes/{remote}/"
    local = out(path, "for-each-ref", "--format=%(refname) %(objectname)", pref).splitlines()
    for line in local:
        ref = line.split(" ", 1)[0]
        name = ref[len(pref):]
        if name != "HEAD" and not name.startswith("pull/") and name not in live:
            git(path, "update-ref", "-d", ref)
    # anche i `pull/<N>` del fallback valgono come prova: si riallineano a refs/pull/<N>/head del remoto (dopo un
    # force-push della PR) o si tolgono se la PR non c'e' piu' (rilievo Sol #615)
    pulls = {ln.split(" ", 1)[0][len(pref) + len("pull/"):]: ln.split(" ", 1)[1]
             for ln in local if ln.startswith(pref + "pull/")}
    if pulls:
        q = git(path, "ls-remote", remote, "refs/pull/*/head", check=False)
        if q.returncode != 0:
            raise CheckpointError(5, "PUBLISHED_REFS_REFRESH_FAILED", repo=path, remote=remote,
                                  stderr=q.stderr.decode(errors="replace")[-300:])
        live_pr = {}
        for ln in q.stdout.decode().splitlines():
            sha, _t, name = ln.partition("\t")
            if name.startswith("refs/pull/") and name.endswith("/head"):
                live_pr[name[len("refs/pull/"):-len("/head")]] = sha
        stale = [n for n, sha in pulls.items() if n in live_pr and live_pr[n] != sha]
        for n in pulls:
            if n not in live_pr:
                git(path, "update-ref", "-d", f"{pref}pull/{n}")
        if stale:
            p = git(path, "fetch", "-q", remote, *[f"+refs/pull/{n}/head:{pref}pull/{n}" for n in stale], check=False)
            if p.returncode != 0:
                raise CheckpointError(5, "PUBLISHED_REFS_REFRESH_FAILED", repo=path, remote=remote,
                                      stderr=p.stderr.decode(errors="replace")[-300:])


def check_handoff(repo, commit, files, i, h, chain_repo=None, repo_paths=None, ckpt_repo=None, remote="origin"):
    """Un record di `handoffs` (PF-MCP-CROSS-REPO-HANDOFF-01). Ogni riferimento si risolve nel PROPRIO repository:
    quello della catena (`checkpoint_repo` == STATE.repository) o uno passato con --repo-path NOME=PERCORSO; mai nel
    repository della catena per ripiego. Ritorna (problemi di integrita', problemi di riferimento, info)."""
    label = f"handoff[{i}]"
    problems, refp = [], []
    info = {"index": i, "status": None, "source_reachable": False, "handoff_proven": False, "verified": False}
    if not isinstance(h, dict):
        return [f"{label}: non un oggetto"], refp, info
    info["status"] = h.get("status")
    for f in HANDOFF_REQUIRED_TEXT:
        if not isinstance(h.get(f), str) or not h[f].strip():
            problems.append(f"{label}: campo {f} mancante o vuoto")
    if chain_repo and h.get("checkpoint_repo") not in (None, chain_repo):
        problems.append(f"{label}: checkpoint_repo {h.get('checkpoint_repo')!r} != STATE.repository {chain_repo!r}")
    published = h.get("source_published", True)
    if published not in (True, False):
        problems.append(f"{label}: source_published deve essere true/false")
    if published is not False and "source_sha" not in h:
        problems.append(f"{label}: source_sha obbligatorio se la sorgente e' pubblicata (altrimenti source_published: false)")
    if not re.fullmatch(r"[0-9a-f]{64}", str(h.get("patch_sha256", ""))):
        problems.append(f"{label}: patch_sha256 deve essere un sha256 esadecimale")
    if h.get("status") not in HANDOFF_STATUSES:
        problems.append(f"{label}: status non ammesso: {h.get('status')!r}")
    for f in HANDOFF_SHA_FIELDS:
        if f in h and h[f] is not None and not _SHA40.fullmatch(str(h[f])):
            problems.append(f"{label}: {f} deve essere uno SHA completo")
    if problems:
        return problems, refp, info
    paths = repo_paths or {}

    def repo_of(name, what):
        if name == h["checkpoint_repo"] and (chain_repo is None or name == chain_repo):
            return repo
        if name in paths:
            if not _repo_identity_ok(paths[name], name):
                refp.append(f"{label}: --repo-path {name}: l'origin del percorso non e' {name!r}; la prova di "
                            "pubblicazione vale solo sul remoto del repository dichiarato (rilievo Sol #615)")
                return None
            return paths[name]
        refp.append(f"{label}: repository {name!r} non disponibile per {what} (--repo-path {name}=PERCORSO); "
                    "mai risolto nel repository della catena")
        return None

    def rem(r):                       # il remote che conta: quello di `save` per la catena, `origin` per --repo-path
        return remote if r is repo else "origin"

    def resolved(r, f, what):
        if r is None:
            return None
        sha = resolve_commit(r, h[f])
        if not sha:
            refp.append(f"{label}: {f} irrisolto in {what}: {h[f]}")
        return sha

    if published is not False:
        src_repo = repo_of(h["source_repo"], "source_sha")
        src_full = resolve_commit(src_repo, h["source_sha"]) if src_repo is not None else None
        if src_full and src_repo is not repo and not _published_anywhere(src_repo, src_full, rem(src_repo)):
            # repository esterno (--repo-path): un commit solo nel clone locale non e' pubblicato (rilievo Sol #615)
            refp.append(f"{label}: source_sha non raggiungibile da un ref pubblicato di {h['source_repo']!r}")
        elif src_full:
            info["source_reachable"] = True
        elif src_repo is not None:
            refp.append(f"{label}: source_sha irrisolto: {h['source_sha']} (branch sorgente cancellato?)")
    patch = None
    art = h.get("artifact")
    if art is not None:
        if not isinstance(art, str) or art not in files:
            problems.append(f"{label}: artefatto patch dichiarato assente: {art!r}")
        else:
            patch = blob(ckpt_repo or repo, commit, files[art])   # il commit del checkpoint vive nel suo repository
            if hashlib.sha256(patch).hexdigest() != h["patch_sha256"]:
                problems.append(f"{label}: sha256 della patch != patch_sha256")
                patch = None
    status = h["status"]
    if status in ("DELIVERED", "INTEGRATED_NOT_PROVEN"):
        return problems, refp, info                 # dichiarazioni: nessuna prova ricalcolata, nessuna inferenza
    has_handoff = any(h.get(f) for f in ("handoff_branch", "handoff_sha", "handoff_tree"))
    claim = f"{status} non provato"
    if status == "DURABLE" or has_handoff:
        # la prova e' il commit di handoff pubblicato nel target, mai la sorgente (eventualmente non pubblicata)
        missing = [f for f in ("handoff_branch", "handoff_sha", "handoff_tree", "base_sha") if not h.get(f)]
        if missing:
            problems.append(f"{label}: {claim}: mancano {', '.join(missing)}")
            return problems, refp, info
        if not _valid_branch_name(repo, h["handoff_branch"]):
            problems.append(f"{label}: handoff_branch non e' un nome di branch valido: {h['handoff_branch']!r} "
                            "(nessuna sintassi di revisione: reflog, ~, ^, @{...})")
            return problems, refp, info
        tgt = repo_of(h["target_repo"], "handoff_sha/base_sha")
        hsha, base = resolved(tgt, "handoff_sha", "target_repo"), resolved(tgt, "base_sha", "target_repo")
        if hsha and not _published_on(tgt, hsha, h["handoff_branch"], rem(tgt)):
            refp.append(f"{label}: handoff_sha non pubblicato sul branch {h['handoff_branch']!r} del target")
        if base and not _published_on(tgt, base, h["handoff_branch"], rem(tgt)):
            refp.append(f"{label}: base_sha non raggiungibile dal branch {h['handoff_branch']!r} del target")
        if not (hsha and base):
            return problems, refp, info
        why = None
        htree = tree_of(tgt, hsha)
        if htree != h["handoff_tree"]:
            why = "tree(handoff_sha) != handoff_tree"
        elif patch is None and art is not None:
            why = "patch non inclusa o non integra nel checkpoint"
        elif patch is None:
            # nessun artefatto (es. catena in un repository dove il codice del target e' vietato): la prova e' il
            # digest canonico ricalcolato dai due commit pubblicati nel target, legato a patch_sha256
            if canonical_diff_sha256(tgt, base, hsha) != h["patch_sha256"]:
                why = ("digest canonico (git diff --binary --full-index base_sha handoff_sha) != patch_sha256; "
                       "patch non inclusa nel checkpoint")
        else:
            applied = apply_patch_tree(tgt, base, patch)
            if applied is None:
                why = "la patch non si applica su base_sha"
            elif applied != htree:
                why = "tree(patch su base_sha) != handoff_tree"
        if why:
            problems.append(f"{label}: {claim}: {why}; dichiarare DELIVERED o INTEGRATED_NOT_PROVEN")
            return problems, refp, info
        if any(p.startswith(f"{label}:") for p in refp):
            return problems, refp, info
        info["handoff_proven"] = True
        if status == "DURABLE":
            return problems, refp, info
        integ = h.get("integration_sha")
        if not integ:
            problems.append(f"{label}: {claim}: integration_sha assente")
            return problems, refp, info
        isha = resolved(tgt, "integration_sha", "target_repo")
        if not isha:
            return problems, refp, info
        if not _published_on(tgt, isha, "main", rem(tgt)):
            refp.append(f"{label}: integration_sha non pubblicato su main del target (rilievo Sol #615)")
            return problems, refp, info
        itree = tree_of(tgt, isha)
        if h.get("integration_tree") and h["integration_tree"] != itree:
            problems.append(f"{label}: {claim}: integration_tree != tree(integration_sha)")
        elif itree != h["handoff_tree"]:
            problems.append(f"{label}: {claim}: tree(integration_sha) != handoff_tree; dichiarare INTEGRATED_NOT_PROVEN")
        else:
            info["verified"] = True
        return problems, refp, info
    # INTEGRATED_VERIFIED senza commit di handoff: prova ricalcolata patch su base_sha == tree(integration_sha)
    if patch is None:
        problems.append(f"{label}: {claim}: patch non inclusa o non integra nel checkpoint; dichiarare INTEGRATED_NOT_PROVEN")
        return problems, refp, info
    if not h.get("base_sha") or not h.get("integration_sha"):
        problems.append(f"{label}: {claim}: base_sha o integration_sha assente")
        return problems, refp, info
    tgt = repo_of(h["target_repo"], "base_sha/integration_sha")
    base, isha = resolved(tgt, "base_sha", "target_repo"), resolved(tgt, "integration_sha", "target_repo")
    if not (base and isha):
        return problems, refp, info
    if tgt is not repo and not _published_anywhere(tgt, base, rem(tgt)):
        refp.append(f"{label}: base_sha non raggiungibile da un ref pubblicato del target (rilievo Sol #615)")
        return problems, refp, info
    if not _published_on(tgt, isha, "main", rem(tgt)):
        refp.append(f"{label}: integration_sha non pubblicato su main del target (rilievo Sol #615)")
        return problems, refp, info
    itree = tree_of(tgt, isha)
    applied = apply_patch_tree(tgt, base, patch)
    if h.get("integration_tree") and h["integration_tree"] != itree:
        problems.append(f"{label}: {claim}: integration_tree != tree(integration_sha)")
    elif applied is None:
        problems.append(f"{label}: {claim}: la patch non si applica su base_sha")
    elif applied != itree:
        problems.append(f"{label}: {claim}: tree(patch su base_sha) != tree(integration_sha); "
                        "dichiarare INTEGRATED_NOT_PROVEN")
    else:
        info["verified"] = True
    return problems, refp, info


def manifest_intact(repo, commit, d=DEFAULT_DIR):
    """True se il checkpoint al commit ha STATE.json e un manifest che elenca esattamente i suoi file con gli hash
    giusti. Usato per accettare ancore dai checkpoint antenati (non sostituisce `verify`)."""
    try:
        files = tree_files(repo, commit, d)
        if MANIFEST not in files or "STATE.json" not in files:
            return False
        manifest = parse_manifest(blob(repo, commit, files[MANIFEST]).decode(errors="replace"))
        if set(manifest) != set(files) - {MANIFEST}:
            return False
        return all(hashlib.sha256(blob(repo, commit, files[f])).hexdigest() == manifest[f] for f in manifest)
    except (CheckpointError, ValueError, OSError):
        return False


def verify(repo, ref, d=DEFAULT_DIR, code_repo=None, repo_paths=None, remote="origin"):
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
    code = state.get("code") if isinstance(state.get("code"), dict) else {}
    code_sha = code.get("sha", "")
    lookup = code_repo or repo
    ref_problems, keep_shas, code_ref = [], [], {"sha": code_sha, "via": None, "effective_sha": None}
    # tree registrati per uno SHA (code.tree scritto dal save quando l'oggetto c'era; tree del code_patch)
    anchors = {}
    if code.get("tree"):
        anchors.setdefault(str(code_sha), []).append(str(code["tree"]))
    pm = code.get("patch") if isinstance(code.get("patch"), dict) else {}
    if pm.get("head") and pm.get("tree"):
        anchors.setdefault(str(pm["head"]), []).append(str(pm["tree"]))

    history = {}
    history_loaded = []

    def _history():
        """code.tree / code.patch.tree registrati dai checkpoint PRECEDENTI della catena (first-parent, max 200)."""
        if not history_loaded:
            history_loaded.append(True)
            for c in out(repo, "rev-list", "--first-parent", "--max-count=200", f"{commit}~1").splitlines() \
                    if git(repo, "rev-parse", "-q", "--verify", f"{commit}~1", check=False).returncode == 0 else []:
                if not manifest_intact(repo, c, d):
                    continue                        # antenato non integro: nessuna ancora (rilievo Sol #615)
                try:
                    st = tree_json(repo, c, d, "STATE.json")
                except (ValueError, CheckpointError):
                    continue
                cd = st.get("code") if isinstance(st, dict) and isinstance(st.get("code"), dict) else {}
                if cd.get("sha") and cd.get("tree"):
                    history.setdefault(str(cd["sha"]), []).append(str(cd["tree"]))
                pmh = cd.get("patch") if isinstance(cd.get("patch"), dict) else {}
                if pmh.get("head") and pmh.get("tree"):
                    history.setdefault(str(pmh["head"]), []).append(str(pmh["tree"]))
        return history

    def _anchors_for(sha, integ):
        orig = integ.get("original_sha") if isinstance(integ, dict) else None
        if isinstance(integ, dict) and not resolve_commit(lookup, str(sha)):
            # originale irraggiungibile: il code.tree dello STATE corrente e' autodichiarato quanto original_tree
            # (rilievo Sol #615); vale solo l'ancora di un checkpoint PRECEDENTE, verificata quando l'oggetto c'era
            source = _history()
        else:
            source = anchors
        return [t for k, ts in source.items() for t in ts
                if sha and (k.startswith(str(sha)) or (orig and k == orig))]

    if code_sha:
        rp, ks, code_ref = check_code_reference(lookup, "code.sha", code_sha, code.get("integration"),
                                                _anchors_for(code_sha, code.get("integration")),
                                                f"SHA del codice inesistente nel repository: {code_sha}")
        ref_problems += rp
        keep_shas += ks
    for t in _tests_list(state):
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
            msg = f"test {t.get('id')!r}: test sha unresolved: {tsha}"
            if not well_formed:
                problems.append(msg)
            elif not declared_unpublished:
                rp, ks, _ = check_code_reference(lookup, f"test {t.get('id')!r}", tsha, t.get("integration"),
                                                 _anchors_for(tsha, t.get("integration")), msg)
                ref_problems += rp
                keep_shas += ks
        art = t.get("artifact") if isinstance(t, dict) else None
        for a in ([art] if isinstance(art, str) else (art or [])):
            if a not in files:
                problems.append(f"test {t.get('id')!r}: artefatto dichiarato assente: {a}")
    # handoffs (consegna di patch fra agenti): source_sha risolvibile, digest della patch, prova ricalcolata
    handoff_report = []
    hs = state.get("handoffs")
    if hs is not None:
        if not isinstance(hs, list):
            problems.append("handoffs non e' una lista")
        else:
            for i, h in enumerate(hs):
                hp, hr, hinfo = check_handoff(lookup, commit, files, i, h, state.get("repository"), repo_paths,
                                              ckpt_repo=repo, remote=remote)
                problems += hp
                ref_problems += hr
                handoff_report.append(hinfo)
            for h, f in _same_repo_handoff_shas(state):
                full = resolve_commit(lookup, str(h[f]))
                if full:
                    keep_shas.append(full)          # KEEP_REF dichiarato solo se il suo ref keep viene pubblicato
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
    patch_meta = _code_dict(state).get("patch")
    if isinstance(patch_meta, dict):
        for f in patch_meta.get("files") or []:
            if f not in files:
                problems.append(f"code.patch: file dichiarato assente: {f}")
    problems += ref_problems
    if problems:
        # ref_problems: riferimenti di codice non provabili (SHA irraggiungibile, transizione non provata); il resto
        # e' integrita'. `bootstrap` distingue NEEDS_RECONCILIATION da LATEST_CHECKPOINT_CORRUPT su questa base.
        raise CheckpointError(2, "CHECKPOINT_INVALID", commit=commit, problems=problems, reference_problems=ref_problems)
    lease = derived.get(LEASE) or {}
    return {"commit": commit, "seq": state["seq"], "code_sha": code_sha, "files": len(files),
            "code_reference": code_ref, "keep_shas": sorted(set(keep_shas)), "handoffs": handoff_report,
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
    skipped_symlinks = {}
    if tracked:
        with open(os.path.join(root, "tracked.patch"), "wb") as fh:
            fh.write(tracked)
        files.append(f"{CODE_PATCH_DIR}/tracked.patch")
    for rel in untracked:
        src = os.path.join(code_repo, rel)
        if os.path.islink(src):
            # Rilievo Codex r4312719746 (#512): seguire il link copierebbe nel checkpoint (poi pubblicato) il
            # contenuto di un file arbitrario fuori dal repo. Mai dereferenziare: si registra solo il testo del
            # link (os.readlink, nessuna lettura del bersaglio) in BASELINE.json `skipped_symlinks`.
            skipped_symlinks[rel] = os.readlink(src)
            continue
        if not os.path.isfile(src):
            continue
        dst = os.path.join(root, "untracked", rel)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copyfile(src, dst)
        files.append(f"{CODE_PATCH_DIR}/untracked/{rel}")
    meta = {"head": head, "tree": tree, "captured_at": iso(utcnow()), "modified": modified, "untracked": untracked,
            "files": files, "skipped_symlinks": skipped_symlinks}
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
    links = meta.get("skipped_symlinks") or {}
    for rel in meta.get("untracked") or []:
        if rel in links:
            # symlink registrato solo come testo del link (mai il contenuto del bersaglio): si ricrea il link
            dst = os.path.join(code_repo, rel)
            os.makedirs(os.path.dirname(dst) or ".", exist_ok=True)
            os.symlink(str(links[rel]), dst)
            continue
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


def fetch_keep_refs(repo):
    git(repo, "fetch", "-q", "origin", KEEP_REFSPEC, check=False)


def _same_repo_handoff_shas(state):
    """(record, campo) degli SHA di handoff che stanno nel repository della catena: la sorgente pubblicata e il commit
    di handoff. Gli SHA di altri repository non si classificano sul remoto della catena (si verificano nel loro)."""
    chain = state.get("repository")
    found = []
    for h in (state["handoffs"] if isinstance(state.get("handoffs"), list) else []):
        if not isinstance(h, dict):
            continue
        if h.get("source_sha") and h.get("source_published", True) is not False and h.get("source_repo") == chain:
            found.append((h, "source_sha"))
        if h.get("target_repo") == chain:
            if h.get("handoff_sha"):
                found.append((h, "handoff_sha"))
            # rilievo Codex #615: base e integrazione sono parte della prova ricalcolata (DURABLE / INTEGRATED_*):
            # devono restare risolvibili quanto la sorgente
            if h.get("status") in ("DURABLE", "INTEGRATED_VERIFIED"):
                for f in ("base_sha", "integration_sha"):
                    if h.get(f):
                        found.append((h, f))
    return found


def _cited_shas_and_prs(state):
    """SHA citati da risolvere (code.sha, tests_executed[].sha PASS/FAIL non dichiarati unpublished) e numeri di PR
    dichiarati (`code.pr`, `tests_executed[].pr`, interi positivi)."""
    shas, prs = [], set()
    code = state.get("code") if isinstance(state.get("code"), dict) else {}
    if code.get("sha"):
        shas.append(str(code["sha"]))
    if isinstance(code.get("pr"), int) and code["pr"] > 0:
        prs.add(code["pr"])
    tests = [t for t in state.get("tests_executed") if isinstance(t, dict)] \
        if isinstance(state.get("tests_executed"), list) else []      # STATE malformato: mai un TypeError (Sol #615)
    for e in [code] + tests:
        integ = e.get("integration")
        if isinstance(integ, dict) and integ.get("integration_sha"):
            shas.append(str(integ["integration_sha"]))     # anche l'integrazione puo' vivere solo in refs/pull
    for h, f in _same_repo_handoff_shas(state):
        shas.append(str(h[f]))
    for t in tests:
        if isinstance(t.get("pr"), int) and t["pr"] > 0:
            prs.add(t["pr"])
        if t.get("status") in ("PASS", "FAIL") and t.get("unpublished_tree") is not True and t.get("sha"):
            shas.append(str(t["sha"]))
    return shas, sorted(prs)


def fetch_pull_heads(repo, d, ref="HEAD"):
    """Fallback per gli SHA di PR integrate con squash e branch cancellato: GitHub conserva `refs/pull/<N>/head`.
    Solo se, dopo i ref keep, uno SHA citato dal checkpoint REF resta irrisolto: si scaricano i `refs/pull/<N>/head`
    dichiarati nello STATE (`code.pr` / `tests_executed[].pr`) o, se nessuno e' dichiarato, tutti i
    `refs/pull/*/head`. Nessuna configurazione per agente o catena. Ritorna cosa e' stato scaricato (None = niente).
    Uno SHA ancora irrisolto dopo il fallback resta un problema di verify: mai READY."""
    try:
        state = tree_json(repo, ref, d, "STATE.json")
    except (ValueError, CheckpointError):
        return None
    if not isinstance(state, dict):
        return None
    shas, prs = _cited_shas_and_prs(state)
    if all(resolve_commit(repo, s) for s in shas if re.fullmatch(r"[0-9a-f]{7,40}", s)):
        return None
    specs = ([f"+refs/pull/{n}/head:refs/remotes/origin/pull/{n}" for n in prs] if prs
             else ["+refs/pull/*/head:refs/remotes/origin/pull/*"])
    for spec in specs:                      # uno per volta: una PR inesistente non blocca le altre (rilievo Codex #615)
        git(repo, "fetch", "-q", "origin", spec, check=False)
    return {"declared_prs": prs, "refspecs": specs}


def keep_ref_status(repo, shas):
    """Quali SHA hanno il ref keep (gia' scaricato in `repo`) che punta esattamente a loro."""
    present, missing = [], []
    for sha in shas:
        p = git(repo, "rev-parse", "-q", "--verify", f"{KEEP_REF_PREFIX}{sha}", check=False)
        (present if p.returncode == 0 and p.stdout.decode().strip() == sha else missing).append(sha)
    return {"present": present, "missing": missing}


def keep_refspecs(shas):
    return [f"{sha}:{KEEP_REF_PREFIX}{sha}" for sha in shas]


def cited_references(repo, state):
    """Riferimenti di codice citati dallo STATE che devono essere durevoli: code.sha e tests_executed[].sha PASS/FAIL
    non dichiarati `unpublished_tree`. Per ognuno: SHA originale completo (se risolvibile) e commit di integrazione
    (solo se la transizione TREE_EQUAL e' provata)."""
    cited = []
    code = state.get("code") if isinstance(state.get("code"), dict) else {}
    entries = [("code.sha", code)] if code.get("sha") else []
    for t in _tests_list(state):
        if isinstance(t, dict) and t.get("status") in ("PASS", "FAIL") and t.get("sha") \
                and not (t.get("unpublished_tree") is True and str(t.get("sha_note", "")).strip()):
            entries.append((f"test {t.get('id')!r}", t))
    for label, e in entries:
        _p, _k, info = check_code_reference(repo, label, str(e["sha"]), e.get("integration"))
        cited.append({"label": label, "sha": str(e["sha"]), "original": resolve_commit(repo, str(e["sha"])),
                      "integration": info.get("integration_sha")})
        integ = e.get("integration")
        if isinstance(integ, dict) and integ.get("equivalence") == "TREE_EQUAL" and integ.get("integration_sha"):
            # la verifica richiede il commit di integrazione anche con l'originale raggiungibile (rilievo Codex #615):
            # deve essere durevole di per se'
            isha = str(integ["integration_sha"])
            cited.append({"label": f"{label}.integration", "sha": isha, "original": resolve_commit(repo, isha),
                          "integration": None})
    # handoffs: la sorgente della patch deve essere durevole come code.sha (mai inferita dall'integrazione)
    hs = state.get("handoffs") if isinstance(state.get("handoffs"), list) else []
    for h, f in _same_repo_handoff_shas(state):
        i = hs.index(h)
        cited.append({"label": f"handoff[{i}]" if f == "source_sha" else f"handoff[{i}].{f}",
                      "sha": str(h[f]), "original": resolve_commit(repo, str(h[f])), "integration": None})
    return cited


def _fetch_into_ns(repo, remote, specs, check):
    p = git(repo, "fetch", "-q", "--no-tags", remote, *specs, check=False)
    if check and p.returncode != 0:
        raise CheckpointError(5, "REMOTE_UNREACHABLE", stderr=p.stderr.decode(errors="replace")[-300:])


def durability_tips(repo, remote, prs=(), all_pulls=False):
    """Punte REMOTE attuali (scaricate in un namespace solo locale, ripulito prima): refs/heads/* e i refs/pull/<N>/head
    dichiarati (o tutti, se all_pulls). Ritorna {"heads/<b>": sha, "pull/<n>": sha}."""
    for name in out(repo, "for-each-ref", "--format=%(refname)", DURABILITY_NS).splitlines():
        git(repo, "update-ref", "-d", name)
    _fetch_into_ns(repo, remote, [f"+refs/heads/*:{DURABILITY_NS}heads/*"], check=True)
    if all_pulls:
        _fetch_into_ns(repo, remote, [f"+refs/pull/*/head:{DURABILITY_NS}pull/*"], check=False)
    for n in prs:                                   # uno per volta: una PR inesistente non fa fallire le altre
        _fetch_into_ns(repo, remote, [f"+refs/pull/{n}/head:{DURABILITY_NS}pull/{n}"], check=False)
    tips = {}
    for line in out(repo, "for-each-ref", "--format=%(objectname) %(refname)", DURABILITY_NS).splitlines():
        sha, name = line.split(" ", 1)
        tips[name[len(DURABILITY_NS):]] = sha
    return tips


def _is_ancestor(repo, sha, tip):
    return git(repo, "merge-base", "--is-ancestor", sha, tip, check=False).returncode == 0


def _classify(repo, c, tips, durable_branches, keep):
    o, i = c["original"], c["integration"]
    if o and keep:
        return "KEEP_REF"
    for b in durable_branches:
        if o and f"heads/{b}" in tips and _is_ancestor(repo, o, tips[f"heads/{b}"]):
            return f"BRANCH:{b}"
    for name, tip in sorted(tips.items()):
        if o and name.startswith("pull/") and _is_ancestor(repo, o, tip):
            return f"PULL:{name.split('/')[1]}"
    for b in durable_branches:
        if i and f"heads/{b}" in tips and _is_ancestor(repo, i, tips[f"heads/{b}"]):
            return f"INTEGRATION:{b}"
    for name, tip in sorted(tips.items()):
        if o and name.startswith("heads/") and _is_ancestor(repo, o, tip):
            return f"BRANCH_ONLY:{name[len('heads/'):]}"
    return None


def check_durability(repo, remote, state, durable_branches=DEFAULT_DURABLE_BRANCHES, keep=False, waiver=()):
    """Ogni riferimento citato deve restare risolvibile dopo uno squash merge con cancellazione del branch:
    BRANCH:<durevole> | PULL:<n> | INTEGRATION:<durevole> | KEEP_REF (solo con --keep-refs). BRANCH_ONLY (solo un
    branch di lavoro, che puo' essere cancellato) e' ammesso SOLO per gli SHA dichiarati in
    `code.durability_waiver.branch_only`. Ritorna (righe, non_durevoli, branch_only_non_dichiarati)."""
    _shas, prs = _cited_shas_and_prs(state)
    tips = durability_tips(repo, remote, prs)        # prima il fetch: un oggetto gia' pubblicato diventa risolvibile
    cited = cited_references(repo, state)
    rows = [dict(c, via=_classify(repo, c, tips, durable_branches, keep)) for c in cited]
    if not prs and any(r["via"] is None or r["via"].startswith("BRANCH_ONLY") for r in rows):
        tips = durability_tips(repo, remote, (), all_pulls=True)          # PR non dichiarata: scoperta
        cited = cited_references(repo, state)                             # e risoluzione ripetuta dopo il fetch
        rows = [dict(c, via=_classify(repo, c, tips, durable_branches, keep)) for c in cited]
    missing = [r for r in rows if r["via"] is None]
    branch_only = [r for r in rows if r["via"] and r["via"].startswith("BRANCH_ONLY")
                   and r["original"] not in set(waiver)]
    return rows, missing, branch_only


def _waiver(state):
    w = (_code_dict(state).get("durability_waiver") or {}) if isinstance(state.get("code"), dict) else {}
    return [str(x) for x in (w.get("branch_only") or [])] if isinstance(w, dict) else []


def _not_durable(code, reason, missing, branch_only):
    return CheckpointError(code, reason, not_durable=[{"label": r["label"], "sha": r["sha"], "via": r["via"]}
                                                       for r in missing + branch_only],
                           action="lo SHA citato sparirebbe con il branch: dichiarare `pr` (code.pr / "
                                  "tests_executed[].pr) della PR che lo contiene, oppure `unpublished_tree` + "
                                  "`sha_note` per un test su albero scratch, oppure integrare su main con "
                                  "`integration` TREE_EQUAL, oppure --allow-branch-only (dichiarato nello STATE)")


def readback(remote_url, branch, d, code_branch=None, expect_commit=None, keep_refs=False,
             durable_branches=DEFAULT_DURABLE_BRANCHES, repo_paths=None):
    """Clone pulito dal remoto e verifica completa li': l'unica prova di durabilita'. Riverifica che ogni SHA citato sia
    durevole sul remoto (branch durevole, refs/pull/<N>/head, integrazione TREE_EQUAL; con keep_refs, il proprio ref
    keep puntato allo SHA stesso)."""
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
        fetch_keep_refs(tmp)
        fetch_pull_heads(tmp, d)
        try:
            report = verify(tmp, "HEAD", d, repo_paths=repo_paths)
        except CheckpointError as exc:
            raise CheckpointError(4, "READBACK_VERIFY_FAILED", inner=exc.reason, **exc.detail)
        if expect_commit and report["commit"] != expect_commit:
            raise CheckpointError(4, "READBACK_SHA_MISMATCH", remote=report["commit"])
        if keep_refs:
            ks = keep_ref_status(tmp, report["keep_shas"])
            if ks["missing"]:
                raise CheckpointError(4, "READBACK_KEEP_REF_MISSING", missing=ks["missing"])
        state = json.loads(blob(tmp, report["commit"], f"{d}/STATE.json"))
        rows, missing, branch_only = check_durability(tmp, "origin", state, durable_branches, keep_refs, _waiver(state))
        if missing or branch_only:
            raise _not_durable(4, "READBACK_CODE_SHA_NOT_DURABLE", missing, branch_only)
        report["durability"] = [{"label": r["label"], "sha": r["sha"], "via": r["via"]} for r in rows]
        return report
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def save(repo, branch, remote, expected, message, d=DEFAULT_DIR, code_branch=None, agent=None,
         ttl_min=DEFAULT_LEASE_MIN, release=False, code_repo=None, now=None, keep_refs=False, allow_branch_only=False,
         durable_branches=DEFAULT_DURABLE_BRANCHES, repo_paths=None):
    """Pubblicazione verificata. Ripresa idempotente: rieseguire save dopo un crash in qualunque punto e' sicuro.
    expected = SHA remoto da cui l'agente e' partito (controllo ottimistico); mai force-push, mai merge di stato.
    agent = identita' dello scrittore: il lease si legge dal checkpoint remoto atteso; tenuto da un altro agente e
    non scaduto => exit 6, nessuna scrittura.
    Durabilita' (pfckpt_squash_durable_refs): ogni SHA citato deve essere raggiungibile sul remoto da un branch
    durevole, da refs/pull/<N>/head o da un'integrazione TREE_EQUAL; altrimenti CODE_SHA_NOT_DURABLE (exit 2) e nulla
    e' pubblicato. keep_refs=True pubblica anche refs/agent-state/keep/<sha> (opt-in). allow_branch_only=True accetta
    uno SHA presente solo su un branch di lavoro, dichiarandolo nello STATE (code.durability_waiver)."""
    durable_branches = tuple(durable_branches or DEFAULT_DURABLE_BRANCHES)
    url = out(repo, "remote", "get-url", remote)
    # prove di pubblicazione su ref FRESCHI (rilievo Sol #615): branch cancellati sul remoto potati prima di verificare
    refresh_published_refs(repo, remote)
    for path in dict.fromkeys((repo_paths or {}).values()):
        refresh_published_refs(path, "origin")
    current = remote_head(repo, remote, branch)
    head = out(repo, "rev-parse", "HEAD")
    parent = out(repo, "rev-parse", "HEAD^") if git(repo, "rev-parse", "--verify", "HEAD^", check=False).returncode == 0 else None
    if current == head and head != expected and parent == expected:
        # crash dopo il push (prima del readback): il remoto ha gia' il NOSTRO checkpoint figlio di expected.
        # I ref keep potrebbero mancare (crash fra i due, strumento precedente): si pubblicano, senza force.
        local = verify(repo, "HEAD", d, repo_paths=repo_paths, remote=remote)
        if keep_refs and local["keep_shas"]:
            p = git(repo, "push", "-q", "--atomic", remote, *keep_refspecs(local["keep_shas"]), check=False)
            if p.returncode != 0:
                raise CheckpointError(3, "KEEP_REF_PUSH_REJECTED", stderr=p.stderr.decode(errors="replace")[-300:])
        report = readback(url, branch, d, code_branch, expect_commit=head, keep_refs=keep_refs,
                          durable_branches=durable_branches, repo_paths=repo_paths)
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
        # durabilita' PRIMA di scrivere/committare: uno SHA che sparirebbe con il branch non entra nel checkpoint
        _rows, missing, branch_only = check_durability(repo, remote, st, durable_branches, keep_refs, _waiver(st))
        # tree ricalcolato dall'oggetto DOPO il fetch dei ref durevoli (rilievi Sol #615): mai un tree ereditato da un
        # code.sha precedente; diventa l'ancora per un futuro `integration.original_tree` una volta pubblicato.
        code_full = resolve_commit(repo, (st.get("code") or {}).get("sha"))
        if code_full:
            st["code"]["tree"] = tree_of(repo, code_full)
        elif isinstance(st.get("code"), dict):
            st["code"].pop("tree", None)          # non verificabile ora: un tree non ricalcolato non e' un'ancora
        if branch_only and allow_branch_only and not missing:
            w = st["code"].setdefault("durability_waiver", {})
            w["branch_only"] = sorted(set(w.get("branch_only") or []) | {r["original"] for r in branch_only})
            w["declared_by"], w["reason"] = agent, "--allow-branch-only: durevole solo finche' il branch esiste"
            branch_only = []
        if missing or branch_only:
            raise _not_durable(2, "CODE_SHA_NOT_DURABLE", missing, branch_only)
        with open(state_path, "w") as fh:
            json.dump(st, fh, indent=1, ensure_ascii=False)
        write_derived(repo, d, st, new_lease(agent, st["seq"], ttl_min, now, released=release))
        write_manifest(repo, d)
        git(repo, "add", "-A", "-f", "--", d)          # -f: un .gitignore non puo' escludere artefatti in silenzio
        git(repo, "commit", "-q", "-m", message)
    local = verify(repo, "HEAD", d, repo_paths=repo_paths, remote=remote)   # verifica del TREE committato prima di pubblicare
    untracked = [l for l in out(repo, "status", "--porcelain", "--ignored", "--", d).splitlines()
                 if not l.endswith(".pyc") and "__pycache__" not in l]
    if untracked:
        raise CheckpointError(2, "LOCAL_ONLY_FILES", files=untracked[:20])
    if resumed == "after_commit":                   # commit gia' fatto (anche da un altro strumento): stesso controllo
        cst = json.loads(blob(repo, "HEAD", f"{d}/STATE.json"))
        _rows, missing, branch_only = check_durability(repo, remote, cst, durable_branches, keep_refs, _waiver(cst))
        if missing or branch_only:
            raise _not_durable(2, "CODE_SHA_NOT_DURABLE", missing, branch_only)
    # senza force. Default: solo il branch di stato. Con keep_refs: push ATOMICO di checkpoint + ref keep; un ref keep
    # esistente su un altro commit (non-FF) fa rifiutare tutto, branch compreso
    extra = keep_refspecs(local["keep_shas"]) if keep_refs else []
    p = git(repo, "push", "-q", *(["--atomic"] if extra else []), remote, f"HEAD:refs/heads/{branch}", *extra,
            check=False)
    if p.returncode != 0:
        now_remote = remote_head(repo, remote, branch)
        if now_remote == out(repo, "rev-parse", "HEAD"):
            resumed = "push_reported_error_but_landed"
        else:
            raise CheckpointError(3, "PUSH_REJECTED", remote=now_remote, stderr=p.stderr.decode(errors="replace")[-300:])
    report = readback(url, branch, d, code_branch, expect_commit=out(repo, "rev-parse", "HEAD"), keep_refs=keep_refs,
                      durable_branches=durable_branches, repo_paths=repo_paths)
    return {"status": "DURABLE", "resumed": resumed, **report}


# ------------------------------------------------------------------ ripresa da zero, recover, divergenza
def last_valid_checkpoint(repo, d, start="HEAD", limit=200, repo_paths=None):
    for c in out(repo, "rev-list", "--first-parent", start).splitlines()[:limit]:
        try:
            fetch_pull_heads(repo, d, c)        # i refs/pull/<N>/head citati da QUESTO checkpoint, non solo da HEAD
        except Exception:                       # rilievo Grok #615: un fetch fallito non interrompe il recupero
            pass
        try:
            verify(repo, c, d, repo_paths=repo_paths)
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


def bootstrap(url, branch, dest, d=DEFAULT_DIR, code_branch=None, agent=None, repo_paths=None):
    p = subprocess.run(["git", "clone", "-q", "--no-local", "--branch", branch, url, dest], capture_output=True)
    if p.returncode != 0:
        raise CheckpointError(5, "CLONE_FAILED", stderr=p.stderr.decode(errors="replace")[-300:])
    if code_branch:
        git(dest, "fetch", "-q", "origin", f"{code_branch}:refs/remotes/origin/{code_branch}", check=False)
    fetch_keep_refs(dest)                           # gli SHA citati restano risolvibili dopo squash + delete branch
    for path in dict.fromkeys((repo_paths or {}).values()):
        refresh_published_refs(path, "origin")      # --repo-path: prove di pubblicazione su ref freschi
    pull_fetch = fetch_pull_heads(dest, d)          # fallback GitHub refs/pull/<N>/head, solo se serve
    try:
        report = verify(dest, "HEAD", d, repo_paths=repo_paths)
    except CheckpointError as exc:
        last_valid = last_valid_checkpoint(dest, d, "HEAD~1", repo_paths=repo_paths) if out(dest, "rev-list", "--count", "HEAD") != "1" else None
        problems = exc.detail.get("problems") or []
        refp = exc.detail.get("reference_problems") or []
        if problems and set(problems) <= set(refp):
            # integro ma con riferimenti di codice non provabili: mai READY, mai rollback; nuovo checkpoint con
            # `integration` TREE_EQUAL (pfckpt.py integrate) o ripubblicazione dell'oggetto originale
            raise CheckpointError(2, "NEEDS_RECONCILIATION", inner=exc.reason, problems=problems,
                                  last_valid_checkpoint=last_valid,
                                  action="nuovo checkpoint con code.integration TREE_EQUAL o SHA originale ripubblicato; "
                                         "nessun rollback automatico")
        raise CheckpointError(2, "LATEST_CHECKPOINT_CORRUPT", inner=exc.reason, problems=exc.detail.get("problems"),
                              last_valid_checkpoint=last_valid, action="nessun rollback automatico: decidere esplicitamente")
    state = json.loads(blob(dest, report["commit"], f"{d}/STATE.json"))
    lease = tree_json(dest, report["commit"], d, LEASE)
    status, _ = lease_status(lease, agent)
    result = {"status": "READY", **report, "mandate_id": state["mandate_id"],
              "owner_authorizations": state["owner_authorizations"], "blocks": state["blocks"],
              "code_branch": _code_dict(state).get("branch"), "lease": status,
              "code_patch": bool(_code_dict(state).get("patch")),
              "keep_refs": keep_ref_status(dest, report["keep_shas"]), "pull_refs_fetched": pull_fetch,
              "handoffs": report["handoffs"]}
    if code_branch:
        result["code_divergence"] = code_divergence(dest, code_branch,
                                                    report["code_reference"].get("effective_sha") or report["code_sha"])
    if status == "HELD":
        result["status"] = "READY_READ_ONLY"
        result["action"] = f"lease tenuto da {lease.get('owner')} fino a {lease.get('expires_at')}: nessuna scrittura"
    return result


def recover(url, branch, dest, d=DEFAULT_DIR, repo_paths=None):
    """Materializza in DEST l'ultimo checkpoint VALIDO della catena, senza toccare il remoto."""
    p = subprocess.run(["git", "clone", "-q", "--no-local", "--branch", branch, url, dest], capture_output=True)
    if p.returncode != 0:
        raise CheckpointError(5, "CLONE_FAILED", stderr=p.stderr.decode(errors="replace")[-300:])
    fetch_keep_refs(dest)
    fetch_pull_heads(dest, d)
    head = out(dest, "rev-parse", "HEAD")
    valid = last_valid_checkpoint(dest, d, "HEAD", repo_paths=repo_paths)
    if valid is None:
        raise CheckpointError(2, "NO_VALID_CHECKPOINT", head=head)
    git(dest, "checkout", "-q", "--detach", valid)
    report = verify(dest, "HEAD", d, repo_paths=repo_paths)
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
    s.add_argument("--keep-refs", action="store_true",
                   help="pubblica anche refs/agent-state/keep/<sha> (opt-in; rifiutato dal proxy delle sessioni)")
    s.add_argument("--allow-branch-only", action="store_true",
                   help="accetta SHA presenti solo su un branch di lavoro, dichiarandolo nello STATE")
    s.add_argument("--durable-branch", action="append", help="branch durevole (ripetibile; default main)")
    b = sub.add_parser("bootstrap"); b.add_argument("--url", required=True); b.add_argument("--branch", required=True)
    b.add_argument("--dest", required=True); b.add_argument("--dir", default=DEFAULT_DIR); b.add_argument("--code-branch")
    b.add_argument("--agent")
    r = sub.add_parser("recover"); r.add_argument("--url", required=True); r.add_argument("--branch", required=True)
    r.add_argument("--dest", required=True); r.add_argument("--dir", default=DEFAULT_DIR)
    rc = sub.add_parser("restore-code"); rc.add_argument("--repo", default="."); rc.add_argument("--ref", default="HEAD")
    rc.add_argument("--dir", default=DEFAULT_DIR); rc.add_argument("--code-repo", required=True)
    ig = sub.add_parser("integrate"); ig.add_argument("--repo", default="."); ig.add_argument("--original", required=True)
    ig.add_argument("--integration", required=True); ig.add_argument("--original-tree")
    hv = sub.add_parser("handoff-verify"); hv.add_argument("--repo", default=".")
    hv.add_argument("--source", required=True); hv.add_argument("--patch", required=True)
    hv.add_argument("--base", required=True); hv.add_argument("--integration", required=True)
    for f in HANDOFF_TEXT_FIELDS:
        hv.add_argument("--" + f.replace("_", "-"))
    for sp in (v, s, b, r):
        sp.add_argument("--repo-path", action="append", default=[],
                        help="NOME=PERCORSO di un repository citato dagli handoff (ripetibile); mai ripiego sulla catena")
    a = ap.parse_args(argv)
    try:
        if a.cmd == "verify":
            res = {"status": "VALID", **verify(a.repo, a.ref, a.dir, a.code_repo, parse_repo_paths(a.repo_path))}
        elif a.cmd == "save":
            res = save(a.repo, a.branch, a.remote, a.expected, a.message, a.dir, a.code_branch, agent=a.agent,
                       ttl_min=a.ttl_min, release=a.release, code_repo=a.code_repo, keep_refs=a.keep_refs,
                       allow_branch_only=a.allow_branch_only, durable_branches=a.durable_branch,
                       repo_paths=parse_repo_paths(a.repo_path))
        elif a.cmd == "bootstrap":
            res = bootstrap(a.url, a.branch, a.dest, a.dir, a.code_branch, agent=a.agent,
                            repo_paths=parse_repo_paths(a.repo_path))
        elif a.cmd == "recover":
            res = recover(a.url, a.branch, a.dest, a.dir, repo_paths=parse_repo_paths(a.repo_path))
        elif a.cmd == "handoff-verify":
            with open(a.patch, "rb") as fh:
                data = fh.read()
            res = handoff_verify(a.repo, a.source, data, a.base, a.integration,
                                 **{f: getattr(a, f) for f in HANDOFF_TEXT_FIELDS})
            print(json.dumps(res, ensure_ascii=False))
            return 0 if res["status"] == "INTEGRATED_VERIFIED" else 2
        elif a.cmd == "integrate":
            res = build_integration(a.repo, a.original, a.integration, a.original_tree)
            print(json.dumps(res, ensure_ascii=False))
            return 0 if res["equivalence"] == "TREE_EQUAL" else 2
        else:
            res = restore_code_patch(a.repo, a.ref, a.dir, a.code_repo)
        print(json.dumps(res, ensure_ascii=False))
        return 0
    except CheckpointError as exc:
        print(json.dumps({"status": "ERROR", "reason": exc.reason, **exc.detail}, ensure_ascii=False, default=str))
        return exc.code


if __name__ == "__main__":
    sys.exit(main())
