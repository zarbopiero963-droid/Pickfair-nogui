#!/usr/bin/env python3
"""pfckpt — checkpoint universale e portabile per il lavoro multi-agente su Pickfair (PF-UNIVERSAL-CHECKPOINT-V1).

Fonte di verita': un branch GitHub di stato. Un checkpoint e' UN commit (atomico) che contiene la directory di stato:
STATE.json (schema versionato), MANIFEST.sha256 (hash di ogni altro file), HANDOFF.md, RUNBOOK.md e gli artefatti.
Solo Python standard library e git. Nessuna dipendenza dalla memoria della conversazione o da file locali.

Comandi:
  verify    --repo R --ref REF [--dir D]          verifica il checkpoint DENTRO il tree committato di REF
  save      --repo R --branch B --remote O --expected SHA --message M [--dir D]
                                                   pubblica in modo atomico e verificato, ripresa idempotente
  bootstrap --url URL --branch B --dest DEST [--code-branch C] [--dir D]
                                                   ripresa da zero in un ambiente pulito
Esiti: stampa una riga JSON; exit 0 = OK, 2 = checkpoint non valido, 3 = conflitto di concorrenza, 4 = readback fallito,
5 = errore git/ambiente.
"""
import argparse
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
SECRET_PATTERNS = [re.compile(p) for p in (
    r"(?i)(api[_-]?key|app[_-]?key|password|secret|session[_-]?token)\s*[:=]\s*['\"][^'\"\s]{12,}",
    r"-----BEGIN [A-Z ]*PRIVATE KEY-----", r"ghp_[A-Za-z0-9]{30,}", r"sk-[A-Za-z0-9]{30,}")]


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


# ------------------------------------------------------------------ lettura dal TREE committato (mai dal disco)
def tree_files(repo, ref, d):
    names = out(repo, "ls-tree", "-r", "--name-only", ref, "--", d).splitlines()
    prefix = d.rstrip("/") + "/"
    return {n[len(prefix):]: n for n in names if n.startswith(prefix)}


def blob(repo, ref, path):
    return git(repo, "show", f"{ref}:{path}").stdout


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
        if not f.endswith(".py"):
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
                pseq = json.loads(blob(repo, parent, pfiles["STATE.json"])).get("seq")
            except ValueError:
                pseq = None
            if isinstance(pseq, int) and state.get("seq") != pseq + 1:
                problems.append(f"seq {state.get('seq')} non monotono (precedente {pseq})")
    code_sha = (state.get("code") or {}).get("sha", "")
    lookup = code_repo or repo
    if code_sha and git(lookup, "cat-file", "-e", f"{code_sha}^{{commit}}", check=False).returncode != 0:
        problems.append(f"SHA del codice inesistente nel repository: {code_sha}")
    for t in state.get("tests_executed") or []:
        art = t.get("artifact") if isinstance(t, dict) else None
        for a in ([art] if isinstance(art, str) else (art or [])):
            if a not in files:
                problems.append(f"test {t.get('id')!r}: artefatto dichiarato assente: {a}")
    if problems:
        raise CheckpointError(2, "CHECKPOINT_INVALID", commit=commit, problems=problems)
    return {"commit": commit, "seq": state["seq"], "code_sha": code_sha, "files": len(files),
            "next_atomic_action": state["next_atomic_action"], "verdict": state["certification_verdict"]}


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
        try:
            return verify(tmp, "HEAD", d)
        except CheckpointError as exc:
            raise CheckpointError(4, "READBACK_VERIFY_FAILED", inner=exc.reason, **exc.detail)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def save(repo, branch, remote, expected, message, d=DEFAULT_DIR, code_branch=None):
    """Pubblicazione verificata. Ripresa idempotente: rieseguire save dopo un crash in qualunque punto e' sicuro.
    expected = SHA remoto da cui l'agente e' partito (controllo ottimistico); mai force-push, mai merge di stato."""
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
        write_manifest(repo, d)
        git(repo, "add", "-A", "-f", "--", d)          # -f: un .gitignore non puo' escludere artefatti in silenzio
        state_path = os.path.join(repo, d, "STATE.json")
        st = json.load(open(state_path))
        if not str(expected).startswith(str(st.get("previous_checkpoint_sha") or "")) or not st.get("previous_checkpoint_sha"):
            raise CheckpointError(2, "PREVIOUS_SHA_MISMATCH", declared=st.get("previous_checkpoint_sha"), expected=expected)
        git(repo, "commit", "-q", "-m", message)
    verify(repo, "HEAD", d)                         # verifica del TREE committato prima di pubblicare
    untracked = [l for l in out(repo, "status", "--porcelain", "--ignored", "--", d).splitlines()
                 if not l.endswith(".pyc") and "__pycache__" not in l]
    if untracked:
        raise CheckpointError(2, "LOCAL_ONLY_FILES", files=untracked[:20])
    p = git(repo, "push", "-q", remote, f"HEAD:refs/heads/{branch}", check=False)   # senza force: non-FF rifiutato
    if p.returncode != 0:
        now = remote_head(repo, remote, branch)
        if now == out(repo, "rev-parse", "HEAD"):
            resumed = "push_reported_error_but_landed"
        else:
            raise CheckpointError(3, "PUSH_REJECTED", remote=now, stderr=p.stderr.decode(errors="replace")[-300:])
    report = readback(url, branch, d, code_branch)
    if report["commit"] != out(repo, "rev-parse", "HEAD"):
        raise CheckpointError(4, "READBACK_SHA_MISMATCH", remote=report["commit"])
    return {"status": "DURABLE", "resumed": resumed, **report}


# ------------------------------------------------------------------ ripresa da zero
def bootstrap(url, branch, dest, d=DEFAULT_DIR, code_branch=None):
    p = subprocess.run(["git", "clone", "-q", "--no-local", "--branch", branch, url, dest], capture_output=True)
    if p.returncode != 0:
        raise CheckpointError(5, "CLONE_FAILED", stderr=p.stderr.decode(errors="replace")[-300:])
    if code_branch:
        git(dest, "fetch", "-q", "origin", f"{code_branch}:refs/remotes/origin/{code_branch}", check=False)
    try:
        report = verify(dest, "HEAD", d)
    except CheckpointError as exc:
        last_valid = None
        for c in out(dest, "rev-list", "--first-parent", "HEAD").splitlines()[1:200]:
            try:
                verify(dest, c, d)
                last_valid = c
                break
            except CheckpointError:
                continue
        raise CheckpointError(2, "LATEST_CHECKPOINT_CORRUPT", inner=exc.reason, problems=exc.detail.get("problems"),
                              last_valid_checkpoint=last_valid, action="nessun rollback automatico: decidere esplicitamente")
    state = json.loads(blob(dest, report["commit"], f"{d}/STATE.json"))
    return {"status": "READY", **report, "mandate_id": state["mandate_id"],
            "owner_authorizations": state["owner_authorizations"], "blocks": state["blocks"],
            "code_branch": (state.get("code") or {}).get("branch")}


def main(argv=None):
    ap = argparse.ArgumentParser(prog="pfckpt")
    sub = ap.add_subparsers(dest="cmd", required=True)
    v = sub.add_parser("verify"); v.add_argument("--repo", default="."); v.add_argument("--ref", default="HEAD")
    v.add_argument("--dir", default=DEFAULT_DIR); v.add_argument("--code-repo")
    s = sub.add_parser("save"); s.add_argument("--repo", default="."); s.add_argument("--branch", required=True)
    s.add_argument("--remote", default="origin"); s.add_argument("--expected", required=True)
    s.add_argument("--message", required=True); s.add_argument("--dir", default=DEFAULT_DIR); s.add_argument("--code-branch")
    b = sub.add_parser("bootstrap"); b.add_argument("--url", required=True); b.add_argument("--branch", required=True)
    b.add_argument("--dest", required=True); b.add_argument("--dir", default=DEFAULT_DIR); b.add_argument("--code-branch")
    a = ap.parse_args(argv)
    try:
        if a.cmd == "verify":
            res = {"status": "VALID", **verify(a.repo, a.ref, a.dir, a.code_repo)}
        elif a.cmd == "save":
            res = save(a.repo, a.branch, a.remote, a.expected, a.message, a.dir, a.code_branch)
        else:
            res = bootstrap(a.url, a.branch, a.dest, a.dir, a.code_branch)
        print(json.dumps(res, ensure_ascii=False))
        return 0
    except CheckpointError as exc:
        print(json.dumps({"status": "ERROR", "reason": exc.reason, **exc.detail}, ensure_ascii=False, default=str))
        return exc.code


if __name__ == "__main__":
    sys.exit(main())
