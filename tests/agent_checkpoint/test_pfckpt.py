"""PF-UNIVERSAL-CHECKPOINT-V1 — test del protocollo di checkpoint con git REALE (offline).

Un repository bare locale fa da «GitHub»; ogni agente/sessione e' un clone separato. Nessun mock di git.
Integrita', crash in ogni fase della pubblicazione, concorrenza fra due agenti, portabilita' (clone pulito, nessun file
locale, nessuna cronologia) e il difetto storico dei tre log ignorati da .gitignore.
"""
import json
import os
import shutil
import subprocess
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(HERE)), "tools", "agent_checkpoint"))
import pfckpt  # noqa: E402

D = pfckpt.DEFAULT_DIR
BR = "agent-state/test"
CODE_BR = "code"


def sh(cwd, *args, check=True):
    p = subprocess.run(["git", "-C", cwd, *args], capture_output=True, text=True)
    if check and p.returncode != 0:
        raise AssertionError(p.stderr)
    return p.stdout.strip()


def _cfg(repo):
    sh(repo, "config", "user.email", "agent@test"); sh(repo, "config", "user.name", "agent")


def _state(code_sha, seq, prev, **over):
    s = {"schema": pfckpt.SCHEMA, "repository": "zarbopiero963-droid/Pickfair-nogui", "seq": seq,
         "mandate_id": "TEST", "previous_checkpoint_sha": prev, "code": {"branch": CODE_BR, "sha": code_sha},
         "lotto_corrente": "L1", "lotti_completati": [], "next_atomic_action": "fare X", "dependencies": {},
         "blocks": [], "owner_authorizations": {"live": False}, "tests_executed": [], "tests_missing": [],
         "missing_historical_evidence": [], "gate_a": "NON PASS", "pr_505": "aperta", "certification_verdict": "NO-GO"}
    s.update(over)
    return s


@pytest.fixture
def world(tmp_path):
    """remote bare con un branch di codice e un primo checkpoint valido; ritorna (url, code_sha, first_ckpt)."""
    remote = tmp_path / "remote.git"
    subprocess.run(["git", "init", "-q", "--bare", str(remote)], check=True)
    seed = tmp_path / "seed"
    subprocess.run(["git", "init", "-q", str(seed)], check=True); _cfg(str(seed))
    (seed / "app.py").write_text("print('pickfair')\n")
    (seed / ".gitignore").write_text("*.log\n")
    sh(str(seed), "add", "-A"); sh(str(seed), "commit", "-q", "-m", "code")
    code_sha = sh(str(seed), "rev-parse", "HEAD")
    sh(str(seed), "remote", "add", "origin", str(remote))
    sh(str(seed), "push", "-q", "origin", f"HEAD:refs/heads/{CODE_BR}")
    os.makedirs(seed / D / "results")
    (seed / D / "STATE.json").write_text(json.dumps(_state(code_sha, 1, code_sha)))
    (seed / D / "HANDOFF.md").write_text("primo checkpoint\n")
    pfckpt.write_manifest(str(seed), D)
    sh(str(seed), "add", "-A", "-f"); sh(str(seed), "commit", "-q", "-m", "ckpt 1")
    sh(str(seed), "push", "-q", "origin", f"HEAD:refs/heads/{BR}")
    first = sh(str(seed), "rev-parse", "HEAD")
    shutil.rmtree(seed)
    return {"url": str(remote), "code": code_sha, "first": first, "tmp": tmp_path}


def agent(world, name):
    """Una sessione/agente nuovo: clone pulito, nessun file locale precedente."""
    path = str(world["tmp"] / name)
    subprocess.run(["git", "clone", "-q", "--no-local", "--branch", BR, world["url"], path], check=True)
    _cfg(path)
    sh(path, "fetch", "-q", "origin", f"{CODE_BR}:refs/remotes/origin/{CODE_BR}")
    return path


def next_state(repo, world, **over):
    head = sh(repo, "rev-parse", "HEAD")
    st = json.load(open(os.path.join(repo, D, "STATE.json")))
    st.update(seq=st["seq"] + 1, previous_checkpoint_sha=head, **over)
    with open(os.path.join(repo, D, "STATE.json"), "w") as fh:
        json.dump(st, fh)
    return head


def add_artifact(repo, name, text):
    os.makedirs(os.path.join(repo, D, "results"), exist_ok=True)     # git non versiona directory vuote
    with open(os.path.join(repo, D, "results", name), "w") as fh:
        fh.write(text)


def raw_commit(repo, msg, force_add=True):
    """Commit SENZA lo strumento (per costruire checkpoint difettosi come farebbe un agente sbagliato)."""
    sh(repo, "add", "-A", *(["-f"] if force_add else []), "--", D)
    sh(repo, "commit", "-q", "-m", msg)
    return sh(repo, "rev-parse", "HEAD")


def expect(code, fn, *a, **k):
    with pytest.raises(pfckpt.CheckpointError) as ei:
        fn(*a, **k)
    assert ei.value.code == code, (ei.value.reason, ei.value.detail)
    return ei.value


# ================================================================== integrita'
def test_valid_first_checkpoint_verifies(world):
    a = agent(world, "a")
    rep = pfckpt.verify(a, "HEAD", D)
    assert rep["seq"] == 1 and rep["code_sha"] == world["code"]


def test_historical_bug_gitignored_artifact_is_detected(world):
    """Il difetto reale di b45f528: manifest calcolato dal disco, *.log escluso in silenzio dal commit."""
    a = agent(world, "a")
    next_state(a, world)
    add_artifact(a, "mutation.log", "MC1 KILLED\n")
    pfckpt.write_manifest(a, D)
    raw_commit(a, "ckpt 2 difettoso", force_add=False)                 # git add senza -f: il .log resta fuori
    err = expect(2, pfckpt.verify, a, "HEAD", D)
    assert any("mutation.log" in p and "assente dal commit" in p for p in err.detail["problems"])


def test_save_commits_gitignored_artifacts(world):
    a = agent(world, "a")
    exp = next_state(a, world)
    add_artifact(a, "mutation.log", "MC1 KILLED\n")
    res = pfckpt.save(a, BR, "origin", exp, "ckpt 2", D, agent="A")
    assert res["status"] == "DURABLE"
    assert "results/mutation.log" in pfckpt.tree_files(a, "HEAD", D)


@pytest.mark.parametrize("case", ["missing", "altered", "wrong_hash", "incomplete_manifest", "bad_json",
                                  "unknown_code_sha", "wrong_seq", "wrong_previous", "bad_status", "pass_without_sha",
                                  "declared_artifact_absent", "secret"])
def test_integrity_violations_are_rejected(world, case):
    a = agent(world, "a")
    head = next_state(a, world)
    add_artifact(a, "r.txt", "risultato\n")
    st_path = os.path.join(a, D, "STATE.json")
    st = json.load(open(st_path))
    if case == "unknown_code_sha":
        st["code"]["sha"] = "f" * 40
    if case == "wrong_seq":
        st["seq"] = 7
    if case == "wrong_previous":
        st["previous_checkpoint_sha"] = "0" * 40
    if case == "bad_status":
        st["tests_executed"] = [{"id": "t", "status": "OK", "sha": world["code"]}]
    if case == "pass_without_sha":
        st["tests_executed"] = [{"id": "t", "status": "PASS"}]
    if case == "declared_artifact_absent":
        st["tests_executed"] = [{"id": "t", "status": "PASS", "sha": world["code"], "artifact": "results/assente.txt"}]
    json.dump(st, open(st_path, "w"))
    if case == "secret":
        add_artifact(a, "leak.txt", 'api_key = "abcdefghijklmnopqrstuvwxyz"\n')
    pfckpt.write_manifest(a, D)
    if case == "missing":
        os.remove(os.path.join(a, D, "results", "r.txt"))
    if case == "altered":
        add_artifact(a, "r.txt", "risultato alterato\n")
    if case == "wrong_hash":
        m = os.path.join(a, D, "MANIFEST.sha256")
        open(m, "w").write(open(m).read().replace(open(m).read()[:8], "00000000", 1))
    if case == "incomplete_manifest":
        add_artifact(a, "non_listato.txt", "x\n")
    if case == "bad_json":
        open(st_path, "w").write("{rotto")
    raw_commit(a, "ckpt difettoso")
    expect(2, pfckpt.verify, a, "HEAD", D)
    # e lo strumento non pubblica mai un checkpoint invalido: il remoto resta al primo checkpoint
    assert pfckpt.remote_head(a, "origin", BR) == head


def test_save_refuses_invalid_state_and_publishes_nothing(world):
    a = agent(world, "a")
    exp = next_state(a, world, certification_verdict="CERTIFICATO")
    with pytest.raises(pfckpt.CheckpointError) as ei:
        pfckpt.save(a, BR, "origin", exp, "ckpt invalido", D, agent="A")
    assert ei.value.code == 2
    assert pfckpt.remote_head(a, "origin", BR) == exp


# ================================================================== crash in ogni fase
def test_crash_before_commit_then_save_again(world):
    a = agent(world, "a")
    exp = next_state(a, world)
    add_artifact(a, "r.txt", "x\n")                                   # crash qui: nulla committato
    assert pfckpt.save(a, BR, "origin", exp, "ckpt 2", D, agent="A")["status"] == "DURABLE"


def test_crash_after_commit_before_push_resumes(world):
    a = agent(world, "a")
    exp = next_state(a, world)
    add_artifact(a, "r.txt", "x\n")
    pfckpt.write_manifest(a, D)
    raw_commit(a, "ckpt 2")                                          # commit locale, crash prima del push
    res = pfckpt.save(a, BR, "origin", exp, "ckpt 2", D, agent="A")
    assert res["status"] == "DURABLE" and res["resumed"] == "after_commit"
    assert sh(a, "rev-list", "--count", f"{exp}..HEAD") == "1", "nessun commit duplicato"


def test_crash_during_push_network_failure_then_resume(world):
    a = agent(world, "a")
    exp = next_state(a, world)
    add_artifact(a, "r.txt", "x\n")
    sh(a, "remote", "set-url", "--push", "origin", str(world["tmp"] / "inesistente.git"))
    with pytest.raises(pfckpt.CheckpointError):
        pfckpt.save(a, BR, "origin", exp, "ckpt 2", D, agent="A")               # push fallito: non DURABLE
    assert pfckpt.remote_head(a, "origin", BR) == exp
    sh(a, "remote", "set-url", "--push", "origin", world["url"])
    res = pfckpt.save(a, BR, "origin", exp, "ckpt 2", D, agent="A")
    assert res["status"] == "DURABLE" and res["resumed"] == "after_commit"


def test_crash_after_push_before_readback_resumes(world):
    a = agent(world, "a")
    exp = next_state(a, world)
    add_artifact(a, "r.txt", "x\n")
    pfckpt.write_manifest(a, D)
    raw_commit(a, "ckpt 2")
    sh(a, "push", "-q", "origin", f"HEAD:refs/heads/{BR}")            # push riuscito, crash prima del readback
    res = pfckpt.save(a, BR, "origin", exp, "ckpt 2", D, agent="A")
    assert res["status"] == "DURABLE" and res["resumed"] == "after_push"


def test_resume_after_commit_refuses_artifact_left_only_on_disk(world):
    """Variante del difetto storico in ripresa: dopo il commit locale nasce un .log ignorato; non si pubblica senza."""
    a = agent(world, "a")
    exp = next_state(a, world)
    add_artifact(a, "r.txt", "x\n")
    pfckpt.write_manifest(a, D)
    raw_commit(a, "ckpt 2")
    add_artifact(a, "mutation.log", "solo locale\n")                  # ignorato da .gitignore, non committato
    err = expect(2, pfckpt.save, a, BR, "origin", exp, "ckpt 2", D, agent="A")
    assert err.reason == "LOCAL_ONLY_FILES"
    assert pfckpt.remote_head(a, "origin", BR) == exp


def test_push_that_does_not_reach_the_source_of_truth_is_not_durable(world):
    """DURABLE solo dal readback sul remoto di verita': un push finito altrove non conta."""
    other = world["tmp"] / "altrove.git"
    subprocess.run(["git", "clone", "-q", "--bare", world["url"], str(other)], check=True)
    a = agent(world, "a")
    exp = next_state(a, world)
    add_artifact(a, "r.txt", "x\n")
    sh(a, "remote", "set-url", "--push", "origin", str(other))
    err = expect(4, pfckpt.save, a, BR, "origin", exp, "ckpt 2", D, agent="A")
    assert err.reason == "READBACK_SHA_MISMATCH"
    assert pfckpt.remote_head(a, "origin", BR) == exp


def test_restart_after_complete_checkpoint_bootstraps(world):
    a = agent(world, "a")
    exp = next_state(a, world, next_atomic_action="mutation d04")
    pfckpt.save(a, BR, "origin", exp, "ckpt 2", D, agent="A")
    shutil.rmtree(a)                                                  # la sessione sparisce
    rep = pfckpt.bootstrap(world["url"], BR, str(world["tmp"] / "nuova"), D, CODE_BR, agent="A")   # stesso agente: lease MINE
    assert rep["status"] == "READY" and rep["seq"] == 2 and rep["next_atomic_action"] == "mutation d04"
    other = pfckpt.bootstrap(world["url"], BR, str(world["tmp"] / "altro"), D, CODE_BR)            # anonimo: solo lettura
    assert other["status"] == "READY_READ_ONLY"


# ================================================================== concorrenza
def test_two_agents_same_base_second_push_blocked_then_reconciled(world):
    a, b = agent(world, "a"), agent(world, "b")
    base = sh(a, "rev-parse", "HEAD")
    next_state(a, world, lotti_completati=["A: mutation"])
    add_artifact(a, "a.txt", "risultato di A\n")
    ra = pfckpt.save(a, BR, "origin", base, "ckpt di A", D, agent="A", release=True)   # A consegna: lease rilasciato
    next_state(b, world, lotti_completati=["B: regressione"])
    add_artifact(b, "b.txt", "risultato di B\n")
    err = expect(3, pfckpt.save, b, BR, "origin", base, "ckpt di B", D, agent="B")
    assert err.reason == "CONCURRENT_UPDATE" and err.detail["remote"] == ra["commit"]
    # riconciliazione esplicita: B legge lo stato di A e ci aggiunge i propri risultati, senza perderne nessuno
    mine = open(os.path.join(b, D, "results", "b.txt")).read()
    sh(b, "fetch", "-q", "origin"); sh(b, "reset", "-q", "--hard", f"origin/{BR}")
    add_artifact(b, "b.txt", mine)
    st = json.load(open(os.path.join(b, D, "STATE.json")))
    next_state(b, world, lotti_completati=st["lotti_completati"] + ["B: regressione"])
    rb = pfckpt.save(b, BR, "origin", ra["commit"], "ckpt di B riconciliato", D, agent="B")
    files = pfckpt.tree_files(b, rb["commit"], D)
    assert {"results/a.txt", "results/b.txt"} <= set(files) and rb["seq"] == 3
    final = json.loads(pfckpt.blob(b, rb["commit"], files["STATE.json"]))
    assert final["lotti_completati"] == ["A: mutation", "B: regressione"]


def test_stale_local_base_is_refused(world):
    a = agent(world, "a")
    head = sh(a, "rev-parse", "HEAD")
    expect(3, pfckpt.save, a, BR, "origin", "1" * 40, "x", D, agent="A")        # expected non coincide col remoto
    assert pfckpt.remote_head(a, "origin", BR) == head


# ================================================================== portabilita' e bootstrap
def test_bootstrap_in_clean_environment_without_history_or_local_files(world):
    rep = pfckpt.bootstrap(world["url"], BR, str(world["tmp"] / "pulito"), D, CODE_BR)
    assert rep["status"] == "READY" and rep["code_sha"] == world["code"]
    assert rep["owner_authorizations"] == {"live": False} and rep["mandate_id"] == "TEST"


def test_bootstrap_on_corrupt_latest_reports_last_valid_without_rollback(world):
    a = agent(world, "a")
    next_state(a, world)
    add_artifact(a, "x.log", "y\n")
    pfckpt.write_manifest(a, D)
    raw_commit(a, "ckpt corrotto", force_add=False)
    sh(a, "push", "-q", "origin", f"HEAD:refs/heads/{BR}")            # un agente ha pubblicato un checkpoint rotto
    err = expect(2, pfckpt.bootstrap, world["url"], BR, str(world["tmp"] / "c"), D, CODE_BR)
    assert err.reason == "LATEST_CHECKPOINT_CORRUPT" and err.detail["last_valid_checkpoint"] == world["first"]
    assert pfckpt.remote_head(a, "origin", BR) != world["first"], "nessun rollback silenzioso del remoto"


def test_cli_exit_codes(world, capsys):
    a = agent(world, "a")
    assert pfckpt.main(["verify", "--repo", a, "--ref", "HEAD", "--dir", D]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "VALID"
    assert pfckpt.main(["save", "--repo", a, "--branch", BR, "--expected", "1" * 40, "--message", "m", "--dir", D, "--agent", "cli"]) == 3


# ================================================================== PF-AGENT-CONTINUITY-01: lease, fencing, puntatori, patch
import datetime as _dt


def _lease_of(repo, ref="HEAD"):
    return pfckpt.tree_json(repo, ref, D, pfckpt.LEASE)


def test_save_writes_consistent_pointers_and_lease(world):
    a = agent(world, "a")
    exp = next_state(a, world, next_atomic_action="mutation d04")
    res = pfckpt.save(a, BR, "origin", exp, "ckpt 2", D, agent="claude", ttl_min=30)
    assert res["status"] == "DURABLE" and res["lease_owner"] == "claude"
    cur = pfckpt.tree_json(a, "HEAD", D, pfckpt.CURRENT)
    task = pfckpt.tree_json(a, "HEAD", D, pfckpt.TASK_STATE)
    assert cur["seq"] == 2 and cur["code"]["sha"] == world["code"] and cur["next_atomic_action"] == "mutation d04"
    assert task["next_atomic_action"] == "mutation d04" and task["blocks"] == []
    lease = _lease_of(a)
    assert lease["owner"] == "claude" and lease["fencing_token"] == 2
    assert pfckpt.parse_iso(lease["expires_at"]) - pfckpt.parse_iso(lease["acquired_at"]) == _dt.timedelta(minutes=30)


def test_tampered_pointer_makes_the_checkpoint_invalid(world):
    a = agent(world, "a")
    exp = next_state(a, world)
    pfckpt.save(a, BR, "origin", exp, "ckpt 2", D, agent="claude")
    next_state(a, world)
    cur = json.load(open(os.path.join(a, D, pfckpt.CURRENT)))
    cur["next_atomic_action"] = "altro"                                # puntatore che mente
    json.dump(cur, open(os.path.join(a, D, pfckpt.CURRENT), "w"))
    pfckpt.write_manifest(a, D)
    raw_commit(a, "ckpt incoerente")
    err = expect(2, pfckpt.verify, a, "HEAD", D)
    assert any("CURRENT.json.next_atomic_action" in p for p in err.detail["problems"])


def test_lease_held_by_another_agent_blocks_writes_until_expiry_then_takeover(world):
    a, b = agent(world, "a"), agent(world, "b")
    base = sh(a, "rev-parse", "HEAD")
    next_state(a, world)
    ra = pfckpt.save(a, BR, "origin", base, "ckpt di A", D, agent="claude", ttl_min=60)
    sh(b, "fetch", "-q", "origin"); sh(b, "reset", "-q", "--hard", f"origin/{BR}")
    next_state(b, world)
    err = expect(6, pfckpt.save, b, BR, "origin", ra["commit"], "ckpt di B", D, agent="codex")
    assert err.reason == "LEASE_HELD" and err.detail["owner"] == "claude"
    assert pfckpt.remote_head(b, "origin", BR) == ra["commit"], "nessuna scrittura con lease altrui"
    later = pfckpt.utcnow() + _dt.timedelta(minutes=61)                 # il lease di A e' scaduto: B subentra
    rb = pfckpt.save(b, BR, "origin", ra["commit"], "ckpt di B", D, agent="codex", now=later)
    assert rb["status"] == "DURABLE" and _lease_of(b)["owner"] == "codex" and _lease_of(b)["fencing_token"] == 3


def test_stale_agent_with_expired_lease_cannot_overwrite_the_new_owner(world):
    a, b = agent(world, "a"), agent(world, "b")
    base = sh(a, "rev-parse", "HEAD")
    next_state(a, world)
    ra = pfckpt.save(a, BR, "origin", base, "A", D, agent="claude", ttl_min=1)
    sh(b, "fetch", "-q", "origin"); sh(b, "reset", "-q", "--hard", f"origin/{BR}")
    next_state(b, world)
    rb = pfckpt.save(b, BR, "origin", ra["commit"], "B", D, agent="codex", now=pfckpt.utcnow() + _dt.timedelta(minutes=2))
    next_state(a, world)                                                # A si risveglia sulla sua vecchia base
    err = expect(3, pfckpt.save, a, BR, "origin", ra["commit"], "A stale", D, agent="claude")
    assert err.reason == "CONCURRENT_UPDATE" and err.detail["remote"] == rb["commit"]
    sh(a, "fetch", "-q", "origin"); sh(a, "reset", "-q", "--hard", f"origin/{BR}")
    next_state(a, world)                                                # anche sulla base nuova: lease di B attivo
    expect(6, pfckpt.save, a, BR, "origin", rb["commit"], "A", D, agent="claude")


def test_owner_can_release_the_lease_and_anyone_can_then_write(world):
    a, b = agent(world, "a"), agent(world, "b")
    base = sh(a, "rev-parse", "HEAD")
    next_state(a, world)
    ra = pfckpt.save(a, BR, "origin", base, "A", D, agent="claude", ttl_min=600, release=True)
    assert _lease_of(a)["owner"] is None and _lease_of(a)["released_by"] == "claude"
    sh(b, "fetch", "-q", "origin"); sh(b, "reset", "-q", "--hard", f"origin/{BR}")
    next_state(b, world)
    assert pfckpt.save(b, BR, "origin", ra["commit"], "B", D, agent="grok")["lease_owner"] == "grok"


def test_save_without_agent_is_refused(world):
    a = agent(world, "a")
    exp = next_state(a, world)
    err = expect(6, pfckpt.save, a, BR, "origin", exp, "x", D)
    assert err.reason == "AGENT_REQUIRED" and pfckpt.remote_head(a, "origin", BR) == exp


def test_bootstrap_reports_lease_and_is_read_only_when_held(world):
    a = agent(world, "a")
    exp = next_state(a, world)
    pfckpt.save(a, BR, "origin", exp, "A", D, agent="claude", ttl_min=60)
    rep = pfckpt.bootstrap(world["url"], BR, str(world["tmp"] / "b1"), D, CODE_BR, agent="codex")
    assert rep["status"] == "READY_READ_ONLY" and rep["lease"] == "HELD"
    rep2 = pfckpt.bootstrap(world["url"], BR, str(world["tmp"] / "b2"), D, CODE_BR, agent="claude")
    assert rep2["status"] == "READY" and rep2["lease"] == "MINE" and rep2["code_divergence"]["status"] == "ALIGNED"


def test_code_patch_is_captured_and_restored_in_a_fresh_environment(world):
    """Lavoro NON committato trasportato dal checkpoint: patch tracciata + file nuovi, ripristinati su baseline pulita."""
    code = str(world["tmp"] / "code")
    subprocess.run(["git", "clone", "-q", "--no-local", "--branch", CODE_BR, world["url"], code], check=True); _cfg(code)
    open(os.path.join(code, "app.py"), "a").write("print('microstake')\n")
    os.makedirs(os.path.join(code, "core"))
    open(os.path.join(code, "core", "nuovo.py"), "w").write("X = 1\n")
    open(os.path.join(code, "scratch.log"), "w").write("ignorato\n")   # .gitignore: non trasportato
    a = agent(world, "a")
    exp = next_state(a, world)
    res = pfckpt.save(a, BR, "origin", exp, "ckpt con patch", D, agent="claude", code_repo=code)
    assert res["status"] == "DURABLE"
    meta = pfckpt.tree_json(a, "HEAD", D, "code_patch/BASELINE.json")
    assert meta["head"] == world["code"] and meta["modified"] == ["app.py"] and meta["untracked"] == ["core/nuovo.py"]
    files = pfckpt.tree_files(a, "HEAD", D)
    assert "code_patch/tracked.patch" in files and "code_patch/untracked/core/nuovo.py" in files
    fresh = str(world["tmp"] / "fresh")
    subprocess.run(["git", "clone", "-q", "--no-local", "--branch", CODE_BR, world["url"], fresh], check=True); _cfg(fresh)
    out = pfckpt.restore_code_patch(a, "HEAD", D, fresh)
    assert out["status"] == "CODE_RESTORED"
    assert open(os.path.join(fresh, "app.py")).read().endswith("print('microstake')\n")
    assert open(os.path.join(fresh, "core", "nuovo.py")).read() == "X = 1\n"
    assert not os.path.exists(os.path.join(fresh, "scratch.log"))
    expect(3, pfckpt.restore_code_patch, a, "HEAD", D, fresh)           # repo gia' sporco: mai sovrascrivere


def test_restore_code_patch_refuses_a_different_baseline(world):
    code = str(world["tmp"] / "code")
    subprocess.run(["git", "clone", "-q", "--no-local", "--branch", CODE_BR, world["url"], code], check=True); _cfg(code)
    open(os.path.join(code, "app.py"), "a").write("x\n")
    a = agent(world, "a")
    exp = next_state(a, world)
    pfckpt.save(a, BR, "origin", exp, "p", D, agent="claude", code_repo=code)
    other = str(world["tmp"] / "other")
    subprocess.run(["git", "clone", "-q", "--no-local", "--branch", CODE_BR, world["url"], other], check=True); _cfg(other)
    open(os.path.join(other, "b.txt"), "w").write("1\n"); sh(other, "add", "-A"); sh(other, "commit", "-q", "-m", "avanti")
    err = expect(3, pfckpt.restore_code_patch, a, "HEAD", D, other)
    assert err.reason == "CODE_BASELINE_MISMATCH"


def test_bootstrap_detects_code_branch_divergence(world):
    code = str(world["tmp"] / "code")
    subprocess.run(["git", "clone", "-q", "--no-local", "--branch", CODE_BR, world["url"], code], check=True); _cfg(code)
    open(os.path.join(code, "b.txt"), "w").write("1\n"); sh(code, "add", "-A"); sh(code, "commit", "-q", "-m", "avanti")
    sh(code, "push", "-q", "origin", f"HEAD:refs/heads/{CODE_BR}")
    rep = pfckpt.bootstrap(world["url"], BR, str(world["tmp"] / "d"), D, CODE_BR, agent="claude")
    assert rep["code_divergence"]["status"] == "DIVERGED" and rep["code_divergence"]["ahead"] == "1"


def test_recover_materialises_the_last_valid_checkpoint_without_touching_the_remote(world):
    a = agent(world, "a")
    exp = next_state(a, world)
    good = pfckpt.save(a, BR, "origin", exp, "buono", D, agent="claude")["commit"]
    next_state(a, world)
    add_artifact(a, "x.log", "y\n")
    pfckpt.write_manifest(a, D)
    raw_commit(a, "rotto", force_add=False)
    sh(a, "push", "-q", "origin", f"HEAD:refs/heads/{BR}")
    rep = pfckpt.recover(world["url"], BR, str(world["tmp"] / "rec"), D)
    assert rep["status"] == "RECOVERED" and rep["commit"] == good and rep["skipped_invalid"] == "1"
    assert pfckpt.remote_head(a, "origin", BR) != good, "il remoto non viene riscritto"


def test_restore_code_patch_detects_a_restored_state_different_from_the_declared_one(world):
    """BASELINE.json che dichiara piu' di quanto la patch contiene: il ripristino NON e' dichiarato riuscito."""
    code = str(world["tmp"] / "code")
    subprocess.run(["git", "clone", "-q", "--no-local", "--branch", CODE_BR, world["url"], code], check=True); _cfg(code)
    open(os.path.join(code, "app.py"), "a").write("x\n")
    a = agent(world, "a")
    exp = next_state(a, world)
    pfckpt.capture_code_patch(code, a, D)
    meta_path = os.path.join(a, D, "code_patch", "BASELINE.json")
    meta = json.load(open(meta_path)); meta["modified"] = ["app.py", "fantasma.py"]
    json.dump(meta, open(meta_path, "w"))
    st = json.load(open(os.path.join(a, D, "STATE.json"))); st["code"]["patch"] = meta
    json.dump(st, open(os.path.join(a, D, "STATE.json"), "w"))
    pfckpt.save(a, BR, "origin", exp, "p", D, agent="claude")
    fresh = str(world["tmp"] / "fresh")
    subprocess.run(["git", "clone", "-q", "--no-local", "--branch", CODE_BR, world["url"], fresh], check=True); _cfg(fresh)
    err = expect(4, pfckpt.restore_code_patch, a, "HEAD", D, fresh)
    assert err.reason == "CODE_PATCH_RESTORE_MISMATCH" and err.detail["restored"] == ["app.py"]
