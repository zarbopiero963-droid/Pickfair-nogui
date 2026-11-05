"""pfckpt_squash_durable_refs: SHA di codice durevoli dopo squash merge e cancellazione del branch (git REALE, offline).

Incidente reale: `agent-state/pf-claude-b` seq 17 citava in `code.sha` e `tests_executed[].sha` l'head di una PR
integrata con squash. Cancellato il branch, lo SHA non era piu' raggiungibile da nessun ref remoto e `bootstrap`
dava LATEST_CHECKPOINT_CORRUPT. Qui un repository bare locale fa da «GitHub»:
- `main` e' il branch durevole;
- `refs/pull/<N>/head` e' creato all'apertura della PR e sopravvive alla cancellazione del branch, come su GitHub;
- lo squash e' un commit nuovo su `main` con lo STESSO tree;
- il branch della PR viene cancellato e il bare potato (gc --prune=now).

Semantica: al save ogni SHA citato deve essere durevole sul remoto (branch durevole, refs/pull/<N>/head, integrazione
TREE_EQUAL su main; ref keep solo opt-in), altrimenti CODE_SHA_NOT_DURABLE e nulla e' pubblicato.
"""
import json
import os
import subprocess
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(HERE)), "tools", "agent_checkpoint"))
import pfckpt  # noqa: E402
from test_pfckpt import BR, CODE_BR, D, _cfg, agent, expect, next_state, raw_commit, sh, world  # noqa: E402,F401

FEATURE = "feature/pr-123"
KEEP = getattr(pfckpt, "KEEP_REF_PREFIX", "refs/agent-state/keep/")   # getattr: RED leggibile sul pfckpt di main


def _code_clone(world, name="codework"):
    path = str(world["tmp"] / name)
    if not os.path.exists(path):
        subprocess.run(["git", "clone", "-q", "--no-local", "--branch", CODE_BR, world["url"], path], check=True)
        _cfg(path)
    return path


def make_feature(world):
    """Branch di PR con un commit nuovo (non su main); ritorna (sha, tree)."""
    c = _code_clone(world)
    sh(c, "checkout", "-q", "-b", "feat")
    with open(os.path.join(c, "feat.py"), "w") as fh:
        fh.write("FEATURE = 1\n")
    sh(c, "add", "-A"); sh(c, "commit", "-q", "-m", "feature")
    sh(c, "push", "-q", "origin", f"HEAD:refs/heads/{FEATURE}")
    return sh(c, "rev-parse", "HEAD"), sh(c, "rev-parse", "HEAD^{tree}")


def open_pr(world, number, sha):
    """Come GitHub all'apertura della PR: refs/pull/<N>/head sul remoto."""
    sh(world["url"], "update-ref", f"refs/pull/{number}/head", sha)


def squash_merge_and_delete(world, feature_sha, alter=None):
    """Squash merge simulato: commit nuovo su main (e sul branch di codice) con lo stesso tree del branch di PR (o, con
    `alter`, con un solo file diverso), poi cancellazione del branch e potatura degli oggetti irraggiungibili."""
    c = _code_clone(world)
    sh(c, "fetch", "-q", "origin")
    sh(c, "checkout", "-q", "-B", "squash", "origin/main")
    sh(c, "checkout", "-q", feature_sha, "--", ".")
    if alter:
        with open(os.path.join(c, alter), "a") as fh:
            fh.write("# diverso\n")
    sh(c, "add", "-A"); sh(c, "commit", "-q", "-m", "Squash merge PR #123")
    squash = sh(c, "rev-parse", "HEAD")
    sh(c, "push", "-q", "origin", "HEAD:refs/heads/main", f"HEAD:refs/heads/{CODE_BR}")
    sh(c, "push", "-q", "origin", "--delete", FEATURE)
    sh(world["url"], "reflog", "expire", "--expire=now", "--all")
    sh(world["url"], "gc", "-q", "--prune=now")
    return squash, sh(c, "rev-parse", "HEAD^{tree}")


def ckpt_agent(world, name):
    """Agente sul branch di stato che conosce anche il branch di PR (la sessione che ha lavorato sulla PR)."""
    a = agent(world, name)
    sh(a, "fetch", "-q", "origin", "+refs/heads/*:refs/remotes/origin/*")
    return a


def _tests(sha, **extra):
    return [{"id": "suite", "status": "PASS", "sha": sha, **extra}]


def _remote_refs(world, prefix):
    return sh(world["url"], "for-each-ref", "--format=%(refname) %(objectname)", prefix).splitlines()


def _vias(res):
    return {r["label"]: r["via"] for r in res["durability"]}


def _legacy_checkpoint_citing(world, feat, **code_extra):
    """Checkpoint scritto SENZA controllo di durabilita' (come seq 17 prima della fix), pubblicato sul remoto."""
    a = ckpt_agent(world, "legacy")
    next_state(a, world, code={"branch": FEATURE, "sha": feat, **code_extra}, tests_executed=_tests(feat))
    pfckpt.write_manifest(a, D)
    raw_commit(a, "ckpt legacy")
    sh(a, "push", "-q", "origin", f"HEAD:refs/heads/{BR}")
    return sh(a, "rev-parse", "HEAD")


# ================================================================== 1. durabilita' di default, nessun ref extra
def test_sha_only_on_a_work_branch_is_refused_and_nothing_is_published(world):
    """Il caso make_case.sh: SHA solo sul branch della PR, nessuna PR => rifiuto chiaro al save, nulla pubblicato."""
    feat, _ = make_feature(world)
    a = ckpt_agent(world, "a")
    exp = next_state(a, world, code={"branch": FEATURE, "sha": feat}, tests_executed=_tests(feat))
    before = open(os.path.join(a, D, "STATE.json")).read()
    err = expect(2, pfckpt.save, a, BR, "origin", exp, "ckpt sulla PR", D, agent="claude")
    assert err.reason == "CODE_SHA_NOT_DURABLE"
    assert {r["label"] for r in err.detail["not_durable"]} == {"code.sha", "test 'suite'"}
    assert "pr" in err.detail["action"] and "unpublished_tree" in err.detail["action"]
    assert pfckpt.remote_head(a, "origin", BR) == exp and sh(a, "rev-parse", "HEAD") == exp, "nessun commit"
    assert open(os.path.join(a, D, "STATE.json")).read() == before, "STATE su disco intatto"


def test_default_save_with_declared_pr_publishes_no_ref_outside_the_state_branch(world):
    feat, _ = make_feature(world)
    open_pr(world, 1, feat)
    a = ckpt_agent(world, "a")
    refs_before = _remote_refs(world, "refs/")
    exp = next_state(a, world, code={"branch": FEATURE, "sha": feat, "pr": 1}, tests_executed=_tests(feat[:12]))
    res = pfckpt.save(a, BR, "origin", exp, "ckpt sulla PR", D, agent="claude")
    assert res["status"] == "DURABLE" and _vias(res) == {"code.sha": "PULL:1", "test 'suite'": "PULL:1"}
    changed = sorted(set(_remote_refs(world, "refs/")) ^ set(refs_before))
    assert [c.split()[0] for c in changed] == [f"refs/heads/{BR}"] * 2, changed   # solo il branch di stato si muove
    assert _remote_refs(world, KEEP) == []


def test_undeclared_pr_is_discovered_from_refs_pull(world):
    feat, _ = make_feature(world)
    open_pr(world, 7, feat)
    a = ckpt_agent(world, "a")
    exp = next_state(a, world, code={"branch": FEATURE, "sha": feat})
    res = pfckpt.save(a, BR, "origin", exp, "ckpt", D, agent="claude")
    assert _vias(res) == {"code.sha": "PULL:7"}


def test_sha_on_main_is_durable(world):
    a = agent(world, "a")
    exp = next_state(a, world, tests_executed=_tests(world["code"]))
    res = pfckpt.save(a, BR, "origin", exp, "ckpt", D, agent="claude")
    assert _vias(res) == {"code.sha": "BRANCH:main", "test 'suite'": "BRANCH:main"}


def test_declared_unpublished_tree_test_is_exempt(world):
    a = agent(world, "a")
    exp = next_state(a, world, tests_executed=[{"id": "scratch", "status": "FAIL", "sha": "f63cf3a5ece4",
                                                "unpublished_tree": True, "sha_note": "albero scratch dichiarato"}])
    assert pfckpt.save(a, BR, "origin", exp, "ckpt", D, agent="claude")["status"] == "DURABLE"


def test_pull_head_of_another_pr_does_not_make_the_sha_durable(world):
    feat, _ = make_feature(world)
    open_pr(world, 2, world["code"])                              # PR diversa: non contiene feat
    a = ckpt_agent(world, "a")
    exp = next_state(a, world, code={"branch": FEATURE, "sha": feat, "pr": 2})
    err = expect(2, pfckpt.save, a, BR, "origin", exp, "ckpt", D, agent="claude")
    assert err.reason == "CODE_SHA_NOT_DURABLE"


def test_allow_branch_only_is_declared_in_state_and_never_hides_the_loss(world):
    feat, _ = make_feature(world)
    a = ckpt_agent(world, "a")
    exp = next_state(a, world, code={"branch": FEATURE, "sha": feat})
    res = pfckpt.save(a, BR, "origin", exp, "ckpt", D, agent="claude", allow_branch_only=True, release=True)
    assert _vias(res) == {"code.sha": f"BRANCH_ONLY:{FEATURE}"}
    waiver = pfckpt.tree_json(a, "HEAD", D, "STATE.json")["code"]["durability_waiver"]
    assert waiver["branch_only"] == [feat] and waiver["declared_by"] == "claude"
    squash_merge_and_delete(world, feat)
    err = expect(2, pfckpt.bootstrap, world["url"], BR, str(world["tmp"] / "fresh"), D, CODE_BR)
    assert err.reason == "NEEDS_RECONCILIATION", "branch cancellato: mai READY"


def test_readback_refuses_a_published_checkpoint_whose_sha_is_not_durable(world):
    feat, _ = make_feature(world)
    _legacy_checkpoint_citing(world, feat)
    err = expect(4, pfckpt.readback, world["url"], BR, D)
    assert err.reason == "READBACK_CODE_SHA_NOT_DURABLE"


# ================================================================== 2. il caso reale: PR GitHub squashata e cancellata
@pytest.mark.parametrize("declared_pr", [1, None])
def test_github_pr_squashed_and_deleted_bootstraps_ready_through_refs_pull(world, declared_pr):
    feat, _ = make_feature(world)
    open_pr(world, 1, feat)
    a = ckpt_agent(world, "a")
    code = {"branch": FEATURE, "sha": feat, **({"pr": declared_pr} if declared_pr else {})}
    exp = next_state(a, world, code=code, tests_executed=_tests(feat))
    assert pfckpt.save(a, BR, "origin", exp, "ckpt sulla PR", D, agent="claude", release=True)["status"] == "DURABLE"
    squash_merge_and_delete(world, feat)
    assert sh(world["url"], "for-each-ref", f"refs/heads/{FEATURE}") == "", "branch di PR cancellato"
    rep = pfckpt.bootstrap(world["url"], BR, str(world["tmp"] / "fresh"), D, CODE_BR)
    assert rep["status"] == "READY" and rep["code_sha"] == feat and rep["code_reference"]["via"] == "ORIGINAL"
    assert rep["pull_refs_fetched"]["declared_prs"] == ([1] if declared_pr else [])


def test_legacy_checkpoint_without_pr_ref_after_squash_is_needs_reconciliation(world):
    feat, _ = make_feature(world)
    _legacy_checkpoint_citing(world, feat)
    squash_merge_and_delete(world, feat)
    err = expect(2, pfckpt.bootstrap, world["url"], BR, str(world["tmp"] / "fresh"), D, CODE_BR)
    assert err.reason == "NEEDS_RECONCILIATION", err.reason
    assert any("SHA del codice inesistente" in p for p in err.detail["problems"])


# ================================================================== 3. transizione verso main (tree-equal)
def test_tree_equal_transition_new_checkpoint_bootstraps_ready_from_fresh_clone(world):
    feat, ftree = make_feature(world)
    legacy = _legacy_checkpoint_citing(world, feat)
    squash, stree = squash_merge_and_delete(world, feat)
    assert stree == ftree
    a = agent(world, "after")                                     # clone NUOVO: lo SHA originale non esiste qui
    assert pfckpt.resolve_commit(a, feat) is None
    sh(a, "fetch", "-q", "origin", "+refs/heads/main:refs/remotes/origin/main")
    integ = pfckpt.build_integration(a, feat, squash, original_tree=ftree)
    assert integ["equivalence"] == "TREE_EQUAL" and integ["original_tree_source"] == "RECORDED"
    st = json.load(open(os.path.join(a, D, "STATE.json")))
    code = dict(st["code"], integration=integ)
    tests = [dict(t, integration=integ) for t in st["tests_executed"]]
    exp = next_state(a, world, code=code, tests_executed=tests)
    assert exp == legacy
    res = pfckpt.save(a, BR, "origin", exp, "transizione verso main", D, agent="claude", release=True)
    assert res["status"] == "DURABLE"
    assert _vias(res) == {"code.sha": "INTEGRATION:main", "test 'suite'": "INTEGRATION:main"}
    rep = pfckpt.bootstrap(world["url"], BR, str(world["tmp"] / "fresh"), D, CODE_BR)
    assert rep["status"] == "READY" and rep["code_sha"] == feat
    assert rep["code_reference"]["via"] == "INTEGRATION_TREE_EQUAL"
    assert rep["code_reference"]["effective_sha"] == squash
    assert rep["owner_authorizations"] == {"live": False}, "autorizzazioni owner invariate"
    final = pfckpt.tree_json(a, "HEAD", D, "STATE.json")
    assert final["code"]["sha"] == feat and final["tests_executed"][0]["sha"] == feat, "SHA originali conservati"


def test_transition_original_still_reachable_requires_matching_original_tree(world):
    feat, ftree = make_feature(world)
    a = ckpt_agent(world, "a")
    squash, _ = squash_merge_and_delete(world, feat)              # il clone `a` conserva ancora l'oggetto originale
    sh(a, "fetch", "-q", "origin", "+refs/heads/main:refs/remotes/origin/main")
    integ = pfckpt.build_integration(a, feat, squash)
    assert integ["original_tree_source"] == "OBJECT" and integ["original_tree"] == ftree
    bad = dict(integ, original_tree=sh(a, "rev-parse", "origin/main~1^{tree}"))
    bad["integration_tree"] = bad["original_tree"]
    next_state(a, world, code={"branch": FEATURE, "sha": feat, "integration": bad})
    pfckpt.write_manifest(a, D)
    raw_commit(a, "original_tree falso")
    err = expect(2, pfckpt.verify, a, "HEAD", D)
    assert any("tree(original_sha)" in p for p in err.detail["problems"]), err.detail["problems"]


@pytest.mark.parametrize("lie", ["declared_tree_equal", "computed"])
def test_transition_with_one_different_file_is_never_ready(world, lie):
    feat, ftree = make_feature(world)
    _legacy_checkpoint_citing(world, feat)
    squash, stree = squash_merge_and_delete(world, feat, alter="app.py")
    assert stree != ftree
    a = agent(world, "after")
    sh(a, "fetch", "-q", "origin", "+refs/heads/main:refs/remotes/origin/main")
    if lie == "computed":
        integ = pfckpt.build_integration(a, feat, squash, original_tree=ftree)
        assert integ["equivalence"] == "NOT_PROVEN"
    else:                                                         # record che mente: dichiara TREE_EQUAL
        integ = {"original_sha": feat, "original_tree": ftree, "integration_sha": squash,
                 "integration_tree": ftree, "equivalence": "TREE_EQUAL"}
    next_state(a, world, code={"branch": FEATURE, "sha": feat, "integration": integ},
               tests_executed=[{"id": "suite", "status": "PASS", "sha": feat, "integration": integ}])
    pfckpt.write_manifest(a, D)
    raw_commit(a, "transizione con tree diverso")
    sh(a, "push", "-q", "origin", f"HEAD:refs/heads/{BR}")
    err = expect(2, pfckpt.bootstrap, world["url"], BR, str(world["tmp"] / "fresh"), D, CODE_BR)
    assert err.reason == "NEEDS_RECONCILIATION"
    b = agent(world, "b")                                         # e save non lo pubblica mai
    exp = next_state(b, world)
    expect(2, pfckpt.save, b, BR, "origin", exp, "x", D, agent="claude")
    assert pfckpt.remote_head(b, "origin", BR) == exp


def test_nonexistent_sha_without_transition_is_not_ready(world):
    a = agent(world, "a")
    next_state(a, world, code={"branch": CODE_BR, "sha": "ab" * 20})
    pfckpt.write_manifest(a, D)
    raw_commit(a, "sha inventato")
    sh(a, "push", "-q", "origin", f"HEAD:refs/heads/{BR}")
    err = expect(2, pfckpt.bootstrap, world["url"], BR, str(world["tmp"] / "fresh"), D, CODE_BR)
    assert err.reason == "NEEDS_RECONCILIATION" and err.detail["last_valid_checkpoint"] == world["first"]


def test_not_proven_transition_or_malformed_record_is_not_ready(world):
    feat, ftree = make_feature(world)
    _legacy_checkpoint_citing(world, feat)
    squash, _ = squash_merge_and_delete(world, feat)
    for i, integ in enumerate([
            {"original_sha": feat, "original_tree": ftree, "integration_sha": squash, "integration_tree": ftree,
             "equivalence": "NOT_PROVEN"},
            {"original_sha": feat, "original_tree": ftree, "integration_sha": "main", "integration_tree": ftree,
             "equivalence": "TREE_EQUAL"},                         # riferimento mobile, mai "l'ultimo main"
            {"original_sha": "0" * 40, "original_tree": ftree, "integration_sha": squash, "integration_tree": ftree,
             "equivalence": "TREE_EQUAL"}]):                       # original_sha diverso dallo SHA citato
        a = agent(world, f"n{i}")
        next_state(a, world, code={"branch": FEATURE, "sha": feat, "integration": integ})
        pfckpt.write_manifest(a, D)
        raw_commit(a, "transizione non provata")
        expect(2, pfckpt.verify, a, "HEAD", D)


def test_build_integration_without_original_object_or_recorded_tree_is_blocked(world):
    feat, _ = make_feature(world)
    _legacy_checkpoint_citing(world, feat)
    squash, _ = squash_merge_and_delete(world, feat)
    a = agent(world, "after")
    sh(a, "fetch", "-q", "origin", "+refs/heads/main:refs/remotes/origin/main")
    err = expect(2, pfckpt.build_integration, a, feat, squash)
    assert err.reason == "ORIGINAL_TREE_UNKNOWN"


def test_recorded_code_tree_anchors_the_original_tree(world):
    """save registra code.tree quando l'oggetto c'e': un original_tree diverso dalla registrazione e' rifiutato."""
    feat, ftree = make_feature(world)
    a = ckpt_agent(world, "a")
    exp = next_state(a, world, code={"branch": FEATURE, "sha": feat})
    pfckpt.save(a, BR, "origin", exp, "ckpt", D, agent="claude", release=True, allow_branch_only=True)
    assert pfckpt.tree_json(a, "HEAD", D, "STATE.json")["code"]["tree"] == ftree
    squash, _ = squash_merge_and_delete(world, feat, alter="app.py")
    b = agent(world, "b")
    sh(b, "fetch", "-q", "origin", "+refs/heads/main:refs/remotes/origin/main")
    sq_tree = sh(b, "rev-parse", f"{squash}^{{tree}}")
    st = json.load(open(os.path.join(b, D, "STATE.json")))
    lie = {"original_sha": feat, "original_tree": sq_tree, "integration_sha": squash,
           "integration_tree": sq_tree, "equivalence": "TREE_EQUAL"}
    next_state(b, world, code=dict(st["code"], integration=lie))
    pfckpt.write_manifest(b, D)
    raw_commit(b, "original_tree che contraddice code.tree")
    err = expect(2, pfckpt.verify, b, "HEAD", D)
    assert any("code.tree" in p for p in err.detail["problems"]), err.detail["problems"]


def test_cli_integrate_reports_tree_equal_and_not_proven(world, capsys):
    feat, _ = make_feature(world)
    a = ckpt_agent(world, "a")
    squash, _ = squash_merge_and_delete(world, feat)
    sh(a, "fetch", "-q", "origin", "+refs/heads/main:refs/remotes/origin/main")
    assert pfckpt.main(["integrate", "--repo", a, "--original", feat, "--integration", squash]) == 0
    assert json.loads(capsys.readouterr().out)["equivalence"] == "TREE_EQUAL"
    assert pfckpt.main(["integrate", "--repo", a, "--original", feat, "--integration", world["code"]]) == 2
    assert json.loads(capsys.readouterr().out)["equivalence"] == "NOT_PROVEN"


# ================================================================== 4. interruzioni e ripresa senza duplicati
def test_crash_after_commit_resume_is_durable_without_duplicates(world):
    feat, _ = make_feature(world)
    open_pr(world, 1, feat)
    a = ckpt_agent(world, "a")
    exp = next_state(a, world, code={"branch": FEATURE, "sha": feat, "pr": 1})
    sh(a, "remote", "set-url", "--push", "origin", str(world["tmp"] / "inesistente.git"))
    with pytest.raises(pfckpt.CheckpointError):
        pfckpt.save(a, BR, "origin", exp, "ckpt 2", D, agent="claude")
    assert pfckpt.remote_head(a, "origin", BR) == exp
    sh(a, "remote", "set-url", "--push", "origin", world["url"])
    res = pfckpt.save(a, BR, "origin", exp, "ckpt 2", D, agent="claude")
    assert res["status"] == "DURABLE" and res["resumed"] == "after_commit" and _vias(res) == {"code.sha": "PULL:1"}
    assert sh(a, "rev-list", "--count", f"{exp}..HEAD") == "1"


def test_resume_of_a_non_durable_local_commit_is_refused(world):
    """Commit locale fatto fuori dallo strumento con uno SHA non durevole: la ripresa non lo pubblica."""
    feat, _ = make_feature(world)
    a = ckpt_agent(world, "a")
    exp = next_state(a, world, code={"branch": FEATURE, "sha": feat})
    pfckpt.write_manifest(a, D)
    raw_commit(a, "ckpt 2")
    err = expect(2, pfckpt.save, a, BR, "origin", exp, "ckpt 2", D, agent="claude")
    assert err.reason == "CODE_SHA_NOT_DURABLE" and pfckpt.remote_head(a, "origin", BR) == exp


def test_crash_after_push_before_readback_resumes(world):
    feat, _ = make_feature(world)
    open_pr(world, 1, feat)
    a = ckpt_agent(world, "a")
    exp = next_state(a, world, code={"branch": FEATURE, "sha": feat, "pr": 1})
    pfckpt.write_manifest(a, D)
    raw_commit(a, "ckpt 2")
    sh(a, "push", "-q", "origin", f"HEAD:refs/heads/{BR}")
    res = pfckpt.save(a, BR, "origin", exp, "ckpt 2", D, agent="claude")
    assert res["status"] == "DURABLE" and res["resumed"] == "after_push"
    assert sh(a, "rev-list", "--count", f"{exp}..HEAD") == "1"


def test_repeated_save_is_idempotent_and_a_stale_writer_is_refused(world):
    """Operazione gia' registrata: ripetere lo stesso save dopo DURABLE non crea un secondo checkpoint; uno scrittore
    sulla vecchia base riceve CONCURRENT_UPDATE."""
    a = agent(world, "a")
    exp = next_state(a, world)
    first = pfckpt.save(a, BR, "origin", exp, "ckpt 2", D, agent="claude")
    again = pfckpt.save(a, BR, "origin", exp, "ckpt 2", D, agent="claude")
    assert again["status"] == "DURABLE" and again["resumed"] == "after_push" and again["commit"] == first["commit"]
    assert sh(a, "rev-list", "--count", f"{exp}..HEAD") == "1"
    b = agent(world, "stale")
    sh(b, "reset", "-q", "--hard", exp)
    next_state(b, world)
    err = expect(3, pfckpt.save, b, BR, "origin", exp, "ckpt 2 bis", D, agent="claude")
    assert err.reason == "CONCURRENT_UPDATE" and pfckpt.remote_head(a, "origin", BR) == first["commit"]


# ================================================================== 5. ref keep: SOLO opt-in
def test_keep_refs_opt_in_published_atomically_and_survive_squash(world):
    feat, _ = make_feature(world)
    a = ckpt_agent(world, "a")
    exp = next_state(a, world, code={"branch": FEATURE, "sha": feat}, tests_executed=_tests(feat))
    res = pfckpt.save(a, BR, "origin", exp, "ckpt", D, agent="claude", keep_refs=True, release=True)
    assert _vias(res) == {"code.sha": "KEEP_REF", "test 'suite'": "KEEP_REF"}
    assert _remote_refs(world, KEEP) == [f"{KEEP}{feat} {feat}"]
    squash_merge_and_delete(world, feat)
    rep = pfckpt.bootstrap(world["url"], BR, str(world["tmp"] / "fresh"), D, CODE_BR)
    assert rep["status"] == "READY" and rep["keep_refs"]["missing"] == []


def test_keep_ref_pointing_elsewhere_is_never_forced(world):
    feat, _ = make_feature(world)
    code = world["code"]
    a = ckpt_agent(world, "a")
    sh(a, "push", "-q", "origin", f"{feat}:{KEEP}{code}")         # ref keep di `code` che punta altrove (non-FF)
    exp = next_state(a, world)
    with pytest.raises(pfckpt.CheckpointError) as ei:
        pfckpt.save(a, BR, "origin", exp, "ckpt 2", D, agent="claude", keep_refs=True)
    assert ei.value.code in (3, 4, 5)
    assert pfckpt.remote_head(a, "origin", BR) == exp, "push atomico: nemmeno il branch e' avanzato"
    assert _remote_refs(world, f"{KEEP}{code}") == [f"{KEEP}{code} {feat}"], "mai force sul ref keep"
