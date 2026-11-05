"""pfckpt_squash_durable_refs — SHA di codice durevoli dopo squash merge + cancellazione del branch (git REALE, offline).

Incidente reale: `agent-state/pf-claude-b` seq 17 citava in `code.sha` e `tests_executed[].sha` l'head di una PR
integrata con squash; cancellato il branch, lo SHA non era piu' raggiungibile da nessun ref remoto e `bootstrap`
dava LATEST_CHECKPOINT_CORRUPT. Qui: un repository bare locale fa da «GitHub», lo squash e' un commit nuovo su
`code` (il «main») con lo STESSO tree, il branch della PR viene cancellato e il bare remoto potato (gc --prune=now).
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
KEEP = getattr(pfckpt, "KEEP_REF_PREFIX", "refs/agent-state/keep/")


def _code_clone(world, name="codework"):
    path = str(world["tmp"] / name)
    if not os.path.exists(path):
        subprocess.run(["git", "clone", "-q", "--no-local", "--branch", CODE_BR, world["url"], path], check=True)
        _cfg(path)
    return path


def make_feature(world):
    """Branch di PR con un commit nuovo (non su `code`); ritorna (sha, tree)."""
    c = _code_clone(world)
    sh(c, "checkout", "-q", "-b", "feat")
    with open(os.path.join(c, "feat.py"), "w") as fh:
        fh.write("FEATURE = 1\n")
    sh(c, "add", "-A"); sh(c, "commit", "-q", "-m", "feature")
    sh(c, "push", "-q", "origin", f"HEAD:refs/heads/{FEATURE}")
    return sh(c, "rev-parse", "HEAD"), sh(c, "rev-parse", "HEAD^{tree}")


def squash_merge_and_delete(world, feature_sha, alter=None, pull_ref=None):
    """Squash merge simulato: nuovo commit su `code` con lo stesso tree del branch di PR (o, con `alter`, con un
    solo file diverso), poi cancellazione del branch sul bare e potatura degli oggetti irraggiungibili."""
    c = _code_clone(world)
    sh(c, "fetch", "-q", "origin")
    sh(c, "checkout", "-q", "-B", "squash", f"origin/{CODE_BR}")
    sh(c, "checkout", "-q", feature_sha, "--", ".")
    if alter:
        with open(os.path.join(c, alter), "a") as fh:
            fh.write("# diverso\n")
    sh(c, "add", "-A"); sh(c, "commit", "-q", "-m", "Squash merge PR #123")
    squash = sh(c, "rev-parse", "HEAD")
    sh(c, "push", "-q", "origin", f"HEAD:refs/heads/{CODE_BR}")
    if pull_ref:                                                # come GitHub: refs/pull/<N>/head sopravvive al branch
        sh(world["url"], "update-ref", pull_ref, feature_sha if pull_ref.endswith("/1/head") else world["code"])
    sh(c, "push", "-q", "origin", "--delete", FEATURE)
    sh(world["url"], "reflog", "expire", "--expire=now", "--all")
    sh(world["url"], "gc", "-q", "--prune=now")
    return squash, sh(c, "rev-parse", "HEAD^{tree}")


def ckpt_agent(world, name):
    """Agente sul branch di stato che conosce anche il branch di PR (come la sessione che ha lavorato sulla PR)."""
    a = agent(world, name)
    sh(a, "fetch", "-q", "origin", f"+refs/heads/*:refs/remotes/origin/*")
    return a


def _tests(sha):
    return [{"id": "suite", "status": "PASS", "sha": sha}]


def _remote_keep(world, sha):
    line = sh(world["url"], "for-each-ref", "--format=%(objectname)", f"{KEEP}{sha}")
    return line or None


# ================================================================== 1. ref durevole al save
def test_save_publishes_keep_refs_for_every_cited_sha_in_the_same_push(world):
    feat, _ = make_feature(world)
    a = ckpt_agent(world, "a")
    code_ref = {"branch": FEATURE, "sha": feat}
    exp = next_state(a, world, code=code_ref, tests_executed=_tests(feat[:12]))
    res = pfckpt.save(a, BR, "origin", exp, "ckpt sulla PR", D, agent="claude")
    assert res["status"] == "DURABLE"
    assert _remote_keep(world, feat) == feat, "ref keep pubblicato sul remoto e puntato allo SHA"
    assert res["keep_shas"] == [feat]


def test_squash_merge_and_branch_deletion_bootstrap_ready_through_keep_ref(world):
    feat, _ = make_feature(world)
    a = ckpt_agent(world, "a")
    exp = next_state(a, world, code={"branch": FEATURE, "sha": feat}, tests_executed=_tests(feat))
    pfckpt.save(a, BR, "origin", exp, "ckpt sulla PR", D, agent="claude", release=True)
    squash_merge_and_delete(world, feat)
    assert sh(world["url"], "for-each-ref", f"refs/heads/{FEATURE}") == "", "branch di PR cancellato"
    rep = pfckpt.bootstrap(world["url"], BR, str(world["tmp"] / "fresh"), D, CODE_BR)
    assert rep["status"] == "READY" and rep["code_sha"] == feat
    assert rep["code_reference"]["via"] == "ORIGINAL" and rep["keep_refs"]["missing"] == []


# ================================================================== 2. transizione verso main (tree-equal)
def _legacy_checkpoint_citing(world, feat):
    """Checkpoint scritto SENZA ref keep (come seq 17 prima della fix), pubblicato sul remoto."""
    a = ckpt_agent(world, "legacy")
    next_state(a, world, code={"branch": FEATURE, "sha": feat}, tests_executed=_tests(feat))
    pfckpt.write_manifest(a, D)
    raw_commit(a, "ckpt legacy senza ref keep")
    sh(a, "push", "-q", "origin", f"HEAD:refs/heads/{BR}")
    return sh(a, "rev-parse", "HEAD")


def test_legacy_checkpoint_after_squash_is_needs_reconciliation_never_ready(world):
    feat, _ = make_feature(world)
    _legacy_checkpoint_citing(world, feat)
    squash_merge_and_delete(world, feat)
    err = expect(2, pfckpt.bootstrap, world["url"], BR, str(world["tmp"] / "fresh"), D, CODE_BR)
    assert err.reason == "NEEDS_RECONCILIATION", err.reason
    assert any("SHA del codice inesistente" in p for p in err.detail["problems"])


def test_tree_equal_transition_new_checkpoint_bootstraps_ready_from_fresh_clone(world):
    feat, ftree = make_feature(world)
    legacy = _legacy_checkpoint_citing(world, feat)
    squash, stree = squash_merge_and_delete(world, feat)
    assert stree == ftree
    a = agent(world, "after")                                   # clone NUOVO: lo SHA originale non esiste qui
    assert pfckpt.git(a, "cat-file", "-e", f"{feat}^{{commit}}", check=False).returncode != 0
    integ = pfckpt.build_integration(a, feat, squash, original_tree=ftree)
    assert integ["equivalence"] == "TREE_EQUAL" and integ["original_tree_source"] == "RECORDED"
    st = json.load(open(os.path.join(a, D, "STATE.json")))
    code = dict(st["code"], integration=integ)
    tests = [dict(t, integration=integ) for t in st["tests_executed"]]
    exp = next_state(a, world, code=code, tests_executed=tests)
    assert exp == legacy
    res = pfckpt.save(a, BR, "origin", exp, "transizione verso main", D, agent="claude", release=True)
    assert res["status"] == "DURABLE" and res["keep_shas"] == [squash]
    assert _remote_keep(world, squash) == squash
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
    squash, _ = squash_merge_and_delete(world, feat)            # il clone `a` conserva ancora l'oggetto originale
    sh(a, "fetch", "-q", "origin", f"{CODE_BR}:refs/remotes/origin/{CODE_BR}")
    integ = pfckpt.build_integration(a, feat, squash)
    assert integ["original_tree_source"] == "OBJECT" and integ["original_tree"] == ftree
    bad = dict(integ, original_tree=sh(a, "rev-parse", f"origin/{CODE_BR}~1^{{tree}}"))
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
    if lie == "computed":
        integ = pfckpt.build_integration(a, feat, squash, original_tree=ftree)
        assert integ["equivalence"] == "NOT_PROVEN"
    else:                                                       # record che mente: dichiara TREE_EQUAL
        integ = {"original_sha": feat, "original_tree": ftree, "integration_sha": squash,
                 "integration_tree": ftree, "equivalence": "TREE_EQUAL"}
    next_state(a, world, code={"branch": FEATURE, "sha": feat, "integration": integ},
               tests_executed=[{"id": "suite", "status": "PASS", "sha": feat, "integration": integ}])
    pfckpt.write_manifest(a, D)
    raw_commit(a, "transizione con tree diverso")
    sh(a, "push", "-q", "origin", f"HEAD:refs/heads/{BR}")
    err = expect(2, pfckpt.bootstrap, world["url"], BR, str(world["tmp"] / "fresh"), D, CODE_BR)
    assert err.reason == "NEEDS_RECONCILIATION"
    with pytest.raises(pfckpt.CheckpointError) as ei:            # e save non lo pubblica mai
        b = agent(world, "b")
        exp = next_state(b, world)
        pfckpt.save(b, BR, "origin", exp, "x", D, agent="claude")
    assert ei.value.code == 2


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
             "equivalence": "TREE_EQUAL"},                       # riferimento mobile, mai "l'ultimo main"
            {"original_sha": "0" * 40, "original_tree": ftree, "integration_sha": squash, "integration_tree": ftree,
             "equivalence": "TREE_EQUAL"}]):                     # original_sha diverso dallo SHA citato
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
    err = expect(2, pfckpt.build_integration, a, feat, squash)
    assert err.reason == "ORIGINAL_TREE_UNKNOWN"


def test_recorded_code_tree_anchors_the_original_tree(world):
    """save registra code.tree quando l'oggetto c'e': un original_tree diverso dalla registrazione e' rifiutato."""
    feat, ftree = make_feature(world)
    a = ckpt_agent(world, "a")
    exp = next_state(a, world, code={"branch": FEATURE, "sha": feat})
    pfckpt.save(a, BR, "origin", exp, "ckpt", D, agent="claude", release=True)
    assert pfckpt.tree_json(a, "HEAD", D, "STATE.json")["code"]["tree"] == ftree
    squash, _ = squash_merge_and_delete(world, feat, alter="app.py")
    b = agent(world, "b")
    sq_tree = sh(b, "rev-parse", f"{squash}^{{tree}}")
    st = json.load(open(os.path.join(b, D, "STATE.json")))
    lie = {"original_sha": feat, "original_tree": sq_tree, "integration_sha": squash,
           "integration_tree": sq_tree, "equivalence": "TREE_EQUAL"}
    next_state(b, world, code=dict(st["code"], integration=lie))
    pfckpt.write_manifest(b, D)
    raw_commit(b, "original_tree che contraddice code.tree")
    err = expect(2, pfckpt.verify, b, "HEAD", D)
    assert any("code.tree" in p for p in err.detail["problems"]), err.detail["problems"]


# ================================================================== 3. interruzioni e ripresa senza duplicati
def test_crash_after_branch_push_before_keep_refs_resumes_without_duplicates(world):
    """Push del solo branch (crash/strumento vecchio) e poi ripresa: i ref keep arrivano, nessun commit duplicato."""
    feat, _ = make_feature(world)
    a = ckpt_agent(world, "a")
    exp = next_state(a, world, code={"branch": FEATURE, "sha": feat}, tests_executed=_tests(feat))
    pfckpt.write_manifest(a, D)
    raw_commit(a, "ckpt 2")
    sh(a, "push", "-q", "origin", f"HEAD:refs/heads/{BR}")
    assert _remote_keep(world, feat) is None
    res = pfckpt.save(a, BR, "origin", exp, "ckpt 2", D, agent="claude")
    assert res["status"] == "DURABLE" and res["resumed"] == "after_push"
    assert _remote_keep(world, feat) == feat
    assert sh(a, "rev-list", "--count", f"{exp}..HEAD") == "1"


def test_crash_after_commit_resume_pushes_branch_and_keep_refs_atomically(world):
    feat, _ = make_feature(world)
    a = ckpt_agent(world, "a")
    exp = next_state(a, world, code={"branch": FEATURE, "sha": feat})
    sh(a, "remote", "set-url", "--push", "origin", str(world["tmp"] / "inesistente.git"))
    with pytest.raises(pfckpt.CheckpointError):
        pfckpt.save(a, BR, "origin", exp, "ckpt 2", D, agent="claude")
    assert pfckpt.remote_head(a, "origin", BR) == exp and _remote_keep(world, feat) is None
    sh(a, "remote", "set-url", "--push", "origin", world["url"])
    res = pfckpt.save(a, BR, "origin", exp, "ckpt 2", D, agent="claude")
    assert res["status"] == "DURABLE" and res["resumed"] == "after_commit"
    assert _remote_keep(world, feat) == feat and sh(a, "rev-list", "--count", f"{exp}..HEAD") == "1"


def test_keep_refs_already_present_are_not_duplicated_and_save_is_not_repeated(world):
    """Operazione gia' registrata: i ref keep pre-esistenti non si duplicano ne' si forzano; ripetere lo stesso save
    dopo DURABLE non crea un secondo checkpoint (CONCURRENT_UPDATE, nessuna scrittura)."""
    feat, _ = make_feature(world)
    a = ckpt_agent(world, "a")
    sh(a, "push", "-q", "origin", f"{feat}:{KEEP}{feat}")       # ref keep gia' presente (save precedente interrotto)
    exp = next_state(a, world, code={"branch": FEATURE, "sha": feat}, tests_executed=_tests(feat))
    res = pfckpt.save(a, BR, "origin", exp, "ckpt 2", D, agent="claude")
    assert res["status"] == "DURABLE"
    keep_refs = sh(world["url"], "for-each-ref", "--format=%(refname)", KEEP.rstrip("/")).splitlines()
    assert keep_refs == [f"{KEEP}{feat}"], keep_refs
    head = pfckpt.remote_head(a, "origin", BR)
    again = pfckpt.save(a, BR, "origin", exp, "ckpt 2", D, agent="claude")   # stesso save ripetuto: idempotente
    assert again["status"] == "DURABLE" and again["resumed"] == "after_push" and again["commit"] == head
    assert sh(a, "rev-list", "--count", f"{exp}..HEAD") == "1" and pfckpt.remote_head(a, "origin", BR) == head
    assert sh(world["url"], "for-each-ref", "--format=%(refname)", KEEP.rstrip("/")).splitlines() == keep_refs
    b = agent(world, "stale")                                   # un altro scrittore sulla vecchia base: rifiutato
    sh(b, "reset", "-q", "--hard", exp)
    next_state(b, world)
    err = expect(3, pfckpt.save, b, BR, "origin", exp, "ckpt 2 bis", D, agent="claude")
    assert err.reason == "CONCURRENT_UPDATE" and pfckpt.remote_head(a, "origin", BR) == head


def test_readback_refuses_when_a_keep_ref_points_elsewhere(world):
    """Un ref keep che punta a un altro commit non viene mai forzato: save fallisce, nessun DURABLE."""
    feat, _ = make_feature(world)
    code = world["code"]
    a = ckpt_agent(world, "a")
    sh(a, "push", "-q", "origin", f"{feat}:{KEEP}{code}")       # ref keep di `code` che punta altrove (non-FF)
    exp = next_state(a, world)
    with pytest.raises(pfckpt.CheckpointError) as ei:
        pfckpt.save(a, BR, "origin", exp, "ckpt 2", D, agent="claude")
    assert ei.value.code in (3, 4, 5)
    assert pfckpt.remote_head(a, "origin", BR) == exp, "push atomico: nemmeno il branch e' avanzato"
    assert _remote_keep(world, code) == feat, "mai force sul ref keep"


# ================================================================== 4. fallback GitHub refs/pull/<N>/head
@pytest.mark.parametrize("declared_pr", [None, 1])
def test_legacy_checkpoint_resolves_through_github_pull_head_after_branch_deletion(world, declared_pr):
    """GitHub conserva refs/pull/<N>/head: senza ref keep ne' transizione, bootstrap lo scarica (dichiarato in
    code.pr, oppure tutti i refs/pull/*/head se non dichiarato) e lo SHA originale torna risolvibile."""
    feat, _ = make_feature(world)
    a = ckpt_agent(world, "legacy")
    code = {"branch": FEATURE, "sha": feat, **({"pr": declared_pr} if declared_pr else {})}
    next_state(a, world, code=code, tests_executed=_tests(feat))
    pfckpt.write_manifest(a, D)
    raw_commit(a, "ckpt legacy")
    sh(a, "push", "-q", "origin", f"HEAD:refs/heads/{BR}")
    squash_merge_and_delete(world, feat, pull_ref="refs/pull/1/head")
    rep = pfckpt.bootstrap(world["url"], BR, str(world["tmp"] / "fresh"), D, CODE_BR)
    assert rep["status"] == "READY" and rep["code_reference"]["via"] == "ORIGINAL"
    assert rep["pull_refs_fetched"]["declared_prs"] == ([1] if declared_pr else [])
    assert rep["keep_refs"]["missing"] == [feat], "ancora senza ref keep: il prossimo save lo pubblica"


def test_pull_head_of_another_pr_does_not_make_the_sha_resolvable(world):
    feat, _ = make_feature(world)
    _legacy_checkpoint_citing(world, feat)
    squash_merge_and_delete(world, feat, pull_ref="refs/pull/2/head")   # punta ad altro: lo SHA resta irrisolto
    err = expect(2, pfckpt.bootstrap, world["url"], BR, str(world["tmp"] / "fresh"), D, CODE_BR)
    assert err.reason == "NEEDS_RECONCILIATION"


def test_cli_integrate_reports_tree_equal_and_not_proven(world, capsys):
    feat, ftree = make_feature(world)
    a = ckpt_agent(world, "a")
    squash, _ = squash_merge_and_delete(world, feat)
    sh(a, "fetch", "-q", "origin", f"+{CODE_BR}:refs/remotes/origin/{CODE_BR}")
    assert pfckpt.main(["integrate", "--repo", a, "--original", feat, "--integration", squash]) == 0
    assert json.loads(capsys.readouterr().out)["equivalence"] == "TREE_EQUAL"
    assert pfckpt.main(["integrate", "--repo", a, "--original", feat, "--integration", world["code"]]) == 2
    assert json.loads(capsys.readouterr().out)["equivalence"] == "NOT_PROVEN"
